// Copyright 2026 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package checker

import (
	"context"
	"fmt"
	"time"

	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const (
	splitScatterPendingTTL        = 3 * time.Minute
	splitScatterQueueGCInterval   = time.Minute
	splitScatterQueueCapacity     = 1024
	splitScatterDispatchLimit     = 4
	splitScatterRetryBaseInterval = time.Second
	splitScatterRetryMaxInterval  = time.Minute
)

type splitScatterPendingItem struct {
	group     string
	rangeHint splitScatterRangeHint
}

type splitScatterPriorityItem struct {
	regionID uint64
	group    string
	attempt  int
	last     time.Time
}

func (i *splitScatterPriorityItem) ID() uint64 {
	return i.regionID
}

func (i *splitScatterPriorityItem) ready(now time.Time) bool {
	if i.attempt <= 0 || i.last.IsZero() {
		return true
	}
	delay := time.Duration(i.attempt) * splitScatterRetryBaseInterval
	if delay > splitScatterRetryMaxInterval {
		delay = splitScatterRetryMaxInterval
	}
	return !now.Before(i.last.Add(delay))
}

type splitScatterCandidate struct {
	regionID  uint64
	group     string
	rangeHint splitScatterRangeHint
}

type splitScatterRangeHint struct {
	startKey []byte
	endKey   []byte
}

func (r splitScatterRangeHint) valid() bool {
	return len(r.startKey) > 0
}

func (r splitScatterRangeHint) clone() splitScatterRangeHint {
	return splitScatterRangeHint{
		startKey: append([]byte(nil), r.startKey...),
		endKey:   append([]byte(nil), r.endKey...),
	}
}

type splitScatterManager struct {
	mu struct {
		syncutil.RWMutex
		pending *cache.TTLUint64
		queue   *cache.PriorityQueue
	}
}

func newSplitScatterManager(ctx context.Context) *splitScatterManager {
	m := &splitScatterManager{}
	m.mu.pending = cache.NewIDTTL(ctx, splitScatterQueueGCInterval, splitScatterPendingTTL)
	m.mu.queue = cache.NewPriorityQueue(splitScatterQueueCapacity)
	return m
}

func (m *splitScatterManager) recordBatch(sourceRegionID uint64, newRegionIDs []uint64) {
	if len(newRegionIDs) == 0 {
		return
	}
	group := makeSplitScatterGroup(sourceRegionID, newRegionIDs[0])
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, regionID := range newRegionIDs {
		m.mu.pending.Put(regionID, &splitScatterPendingItem{group: group})
	}
	m.mu.pending.Put(sourceRegionID, &splitScatterPendingItem{group: group})
}

func (m *splitScatterManager) observe(region *core.RegionInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()

	item, ok := m.getPendingItemLocked(region.GetID())
	if !ok {
		return
	}
	hint := resolveSplitScatterGroup(region, item.group)
	item.group = hint.group
	item.rangeHint = hint.rangeHint
	m.mu.pending.Put(region.GetID(), item)
	priority := splitScatterPriority(region.GetCPUUsage())
	if entry := m.mu.queue.Get(region.GetID()); entry != nil {
		item := entry.Value.(*splitScatterPriorityItem)
		item.group = hint.group
		item.attempt = 0
		item.last = time.Time{}
		m.mu.queue.Put(priority, item)
		return
	}
	m.mu.queue.Put(priority, &splitScatterPriorityItem{
		regionID: region.GetID(),
		group:    hint.group,
	})
}

func (m *splitScatterManager) getCandidates(limit int) []splitScatterCandidate {
	if limit <= 0 {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	now := time.Now()
	entries := m.mu.queue.Elems()
	candidates := make([]splitScatterCandidate, 0, min(limit, len(entries)))
	for _, entry := range entries {
		item := entry.Value.(*splitScatterPriorityItem)
		pendingItem, ok := m.getPendingItemLocked(item.regionID)
		if !ok {
			continue
		}
		if !item.ready(now) {
			continue
		}
		candidates = append(candidates, splitScatterCandidate{
			regionID:  item.regionID,
			group:     pendingItem.group,
			rangeHint: pendingItem.rangeHint.clone(),
		})
		if len(candidates) >= limit {
			break
		}
	}
	return candidates
}

func (m *splitScatterManager) recordFailure(regionID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.getPendingItemLocked(regionID); !ok {
		m.mu.queue.Remove(regionID)
		return
	}
	entry := m.mu.queue.Get(regionID)
	if entry == nil {
		return
	}
	item := entry.Value.(*splitScatterPriorityItem)
	item.attempt++
	item.last = time.Now()
	m.mu.queue.Put(entry.Priority, item)
}

func (m *splitScatterManager) markSucceeded(regionID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.pending.Remove(regionID)
	m.mu.queue.Remove(regionID)
}

func (m *splitScatterManager) remove(regionID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.pending.Remove(regionID)
	m.mu.queue.Remove(regionID)
}

func (m *splitScatterManager) getPendingGroup(regionID uint64) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	item, ok := m.getPendingItemLocked(regionID)
	if !ok {
		return "", false
	}
	return item.group, true
}

func (m *splitScatterManager) getPendingItemLocked(regionID uint64) (*splitScatterPendingItem, bool) {
	value, ok := m.mu.pending.Get(regionID)
	if !ok {
		return nil, false
	}
	item, ok := value.(*splitScatterPendingItem)
	if !ok || item == nil {
		return nil, false
	}
	return item, true
}

func (m *splitScatterManager) pendingCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.mu.pending.GetAllID())
}

func splitScatterPriority(score uint64) int {
	maxInt := int(^uint(0) >> 1)
	if score > uint64(maxInt) {
		return -maxInt
	}
	return -int(score)
}

func makeSplitScatterGroup(sourceRegionID, firstNewRegionID uint64) string {
	return fmt.Sprintf("split-scatter-%d-%d", sourceRegionID, firstNewRegionID)
}

func (c *Controller) RecordSplitScatterBatch(sourceRegionID uint64, newRegionIDs []uint64) {
	c.splitScatter.recordBatch(sourceRegionID, newRegionIDs)
}

func (c *Controller) ObserveSplitScatterRegion(region *core.RegionInfo) {
	c.splitScatter.observe(region)
}

// CheckSplitScatterRegions dispatches recently split regions.
// The function is exposed for test purpose.
func (c *Controller) CheckSplitScatterRegions() {
	c.checkSplitScatterRegions()
}

func (c *Controller) checkSplitScatterRegions() {
	if c.splitScatterer == nil {
		return
	}
	candidates := c.splitScatter.getCandidates(splitScatterDispatchLimit)
	for _, candidate := range candidates {
		region := c.cluster.GetRegion(candidate.regionID)
		if region == nil {
			c.splitScatter.remove(candidate.regionID)
			continue
		}
		if candidate.rangeHint.valid() {
			c.splitScatterer.SeedGroupDistributionByRange(candidate.group, candidate.rangeHint.startKey, candidate.rangeHint.endKey)
		}
		op, err := c.splitScatterer.Scatter(region, candidate.group, false)
		if err != nil {
			c.splitScatter.recordFailure(candidate.regionID)
			continue
		}
		if op != nil && c.opController.AddWaitingOperator(op) == 0 {
			c.splitScatter.recordFailure(candidate.regionID)
			continue
		}
		c.splitScatter.markSucceeded(candidate.regionID)
	}
}
