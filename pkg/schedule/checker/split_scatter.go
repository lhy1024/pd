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
	"math"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const (
	splitScatterPendingTTL      = 3 * time.Minute
	splitScatterQueueGCInterval = time.Minute
	splitScatterDispatchLimit   = 4
)

type splitScatterPendingItem struct {
	group string
	// rangeHint is the best-effort key range used to seed table/index-scoped
	// scatter distribution before the first dispatch of this pending region.
	rangeHint splitScatterRangeHint
	observed bool
	score    uint64
}

// splitScatterRangeHint is a derived key range for the current table/index
// group. When available, split-scatter seeds the scatterer's group
// distribution with the existing region count in this range before dispatch.
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
	hasPending atomic.Bool
	mu         struct {
		syncutil.RWMutex
		pending *cache.TTLUint64
	}
}

func newSplitScatterManager(ctx context.Context) *splitScatterManager {
	m := &splitScatterManager{}
	m.mu.pending = cache.NewIDTTL(ctx, splitScatterQueueGCInterval, splitScatterPendingTTL)
	return m
}

func (m *splitScatterManager) recordBatch(sourceRegionID uint64, newRegionIDs []uint64) {
	if len(newRegionIDs) == 0 {
		return
	}
	group := makeSplitScatterGroup(sourceRegionID, newRegionIDs[0])
	m.hasPending.Store(true)
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
	item.rangeHint = resolveSplitScatterRangeHint(region)
	item.observed = true
	item.score = splitScatterCPUScore(region)
	m.mu.pending.Put(region.GetID(), item)
}

func (m *splitScatterManager) getTopPendingRegionIDs(limit int) []uint64 {
	if limit <= 0 {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	pendingIDs := m.mu.pending.GetAllID()
	if len(pendingIDs) == 0 {
		m.hasPending.Store(false)
		return nil
	}
	topIDs := make([]uint64, 0, min(limit, len(pendingIDs)))
	topScores := make([]uint64, 0, min(limit, len(pendingIDs)))
	for _, regionID := range pendingIDs {
		pendingItem, ok := m.getPendingItemLocked(regionID)
		if !ok {
			continue
		}
		if !pendingItem.observed {
			continue
		}
		insertAt := len(topIDs)
		for i, existingID := range topIDs {
			existingScore := topScores[i]
			if pendingItem.score > existingScore || (pendingItem.score == existingScore && regionID < existingID) {
				insertAt = i
				break
			}
		}
		if insertAt == len(topIDs) && len(topIDs) >= limit {
			continue
		}
		topIDs = append(topIDs, 0)
		topScores = append(topScores, 0)
		copy(topIDs[insertAt+1:], topIDs[insertAt:])
		copy(topScores[insertAt+1:], topScores[insertAt:])
		topIDs[insertAt] = regionID
		topScores[insertAt] = pendingItem.score
		if len(topIDs) > limit {
			topIDs = topIDs[:limit]
			topScores = topScores[:limit]
		}
	}
	return topIDs
}

func (m *splitScatterManager) getPendingSnapshot(regionID uint64) (string, splitScatterRangeHint, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	item, ok := m.getPendingItemLocked(regionID)
	if !ok || !item.observed {
		return "", splitScatterRangeHint{}, false
	}
	return item.group, item.rangeHint.clone(), true
}

func (m *splitScatterManager) remove(regionID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.pending.Remove(regionID)
	m.clearPotentialPendingIfEmptyLocked()
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

func (m *splitScatterManager) pendingCountLocked() int {
	return len(m.mu.pending.GetAllID())
}

func (m *splitScatterManager) hasPotentialPending() bool {
	return m.hasPending.Load()
}

func (m *splitScatterManager) clearPotentialPendingIfEmptyLocked() {
	if m.pendingCountLocked() == 0 {
		m.hasPending.Store(false)
	}
}

func splitScatterCPUScore(region *core.RegionInfo) uint64 {
	if !region.HasCPUStats() {
		// Split-scatter falls back to the legacy heartbeat cpu_usage field only
		// when cpu_stats is entirely unavailable, so cpu_stats-reported zeroes
		// still mean zero instead of implicitly reviving the deprecated metric.
		return region.GetCPUUsage()
	}
	readCPU := region.GetReadCPUUsage()
	schedulerCPU := region.GetSchedulerCPUUsage()
	if schedulerCPU > math.MaxUint64-readCPU {
		return math.MaxUint64
	}
	return readCPU + schedulerCPU
}

func makeSplitScatterGroup(sourceRegionID, firstNewRegionID uint64) string {
	return fmt.Sprintf("split-scatter-%d-%d", sourceRegionID, firstNewRegionID)
}

// RecordSplitScatterBatch records a newly split batch for later scatter.
func (c *Controller) RecordSplitScatterBatch(sourceRegionID uint64, newRegionIDs []uint64) {
	c.splitScatterQueue.recordBatch(sourceRegionID, newRegionIDs)
}

// ObserveSplitScatterRegion updates split-scatter priority using the latest region stats.
func (c *Controller) ObserveSplitScatterRegion(region *core.RegionInfo) {
	c.splitScatterQueue.observe(region)
}

// HasPotentialPendingSplitScatterRegions returns whether split-scatter may have
// pending work. It is a cheap fast-path guard for heartbeat hot paths.
func (c *Controller) HasPotentialPendingSplitScatterRegions() bool {
	return c.splitScatterQueue.hasPotentialPending()
}

// DispatchSplitScatterRegionsForTest dispatches pending split-scatter regions.
// The function is exposed for test purpose.
func (c *Controller) DispatchSplitScatterRegionsForTest() {
	c.dispatchSplitScatterRegions()
}

func (c *Controller) dispatchSplitScatterRegions() {
	if c.regionScatterer == nil {
		return
	}
	regionIDs := c.splitScatterQueue.getTopPendingRegionIDs(splitScatterDispatchLimit)
	for _, regionID := range regionIDs {
		group, rangeHint, ok := c.splitScatterQueue.getPendingSnapshot(regionID)
		if !ok {
			c.splitScatterQueue.remove(regionID)
			continue
		}
		region := c.cluster.GetRegion(regionID)
		if region == nil {
			c.splitScatterQueue.remove(regionID)
			continue
		}
		if rangeHint.valid() {
			c.regionScatterer.SeedGroupDistributionByRange(group, rangeHint.startKey, rangeHint.endKey)
		}
		op, err := c.regionScatterer.ScatterInternal(region, group)
		if err != nil {
			log.Info("dispatch internal split scatter failed",
				zap.Uint64("region-id", regionID),
				zap.String("group", group),
				zap.Error(err))
			continue
		}
		if op != nil {
			if c.opController.AddWaitingOperator(op) == 0 {
				log.Info("dispatch internal split scatter add operator failed",
					zap.Uint64("region-id", regionID),
					zap.String("group", group),
					zap.String("operator-desc", op.Desc()))
				continue
			}
			if c.opController.GetOperator(region.GetID()) != op {
				log.Info("dispatch internal split scatter operator lost before commit",
					zap.Uint64("region-id", regionID),
					zap.String("group", group),
					zap.String("operator-desc", op.Desc()))
				continue
			}
			c.regionScatterer.Commit(region, op, group)
		}
		c.splitScatterQueue.remove(regionID)
	}
}
