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
	observed  bool
	score     uint64
}

type splitScatterDispatchSnapshot struct {
	regionID uint64
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

func (m *splitScatterManager) collectTopPending(limit int) []splitScatterDispatchSnapshot {
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
	snapshots := make([]splitScatterDispatchSnapshot, 0, min(limit, len(pendingIDs)))
	for _, regionID := range pendingIDs {
		pendingItem, ok := m.getPendingItemLocked(regionID)
		if !ok {
			continue
		}
		if !pendingItem.observed {
			continue
		}
		snapshot := splitScatterDispatchSnapshot{
			regionID: regionID,
			score:    pendingItem.score,
		}
		insertAt := len(snapshots)
		for i, existing := range snapshots {
			if snapshot.score > existing.score || (snapshot.score == existing.score && snapshot.regionID < existing.regionID) {
				insertAt = i
				break
			}
		}
		if insertAt == len(snapshots) && len(snapshots) >= limit {
			continue
		}
		snapshots = append(snapshots, splitScatterDispatchSnapshot{})
		copy(snapshots[insertAt+1:], snapshots[insertAt:])
		snapshots[insertAt] = snapshot
		if len(snapshots) > limit {
			snapshots = snapshots[:limit]
		}
	}
	return snapshots
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
	snapshots := c.splitScatterQueue.collectTopPending(splitScatterDispatchLimit)
	for _, snapshot := range snapshots {
		c.splitScatterQueue.mu.RLock()
		item, ok := c.splitScatterQueue.getPendingItemLocked(snapshot.regionID)
		if ok {
			ok = item.observed
		}
		var group string
		var rangeHint splitScatterRangeHint
		if ok {
			group = item.group
			rangeHint = item.rangeHint.clone()
		}
		c.splitScatterQueue.mu.RUnlock()
		if !ok {
			c.splitScatterQueue.remove(snapshot.regionID)
			continue
		}
		region := c.cluster.GetRegion(snapshot.regionID)
		if region == nil {
			c.splitScatterQueue.remove(snapshot.regionID)
			continue
		}
		if rangeHint.valid() {
			c.regionScatterer.SeedGroupDistributionByRange(group, rangeHint.startKey, rangeHint.endKey)
		}
		op, err := c.regionScatterer.ScatterInternal(region, group)
		if err != nil {
			log.Info("dispatch internal split scatter failed",
				zap.Uint64("region-id", snapshot.regionID),
				zap.String("group", group),
				zap.Error(err))
			continue
		}
		if op != nil {
			if c.opController.AddWaitingOperator(op) == 0 {
				log.Info("dispatch internal split scatter add operator failed",
					zap.Uint64("region-id", snapshot.regionID),
					zap.String("group", group),
					zap.String("operator-desc", op.Desc()))
				continue
			}
			if c.opController.GetOperator(region.GetID()) != op {
				log.Info("dispatch internal split scatter operator lost before commit",
					zap.Uint64("region-id", snapshot.regionID),
					zap.String("group", group),
					zap.String("operator-desc", op.Desc()))
				continue
			}
			c.regionScatterer.Commit(region, op, group)
		}
		c.splitScatterQueue.remove(snapshot.regionID)
	}
}
