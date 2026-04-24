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
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/log"
)

const (
	splitScatterPendingTTL      = 3 * time.Minute
	splitScatterQueueGCInterval = time.Minute
	splitScatterDispatchLimit   = 4
)

type splitScatterPendingItem struct {
	regionID uint64
	group    string
}

// splitScatterRangeHint is a derived key range for the current table/index
// group. When available, split-scatter seeds the scatterer's group
// distribution with the existing region count in this range before dispatch.
type splitScatterRangeHint struct {
	startKey []byte
	endKey   []byte
}

func (c *Controller) recordSplitScatterBatch(sourceRegionID uint64, newRegionIDs []uint64) {
	if len(newRegionIDs) == 0 {
		return
	}
	group := makeSplitScatterGroup(sourceRegionID, newRegionIDs[0])
	c.splitScatterPendingMu.Lock()
	defer c.splitScatterPendingMu.Unlock()

	for _, regionID := range newRegionIDs {
		c.splitScatterPending.Put(regionID, group)
	}
	c.splitScatterPending.Put(sourceRegionID, group)
}

func (c *Controller) collectTopPendingSplitScatter(limit int) []splitScatterPendingItem {
	if limit <= 0 {
		return nil
	}
	c.splitScatterPendingMu.RLock()
	pendingIDs := c.splitScatterPending.GetAllID()
	pendingRegions := make([]splitScatterPendingItem, 0, len(pendingIDs))
	for _, regionID := range pendingIDs {
		value, ok := c.splitScatterPending.Get(regionID)
		if !ok {
			continue
		}
		group, ok := value.(string)
		if !ok {
			continue
		}
		pendingRegions = append(pendingRegions, splitScatterPendingItem{
			regionID: regionID,
			group:    group,
		})
	}
	c.splitScatterPendingMu.RUnlock()

	type dispatchCandidate struct {
		pending splitScatterPendingItem
		score   uint64
	}
	candidates := make([]dispatchCandidate, 0, min(limit, len(pendingRegions)))
	for _, pending := range pendingRegions {
		region := c.cluster.GetRegion(pending.regionID)
		if region == nil {
			continue
		}
		candidate := dispatchCandidate{
			pending: pending,
			score:   region.GetCPUUsage(),
		}
		insertAt := len(candidates)
		for i, existing := range candidates {
			if candidate.score > existing.score || (candidate.score == existing.score && candidate.pending.regionID < existing.pending.regionID) {
				insertAt = i
				break
			}
		}
		if insertAt == len(candidates) && len(candidates) >= limit {
			continue
		}
		candidates = append(candidates, dispatchCandidate{})
		copy(candidates[insertAt+1:], candidates[insertAt:])
		candidates[insertAt] = candidate
		if len(candidates) > limit {
			candidates = candidates[:limit]
		}
	}
	regions := make([]splitScatterPendingItem, 0, len(candidates))
	for _, candidate := range candidates {
		regions = append(regions, candidate.pending)
	}
	return regions
}

func (c *Controller) removePendingSplitScatter(regionID uint64) {
	c.splitScatterPendingMu.Lock()
	defer c.splitScatterPendingMu.Unlock()
	c.splitScatterPending.Remove(regionID)
}

func makeSplitScatterGroup(sourceRegionID, firstNewRegionID uint64) string {
	return fmt.Sprintf("split-scatter-%d-%d", sourceRegionID, firstNewRegionID)
}

// RecordSplitScatterBatch records a newly split batch for later scatter.
func (c *Controller) RecordSplitScatterBatch(sourceRegionID uint64, newRegionIDs []uint64) {
	c.recordSplitScatterBatch(sourceRegionID, newRegionIDs)
}

// DispatchSplitScatterRegions dispatches pending split-scatter regions.
func (c *Controller) DispatchSplitScatterRegions() {
	for _, pending := range c.collectTopPendingSplitScatter(splitScatterDispatchLimit) {
		region := c.cluster.GetRegion(pending.regionID)
		if region == nil {
			continue
		}
		rangeHint := resolveSplitScatterRangeHint(region)
		if len(rangeHint.startKey) > 0 {
			c.regionScatterer.SeedGroupDistributionByRange(pending.group, rangeHint.startKey, rangeHint.endKey)
		}
		op, err := c.regionScatterer.ScatterInternal(region, pending.group)
		if err != nil {
			log.Info("dispatch internal split scatter failed",
				zap.Uint64("region-id", pending.regionID),
				zap.String("group", pending.group),
				zap.Error(err))
			continue
		}
		if op != nil {
			if c.opController.AddWaitingOperator(op) == 0 {
				log.Info("dispatch internal split scatter add operator failed",
					zap.Uint64("region-id", pending.regionID),
					zap.String("group", pending.group),
					zap.String("operator-desc", op.Desc()))
				continue
			}
			if c.opController.GetOperator(region.GetID()) != op {
				log.Info("dispatch internal split scatter operator lost before commit",
					zap.Uint64("region-id", pending.regionID),
					zap.String("group", pending.group),
					zap.String("operator-desc", op.Desc()))
				continue
			}
			c.regionScatterer.Commit(region, op, pending.group)
		}
		c.removePendingSplitScatter(pending.regionID)
	}
}
