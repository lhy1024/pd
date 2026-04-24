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
	"sort"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/schedule/filter"
)

const (
	splitScatterPendingTTL      = 3 * time.Minute
	splitScatterQueueGCInterval = time.Minute
	splitScatterDispatchLimit   = 4
	splitScatterRetryBackoff    = time.Second
)

type splitScatterPendingItem struct {
	regionID    uint64
	group       string
	retryAt     time.Time
}

type splitScatterDispatchCandidate struct {
	pending splitScatterPendingItem
	score   uint64
}

// splitScatterRangeHint is a derived key range for the current table/index
// group. When available, split-scatter seeds the scatterer's group
// distribution with the existing region count in this range before dispatch.
type splitScatterRangeHint struct {
	startKey []byte
	endKey   []byte
}

func (c *Controller) collectTopPendingSplitScatter(limit int) []splitScatterPendingItem {
	if limit <= 0 {
		return nil
	}
	now := time.Now()
	c.splitScatterPendingMu.RLock()
	pendingIDs := c.splitScatterPending.GetAllID()
	pendingRegions := make([]splitScatterPendingItem, 0, len(pendingIDs))
	for _, regionID := range pendingIDs {
		value, ok := c.splitScatterPending.Get(regionID)
		if !ok {
			continue
		}
		pending, ok := value.(splitScatterPendingItem)
		if !ok {
			continue
		}
		pending.regionID = regionID
		pendingRegions = append(pendingRegions, pending)
	}
	c.splitScatterPendingMu.RUnlock()

	candidates := make([]splitScatterDispatchCandidate, 0, len(pendingRegions))
	for _, pending := range pendingRegions {
		region := c.cluster.GetRegion(pending.regionID)
		if region == nil {
			continue
		}
		if !pending.retryAt.IsZero() && now.Before(pending.retryAt) {
			continue
		}
		candidates = append(candidates, splitScatterDispatchCandidate{
			pending: pending,
			score:   region.GetCPUUsage(),
		})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].score == candidates[j].score {
			return candidates[i].pending.regionID < candidates[j].pending.regionID
		}
		return candidates[i].score > candidates[j].score
	})
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	regions := make([]splitScatterPendingItem, 0, len(candidates))
	for _, candidate := range candidates {
		regions = append(regions, candidate.pending)
	}
	return regions
}

func (c *Controller) delayPendingSplitScatter(regionID uint64, delay time.Duration) {
	c.splitScatterPendingMu.Lock()
	defer c.splitScatterPendingMu.Unlock()
	value, ok := c.splitScatterPending.Get(regionID)
	if !ok {
		return
	}
	pending, ok := value.(splitScatterPendingItem)
	if !ok {
		return
	}
	pending.retryAt = time.Now().Add(delay)
	c.splitScatterPending.Put(regionID, pending)
}

func makeSplitScatterGroup(sourceRegionID, firstNewRegionID uint64) string {
	return fmt.Sprintf("split-scatter-%d-%d", sourceRegionID, firstNewRegionID)
}

// RecordSplitScatterBatch records a newly split batch for later scatter.
func (c *Controller) RecordSplitScatterBatch(sourceRegionID uint64, newRegionIDs []uint64) {
	if len(newRegionIDs) == 0 {
		return
	}
	group := makeSplitScatterGroup(sourceRegionID, newRegionIDs[0])
	c.splitScatterPendingMu.Lock()
	defer c.splitScatterPendingMu.Unlock()
	for _, regionID := range newRegionIDs {
		c.splitScatterPending.Put(regionID, splitScatterPendingItem{group: group})
	}
}

// DispatchSplitScatterRegions dispatches pending split-scatter regions.
func (c *Controller) DispatchSplitScatterRegions() {
	for _, pending := range c.collectTopPendingSplitScatter(splitScatterDispatchLimit) {
		region := c.cluster.GetRegion(pending.regionID)
		if region == nil {
			continue
		}
		if !filter.IsRegionReplicated(c.cluster, region) {
			c.delayPendingSplitScatter(pending.regionID, splitScatterRetryBackoff)
			log.Info("dispatch internal split scatter delayed",
				zap.Uint64("region-id", pending.regionID),
				zap.String("group", pending.group),
				zap.String("reason", "not-fully-replicated"))
			continue
		}
		rangeHint := resolveSplitScatterRangeHint(region)
		log.Info("dispatch internal split scatter",
			zap.Uint64("region-id", pending.regionID),
			zap.String("group", pending.group),
			zap.Bool("range-hint-valid", len(rangeHint.startKey) > 0),
			zap.Binary("range-hint-start-key", rangeHint.startKey),
			zap.Binary("range-hint-end-key", rangeHint.endKey),
			zap.Uint64("cpu-score", region.GetCPUUsage()),
			zap.Binary("region-start-key", region.GetStartKey()),
			zap.Binary("region-end-key", region.GetEndKey()))
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
			if c.opController.ExceedStoreLimit(op) {
				c.delayPendingSplitScatter(pending.regionID, splitScatterRetryBackoff)
				log.Info("dispatch internal split scatter delayed",
					zap.Uint64("region-id", pending.regionID),
					zap.String("group", pending.group),
					zap.String("reason", "exceed-store-limit"),
					zap.String("operator-desc", op.Desc()))
				continue
			}
			if !c.opController.AddOperator(op) {
				log.Info("dispatch internal split scatter add operator failed",
					zap.Uint64("region-id", pending.regionID),
					zap.String("group", pending.group),
					zap.String("operator-desc", op.Desc()))
				continue
			}
			c.regionScatterer.Commit(region, op, pending.group)
		}
		c.splitScatterPendingMu.Lock()
		c.splitScatterPending.Remove(pending.regionID)
		c.splitScatterPendingMu.Unlock()
	}
}
