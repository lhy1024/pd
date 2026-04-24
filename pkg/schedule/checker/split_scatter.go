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

	"go.uber.org/zap"

	"github.com/pingcap/log"

	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/scatter"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const (
	splitScatterDispatchLimit = 4
)

type splitScatterPendingItem struct {
	group string
}

// splitScatterRangeHint is a derived key range for the current table/index
// group. When available, split-scatter seeds the scatterer's group
// distribution with the existing region count in this range before dispatch.
type splitScatterRangeHint struct {
	startKey []byte
	endKey   []byte
}

type splitScatterManager struct {
	mu struct {
		syncutil.RWMutex
		pending map[uint64]*splitScatterPendingItem
	}
}

type splitScatterController struct {
	cluster         sche.CheckerCluster
	pending         *splitScatterManager
	regionScatterer *scatter.RegionScatterer
}

func newSplitScatterManager() *splitScatterManager {
	m := &splitScatterManager{}
	m.mu.pending = make(map[uint64]*splitScatterPendingItem)
	return m
}

func newSplitScatterController(
	ctx context.Context,
	cluster sche.CheckerCluster,
	opController *operator.Controller,
	addPendingProcessedRegions func(bool, ...uint64),
) *splitScatterController {
	return &splitScatterController{
		cluster:         cluster,
		pending:         newSplitScatterManager(),
		regionScatterer: scatter.NewRegionScatterer(ctx, cluster, opController, addPendingProcessedRegions),
	}
}

func (m *splitScatterManager) recordBatch(sourceRegionID uint64, newRegionIDs []uint64) {
	if len(newRegionIDs) == 0 {
		return
	}
	group := makeSplitScatterGroup(sourceRegionID, newRegionIDs[0])
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, regionID := range newRegionIDs {
		m.mu.pending[regionID] = &splitScatterPendingItem{group: group}
	}
	m.mu.pending[sourceRegionID] = &splitScatterPendingItem{group: group}
}

func (c *splitScatterController) collectTopPending(limit int) []uint64 {
	if limit <= 0 {
		return nil
	}
	pendingIDs := c.pending.pendingIDs()
	type dispatchCandidate struct {
		regionID uint64
		score    uint64
	}
	candidates := make([]dispatchCandidate, 0, min(limit, len(pendingIDs)))
	for _, regionID := range pendingIDs {
		region := c.cluster.GetRegion(regionID)
		if region == nil {
			c.pending.remove(regionID)
			continue
		}
		candidate := dispatchCandidate{
			regionID: regionID,
			score:    region.GetCPUUsage(),
		}
		insertAt := len(candidates)
		for i, existing := range candidates {
			if candidate.score > existing.score || (candidate.score == existing.score && candidate.regionID < existing.regionID) {
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
	regionIDs := make([]uint64, 0, len(candidates))
	for _, candidate := range candidates {
		regionIDs = append(regionIDs, candidate.regionID)
	}
	return regionIDs
}

func (m *splitScatterManager) remove(regionID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.mu.pending, regionID)
}

func (m *splitScatterManager) getPendingItemLocked(regionID uint64) (*splitScatterPendingItem, bool) {
	item, ok := m.mu.pending[regionID]
	if !ok || item == nil {
		return nil, false
	}
	return item, true
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

func (m *splitScatterManager) pendingIDs() []uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ids := make([]uint64, 0, len(m.mu.pending))
	for regionID := range m.mu.pending {
		ids = append(ids, regionID)
	}
	return ids
}

func makeSplitScatterGroup(sourceRegionID, firstNewRegionID uint64) string {
	return fmt.Sprintf("split-scatter-%d-%d", sourceRegionID, firstNewRegionID)
}

// RecordSplitScatterBatch records a newly split batch for later scatter.
func (c *Controller) RecordSplitScatterBatch(sourceRegionID uint64, newRegionIDs []uint64) {
	c.splitScatter.pending.recordBatch(sourceRegionID, newRegionIDs)
}

// DispatchSplitScatterRegionsForTest dispatches pending split-scatter regions.
// The function is exposed for test purpose.
func (c *Controller) DispatchSplitScatterRegionsForTest() {
	c.dispatchSplitScatterRegions()
}

func (c *Controller) dispatchSplitScatterRegions() {
	for _, regionID := range c.splitScatter.collectTopPending(splitScatterDispatchLimit) {
		group, ok := c.splitScatter.pending.getPendingGroup(regionID)
		if !ok {
			continue
		}
		region := c.cluster.GetRegion(regionID)
		if region == nil {
			c.splitScatter.pending.remove(regionID)
			continue
		}
		rangeHint := resolveSplitScatterRangeHint(region)
		if len(rangeHint.startKey) > 0 {
			c.splitScatter.regionScatterer.SeedGroupDistributionByRange(group, rangeHint.startKey, rangeHint.endKey)
		}
		op, err := c.splitScatter.regionScatterer.ScatterInternal(region, group)
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
			c.splitScatter.regionScatterer.Commit(region, op, group)
		}
		c.splitScatter.pending.remove(regionID)
	}
}
