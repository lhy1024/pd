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
	"bytes"
	"cmp"
	"context"
	"slices"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

type affinityBalancer struct {
	checker *AffinityChecker
	mu      sync.Mutex
	tables  map[string]*affinityBalanceTask
}

type affinityBalanceTask struct {
	// Dirty admission is protected by balancer.mu. Completed searches are
	// retried by the periodic ticker; no permanent stable cache is retained.
	dirty bool
}

type balanceCheckedRegion struct {
	group  *affinity.GroupState
	region *core.RegionInfo
	fit    *placement.RegionFit
}

func newAffinityBalancer(ctx context.Context, checker *AffinityChecker) *affinityBalancer {
	balancer := &affinityBalancer{checker: checker, tables: make(map[string]*affinityBalanceTask)}
	go balancer.run(ctx)
	return balancer
}

func (b *affinityBalancer) enqueue(groupID string) {
	_, exists := b.checker.affinityManager.GetBalanceVersion(groupID)
	if !exists {
		return
	}
	tableID := affinity.TableGroupPrefix(groupID)
	b.mu.Lock()
	task := b.tables[tableID]
	if task == nil {
		task = &affinityBalanceTask{}
		b.tables[tableID] = task
	}
	task.dirty = true
	b.mu.Unlock()
}

func (b *affinityBalancer) run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.markAllDirty()
			b.processDirty(ctx)
		}
	}
}

func (b *affinityBalancer) markAllDirty() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for id, task := range b.tables {
		if _, exists := b.checker.affinityManager.GetBalanceVersion(id); !exists {
			delete(b.tables, id)
			continue
		}
		task.dirty = true
	}
}

func (b *affinityBalancer) processDirty(ctx context.Context) {
	b.mu.Lock()
	ids := make([]string, 0, len(b.tables))
	for id, task := range b.tables {
		if task.dirty {
			ids = append(ids, id)
			task.dirty = false
		}
	}
	b.mu.Unlock()
	if len(ids) == 0 || ctx.Err() != nil {
		return
	}
	sort.Strings(ids)
	// Global counts only order candidate stores. Capture them once for this
	// batch so a stable set of tables does not rescan every group per table.
	counts := b.checker.affinityManager.GetBalanceGlobalCounts()
	for _, id := range ids {
		err := b.stepWithCounts(ctx, id, counts)
		if err != nil && !errs.ErrEtcdTxnConflict.Equal(err) && ctx.Err() == nil {
			log.Warn("failed to balance affinity table", zap.String("table-id", id), zap.Error(err))
		}
	}
}

// step completes one table search, including final review and one commit
// attempt. The dirty-table worker retries with a fresh snapshot; no search progress is
// retained between calls. Large tables occupy the worker until this call ends.
func (b *affinityBalancer) step(ctx context.Context, id string) error {
	return b.stepWithCounts(ctx, id, b.checker.affinityManager.GetBalanceGlobalCounts())
}

func (b *affinityBalancer) stepWithCounts(ctx context.Context, id string, counts affinity.BalanceGlobalCounts) error {
	c, manager := b.checker, b.checker.affinityManager
	if !c.balanceSchedulingAllowed() {
		return nil
	}
	snapshot := manager.GetBalanceSnapshotWithCounts(id, counts)
	if snapshot == nil {
		return nil
	}
	if len(snapshot.Groups) <= 1 {
		return nil
	}
	groups := make([]*affinity.Group, 0, len(snapshot.Groups))
	for _, group := range snapshot.Groups {
		groups = append(groups, &group.Group)
	}
	scorer, err := affinity.NewBalanceScorer(groups)
	if err != nil {
		return err
	}
	// Once both dimensions attain the mathematical minimum over every candidate
	// store, no strict improvement exists. The regular checker still owns safety
	// repairs and operator convergence, so no Region scan is needed here.
	if balanceScoreAtTheoreticalLowerBound(snapshot, scorer) {
		return nil
	}
	checkInput := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !manager.BalanceSnapshotCurrent(snapshot) {
			return errs.ErrEtcdTxnConflict.FastGenByArgs()
		}
		return nil
	}
	waiting := c.waitingAffinityRegions()
	checked := make(map[uint64]balanceCheckedRegion)
	for _, group := range snapshot.Groups {
		if group.BalancePolicy == affinity.BalancePolicyFixed {
			continue
		}
		ranges := snapshot.Ranges[group.ID]
		if !group.AffinitySchedulingAllowed || len(ranges.KeyRanges) == 0 {
			return nil
		}
		scan := newBalanceRegionScan(ranges)
		for {
			if err := checkInput(); err != nil {
				return err
			}
			region, covered, done := scan.next(c.cluster)
			if !covered {
				return nil
			}
			if done {
				break
			}
			fit, status := c.validateBalanceCurrentRegion(group, region, waiting)
			if status != balanceAllowed {
				return nil
			}
			checked[region.GetID()] = balanceCheckedRegion{group, region, fit}
		}
	}
	// Exhaust leader-only improvements before trying voter replacements.
	for _, replace := range []bool{false, true} {
		candidates := newBalanceCandidates(snapshot, scorer, replace)
		blocked := false
		for {
			if err := checkInput(); err != nil {
				return err
			}
			move, found := candidates.next()
			if !found {
				break
			}
			if _, improves := scorer.Evaluate(move); !improves {
				continue
			}
			group := snapshot.Groups[candidates.group]
			target := group.Group
			target.LeaderStoreID, target.VoterStoreIDs = move.LeaderStoreID, slices.Clone(group.VoterStoreIDs)
			if move.FromStoreID != 0 {
				target.VoterStoreIDs[slices.Index(target.VoterStoreIDs, move.FromStoreID)] = move.ToStoreID
			}
			scan := newBalanceRegionScan(snapshot.Ranges[group.ID])
			status := balanceAllowed
			for {
				if err := checkInput(); err != nil {
					return err
				}
				status = c.balanceTargetExecutionStatus(&group.Group, &target)
				if status != balanceAllowed {
					break
				}
				region, covered, done := scan.next(c.cluster)
				if !covered {
					return nil
				}
				if done {
					break
				}
				current, ok := checked[region.GetID()]
				if !ok || !sameBalanceRegionInput(current.region, region) || !c.balanceRegionReady(group, region, waiting) {
					return nil
				}
				status = c.validateBalanceTarget(&target, region, current.fit)
				if status != balanceAllowed {
					break
				}
			}
			if status != balanceAllowed {
				blocked = blocked || status == balanceTemporarilyBlocked
				continue
			}
			if err := manager.PrepareBalanceCommit(ctx, snapshot); err != nil {
				return err
			}
			if !b.verifyFinalRegions(ctx, snapshot, checked) {
				return ctx.Err()
			}
			if !c.balanceSchedulingAllowed() || c.balanceTargetExecutionStatus(&group.Group, &target) != balanceAllowed {
				return nil
			}
			committed, err := manager.CommitBalanceTarget(ctx, snapshot, &target)
			if committed {
				log.Debug("balanced affinity partition", zap.String("group-id", target.ID))
			}
			return err
		}
		// A temporary leader restriction must not trigger a voter replacement.
		if blocked {
			return nil
		}
	}
	return nil
}

func sameBalanceRegionInput(a, b *core.RegionInfo) bool {
	return b != nil && a.GetRegionEpoch().GetVersion() == b.GetRegionEpoch().GetVersion() &&
		a.GetRegionEpoch().GetConfVer() == b.GetRegionEpoch().GetConfVer() &&
		bytes.Equal(a.GetStartKey(), b.GetStartKey()) && bytes.Equal(a.GetEndKey(), b.GetEndKey()) &&
		a.GetLeader().GetStoreId() == b.GetLeader().GetStoreId() && core.SortedPeersEqual(a.GetPeers(), b.GetPeers())
}

// verifyFinalRegions takes one complete RegionTree range view. Cancellation
// aborts the check; placement and etcd IO run outside the RegionTree lock.
func (b *affinityBalancer) verifyFinalRegions(ctx context.Context, snapshot *affinity.BalanceSnapshot, checkedRegions map[uint64]balanceCheckedRegion) bool {
	var ranges []keyutil.KeyRange
	for _, group := range snapshot.Groups {
		if group.BalancePolicy != affinity.BalancePolicyFixed {
			ranges = append(ranges, snapshot.Ranges[group.ID].KeyRanges...)
		}
	}
	slices.SortFunc(ranges, func(a, b keyutil.KeyRange) int { return bytes.Compare(a.StartKey, b.StartKey) })
	regions, err := b.checker.cluster.BatchScanRegions(keyutil.NewKeyRanges(ranges), core.WithOutputMustContainAllKeyRange())
	if err != nil || len(regions) != len(checkedRegions) {
		return false
	}
	waiting := b.checker.waitingAffinityRegions()
	for _, region := range regions {
		checked, ok := checkedRegions[region.GetID()]
		if ctx.Err() != nil || !ok || !sameBalanceRegionInput(checked.region, region) ||
			!b.checker.balanceRegionReady(checked.group, region, waiting) {
			return false
		}
	}
	return true
}

// A concrete cursor enumerates raw single moves without retaining actions.
// Scoring occurs before any Region scan; the active group's small leader slice
// puts the original leader first and uses global counts to avoid an ID bias.
type balanceCandidates struct {
	snapshot                *affinity.BalanceSnapshot
	scorer                  *affinity.BalanceScorer
	replace                 bool
	group, from, to, leader int
	voters, leaders, stores []uint64
}

func balanceScoreAtTheoreticalLowerBound(snapshot *affinity.BalanceSnapshot, scorer *affinity.BalanceScorer) bool {
	stores := make(map[uint64]struct{}, len(snapshot.Stores))
	for _, storeID := range snapshot.Stores {
		stores[storeID] = struct{}{}
	}
	// Fixed or degraded groups still contribute to the scorer. If one of their
	// stores is outside the eligible candidate set, a lower bound computed over
	// only the eligible stores cannot prove that an auto move is impossible.
	for _, group := range snapshot.Groups {
		if group.LeaderStoreID != 0 {
			if _, ok := stores[group.LeaderStoreID]; !ok {
				return false
			}
		}
		for _, storeID := range group.VoterStoreIDs {
			if _, ok := stores[storeID]; !ok {
				return false
			}
		}
	}
	storeCount := len(stores)
	if storeCount == 0 {
		return false
	}
	var leaders, voters int
	for _, group := range snapshot.Groups {
		if group.LeaderStoreID != 0 {
			leaders++
		}
		voters += len(group.VoterStoreIDs)
	}
	minimum := func(total int) int {
		q, r := total/storeCount, total%storeCount
		return (storeCount-r)*q*q + r*(q+1)*(q+1)
	}
	score := scorer.Score()
	return score.Leader == minimum(leaders) && score.Voter == minimum(voters)
}

func newBalanceCandidates(snapshot *affinity.BalanceSnapshot, scorer *affinity.BalanceScorer, replace bool) balanceCandidates {
	var stores []uint64
	if replace {
		stores = sortedBalanceCandidateStores(snapshot, scorer)
	}
	return balanceCandidates{snapshot: snapshot, scorer: scorer, replace: replace, stores: stores}
}

func sortedBalanceCandidateStores(snapshot *affinity.BalanceSnapshot, scorer *affinity.BalanceScorer) []uint64 {
	stores := slices.Clone(snapshot.Stores)
	slices.SortFunc(stores, func(a, b uint64) int {
		if order := cmp.Compare(scorer.VoterCount(a), scorer.VoterCount(b)); order != 0 {
			return order
		}
		if order := cmp.Compare(snapshot.GlobalVoterCounts[a], snapshot.GlobalVoterCounts[b]); order != 0 {
			return order
		}
		return cmp.Compare(a, b)
	})
	return stores
}

func (p *balanceCandidates) next() (affinity.BalanceMove, bool) {
	for p.group < len(p.snapshot.Groups) {
		group := p.snapshot.Groups[p.group]
		if group.BalancePolicy == affinity.BalancePolicyFixed || len(group.VoterStoreIDs) == 0 {
			p.group++
			continue
		}
		if p.voters == nil {
			p.voters = slices.Clone(group.VoterStoreIDs)
			slices.Sort(p.voters)
			p.leaders = slices.Clone(p.voters)
			slices.SortFunc(p.leaders, func(a, b uint64) int {
				if a == b {
					return 0
				}
				if a == group.LeaderStoreID {
					return -1
				}
				if b == group.LeaderStoreID {
					return 1
				}
				if order := cmp.Compare(p.snapshot.GlobalLeaderCounts[a], p.snapshot.GlobalLeaderCounts[b]); order != 0 {
					return order
				}
				return cmp.Compare(a, b)
			})
		}
		if !p.replace && p.leader < len(p.leaders) {
			move := affinity.BalanceMove{GroupID: group.ID, LeaderStoreID: p.leaders[p.leader]}
			p.leader++
			return move, true
		}
		if p.replace && p.from < len(p.voters) && len(p.stores) > 0 {
			if slices.Contains(p.voters, p.stores[p.to]) {
				p.leader = 0
				p.to++
				if p.to == len(p.stores) {
					p.to = 0
					p.from++
				}
				continue
			}
			var leader uint64
			if p.leader < len(p.leaders) {
				leader = p.leaders[p.leader]
			} else {
				leader = p.stores[p.to]
			}
			move := affinity.BalanceMove{GroupID: group.ID, LeaderStoreID: leader, FromStoreID: p.voters[p.from], ToStoreID: p.stores[p.to]}
			p.leader++
			if p.leader > len(p.leaders) {
				p.leader = 0
				p.to++
				if p.to == len(p.stores) {
					p.to = 0
					p.from++
				}
			}
			return move, true
		}
		p.group++
		p.from, p.to, p.leader = 0, 0, 0
		p.voters, p.leaders = nil, nil
	}
	return affinity.BalanceMove{}, false
}
