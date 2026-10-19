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

// A worker yields between complete Region checks or raw candidates. A single
// placement/best-location call runs normally; this is no internal work budget.
const affinityBalanceTurn = 10 * time.Millisecond

type affinityBalancer struct {
	checker *AffinityChecker
	mu      sync.Mutex
	tables  map[string]*affinityBalanceTask
	queue   chan string
	// roundRobinCursor advances across the sorted table IDs so a full queue
	// cannot repeatedly reserve the same map prefix and starve later tables.
	roundRobinCursor int
}

type affinityBalanceTask struct {
	// Queue admission and completed results are protected by balancer.mu.
	queued     bool
	stable     bool
	version    affinity.BalanceVersion
	retryAfter time.Time
	retryDelay time.Duration
	// Only the worker reads or writes unfinished search progress.
	search *balanceSearch
}

type balanceSearchPhase uint8

const (
	balanceScanCurrent balanceSearchPhase = iota
	balanceFindLeader
	balanceFindVoter
	balanceVerifyFinal
)

type balanceSearch struct {
	snapshot   *affinity.BalanceSnapshot
	scorer     *affinity.BalanceScorer
	phase      balanceSearchPhase
	groupIndex int
	scan       *balanceRegionScan
	candidates balanceCandidates
	move       affinity.BalanceMove
	target     *affinity.Group
	blocked    bool
	checked    map[uint64]balanceCheckedRegion
}

type balanceCheckedRegion struct {
	group  *affinity.GroupState
	region *core.RegionInfo
	fit    *placement.RegionFit
}

func newAffinityBalancer(ctx context.Context, checker *AffinityChecker) *affinityBalancer {
	balancer := &affinityBalancer{checker: checker, tables: make(map[string]*affinityBalanceTask), queue: make(chan string, 64)}
	go balancer.run(ctx)
	return balancer
}

func (b *affinityBalancer) enqueue(groupID string) {
	version, exists := b.checker.affinityManager.GetBalanceVersion(groupID)
	if !exists {
		return
	}
	tableID := affinity.TableGroupPrefix(groupID)
	b.mu.Lock()
	defer b.mu.Unlock()
	task := b.tables[tableID]
	if task == nil {
		task = &affinityBalanceTask{}
		b.tables[tableID] = task
	}
	if task.queued || task.stable && task.version == version ||
		task.version == version && time.Now().Before(task.retryAfter) {
		return
	}
	select {
	case b.queue <- tableID:
		task.queued = true
	default:
	}
}

func (b *affinityBalancer) run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.mu.Lock()
			ids := make([]string, 0, len(b.tables))
			for id := range b.tables {
				ids = append(ids, id)
			}
			sort.Strings(ids)
			if len(ids) == 0 {
				b.mu.Unlock()
				continue
			}
			start := b.roundRobinCursor % len(ids)
			for offset := range ids {
				id := ids[(start+offset)%len(ids)]
				task := b.tables[id]
				if _, exists := b.checker.affinityManager.GetBalanceVersion(id); !exists && !task.queued {
					delete(b.tables, id)
					continue
				}
				current, exists := b.checker.affinityManager.GetBalanceVersion(id)
				versionChanged := exists && task.version != (affinity.BalanceVersion{}) && current != task.version
				if !task.queued && (versionChanged || !task.stable) && !time.Now().Before(task.retryAfter) {
					select {
					case b.queue <- id:
						task.queued = true
					default:
					}
				}
			}
			b.roundRobinCursor = (start + 1) % len(ids)
			b.mu.Unlock()
		case id := <-b.queue:
			b.mu.Lock()
			task := b.tables[id]
			b.mu.Unlock()
			if task == nil {
				continue
			}
			stable, resume, err := b.step(ctx, id, task)
			if err != nil && !errs.ErrEtcdTxnConflict.Equal(err) && ctx.Err() == nil {
				log.Warn("failed to balance affinity table", zap.String("table-id", id), zap.Error(err))
			}
			b.mu.Lock()
			task.queued, task.stable = false, stable && err == nil
			if task.search != nil {
				task.version = task.search.snapshot.Version
			}
			if !resume {
				task.search = nil
			}
			if resume && err == nil {
				task.retryDelay = 0
				select {
				case b.queue <- id:
					task.queued = true
				default:
					task.retryAfter = time.Now()
				}
			} else {
				task.retryDelay = min(max(time.Second, task.retryDelay*2), 30*time.Second)
				task.retryAfter = time.Now().Add(task.retryDelay)
			}
			b.mu.Unlock()
		}
	}
}

func (b *affinityBalancer) prepareSearch(id string, task *affinityBalanceTask) (*balanceSearch, error) {
	manager := b.checker.affinityManager
	version, exists := manager.GetBalanceVersion(id)
	if !exists {
		task.search = nil
		return nil, nil
	}
	search := task.search
	if search != nil && search.snapshot.Version == version {
		return search, nil
	}
	snapshot := manager.GetBalanceSnapshot(id)
	if snapshot == nil {
		return nil, nil
	}
	// A single-group table has no balance move to search. Avoid traversing the
	// cluster's label rules for this common no-planning path.
	if len(snapshot.Groups) <= 1 {
		search = &balanceSearch{snapshot: snapshot}
		task.search = search
		return search, nil
	}
	// Build the scorer before searching ranges. A table already at the
	// mathematical lower bound has no candidate to validate or commit, so its
	// stable result needs only the cheap local counts.
	groups := make([]*affinity.Group, 0, len(snapshot.Groups))
	for _, group := range snapshot.Groups {
		groups = append(groups, &group.Group)
	}
	scorer, err := affinity.NewBalanceScorer(groups)
	if err != nil {
		return nil, err
	}
	if balanceScoreAtTheoreticalLowerBound(snapshot, scorer) {
		search = &balanceSearch{snapshot: snapshot, scorer: scorer}
		task.search = search
		return search, nil
	}
	if !manager.BalanceSnapshotCurrent(snapshot) {
		return nil, errs.ErrEtcdTxnConflict.FastGenByArgs()
	}
	search = &balanceSearch{snapshot: snapshot, scorer: scorer, checked: make(map[uint64]balanceCheckedRegion)}
	search.candidates = newBalanceCandidates(snapshot, scorer, false)
	task.search = search
	return search, nil
}

// step searches for the first legal strict improvement, rather than constructing
// a legal action library or promising the best action in this turn.
func (b *affinityBalancer) step(ctx context.Context, id string, task *affinityBalanceTask) (stable, resume bool, err error) {
	c, manager := b.checker, b.checker.affinityManager
	if !c.balanceSchedulingAllowed() {
		return false, false, nil
	}
	search, err := b.prepareSearch(id, task)
	if err != nil || search == nil {
		return false, false, err
	}
	task.search = search
	snapshot := search.snapshot
	if search.scorer == nil || len(snapshot.Groups) <= 1 {
		return true, false, nil
	}
	// Once both dimensions already attain the mathematical minimum over every
	// candidate store (including zero-count stores), no strict improvement can
	// exist. Avoid scanning the table's Regions; the regular checker still owns
	// safety repairs and operator convergence.
	if balanceScoreAtTheoreticalLowerBound(snapshot, search.scorer) {
		return true, false, nil
	}
	deadline := time.Now().Add(affinityBalanceTurn)
	waiting := c.waitingAffinityRegions()
	for ctx.Err() == nil {
		if !manager.BalanceSnapshotCurrent(snapshot) {
			return false, false, errs.ErrEtcdTxnConflict.FastGenByArgs()
		}
		if time.Now().After(deadline) {
			return false, true, nil
		}
		switch search.phase {
		case balanceScanCurrent:
			if search.groupIndex >= len(snapshot.Groups) {
				search.phase = balanceFindLeader
				continue
			}
			group := snapshot.Groups[search.groupIndex]
			if group.BalancePolicy == affinity.BalancePolicyFixed {
				search.groupIndex++
				continue
			}
			if search.scan == nil {
				ranges := snapshot.Ranges[group.ID]
				if !group.AffinitySchedulingAllowed || len(ranges.KeyRanges) == 0 {
					return false, false, nil
				}
				scan := newBalanceRegionScan(ranges)
				search.scan = &scan
			}
			region, covered, done := search.scan.next(c.cluster)
			if !covered {
				return false, false, nil
			}
			if done {
				search.groupIndex++
				search.scan = nil
				continue
			}
			fit, status := c.validateBalanceCurrentRegion(group, region, waiting)
			if status != balanceAllowed {
				return false, false, nil
			}
			search.checked[region.GetID()] = balanceCheckedRegion{group, region, fit}
		case balanceFindLeader, balanceFindVoter:
			if search.target == nil {
				move, found := search.candidates.next()
				if !found {
					if search.blocked {
						return false, false, nil
					}
					if search.phase == balanceFindLeader {
						search.phase = balanceFindVoter
						search.candidates = newBalanceCandidates(snapshot, search.scorer, true)
						continue
					}
					return true, false, nil
				}
				if _, improves := search.scorer.Evaluate(move); !improves {
					continue
				}
				search.move = move
				group := snapshot.Groups[search.candidates.group]
				target := group.Group
				target.LeaderStoreID, target.VoterStoreIDs = move.LeaderStoreID, slices.Clone(group.VoterStoreIDs)
				if move.FromStoreID != 0 {
					target.VoterStoreIDs[slices.Index(target.VoterStoreIDs, move.FromStoreID)] = move.ToStoreID
				}
				search.target = &target
				scan := newBalanceRegionScan(snapshot.Ranges[group.ID])
				search.scan = &scan
			}
			group := snapshot.Groups[search.candidates.group]
			status := c.balanceTargetExecutionStatus(&group.Group, search.target)
			if status != balanceAllowed {
				search.blocked = search.blocked || status == balanceTemporarilyBlocked
				search.target, search.scan = nil, nil
				continue
			}
			region, covered, done := search.scan.next(c.cluster)
			if !covered {
				return false, false, nil
			}
			if done {
				if err := manager.PrepareBalanceCommit(ctx, snapshot); err != nil {
					return false, false, err
				}
				search.phase, search.scan = balanceVerifyFinal, nil
				continue
			}
			checked, ok := search.checked[region.GetID()]
			if !ok || !sameBalanceRegionInput(checked.region, region) || !c.balanceRegionReady(group, region, waiting) {
				return false, false, nil
			}
			status = c.validateBalanceTarget(search.target, region, checked.fit)
			if status != balanceAllowed {
				search.blocked = search.blocked || status == balanceTemporarilyBlocked
				search.target, search.scan = nil, nil
			}
		case balanceVerifyFinal:
			if !b.verifyFinalRegions(ctx, search) {
				return false, false, ctx.Err()
			}
			group := snapshot.Groups[search.candidates.group]
			if !c.balanceSchedulingAllowed() || c.balanceTargetExecutionStatus(&group.Group, search.target) != balanceAllowed {
				return false, false, nil
			}
			committed, err := manager.CommitBalanceTarget(ctx, snapshot, search.target)
			if committed {
				log.Debug("balanced affinity partition", zap.String("group-id", search.target.ID))
			}
			return false, false, err
		}
	}
	return false, false, ctx.Err()
}

func sameBalanceRegionInput(a, b *core.RegionInfo) bool {
	return b != nil && a.GetRegionEpoch().GetVersion() == b.GetRegionEpoch().GetVersion() &&
		a.GetRegionEpoch().GetConfVer() == b.GetRegionEpoch().GetConfVer() &&
		bytes.Equal(a.GetStartKey(), b.GetStartKey()) && bytes.Equal(a.GetEndKey(), b.GetEndKey()) &&
		a.GetLeader().GetStoreId() == b.GetLeader().GetStoreId() && core.SortedPeersEqual(a.GetPeers(), b.GetPeers())
}

// verifyFinalRegions takes one complete RegionTree range view. It never saves
// partial success across turns: cancellation aborts the check, and a later
// attempt must collect the whole view again. This lightweight pass may exceed
// the turn duration; placement and etcd IO run outside the RegionTree lock.
func (b *affinityBalancer) verifyFinalRegions(ctx context.Context, search *balanceSearch) bool {
	var ranges []keyutil.KeyRange
	for _, group := range search.snapshot.Groups {
		if group.BalancePolicy != affinity.BalancePolicyFixed {
			ranges = append(ranges, search.snapshot.Ranges[group.ID].KeyRanges...)
		}
	}
	slices.SortFunc(ranges, func(a, b keyutil.KeyRange) int { return bytes.Compare(a.StartKey, b.StartKey) })
	regions, err := b.checker.cluster.BatchScanRegions(keyutil.NewKeyRanges(ranges), core.WithOutputMustContainAllKeyRange())
	if err != nil || len(regions) != len(search.checked) {
		return false
	}
	waiting := b.checker.waitingAffinityRegions()
	for _, region := range regions {
		checked, ok := search.checked[region.GetID()]
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
