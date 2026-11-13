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
	"encoding/hex"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/affinity/balanceplan"
	"github.com/tikv/pd/pkg/schedule/placement"
)

func TestAffinityBalanceSameRoleIsolation(t *testing.T) {
	re := require.New(t)
	f := newAffinityBalanceTest(t, 3, "a", "a", "b")
	for id, zone := range []string{"a", "a", "b"} {
		f.SetStoreLabel(uint64(id+1), map[string]string{"zone": zone, "id": strconv.Itoa(id + 1)})
	}
	rule := f.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
	rule.Count = 1
	rule.LabelConstraints = []placement.LabelConstraint{{Key: "id", Op: placement.In, Values: []string{"1"}}}
	re.NoError(f.ruleManager.SetRule(rule))
	second := rule.Clone()
	second.ID, second.Index = "second", 1
	second.LabelConstraints[0].Values = []string{"2", "3"}
	re.NoError(f.ruleManager.SetRule(second))
	for i := range 2 {
		f.group(balanceTestGroupID(fmt.Sprintf("_tidb_pt_100_p%d", i+1)), uint64(i+1), 1, []uint64{1, 3}, fmt.Sprintf("k%d", i), fmt.Sprintf("k%d", i+1))
	}
	snapshot := f.snapshot()
	plan := f.balancePlanInput(snapshot, nil, nil)
	for i := range plan.input.Groups {
		plan.input.Groups[i].CheckTarget = nil
	}
	result := balanceplan.Plan(&plan.input, balanceplan.Options{Seed: snapshot.Version})
	re.Equal(int64(8), result.Baseline.Voter)
	re.Equal(int64(6), result.Score.Voter)
	for _, target := range plan.changedTargets(&result) {
		status, _, _, _ := f.validateBalancePlanTarget(t.Context(), snapshot, snapshot.Group(target.ID), target)
		t.Logf("raw target: group=%s voters=%v leader=%d status=%d", target.ID, target.VoterStoreIDs, target.LeaderStoreID, status)
	}
	region := f.GetRegion(1)
	currentFit, status := f.validateBalanceCurrentRegion(snapshot.Groups[0], region)
	re.Equal(balanceAllowed, status)
	virtual := cloneRegionWithReplacePeerStores(region, 1, 1, 2)
	fit := f.ruleManager.FitRegionWithoutCache(f.Cluster, virtual)
	re.True(fit.IsSatisfied())
	re.True(placementNotWorse(currentFit, fit))
	re.Equal(bestLocationRejected, f.regionPlacementBestLocation(virtual, fit))
	strategy := f.strategy(virtual, second, false)
	old, next, temporary := strategy.getBetterLocation(f.Cluster, virtual, fit, fit.RuleFits[1])
	re.Equal(uint64(2), old)
	re.Equal(uint64(3), next)
	re.False(temporary)
	res, err := f.planBalanceTargets(t.Context(), snapshot)
	re.NoError(err)
	t.Logf("raw score=%v -> %v changed=%d; validated score=%v targets=%d rounds=%d groupCounts=%v retry=%v", result.Baseline, result.Score, result.Changed, res.planner.Score, len(res.targets), res.rounds, res.groupCounts, res.retry)
	re.Equal(balanceplan.Score{Voter: 8, Leader: 2}, res.planner.Score)
	re.Len(res.targets, 1)
	re.Equal(2, res.rounds)
	re.Equal(2, res.groupCounts[balanceGroupMutable])
	re.True(f.step())
	re.Equal([2]int{8, 2}, balanceTableScore(f.affinityManager, balanceTestTable()))
	for _, group := range f.snapshot().Groups {
		re.Equal([]uint64{1, 3}, group.VoterStoreIDs)
	}
	f.settle()
	re.False(f.step())
	// A new store in zone b is usable even though store 2 is cheaper to try.
	f.AddRegionStore(4, 10)
	f.SetStoreLabel(4, map[string]string{"zone": "b", "id": "4"})
	second.LabelConstraints[0].Values = []string{"2", "3", "4"}
	re.NoError(f.ruleManager.SetRule(second))
	// Simply pinning each rejected relaxed choice makes no progress on this
	// four-store expansion, although moving a voter onto store 4 is legal.
	snapshot = f.snapshot()
	pinned := make(map[string]balancePlanGroupKind)
	for range 2 {
		plan := f.balancePlanInput(snapshot, nil, pinned)
		raw := balanceplan.Plan(&plan.input, balanceplan.Options{Seed: snapshot.Version})
		re.True(raw.Score.Less(raw.Baseline))
		for _, target := range plan.changedTargets(&raw) {
			status, _, reason, _ := f.validateBalancePlanTarget(t.Context(), snapshot, snapshot.Group(target.ID), target)
			re.Equal(balanceRejected, status)
			re.Equal(balanceTargetLocation, reason)
			pinned[target.ID] = balanceGroupUnsupported
		}
	}
	pinnedPlan := f.balancePlanInput(snapshot, nil, pinned)
	unchanged := balanceplan.Plan(&pinnedPlan.input, balanceplan.Options{Seed: snapshot.Version})
	re.Equal(unchanged.Baseline, unchanged.Score)
	re.True(f.step())
	re.Equal([2]int{6, 2}, balanceTableScore(f.affinityManager, balanceTestTable()))
	for _, group := range f.snapshot().Groups {
		re.NotContains(group.VoterStoreIDs, uint64(2))
	}
}

func TestAffinityBalanceLaterRegionPlacement(t *testing.T) {
	f := newAffinityBalanceTest(t, 5, "a", "a", "b", "c", "b")
	for id := uint64(1); id <= 5; id++ {
		f.SetStoreLabel(id, map[string]string{"zone": []string{"a", "a", "b", "c", "b"}[id-1], "id": strconv.Itoa(int(id))})
	}
	f.UpdateRegionCount(3, 100)
	rule := f.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
	rule.Count = 2
	rule.LabelConstraints = []placement.LabelConstraint{{Key: "id", Op: placement.In, Values: []string{"1", "2", "3", "4", "5"}}}
	require.NoError(t, f.ruleManager.SetRule(rule))
	require.NoError(t, f.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: "learner", Role: placement.Learner, Count: 1,
		StartKeyHex: hex.EncodeToString([]byte(balanceTestKey("b"))), EndKeyHex: hex.EncodeToString([]byte(balanceTestKey("c"))), LabelConstraints: []placement.LabelConstraint{{Key: "id", Op: placement.In, Values: []string{"5"}}}}))
	f.group(balanceTestGroupID("_tidb_pt_100_p1"), 1, 1, []uint64{1, 4}, "a", "b", "c")
	f.group(balanceTestGroupID("_tidb_pt_100_p2"), 3, 1, []uint64{1, 4}, "d", "e")
	f.policy(affinity.BalancePolicyFixed, balanceTestGroupID("_tidb_pt_100_p2"))
	later := f.GetRegion(2)
	f.PutRegion(later.Clone(core.WithAddPeer(&metapb.Peer{Id: 100, StoreId: 5, Role: metapb.PeerRole_Learner})))
	snapshot := f.snapshot()
	rawPlan := f.balancePlanInput(snapshot, nil, nil)
	raw := balanceplan.Plan(&rawPlan.input, balanceplan.Options{Seed: snapshot.Version})
	t.Logf("later probe raw=%v voters=%v targets=%v", raw.Score, raw.Voters, rawPlan.changedTargets(&raw))
	for _, target := range rawPlan.changedTargets(&raw) {
		for _, id := range []uint64{1, 2} {
			region := f.GetRegion(id)
			fit := f.ruleManager.FitRegionWithoutCache(f.Cluster, region)
			status, reason := f.validateBalanceTarget(target, region, fit)
			if id == 1 {
				require.Equal(t, balanceAllowed, status)
			} else {
				require.Equal(t, balanceRejected, status)
				require.Equal(t, balanceTargetLocation, reason)
			}
		}
	}
	// A heartbeat after the snapshot makes the first Region temporarily
	// unready. A later Region still rejects the relaxed target's location.
	first := f.GetRegion(1)
	extra := &metapb.Peer{Id: 200, StoreId: 3}
	f.PutRegion(first.Clone(core.WithAddPeer(extra), core.WithPendingPeers([]*metapb.Peer{extra})))
	blocked, err := f.planBalanceTargets(t.Context(), snapshot)
	require.NoError(t, err)
	t.Logf("mixed-state: targets=%d rounds=%d retry=%v temporary=%v limited=%v stop=%s", len(blocked.targets), blocked.rounds, blocked.retry, blocked.planner.TemporarilyBlocked, blocked.planner.PlacementLimited, blocked.planner.Stop)
	require.Empty(t, blocked.targets)
	require.Equal(t, 2, blocked.rounds)
	require.False(t, blocked.planner.TemporarilyBlocked)
	require.False(t, blocked.planner.PlacementLimited)
	require.Equal(t, balanceplan.StopGreedy, blocked.planner.Stop)
	require.True(t, blocked.retry)
	target := rawPlan.changedTargets(&raw)[0]
	status, _, reason, retry := f.validateBalancePlanTarget(t.Context(), snapshot, snapshot.Group(target.ID), target)
	require.Equal(t, balanceRejected, status)
	require.Equal(t, balanceTargetLocation, reason)
	require.True(t, retry)
	f.PutRegion(first)
	res, err := f.planBalanceTargets(t.Context(), snapshot)
	require.NoError(t, err)
	require.NotEmpty(t, res.targets)
	require.Greater(t, res.rounds, 1)
	for _, target := range res.targets {
		status, _, _, _ := f.validateBalancePlanTarget(t.Context(), snapshot, snapshot.Group(target.ID), target)
		require.Equal(t, balanceAllowed, status)
	}
	t.Logf("later Region: raw=%v result=%v targets=%d rounds=%d retry=%v", raw.Score, res.planner.Score, len(res.targets), res.rounds, res.retry)
}
