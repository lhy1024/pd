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
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule/affinity/balanceplan"
	"github.com/tikv/pd/pkg/schedule/placement"
)

func TestAffinityFeedbackRejectionDominatesTemporary(t *testing.T) {
	tc := mockcluster.NewCluster(t.Context(), newAffinityTestOptions())
	c := newTestAffinityChecker(t.Context(), tc, tc.PersistOptions)
	for i, z := range []string{"a", "a", "b", "b"} {
		tc.AddRegionStore(uint64(i+1), 10+i)
		tc.SetStoreLabel(uint64(i+1), map[string]string{"zone": z, "id": strconv.Itoa(i + 1)})
	}
	for i, ids := range [][]string{{"1", "3"}, {"2", "4"}} {
		require.NoError(t, c.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: strconv.Itoa(i), Index: i + 1, Override: i == 0, Role: placement.Voter, Count: 1, LocationLabels: []string{"zone"}, LabelConstraints: []placement.LabelConstraint{{Key: "id", Op: placement.In, Values: ids}}}))
	}
	tc.AddLeaderRegion(1, 1, 2)
	region := tc.GetRegion(1)
	tc.SetStoreBusy(3, true)
	fit := c.ruleManager.FitRegionWithoutCache(tc, region)
	_, _, temp := c.strategy(region, fit.RuleFits[0].Rule, false).getBetterLocation(tc, region, fit, fit.RuleFits[0])
	require.True(t, temp)
	_, dst, temp := c.strategy(region, fit.RuleFits[1].Rule, false).getBetterLocation(tc, region, fit, fit.RuleFits[1])
	require.False(t, temp)
	require.Equal(t, uint64(4), dst)
	d := c.regionPlacementFeedback(region, fit)
	t.Logf("first rule temporary, second rule rejects: status=%v", d.status)
	require.Equal(t, bestLocationRejected, d.status)
	require.Equal(t, d.status, c.regionPlacementBestLocation(region, fit))
	// Neither rule order nor temporary recovery may change a definite rejection.
	fit.RuleFits[0], fit.RuleFits[1] = fit.RuleFits[1], fit.RuleFits[0]
	require.Equal(t, bestLocationRejected, c.regionPlacementFeedback(region, fit).status)
	tc.SetStoreBusy(3, false)
	require.Equal(t, bestLocationRejected, c.regionPlacementFeedback(region, fit).status)
}

func TestAffinityFeedbackConflictingRegions(t *testing.T) {
	tc := mockcluster.NewCluster(t.Context(), newAffinityTestOptions())
	c := newTestAffinityChecker(t.Context(), tc, tc.PersistOptions)
	for i, z := range []string{"a", "d", "b", "c", "b", "c", "d"} {
		tc.AddRegionStore(uint64(i+1), 20+i*10)
		tc.SetStoreLabel(uint64(i+1), map[string]string{"zone": z, "id": strconv.Itoa(i + 1)})
	}
	rule := c.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
	rule.Count = 2
	rule.LocationLabels = []string{"zone"}
	require.NoError(t, c.ruleManager.SetRule(rule))
	require.NoError(t, c.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: "learner", Role: placement.Learner, Count: 1}))
	tc.AddLeaderRegion(1, 1, 2)
	tc.AddLeaderRegion(2, 1, 2)
	regions := []*core.RegionInfo{tc.GetRegion(1).Clone(core.WithAddPeer(&metapb.Peer{Id: 100, StoreId: 5, Role: metapb.PeerRole_Learner})), tc.GetRegion(2).Clone(core.WithAddPeer(&metapb.Peer{Id: 101, StoreId: 6, Role: metapb.PeerRole_Learner}))}
	ids := []uint64{1, 2, 3, 4, 5, 6, 7}
	check := c.balanceTargetCheck(t.Context(), ids, regions)
	loc := [][]int32{{0}, {3}, {1}, {2}, {1}, {2}, {3}}
	in := &balanceplan.Input{Stores: 7, BaseVoters: []int{0, 5, 0, 0, 0, 0, 0}, Groups: []balanceplan.Group{{Rules: []*balanceplan.Rule{{Role: balanceplan.RoleVoter, Count: 2, Stores: []int{0, 1, 2, 3, 6}, Location: loc}}, Voters: []int{0, 1}, Leader: 0, Forced: []int{0}, LeaderPinned: true, CanLead: slices.Repeat([]bool{true}, 7), Exclude: []int{4, 5}, CheckTarget: check}}}
	legal := 0
	for _, s := range []int{1, 2, 3, 6} {
		f := check([]int{0, s}, 0, balanceplan.DefaultWorkBudget)
		t.Logf("candidate=[0 %d] feedback=%+v", s, f)
		if f.Status == balanceplan.TargetSatisfied {
			legal++
		}
	}
	require.Equal(t, 2, legal)
	r := balanceplan.Plan(in, balanceplan.Options{Seed: 7})
	t.Logf("regions disagree: baseline=%v score=%v voters=%v limited=%v work=%d", r.Baseline, r.Score, r.Voters, r.PlacementLimited, r.Work)
	require.True(t, r.Score.Less(r.Baseline))
	require.Contains(t, r.Voters[0], 6)
	for i, v := range r.Voters {
		require.Equal(t, balanceplan.TargetSatisfied, check(v, r.Leaders[i], balanceplan.DefaultWorkBudget).Status)
	}
}

func TestAffinityFeedbackRuleReassignment(t *testing.T) {
	tc := mockcluster.NewCluster(t.Context(), newAffinityTestOptions())
	c := newTestAffinityChecker(t.Context(), tc, tc.PersistOptions)
	for i, z := range []string{"x", "a", "a", "b", "d", "d"} {
		tc.AddRegionStore(uint64(i+1), 10+i*10)
		tc.SetStoreLabel(uint64(i+1), map[string]string{"zone": z, "id": strconv.Itoa(i + 1)})
	}
	for i, ids := range [][]string{{"1"}, {"2", "3", "4", "6"}, {"2", "3", "5"}} {
		count, role := 1, placement.Voter
		if i == 0 {
			role = placement.Leader
		}
		if i == 1 {
			count = 2
		}
		require.NoError(t, c.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: strconv.Itoa(i), Index: i + 1, Override: i == 0, Role: role, Count: count, LocationLabels: []string{"zone"}, LabelConstraints: []placement.LabelConstraint{{Key: "id", Op: placement.In, Values: ids}}}))
	}
	// Replacing store 5 keeps its peer ID, so the real fitter assigns store 3 to A.
	tc.AddLeaderRegion(1, 5, 1, 2, 4)
	region := tc.GetRegion(1).Clone(core.WithReplaceLeaderStore(1))
	ids := []uint64{1, 2, 3, 4, 5, 6}
	loc := [][]int32{{0}, {1}, {1}, {2}, {3}, {3}}
	rules := []*balanceplan.Rule{{Role: balanceplan.RoleLeader, Count: 1, Stores: []int{0}, Location: loc}, {Role: balanceplan.RoleVoter, Count: 2, Stores: []int{1, 2, 3, 5}, Location: loc}, {Role: balanceplan.RoleVoter, Count: 1, Stores: []int{1, 2, 4}, Location: loc}}
	check := c.balanceTargetCheck(t.Context(), ids, []*core.RegionInfo{region})
	in := &balanceplan.Input{Stores: 6, BaseVoters: []int{0, 0, 0, 0, 5, 0}, Groups: []balanceplan.Group{{Rules: rules, Voters: []int{0, 1, 3, 4}, RuleOf: []int{0, 1, 1, 2}, Leader: 0, LeaderPinned: true, CanLead: []bool{true, false, false, false, false, false}}}}
	raw := balanceplan.Plan(in, balanceplan.Options{Seed: 7})
	t.Logf("raw voters=%v feedback=%+v", raw.Voters, check(raw.Voters[0], raw.Leaders[0], balanceplan.DefaultWorkBudget))
	require.Equal(t, balanceplan.TargetRejected, check(raw.Voters[0], raw.Leaders[0], balanceplan.DefaultWorkBudget).Status)
	require.Equal(t, balanceplan.TargetSatisfied, check([]int{0, 1, 3, 5}, 0, balanceplan.DefaultWorkBudget).Status)
	in.Groups[0].CheckTarget = check
	r := balanceplan.Plan(in, balanceplan.Options{Seed: 7})
	t.Logf("assignment: baseline=%v score=%v voters=%v work=%d", r.Baseline, r.Score, r.Voters, r.Work)
	require.True(t, r.Score.Less(r.Baseline))
	require.Equal(t, balanceplan.TargetSatisfied, check(r.Voters[0], r.Leaders[0], balanceplan.DefaultWorkBudget).Status)
}

func TestAffinityFeedbackForcedSource(t *testing.T) {
	tc := mockcluster.NewCluster(t.Context(), newAffinityTestOptions())
	c := newTestAffinityChecker(t.Context(), tc, tc.PersistOptions)
	for i, z := range []string{"a", "a", "b", "b", "b"} {
		tc.AddRegionStore(uint64(i+1), 10+i)
		tc.SetStoreLabel(uint64(i+1), map[string]string{"zone": z, "id": strconv.Itoa(i + 1)})
	}
	for i, ids := range [][]string{{"1", "3"}, {"2", "4", "5"}} {
		require.NoError(t, c.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: strconv.Itoa(i), Index: i + 1, Override: i == 0, Role: placement.Voter, Count: 1, LocationLabels: []string{"zone"}, LabelConstraints: []placement.LabelConstraint{{Key: "id", Op: placement.In, Values: ids}}}))
	}
	tc.AddLeaderRegion(1, 1, 4)
	region := tc.GetRegion(1)
	check := c.balanceTargetCheck(t.Context(), []uint64{1, 2, 3, 4, 5}, []*core.RegionInfo{region})
	loc := [][]int32{{0}, {0}, {1}, {1}, {1}}
	in := &balanceplan.Input{Stores: 5, BaseVoters: []int{0, 0, 0, 5, 0}, Groups: []balanceplan.Group{{Rules: []*balanceplan.Rule{{Count: 1, Stores: []int{0, 2}, Location: loc}, {Count: 1, Stores: []int{1, 3, 4}, Location: loc}}, Voters: []int{0, 3}, RuleOf: []int{0, 1}, Leader: 0, Forced: []int{0}, LeaderPinned: true, CanLead: slices.Repeat([]bool{true}, 5), CheckTarget: check}}}
	require.Equal(t, balanceplan.TargetSatisfied, check([]int{0, 4}, 0, balanceplan.DefaultWorkBudget).Status)
	r := balanceplan.Plan(in, balanceplan.Options{Seed: 7})
	t.Logf("forced source: baseline=%v score=%v voters=%v", r.Baseline, r.Score, r.Voters)
	require.True(t, r.Score.Less(r.Baseline))
	require.Contains(t, r.Voters[0], 0)
	require.Equal(t, 0, r.Leaders[0])
	require.Equal(t, balanceplan.TargetSatisfied, check(r.Voters[0], r.Leaders[0], balanceplan.DefaultWorkBudget).Status)
}

func TestAffinityFeedbackForcedSingleRule(t *testing.T) {
	tc := mockcluster.NewCluster(t.Context(), newAffinityTestOptions())
	c := newTestAffinityChecker(t.Context(), tc, tc.PersistOptions)
	for i, z := range []string{"a", "b", "c", "c", "a", "b"} {
		count := 10 + i
		if i == 0 {
			count = 100
		}
		tc.AddRegionStore(uint64(i+1), count)
		tc.SetStoreLabel(uint64(i+1), map[string]string{"zone": z})
	}
	rule := c.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
	rule.Count = 2
	rule.LocationLabels = []string{"zone"}
	require.NoError(t, c.ruleManager.SetRule(rule))
	require.NoError(t, c.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: "learners", Role: placement.Learner, Count: 2}))
	tc.AddLeaderRegion(1, 1, 3)
	region := tc.GetRegion(1).Clone(core.WithAddPeer(&metapb.Peer{Id: 100, StoreId: 5, Role: metapb.PeerRole_Learner}), core.WithAddPeer(&metapb.Peer{Id: 101, StoreId: 6, Role: metapb.PeerRole_Learner}))
	check := c.balanceTargetCheck(t.Context(), []uint64{1, 2, 3, 4, 5, 6}, []*core.RegionInfo{region})
	// The PD removal tie break chooses the pinned leader in [0,1]. Moving 1 also fixes it.
	in := &balanceplan.Input{Stores: 6, BaseVoters: []int{0, 0, 5, 0, 0, 0}, Groups: []balanceplan.Group{{Rules: []*balanceplan.Rule{{Count: 2, Stores: []int{0, 1, 2, 3}, Location: [][]int32{{0}, {1}, {2}, {2}, {0}, {1}}}}, Voters: []int{0, 2}, Leader: 0, LeaderPinned: true, CanLead: slices.Repeat([]bool{true}, 6), Exclude: []int{4, 5}, CheckTarget: check}}}
	require.Equal(t, balanceplan.TargetSatisfied, check([]int{0, 3}, 0, balanceplan.DefaultWorkBudget).Status)
	r := balanceplan.Plan(in, balanceplan.Options{Seed: 7})
	t.Logf("forced single rule: baseline=%v score=%v voters=%v", r.Baseline, r.Score, r.Voters)
	require.True(t, r.Score.Less(r.Baseline))
	require.Equal(t, []int{0, 3}, r.Voters[0])
}

func TestAffinityFeedbackRegionRejectionDominatesTemporary(t *testing.T) {
	tc := mockcluster.NewCluster(t.Context(), newAffinityTestOptions())
	c := newTestAffinityChecker(t.Context(), tc, tc.PersistOptions)
	for i, z := range []string{"a", "a", "b", "c", "b", "c", "d"} {
		count := 10 + i
		if i == 1 {
			count = 100
		}
		tc.AddRegionStore(uint64(i+1), count)
		tc.SetStoreLabel(uint64(i+1), map[string]string{"zone": z})
	}
	rule := c.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
	rule.Count = 2
	rule.LocationLabels = []string{"zone"}
	require.NoError(t, c.ruleManager.SetRule(rule))
	require.NoError(t, c.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: "learner", Role: placement.Learner, Count: 1}))
	tc.AddLeaderRegion(1, 1, 2)
	tc.AddLeaderRegion(2, 1, 2)
	r1 := tc.GetRegion(1).Clone(core.WithAddPeer(&metapb.Peer{Id: 100, StoreId: 5, Role: metapb.PeerRole_Learner}))
	r2 := tc.GetRegion(2).Clone(core.WithAddPeer(&metapb.Peer{Id: 101, StoreId: 6, Role: metapb.PeerRole_Learner}))
	for _, id := range []uint64{4, 6, 7} {
		tc.SetStoreBusy(id, true)
	}
	require.Equal(t, bestLocationTemporarilyBlocked, c.regionPlacementFeedback(r1, c.ruleManager.FitRegionWithoutCache(tc, r1)).status)
	require.Equal(t, bestLocationRejected, c.regionPlacementFeedback(r2, c.ruleManager.FitRegionWithoutCache(tc, r2)).status)
	for _, regions := range [][]*core.RegionInfo{{r1, r2}, {r2, r1}} {
		f := c.balanceTargetCheck(t.Context(), []uint64{1, 2, 3, 4, 5, 6, 7}, regions)([]int{0, 1}, 0, balanceplan.DefaultWorkBudget)
		require.Equal(t, balanceplan.TargetRejected, f.Status)
		t.Logf("temporary and rejected Regions: status=%v repairs=%v", f.Status, f.Replace)
	}
}

func TestAffinityFeedbackVirtualTargetDeterministic(t *testing.T) {
	tc := mockcluster.NewCluster(t.Context(), newAffinityTestOptions())
	c := newTestAffinityChecker(t.Context(), tc, tc.PersistOptions)
	for i, z := range []string{"x", "a", "a", "b", "d", "a"} {
		count := []int{10, 100, 10, 50, 40, 20}[i]
		tc.AddRegionStore(uint64(i+1), count)
		tc.SetStoreLabel(uint64(i+1), map[string]string{"zone": z, "id": strconv.Itoa(i + 1)})
	}
	for i, ids := range [][]string{{"1"}, {"2", "3", "4", "5"}, {"2", "3", "6"}} {
		count, role := 1, placement.Voter
		if i == 0 {
			role = placement.Leader
		}
		if i == 1 {
			count = 2
		}
		require.NoError(t, c.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: strconv.Itoa(i), Index: i + 1, Override: i == 0, Role: role, Count: count, LocationLabels: []string{"zone"}, LabelConstraints: []placement.LabelConstraint{{Key: "id", Op: placement.In, Values: ids}}}))
	}
	tc.AddLeaderRegion(1, 5, 1, 6, 4)
	region := tc.GetRegion(1).Clone(core.WithReplaceLeaderStore(1))
	require.True(t, c.isRegionPlacementRuleSatisfiedWithBestLocation(region, false))
	statuses := map[bestLocationStatus]int{}
	for range 100 {
		virtual := cloneRegionWithReplacePeerStores(region, 1, 1, 2, 3, 4)
		fit := c.ruleManager.FitRegionWithoutCache(tc, virtual)
		status := c.regionPlacementFeedback(virtual, fit).status
		if statuses[status] == 0 {
			for _, rf := range fit.RuleFits {
				t.Logf("status=%v rule=%s peers=%v", status, rf.Rule.ID, rf.Peers)
			}
		}
		statuses[status]++
	}
	t.Logf("same physical target repeated 100 times: statuses=%v", statuses)
	require.Len(t, statuses, 1)
}

func TestAffinityFeedbackStopsBetweenRegions(t *testing.T) {
	tc := mockcluster.NewCluster(t.Context(), newAffinityTestOptions())
	c := newTestAffinityChecker(t.Context(), tc, tc.PersistOptions)
	for i, z := range []string{"a", "b", "c"} {
		tc.AddRegionStore(uint64(i+1), 10)
		tc.SetStoreLabel(uint64(i+1), map[string]string{"zone": z})
	}
	rule := c.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
	rule.LocationLabels = []string{"zone"}
	require.NoError(t, c.ruleManager.SetRule(rule))
	tc.AddLeaderRegion(1, 1, 2, 3)
	regions := slices.Repeat([]*core.RegionInfo{tc.GetRegion(1)}, 100)
	check := c.balanceTargetCheck(t.Context(), []uint64{1, 2, 3}, regions)
	limited := check([]int{0, 1, 2}, 0, 1)
	full := check([]int{0, 1, 2}, 0, balanceplan.DefaultWorkBudget)
	require.Equal(t, balanceplan.TargetRejected, limited.Status)
	require.Equal(t, balanceplan.TargetSatisfied, full.Status)
	require.Equal(t, limited.Work*100, full.Work)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	canceled := c.balanceTargetCheck(ctx, []uint64{1, 2, 3}, regions)([]int{0, 1, 2}, 0, balanceplan.DefaultWorkBudget)
	require.Zero(t, canceled.Work)
	t.Logf("callback region checks bounded: one=%d full100=%d canceled=%d", limited.Work, full.Work, canceled.Work)
}

func TestAffinityFeedbackHardIsolationTemporary(t *testing.T) {
	tc := mockcluster.NewCluster(t.Context(), newAffinityTestOptions())
	c := newTestAffinityChecker(t.Context(), tc, tc.PersistOptions)
	for i, z := range []string{"a", "a", "b"} {
		tc.AddRegionStore(uint64(i+1), 10)
		tc.SetStoreLabel(uint64(i+1), map[string]string{"zone": z})
	}
	rule := c.ruleManager.GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
	rule.Count = 2
	rule.LocationLabels = []string{"zone"}
	rule.IsolationLevel = "zone"
	require.NoError(t, c.ruleManager.SetRule(rule))
	tc.AddLeaderRegion(1, 1, 2)
	region := tc.GetRegion(1)
	fit := c.ruleManager.FitRegionWithoutCache(tc, region)
	require.True(t, fit.IsSatisfied())
	tc.SetStoreBusy(3, true)
	_, _, temporary := c.strategy(region, rule, false).getBetterLocation(tc, region, fit, fit.RuleFits[0])
	require.True(t, temporary)
	require.Equal(t, bestLocationTemporarilyBlocked, c.regionPlacementBestLocation(region, fit))
	require.Equal(t, bestLocationTemporarilyBlocked, c.regionPlacementFeedback(region, fit).status)
	tc.SetStoreBusy(3, false)
	require.Equal(t, bestLocationRejected, c.regionPlacementBestLocation(region, fit))
	require.Equal(t, bestLocationRejected, c.regionPlacementFeedback(region, fit).status)
	statusOnly := c.checkRegionPlacement(region, fit, false)
	require.Empty(t, statusOnly.replacements)
	require.NotEmpty(t, c.regionPlacementFeedback(region, fit).replacements)
	tc.SetStoreOffline(3)
	require.Empty(t, c.regionPlacementFeedback(region, fit).replacements)
	require.Equal(t, bestLocationRejected, c.regionPlacementBestLocation(region, fit))
	require.Equal(t, bestLocationRejected, c.regionPlacementFeedback(region, fit).status)
}
