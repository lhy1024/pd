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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/schedule/affinity/balanceplan"
	"github.com/tikv/pd/pkg/schedule/placement"
)

func TestAffinityBalanceSameRoleIsolation(t *testing.T) {
	re := require.New(t)
	f := newAffinityBalanceTest(t, 3, "a", "a", "b")
	for id, zone := range []string{"a", "a", "b"} {
		f.SetStoreLabel(uint64(id+1), map[string]string{"zone": zone, "id": fmt.Sprint(id + 1)})
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
		status, _ := f.validateBalancePlanTarget(t.Context(), snapshot, snapshot.Group(target.ID), target)
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
	t.Logf("raw score=%v -> %v changed=%d; validated score=%v targets=%d rounds=%d groupCounts=%v retry=%v", result.Baseline, result.Score, result.Changed, res.planner.Score, len(res.targets), len(res.stops), res.groupCounts, res.retry)
	re.Equal(balanceplan.Score{Voter: 8, Leader: 2}, res.planner.Score)
	re.Len(res.targets, 1)
	re.Len(res.stops, 1)
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
	re.True(f.step())
	re.Equal([2]int{6, 2}, balanceTableScore(f.affinityManager, balanceTestTable()))
	for _, group := range f.snapshot().Groups {
		re.NotContains(group.VoterStoreIDs, uint64(2))
	}
}
