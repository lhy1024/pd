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
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule/affinity/balanceplan"
	"github.com/tikv/pd/pkg/schedule/placement"
)

// The relaxed target needs two directed replacements before any legal gain.
func TestAffinityPlannerMultipleRepairs(t *testing.T) {
	tc := mockcluster.NewCluster(t.Context(), newAffinityTestOptions())
	c := newTestAffinityChecker(t.Context(), tc, tc.PersistOptions)
	for i, zone := range []string{"a", "a", "b", "b", "a", "c"} {
		tc.AddRegionStore(uint64(i+1), 10+i)
		tc.SetStoreLabel(uint64(i+1), map[string]string{"zone": zone, "id": strconv.Itoa(i + 1)})
	}
	for i, ids := range [][]string{{"1"}, {"2", "3", "4"}, {"5", "6"}} {
		require.NoError(t, c.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: strconv.Itoa(i), Index: i + 1,
			Override: i == 0, Role: placement.Voter, Count: 1, LocationLabels: []string{"zone"},
			LabelConstraints: []placement.LabelConstraint{{Key: "id", Op: placement.In, Values: ids}}}))
	}
	tc.AddLeaderRegion(1, 1, 3, 6)
	loc := [][]int32{{0}, {0}, {1}, {1}, {0}, {2}}
	rules := []*balanceplan.Rule{{Count: 1, Stores: []int{0}, Location: loc}, {Count: 1, Stores: []int{1, 2, 3}, Location: loc}, {Count: 1, Stores: []int{4, 5}, Location: loc}}
	// Five fixed groups have the same legal voters and leader as this group.
	in := &balanceplan.Input{Stores: 6, BaseVoters: []int{5, 0, 5, 0, 0, 5}, BaseLeaders: []int{5, 0, 0, 0, 0, 0}, Groups: []balanceplan.Group{{Rules: rules, Voters: []int{0, 2, 5}, RuleOf: []int{0, 1, 2}, Leader: 0, CanLead: []bool{true, false, false, false, false, false}}}}
	check := c.balanceTargetCheck(t.Context(), []uint64{1, 2, 3, 4, 5, 6}, []*core.RegionInfo{tc.GetRegion(1)})
	raw := balanceplan.Plan(in, balanceplan.Options{Seed: 7})
	require.Equal(t, []int{0, 1, 4}, raw.Voters[0])
	require.Equal(t, balanceplan.Score{Voter: 108, Leader: 36}, raw.Baseline)
	for _, neighbor := range [][]int{{0, 2, 4}, {0, 3, 4}, {0, 1, 5}} {
		require.Equal(t, balanceplan.TargetRejected, check(neighbor, 0, balanceplan.DefaultWorkBudget).Status)
	}
	in.Groups[0].CheckTarget = func(voters []int, leader int, limit int64) balanceplan.TargetFeedback {
		feedback := check(voters, leader, limit)
		if !slices.Equal(voters, raw.Voters[0]) {
			feedback.Replace = nil
		}
		return feedback
	}
	once := balanceplan.Plan(in, balanceplan.Options{Seed: 7})
	require.Equal(t, once.Baseline, once.Score)
	in.Groups[0].CheckTarget = check
	full := balanceplan.Plan(in, balanceplan.Options{Seed: 7})
	require.Equal(t, balanceplan.Score{Voter: 98, Leader: 36}, full.Score)
	require.ElementsMatch(t, []int{0, 3, 5}, full.Voters[0])
	require.Equal(t, balanceplan.TargetSatisfied, check(full.Voters[0], full.Leaders[0], balanceplan.DefaultWorkBudget).Status)
}
