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
	"slices"
	"testing"

	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/placement"
)

type affinityBalanceBenchmarkSetup struct {
	ctx      context.Context
	checker  *AffinityChecker
	manager  *affinity.Manager
	tableID  string
	groupIDs []string
}

func newAffinityBalanceBenchmarkSetup(
	b *testing.B, storeCount, groupCount, regionsPerGroup int, constrained, balanced bool,
) *affinityBalanceBenchmarkSetup {
	b.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	b.Cleanup(cancel)

	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	for id := 1; id <= storeCount; id++ {
		if constrained {
			// Only the first three stores carry tier=source. Replacements to
			// other stores fail the label constraint, so no legal move exists.
			zone := "z1"
			if id <= 3 {
				zone = fmt.Sprintf("z%d", id)
			}
			labels := map[string]string{"zone": zone}
			if id <= 3 {
				labels["tier"] = "source"
			}
			tc.AddLabelsStore(uint64(id), 10, labels)
		} else {
			tc.AddRegionStore(uint64(id), 10)
		}
		tc.SetStoreUp(uint64(id))
	}
	if constrained {
		rule := tc.GetRuleManager().GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
		rule.LocationLabels = []string{"zone"}
		rule.IsolationLevel = "zone"
		rule.LabelConstraints = []placement.LabelConstraint{{Key: "tier", Op: placement.In, Values: []string{"source"}}}
		if err := tc.GetRuleManager().SetRule(rule); err != nil {
			b.Fatal(err)
		}
	}

	manager := tc.GetAffinityManager()
	checker := newTestAffinityChecker(ctx, tc, opt)
	setup := &affinityBalanceBenchmarkSetup{
		ctx:     ctx,
		checker: checker,
		manager: manager,
		tableID: "_tidb_pt_affinity_benchmark",
	}
	for groupIndex := 0; groupIndex < groupCount; groupIndex++ {
		groupID := fmt.Sprintf("%s_p%04d", setup.tableID, groupIndex)
		start := fmt.Sprintf("%08d", groupIndex*regionsPerGroup)
		end := fmt.Sprintf("%08d", (groupIndex+1)*regionsPerGroup)
		leader := uint64(1)
		if constrained || balanced {
			// Keep leader counts even, so the leader phase has no strict
			// improvement and the benchmark reaches voter candidate checking.
			leader = uint64(groupIndex%3 + 1)
		}
		voters := []uint64{1, 2, 3}
		createBalanceGroup(b, manager, groupID, start, end, leader, voters, affinity.BalancePolicyAuto)
		setup.groupIDs = append(setup.groupIDs, groupID)
		for regionIndex := 0; regionIndex < regionsPerGroup; regionIndex++ {
			regionID := uint64(groupIndex*regionsPerGroup + regionIndex + 1)
			regionStart := fmt.Sprintf("%08d", groupIndex*regionsPerGroup+regionIndex)
			regionEnd := fmt.Sprintf("%08d", groupIndex*regionsPerGroup+regionIndex+1)
			addBalanceRegion(b, tc, manager, regionID, regionStart, regionEnd, leader, voters...)
		}
	}
	return setup
}

func (s *affinityBalanceBenchmarkSetup) resetImprovingTargets(b *testing.B) {
	b.Helper()
	b.StopTimer()
	committed := false
	for _, groupID := range s.groupIDs {
		committed = committed || s.manager.GetAffinityGroupState(groupID).LeaderStoreID != 1
		if _, err := s.manager.UpdateAffinityGroupPeers(groupID, 1, []uint64{1, 2, 3}); err != nil {
			b.Fatal(err)
		}
	}
	if !committed {
		b.Fatal("improving setup produced no committed target")
	}
	b.StartTimer()
}

// BenchmarkAffinityBalance measures the complete affinity planner path. Setup
// is excluded from timing; each iteration includes the balance snapshot, global
// counts, lazy candidates, Region placement checks, final Region review, and a
// single target commit when a legal improvement exists.
//
// The constrained cases measure searches where the scorer finds improvements
// but store label constraints reject every target; they are not a general
// worst-case bound for placement validation.
// They must be kept separate from placement's FitRegion microbenchmarks because
// the cost being measured includes affinity table and Region traversal.
func BenchmarkAffinityBalance(b *testing.B) {
	cases := []struct {
		name                    string
		stores, groups, regions int
		constrained             bool
		balanced                bool
		resetAfterEachIteration bool
	}{
		{name: "balanced/s3_g12_r8", stores: 3, groups: 12, regions: 8, balanced: true},
		{name: "improving/s8_g12_r4", stores: 8, groups: 12, regions: 4, resetAfterEachIteration: true},
		{name: "constrained/s32_g12_r64", stores: 32, groups: 12, regions: 64, constrained: true},
		{name: "constrained/s128_g24_r8", stores: 128, groups: 24, regions: 8, constrained: true},
	}
	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			setup := newAffinityBalanceBenchmarkSetup(b, tc.stores, tc.groups, tc.regions, tc.constrained, tc.balanced)
			if tc.constrained {
				// Validate the scenario once outside the timed region. If a future
				// placement change makes a candidate legal, this benchmark must fail
				// instead of silently measuring a different workload.
				before := make(map[string]affinity.Group, len(setup.groupIDs))
				for _, groupID := range setup.groupIDs {
					group := setup.manager.GetAffinityGroupState(groupID).Group
					group.VoterStoreIDs = slices.Clone(group.VoterStoreIDs)
					before[groupID] = group
				}
				if err := setup.checker.balancer.step(setup.ctx, setup.tableID); err != nil {
					b.Fatal(err)
				}
				for groupID, expected := range before {
					actual := setup.manager.GetAffinityGroupState(groupID).Group
					if actual.LeaderStoreID != expected.LeaderStoreID || !slices.Equal(actual.VoterStoreIDs, expected.VoterStoreIDs) {
						b.Fatalf("constrained setup produced a committed target for %s", groupID)
					}
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := setup.checker.balancer.step(setup.ctx, setup.tableID); err != nil {
					b.Fatal(err)
				}
				if tc.resetAfterEachIteration {
					setup.resetImprovingTargets(b)
				}
			}
		})
	}
}
