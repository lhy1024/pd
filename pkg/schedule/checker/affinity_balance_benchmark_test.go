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
	"encoding/hex"
	"fmt"
	"slices"
	"testing"

	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

type affinityBalanceBenchmarkCase struct {
	name                    string
	stores, groups, regions int
	ranges                  int
	constrained             bool
	evenLeaders             bool
	splitRules              bool
	busyDestinations        bool
	resetAfterEachIteration bool
}

type affinityBalanceBenchmarkSetup struct {
	ctx     context.Context
	cluster *mockcluster.Cluster
	checker *AffinityChecker
	manager *affinity.Manager
	tableID string
	groups  []affinity.Group
}

func newAffinityBalanceBenchmarkSetup(b *testing.B, tc affinityBalanceBenchmarkCase) *affinityBalanceBenchmarkSetup {
	b.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	b.Cleanup(cancel)

	opt := newAffinityTestOptions()
	cluster := mockcluster.NewCluster(ctx, opt)
	for id := 1; id <= tc.stores; id++ {
		if tc.constrained {
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
			cluster.AddLabelsStore(uint64(id), 10, labels)
		} else {
			cluster.AddRegionStore(uint64(id), 10)
		}
		cluster.SetStoreUp(uint64(id))
	}
	if tc.constrained {
		rule := cluster.GetRuleManager().GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
		rule.LocationLabels = []string{"zone"}
		rule.IsolationLevel = "zone"
		rule.LabelConstraints = []placement.LabelConstraint{{Key: "tier", Op: placement.In, Values: []string{"source"}}}
		if err := cluster.GetRuleManager().SetRule(rule); err != nil {
			b.Fatal(err)
		}
	}

	manager := cluster.GetAffinityManager()
	checker := newTestAffinityChecker(ctx, cluster, opt)
	setup := &affinityBalanceBenchmarkSetup{
		ctx:     ctx,
		cluster: cluster,
		checker: checker,
		manager: manager,
		tableID: "_tidb_pt_affinity_benchmark",
	}
	rangeCount := max(1, tc.ranges)
	if tc.regions%rangeCount != 0 {
		b.Fatal("regions per group must be divisible by ranges per group")
	}
	regionsPerRange := tc.regions / rangeCount
	if tc.splitRules && regionsPerRange < 2 {
		b.Fatal("a rule boundary requires at least two Regions per range")
	}
	for groupIndex := range tc.groups {
		groupID := fmt.Sprintf("%s_p%04d", setup.tableID, groupIndex)
		leader := uint64(1)
		if tc.constrained || tc.evenLeaders {
			// Keep leader counts even, so the leader phase has no strict
			// improvement and the benchmark reaches voter candidate checking.
			leader = uint64(groupIndex%3 + 1)
		}
		voters := []uint64{1, 2, 3}
		ranges := affinity.GroupKeyRanges{GroupID: groupID}
		for rangeIndex := range rangeCount {
			// Interleave groups so multiple ranges remain disjoint after Merge.
			start := (rangeIndex*tc.groups + groupIndex) * regionsPerRange
			ranges.KeyRanges = append(ranges.KeyRanges, keyutil.KeyRange{
				StartKey: []byte(fmt.Sprintf("%08d", start)),
				EndKey:   []byte(fmt.Sprintf("%08d", start+regionsPerRange)),
			})
			if tc.splitRules {
				rule := cluster.GetRuleManager().GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
				rule.ID, rule.Index, rule.Override = fmt.Sprintf("boundary-%d-%d", groupIndex, rangeIndex), 1, true
				rule.StartKeyHex = hex.EncodeToString([]byte(fmt.Sprintf("%08d", start+regionsPerRange/2)))
				rule.EndKeyHex = hex.EncodeToString([]byte(fmt.Sprintf("%08d", start+regionsPerRange)))
				if err := cluster.GetRuleManager().SetRule(rule); err != nil {
					b.Fatal(err)
				}
			}
		}
		if err := manager.CreateAffinityGroups([]affinity.GroupKeyRanges{ranges}); err != nil {
			b.Fatal(err)
		}
		if _, err := manager.UpdateAffinityGroupPeers(groupID, leader, voters); err != nil {
			b.Fatal(err)
		}
		setup.groups = append(setup.groups, affinity.Group{ID: groupID, LeaderStoreID: leader, VoterStoreIDs: voters})
		for rangeIndex := range rangeCount {
			start := (rangeIndex*tc.groups + groupIndex) * regionsPerRange
			for regionIndex := range regionsPerRange {
				key := start + regionIndex
				addBalanceRegion(b, cluster, manager, uint64(key+1),
					fmt.Sprintf("%08d", key), fmt.Sprintf("%08d", key+1), leader, voters...)
			}
		}
	}
	if tc.busyDestinations {
		for id := 4; id <= tc.stores; id++ {
			cluster.SetStoreBusy(uint64(id), true)
		}
	}
	return setup
}

func (s *affinityBalanceBenchmarkSetup) assertTargetsUnchanged(b *testing.B) {
	b.Helper()
	for _, expected := range s.groups {
		actual := s.manager.GetAffinityGroupState(expected.ID)
		if actual.LeaderStoreID != expected.LeaderStoreID || !slices.Equal(actual.VoterStoreIDs, expected.VoterStoreIDs) {
			b.Fatalf("no-action setup produced a committed target for %s", expected.ID)
		}
	}
}

func (s *affinityBalanceBenchmarkSetup) resetImprovingTargets(b *testing.B) {
	b.Helper()
	b.StopTimer()
	committed := false
	for _, expected := range s.groups {
		actual := s.manager.GetAffinityGroupState(expected.ID)
		if actual.LeaderStoreID == expected.LeaderStoreID && slices.Equal(actual.VoterStoreIDs, expected.VoterStoreIDs) {
			continue
		}
		committed = true
		if _, err := s.manager.UpdateAffinityGroupPeers(expected.ID, expected.LeaderStoreID, expected.VoterStoreIDs); err != nil {
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
	cases := []affinityBalanceBenchmarkCase{
		{name: "balanced/s3_g12_r8", stores: 3, groups: 12, regions: 8, evenLeaders: true},
		{name: "balanced/s3_g120_r8", stores: 3, groups: 120, regions: 8, evenLeaders: true},
		{name: "improving/s8_g12_r4", stores: 8, groups: 12, regions: 4, resetAfterEachIteration: true},
		{name: "improving/s8_g12_r64", stores: 8, groups: 12, regions: 64, resetAfterEachIteration: true},
		{name: "improving/s8_g96_r4", stores: 8, groups: 96, regions: 4, resetAfterEachIteration: true},
		{name: "voter/s8_g12_r4", stores: 8, groups: 12, regions: 4, evenLeaders: true, resetAfterEachIteration: true},
		{name: "voter/s128_g24_r8", stores: 128, groups: 24, regions: 8, evenLeaders: true, resetAfterEachIteration: true},
		{name: "constrained/s32_g12_r64", stores: 32, groups: 12, regions: 64, constrained: true},
		{name: "constrained/s128_g24_r8", stores: 128, groups: 24, regions: 8, constrained: true},
		{name: "constrained/s128_g96_r8", stores: 128, groups: 96, regions: 8, constrained: true},
		{name: "ranges/s32_g12_r64_k8", stores: 32, groups: 12, regions: 64, ranges: 8, constrained: true},
		{name: "rule_boundaries/s32_g12_r64", stores: 32, groups: 12, regions: 64, splitRules: true, constrained: true},
		{name: "busy/s128_g24_r8", stores: 128, groups: 24, regions: 8, evenLeaders: true, busyDestinations: true},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			setup := newAffinityBalanceBenchmarkSetup(b, tc)
			if !tc.resetAfterEachIteration {
				// Validate the scenario once outside the timed region. If a future
				// placement change makes a candidate legal, this benchmark must fail
				// instead of silently measuring a different workload.
				if err := setup.checker.balancer.step(setup.ctx, setup.tableID); err != nil {
					b.Fatal(err)
				}
				setup.assertTargetsUnchanged(b)
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
			b.StopTimer()
			setup.assertTargetsUnchanged(b)
		})
	}
}

// BenchmarkAffinityBalanceIdle measures one periodic batch for a registered
// table, including dirty marking and admission. It uses a local balancer with
// no goroutine so a background worker cannot consume the measured batch.
func BenchmarkAffinityBalanceIdle(b *testing.B) {
	for _, tc := range []struct {
		state          string
		stores, groups int
	}{
		{state: "paused", stores: 128, groups: 24},
		{state: "paused", stores: 128, groups: 240},
		{state: "halted", stores: 128, groups: 24},
		{state: "limit_zero", stores: 128, groups: 24},
		{state: "fixed", stores: 128, groups: 24},
		{state: "fixed", stores: 128, groups: 240},
		{state: "single", stores: 128, groups: 1},
	} {
		b.Run(fmt.Sprintf("%s/s%d_g%d", tc.state, tc.stores, tc.groups), func(b *testing.B) {
			setup := newAffinityBalanceBenchmarkSetup(b, affinityBalanceBenchmarkCase{stores: tc.stores, groups: tc.groups, regions: 1})
			switch tc.state {
			case "paused":
				setup.checker.PauseOrResume(3600)
			case "halted":
				setup.cluster.SetHaltScheduling(true, "benchmark")
			case "limit_zero":
				setup.cluster.SetAffinityScheduleLimit(0)
			case "fixed":
				for _, group := range setup.groups {
					if _, err := setup.manager.UpdateAffinityGroupPolicy(group.ID, affinity.BalancePolicyFixed); err != nil {
						b.Fatal(err)
					}
				}
			}
			balancer := &affinityBalancer{checker: setup.checker, tables: map[string]*affinityBalanceTask{setup.tableID: {}}}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				balancer.markAllDirty()
				balancer.processDirty(setup.ctx)
			}
			b.StopTimer()
			setup.assertTargetsUnchanged(b)
		})
	}
}
