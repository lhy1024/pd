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
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/affinity/balanceplan"
	"github.com/tikv/pd/pkg/schedule/placement"
)

// The review topology: 1000 groups on 300 stores in three zones. Exercise the
// execution path and real PD validation in both numbering and rule orders.
func TestAffinityBalanceAtScale(t *testing.T) {
	for _, roundRobin := range []bool{false, true} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("roundRobin=%v/reverse=%v", roundRobin, reverse), func(t *testing.T) {
				const stores, groups = 300, 1000
				f := newAffinityBalanceTest(t, stores)
				first := []uint64{0, 0, 0}
				for s := range stores {
					z, id := s/100, uint64(s+1)
					if roundRobin {
						z = s % 3
					}
					f.UpdateRegionCount(id, 10+(s*7919)%1009)
					store := f.GetStore(id)
					capacity := uint64(1) << 40
					used := store.GetStoreStats().GetUsedSize()
					f.PutStore(store.Clone(core.SetStoreStats(&pdpb.StoreStats{Capacity: capacity, UsedSize: used, Available: capacity - used})))
					f.SetStoreLabel(id, map[string]string{"zone": fmt.Sprintf("z%d", z)})
					if first[z] == 0 {
						first[z] = id
					}
				}
				li, vi := 1, 2
				if reverse {
					li, vi = 2, 1
				}
				require.NoError(t, f.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: "leader", Index: li, Override: !reverse,
					Role: placement.Leader, Count: 1, LocationLabels: []string{"zone"}, LabelConstraints: []placement.LabelConstraint{{Key: "zone", Op: placement.In, Values: []string{"z0"}}}}))
				require.NoError(t, f.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: "voter", Index: vi, Override: reverse,
					Role: placement.Voter, Count: 2, LocationLabels: []string{"zone"}}))
				for i := range groups {
					f.group(balanceTestGroupID(fmt.Sprintf("_tidb_pt_100_p%d", i+1)), uint64(i+1), first[0], first, fmt.Sprintf("k%04d", i), fmt.Sprintf("k%04d", i+1))
				}
				snapshot := f.snapshot()
				start := time.Now()
				result, err := f.planBalanceTargets(t.Context(), snapshot)
				require.NoError(t, err)
				t.Logf("1000x300 score=%v baseline=%v targets=%d rounds=%d work=%d stop=%s retry=%v elapsed=%v", result.planner.Score, result.planner.Baseline, len(result.targets), result.rounds, result.work, result.planner.Stop, result.retry, time.Since(start))
				require.Equal(t, balanceplan.Score{Voter: 30000, Leader: 10000}, result.planner.Score)
				require.Len(t, result.targets, 990)
				require.Equal(t, 1, result.rounds)
				require.Equal(t, balanceplan.StopGreedy, result.planner.Stop)
				require.False(t, result.retry)
				for _, target := range result.targets {
					status, _, _, _ := f.validateBalancePlanTarget(t.Context(), snapshot, snapshot.Group(target.ID), target)
					require.Equal(t, balanceAllowed, status)
				}
				written, _, err := f.commitBalancePlan(t.Context(), snapshot, result, f.ruleManager.GetRevision())
				require.NoError(t, err)
				require.Len(t, written, len(result.targets))
				f.settle()
				stableSnapshot := f.snapshot()
				stable, err := f.planBalanceTargets(t.Context(), stableSnapshot)
				require.NoError(t, err)
				require.Empty(t, stable.targets)
				require.Zero(t, stable.planner.Changed)
				require.False(t, f.step())
				require.Equal(t, stableSnapshot.Version, f.snapshot().Version)
			})
		}
	}
}
