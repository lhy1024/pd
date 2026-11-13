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

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/affinity/balanceplan"
	"github.com/tikv/pd/pkg/schedule/placement"
)

// Regression from a constructed 60-group capacity/expansion input. Each group
// has two Regions with different learners. This is not the reviewer's input.
func TestAffinityBalanceCapacityExpansion(t *testing.T) {
	f := newAffinityBalanceTest(t, 0)
	addStore := func(id uint64, z int, blocked bool) {
		f.AddRegionStore(id, 100+int(id%101))
		f.SetStoreLastHeartbeatInterval(id, -time.Hour)
		f.SetStoreLabel(id, map[string]string{"zone": fmt.Sprintf("z%d", z)})
		store := f.GetStore(id)
		capacity := uint64(1) << 40
		available := capacity - store.GetStoreStats().GetUsedSize()
		if blocked {
			available = uint64(1)
		}
		f.PutStore(store.Clone(core.SetStoreStats(&pdpb.StoreStats{Capacity: capacity, Available: available, UsedSize: capacity - available})))
		if blocked {
			require.True(t, f.GetStore(id).IsLowSpace(f.GetLowSpaceRatio()))
		}
	}
	first := []uint64{0, 0, 0}
	for s := range 18 {
		z := s / 6
		addStore(uint64(s+1), z, s%5 == 4)
		if first[z] == 0 {
			first[z] = uint64(s + 1)
		}
	}
	addStore(1001, 1, false)
	addStore(1002, 2, false)
	require.NoError(t, f.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: "leader", Index: 1, Override: true, Role: placement.Leader, Count: 1, LocationLabels: []string{"zone"}, LabelConstraints: []placement.LabelConstraint{{Key: "zone", Op: placement.In, Values: []string{"z0"}}}}))
	require.NoError(t, f.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: "voter", Index: 2, Role: placement.Voter, Count: 2, LocationLabels: []string{"zone"}}))
	require.NoError(t, f.ruleManager.SetRule(&placement.Rule{GroupID: placement.DefaultGroupID, ID: "learner", Index: 3, Role: placement.Learner, Count: 1}))
	for g := range 60 {
		id := uint64(2*g + 1)
		f.group(balanceTestGroupID(fmt.Sprintf("_tidb_pt_100_p%d", g+1)), id, first[0], first, fmt.Sprintf("k%04da", g), fmt.Sprintf("k%04db", g), fmt.Sprintf("k%04dc", g))
		for j := range 2 {
			r := f.GetRegion(id + uint64(j))
			f.PutRegion(r.Clone(core.WithAddPeer(&metapb.Peer{Id: 10000 + r.GetID(), StoreId: 1001 + uint64(j), Role: metapb.PeerRole_Learner})))
		}
	}
	for phase := range 2 {
		if phase == 1 {
			for s := range 12 {
				z := s / 4
				addStore(uint64(19+s), z, s%5 == 4)
			}
		}
		snapshot := f.snapshot()
		input := f.balancePlanInput(snapshot, nil, nil)
		for _, rule := range input.input.Groups[0].Rules {
			if rule.Count == 2 {
				expected := 20
				if phase == 1 {
					expected = 32
				}
				expected -= []int{3, 5}[phase]
				require.Len(t, rule.Stores, expected)
			}
		}
		result, err := f.planBalanceTargets(t.Context(), snapshot)
		require.NoError(t, err)
		rejected := 0
		for _, target := range result.targets {
			status, _, _ := f.validateBalancePlanTarget(t.Context(), snapshot, snapshot.Group(target.ID), target)
			if status != balanceAllowed {
				rejected++
			}
		}
		t.Logf("phase=%d groups=60 regions=120 score=%v baseline=%v targets=%d rejected=%d rounds=%d work=%d stop=%s retry=%v", phase, result.planner.Score, result.planner.Baseline, len(result.targets), rejected, result.rounds, result.work, result.planner.Stop, result.retry)
		require.Zero(t, rejected)
		require.NotEmpty(t, result.targets)
		require.True(t, result.planner.Score.Less(result.planner.Baseline))
		require.Greater(t, result.rounds, 1)
		require.LessOrEqual(t, result.rounds, maxBalanceValidationRounds)
		require.Equal(t, balanceplan.StopGreedy, result.planner.Stop)
		written, _, err := f.commitBalancePlan(t.Context(), snapshot, result, f.ruleManager.GetRevision())
		require.NoError(t, err)
		require.Len(t, written, len(result.targets))
		f.settle()
		if phase == 1 {
			stableSnapshot := f.snapshot()
			stable, err := f.planBalanceTargets(t.Context(), stableSnapshot)
			require.NoError(t, err)
			require.Empty(t, stable.targets)
			require.Zero(t, stable.planner.Changed)
			require.False(t, f.step())
			require.Equal(t, stableSnapshot.Version, f.snapshot().Version)
			t.Logf("settled sixty: score=%v changed=%d work=%d retry=%v", stable.planner.Score, stable.planner.Changed, stable.work, stable.retry)
		}
	}
}
