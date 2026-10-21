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
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule/affinity"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

type affinityBalanceScanCluster struct {
	sche.CheckerCluster
	delay       time.Duration
	starts      [][]byte
	labelReads  int
	beforeScan  func()
	beforeFinal func()
}

func (c *AffinityChecker) validateBalanceRegion(actual *affinity.GroupState, target *affinity.Group,
	region *core.RegionInfo, waiting map[uint64]struct{}) balanceStatus {
	fit, status := c.validateBalanceCurrentRegion(actual, region, waiting)
	if status != balanceAllowed || target == nil {
		return status
	}
	return c.validateBalanceTarget(target, region, fit)
}

func (c *affinityBalanceScanCluster) GetRegionLabeler() *labeler.RegionLabeler {
	c.labelReads++
	return c.CheckerCluster.GetRegionLabeler()
}

func (c *affinityBalanceScanCluster) ScanRegions(startKey, endKey []byte, limit int) []*core.RegionInfo {
	if c.beforeScan != nil {
		c.beforeScan()
	}
	c.starts = append(c.starts, slices.Clone(startKey))
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	return c.CheckerCluster.ScanRegions(startKey, endKey, limit)
}

func (c *affinityBalanceScanCluster) BatchScanRegions(ranges *keyutil.KeyRanges, opts ...core.BatchScanRegionsOptionFunc) ([]*core.RegionInfo, error) {
	if c.beforeFinal != nil {
		c.beforeFinal()
	}
	return c.CheckerCluster.BatchScanRegions(ranges, opts...)
}

func newAffinityBalanceTestCluster(t *testing.T, storeCount uint64) (*mockcluster.Cluster, *AffinityChecker) {
	t.Helper()
	opt := newAffinityTestOptions()
	tc := mockcluster.NewCluster(t.Context(), opt)
	for id := uint64(1); id <= storeCount; id++ {
		tc.AddRegionStore(id, 10)
		tc.SetStoreUp(id)
	}
	return tc, newTestAffinityChecker(t.Context(), tc, opt)
}

func createBalanceGroup(t *testing.T, manager *affinity.Manager, id, start, end string,
	leader uint64, voters []uint64, policy affinity.BalancePolicy) {
	t.Helper()
	group := &affinity.Group{ID: id, LeaderStoreID: leader, VoterStoreIDs: voters}
	require.NoError(t, createAffinityGroupForTest(manager, group, []byte(start), []byte(end)))
	if policy == affinity.BalancePolicyFixed {
		_, err := manager.UpdateAffinityGroupPolicy(id, affinity.BalancePolicyFixed)
		require.NoError(t, err)
	}
}

func addBalanceRegion(t *testing.T, tc *mockcluster.Cluster, manager *affinity.Manager, id uint64,
	start, end string, leader uint64, voters ...uint64) *core.RegionInfo {
	t.Helper()
	followers := slices.DeleteFunc(slices.Clone(voters), func(id uint64) bool { return id == leader })
	tc.AddLeaderRegion(id, leader, followers...)
	tc.PutRegion(tc.GetRegion(id).Clone(core.WithStartKey([]byte(start)), core.WithEndKey([]byte(end))))
	region := tc.GetRegion(id)
	group, _ := manager.GetAndCacheRegionAffinityGroupState(region)
	require.NotNil(t, group)
	return region
}

func runAffinityBalanceTask(t *testing.T, ctx context.Context, checker *AffinityChecker) bool {
	t.Helper()
	stable, err := checker.balancer.step(ctx, "_tidb_pt_100")
	require.NoError(t, err)
	return stable
}

func TestAffinityBalanceLeaderConvergence(t *testing.T) {
	tc, checker := newAffinityBalanceTestCluster(t, 3)
	manager := tc.GetAffinityManager()
	starts := []string{"a", "b", "c"}
	ends := []string{"b", "c", "d"}
	for i := uint64(1); i <= 3; i++ {
		id := []string{"_tidb_pt_100_p1", "_tidb_pt_100_p2", "_tidb_pt_100_p3"}[i-1]
		createBalanceGroup(t, manager, id, starts[i-1], ends[i-1], 1, []uint64{1, 2, 3}, affinity.BalancePolicyAuto)
		addBalanceRegion(t, tc, manager, i, starts[i-1], ends[i-1], 1, 1, 2, 3)
	}
	check := func(region *core.RegionInfo) []*operator.Operator {
		manager.ObserveBalanceRegion(region) // Controller observes before invoking individual checkers.
		return checker.Check(region)
	}
	for i := uint64(1); i <= 3; i++ {
		check(tc.GetRegion(i))
	}
	check(tc.GetRegion(1))
	require.Eventually(t, func() bool {
		check(tc.GetRegion(1))
		return manager.GetAffinityGroupState("_tidb_pt_100_p1").LeaderStoreID != 1
	}, 3*time.Second, 10*time.Millisecond)
	target := manager.GetAffinityGroupState("_tidb_pt_100_p1")
	require.NotEqual(t, uint64(1), target.LeaderStoreID)
	ops := check(tc.GetRegion(1))
	require.Len(t, ops, 1, "a target change must produce a transfer operator, not merge using stale isAffinity")
	require.Equal(t, operator.OpLeader|operator.OpAffinity, ops[0].Kind())
	region := tc.GetRegion(1)
	peer := region.GetStorePeer(target.LeaderStoreID)
	require.NotNil(t, peer)
	tc.PutRegion(region.Clone(core.WithLeader(peer)))
	check(tc.GetRegion(1))
	require.Equal(t, affinity.PhaseStable, manager.GetAffinityGroupState(target.ID).Phase)
}

func TestAffinityBalancePreparingScaleOut(t *testing.T) {
	tc, checker := newAffinityBalanceTestCluster(t, 4)
	tc.SetEnableUseJointConsensus(false) // ApplyOperator supports non-joint steps.
	manager := tc.GetAffinityManager()
	tc.PutStore(tc.GetStore(4).Clone(core.SetNodeState(metapb.NodeState_Preparing), core.SetRegionCount(0)))
	ids := []string{"_tidb_pt_100_p1", "_tidb_pt_100_p2", "_tidb_pt_100_p3"}
	keys := []string{"a", "b", "c", "d"}
	for i, id := range ids {
		leader := uint64(i + 1)
		createBalanceGroup(t, manager, id, keys[i], keys[i+1], leader, []uint64{1, 2, 3}, affinity.BalancePolicyAuto)
		addBalanceRegion(t, tc, manager, leader, keys[i], keys[i+1], leader, 1, 2, 3)
	}
	score := func() affinity.BalanceScore {
		groups := make([]*affinity.Group, 0, len(ids))
		for _, id := range ids {
			group := manager.GetAffinityGroupState(id).Group
			groups = append(groups, &group)
		}
		scorer, err := affinity.NewBalanceScorer(groups)
		require.NoError(t, err)
		return scorer.Score()
	}
	require.Equal(t, affinity.BalanceScore{Leader: 3, Voter: 27}, score())
	moves := 0
	for range 4 {
		before := score()
		if runAffinityBalanceTask(t, t.Context(), checker) {
			break
		}
		require.Negative(t, score().Compare(before), "every committed target must strictly improve the score")
		for i, id := range ids {
			region := tc.GetRegion(uint64(i + 1))
			group := manager.GetAffinityGroupState(id)
			require.True(t, group.AffinitySchedulingAllowed)
			op := checker.createAffinityOperator(region, group)
			if op != nil {
				require.NotZero(t, op.Kind()&operator.OpAffinity)
				operator.ApplyOperator(tc, op)
				moves++
			}
			region = tc.GetRegion(region.GetID())
			manager.ObserveBalanceRegion(region)
			_, aligned := manager.GetAndCacheRegionAffinityGroupState(region)
			require.True(t, aligned)
			require.Equal(t, affinity.PhaseStable, manager.GetAffinityGroupState(id).Phase)
		}
	}
	require.Positive(t, moves, "the new store must receive real peer-move operators")
	require.Equal(t, affinity.BalanceScore{Leader: 3, Voter: 21}, score())
	require.True(t, tc.GetStore(4).IsPreparing(), "convergence must not depend on a Serving transition")
	require.Positive(t, tc.GetStore(4).GetRegionCount())
}

func TestControllerObservesAffinityRegionBeforeRuleRepair(t *testing.T) {
	tc, _ := newAffinityBalanceTestCluster(t, 3)
	manager := tc.GetAffinityManager()
	createBalanceGroup(t, manager, "_tidb_pt_100_p0", "a", "b", 1, []uint64{1, 2, 3}, affinity.BalancePolicyAuto)
	region := addBalanceRegion(t, tc, manager, 1, "a", "b", 1, 1, 2, 3)
	controller := NewController(t.Context(), tc, tc.GetCheckerConfig(), operator.NewController(t.Context(), tc.GetBasicCluster(), tc.GetSharedConfig(), nil))
	version, exists := manager.GetBalanceVersion("_tidb_pt_100")
	require.True(t, exists)
	heartbeat := region.Clone(core.SetPeers([]*metapb.Peer{region.GetStorePeer(1), region.GetStorePeer(2)}))
	tc.PutRegion(heartbeat)

	ops := controller.CheckRegion(heartbeat)

	require.Len(t, ops, 1)
	require.Equal(t, operator.OpReplica, ops[0].Kind()&operator.OpReplica)
	current, exists := manager.GetBalanceVersion("_tidb_pt_100")
	require.True(t, exists)
	require.Greater(t, current.Table, version.Table)
}

func TestAffinityBalanceRequiresCompleteRangeCoverage(t *testing.T) {
	tc, checker := newAffinityBalanceTestCluster(t, 4)
	manager := tc.GetAffinityManager()
	createBalanceGroup(t, manager, "_tidb_pt_100_p0", "a", "c", 1, []uint64{1, 2, 3}, affinity.BalancePolicyAuto)
	createBalanceGroup(t, manager, "_tidb_pt_100_p1", "d", "e", 1, []uint64{1, 2, 4}, affinity.BalancePolicyFixed)
	addBalanceRegion(t, tc, manager, 1, "a", "b", 1, 1, 2, 3)
	addBalanceRegion(t, tc, manager, 3, "d", "e", 1, 1, 2, 4)

	require.False(t, runAffinityBalanceTask(t, t.Context(), checker))
	require.Equal(t, uint64(1), manager.GetAffinityGroupState("_tidb_pt_100_p0").LeaderStoreID,
		"a gap in the group's key range must prevent a target update")

	addBalanceRegion(t, tc, manager, 2, "b", "c", 1, 1, 2, 3)
	require.False(t, runAffinityBalanceTask(t, t.Context(), checker))
	require.Equal(t, uint64(2), manager.GetAffinityGroupState("_tidb_pt_100_p0").LeaderStoreID,
		"the improvement may commit once the complete range is covered")
}

func TestAffinityBalancePendingRegionAndOperatorBlockOtherGroup(t *testing.T) {
	for _, blocker := range []string{"pending-region", "waiting-operator"} {
		t.Run(blocker, func(t *testing.T) {
			tc, checker := newAffinityBalanceTestCluster(t, 4)
			manager := tc.GetAffinityManager()
			createBalanceGroup(t, manager, "_tidb_pt_100_p0", "a", "b", 1, []uint64{1, 2, 3}, affinity.BalancePolicyAuto)
			createBalanceGroup(t, manager, "_tidb_pt_100_p1", "b", "c", 1, []uint64{1, 2, 4}, affinity.BalancePolicyAuto)
			addBalanceRegion(t, tc, manager, 1, "a", "b", 1, 1, 2, 3)
			region := addBalanceRegion(t, tc, manager, 2, "b", "c", 1, 1, 2, 4)
			if blocker == "pending-region" {
				region = region.Clone(core.WithPendingPeers([]*metapb.Peer{region.GetStorePeer(2)}))
				tc.PutRegion(region)
				manager.GetAndCacheRegionAffinityGroupState(tc.GetRegion(region.GetID()))
			} else {
				controller := operator.NewController(t.Context(), tc.GetBasicCluster(), tc.GetSharedConfig(), nil)
				checker.opController = controller
				op, err := operator.CreateTransferLeaderOperator("affinity-balance-test", tc, region, 2,
					[]uint64{1, 2, 4}, operator.OpAffinity)
				require.NoError(t, err)
				require.Equal(t, 1, controller.AddWaitingOperator(op))
			}

			require.False(t, runAffinityBalanceTask(t, t.Context(), checker))
			require.Equal(t, uint64(1), manager.GetAffinityGroupState("_tidb_pt_100_p0").LeaderStoreID,
				"an unready Region in another group must block committing this group's candidate")
		})
	}
}

func TestAffinityBalanceSynchronousValidationStatuses(t *testing.T) {
	tc, checker := newAffinityBalanceTestCluster(t, 4)
	manager := tc.GetAffinityManager()
	createBalanceGroup(t, manager, "validation", "a", "b", 1, []uint64{1, 2, 3}, affinity.BalancePolicyAuto)
	region := addBalanceRegion(t, tc, manager, 1, "a", "b", 1, 1, 2, 3)
	actual := manager.GetAffinityGroupState("validation")
	target := actual.Group
	target.LeaderStoreID = 2
	target.VoterStoreIDs = []uint64{1, 2, 4}

	require.Equal(t, balanceAllowed, checker.validateBalanceRegion(actual, &target, region, nil))
	require.Equal(t, balanceUnready, checker.validateBalanceRegion(actual, &target, region,
		map[uint64]struct{}{region.GetID(): {}}), "a waiting operator must keep the Region out of a group commit")
	invalid := target
	invalid.LeaderStoreID = 4
	invalid.VoterStoreIDs = []uint64{1, 2, 3}
	require.Equal(t, balanceRejected, checker.validateBalanceRegion(actual, &invalid, region, nil))

	actualGroup := actual.Group
	require.Equal(t, balanceAllowed, checker.balanceTargetExecutionStatus(&actualGroup, &target))
	tc.SetStoreBusy(3, true)
	require.Equal(t, balanceTemporarilyBlocked, checker.balanceTargetExecutionStatus(&actualGroup, &target),
		"a busy source store must temporarily block the move")
	tc.SetStoreBusy(3, false)
	tc.SetStoreBusy(2, true)
	require.Equal(t, balanceTemporarilyBlocked, checker.balanceTargetExecutionStatus(&actualGroup, &target))
	tc.SetStoreBusy(2, false)
	tc.SetStoreOffline(4)
	require.Equal(t, balanceRejected, checker.balanceTargetExecutionStatus(&actualGroup, &target))
}

func TestAffinityBalanceRejectsIsolationRegression(t *testing.T) {
	opt := newAffinityTestOptions()
	opt.SetLocationLabels([]string{"zone", "host"})
	tc := mockcluster.NewCluster(t.Context(), opt)
	stores := map[uint64]map[string]string{
		1: {"zone": "z1", "host": "h1"},
		2: {"zone": "z1", "host": "h2"},
		3: {"zone": "z1", "host": "h3"},
		4: {"zone": "z2", "host": "h1"},
		5: {"zone": "z3", "host": "h1"},
	}
	for id, labels := range stores {
		tc.AddLabelsStore(id, 10, labels)
		tc.SetStoreUp(id)
	}
	rule := tc.GetRuleManager().GetRule(placement.DefaultGroupID, placement.DefaultRuleID)
	require.NotNil(t, rule)
	rule.LocationLabels = []string{"zone", "host"}
	rule.IsolationLevel = "zone"
	require.NoError(t, tc.GetRuleManager().SetRule(rule))

	checker := newTestAffinityChecker(t.Context(), tc, opt)
	manager := tc.GetAffinityManager()
	createBalanceGroup(t, manager, "isolation", "a", "b", 1, []uint64{1, 4, 5}, affinity.BalancePolicyAuto)
	region := addBalanceRegion(t, tc, manager, 1, "a", "b", 1, 1, 4, 5)
	actual := manager.GetAffinityGroupState("isolation")
	target := actual.Group
	target.VoterStoreIDs = []uint64{1, 3, 5}

	require.Equal(t, balanceAllowed, checker.validateBalanceRegion(actual, nil, region, nil))
	require.Equal(t, balanceRejected, checker.validateBalanceRegion(actual, &target, region, nil),
		"a target with worse zone isolation must be rejected")
}

func TestAffinityBalanceRejectsLearnerStoreAsVoter(t *testing.T) {
	tc, checker := newAffinityBalanceTestCluster(t, 5)
	require.NoError(t, tc.GetRuleManager().SetRule(&placement.Rule{
		GroupID: placement.DefaultGroupID, ID: "learner", Role: placement.Learner, Count: 1,
	}))
	manager := tc.GetAffinityManager()
	createBalanceGroup(t, manager, "_tidb_pt_100_p0", "a", "b", 1, []uint64{1, 2, 3}, affinity.BalancePolicyAuto)
	createBalanceGroup(t, manager, "_tidb_pt_100_p1", "b", "c", 2, []uint64{1, 2, 3}, affinity.BalancePolicyFixed)
	region := addBalanceRegion(t, tc, manager, 1, "a", "b", 1, 1, 2, 3)
	region = region.Clone(core.SetPeers(append(slices.Clone(region.GetPeers()), &metapb.Peer{
		Id: 4, StoreId: 4, Role: metapb.PeerRole_Learner,
	})))
	tc.PutRegion(region)
	actual, aligned := manager.GetAndCacheRegionAffinityGroupState(region)
	require.True(t, aligned)
	require.True(t, tc.GetRuleManager().FitRegionWithoutCache(tc, region).IsSatisfied())
	target := actual.Group
	target.VoterStoreIDs = []uint64{1, 2, 4}
	require.Nil(t, cloneRegionWithReplacePeerStores(region, target.LeaderStoreID, target.VoterStoreIDs...))
	require.Equal(t, balanceRejected, checker.validateBalanceRegion(actual, &target, region, nil))

	require.False(t, runAffinityBalanceTask(t, t.Context(), checker))
	targetState := manager.GetAffinityGroupState(actual.ID)
	require.NotContains(t, targetState.VoterStoreIDs, uint64(4), "a preserved learner must not become a target voter")
	require.Contains(t, targetState.VoterStoreIDs, uint64(5), "another legal destination remains usable")
	require.NotNil(t, checker.createAffinityOperator(region, targetState))
}

func setupLeaderLimitBalanceTable(t *testing.T, tc *mockcluster.Cluster) *affinity.Manager {
	t.Helper()
	manager := tc.GetAffinityManager()
	createBalanceGroup(t, manager, "_tidb_pt_100_p0", "a", "b", 1, []uint64{1, 2, 3}, affinity.BalancePolicyAuto)
	createBalanceGroup(t, manager, "_tidb_pt_100_p1", "b", "c", 1, []uint64{1, 4, 5}, affinity.BalancePolicyFixed)
	createBalanceGroup(t, manager, "_tidb_pt_100_p2", "c", "d", 4, []uint64{4, 5, 6}, affinity.BalancePolicyFixed)
	addBalanceRegion(t, tc, manager, 1, "a", "b", 1, 1, 2, 3)
	addBalanceRegion(t, tc, manager, 2, "b", "c", 1, 1, 4, 5)
	addBalanceRegion(t, tc, manager, 3, "c", "d", 4, 4, 5, 6)
	return manager
}

func exhaustLeaderTransferIn(t *testing.T, tc *mockcluster.Cluster, id uint64) {
	t.Helper()
	tc.SetStoreLimit(id, storelimit.TransferLeaderIn, 0.00006)
	tc.ResetStoreLimit(id, storelimit.TransferLeaderIn, 0.000001)
	require.True(t, tc.GetStore(id).GetStoreLimit().Take(
		storelimit.RegionInfluence[storelimit.TransferLeaderIn], storelimit.TransferLeaderIn, constant.Medium))
	require.False(t, tc.GetStore(id).IsAvailable(storelimit.TransferLeaderIn, constant.Medium))
}

func restoreLeaderTransferIn(tc *mockcluster.Cluster, id uint64) {
	tc.SetStoreLimit(id, storelimit.TransferLeaderIn, storelimit.Unlimited)
	tc.ResetStoreLimit(id, storelimit.TransferLeaderIn, storelimit.Unlimited/time.Minute.Seconds())
}

func TestAffinityBalanceTemporaryLeaderLimitUsesNextCandidate(t *testing.T) {
	tc, checker := newAffinityBalanceTestCluster(t, 6)
	manager := setupLeaderLimitBalanceTable(t, tc)
	exhaustLeaderTransferIn(t, tc, 2)

	require.False(t, runAffinityBalanceTask(t, t.Context(), checker))
	require.Equal(t, uint64(3), manager.GetAffinityGroupState("_tidb_pt_100_p0").LeaderStoreID,
		"a temporarily rate-limited candidate must not hide another legal improvement")
}

func TestAffinityBalanceAllLeaderLimitsRetryBeforeVoterMoves(t *testing.T) {
	tc, checker := newAffinityBalanceTestCluster(t, 6)
	manager := setupLeaderLimitBalanceTable(t, tc)
	exhaustLeaderTransferIn(t, tc, 2)
	exhaustLeaderTransferIn(t, tc, 3)
	stable, err := checker.balancer.step(t.Context(), "_tidb_pt_100")
	require.NoError(t, err)
	require.False(t, stable)
	require.Equal(t, []uint64{1, 2, 3}, manager.GetAffinityGroupState("_tidb_pt_100_p0").VoterStoreIDs,
		"temporarily blocked leader candidates must not fall through to voter replacements")
	require.Equal(t, uint64(1), manager.GetAffinityGroupState("_tidb_pt_100_p0").LeaderStoreID)

	restoreLeaderTransferIn(tc, 2)
	restoreLeaderTransferIn(tc, 3)
	require.False(t, runAffinityBalanceTask(t, t.Context(), checker))
	require.Equal(t, uint64(2), manager.GetAffinityGroupState("_tidb_pt_100_p0").LeaderStoreID,
		"the next attempt can use the recovered candidate")
}

func TestAffinityBalanceBusyVoterDestinationRetries(t *testing.T) {
	tc, checker := newAffinityBalanceTestCluster(t, 4)
	tc.PutStore(tc.GetStore(4).Clone(core.SetNodeState(metapb.NodeState_Preparing)))
	manager := tc.GetAffinityManager()
	createBalanceGroup(t, manager, "_tidb_pt_100_p0", "a", "b", 1, []uint64{1, 2, 3}, affinity.BalancePolicyAuto)
	createBalanceGroup(t, manager, "_tidb_pt_100_p1", "b", "c", 2, []uint64{1, 2, 3}, affinity.BalancePolicyFixed)
	addBalanceRegion(t, tc, manager, 1, "a", "b", 1, 1, 2, 3)
	addBalanceRegion(t, tc, manager, 2, "b", "c", 2, 1, 2, 3)
	tc.SetStoreBusy(4, true)
	require.False(t, runAffinityBalanceTask(t, t.Context(), checker))
	require.NotContains(t, manager.GetAffinityGroupState("_tidb_pt_100_p0").VoterStoreIDs, uint64(4))

	tc.SetStoreBusy(4, false)
	require.False(t, runAffinityBalanceTask(t, t.Context(), checker))
	require.Contains(t, manager.GetAffinityGroupState("_tidb_pt_100_p0").VoterStoreIDs, uint64(4))
}

func TestAffinityBalanceRecoveredDestinationInvalidatesStable(t *testing.T) {
	tc, checker := newAffinityBalanceTestCluster(t, 4)
	tc.SetMaxStoreDownTime(time.Minute)
	tc.SetStoreLastHeartbeatInterval(4, 2*time.Minute)
	require.False(t, tc.GetStore(4).IsUnhealthy(), "the configured down threshold is below the fixed unhealthy threshold")
	manager := tc.GetAffinityManager()
	ids := []string{"_tidb_pt_100_p1", "_tidb_pt_100_p2", "_tidb_pt_100_p3"}
	keys := []string{"a", "b", "c", "d"}
	for i, id := range ids {
		leader := uint64(i + 1)
		createBalanceGroup(t, manager, id, keys[i], keys[i+1], leader, []uint64{1, 2, 3}, affinity.BalancePolicyAuto)
		addBalanceRegion(t, tc, manager, leader, keys[i], keys[i+1], leader, 1, 2, 3)
	}
	beforeVersion, exists := manager.GetBalanceVersion(ids[0])
	require.True(t, exists)

	require.True(t, runAffinityBalanceTask(t, t.Context(), checker))
	balancer := &affinityBalancer{checker: checker, tables: map[string]*affinityBalanceTask{"_tidb_pt_100": {}}, queue: make(chan string, 1)}
	balancer.enqueue(ids[0])
	require.Len(t, balancer.queue, 1, "periodic retry should revisit a completed table")

	tc.SetStoreUp(4)
	// Snapshot creation samples the same store fingerprint as the availability loop.
	recovered := manager.GetBalanceSnapshot(ids[0])
	require.Greater(t, recovered.Version.Stores, beforeVersion.Stores)
	require.Equal(t, beforeVersion.Table, recovered.Version.Table, "recovery of an unused destination does not change the table's Regions")
	balancer.enqueue(ids[0])
	require.Len(t, balancer.queue, 1, "destination recovery must wake the stable table")
	<-balancer.queue
	require.False(t, runAffinityBalanceTask(t, t.Context(), checker))
	require.Contains(t, manager.GetAffinityGroupState(ids[0]).VoterStoreIDs, uint64(4))
}

func TestAffinityBalanceCompletesSlowScanInOneCall(t *testing.T) {
	tc, checker := newAffinityBalanceTestCluster(t, 4)
	manager := tc.GetAffinityManager()
	createBalanceGroup(t, manager, "_tidb_pt_100_p0", "a", "c", 1, []uint64{1, 2, 3}, affinity.BalancePolicyAuto)
	createBalanceGroup(t, manager, "_tidb_pt_100_p1", "d", "e", 1, []uint64{1, 2, 4}, affinity.BalancePolicyFixed)
	addBalanceRegion(t, tc, manager, 1, "a", "b", 1, 1, 2, 3)
	addBalanceRegion(t, tc, manager, 2, "b", "c", 1, 1, 2, 3)
	addBalanceRegion(t, tc, manager, 3, "d", "e", 1, 1, 2, 4)

	// Each Region alone exceeds the former 10ms time slice.
	scanCluster := &affinityBalanceScanCluster{CheckerCluster: tc, delay: 15 * time.Millisecond}
	checker.cluster = scanCluster
	require.False(t, runAffinityBalanceTask(t, t.Context(), checker))
	require.Equal(t, [][]byte{[]byte("a"), []byte("b"), []byte("a"), []byte("b")}, scanCluster.starts,
		"current and target scans must each visit every Region once")
	require.Equal(t, uint64(2), manager.GetAffinityGroupState("_tidb_pt_100_p0").LeaderStoreID)
}

func TestAffinityBalanceCancellationRestartsFromFreshSnapshot(t *testing.T) {
	tc, checker := newAffinityBalanceTestCluster(t, 4)
	manager := tc.GetAffinityManager()
	createBalanceGroup(t, manager, "_tidb_pt_100_p0", "a", "c", 1, []uint64{1, 2, 3}, affinity.BalancePolicyAuto)
	createBalanceGroup(t, manager, "_tidb_pt_100_p1", "d", "e", 1, []uint64{1, 2, 4}, affinity.BalancePolicyFixed)
	addBalanceRegion(t, tc, manager, 1, "a", "b", 1, 1, 2, 3)
	addBalanceRegion(t, tc, manager, 2, "b", "c", 1, 1, 2, 3)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	scanCluster := &affinityBalanceScanCluster{CheckerCluster: tc, beforeScan: cancel}
	checker.cluster = scanCluster
	stable, err := checker.balancer.step(ctx, "_tidb_pt_100")
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, stable)
	require.Equal(t, uint64(1), manager.GetAffinityGroupState("_tidb_pt_100_p0").LeaderStoreID)

	// A cancelled attempt retains neither the first Region nor the old policy.
	scanCluster.beforeScan, scanCluster.starts = nil, nil
	_, err = manager.UpdateAffinityGroupPolicy("_tidb_pt_100_p0", affinity.BalancePolicyFixed)
	require.NoError(t, err)
	require.True(t, runAffinityBalanceTask(t, t.Context(), checker))
	require.Empty(t, scanCluster.starts)
	_, err = manager.UpdateAffinityGroupPolicy("_tidb_pt_100_p0", affinity.BalancePolicyAuto)
	require.NoError(t, err)
	require.False(t, runAffinityBalanceTask(t, t.Context(), checker))
	require.Equal(t, []byte("a"), scanCluster.starts[0])
	require.Equal(t, uint64(2), manager.GetAffinityGroupState("_tidb_pt_100_p0").LeaderStoreID)
}

func TestAffinityBalanceFinalReviewRejectsChangedRegion(t *testing.T) {
	tc, checker := newAffinityBalanceTestCluster(t, 3)
	manager := tc.GetAffinityManager()
	createBalanceGroup(t, manager, "_tidb_pt_100_p0", "a", "b", 1, []uint64{1, 2, 3}, affinity.BalancePolicyAuto)
	createBalanceGroup(t, manager, "_tidb_pt_100_p1", "b", "c", 1, []uint64{1, 2, 3}, affinity.BalancePolicyAuto)
	addBalanceRegion(t, tc, manager, 1, "a", "b", 1, 1, 2, 3)
	other := addBalanceRegion(t, tc, manager, 2, "b", "c", 1, 1, 2, 3)
	checker.cluster = &affinityBalanceScanCluster{CheckerCluster: tc, beforeFinal: func() {
		// Simulate a heartbeat changing another group after candidate validation.
		tc.PutRegion(other.Clone(core.WithLeader(other.GetStorePeer(2))))
	}}
	require.False(t, runAffinityBalanceTask(t, t.Context(), checker))
	require.Equal(t, uint64(1), manager.GetAffinityGroupState("_tidb_pt_100_p0").LeaderStoreID,
		"the final whole-table review must reject changed Regions outside the selected group")
}

func TestAffinityBalanceFixedGroupsSkipRegionScan(t *testing.T) {
	tc, checker := newAffinityBalanceTestCluster(t, 3)
	manager := tc.GetAffinityManager()
	createBalanceGroup(t, manager, "_tidb_pt_100_p0", "a", "b", 1, []uint64{1, 2, 3}, affinity.BalancePolicyFixed)
	createBalanceGroup(t, manager, "_tidb_pt_100_p1", "b", "c", 1, []uint64{1, 2, 3}, affinity.BalancePolicyFixed)
	scanCluster := &affinityBalanceScanCluster{CheckerCluster: tc}
	checker.cluster = scanCluster
	require.True(t, runAffinityBalanceTask(t, t.Context(), checker))
	require.Empty(t, scanCluster.starts, "fixed groups need no Region candidate scan")

	require.NoError(t, tc.GetRegionLabeler().SetLabelRule(&labeler.LabelRule{
		ID: "unrelated-stable-input", Index: 2,
		Labels:   []labeler.RegionLabel{{Key: "balance-test", Value: "unrelated"}},
		RuleType: labeler.KeyRange, Data: labeler.MakeKeyRanges("79", "7a"),
	}))
	require.True(t, runAffinityBalanceTask(t, t.Context(), checker))
	require.Empty(t, scanCluster.starts)
	require.Zero(t, scanCluster.labelReads, "snapshot creation must not scan global label rules")
}

func TestAffinityBalanceRoleReassignmentCanReplaceOldLeader(t *testing.T) {
	opt := newAffinityTestOptions()
	opt.SetLabelProperty("reject-leader", "reject", "yes")
	tc := mockcluster.NewCluster(t.Context(), opt)
	tc.SetMaxReplicasWithLabel(true, 3)
	for id := uint64(1); id <= 6; id++ {
		tc.AddRegionStore(id, 10)
		tc.SetStoreUp(id)
	}
	for _, id := range []uint64{1, 2, 5} {
		tc.SetStoreLabel(id, map[string]string{"zone": "a"})
	}
	for _, id := range []uint64{3, 4, 6} {
		tc.SetStoreLabel(id, map[string]string{"zone": "b"})
	}
	require.NoError(t, tc.GetRuleManager().SetRule(&placement.Rule{
		GroupID: placement.DefaultGroupID, ID: "leader-a", Index: 1, Override: true, Role: placement.Leader, Count: 1,
		LabelConstraints: []placement.LabelConstraint{{Key: "zone", Op: placement.In, Values: []string{"a"}}},
	}))
	require.NoError(t, tc.GetRuleManager().SetRule(&placement.Rule{
		GroupID: placement.DefaultGroupID, ID: "followers", Index: 2, Role: placement.Follower, Count: 2,
	}))
	checker := newTestAffinityChecker(t.Context(), tc, opt)
	manager := tc.GetAffinityManager()
	createBalanceGroup(t, manager, "_tidb_pt_100_p0", "a", "b", 1, []uint64{1, 2, 3}, affinity.BalancePolicyAuto)
	createBalanceGroup(t, manager, "_tidb_pt_100_p1", "b", "c", 5, []uint64{1, 5, 6}, affinity.BalancePolicyFixed)
	addBalanceRegion(t, tc, manager, 1, "a", "b", 1, 1, 2, 3)
	addBalanceRegion(t, tc, manager, 2, "b", "c", 5, 1, 5, 6)

	require.False(t, runAffinityBalanceTask(t, t.Context(), checker))
	group := manager.GetAffinityGroupState("_tidb_pt_100_p0")
	require.Equal(t, uint64(2), group.LeaderStoreID)
	require.ElementsMatch(t, []uint64{2, 3, 4}, group.VoterStoreIDs,
		"the improving role move may remove the old leader once the new leader remains a voter")
}

func TestBalanceScoreLowerBoundIncludesZeroCountStores(t *testing.T) {
	groups := []*affinity.GroupState{
		{Group: affinity.Group{ID: "g1", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 3}}},
		{Group: affinity.Group{ID: "g2", LeaderStoreID: 2, VoterStoreIDs: []uint64{2, 4}}},
	}
	scorer, err := affinity.NewBalanceScorer([]*affinity.Group{&groups[0].Group, &groups[1].Group})
	require.NoError(t, err)
	snapshot := &affinity.BalanceSnapshot{Stores: []uint64{1, 2, 3, 4}, Groups: groups}
	require.True(t, balanceScoreAtTheoreticalLowerBound(snapshot, scorer))

	groups[1].VoterStoreIDs = []uint64{1, 2}
	scorer, err = affinity.NewBalanceScorer([]*affinity.Group{&groups[0].Group, &groups[1].Group})
	require.NoError(t, err)
	require.False(t, balanceScoreAtTheoreticalLowerBound(snapshot, scorer), "leader balance alone must not skip voter search")
}

func TestBalanceScoreLowerBoundIncludesFixedTargetsOutsideCandidates(t *testing.T) {
	leaders := []uint64{1, 1, 2, 3, 1, 4, 5, 5}
	groups := make([]*affinity.GroupState, len(leaders))
	inputs := make([]*affinity.Group, len(leaders))
	for i, leader := range leaders {
		voters := []uint64{1, 2, 3}
		policy := affinity.BalancePolicyAuto
		if i >= 4 {
			voters = []uint64{1, 3, 4}
		}
		if i >= 6 {
			voters, policy = []uint64{1, 2, 5}, affinity.BalancePolicyFixed
		}
		groups[i] = &affinity.GroupState{Group: affinity.Group{ID: fmt.Sprintf("g%d", i), LeaderStoreID: leader, VoterStoreIDs: voters, BalancePolicy: policy}}
		inputs[i] = &groups[i].Group
	}
	scorer, err := affinity.NewBalanceScorer(inputs)
	require.NoError(t, err)
	require.False(t, balanceScoreAtTheoreticalLowerBound(&affinity.BalanceSnapshot{
		Stores: []uint64{1, 2, 3, 4}, Groups: groups,
	}, scorer))
}

func TestLeaderCandidatesDeferStoreSortingUntilVoterPhase(t *testing.T) {
	group := &affinity.Group{ID: "g", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2}}
	scorer, err := affinity.NewBalanceScorer([]*affinity.Group{group})
	require.NoError(t, err)
	snapshot := &affinity.BalanceSnapshot{Stores: []uint64{3, 2, 1}, Groups: []*affinity.GroupState{{Group: *group}}}
	require.Nil(t, newBalanceCandidates(snapshot, scorer, false).stores)
	require.NotNil(t, newBalanceCandidates(snapshot, scorer, true).stores)
}
