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

package affinity

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

func prepareBalanceCommit(t *testing.T, manager *Manager, groupID string) *BalanceSnapshot {
	t.Helper()
	snapshot := manager.GetBalanceSnapshot(groupID)
	require.NotNil(t, snapshot)
	require.NoError(t, manager.PrepareBalanceCommit(t.Context(), snapshot))
	return snapshot
}

func balanceCommitTarget(snapshot *BalanceSnapshot, groupID string, leaderStoreID uint64) *Group {
	for _, group := range snapshot.Groups {
		if group.ID == groupID {
			target := group.Group
			target.LeaderStoreID = leaderStoreID
			return &target
		}
	}
	return nil
}

func TestBalanceCommitPreparingStoreRemainsAvailable(t *testing.T) {
	manager := newMetadataTestManager(t)
	id := seedMetadataTestGroups(t, manager, []uint64{1})[0]
	stores := manager.storeSetInformer.(*core.StoresInfo)
	stores.PutStore(stores.GetStore(4).Clone(core.SetNodeState(metapb.NodeState_Preparing)))
	snapshot := prepareBalanceCommit(t, manager, id)
	target := balanceCommitTarget(snapshot, id, 1)
	target.VoterStoreIDs = []uint64{1, 2, 4}

	committed, err := manager.CommitBalanceTarget(t.Context(), snapshot, target)
	require.NoError(t, err)
	require.True(t, committed)
	require.ElementsMatch(t, target.VoterStoreIDs, loadStoredGroup(t, manager, id).VoterStoreIDs)

	// A successful commit alone is insufficient: the periodic availability
	// check must also let the checker execute this target while 4 is Preparing.
	manager.checkGroupsAvailability()
	group := manager.GetAffinityGroupState(id)
	require.True(t, stores.GetStore(4).IsPreparing())
	require.True(t, group.AffinitySchedulingAllowed)
	require.Equal(t, PhasePreparing, group.Phase)
}

func TestBalanceCommitPreparingStoreRejectionRetries(t *testing.T) {
	for _, tc := range []struct {
		reason string
		option core.StoreCreateOption
	}{
		{"disconnected", core.SetLastHeartbeatTS(time.Now().Add(-2 * time.Minute))},
		{"down", core.SetLastHeartbeatTS(time.Now().Add(-2 * time.Hour))},
		{"low-space", core.SetStoreStats(&pdpb.StoreStats{Capacity: 100, Available: 1, UsedSize: 99})},
		{"busy", core.SetStoreStats(&pdpb.StoreStats{IsBusy: true})},
		{"leader-evicted", core.PauseLeaderTransfer(constant.In)},
		{"removing", core.SetStoreState(metapb.StoreState_Offline, false)},
		{"removed", core.SetStoreState(metapb.StoreState_Tombstone)},
	} {
		t.Run(tc.reason, func(t *testing.T) {
			manager := newMetadataTestManager(t)
			id := seedMetadataTestGroups(t, manager, []uint64{1})[0]
			stores := manager.storeSetInformer.(*core.StoresInfo)
			healthy := stores.GetStore(4).Clone(core.SetNodeState(metapb.NodeState_Preparing))
			stores.PutStore(healthy)
			snapshot := prepareBalanceCommit(t, manager, id)
			target := balanceCommitTarget(snapshot, id, 4)
			target.VoterStoreIDs = []uint64{1, 2, 4}
			// The store changes after validation, before the final commit check.
			stores.PutStore(healthy.Clone(tc.option))
			committed, err := manager.CommitBalanceTarget(t.Context(), snapshot, target)
			require.False(t, committed)
			require.ErrorContains(t, err, id)
			require.ErrorContains(t, err, "store 4 is "+tc.reason)
			require.NotContains(t, loadStoredGroup(t, manager, id).VoterStoreIDs, uint64(4))

			stores.PutStore(healthy.Clone(core.SetStoreStats(&pdpb.StoreStats{})))
			committed, err = manager.CommitBalanceTarget(t.Context(), snapshot, target)
			require.NoError(t, err)
			require.True(t, committed, "a rejected commit must be retryable after recovery")
		})
	}
}

func TestBalanceCommitRejectsNewSpecialUseVoter(t *testing.T) {
	manager := newMetadataTestManager(t)
	id := seedMetadataTestGroups(t, manager, []uint64{1})[0]
	stores := manager.storeSetInformer.(*core.StoresInfo)
	// Store 2 is already an existing voter. Its special-use label must not
	// invalidate a legacy group; store 4 is introduced by this target and must
	// be rejected at the commit boundary.
	stores.PutStore(stores.GetStore(2).Clone(core.SetStoreLabels([]*metapb.StoreLabel{{Key: "specialUse", Value: "reserved"}})))
	stores.PutStore(stores.GetStore(4).Clone(core.SetStoreLabels([]*metapb.StoreLabel{{Key: "specialUse", Value: "reserved"}})))
	snapshot := prepareBalanceCommit(t, manager, id)
	target := balanceCommitTarget(snapshot, id, 1)
	target.VoterStoreIDs = []uint64{1, 2, 3, 4}

	committed, err := manager.CommitBalanceTarget(t.Context(), snapshot, target)
	require.False(t, committed)
	require.ErrorContains(t, err, "store 4 has special use \"reserved\"")
	require.NotContains(t, loadStoredGroup(t, manager, id).VoterStoreIDs, uint64(4))

	// Once the newly added store is usable, the same snapshot can retry even
	// though the pre-existing voter remains special-use.
	stores.PutStore(stores.GetStore(4).Clone(core.SetStoreLabels(nil)))
	snapshot = prepareBalanceCommit(t, manager, id)
	target = balanceCommitTarget(snapshot, id, 1)
	target.VoterStoreIDs = []uint64{1, 2, 3, 4}
	committed, err = manager.CommitBalanceTarget(t.Context(), snapshot, target)
	require.NoError(t, err)
	require.True(t, committed)
	require.ElementsMatch(t, target.VoterStoreIDs, loadStoredGroup(t, manager, id).VoterStoreIDs)
}

type balanceConditionalStorage struct {
	storage.Storage
	expected clientv3.Cmp
	received [][]clientv3.Cmp
}

func (s *balanceConditionalStorage) RunInTxnWithConditions(ctx context.Context, conditions []clientv3.Cmp, f func(kv.Txn) error) error {
	s.received = append(s.received, slices.Clone(conditions))
	if len(conditions) != 1 ||
		string(conditions[0].KeyBytes()) != string(s.expected.KeyBytes()) ||
		conditions[0].Target != s.expected.Target || conditions[0].Result != s.expected.Result ||
		string(conditions[0].ValueBytes()) != string(s.expected.ValueBytes()) {
		return errs.ErrEtcdTxnConflict.FastGenByArgs()
	}
	value, err := s.Load(string(s.expected.KeyBytes()))
	if err != nil {
		return err
	}
	if value != string(s.expected.ValueBytes()) {
		return errs.ErrEtcdTxnConflict.FastGenByArgs()
	}
	return s.RunInTxn(ctx, f)
}

func TestPrepareBalanceCommitRejectsMissingPrimaryTerm(t *testing.T) {
	manager := newMetadataTestManager(t)
	ids := seedMetadataTestGroups(t, manager, []uint64{1})
	manager.SetAffinityWriteGuard(func() ([]clientv3.Cmp, bool) {
		return nil, false
	})
	snapshot := manager.GetBalanceSnapshot(ids[0])
	require.NotNil(t, snapshot)
	require.True(t, errs.ErrEtcdTxnConflict.Equal(manager.PrepareBalanceCommit(t.Context(), snapshot)))
}

func TestBalanceCommitRejectsRevokedPrimaryTerm(t *testing.T) {
	manager := newMetadataTestManager(t)
	ids := seedMetadataTestGroups(t, manager, []uint64{1})
	base := manager.storage.(storage.Storage)
	termKey, termValue := "affinity-test-primary", "primary-term-1"
	require.NoError(t, base.RunInTxn(t.Context(), func(txn kv.Txn) error {
		return txn.Save(termKey, termValue)
	}))
	expected := clientv3.Compare(clientv3.Value(termKey), "=", termValue)
	conditional := &balanceConditionalStorage{Storage: base, expected: expected}
	manager.storage = conditional
	manager.SetAffinityWriteGuard(func() ([]clientv3.Cmp, bool) {
		return []clientv3.Cmp{expected}, true
	})

	snapshot := manager.GetBalanceSnapshot(ids[0])
	require.NotNil(t, snapshot)
	require.NoError(t, manager.PrepareBalanceCommit(t.Context(), snapshot))
	require.Empty(t, conditional.received)

	require.NoError(t, base.RunInTxn(t.Context(), func(txn kv.Txn) error {
		return txn.Save(termKey, "primary-term-2")
	}))
	committed, err := manager.CommitBalanceTarget(t.Context(), snapshot, balanceCommitTarget(snapshot, ids[0], 2))
	require.NoError(t, err)
	require.False(t, committed)
	require.Len(t, conditional.received, 1)
	require.Equal(t, []clientv3.Cmp{expected}, conditional.received[0])
	require.Equal(t, uint64(1), manager.GetAffinityGroupState(ids[0]).LeaderStoreID)
	require.Equal(t, uint64(1), loadStoredGroup(t, manager, ids[0]).LeaderStoreID)
}

func TestPrepareBalanceCommitRejectsIndependentRangeWatcherUpdates(t *testing.T) {
	for _, tc := range []struct {
		name  string
		apply func(*testing.T, *Manager, string)
	}{
		{
			name: "labeler-ahead-of-range-cache",
			apply: func(t *testing.T, manager *Manager, groupID string) {
				rule := MakeLabelRule(GroupKeyRanges{GroupID: groupID, KeyRanges: []keyutil.KeyRange{{StartKey: []byte{9}, EndKey: []byte{10}}}})
				require.NoError(t, manager.regionLabeler.SetLabelRule(rule))
			},
		},
		{
			name: "range-cache-ahead-of-labeler",
			apply: func(t *testing.T, manager *Manager, groupID string) {
				rule := MakeLabelRule(GroupKeyRanges{GroupID: groupID, KeyRanges: []keyutil.KeyRange{{StartKey: []byte{9}, EndKey: []byte{10}}}})
				data, err := json.Marshal(rule)
				require.NoError(t, err)
				rule, err = labeler.NewLabelRuleFromJSON(data)
				require.NoError(t, err)
				require.NoError(t, manager.SyncKeyRangesFromEtcd(rule))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager := newMetadataTestManager(t)
			groupID := seedMetadataTestGroups(t, manager, []uint64{1})[0]
			tc.apply(t, manager, groupID)
			snapshot := manager.GetBalanceSnapshot(groupID)
			require.NotNil(t, snapshot)
			require.True(t, manager.BalanceSnapshotCurrent(snapshot))

			require.NoError(t, manager.PrepareBalanceCommit(t.Context(), snapshot))
		})
	}
}

func TestPrepareBalanceCommitIgnoresRangeOrder(t *testing.T) {
	manager := newMetadataTestManager(t)
	ranges := GroupKeyRanges{GroupID: "_tidb_pt_10_p1", KeyRanges: []keyutil.KeyRange{
		{StartKey: []byte{0}, EndKey: []byte{1}}, {StartKey: []byte{2}, EndKey: []byte{3}},
	}}
	require.NoError(t, manager.CreateAffinityGroups([]GroupKeyRanges{ranges}))
	reordered := cloneBalanceRanges(ranges)
	slices.Reverse(reordered.KeyRanges)
	require.NoError(t, manager.regionLabeler.SetLabelRule(MakeLabelRule(reordered)))

	snapshot := manager.GetBalanceSnapshot(ranges.GroupID)
	require.NotNil(t, snapshot)
	require.NoError(t, manager.PrepareBalanceCommit(t.Context(), snapshot))
	require.Equal(t, ranges, snapshot.Ranges[ranges.GroupID])
}

func TestPrepareBalanceCommitIgnoresSimilarTableKeys(t *testing.T) {
	manager := newMetadataTestManager(t)
	ids := seedMetadataTestGroups(t, manager, []uint64{1})
	otherID := "_tidb_pt_10_pother_p1"
	require.NoError(t, manager.CreateAffinityGroups([]GroupKeyRanges{{
		GroupID: otherID,
		KeyRanges: []keyutil.KeyRange{{
			StartKey: []byte{10},
			EndKey:   []byte{11},
		}},
	}}))

	snapshot := manager.GetBalanceSnapshot(ids[0])
	require.NotNil(t, snapshot)
	require.NoError(t, manager.PrepareBalanceCommit(t.Context(), snapshot))
}

func TestBalanceCommitDoesNotOverwriteLaterWatcherTarget(t *testing.T) {
	manager := newMetadataTestManager(t)
	ids := seedMetadataTestGroups(t, manager, []uint64{1, 1})
	snapshot := prepareBalanceCommit(t, manager, ids[0])
	base := manager.storage.(storage.Storage)
	later := manager.GetAffinityGroupState(ids[0]).Group
	later.LeaderStoreID = 3
	require.NoError(t, base.RunInTxn(t.Context(), func(txn kv.Txn) error {
		return base.SaveAffinityGroup(txn, later.ID, &later)
	}))
	manager.SyncGroupFromEtcd(&later)

	committed, err := manager.CommitBalanceTarget(t.Context(), snapshot, balanceCommitTarget(snapshot, ids[0], 2))
	require.ErrorIs(t, err, errs.ErrEtcdTxnConflict)
	require.False(t, committed)
	require.Equal(t, uint64(3), manager.GetAffinityGroupState(ids[0]).LeaderStoreID)
	require.Equal(t, uint64(3), loadStoredGroup(t, manager, ids[0]).LeaderStoreID)
}

func TestBalanceCommitRejectsChangedRangeLabel(t *testing.T) {
	manager := newMetadataTestManager(t)
	id := seedMetadataTestGroups(t, manager, []uint64{1})[0]
	snapshot := prepareBalanceCommit(t, manager, id)
	base := manager.storage.(storage.Storage)
	rule := MakeLabelRule(GroupKeyRanges{GroupID: id, KeyRanges: []keyutil.KeyRange{{StartKey: []byte{9}, EndKey: []byte{10}}}})
	require.NoError(t, base.RunInTxn(t.Context(), func(txn kv.Txn) error {
		return base.SaveRegionRule(txn, rule.ID, rule)
	}))

	committed, err := manager.CommitBalanceTarget(t.Context(), snapshot, balanceCommitTarget(snapshot, id, 2))
	require.False(t, committed)
	require.NoError(t, err)
	require.Equal(t, 1, manager.GetAffinityGroupState(id).RangeCount)
	require.Equal(t, []keyutil.KeyRange{{StartKey: []byte{0}, EndKey: []byte{1}}}, manager.keyRanges[id].KeyRanges)
	value, err := base.LoadRegionRule(rule.ID)
	require.NoError(t, err)
	remoteRule, err := labeler.NewLabelRuleFromJSON([]byte(value))
	require.NoError(t, err)
	require.NoError(t, manager.SyncKeyRangesFromEtcd(remoteRule))
	require.Equal(t, []keyutil.KeyRange{{StartKey: []byte{9}, EndKey: []byte{10}}}, manager.keyRanges[id].KeyRanges)
}

func TestBalanceCommitRejectsChangedStoreInputs(t *testing.T) {
	manager := newMetadataTestManager(t)
	id := seedMetadataTestGroups(t, manager, []uint64{1})[0]
	snapshot := prepareBalanceCommit(t, manager, id)
	stores := manager.storeSetInformer.(*core.StoresInfo)
	store := stores.GetStore(4)
	stores.PutStore(store.Clone(core.SetStoreLabels([]*metapb.StoreLabel{{Key: "zone", Value: "z1"}})))
	target := balanceCommitTarget(snapshot, id, 1)
	target.VoterStoreIDs = []uint64{1, 2, 4}

	committed, err := manager.CommitBalanceTarget(t.Context(), snapshot, target)
	require.False(t, committed)
	require.ErrorIs(t, err, errs.ErrEtcdTxnConflict)
	require.NotContains(t, loadStoredGroup(t, manager, id).VoterStoreIDs, uint64(4))
}

func TestBalanceCommitDoesNotPublishToRebuiltGroupWithSameID(t *testing.T) {
	manager := newMetadataTestManager(t)
	ids := seedMetadataTestGroups(t, manager, []uint64{1})
	snapshot := prepareBalanceCommit(t, manager, ids[0])
	base := manager.storage.(storage.Storage)
	rebuilt := manager.GetAffinityGroupState(ids[0]).Group
	rebuilt.CreateTimestamp++
	require.NoError(t, base.RunInTxn(t.Context(), func(txn kv.Txn) error {
		return base.SaveAffinityGroup(txn, rebuilt.ID, &rebuilt)
	}))
	manager.SyncGroupFromEtcd(&rebuilt)

	committed, err := manager.CommitBalanceTarget(t.Context(), snapshot, balanceCommitTarget(snapshot, ids[0], 2))
	require.ErrorIs(t, err, errs.ErrEtcdTxnConflict)
	require.False(t, committed)
	current := manager.GetAffinityGroupState(ids[0])
	require.Equal(t, rebuilt.CreateTimestamp, current.CreateTimestamp)
	require.Equal(t, uint64(1), current.LeaderStoreID)
	persisted := loadStoredGroup(t, manager, ids[0])
	require.Equal(t, rebuilt.CreateTimestamp, persisted.CreateTimestamp)
	require.Equal(t, uint64(1), persisted.LeaderStoreID)
}
