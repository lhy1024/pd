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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/kv"
)

func TestCustomGroupsKeepIndependentMetadataTables(t *testing.T) {
	manager := newMetadataTestManager(t)
	for _, id := range []string{"_tidb_pt_orders_pmanual", "_tidb_pt_orders_p1", "_tidb_pt_10_pmanual", "_tidb_pt_10_p1", "_tidb_pt_10_p2"} {
		require.NoError(t, manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: id}}))
	}
	require.Equal(t, "_tidb_pt_10", TableGroupPrefix("_tidb_pt_10_p1"))
	require.Len(t, manager.metadataTables["_tidb_pt_10"].groups, 2)
	for _, id := range []string{"_tidb_pt_orders_pmanual", "_tidb_pt_orders_p1", "_tidb_pt_10_pmanual"} {
		require.Equal(t, id, TableGroupPrefix(id))
		require.Len(t, manager.metadataTables[id].groups, 1)
		version := manager.metadataTables[id].version
		_, err := manager.UpdateAffinityGroupPolicy("_tidb_pt_10_p1", BalancePolicyFixed)
		require.NoError(t, err)
		require.Equal(t, version, manager.metadataTables[id].version)
	}
}

func TestUpdateAffinityGroupPolicyPersistsAndInvalidates(t *testing.T) {
	manager := newMetadataTestManager(t)
	groupID := seedMetadataTestGroups(t, manager, []uint64{1})[0]
	group := manager.GetAffinityGroupState(groupID)
	require.NotNil(t, group)
	require.Equal(t, PhaseStable, group.Phase)
	affinityVersion := group.affinityVer
	balanceVersion := manager.metadataTables[TableGroupPrefix(groupID)].version

	updated, err := manager.UpdateAffinityGroupPolicy(groupID, BalancePolicyFixed)
	require.NoError(t, err)
	require.Equal(t, BalancePolicyFixed, updated.BalancePolicy)
	current := manager.GetAffinityGroupState(groupID)
	require.Equal(t, PhaseStable, current.Phase)
	require.Equal(t, group.AffinityRegionCount, current.AffinityRegionCount)
	require.Equal(t, group.RegionCount, current.RegionCount)
	require.Equal(t, affinityVersion, current.affinityVer)
	require.Greater(t, manager.metadataTables[TableGroupPrefix(groupID)].version, balanceVersion)

	value, err := manager.storage.LoadAffinityGroup(groupID)
	require.NoError(t, err)
	var persisted Group
	require.NoError(t, json.Unmarshal([]byte(value), &persisted))
	require.Equal(t, BalancePolicyFixed, persisted.BalancePolicy)

	affinityVersion = current.affinityVer
	balanceVersion = manager.metadataTables[TableGroupPrefix(groupID)].version
	updated, err = manager.UpdateAffinityGroupPolicy(groupID, BalancePolicyFixed)
	require.NoError(t, err)
	require.Equal(t, affinityVersion, updated.affinityVer)
	require.Equal(t, balanceVersion, manager.metadataTables[TableGroupPrefix(groupID)].version)

	updated, err = manager.UpdateAffinityGroupPolicy(groupID, "")
	require.NoError(t, err)
	require.Equal(t, BalancePolicyAuto, updated.BalancePolicy)
	require.Equal(t, PhaseStable, updated.Phase)
	require.Equal(t, affinityVersion, updated.affinityVer)

	for _, policy := range []BalancePolicy{BalancePolicyFixed, BalancePolicyAuto} {
		before := manager.GetAffinityGroupState(groupID)
		balanceVersion = manager.metadataTables[TableGroupPrefix(groupID)].version
		remote := before.Group
		remote.BalancePolicy = policy
		require.NoError(t, manager.storage.RunInTxn(t.Context(), func(txn kv.Txn) error {
			return manager.storage.SaveAffinityGroup(txn, groupID, &remote)
		}))
		require.NoError(t, manager.SyncGroupFromEtcd(&remote))
		current = manager.GetAffinityGroupState(groupID)
		require.Equal(t, policy, current.BalancePolicy)
		require.Equal(t, PhaseStable, current.Phase)
		require.Equal(t, before.affinityVer, current.affinityVer)
		require.Equal(t, before.AffinityRegionCount, current.AffinityRegionCount)
		require.Equal(t, before.RegionCount, current.RegionCount)
		require.Greater(t, manager.metadataTables[TableGroupPrefix(groupID)].version, balanceVersion)
	}

	_, err = manager.UpdateAffinityGroupPolicy(groupID, BalancePolicy("manual"))
	require.ErrorIs(t, err, errs.ErrAffinityGroupContent)
	_, err = manager.UpdateAffinityGroupPolicy("missing", BalancePolicyAuto)
	require.ErrorIs(t, err, errs.ErrAffinityGroupNotFound)
}

func TestUpdateAffinityGroupPolicyRejectsStaleStorage(t *testing.T) {
	manager := newMetadataTestManager(t)
	groupID := seedMetadataTestGroups(t, manager, []uint64{1})[0]
	remote := manager.GetAffinityGroupState(groupID).Group
	remote.LeaderStoreID = 2
	require.NoError(t, manager.storage.RunInTxn(t.Context(), func(txn kv.Txn) error {
		return manager.storage.SaveAffinityGroup(txn, groupID, &remote)
	}))

	_, err := manager.UpdateAffinityGroupPolicy(groupID, BalancePolicyFixed)
	require.ErrorIs(t, err, errs.ErrEtcdTxnConflict)
	require.Equal(t, BalancePolicyAuto, manager.GetAffinityGroupState(groupID).BalancePolicy)
	value, err := manager.storage.LoadAffinityGroup(groupID)
	require.NoError(t, err)
	var persisted Group
	require.NoError(t, json.Unmarshal([]byte(value), &persisted))
	require.Equal(t, BalancePolicyAuto, persisted.BalancePolicy)
	require.Equal(t, uint64(2), persisted.LeaderStoreID)
}

func TestUpdateAffinityGroupPolicyRejectsStaleNoop(t *testing.T) {
	for _, policy := range []BalancePolicy{BalancePolicyAuto, BalancePolicyFixed} {
		t.Run(string(policy), func(t *testing.T) {
			manager := newMetadataTestManager(t)
			groupID := seedMetadataTestGroups(t, manager, []uint64{1})[0]
			_, err := manager.UpdateAffinityGroupPolicy(groupID, policy)
			require.NoError(t, err)

			remote := manager.GetAffinityGroupState(groupID).Group
			remote.BalancePolicy = BalancePolicyFixed
			if policy == BalancePolicyFixed {
				remote.BalancePolicy = BalancePolicyAuto
			}
			require.NoError(t, manager.storage.RunInTxn(t.Context(), func(txn kv.Txn) error {
				return manager.storage.SaveAffinityGroup(txn, groupID, &remote)
			}))

			// The watcher has not delivered the other instance's policy change.
			require.Equal(t, policy, manager.GetAffinityGroupState(groupID).BalancePolicy)
			_, err = manager.UpdateAffinityGroupPolicy(groupID, policy)
			require.ErrorIs(t, err, errs.ErrEtcdTxnConflict)
			require.Equal(t, remote, loadStoredGroup(t, manager, groupID))
			require.Equal(t, policy, manager.GetAffinityGroupState(groupID).BalancePolicy,
				"deterministic conflicts wait for the normal watcher")
			require.NoError(t, manager.SyncGroupFromEtcd(&remote))

			updated, err := manager.UpdateAffinityGroupPolicy(groupID, policy)
			require.NoError(t, err)
			require.Equal(t, policy, updated.BalancePolicy)
			remote.BalancePolicy = policy
			require.Equal(t, remote, loadStoredGroup(t, manager, groupID))
		})
	}
}

func TestAdminPeersUpdatePreservesRemotePolicy(t *testing.T) {
	manager := newMetadataTestManager(t)
	id := seedMetadataTestGroups(t, manager, []uint64{1})[0]
	remote := manager.GetAffinityGroupState(id).Group
	remote.BalancePolicy = BalancePolicyFixed
	require.NoError(t, manager.storage.RunInTxn(t.Context(), func(txn kv.Txn) error {
		return manager.storage.SaveAffinityGroup(txn, id, &remote)
	}))
	// Scheduling's policy write has not reached PD's local Manager.
	require.Equal(t, BalancePolicyAuto, manager.GetAffinityGroupState(id).BalancePolicy)
	updated, err := manager.UpdateAffinityGroupPeers(id, 2, []uint64{3, 1, 2})
	require.NoError(t, err)
	require.Equal(t, BalancePolicyFixed, updated.BalancePolicy)
	require.Equal(t, BalancePolicyFixed, manager.GetAffinityGroupState(id).BalancePolicy)
	value, err := manager.storage.LoadAffinityGroup(id)
	require.NoError(t, err)
	var persisted Group
	require.NoError(t, json.Unmarshal([]byte(value), &persisted))
	require.Equal(t, BalancePolicyFixed, persisted.BalancePolicy)
	require.Equal(t, uint64(2), persisted.LeaderStoreID)
	require.Equal(t, []uint64{1, 2, 3}, persisted.VoterStoreIDs)
}
