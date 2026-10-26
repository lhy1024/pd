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
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

func makeGroupTargetUpdate(m *Manager, groupID string, leader uint64, voters []uint64) groupTargetUpdate {
	original := m.GetAffinityGroupState(groupID).Group
	target := original
	target.LeaderStoreID = leader
	target.VoterStoreIDs = voters
	return groupTargetUpdate{original: original, target: target, ranges: m.keyRangesOrEmpty(groupID)}
}

func seedBatchTargetGroups(t *testing.T, m *Manager, count int) []string {
	t.Helper()
	ids := make([]string, count)
	for i := range ids {
		ids[i] = fmt.Sprintf("batch-target-%d", i)
		require.NoError(t, m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: ids[i]}}))
		_, err := m.UpdateAffinityGroupPeers(ids[i], 1, []uint64{1, 2, 3})
		require.NoError(t, err)
	}
	return ids
}

func TestCommitGroupTargetsPublishesBatchOnce(t *testing.T) {
	m := newMetadataTestManager(t)
	ids := seedMetadataTestGroups(t, m, []uint64{1, 1})
	changes := []groupTargetUpdate{
		makeGroupTargetUpdate(m, ids[0], 4, []uint64{2, 3, 4}),
		makeGroupTargetUpdate(m, ids[1], 4, []uint64{2, 3, 4}),
	}
	before := make([]*GroupState, len(ids))
	for i, id := range ids {
		before[i] = m.GetAffinityGroupState(id)
		require.Equal(t, 1, before[i].AffinityRegionCount)
	}

	committed, err := m.commitGroupTargets(t.Context(), changes, nil, func() bool { return true })
	require.NoError(t, err)
	require.True(t, committed)
	for i, id := range ids {
		require.Equal(t, changes[i].target.LeaderStoreID, m.GetAffinityGroupState(id).LeaderStoreID)
		require.Equal(t, changes[i].target, loadStoredGroup(t, m, id))
		after := m.GetAffinityGroupState(id)
		require.Equal(t, before[i].affinityVer+1, after.affinityVer)
		require.Zero(t, after.AffinityRegionCount)
	}
	m.RLock()
	require.Zero(t, m.affinityRegionCount)
	m.RUnlock()
}

func TestCommitGroupTargetsConflictRejectsWholeBatch(t *testing.T) {
	for _, conflictOn := range []string{"group", "label"} {
		t.Run(conflictOn, func(t *testing.T) {
			m := newMetadataTestManager(t)
			ids := seedMetadataTestGroups(t, m, []uint64{1, 1})
			changes := []groupTargetUpdate{
				makeGroupTargetUpdate(m, ids[0], 4, []uint64{2, 3, 4}),
				makeGroupTargetUpdate(m, ids[1], 4, []uint64{2, 3, 4}),
			}
			before := []Group{loadStoredGroup(t, m, ids[0]), loadStoredGroup(t, m, ids[1])}
			base := m.storage.(storage.Storage)
			if conflictOn == "group" {
				remote := before[1]
				remote.LeaderStoreID = 3
				require.NoError(t, base.RunInTxn(t.Context(), func(txn kv.Txn) error {
					return base.SaveAffinityGroup(txn, remote.ID, &remote)
				}))
				before[1] = remote
			} else {
				ranges := GroupKeyRanges{GroupID: ids[1], KeyRanges: []keyutil.KeyRange{{StartKey: []byte{4}, EndKey: []byte{5}}}}
				require.NoError(t, m.regionLabeler.SetLabelRule(MakeLabelRule(ranges)))
			}

			committed, err := m.commitGroupTargets(t.Context(), changes, nil, func() bool { return true })
			require.NoError(t, err)
			require.False(t, committed)
			require.Equal(t, before[0], loadStoredGroup(t, m, ids[0]), "the earlier target must not be written")
			require.Equal(t, before[1], loadStoredGroup(t, m, ids[1]))
			for i, id := range ids {
				require.Equal(t, uint64(1), m.GetAffinityGroupState(id).LeaderStoreID, "the failed batch must not be published")
				if conflictOn == "label" || i == 0 {
					require.Equal(t, uint64(1), loadStoredGroup(t, m, id).LeaderStoreID)
				}
			}
		})
	}
}

func TestCommitGroupTargetsChecksCurrentAtCommitBoundary(t *testing.T) {
	m := newMetadataTestManager(t)
	id := seedMetadataTestGroups(t, m, []uint64{1})[0]
	change := makeGroupTargetUpdate(m, id, 4, []uint64{2, 3, 4})
	calls := 0
	committed, err := m.commitGroupTargets(t.Context(), []groupTargetUpdate{change}, nil, func() bool {
		calls++
		return calls == 1
	})
	require.NoError(t, err)
	require.False(t, committed)
	require.Equal(t, 2, calls)
	require.Equal(t, uint64(1), loadStoredGroup(t, m, id).LeaderStoreID)
	require.Equal(t, uint64(1), m.GetAffinityGroupState(id).LeaderStoreID)
}

func TestCommitGroupTargetsUsesCapturedTermAndConditions(t *testing.T) {
	_, client, cleanup := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer cleanup()
	base := storage.NewStorageWithEtcdBackend(client)
	m := newMetadataTestManager(t)
	m.storage = base
	termKey := "/test/affinity-batch-primary-term"
	_, err := client.Put(t.Context(), termKey, "old")
	require.NoError(t, err)
	guardCalls := 0
	m.SetAffinityWriteGuard(func() ([]clientv3.Cmp, bool) {
		guardCalls++
		return []clientv3.Cmp{clientv3.Compare(clientv3.Value(termKey), "=", "old")}, true
	})
	require.NoError(t, m.ReloadAffinityMetadataFromStorage())
	ids := seedMetadataTestGroups(t, m, []uint64{1, 1})
	conditions, err := m.captureAffinityWriteConditions()
	require.NoError(t, err)
	capturedCalls := guardCalls
	_, err = client.Put(t.Context(), termKey, "new")
	require.NoError(t, err)
	changes := []groupTargetUpdate{
		makeGroupTargetUpdate(m, ids[0], 4, []uint64{2, 3, 4}),
		makeGroupTargetUpdate(m, ids[1], 4, []uint64{2, 3, 4}),
	}

	committed, err := m.commitGroupTargets(t.Context(), changes, conditions, func() bool { return true })
	require.NoError(t, err)
	require.False(t, committed)
	require.Equal(t, capturedCalls, guardCalls, "the batch must not recapture or renew its term")
	for _, id := range ids {
		require.Equal(t, uint64(1), loadStoredGroup(t, m, id).LeaderStoreID)
		require.Equal(t, uint64(1), m.GetAffinityGroupState(id).LeaderStoreID)
	}
}

func TestCommitGroupTargetsCapacityAndNoop(t *testing.T) {
	m := newMetadataTestManager(t)
	ids := seedBatchTargetGroups(t, m, 41)
	changes := make([]groupTargetUpdate, len(ids))
	for i, id := range ids {
		changes[i] = makeGroupTargetUpdate(m, id, 2, []uint64{2, 3, 4})
	}
	versions := make([]uint64, len(ids))
	for i, id := range ids {
		versions[i] = m.GetAffinityGroupState(id).affinityVer
	}
	committed, err := m.commitGroupTargets(t.Context(), changes, nil, func() bool { return true })
	require.ErrorIs(t, err, errs.ErrExceedMaxEtcdTxnOps)
	require.False(t, committed)
	for i, id := range ids {
		require.Equal(t, uint64(1), loadStoredGroup(t, m, id).LeaderStoreID)
		require.Equal(t, versions[i], m.GetAffinityGroupState(id).affinityVer)
	}

	changes[len(changes)-1] = makeGroupTargetUpdate(m, ids[len(ids)-1], 1, []uint64{1, 2, 3})
	committed, err = m.commitGroupTargets(t.Context(), changes, nil, func() bool { return true })
	require.NoError(t, err)
	require.True(t, committed)
	for i, id := range ids {
		want := uint64(2)
		if i == len(ids)-1 {
			want = 1
			require.Equal(t, versions[i], m.GetAffinityGroupState(id).affinityVer)
		} else {
			require.Equal(t, versions[i]+1, m.GetAffinityGroupState(id).affinityVer)
		}
		require.Equal(t, want, loadStoredGroup(t, m, id).LeaderStoreID)
	}
}

func TestUnknownGroupTargetBatchReloadsAllChanges(t *testing.T) {
	m := newMetadataTestManager(t)
	ids := seedMetadataTestGroups(t, m, []uint64{1, 1})
	base := m.storage.(storage.Storage)
	responseErr := errors.New("batch response lost")
	m.storage = &committedErrorStorage{Storage: base, err: responseErr}
	changes := []groupTargetUpdate{
		makeGroupTargetUpdate(m, ids[0], 4, []uint64{2, 3, 4}),
		makeGroupTargetUpdate(m, ids[1], 4, []uint64{2, 3, 4}),
	}
	committed, err := m.commitGroupTargets(t.Context(), changes, nil, func() bool { return true })
	require.ErrorIs(t, err, responseErr)
	require.False(t, committed)
	for _, id := range ids {
		require.Equal(t, uint64(4), loadStoredGroup(t, m, id).LeaderStoreID)
		require.Equal(t, uint64(4), m.GetAffinityGroupState(id).LeaderStoreID, "one reload must publish the complete committed batch")
	}
}

func TestUnknownGroupTargetReloadFailureAllowsExplicitRetry(t *testing.T) {
	m := newMetadataTestManager(t)
	ids := seedMetadataTestGroups(t, m, []uint64{1, 1})
	base := m.storage.(storage.Storage)
	responseErr := errors.New("batch response lost")
	reloadErr := errors.New("batch reload failed")
	m.storage = &committedErrorStorage{Storage: base, err: responseErr, reloadErr: reloadErr}
	changes := []groupTargetUpdate{
		makeGroupTargetUpdate(m, ids[0], 4, []uint64{2, 3, 4}),
		makeGroupTargetUpdate(m, ids[1], 4, []uint64{2, 3, 4}),
	}
	committed, err := m.commitGroupTargets(t.Context(), changes, nil, func() bool { return true })
	require.ErrorIs(t, err, responseErr)
	require.False(t, committed)
	require.True(t, m.metadataNeedsReload)
	for _, id := range ids {
		require.Equal(t, uint64(4), loadStoredGroup(t, m, id).LeaderStoreID)
		require.Equal(t, uint64(1), m.GetAffinityGroupState(id).LeaderStoreID)
	}
	m.storage = base
	retry := make([]groupTargetUpdate, len(ids))
	for i, id := range ids {
		original := loadStoredGroup(t, m, id)
		target := original
		target.LeaderStoreID = 3
		target.VoterStoreIDs = []uint64{2, 3, 4}
		retry[i] = groupTargetUpdate{original: original, target: target, ranges: m.keyRangesOrEmpty(id)}
	}
	committed, err = m.commitGroupTargets(t.Context(), retry, nil, func() bool { return true })
	require.NoError(t, err)
	require.True(t, committed)
	for _, id := range ids {
		require.Equal(t, uint64(3), loadStoredGroup(t, m, id).LeaderStoreID)
		require.Equal(t, uint64(3), m.GetAffinityGroupState(id).LeaderStoreID)
	}
}

func TestCommitGroupTargetsRejectsDuplicatesAndBadIDs(t *testing.T) {
	m := newMetadataTestManager(t)
	id := seedMetadataTestGroups(t, m, []uint64{1})[0]
	change := makeGroupTargetUpdate(m, id, 4, []uint64{2, 3, 4})
	committed, err := m.commitGroupTargets(t.Context(), []groupTargetUpdate{change, change}, nil, func() bool { return true })
	require.Error(t, err)
	require.False(t, committed)

	invalid := change
	invalid.target.ID = "bad/id"
	committed, err = m.commitGroupTargets(t.Context(), []groupTargetUpdate{invalid}, nil, func() bool { return true })
	require.Error(t, err)
	require.False(t, committed)
	conflicting := change
	conflicting.target.LeaderStoreID = 3
	committed, err = m.commitGroupTargets(t.Context(), []groupTargetUpdate{change, conflicting}, nil, func() bool { return true })
	require.Error(t, err)
	require.False(t, committed)
}

func TestCommitGroupTargetsNoopDoesNotWriteOrResetCounts(t *testing.T) {
	m := newMetadataTestManager(t)
	id := seedMetadataTestGroups(t, m, []uint64{1})[0]
	change := makeGroupTargetUpdate(m, id, 1, []uint64{1, 2, 3})
	before := m.GetAffinityGroupState(id)
	counted := &committedErrorStorage{Storage: m.storage.(storage.Storage)}
	m.storage = counted

	committed, err := m.commitGroupTargets(t.Context(), []groupTargetUpdate{change}, nil, func() bool { return false })
	require.NoError(t, err)
	require.False(t, committed)
	require.Zero(t, counted.calls)
	after := m.GetAffinityGroupState(id)
	require.Equal(t, before.affinityVer, after.affinityVer)
	require.Equal(t, before.AffinityRegionCount, after.AffinityRegionCount)
	require.Equal(t, uint64(1), loadStoredGroup(t, m, id).LeaderStoreID)

	committed, err = m.commitGroupTargets(t.Context(), []groupTargetUpdate{change}, nil, func() bool { return true })
	require.NoError(t, err)
	require.False(t, committed, "a current but unchanged target is not a commit")
	require.Zero(t, counted.calls)
	require.Equal(t, before.affinityVer, m.GetAffinityGroupState(id).affinityVer)
}
