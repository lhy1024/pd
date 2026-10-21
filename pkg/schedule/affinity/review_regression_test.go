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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

// committedErrorStorage models a lost response after etcd has committed. The
// Manager follows PD's metadata contract: reload the authoritative object and
// return the ambiguous error; the next request may retry explicitly.
type committedErrorStorage struct {
	storage.Storage
	err       error
	reloadErr error
	calls     int
}

func (s *committedErrorStorage) RunInTxn(ctx context.Context, f func(kv.Txn) error) error {
	return s.RunInTxnWithConditions(ctx, nil, f)
}

func (s *committedErrorStorage) RunInTxnWithConditions(ctx context.Context, conditions []clientv3.Cmp, f func(kv.Txn) error) error {
	s.calls++
	if s.calls == 2 && s.reloadErr != nil {
		return s.reloadErr
	}
	var err error
	if len(conditions) == 0 {
		err = s.Storage.RunInTxn(ctx, f)
	} else {
		err = s.Storage.(kv.ConditionalTxnRunner).RunInTxnWithConditions(ctx, conditions, f)
	}
	if err != nil {
		return err
	}
	err = s.err
	s.err = nil
	return err
}

func TestUnknownWriteReloadFailureRecoversOnExplicitRetry(t *testing.T) {
	_, client, cleanup := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer cleanup()
	base := storage.NewStorageWithEtcdBackend(client)
	m := newMetadataTestManager(t)
	m.storage = base
	termKey := "/test/affinity-primary-term-reload-failure"
	termValue := "old"
	_, err := client.Put(t.Context(), termKey, termValue)
	require.NoError(t, err)
	m.SetAffinityWriteGuard(func() ([]clientv3.Cmp, bool) {
		return []clientv3.Cmp{clientv3.Compare(clientv3.Value(termKey), "=", termValue)}, true
	})
	require.NoError(t, m.ReloadAffinityMetadataFromStorage())
	id := seedMetadataTestGroups(t, m, []uint64{1})[0]
	responseErr := errors.New("commit response lost")
	reloadErr := errors.New("authoritative read failed")
	m.storage = &committedErrorStorage{Storage: base, err: responseErr, reloadErr: reloadErr}
	_, err = m.UpdateAffinityGroupPeers(id, 2, []uint64{1, 2, 3})
	require.ErrorIs(t, err, responseErr)
	// The commit did happen, but the first authoritative reload also failed.
	// Switching storage back is the only recovery step before an explicit retry.
	m.storage = base
	_, err = m.UpdateAffinityGroupPeers(id, 3, []uint64{1, 2, 3})
	require.NoError(t, err)
	require.Equal(t, uint64(3), m.GetAffinityGroupState(id).LeaderStoreID)
}

func TestUnknownPolicyWriteReloadFailureRefreshesBaseline(t *testing.T) {
	_, client, cleanup := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer cleanup()
	base := storage.NewStorageWithEtcdBackend(client)
	m := newMetadataTestManager(t)
	m.storage = base
	termKey := "/test/affinity-policy-primary-term-reload-failure"
	_, err := client.Put(t.Context(), termKey, "old")
	require.NoError(t, err)
	m.SetAffinityWriteGuard(func() ([]clientv3.Cmp, bool) {
		return []clientv3.Cmp{clientv3.Compare(clientv3.Value(termKey), "=", "old")}, true
	})
	require.NoError(t, m.ReloadAffinityMetadataFromStorage())
	id := seedMetadataTestGroups(t, m, []uint64{1})[0]
	responseErr := errors.New("policy response lost")
	reloadErr := errors.New("policy authoritative read failed")
	m.storage = &committedErrorStorage{Storage: base, err: responseErr, reloadErr: reloadErr}
	_, err = m.UpdateAffinityGroupPolicy(id, BalancePolicyFixed)
	require.ErrorIs(t, err, responseErr)
	require.Equal(t, BalancePolicyAuto, m.GetAffinityGroupState(id).BalancePolicy)

	m.storage = base
	_, err = m.UpdateAffinityGroupPolicy(id, BalancePolicyFixed)
	require.NoError(t, err)
	require.Equal(t, BalancePolicyFixed, m.GetAffinityGroupState(id).BalancePolicy)
}

func TestUnknownRangeWriteReloadFailureRefreshesBaseline(t *testing.T) {
	_, client, cleanup := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer cleanup()
	base := storage.NewStorageWithEtcdBackend(client)
	m := newMetadataTestManager(t)
	m.storage = base
	termKey := "/test/affinity-range-primary-term-reload-failure"
	_, err := client.Put(t.Context(), termKey, "old")
	require.NoError(t, err)
	m.SetAffinityWriteGuard(func() ([]clientv3.Cmp, bool) {
		return []clientv3.Cmp{clientv3.Compare(clientv3.Value(termKey), "=", "old")}, true
	})
	require.NoError(t, m.ReloadAffinityMetadataFromStorage())
	id := seedMetadataTestGroups(t, m, []uint64{1})[0]
	addFirst := []GroupKeyRanges{{GroupID: id, KeyRanges: []keyutil.KeyRange{{StartKey: []byte{4}, EndKey: []byte{5}}}}}
	m.storage = &committedErrorStorage{Storage: base, err: errors.New("range response lost"), reloadErr: errors.New("range authoritative read failed")}
	require.Error(t, m.UpdateAffinityGroupKeyRanges(addFirst, nil))
	require.Equal(t, 1, m.GetAffinityGroupState(id).RangeCount)

	m.storage = base
	addSecond := []GroupKeyRanges{{GroupID: id, KeyRanges: []keyutil.KeyRange{{StartKey: []byte{6}, EndKey: []byte{7}}}}}
	require.NoError(t, m.UpdateAffinityGroupKeyRanges(addSecond, nil))
	require.Equal(t, 3, m.GetAffinityGroupState(id).RangeCount)
}

func TestUnknownWriteReloadsAndAllowsExplicitRetry(t *testing.T) {
	_, client, cleanup := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer cleanup()
	base := storage.NewStorageWithEtcdBackend(client)
	m := newMetadataTestManager(t)
	m.storage = base
	termKey := "/test/affinity-primary-term"
	termValue := "old"
	_, err := client.Put(t.Context(), termKey, termValue)
	require.NoError(t, err)
	m.SetAffinityWriteGuard(func() ([]clientv3.Cmp, bool) {
		return []clientv3.Cmp{clientv3.Compare(clientv3.Value(termKey), "=", termValue)}, true
	})
	require.NoError(t, m.ReloadAffinityMetadataFromStorage())
	id := seedMetadataTestGroups(t, m, []uint64{1})[0]
	responseErr := errors.New("commit response lost")
	m.storage = &committedErrorStorage{Storage: base, err: responseErr}
	_, err = m.UpdateAffinityGroupPeers(id, 2, []uint64{1, 2, 3})
	require.ErrorIs(t, err, responseErr)
	require.Equal(t, uint64(2), m.GetAffinityGroupState(id).LeaderStoreID)

	m.storage = base
	_, err = m.UpdateAffinityGroupPeers(id, 3, []uint64{1, 2, 3})
	require.NoError(t, err)
}

func TestAffinityTermFenceRejectsOldWriter(t *testing.T) {
	_, client, cleanup := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer cleanup()
	base := storage.NewStorageWithEtcdBackend(client)
	pd := newMetadataTestManager(t)
	pd.storage = base
	pdKey := keypath.ElectionPath(nil)
	schedulingKey := keypath.ElectionPath(&keypath.MsParam{ServiceName: "scheduling"})
	_, err := client.Put(t.Context(), pdKey, "pd-leader")
	require.NoError(t, err)
	pd.SetAffinityWriteGuard(func() ([]clientv3.Cmp, bool) {
		return []clientv3.Cmp{
			clientv3.Compare(clientv3.Value(pdKey), "=", "pd-leader"),
			clientv3.Compare(clientv3.CreateRevision(schedulingKey), "=", 0),
		}, true
	})
	ids := seedMetadataTestGroups(t, pd, []uint64{1, 1})
	_, err = client.Put(t.Context(), schedulingKey, "scheduling-primary")
	require.NoError(t, err)
	mcs := newMetadataTestManager(t)
	mcs.storage = base
	mcs.SetAffinityWriteGuard(func() ([]clientv3.Cmp, bool) {
		return []clientv3.Cmp{clientv3.Compare(clientv3.Value(schedulingKey), "=", "scheduling-primary")}, true
	})
	require.NoError(t, mcs.ReloadAffinityMetadataFromStorage())
	_, err = mcs.UpdateAffinityGroupPolicy(ids[1], BalancePolicyFixed)
	require.NoError(t, err)
	_, err = pd.UpdateAffinityGroupPolicy(ids[0], BalancePolicyFixed)
	require.ErrorIs(t, err, errs.ErrEtcdTxnConflict)
}

func TestAffinityRangesRejectStaleBaseAndRecover(t *testing.T) {
	m := newMetadataTestManager(t)
	id := seedMetadataTestGroups(t, m, []uint64{1})[0]
	ranges := m.keyRanges[id]
	ranges.KeyRanges = append(ranges.KeyRanges, keyutil.KeyRange{StartKey: []byte{3}, EndKey: []byte{4}})
	require.NoError(t, m.regionLabeler.SetLabelRule(MakeLabelRule(ranges)))
	add := []GroupKeyRanges{{GroupID: id, KeyRanges: []keyutil.KeyRange{{StartKey: []byte{6}, EndKey: []byte{7}}}}}
	require.ErrorIs(t, m.UpdateAffinityGroupKeyRanges(add, nil), errs.ErrEtcdTxnConflict)
	// A deterministic CAS conflict does not trigger a full metadata reload.
	// The normal watcher applies the winning label before the caller retries.
	require.Equal(t, 1, m.GetAffinityGroupState(id).RangeCount)
	require.NoError(t, m.SyncKeyRangesFromEtcd(m.regionLabeler.GetLabelRule(GetLabelRuleID(id))))
	require.NoError(t, m.UpdateAffinityGroupKeyRanges(add, nil))
	require.Equal(t, 3, m.GetAffinityGroupState(id).RangeCount)
}

func TestAffinityRangeWatcherIgnoresDelayedPut(t *testing.T) {
	m := newMetadataTestManager(t)
	ids := seedMetadataTestGroups(t, m, []uint64{1, 1})
	old := m.regionLabeler.GetLabelRule(GetLabelRuleID(ids[0]))
	require.NotNil(t, old)
	add := []GroupKeyRanges{{GroupID: ids[0], KeyRanges: []keyutil.KeyRange{{StartKey: []byte{4}, EndKey: []byte{5}}}}}
	require.NoError(t, m.UpdateAffinityGroupKeyRanges(add, nil))
	require.Equal(t, 2, m.GetAffinityGroupState(ids[0]).RangeCount)
	// A delayed watcher event still carries the old value. It must not replace
	// the range view used by the next cross-group overlap check.
	require.NoError(t, m.SyncKeyRangesFromEtcdEvent(old))
	require.Equal(t, 2, m.GetAffinityGroupState(ids[0]).RangeCount)
	conflicting := []GroupKeyRanges{{GroupID: ids[1], KeyRanges: []keyutil.KeyRange{{StartKey: []byte{4}, EndKey: []byte{5}}}}}
	require.Error(t, m.UpdateAffinityGroupKeyRanges(conflicting, nil))
	require.Equal(t, 1, m.GetAffinityGroupState(ids[1]).RangeCount)
}

func TestAffinityGroupWatcherIgnoresDelayedDeleteAfterRecreate(t *testing.T) {
	m := newMetadataTestManager(t)
	ids := seedMetadataTestGroups(t, m, []uint64{1, 1})
	require.NoError(t, m.DeleteAffinityGroups([]string{ids[0]}, true))
	rangeToProtect := GroupKeyRanges{GroupID: ids[0], KeyRanges: []keyutil.KeyRange{{StartKey: []byte{4}, EndKey: []byte{5}}}}
	require.NoError(t, m.CreateAffinityGroups([]GroupKeyRanges{rangeToProtect}))
	require.Equal(t, 1, m.GetAffinityGroupState(ids[0]).RangeCount)
	// The old delete event must observe the recreated group in storage and
	// leave its ranges available to the next cross-group overlap check.
	require.NoError(t, m.SyncGroupDeleteFromEtcd(ids[0]))
	require.NotNil(t, m.GetAffinityGroupState(ids[0]))
	conflicting := []GroupKeyRanges{{GroupID: ids[1], KeyRanges: []keyutil.KeyRange{{StartKey: []byte{4}, EndKey: []byte{5}}}}}
	require.Error(t, m.UpdateAffinityGroupKeyRanges(conflicting, nil))
	require.Equal(t, 1, m.GetAffinityGroupState(ids[1]).RangeCount)
}

func TestAffinityBatchCapacityRejectsBeforeAnyWrite(t *testing.T) {
	m := newMetadataTestManager(t)
	withoutRanges := make([]GroupKeyRanges, etcdutil.MaxEtcdTxnOps/3+1)
	for i := range withoutRanges {
		withoutRanges[i].GroupID = fmt.Sprintf("no-ranges-capacity-%d", i)
	}
	require.ErrorIs(t, m.CreateAffinityGroups(withoutRanges), errs.ErrExceedMaxEtcdTxnOps)
	for _, change := range withoutRanges {
		value, err := m.storage.LoadAffinityGroup(change.GroupID)
		require.NoError(t, err)
		require.Empty(t, value)
	}
	changes := make([]GroupKeyRanges, 30)
	ids := make([]string, len(changes))
	for i := range changes {
		ids[i] = fmt.Sprintf("capacity-%d", i)
		changes[i] = GroupKeyRanges{GroupID: ids[i], KeyRanges: []keyutil.KeyRange{{StartKey: []byte{byte(i)}, EndKey: []byte{byte(i + 1)}}}}
	}
	require.ErrorIs(t, m.CreateAffinityGroups(changes), errs.ErrExceedMaxEtcdTxnOps)
	for _, change := range changes {
		value, err := m.storage.LoadAffinityGroup(change.GroupID)
		require.NoError(t, err)
		require.Empty(t, value)
		require.Nil(t, m.regionLabeler.GetLabelRule(GetLabelRuleID(change.GroupID)))
		require.NoError(t, m.CreateAffinityGroups([]GroupKeyRanges{change}))
	}
	require.ErrorIs(t, m.DeleteAffinityGroups(ids, true), errs.ErrExceedMaxEtcdTxnOps)
	for _, id := range ids {
		require.True(t, m.IsGroupExist(id))
	}
}

func TestAffinityReloadUsesAuthoritativeLabelsWithSeparateCache(t *testing.T) {
	m := newMetadataTestManager(t)
	ids := seedMetadataTestGroups(t, m, []uint64{1, 1})
	cache, err := labeler.NewRegionLabeler(m.ctx, storage.NewStorageWithMemoryBackend(), time.Hour)
	require.NoError(t, err)
	follower, err := NewManager(m.ctx, m.storage, m.storeSetInformer, m.conf, cache)
	require.NoError(t, err)
	require.Equal(t, 1, follower.GetAffinityGroupState(ids[0]).RangeCount)
	group := m.GetAffinityGroupState(ids[0]).Group
	group.LeaderStoreID = 2
	rule := MakeLabelRule(GroupKeyRanges{GroupID: ids[0], KeyRanges: []keyutil.KeyRange{
		{StartKey: []byte{0}, EndKey: []byte{1}},
		{StartKey: []byte{4}, EndKey: []byte{5}},
	}})
	base := m.storage.(storage.Storage)
	require.NoError(t, base.RunInTxn(m.ctx, func(txn kv.Txn) error {
		if err := base.SaveAffinityGroup(txn, group.ID, &group); err != nil {
			return err
		}
		if err := base.DeleteAffinityGroup(txn, ids[1]); err != nil {
			return err
		}
		if err := base.DeleteRegionRule(txn, GetLabelRuleID(ids[1])); err != nil {
			return err
		}
		return base.SaveRegionRule(txn, rule.ID, rule)
	}))
	require.NoError(t, follower.ReloadAffinityMetadataFromStorage())
	require.Nil(t, follower.GetAffinityGroupState(ids[1]))
	require.Equal(t, uint64(2), follower.GetAffinityGroupState(ids[0]).LeaderStoreID)
	require.Equal(t, 2, follower.GetAffinityGroupState(ids[0]).RangeCount)
	require.Len(t, cache.GetLabelRule(rule.ID).GetKeyRanges(), 2)
}
