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
	"github.com/tikv/pd/pkg/utils/keyutil"
)

type committedErrorStorage struct {
	storage.Storage
	err error
}

// delayedCommitStorage returns an unknown result after the original callback
// captured etcd revisions, but before its transaction is allowed to commit.
type delayedCommitStorage struct {
	storage.Storage
	delay        bool
	failRecovery bool
	release      chan struct{}
	done         chan error
}

func (s *delayedCommitStorage) RunInTxn(ctx context.Context, f func(kv.Txn) error) error {
	return s.RunInTxnWithConditions(ctx, nil, f)
}

func (s *delayedCommitStorage) RunInTxnWithConditions(ctx context.Context, conditions []clientv3.Cmp, f func(kv.Txn) error) error {
	run := func(ctx context.Context, callback func(kv.Txn) error) error {
		if len(conditions) == 0 {
			return s.Storage.RunInTxn(ctx, callback)
		}
		return s.Storage.(kv.ConditionalTxnRunner).RunInTxnWithConditions(ctx, conditions, callback)
	}
	if s.delay {
		s.delay = false
		ready := make(chan struct{})
		go func() {
			s.done <- run(context.WithoutCancel(ctx), func(txn kv.Txn) error {
				err := f(txn)
				close(ready)
				if err != nil {
					return err
				}
				<-s.release
				return nil
			})
		}()
		<-ready
		return errors.New("commit response lost before completion")
	}
	if s.failRecovery {
		return errors.New("recovery unavailable")
	}
	return run(ctx, f)
}

func TestUnknownWriteFencesLateCommitAndRecoversOnlyAffectedGroups(t *testing.T) {
	_, client, cleanup := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer cleanup()
	base := storage.NewStorageWithEtcdBackend(client)
	for _, kind := range []string{"target", "ranges", "policy", "peers", "delete"} {
		for _, commitFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/visible-before-recovery=%t", kind, commitFirst), func(t *testing.T) {
				m := newMetadataTestManager(t)
				m.storage = base
				// Each subtest uses the same deterministic fixture on fresh keys.
				require.NoError(t, base.RunInTxn(t.Context(), func(txn kv.Txn) error {
					for _, id := range []string{"_tidb_pt_10_p1", "_tidb_pt_10_p2", "_tidb_pt_20_p1"} {
						if err := base.DeleteAffinityGroup(txn, id); err != nil {
							return err
						}
						if err := base.DeleteRegionRule(txn, GetLabelRuleID(id)); err != nil {
							return err
						}
					}
					return nil
				}))
				ids := seedMetadataTestGroups(t, m, []uint64{1, 1})
				other := "_tidb_pt_20_p1"
				require.NoError(t, m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: other}}))
				otherGroup := m.groups[other]
				snapshot := prepareMetadataTarget(t, m, ids[0])
				otherVersion := m.metadataTables[TableGroupPrefix(other)].version
				s := &delayedCommitStorage{Storage: base, delay: true, failRecovery: true,
					release: make(chan struct{}), done: make(chan error, 1)}
				m.storage = s
				switch kind {
				case "target":
					ok, err := commitMetadataTarget(m, snapshot, metadataTarget(snapshot, ids[0], 2))
					require.False(t, ok)
					require.ErrorContains(t, err, "recovery unavailable")
				case "ranges":
					err := m.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{{GroupID: ids[0],
						KeyRanges: []keyutil.KeyRange{{StartKey: []byte{8}, EndKey: []byte{9}}}}}, nil)
					require.ErrorContains(t, err, "recovery unavailable")
				case "policy":
					_, err := m.UpdateAffinityGroupPolicy(ids[0], BalancePolicyFixed)
					require.ErrorContains(t, err, "recovery unavailable")
				case "peers":
					_, err := m.UpdateAffinityGroupPeers(ids[0], 2, []uint64{1, 2, 3})
					require.ErrorContains(t, err, "recovery unavailable")
				case "delete":
					require.ErrorContains(t, m.DeleteAffinityGroups([]string{ids[0]}, true), "recovery unavailable")
				}
				require.Nil(t, getMetadataTarget(m, ids[1]), "the entire affected table must wait")
				require.Nil(t, getMetadataTarget(m, other), "all writers share the Manager recovery boundary")
				finish := func() error {
					close(s.release)
					select {
					case err := <-s.done:
						return err
					case <-time.After(5 * time.Second):
						t.Fatal("delayed transaction did not finish")
						return nil
					}
				}
				if commitFirst {
					require.NoError(t, finish())
				}
				s.failRecovery = false
				fresh := prepareMetadataTarget(t, m, ids[1])
				if !commitFirst && kind == "delete" {
					require.NoError(t, m.DeleteAffinityGroups([]string{ids[0]}, true))
					require.NoError(t, m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: ids[0]}}))
					fresh = prepareMetadataTarget(t, m, ids[1])
				}
				if commitFirst && kind == "target" {
					require.Equal(t, uint64(2), m.GetAffinityGroupState(ids[0]).LeaderStoreID)
				}
				if commitFirst && kind == "ranges" {
					require.Equal(t, 2, m.GetAffinityGroupState(ids[0]).RangeCount)
				}
				if commitFirst && kind == "policy" {
					require.Equal(t, BalancePolicyFixed, m.GetAffinityGroupState(ids[0]).BalancePolicy)
				}
				if commitFirst && kind == "peers" {
					require.Equal(t, uint64(2), m.GetAffinityGroupState(ids[0]).LeaderStoreID)
				}
				if commitFirst && kind == "delete" {
					require.Nil(t, m.GetAffinityGroupState(ids[0]))
				}
				ok, err := commitMetadataTarget(m, fresh, metadataTarget(fresh, ids[1], 3))
				require.NoError(t, err)
				require.True(t, ok)
				if !commitFirst {
					require.ErrorIs(t, finish(), errs.ErrEtcdTxnConflict)
					if kind == "delete" {
						require.NotNil(t, m.GetAffinityGroupState(ids[0]), "late delete must not remove a recreated group")
						require.NotNil(t, loadStoredGroup(t, m, ids[0]))
					}
				}
				require.Same(t, otherGroup, m.groups[other])
				currentVersion := m.metadataTables[TableGroupPrefix(other)].version
				require.Equal(t, otherVersion, currentVersion)
			})
		}
	}
}

func (s *committedErrorStorage) RunInTxn(ctx context.Context, f func(kv.Txn) error) error {
	if err := s.Storage.RunInTxn(ctx, f); err != nil {
		return err
	}
	err := s.err
	s.err = nil
	return err
}

func TestTargetCommitRecoversCommittedResponseError(t *testing.T) {
	m := newMetadataTestManager(t)
	id := seedMetadataTestGroups(t, m, []uint64{1})[0]
	snapshot := prepareMetadataTarget(t, m, id)
	responseErr := errors.New("commit response lost")
	m.storage = &committedErrorStorage{Storage: m.storage.(storage.Storage), err: responseErr}
	committed, err := commitMetadataTarget(m, snapshot, metadataTarget(snapshot, id, 2))
	require.ErrorIs(t, err, responseErr)
	require.False(t, committed)
	require.Equal(t, uint64(2), m.GetAffinityGroupState(id).LeaderStoreID)
	snapshot = prepareMetadataTarget(t, m, id)
	committed, err = commitMetadataTarget(m, snapshot, metadataTarget(snapshot, id, 3))
	require.NoError(t, err)
	require.True(t, committed)
	require.Equal(t, uint64(3), loadStoredGroup(t, m, id).LeaderStoreID)
}

func TestManagementWritesRecoverCommittedResponseError(t *testing.T) {
	for _, kind := range []string{"create", "delete", "ranges", "policy", "peers"} {
		t.Run(kind, func(t *testing.T) {
			m := newMetadataTestManager(t)
			id := seedMetadataTestGroups(t, m, []uint64{1})[0]
			responseErr := errors.New("commit response lost")
			m.storage = &committedErrorStorage{Storage: m.storage.(storage.Storage), err: responseErr}
			var err error
			switch kind {
			case "create":
				id = "_tidb_pt_20_p1"
				err = m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: id, KeyRanges: []keyutil.KeyRange{{StartKey: []byte{8}, EndKey: []byte{9}}}}})
			case "delete":
				err = m.DeleteAffinityGroups([]string{id}, true)
			case "ranges":
				err = m.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{{GroupID: id, KeyRanges: []keyutil.KeyRange{{StartKey: []byte{8}, EndKey: []byte{9}}}}}, nil)
			case "policy":
				_, err = m.UpdateAffinityGroupPolicy(id, BalancePolicyFixed)
			case "peers":
				_, err = m.UpdateAffinityGroupPeers(id, 2, []uint64{1, 2, 3})
			}
			require.ErrorIs(t, err, responseErr)
			require.Nil(t, m.pendingMutation)
			group := m.GetAffinityGroupState(id)
			switch kind {
			case "create":
				require.NotNil(t, group)
				require.Equal(t, 1, group.RangeCount)
			case "delete":
				require.Nil(t, group)
			case "ranges":
				require.Equal(t, 2, group.RangeCount)
			case "policy":
				require.Equal(t, BalancePolicyFixed, group.BalancePolicy)
			case "peers":
				require.Equal(t, uint64(2), group.LeaderStoreID)
			}
		})
	}
}

func TestUnknownCreateBlocksAllWritersUntilCompletionOrTermChange(t *testing.T) {
	_, client, cleanup := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer cleanup()
	base := storage.NewStorageWithEtcdBackend(client)
	for _, changeTerm := range []bool{false, true} {
		t.Run(fmt.Sprintf("term-change=%t", changeTerm), func(t *testing.T) {
			m := newMetadataTestManager(t)
			m.storage = base
			id := fmt.Sprintf("pending-create-%t", changeTerm)
			other := fmt.Sprintf("existing-%t", changeTerm)
			require.NoError(t, m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: other}}))
			_, err := m.UpdateAffinityGroupPeers(other, 1, []uint64{1, 2, 3})
			require.NoError(t, err)
			snapshot := prepareMetadataTarget(t, m, other)
			termKey := "/test/affinity-primary-term"
			_, err = client.Put(t.Context(), termKey, "old")
			require.NoError(t, err)
			m.SetAffinityWriteGuard(func() ([]clientv3.Cmp, bool) {
				return []clientv3.Cmp{clientv3.Compare(clientv3.Value(termKey), "=", "old")}, true
			})
			s := &delayedCommitStorage{Storage: base, delay: true, release: make(chan struct{}), done: make(chan error, 1)}
			m.storage = s
			require.ErrorContains(t, m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: id}}), "outcome is unresolved")
			require.NotNil(t, m.pendingMutation)
			// Repeated recovery queries must never submit another create.
			require.Error(t, m.CreateAffinityGroupsIfNotExists([]GroupKeyRanges{{GroupID: id}}))
			require.Error(t, m.DeleteAffinityGroups([]string{id}, true))
			_, err = m.UpdateAffinityGroupPolicy(other, BalancePolicyFixed)
			require.Error(t, err)
			_, err = m.UpdateAffinityGroupPeers(other, 1, []uint64{1, 2, 3})
			require.Error(t, err)
			require.Error(t, m.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{{GroupID: other, KeyRanges: []keyutil.KeyRange{{StartKey: []byte{8}, EndKey: []byte{9}}}}}, nil))
			require.Error(t, m.ReloadAffinityMetadataFromStorage())
			require.Nil(t, getMetadataTarget(m, other))
			ok, err := commitMetadataTarget(m, snapshot, metadataTarget(snapshot, other, 2))
			require.False(t, ok)
			require.Error(t, err)
			value, err := base.LoadAffinityGroup(id)
			require.NoError(t, err)
			require.Empty(t, value, "recovery must not replay the create")
			if changeTerm {
				_, err = client.Put(t.Context(), termKey, "new")
				require.NoError(t, err)
				m.SetAffinityWriteGuard(func() ([]clientv3.Cmp, bool) {
					return []clientv3.Cmp{clientv3.Compare(clientv3.Value(termKey), "=", "new")}, true
				})
				require.NoError(t, m.ReloadAffinityMetadataFromStorage())
				require.NoError(t, m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: id}}))
				require.NoError(t, m.DeleteAffinityGroups([]string{id}, true))
			}
			close(s.release)
			select {
			case err := <-s.done:
				if changeTerm {
					require.ErrorIs(t, err, errs.ErrEtcdTxnConflict)
				} else {
					require.NoError(t, err)
					require.NoError(t, m.DeleteAffinityGroups([]string{id}, true))
				}
			case <-time.After(5 * time.Second):
				t.Fatal("delayed create did not finish")
			}
			require.Nil(t, m.pendingMutation)
			value, err = base.LoadAffinityGroup(id)
			require.NoError(t, err)
			require.Empty(t, value, "late create must not resurrect the deleted group")
		})
	}
}

func TestAffinityRangesRejectStaleBaseAndRecover(t *testing.T) {
	m := newMetadataTestManager(t)
	id := seedMetadataTestGroups(t, m, []uint64{1})[0]
	ranges := m.keyRanges[id]
	ranges.KeyRanges = append(ranges.KeyRanges, keyutil.KeyRange{StartKey: []byte{3}, EndKey: []byte{4}})
	rule := MakeLabelRule(ranges)
	require.NoError(t, m.regionLabeler.SetLabelRule(rule))
	add := []GroupKeyRanges{{GroupID: id, KeyRanges: []keyutil.KeyRange{{StartKey: []byte{6}, EndKey: []byte{7}}}}}
	require.ErrorIs(t, m.UpdateAffinityGroupKeyRanges(add, nil), errs.ErrEtcdTxnConflict)
	require.Equal(t, 2, m.GetAffinityGroupState(id).RangeCount)
	require.NoError(t, m.UpdateAffinityGroupKeyRanges(add, nil))
	require.Equal(t, 3, m.GetAffinityGroupState(id).RangeCount)
}

func TestAffinityOldEchoDoesNotLoseNextRangeUpdate(t *testing.T) {
	m := newMetadataTestManager(t)
	id := seedMetadataTestGroups(t, m, []uint64{1})[0]
	add := func(start byte) {
		require.NoError(t, m.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{{GroupID: id,
			KeyRanges: []keyutil.KeyRange{{StartKey: []byte{start}, EndKey: []byte{start + 1}}}}}, nil))
	}
	add(3)
	old := m.regionLabeler.GetLabelRule(GetLabelRuleID(id))
	add(6)
	require.NoError(t, m.SyncKeyRangesFromEtcdAtRevision(old, 10))
	require.Len(t, m.regionLabeler.GetLabelRule(GetLabelRuleID(id)).GetKeyRanges(), 3,
		"a delayed event must not regress the shared label index")
	add(9)
	require.Equal(t, 4, m.GetAffinityGroupState(id).RangeCount)
	require.Len(t, m.regionLabeler.GetLabelRule(GetLabelRuleID(id)).GetKeyRanges(), 4)
}

func TestAffinityOldPutDoesNotResurrectLocallyDeletedGroup(t *testing.T) {
	m := newMetadataTestManager(t)
	id := seedMetadataTestGroups(t, m, []uint64{1})[0]
	old := m.GetAffinityGroupState(id).Group
	rule := m.regionLabeler.GetLabelRule(GetLabelRuleID(id))
	require.NoError(t, m.DeleteAffinityGroups([]string{id}, true))
	require.NoError(t, m.SyncGroupFromEtcdAtRevision(&old, 10))
	require.NoError(t, m.SyncKeyRangesFromEtcdAtRevision(rule, 10))
	require.False(t, m.IsGroupExist(id))
	require.Empty(t, m.labelRuleBuffer)
}

func TestAffinityBatchCapacityRejectsBeforeAnyWrite(t *testing.T) {
	m := newMetadataTestManager(t)
	// Even requests with few writes must count their CAS comparisons. These
	// empty-range groups fit the write-only budget but exceed the full budget.
	withoutRanges := make([]GroupKeyRanges, etcdutil.MaxEtcdTxnOps/3+1)
	for i := range withoutRanges {
		withoutRanges[i].GroupID = fmt.Sprintf("no-ranges-capacity-%d", i)
	}
	require.ErrorIs(t, m.CreateAffinityGroups(withoutRanges), errs.ErrExceedMaxEtcdTxnOps)
	require.Nil(t, m.pendingMutation)
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
		value, err := m.storage.LoadAffinityGroup(id)
		require.NoError(t, err)
		require.NotEmpty(t, value)
	}
}

func TestAffinityReloadUsesAuthoritativeLabelsWithSeparateCache(t *testing.T) {
	m := newMetadataTestManager(t)
	ids := seedMetadataTestGroups(t, m, []uint64{1, 1})
	cache, err := labeler.NewRegionLabeler(m.ctx, storage.NewStorageWithMemoryBackend(), time.Hour)
	require.NoError(t, err)
	follower, err := NewManager(m.ctx, m.storage, m.storeSetInformer, m.conf, cache)
	require.NoError(t, err)
	require.Zero(t, follower.GetAffinityGroupState(ids[0]).RangeCount)
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
