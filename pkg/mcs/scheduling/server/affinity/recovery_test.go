// Copyright 2025 TiKV Project Authors.
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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	metadata "github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

func TestWatcherRecoveryRetriesPartialBatch(t *testing.T) {
	ctx, client, clean := prepare(t)
	defer clean()
	store := &failingGroupStorage{StorageEndpoint: endpoint.NewStorageEndpoint(kv.NewEtcdKVBase(client), nil)}
	l, err := labeler.NewRegionLabeler(ctx, endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil), time.Hour)
	require.NoError(t, err)
	manager, err := metadata.NewManager(ctx, store, core.NewBasicCluster(), mockconfig.NewTestOptions(), l)
	require.NoError(t, err)
	w := &Watcher{wg: &sync.WaitGroup{}, etcdClient: client, affinityManager: manager}
	watchCtx, cancel := context.WithCancel(ctx)
	w.cancel = cancel
	defer w.Close()
	// The shared adapter must recover even if the event handler partially
	// publishes state and storage remains unavailable on two recovery attempts.
	w.groupWatcher = w.newLoopWatcher(watchCtx, "test-partial-batch", keypath.AffinityGroupsPrefix(), func([]*clientv3.Event) error {
		if err := manager.SyncGroupFromEtcd(&metadata.Group{ID: "ghost"}); err != nil {
			return err
		}
		store.failReloads.Store(2)
		return errors.New("event batch failed after publication")
	})
	require.NoError(t, w.groupWatcher.WaitLoad())
	require.NoError(t, putGroup(ctx, client, &metadata.Group{ID: "trigger"}))
	require.Eventually(t, func() bool {
		return store.failReloads.Load() < 0 && manager.IsGroupExist("trigger") && !manager.IsGroupExist("ghost")
	}, 10*time.Second, 20*time.Millisecond)
}

type loadHookStorage struct {
	*endpoint.StorageEndpoint
	afterGroups func() error
}

func (s *loadHookStorage) LoadAllAffinityGroups(fn func(string, string)) error {
	if err := s.StorageEndpoint.LoadAllAffinityGroups(fn); err != nil {
		return err
	}
	if s.afterGroups != nil {
		return s.afterGroups()
	}
	return nil
}

func TestRecoveryWatchRevisionPrecedesLoad(t *testing.T) {
	ctx, client, clean := prepare(t)
	defer clean()
	store := &loadHookStorage{StorageEndpoint: endpoint.NewStorageEndpoint(kv.NewEtcdKVBase(client), nil)}
	l, err := labeler.NewRegionLabeler(ctx, endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil), time.Hour)
	require.NoError(t, err)
	manager, err := metadata.NewManager(ctx, store, core.NewBasicCluster(), mockconfig.NewTestOptions(), l)
	require.NoError(t, err)
	// Commit after the groups prefix has been read. The new watch must replay
	// this change even though it is absent from the completed Manager snapshot.
	store.afterGroups = func() error { return putGroup(ctx, client, &metadata.Group{ID: "during-load"}) }
	w := &Watcher{etcdClient: client, affinityManager: manager}
	next, err := w.reconcile(ctx)
	require.NoError(t, err)
	require.False(t, manager.IsGroupExist("during-load"))
	watchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ch := client.Watch(watchCtx, keypath.AffinityGroupPath("during-load"), clientv3.WithRev(next))
	select {
	case response := <-ch:
		require.NoError(t, response.Err())
		require.Len(t, response.Events, 1)
		require.Equal(t, keypath.AffinityGroupPath("during-load"), string(response.Events[0].Kv.Key))
	case <-watchCtx.Done():
		t.Fatal("recovery skipped the event committed during its load")
	}
}

type lateCommitStorage struct {
	*endpoint.StorageEndpoint
	armed  atomic.Bool
	ready  chan struct{}
	finish chan struct{}
	done   chan error
}

func TestLocalRangeLateDeleteLabelSnapshotReconcile(t *testing.T) {
	ctx, client, clean := prepare(t)
	defer clean()
	store := &lateCommitStorage{
		StorageEndpoint: endpoint.NewStorageEndpoint(kv.NewEtcdKVBase(client), nil),
		ready:           make(chan struct{}), finish: make(chan struct{}), done: make(chan error, 1),
	}
	l, err := labeler.NewRegionLabeler(ctx, endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil), time.Hour)
	require.NoError(t, err)
	manager, err := metadata.NewManager(ctx, store, core.NewBasicCluster(), mockconfig.NewTestOptions(), l)
	require.NoError(t, err)
	const termKey = "/test/review-label-primary"
	_, err = client.Put(ctx, termKey, "same-primary")
	require.NoError(t, err)
	manager.SetAffinityWriteGuard(func() ([]clientv3.Cmp, bool) {
		return []clientv3.Cmp{clientv3.Compare(clientv3.Value(termKey), "=", "same-primary")}, true
	})
	marker := metadata.GroupKeyRanges{GroupID: "marker", KeyRanges: []keyutil.KeyRange{{StartKey: []byte{10}, EndKey: []byte{11}}}}
	require.NoError(t, manager.CreateAffinityGroups([]metadata.GroupKeyRanges{marker, {GroupID: "x"}}))
	// Exercise the independent label stream. The group keys never change in
	// this scenario, so a healthy group watcher has no batch to reconcile.
	w := &Watcher{wg: &sync.WaitGroup{}, etcdClient: client, affinityManager: manager}
	watchCtx, cancel := context.WithCancel(ctx)
	w.cancel = cancel
	require.NoError(t, w.initializeAffinityLabelWatcher(watchCtx))
	defer w.Close()
	const block = "github.com/tikv/pd/pkg/utils/etcdutil/watchChanBlock"
	require.NoError(t, failpoint.Enable(block, "return(true)"))
	defer func() { require.NoError(t, failpoint.Disable(block)) }()
	x := metadata.GroupKeyRanges{GroupID: "x", KeyRanges: []keyutil.KeyRange{{StartKey: []byte{1}, EndKey: []byte{2}}}}
	require.NoError(t, manager.UpdateAffinityGroupKeyRanges([]metadata.GroupKeyRanges{x}, nil))
	store.armed.Store(true)
	require.ErrorContains(t, manager.UpdateAffinityGroupKeyRanges(nil, []metadata.GroupKeyRanges{x}), "unknown response")
	require.Equal(t, 1, manager.GetAffinityGroupState("x").RangeCount)
	close(store.finish)
	require.NoError(t, <-store.done)
	resp, err := client.Get(ctx, keypath.RegionLabelKeyPath(metadata.GetLabelRuleID("x")))
	require.NoError(t, err)
	require.Empty(t, resp.Kvs)
	_, err = client.Compact(ctx, resp.Header.Revision)
	require.NoError(t, err)
	// Change only the marker cache; its restoration proves that a full label
	// load was successfully published before testing the missing X deletion.
	require.NoError(t, manager.SyncKeyRangesDeleteFromEtcd(metadata.GetLabelRuleID("marker")))
	require.Zero(t, manager.GetAffinityGroupState("marker").RangeCount)
	require.Eventually(t, func() bool {
		w.labelWatcher.ForceLoad()
		return manager.GetAffinityGroupState("marker").RangeCount == 1
	}, 5*time.Second, 20*time.Millisecond)
	require.Zero(t, manager.GetAffinityGroupState("x").RangeCount, "label full load must remove a locally published rule absent from the snapshot")
	require.Nil(t, l.GetLabelRule(metadata.GetLabelRuleID("x")))
}

func (s *lateCommitStorage) RunInTxn(ctx context.Context, f func(kv.Txn) error) error {
	return s.RunInTxnWithConditions(ctx, nil, f)
}

func (s *lateCommitStorage) RunInTxnWithConditions(ctx context.Context, conditions []clientv3.Cmp, f func(kv.Txn) error) error {
	if !s.armed.Swap(false) {
		return s.StorageEndpoint.RunInTxnWithConditions(ctx, conditions, f)
	}
	go func() {
		s.done <- s.StorageEndpoint.RunInTxnWithConditions(ctx, conditions, func(txn kv.Txn) error {
			err := f(txn)
			close(s.ready)
			<-s.finish
			return err
		})
	}()
	<-s.ready
	return errors.New("simulated unknown response before late commit")
}

func TestLocalCreateLateDeleteSnapshotReconcile(t *testing.T) {
	ctx, client, clean := prepare(t)
	defer clean()
	store := &lateCommitStorage{
		StorageEndpoint: endpoint.NewStorageEndpoint(kv.NewEtcdKVBase(client), nil),
		ready:           make(chan struct{}), finish: make(chan struct{}), done: make(chan error, 1),
	}
	l, err := labeler.NewRegionLabeler(ctx, endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil), time.Hour)
	require.NoError(t, err)
	manager, err := metadata.NewManager(ctx, store, core.NewBasicCluster(), mockconfig.NewTestOptions(), l)
	require.NoError(t, err)
	const termKey = "/test/review-primary"
	_, err = client.Put(ctx, termKey, "same-primary")
	require.NoError(t, err)
	manager.SetAffinityWriteGuard(func() ([]clientv3.Cmp, bool) {
		return []clientv3.Cmp{clientv3.Compare(clientv3.Value(termKey), "=", "same-primary")}, true
	})
	// Seed an existing group, used only to prove the full snapshot was applied.
	require.NoError(t, manager.CreateAffinityGroups([]metadata.GroupKeyRanges{{GroupID: "marker"}}))
	w, err := NewWatcher(ctx, client, manager)
	require.NoError(t, err)
	defer w.Close()
	const block = "github.com/tikv/pd/pkg/utils/etcdutil/watchChanBlock"
	require.NoError(t, failpoint.Enable(block, "return(true)"))
	defer func() { require.NoError(t, failpoint.Disable(block)) }()

	// Both business writes use the same Manager. The watch has not observed X.
	require.NoError(t, manager.CreateAffinityGroups([]metadata.GroupKeyRanges{{GroupID: "x"}}))
	store.armed.Store(true)
	require.ErrorContains(t, manager.DeleteAffinityGroups([]string{"x"}, false), "unknown response")
	require.True(t, manager.IsGroupExist("x"), "one reload happened before the late commit")
	close(store.finish)
	require.NoError(t, <-store.done)
	resp, err := client.Get(ctx, keypath.AffinityGroupPath("x"))
	require.NoError(t, err)
	require.Empty(t, resp.Kvs, "delete committed in authoritative storage")
	_, err = client.Compact(ctx, resp.Header.Revision)
	require.NoError(t, err)

	// Make the existing marker locally stale, so observing its original value
	// proves ForceLoad applied the full snapshot, not merely a timer firing.
	marker := manager.GetAffinityGroupState("marker").Group
	marker.CreateTimestamp++
	require.NoError(t, manager.SyncGroupFromEtcd(&marker))
	require.Eventually(t, func() bool {
		w.groupWatcher.ForceLoad()
		return manager.GetAffinityGroupState("marker").CreateTimestamp != marker.CreateTimestamp
	}, 5*time.Second, 20*time.Millisecond)
	require.False(t, manager.IsGroupExist("x"), "authoritative full load must remove locally published keys absent from its snapshot")
}
