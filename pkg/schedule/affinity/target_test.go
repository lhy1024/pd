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
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

func newTargetTestManager(t *testing.T) (*Manager, storage.Storage) {
	store := storage.NewStorageWithMemoryBackend()
	return newTargetTestManagerWithStorage(t, t.Context(), store), store
}

func newTargetTestManagerWithStorage(t *testing.T, ctx context.Context, store storage.Storage) *Manager {
	re := require.New(t)
	stores := core.NewStoresInfo()
	for id := uint64(1); id <= 4; id++ {
		stores.PutStore(core.NewStoreInfo(&metapb.Store{Id: id, Address: fmt.Sprintf("s%d", id), NodeState: metapb.NodeState_Serving},
			core.SetLastHeartbeatTS(time.Now())))
	}
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, 5*time.Second)
	re.NoError(err)
	m, err := NewManager(ctx, store, stores, mockconfig.NewTestOptions(), regionLabeler)
	re.NoError(err)
	return m
}

func loadStoredGroup(re *require.Assertions, store storage.Storage, id string) *Group {
	value, err := store.LoadAffinityGroup(id)
	re.NoError(err)
	group := &Group{}
	re.NoError(json.Unmarshal([]byte(value), group))
	return group
}

func targetFor(m *Manager, id string, leader uint64, voters ...uint64) GroupTarget {
	return GroupTarget{Expected: m.GetAffinityGroupState(id).Group, LeaderStoreID: leader, VoterStoreIDs: voters}
}

func TestUpdateAffinityGroupTargetsRejectsInvalidInput(t *testing.T) {
	re := require.New(t)
	m, store := newTargetTestManager(t)
	re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}, {GroupID: "h"}}))
	for _, targets := range [][]GroupTarget{
		nil,
		{targetFor(m, "g", 1, 1, 2), targetFor(m, "g", 2, 1, 2)},
		{targetFor(m, "g", 3, 1, 2)},
		{targetFor(m, "g", 1, 1, 1, 2)},
		{targetFor(m, "g", 0, 1, 2)},
		{targetFor(m, "g", 9, 9)},
		{targetFor(m, "h", 1, 1, 2), {Expected: Group{ID: "bad id"}, LeaderStoreID: 1, VoterStoreIDs: []uint64{1}}},
	} {
		_, err := m.UpdateAffinityGroupTargets(targets)
		re.Error(err)
	}
	for _, id := range []string{"g", "h"} {
		re.Zero(loadStoredGroup(re, store, id).LeaderStoreID, "validation must precede all writes")
		re.Zero(m.GetAffinityGroupState(id).LeaderStoreID)
	}
	m.applyStoreConditions(map[uint64]storeCondition{4: storeDown})
	stale := targetFor(m, "g", 1, 1, 2)
	stale.Expected.CreateTimestamp++
	for _, skipped := range []GroupTarget{
		{Expected: Group{ID: "missing"}, LeaderStoreID: 1, VoterStoreIDs: []uint64{1}},
		stale, targetFor(m, "g", 1, 1, 4),
	} {
		written, err := m.UpdateAffinityGroupTargets([]GroupTarget{skipped, targetFor(m, "h", 2, 3, 2, 1)})
		re.NoError(err)
		re.Equal([]string{"h"}, written)
	}
	re.Zero(loadStoredGroup(re, store, "g").LeaderStoreID)
	re.Equal([]uint64{1, 2, 3}, m.GetAffinityGroupState("h").VoterStoreIDs)
}

func TestSyncRecreatedAffinityGroup(t *testing.T) {
	for _, path := range []string{"watcher", "conflict", "written", "policy", "peers"} {
		t.Run(path, func(t *testing.T) {
			re := require.New(t)
			m, store := newTargetTestManager(t)
			old := &Group{ID: "g", CreateTimestamp: 1, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}
			m.SyncGroupFromEtcd(old)
			re.NoError(m.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{{GroupID: old.ID, KeyRanges: []keyutil.KeyRange{nonOverlappingRange}}}, nil))
			m.RestoreAffinityGroup(old.ID)
			_, cached := m.GetAndCacheRegionAffinityGroupState(generateRegionForTest(10, old.VoterStoreIDs, nonOverlappingRange))
			re.True(cached)
			recreated := *old
			recreated.CreateTimestamp++
			re.NoError(store.RunInTxn(t.Context(), func(txn kv.Txn) error { return store.SaveAffinityGroup(txn, old.ID, &recreated) }))
			switch path {
			case "watcher":
				m.SyncGroupFromEtcd(&recreated)
			case "conflict", "written":
				target := targetFor(m, old.ID, 2, 1, 2, 3)
				if path == "written" {
					target.Expected = recreated
					recreated.LeaderStoreID = 2
				}
				written, err := m.UpdateAffinityGroupTargets([]GroupTarget{target})
				re.NoError(err)
				re.Equal(path == "written", len(written) == 1)
			case "policy":
				_, err := m.UpdateAffinityGroupPolicy(old.ID, BalancePolicyFixed)
				re.NoError(err)
				recreated.BalancePolicy = BalancePolicyFixed
			case "peers":
				_, err := m.UpdateAffinityGroupPeers(old.ID, 1, []uint64{1, 2, 3})
				re.NoError(err)
			}
			state := m.GetAffinityGroupState(old.ID)
			re.Equal(recreated, state.Group)
			re.Zero(state.RegionCount)
			re.Equal(1, state.RangeCount)
		})
	}
}

func TestRecreatedGroupKeepsRangeConflict(t *testing.T) {
	for _, path := range []string{"watcher", "policy"} {
		t.Run(path, func(t *testing.T) {
			re := require.New(t)
			m, store := newTargetTestManager(t)
			re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}, {GroupID: "h"}}))
			for _, id := range []string{"g", "h"} {
				data, err := json.Marshal(MakeLabelRule(GroupKeyRanges{GroupID: id, KeyRanges: []keyutil.KeyRange{nonOverlappingRange}}))
				re.NoError(err)
				rule, err := labeler.NewLabelRuleFromJSON(data)
				re.NoError(err)
				re.NoError(m.SyncKeyRangesFromEtcd(rule))
			}
			m.ReconcileRangeConflicts()
			before := m.GetAffinityGroupState("g")
			re.True(before.rangeConflict)
			recreated := before.Group
			recreated.CreateTimestamp++
			if path == "watcher" {
				m.SyncGroupFromEtcd(&recreated)
			} else {
				re.NoError(store.RunInTxn(t.Context(), func(txn kv.Txn) error {
					return store.SaveAffinityGroup(txn, "g", &recreated)
				}))
				_, err := m.UpdateAffinityGroupPolicy("g", BalancePolicyAuto)
				re.NoError(err)
			}
			after := m.GetAffinityGroupState("g")
			re.NotSame(before.groupInfoPtr, after.groupInfoPtr)
			re.True(after.rangeConflict)
			re.Equal(1, after.RangeCount)
			re.True(after.RegularSchedulingAllowed)
			re.False(after.AffinitySchedulingAllowed)
			m.SyncKeyRangesDeleteFromEtcd(GetLabelRuleID("h"))
			m.ReconcileRangeConflicts()
			re.False(m.GetAffinityGroupState("g").rangeConflict)
		})
	}
}

func TestTargetAvailability(t *testing.T) {
	re := require.New(t)
	store := storage.NewStorageWithMemoryBackend()
	group := &Group{ID: "g", CreateTimestamp: 1, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}
	re.NoError(store.RunInTxn(t.Context(), func(txn kv.Txn) error { return store.SaveAffinityGroup(txn, group.ID, group) }))
	m := newTargetTestManagerWithStorage(t, t.Context(), store)
	re.False(m.GetAffinityGroupState("g").AffinitySchedulingAllowed)
	m.checkGroupsAvailability()
	re.True(m.GetAffinityGroupState("g").AffinitySchedulingAllowed)
	for _, voters := range [][]uint64{{1, 2, 4}, {1, 2, 3}} {
		m.ExpireAffinityGroup("g")
		m.applyStoreConditions(map[uint64]storeCondition{3: storeRemoving})
		group.VoterStoreIDs = voters
		m.SyncGroupFromEtcd(group)
		state := m.GetAffinityGroupState("g")
		re.Equal(voters[2] != 3, state.AffinitySchedulingAllowed)
		re.Equal(voters[2] == 3, state.RegularSchedulingAllowed)
	}
	group.CreateTimestamp++
	group.VoterStoreIDs = []uint64{1, 2, 4}
	m.SyncGroupFromEtcd(group)
	re.True(m.GetAffinityGroupState("g").AffinitySchedulingAllowed)
}

func TestStoredGroupFields(t *testing.T) {
	base := Group{ID: "g", CreateTimestamp: 1, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2}}
	reordered := base
	reordered.VoterStoreIDs = []uint64{2, 1}
	require.True(t, sameStoredGroup(&base, &reordered))
	for name, change := range map[string]func(*Group){
		"ID":              func(g *Group) { g.ID = "h" },
		"CreateTimestamp": func(g *Group) { g.CreateTimestamp++ },
		"LeaderStoreID":   func(g *Group) { g.LeaderStoreID = 2 },
		"VoterStoreIDs":   func(g *Group) { g.VoterStoreIDs = []uint64{1, 3} },
	} {
		t.Run(name, func(t *testing.T) {
			changed := base
			change(&changed)
			re := require.New(t)
			re.False(sameStoredGroup(&base, &changed))
		})
	}
}

type targetWriterFunc func(context.Context, []GroupTarget) ([]string, error)

func (f targetWriterFunc) WriteGroupTargets(ctx context.Context, targets []GroupTarget) ([]string, error) {
	return f(ctx, targets)
}

func TestFirstWinWithOverlappingRanges(t *testing.T) {
	for _, mode := range []string{"classic", "mcs"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				re := require.New(t)
				m, store := newTargetTestManager(t)
				re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}, {GroupID: "h"}}))
				_, err := m.UpdateAffinityGroupPeers("g", 1, []uint64{1, 2, 3})
				re.NoError(err)
				m.ExpireAffinityGroup("g")
				before := *loadStoredGroup(re, store, "g")
				for _, id := range []string{"g", "h"} {
					data, err := json.Marshal(MakeLabelRule(GroupKeyRanges{GroupID: id, KeyRanges: []keyutil.KeyRange{nonOverlappingRange}}))
					re.NoError(err)
					rule, err := labeler.NewLabelRuleFromJSON(data)
					re.NoError(err)
					re.NoError(m.SyncKeyRangesFromEtcd(rule))
				}
				m.ReconcileRangeConflicts()
				re.True(m.GetAffinityGroupState("g").rangeConflict)
				calls := 0
				if mode == "mcs" {
					m.SetTargetWriter(targetWriterFunc(func(context.Context, []GroupTarget) ([]string, error) {
						calls++
						return nil, nil
					}))
				}
				observe := func() {
					m.ObserveAvailableRegion(generateRegionForTest(10, []uint64{2, 1, 3}, nonOverlappingRange), m.GetAffinityGroupState("g"))
					synctest.Wait()
				}
				observe()
				re.Zero(calls)
				re.Equal(before, *loadStoredGroup(re, store, "g"))
				re.Equal(before, m.GetAffinityGroupState("g").Group)
				m.SyncKeyRangesDeleteFromEtcd(GetLabelRuleID("h"))
				m.ReconcileRangeConflicts()
				re.False(m.GetAffinityGroupState("g").rangeConflict)
				observe()
				if mode == "mcs" {
					re.Equal(1, calls)
					re.Equal(before, *loadStoredGroup(re, store, "g"))
				} else {
					re.Equal(uint64(2), loadStoredGroup(re, store, "g").LeaderStoreID)
				}
			})
		})
	}
}

func TestFirstWinSingleFlightAndGlobalBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		m, store := newTargetTestManager(t)
		re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}, {GroupID: "h"}}))
		calls, failures := 0, testutil.ToFloat64(targetWriteFailureCounter)
		release := make(chan struct{})
		defer close(release)
		var sent GroupTarget
		m.SetTargetWriter(targetWriterFunc(func(_ context.Context, targets []GroupTarget) ([]string, error) {
			calls++
			sent = targets[0]
			if calls == 1 {
				<-release
				return nil, errors.New("PD unavailable")
			}
			return []string{sent.Expected.ID}, nil
		}))
		region := generateRegionForTest(10, []uint64{1, 2}, nonOverlappingRange)
		observe := func(id string) {
			m.ObserveAvailableRegion(region, m.GetAffinityGroupState(id))
			synctest.Wait()
		}
		observe("g")
		for range 10 {
			observe("g")
			observe("h")
		}
		re.Equal(1, calls, "busy observations are discarded")
		release <- struct{}{}
		synctest.Wait()
		observe("h")
		re.Equal(1, calls, "failure delays all groups")
		re.Equal(failures+1, testutil.ToFloat64(targetWriteFailureCounter))
		time.Sleep(time.Second)
		observe("h")
		re.Equal(2, calls)
		re.Equal(GroupTarget{Expected: m.GetAffinityGroupState("h").Group, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2}}, sent)
		re.Zero(m.GetAffinityGroupState("h").LeaderStoreID, "HTTP success waits for the watcher")
		re.Zero(loadStoredGroup(re, store, "h").LeaderStoreID)
		// A store can become unavailable before the watcher publishes the target.
		m.applyStoreConditions(map[uint64]storeCondition{2: storeRemoving})
		m.SyncGroupFromEtcd(sent.result())
		re.False(m.GetAffinityGroupState("h").AffinitySchedulingAllowed)
		re.True(m.GetAffinityGroupState("h").RegularSchedulingAllowed)
		m.applyStoreConditions(map[uint64]storeCondition{})
		re.Zero(m.GetAffinityGroupState("g").LeaderStoreID, "discarded observations are never replayed")
		observe("g")
		re.Equal(3, calls, "success has no cooldown")
		m.SyncGroupFromEtcd(sent.result())
		m.DegradeAffinityGroup("g")
		observe("g")
		re.Equal(3, calls, "a matching Region confirms the target without writing")
		re.True(m.GetAffinityGroupState("g").AffinitySchedulingAllowed)
	})
}

func TestFirstWinCancelsInFlight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		m := newTargetTestManagerWithStorage(t, ctx, storage.NewStorageWithMemoryBackend())
		re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))
		calls := 0
		stopped := false
		m.SetTargetWriter(targetWriterFunc(func(ctx context.Context, _ []GroupTarget) ([]string, error) {
			calls++
			<-ctx.Done()
			stopped = true
			return nil, ctx.Err()
		}))
		region := generateRegionForTest(10, []uint64{1, 2}, nonOverlappingRange)
		m.ObserveAvailableRegion(region, m.GetAffinityGroupState("g"))
		synctest.Wait()
		re.Equal(1, calls)
		cancel()
		synctest.Wait()
		re.True(stopped)
		m.ObserveAvailableRegion(region, m.GetAffinityGroupState("g"))
		synctest.Wait()
		re.Equal(1, calls)
	})
}

func TestFirstWinReplyKeepsNewerWatcherTarget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		m, _ := newTargetTestManager(t)
		re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))
		newer := m.GetAffinityGroupState("g").Group
		newer.LeaderStoreID, newer.VoterStoreIDs = 2, []uint64{1, 2}
		m.SetTargetWriter(targetWriterFunc(func(_ context.Context, _ []GroupTarget) ([]string, error) {
			m.SyncGroupFromEtcd(&newer)
			return []string{"g"}, nil
		}))
		m.ObserveAvailableRegion(generateRegionForTest(10, []uint64{1, 2}, nonOverlappingRange), m.GetAffinityGroupState("g"))
		synctest.Wait()
		re.Equal(newer, m.GetAffinityGroupState("g").Group)
	})
}

type targetReplyStorage struct {
	endpoint.AffinityStorage
	beforeCommit func()
	loseReply    bool
}

func (s *targetReplyStorage) RunInTxn(ctx context.Context, f func(kv.Txn) error) error {
	err := s.AffinityStorage.RunInTxn(ctx, func(txn kv.Txn) error {
		if err := f(txn); err != nil {
			return err
		}
		if s.beforeCommit != nil {
			s.beforeCommit()
		}
		return nil
	})
	if err == nil && s.loseReply {
		return errors.New("committed reply lost")
	}
	return err
}

func TestTargetCASAndLostReplyWithEtcd(t *testing.T) {
	re := require.New(t)
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()
	store := storage.NewStorageWithEtcdBackend(client)
	m := newTargetTestManagerWithStorage(t, t.Context(), store)
	re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}, {GroupID: "h"}}))
	other := newTargetTestManagerWithStorage(t, t.Context(), store)
	stale := targetFor(m, "g", 1, 1, 2, 3)
	commits := 0
	wrapped := &targetReplyStorage{AffinityStorage: store, beforeCommit: func() {
		commits++
		if commits == 2 {
			_, err := other.UpdateAffinityGroupPeers("g", 2, []uint64{1, 2, 3})
			re.NoError(err)
		}
	}}
	m.storage = wrapped
	written, err := m.UpdateAffinityGroupTargets([]GroupTarget{targetFor(m, "h", 1, 1, 2, 3), stale})
	re.Error(err, "another Manager wrote after the read, before commit")
	re.Equal([]string{"h"}, written)
	re.Equal(uint64(1), loadStoredGroup(re, store, "h").LeaderStoreID, "earlier commits survive a later error")
	re.Equal(uint64(2), loadStoredGroup(re, store, "g").LeaderStoreID)
	re.Zero(m.GetAffinityGroupState("g").LeaderStoreID)
	wrapped.beforeCommit = nil
	m.applyStoreConditions(map[uint64]storeCondition{2: storeLeaderEvicted})
	written, err = m.UpdateAffinityGroupTargets([]GroupTarget{stale})
	re.NoError(err)
	re.Empty(written)
	re.Equal(uint64(2), m.GetAffinityGroupState("g").LeaderStoreID)
	re.False(m.GetAffinityGroupState("g").AffinitySchedulingAllowed)
	m.applyStoreConditions(map[uint64]storeCondition{})

	target := targetFor(m, "g", 3, 1, 2, 3)
	wrapped.loseReply = true
	_, err = m.UpdateAffinityGroupTargets([]GroupTarget{target})
	re.ErrorContains(err, "committed reply lost")
	re.Equal(uint64(2), m.GetAffinityGroupState("g").LeaderStoreID)
	re.Equal(uint64(3), loadStoredGroup(re, store, "g").LeaderStoreID)
	wrapped.loseReply = false
	before, err := client.Get(t.Context(), keypath.AffinityGroupPath("g"))
	re.NoError(err)
	target.Expected.VoterStoreIDs = []uint64{3, 2, 1}
	written, err = m.UpdateAffinityGroupTargets([]GroupTarget{target})
	re.NoError(err)
	re.Equal([]string{"g"}, written)
	re.Equal(*loadStoredGroup(re, store, "g"), m.GetAffinityGroupState("g").Group)
	after, err := client.Get(t.Context(), keypath.AffinityGroupPath("g"))
	re.NoError(err)
	re.Equal(before.Kvs[0].ModRevision, after.Kvs[0].ModRevision, "idempotent retries do not put again")
}

// TestUpdateAffinityGroupTargetsChecksPolicy checks that a target decided before
// a policy change is not written.
func TestUpdateAffinityGroupTargetsChecksPolicy(t *testing.T) {
	re := require.New(t)
	manager, store := newTargetTestManager(t)
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))
	target := targetFor(manager, "g", 1, 1, 2, 3)
	_, err := manager.UpdateAffinityGroupPolicy("g", BalancePolicyFixed)
	re.NoError(err)

	written, err := manager.UpdateAffinityGroupTargets([]GroupTarget{target})
	re.NoError(err)
	re.Empty(written)
	re.Zero(loadStoredGroup(re, store, "g").LeaderStoreID)
	re.Equal(BalancePolicyFixed, manager.GetAffinityGroupState("g").BalancePolicy)
}

// replyTestStorage counts group saves and can lose the reply of one committed
// transaction.
type replyTestStorage struct {
	endpoint.AffinityStorage
	saves     int
	loseReply bool
}

func (s *replyTestStorage) SaveAffinityGroup(txn kv.Txn, groupID string, group any) error {
	s.saves++
	return s.AffinityStorage.SaveAffinityGroup(txn, groupID, group)
}

func (s *replyTestStorage) RunInTxn(ctx context.Context, f func(kv.Txn) error) error {
	if err := s.AffinityStorage.RunInTxn(ctx, f); err != nil || !s.loseReply {
		return err
	}
	s.loseReply = false
	return errors.New("the reply of a committed transaction is lost")
}

// TestConfirmationChecksStoredTarget checks that without a target writer a
// Region with the layout of the target in memory confirms it only if storage
// still holds it. A first-win write whose reply was lost may have stored another
// target, and confirming the old one would keep memory and storage apart.
func TestConfirmationChecksStoredTarget(t *testing.T) {
	re := require.New(t)
	m, store := newTargetTestManager(t)
	re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g", KeyRanges: []keyutil.KeyRange{nonOverlappingRange}}}))
	_, err := m.UpdateAffinityGroupPeers("g", 1, []uint64{1, 2, 3})
	re.NoError(err)
	m.checkGroupsAvailability()
	wrapped := &replyTestStorage{AffinityStorage: store}
	m.storage = wrapped

	// A Region with the current target confirms it without a write.
	m.ExpireAffinityGroup("g")
	m.ObserveAvailableRegion(generateRegionForTest(1, []uint64{1, 2, 3}, nonOverlappingRange), m.GetAffinityGroupState("g"))
	re.Zero(wrapped.saves)
	re.True(m.GetAffinityGroupState("g").AffinitySchedulingAllowed)

	// First-win stores leader 2, but the reply is lost, so memory keeps leader 1.
	m.ExpireAffinityGroup("g")
	wrapped.loseReply = true
	m.ObserveAvailableRegion(generateRegionForTest(2, []uint64{2, 1, 3}, nonOverlappingRange), m.GetAffinityGroupState("g"))
	re.Equal(uint64(2), loadStoredGroup(re, store, "g").LeaderStoreID)
	re.Equal(uint64(1), m.GetAffinityGroupState("g").LeaderStoreID)

	// A Region with the old layout does not confirm it: memory takes the stored target.
	m.ObserveAvailableRegion(generateRegionForTest(3, []uint64{1, 2, 3}, nonOverlappingRange), m.GetAffinityGroupState("g"))
	state := m.GetAffinityGroupState("g")
	re.Equal(*loadStoredGroup(re, store, "g"), state.Group)
	re.Equal(uint64(2), state.LeaderStoreID)
}

func TestClassicFirstWinKeepsCommittedPolicy(t *testing.T) {
	for _, hasTarget := range []bool{false, true} {
		t.Run(fmt.Sprintf("has-target-%t", hasTarget), func(t *testing.T) {
			re := require.New(t)
			m, store := newTargetTestManager(t)
			re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))
			if hasTarget {
				_, err := m.UpdateAffinityGroupPeers("g", 1, []uint64{1, 2, 3})
				re.NoError(err)
			}
			wrapped := &replyTestStorage{AffinityStorage: store, loseReply: true}
			m.storage = wrapped
			_, err := m.UpdateAffinityGroupPolicy("g", BalancePolicyFixed)
			re.Error(err)
			re.Empty(m.GetAffinityGroupState("g").BalancePolicy)
			committed := *loadStoredGroup(re, store, "g")
			re.Equal(BalancePolicyFixed, committed.BalancePolicy)
			saves := wrapped.saves
			m.ExpireAffinityGroup("g")
			m.ObserveAvailableRegion(generateRegionForTest(10, []uint64{2, 1, 3}, nonOverlappingRange), m.GetAffinityGroupState("g"))
			re.Equal(saves, wrapped.saves, "stale first-win must not overwrite the committed fixed policy")
			re.Equal(committed, *loadStoredGroup(re, store, "g"))
			re.Equal(committed, m.GetAffinityGroupState("g").Group)
			m.ObserveAvailableRegion(generateRegionForTest(11, []uint64{2, 1, 3}, nonOverlappingRange), m.GetAffinityGroupState("g"))
			state := m.GetAffinityGroupState("g")
			re.Equal(BalancePolicyFixed, state.BalancePolicy)
			re.Equal(uint64(2), state.LeaderStoreID)
			re.Equal(state.Group, *loadStoredGroup(re, store, "g"))
		})
	}
}

func TestMCSFirstWinKeepsFixedTarget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		m, _ := newTargetTestManager(t)
		re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))
		_, err := m.UpdateAffinityGroupPeers("g", 1, []uint64{1, 2})
		re.NoError(err)
		m.DegradeAffinityGroup("g")
		observed := m.GetAffinityGroupState("g")
		_, err = m.UpdateAffinityGroupPolicy("g", BalancePolicyFixed)
		re.NoError(err)
		writer := &fakeTargetWriter{reply: func(target GroupTarget) ([]string, error) {
			return []string{target.Expected.ID}, nil
		}}
		m.SetTargetWriter(writer)
		m.ObserveAvailableRegion(generateRegionForTest(10, []uint64{2, 1}, nonOverlappingRange), observed)
		synctest.Wait()
		re.Empty(writer.sent())
		re.Equal(uint64(1), m.GetAffinityGroupState("g").LeaderStoreID)
	})
}
