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
	"reflect"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

func newTargetTestManager(t *testing.T) (*Manager, storage.Storage) {
	store := storage.NewStorageWithMemoryBackend()
	return newTargetTestManagerWithStorage(t, store), store
}

func newTargetTestManagerWithStorage(t *testing.T, store storage.Storage) *Manager {
	return newTargetTestManagerWith(t, t.Context(), store, time.Now())
}

func newTargetTestManagerWith(t *testing.T, ctx context.Context, store storage.Storage, heartbeat time.Time) *Manager {
	re := require.New(t)
	storeInfos := core.NewStoresInfo()
	for id := uint64(1); id <= 4; id++ {
		storeInfo := core.NewStoreInfo(&metapb.Store{Id: id, Address: fmt.Sprintf("s%d", id), NodeState: metapb.NodeState_Serving})
		storeInfos.PutStore(storeInfo.Clone(core.SetLastHeartbeatTS(heartbeat)))
	}
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, 5*time.Second)
	re.NoError(err)
	manager, err := NewManager(ctx, store, storeInfos, mockconfig.NewTestOptions(), regionLabeler)
	re.NoError(err)
	return manager
}

func loadStoredGroup(re *require.Assertions, store storage.Storage, id string) *Group {
	value, err := store.LoadAffinityGroup(id)
	re.NoError(err)
	re.NotEmpty(value)
	group := &Group{}
	re.NoError(json.Unmarshal([]byte(value), group))
	return group
}

func targetFor(m *Manager, id string, leader uint64, voters ...uint64) GroupTarget {
	return GroupTarget{Expected: m.GetAffinityGroupState(id).Group, LeaderStoreID: leader, VoterStoreIDs: voters}
}

func TestUpdateAffinityGroupTargets(t *testing.T) {
	re := require.New(t)
	manager, store := newTargetTestManager(t)
	const id = "g"
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "stale"}, {GroupID: id}}))

	// A target for a missing or changed group is not written.
	stale := targetFor(manager, "stale", 2, 3, 2, 1)
	stale.Expected.CreateTimestamp++
	written, err := manager.UpdateAffinityGroupTargets([]GroupTarget{
		{Expected: Group{ID: "missing"}, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}},
		stale,
		targetFor(manager, id, 2, 3, 2, 1),
	})
	re.NoError(err)
	re.Equal([]string{id}, written)
	state := manager.GetAffinityGroupState(id)
	re.True(state.AffinitySchedulingAllowed)
	re.Equal([]uint64{1, 2, 3}, state.VoterStoreIDs)
	re.Equal(state.Group, *loadStoredGroup(re, store, id))
	re.Zero(manager.GetAffinityGroupState("stale").LeaderStoreID)
	re.Zero(loadStoredGroup(re, store, "stale").LeaderStoreID)

	// A commit that the Manager did not see leaves its memory stale. A target
	// decided from the stale memory is not written, and the memory is refreshed
	// with the availability that the store conditions known now give.
	committed := manager.GetAffinityGroupState(id).Group
	committed.LeaderStoreID = 1
	re.NoError(store.RunInTxn(t.Context(), func(txn kv.Txn) error { return store.SaveAffinityGroup(txn, id, &committed) }))
	manager.Lock()
	manager.unavailableStores = map[uint64]storeCondition{1: storeLeaderEvicted}
	manager.Unlock()
	written, err = manager.UpdateAffinityGroupTargets([]GroupTarget{targetFor(manager, id, 3, 1, 2, 3)})
	re.NoError(err)
	re.Empty(written)
	re.Equal(committed, manager.GetAffinityGroupState(id).Group)
	re.False(manager.GetAffinityGroupState(id).AffinitySchedulingAllowed)
	manager.Lock()
	manager.unavailableStores = map[uint64]storeCondition{}
	manager.Unlock()

	// The next target is decided from the refreshed group, whose voter order does not matter.
	target := targetFor(manager, id, 3, 1, 2, 3)
	slices.Reverse(target.Expected.VoterStoreIDs)
	written, err = manager.UpdateAffinityGroupTargets([]GroupTarget{target})
	re.NoError(err)
	re.Equal([]string{id}, written)
	re.Equal(uint64(3), loadStoredGroup(re, store, id).LeaderStoreID)
	written, err = manager.UpdateAffinityGroupTargets([]GroupTarget{target})
	re.NoError(err)
	re.Equal([]string{id}, written, "an identical retry reports the committed target")
	re.True(manager.GetAffinityGroupState(id).AffinitySchedulingAllowed)
}

// TestSyncRecreatedAffinityGroup checks that a group deleted and created again
// with the same ID replaces the old instance, when the scheduling service
// reloads it, when a conflicting write refreshes it and when a policy or peers
// update finds it in storage. The new instance keeps the old target, so only
// its create timestamp tells them apart.
func TestSyncRecreatedAffinityGroup(t *testing.T) {
	for _, path := range []string{"reload", "refresh", "current expected", "policy", "peers"} {
		t.Run(path, func(t *testing.T) {
			re := require.New(t)
			manager, store := newTargetTestManager(t)
			old := &Group{ID: "g", CreateTimestamp: 1, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}
			manager.SyncGroupFromEtcd(old)
			ranges := GroupKeyRanges{GroupID: old.ID, KeyRanges: []keyutil.KeyRange{nonOverlappingRange}}
			re.NoError(manager.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{ranges}, nil))
			manager.RestoreAffinityGroup(old.ID)
			_, isAffinity := manager.GetAndCacheRegionAffinityGroupState(generateRegionForTest(10, old.VoterStoreIDs, ranges.KeyRanges[0]))
			re.True(isAffinity)

			recreated := *old
			recreated.CreateTimestamp = 2
			re.NoError(store.RunInTxn(t.Context(), func(txn kv.Txn) error { return store.SaveAffinityGroup(txn, old.ID, &recreated) }))
			var err error
			switch path {
			case "reload":
				manager.SyncGroupFromEtcd(&recreated)
			case "refresh", "current expected":
				target := targetFor(manager, old.ID, 2, 1, 2, 3)
				if path == "current expected" {
					target.Expected = recreated
					recreated.LeaderStoreID = 2
				}
				var written []string
				written, err = manager.UpdateAffinityGroupTargets([]GroupTarget{target})
				re.Equal(path == "current expected", len(written) == 1)
			case "policy":
				_, err = manager.UpdateAffinityGroupPolicy(old.ID, BalancePolicyFixed)
				recreated.BalancePolicy = BalancePolicyFixed
			case "peers":
				_, err = manager.UpdateAffinityGroupPeers(old.ID, 1, []uint64{1, 2, 3})
			}
			re.NoError(err)
			state := manager.GetAffinityGroupState(old.ID)
			re.Equal(recreated, state.Group)
			re.Zero(state.RegionCount)
			re.Equal(1, state.RangeCount)
			if path == "current expected" {
				re.True(state.AffinitySchedulingAllowed)
			}
			written, err := manager.UpdateAffinityGroupTargets([]GroupTarget{targetFor(manager, old.ID, 2, 1, 2, 3)})
			re.NoError(err)
			re.Equal([]string{old.ID}, written)
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

func TestUpdateAffinityGroupTargetsRejectsInvalidInput(t *testing.T) {
	re := require.New(t)
	manager, store := newTargetTestManager(t)
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}, {GroupID: "h"}}))
	_, err := manager.UpdateAffinityGroupTargets(nil)
	re.True(errs.ErrEmptyRequest.Equal(err))

	manager.Lock()
	manager.unavailableStores = map[uint64]storeCondition{4: storeDown}
	manager.Unlock()
	for _, targets := range [][]GroupTarget{
		{targetFor(manager, "g", 1, 1, 2), targetFor(manager, "g", 2, 1, 2)},
		{targetFor(manager, "g", 3, 1, 2)},
		{targetFor(manager, "g", 1, 1, 1, 2)},
		{targetFor(manager, "g", 0, 1, 2)},
		{targetFor(manager, "g", 9, 9)},
		{targetFor(manager, "h", 1, 1, 2), {Expected: Group{ID: "bad id"}, LeaderStoreID: 1, VoterStoreIDs: []uint64{1}}},
	} {
		_, err := manager.UpdateAffinityGroupTargets(targets)
		re.Error(err)
	}
	// An invalid request writes nothing.
	for _, id := range []string{"g", "h"} {
		re.Zero(manager.GetAffinityGroupState(id).LeaderStoreID)
		re.Zero(loadStoredGroup(re, store, id).LeaderStoreID)
	}

	// A target with a store that PD knows to be unavailable is skipped, and the
	// others are written.
	written, err := manager.UpdateAffinityGroupTargets([]GroupTarget{targetFor(manager, "g", 1, 1, 4), targetFor(manager, "h", 1, 1, 2)})
	re.NoError(err)
	re.Equal([]string{"h"}, written)
	re.Zero(loadStoredGroup(re, store, "g").LeaderStoreID)
	re.Equal(uint64(1), loadStoredGroup(re, store, "h").LeaderStoreID)
}

type fakeTargetWriter struct {
	mu      sync.Mutex
	targets []GroupTarget
	// write runs before the reply, like the watcher delivering a change.
	write func(GroupTarget)
	reply func(GroupTarget) ([]string, error)
}

func (w *fakeTargetWriter) WriteGroupTargets(_ context.Context, targets []GroupTarget) ([]string, error) {
	w.mu.Lock()
	w.targets = append(w.targets, targets...)
	w.mu.Unlock()
	if w.write != nil {
		w.write(targets[0])
	}
	return w.reply(targets[0])
}

// sent returns the targets sent to the writer.
func (w *fakeTargetWriter) sent() []GroupTarget {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.targets)
}

func TestObserveAvailableRegionWithTargetWriter(t *testing.T) {
	region := generateRegionForTest(10, []uint64{1, 2}, nonOverlappingRange)
	written := func(target GroupTarget) ([]string, error) { return []string{target.Expected.ID}, nil }
	for _, tc := range []struct {
		name    string
		reply   func(GroupTarget) ([]string, error)
		written bool
		failed  bool
	}{
		{name: "written", reply: written, written: true},
		{name: "not written", reply: func(GroupTarget) ([]string, error) { return nil, nil }},
		{name: "write failed", reply: func(GroupTarget) ([]string, error) { return nil, errors.New("PD is unavailable") }, failed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				re := require.New(t)
				manager, store := newTargetTestManager(t)
				re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))
				group := manager.GetAffinityGroupState("g")
				writer := &fakeTargetWriter{reply: tc.reply}
				manager.SetTargetWriter(writer)

				failures := testutil.ToFloat64(targetWriteFailureCounter)
				manager.ObserveAvailableRegion(region, group)
				// The target is sent in the background.
				synctest.Wait()
				re.Equal([]GroupTarget{{Expected: group.Group, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2}}}, writer.sent())
				re.Equal(tc.failed, testutil.ToFloat64(targetWriteFailureCounter) > failures)
				state := manager.GetAffinityGroupState("g")
				re.Equal(group.Group, state.Group, "the reply must not publish the target")
				re.False(state.AffinitySchedulingAllowed)
				// The writer stores the target; the Manager does not write its own storage.
				re.Zero(loadStoredGroup(re, store, "g").LeaderStoreID)
				if tc.written {
					manager.SyncGroupFromEtcd(writer.sent()[0].result())
					state = manager.GetAffinityGroupState("g")
					re.Equal(*writer.sent()[0].result(), state.Group)
					re.True(state.AffinitySchedulingAllowed)
				}
			})
		})
	}
}

// TestTargetAvailability checks that a group takes the availability of its target
// from the store conditions known when the target is set, because the
// availability check applies the conditions only when they change.
func TestTargetAvailability(t *testing.T) {
	allowed := func(m *Manager, id string) bool { return m.GetAffinityGroupState(id).AffinitySchedulingAllowed }

	t.Run("loaded", func(t *testing.T) {
		re := require.New(t)
		store := storage.NewStorageWithMemoryBackend()
		group := &Group{ID: "g", CreateTimestamp: 1, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}
		re.NoError(store.RunInTxn(t.Context(), func(txn kv.Txn) error { return store.SaveAffinityGroup(txn, group.ID, group) }))
		manager := newTargetTestManagerWithStorage(t, store)
		re.False(allowed(manager, "g"))
		// The first check sets the loaded group, although no store condition changed.
		manager.checkGroupsAvailability()
		re.True(allowed(manager, "g"))
	})

	t.Run("synced", func(t *testing.T) {
		re := require.New(t)
		manager, _ := newTargetTestManager(t)
		manager.applyStoreConditions(map[uint64]storeCondition{})
		group := &Group{ID: "g", CreateTimestamp: 1, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}
		manager.SyncGroupFromEtcd(group)
		re.True(allowed(manager, "g"))

		// An admin target that reaches an expired group through the watcher is used.
		manager.ExpireAffinityGroup("g")
		group.VoterStoreIDs = []uint64{1, 2, 4}
		manager.SyncGroupFromEtcd(group)
		re.True(allowed(manager, "g"))

		// A target synced with an unavailable store is not.
		manager.applyStoreConditions(map[uint64]storeCondition{3: storeRemoving})
		group.VoterStoreIDs = []uint64{1, 2, 3}
		manager.SyncGroupFromEtcd(group)
		re.False(allowed(manager, "g"))
		re.True(manager.GetAffinityGroupState("g").RegularSchedulingAllowed)
		recreated := &Group{ID: "g", CreateTimestamp: 2, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 4}}
		manager.SyncGroupFromEtcd(recreated)
		re.True(allowed(manager, "g"))
	})

	t.Run("written", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			re := require.New(t)
			manager, _ := newTargetTestManager(t)
			re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))
			// Store 2 starts to be removed while the target is written.
			manager.SetTargetWriter(&fakeTargetWriter{
				write: func(GroupTarget) { manager.applyStoreConditions(map[uint64]storeCondition{2: storeRemoving}) },
				reply: func(target GroupTarget) ([]string, error) { return []string{target.Expected.ID}, nil },
			})
			region := generateRegionForTest(10, []uint64{1, 2}, nonOverlappingRange)
			manager.ObserveAvailableRegion(region, manager.GetAffinityGroupState("g"))
			synctest.Wait()
			writer := manager.targetWriter.(*fakeTargetWriter)
			manager.SyncGroupFromEtcd(writer.sent()[0].result())
			state := manager.GetAffinityGroupState("g")
			re.Equal([]uint64{1, 2}, state.VoterStoreIDs)
			re.False(state.AffinitySchedulingAllowed)
			re.True(state.RegularSchedulingAllowed)
		})
	})
}

// TestObserveAvailableRegionConfirmsTarget checks that a Region with the layout
// of the target of a degraded group confirms the target without writing it.
func TestObserveAvailableRegionConfirmsTarget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		manager, _ := newTargetTestManager(t)
		re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))
		_, err := manager.UpdateAffinityGroupTargets([]GroupTarget{targetFor(manager, "g", 1, 1, 2)})
		re.NoError(err)
		writer := &fakeTargetWriter{reply: func(GroupTarget) ([]string, error) { return nil, errors.New("unexpected write") }}
		manager.SetTargetWriter(writer)
		manager.DegradeAffinityGroup("g")
		re.False(manager.GetAffinityGroupState("g").AffinitySchedulingAllowed)

		region := generateRegionForTest(10, []uint64{1, 2}, nonOverlappingRange)
		manager.ObserveAvailableRegion(region, manager.GetAffinityGroupState("g"))
		synctest.Wait()
		re.Empty(writer.sent())
		re.True(manager.GetAffinityGroupState("g").AffinitySchedulingAllowed)
	})
}

// TestStoredGroupFields checks comparison and synchronization of every persisted field.
func TestStoredGroupFields(t *testing.T) {
	base := Group{ID: "g", CreateTimestamp: 1, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2}}
	reordered := base
	reordered.VoterStoreIDs = []uint64{2, 1}
	require.True(t, sameStoredGroup(&base, &reordered))
	typ := reflect.TypeFor[Group]()
	for i := range typ.NumField() {
		t.Run(typ.Field(i).Name, func(t *testing.T) {
			re := require.New(t)
			changed := base
			changed.VoterStoreIDs = slices.Clone(base.VoterStoreIDs)
			field := reflect.ValueOf(&changed).Elem().Field(i)
			switch field.Kind() {
			case reflect.String:
				field.SetString(field.String() + "x")
			case reflect.Uint64:
				field.SetUint(field.Uint() + 1)
			case reflect.Slice:
				field.Set(reflect.Append(field, reflect.ValueOf(uint64(3))))
			default:
				re.Failf("unexpected field", "field %s has kind %s", typ.Field(i).Name, field.Kind())
			}
			re.False(sameStoredGroup(&base, &changed))
			if typ.Field(i).Name != "ID" {
				manager, _ := newTargetTestManager(t)
				manager.SyncGroupFromEtcd(&base)
				manager.SyncGroupFromEtcd(&changed)
				re.Equal(changed, manager.GetAffinityGroupState(base.ID).Group)
			}
		})
	}
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
