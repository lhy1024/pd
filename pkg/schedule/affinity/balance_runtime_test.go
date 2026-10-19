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
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

type balanceSamplingStorage struct {
	storage.Storage
	after func()
	once  sync.Once
}

func (s *balanceSamplingStorage) RunInTxn(ctx context.Context, f func(kv.Txn) error) error {
	err := s.Storage.RunInTxn(ctx, f)
	if err == nil && s.after != nil {
		s.once.Do(s.after)
	}
	return err
}

func TestGetBalanceSnapshotScopesAndCopiesTableState(t *testing.T) {
	manager := newMetadataTestManager(t)
	ids := seedMetadataTestGroups(t, manager, []uint64{1, 2, 3})
	otherTableID := "_tidb_pt_20_p1"
	require.NoError(t, manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: otherTableID, KeyRanges: []keyutil.KeyRange{{StartKey: []byte{10}, EndKey: []byte{11}}}}}))
	_, err := manager.UpdateAffinityGroupPeers(otherTableID, 4, []uint64{2, 3, 4})
	require.NoError(t, err)

	snapshot := manager.GetBalanceSnapshot(ids[0])
	require.NotNil(t, snapshot)
	require.Equal(t, TableGroupPrefix(ids[0]), snapshot.TableID)
	require.Len(t, snapshot.Groups, len(ids))
	require.ElementsMatch(t, ids, []string{snapshot.Groups[0].ID, snapshot.Groups[1].ID, snapshot.Groups[2].ID})
	require.NotContains(t, snapshot.Ranges, otherTableID)
	require.ElementsMatch(t, []uint64{1, 2, 3, 4}, snapshot.Stores)

	snapshot.Groups[0].LeaderStoreID = 4
	ranges := snapshot.Ranges[ids[0]]
	ranges.KeyRanges[0].StartKey[0] = 9
	snapshot.Ranges[ids[0]] = ranges

	fresh := manager.GetBalanceSnapshot(ids[0])
	require.Equal(t, uint64(1), fresh.Groups[0].LeaderStoreID)
	require.Equal(t, byte(0), fresh.Ranges[ids[0]].KeyRanges[0].StartKey[0])
}

func TestBalanceSnapshotVersionTracksTargetAndMembership(t *testing.T) {
	manager := newMetadataTestManager(t)
	ids := seedMetadataTestGroups(t, manager, []uint64{1, 1})
	initial := manager.GetBalanceSnapshot(ids[0])

	_, err := manager.UpdateAffinityGroupPeers(ids[0], 2, []uint64{1, 2, 3})
	require.NoError(t, err)
	afterTarget := manager.GetBalanceSnapshot(ids[0])
	require.Greater(t, afterTarget.Version.Table, initial.Version.Table)
	require.Equal(t, uint64(2), afterTarget.Groups[0].LeaderStoreID)

	newID := "_tidb_pt_10_p3"
	require.NoError(t, manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: newID, KeyRanges: []keyutil.KeyRange{{StartKey: []byte{2}, EndKey: []byte{3}}}}}))
	afterAdd := manager.GetBalanceSnapshot(ids[0])
	require.Greater(t, afterAdd.Version.Table, afterTarget.Version.Table)
	require.Len(t, afterAdd.Groups, 3)

	require.NoError(t, manager.DeleteAffinityGroups([]string{newID}, true))
	afterDelete := manager.GetBalanceSnapshot(ids[0])
	require.Greater(t, afterDelete.Version.Table, afterAdd.Version.Table)
	require.Len(t, afterDelete.Groups, 2)
}

func TestBalanceSnapshotVersionTracksPlacementRules(t *testing.T) {
	manager := newMetadataTestManager(t)
	id := seedMetadataTestGroups(t, manager, []uint64{1})[0]
	var rulesRevision uint64
	manager.SetBalanceRuleRevision(func() uint64 { return rulesRevision })
	initial := manager.GetBalanceSnapshot(id)
	rulesRevision++
	updated := manager.GetBalanceSnapshot(id)
	require.NotEqual(t, initial.Version, updated.Version)
	require.Equal(t, uint64(1), updated.Version.Rules)
}

func TestBalanceSnapshotRejectsSameIDRebuild(t *testing.T) {
	manager := newMetadataTestManager(t)
	ids := seedMetadataTestGroups(t, manager, []uint64{1})
	old := manager.GetBalanceSnapshot(ids[0])
	require.NoError(t, manager.DeleteAffinityGroups(ids, true))
	require.Nil(t, manager.GetBalanceSnapshot(ids[0]))
	require.NoError(t, manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: ids[0], KeyRanges: []keyutil.KeyRange{{StartKey: []byte{0}, EndKey: []byte{1}}}}}))
	_, err := manager.UpdateAffinityGroupPeers(ids[0], 1, []uint64{1, 2, 3})
	require.NoError(t, err)

	current := manager.GetBalanceSnapshot(ids[0])
	require.NotNil(t, current)
	require.False(t, manager.BalanceSnapshotCurrent(old))
	require.NotEqual(t, old.Version, current.Version)
	require.Greater(t, current.Version.Table, old.Version.Table)
}

func TestObserveBalanceRegionVersionTracksRelevantChanges(t *testing.T) {
	for _, tc := range []struct {
		name        string
		change      func(*core.RegionInfo) *core.RegionInfo
		invalidates bool
	}{
		{
			name: "approximate size",
			change: func(region *core.RegionInfo) *core.RegionInfo {
				return region.Clone(core.SetApproximateSize(100))
			},
		},
		{
			name:        "peers",
			invalidates: true,
			change: func(region *core.RegionInfo) *core.RegionInfo {
				return region.Clone(core.SetPeers([]*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}, {Id: 3, StoreId: 4}}))
			},
		},
		{
			name:        "leader",
			invalidates: true,
			change: func(region *core.RegionInfo) *core.RegionInfo {
				return region.Clone(core.WithLeader(&metapb.Peer{Id: 2, StoreId: 2}))
			},
		},
		{
			name:        "ranges",
			invalidates: true,
			change: func(region *core.RegionInfo) *core.RegionInfo {
				return region.Clone(core.WithStartKey([]byte{9}))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager := newMetadataTestManager(t)
			ids := seedMetadataTestGroups(t, manager, []uint64{1})
			manager.GetBalanceSnapshot(ids[0])
			before, ok := manager.GetBalanceVersion(ids[0])
			require.True(t, ok)
			region := manager.regions[1].balanceInput()

			manager.ObserveBalanceRegion(tc.change(region))

			after, ok := manager.GetBalanceVersion(ids[0])
			require.True(t, ok)
			if tc.invalidates {
				require.Greater(t, after.Table, before.Table)
			} else {
				require.Equal(t, before, after)
			}
		})
	}
}
