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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

func loadStoredGroup(t *testing.T, manager *Manager, groupID string) Group {
	t.Helper()
	value, err := manager.storage.LoadAffinityGroup(groupID)
	require.NoError(t, err)
	var group Group
	require.NoError(t, json.Unmarshal([]byte(value), &group))
	return group
}

// metadataTargetSnapshot is a test fixture for the foundation's narrow target
// submission boundary. It has no store/rule inputs or planner state.
type metadataTargetSnapshot struct {
	original Group
	ranges   GroupKeyRanges
	version  uint64
}

func getMetadataTarget(m *Manager, id string) *metadataTargetSnapshot {
	m.metaMutex.Lock()
	defer m.metaMutex.Unlock()
	if err := m.recoverWritesLocked(m.ctx); err != nil {
		return nil
	}
	group := m.GetAffinityGroupState(id)
	if group == nil {
		return nil
	}
	m.RLock()
	version := m.metadataTables[TableGroupPrefix(id)].version
	m.RUnlock()
	return &metadataTargetSnapshot{original: group.Group, ranges: m.keyRanges[id], version: version}
}

func prepareMetadataTarget(t *testing.T, m *Manager, id string) *metadataTargetSnapshot {
	t.Helper()
	snapshot := getMetadataTarget(m, id)
	require.NotNil(t, snapshot)
	return snapshot
}

func metadataTarget(snapshot *metadataTargetSnapshot, id string, leader uint64) *Group {
	target := snapshot.original
	target.ID, target.LeaderStoreID = id, leader
	return &target
}

func commitMetadataTarget(m *Manager, snapshot *metadataTargetSnapshot, target *Group) (bool, error) {
	return m.commitGroupTarget(m.ctx, snapshot.original, *target, snapshot.ranges, nil, func() bool {
		m.RLock()
		defer m.RUnlock()
		state := m.metadataTables[TableGroupPrefix(target.ID)]
		return state != nil && state.version == snapshot.version
	})
}

func newMetadataTestManager(t *testing.T) *Manager {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	store := storage.NewStorageWithMemoryBackend()
	stores := core.NewStoresInfo()
	for id := uint64(1); id <= 4; id++ {
		stores.PutStore(core.NewStoreInfo(&metapb.Store{Id: id, NodeState: metapb.NodeState_Serving}, core.SetLastHeartbeatTS(time.Now())))
	}
	labelerManager, err := labeler.NewRegionLabeler(ctx, store, time.Second)
	require.NoError(t, err)
	manager, err := NewManager(ctx, store, stores, mockconfig.NewTestOptions(), labelerManager)
	require.NoError(t, err)
	return manager
}

func seedMetadataTestGroups(t *testing.T, manager *Manager, leaders []uint64) []string {
	t.Helper()
	ids := []string{"_tidb_pt_10_p1", "_tidb_pt_10_p2", "_tidb_pt_10_p3"}
	for i, leader := range leaders {
		start, end := []byte{byte(i)}, []byte{byte(i + 1)}
		require.NoError(t, manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: ids[i], KeyRanges: []keyutil.KeyRange{{StartKey: start, EndKey: end}}}}))
		_, err := manager.UpdateAffinityGroupPeers(ids[i], leader, []uint64{1, 2, 3})
		require.NoError(t, err)
		peers := []*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}, {Id: 3, StoreId: 3}}
		region := core.NewRegionInfo(&metapb.Region{Id: uint64(i + 1), StartKey: start, EndKey: end, Peers: peers,
			RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1}}, peers[leader-1])
		_, aligned := manager.GetAndCacheRegionAffinityGroupState(region)
		require.True(t, aligned)
	}
	return ids[:len(leaders)]
}
