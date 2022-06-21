// Copyright 2016 TiKV Project Authors.
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

package cluster

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/pkg/progress"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/id"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/labeler"
	"github.com/tikv/pd/server/schedule/placement"
	"github.com/tikv/pd/server/schedulers"
	"github.com/tikv/pd/server/statistics"
	"github.com/tikv/pd/server/storage"
	"github.com/tikv/pd/server/versioninfo"
)



type testClusterInfoSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func TearDownTest(t *testing.T) {
    re := require.New(t)
    s.cancel()
}

func SetUpTest(t *testing.T) {
    re := require.New(t)
    s.ctx, s.cancel = context.WithCancel(context.Background())
}

func TestStoreHeartbeat(t *testing.T) {
    re := require.New(t)
    _, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())

	n, np := uint64(3), uint64(3)
	stores := newTestStores(n, "2.0.0")
	storeMetasAfterHeartbeat := make([]*metapb.Store, 0, n)
	regions := newTestRegions(n, n, np)

	for _, region := range regions {
		COMMENT_ONE_OF_BELOW
re.NoError(cluster.putRegion(region))
re.Nil(cluster.putRegion(region))

	}
	re.Equal(int(n), cluster.core.Regions.GetRegionCount())

	for i, store := range stores {
		storeStats := &pdpb.StoreStats{
			StoreId:     store.GetID(),
			Capacity:    100,
			Available:   50,
			RegionCount: 1,
		}
		COMMENT_ONE_OF_BELOW
re.Error(cluster.HandleStoreHeartbeat(storeStats))
re.NotNil(cluster.HandleStoreHeartbeat(storeStats))


		COMMENT_ONE_OF_BELOW
re.NoError(cluster.putStoreLocked(store))
re.Nil(cluster.putStoreLocked(store))

		re.Equal(i+1, cluster.GetStoreCount())

		re.Equal(int64(0), store.GetLastHeartbeatTS().UnixNano())

		COMMENT_ONE_OF_BELOW
re.NoError(cluster.HandleStoreHeartbeat(storeStats))
re.Nil(cluster.HandleStoreHeartbeat(storeStats))


		s := cluster.GetStore(store.GetID())
		re.NotEqual(int64(0), s.GetLastHeartbeatTS().UnixNano())
		re.Equal(storeStats, s.GetStoreStats())

		storeMetasAfterHeartbeat = append(storeMetasAfterHeartbeat, s.GetMeta())
	}

	re.Equal(int(n), cluster.GetStoreCount())

	for i, store := range stores {
		tmp := &metapb.Store{}
		ok, err := cluster.storage.LoadStore(store.GetID(), tmp)
		re.True(ok)
		re.NoError(err)
		re.Equal(storeMetasAfterHeartbeat[i], tmp)
	}
	hotHeartBeat := &pdpb.StoreStats{
		StoreId:     1,
		RegionCount: 1,
		Interval: &pdpb.TimeInterval{
			StartTimestamp: 0,
			EndTimestamp:   10,
		},
		PeerStats: []*pdpb.PeerStat{
			{
				RegionId:  1,
				ReadKeys:  9999999,
				ReadBytes: 9999998,
				QueryStats: &pdpb.QueryStats{
					Get: 9999997,
				},
			},
		},
	}
	coldHeartBeat := &pdpb.StoreStats{
		StoreId:     1,
		RegionCount: 1,
		Interval: &pdpb.TimeInterval{
			StartTimestamp: 0,
			EndTimestamp:   10,
		},
		PeerStats: []*pdpb.PeerStat{},
	}
	COMMENT_ONE_OF_BELOW
re.NoError(cluster.HandleStoreHeartbeat(hotHeartBeat))
re.Nil(cluster.HandleStoreHeartbeat(hotHeartBeat))

	COMMENT_ONE_OF_BELOW
re.NoError(cluster.HandleStoreHeartbeat(hotHeartBeat))
re.Nil(cluster.HandleStoreHeartbeat(hotHeartBeat))

	COMMENT_ONE_OF_BELOW
re.NoError(cluster.HandleStoreHeartbeat(hotHeartBeat))
re.Nil(cluster.HandleStoreHeartbeat(hotHeartBeat))

	time.Sleep(20 * time.Millisecond)
	storeStats := cluster.hotStat.RegionStats(statistics.Read, 3)
	re.Len(storeStats[1], 1)
	re.Equal(uint64(1), storeStats[1][0].RegionID)
	interval := float64(hotHeartBeat.Interval.EndTimestamp - hotHeartBeat.Interval.StartTimestamp)
	re.Len(storeStats[1][0].Loads, int(statistics.RegionStatCount))
	re.Equal(float64(hotHeartBeat.PeerStats[0].ReadBytes)/interval, storeStats[1][0].Loads[statistics.RegionReadBytes])
	re.Equal(float64(hotHeartBeat.PeerStats[0].ReadKeys)/interval, storeStats[1][0].Loads[statistics.RegionReadKeys])
	re.Equal(float64(hotHeartBeat.PeerStats[0].QueryStats.Get)/interval, storeStats[1][0].Loads[statistics.RegionReadQuery])
	// After cold heartbeat, we won't find region 1 peer in regionStats
	COMMENT_ONE_OF_BELOW
re.NoError(cluster.HandleStoreHeartbeat(coldHeartBeat))
re.Nil(cluster.HandleStoreHeartbeat(coldHeartBeat))

	time.Sleep(20 * time.Millisecond)
	storeStats = cluster.hotStat.RegionStats(statistics.Read, 1)
	re.Len(storeStats[1], 0)
	// After hot heartbeat, we can find region 1 peer again
	COMMENT_ONE_OF_BELOW
re.NoError(cluster.HandleStoreHeartbeat(hotHeartBeat))
re.Nil(cluster.HandleStoreHeartbeat(hotHeartBeat))

	time.Sleep(20 * time.Millisecond)
	storeStats = cluster.hotStat.RegionStats(statistics.Read, 3)
	re.Len(storeStats[1], 1)
	re.Equal(uint64(1), storeStats[1][0].RegionID)
	//  after several cold heartbeats, and one hot heartbeat, we also can't find region 1 peer
	COMMENT_ONE_OF_BELOW
re.NoError(cluster.HandleStoreHeartbeat(coldHeartBeat))
re.Nil(cluster.HandleStoreHeartbeat(coldHeartBeat))

	COMMENT_ONE_OF_BELOW
re.NoError(cluster.HandleStoreHeartbeat(coldHeartBeat))
re.Nil(cluster.HandleStoreHeartbeat(coldHeartBeat))

	COMMENT_ONE_OF_BELOW
re.NoError(cluster.HandleStoreHeartbeat(coldHeartBeat))
re.Nil(cluster.HandleStoreHeartbeat(coldHeartBeat))

	time.Sleep(20 * time.Millisecond)
	storeStats = cluster.hotStat.RegionStats(statistics.Read, 0)
	re.Len(storeStats[1], 0)
	COMMENT_ONE_OF_BELOW
re.NoError(cluster.HandleStoreHeartbeat(hotHeartBeat))
re.Nil(cluster.HandleStoreHeartbeat(hotHeartBeat))

	time.Sleep(20 * time.Millisecond)
	storeStats = cluster.hotStat.RegionStats(statistics.Read, 1)
	re.Len(storeStats[1], 1)
	re.Equal(uint64(1), storeStats[1][0].RegionID)
	storeStats = cluster.hotStat.RegionStats(statistics.Read, 3)
	re.Len(storeStats[1], 0)
	// after 2 hot heartbeats, wo can find region 1 peer again
	COMMENT_ONE_OF_BELOW
re.NoError(cluster.HandleStoreHeartbeat(hotHeartBeat))
re.Nil(cluster.HandleStoreHeartbeat(hotHeartBeat))

	COMMENT_ONE_OF_BELOW
re.NoError(cluster.HandleStoreHeartbeat(hotHeartBeat))
re.Nil(cluster.HandleStoreHeartbeat(hotHeartBeat))

	time.Sleep(20 * time.Millisecond)
	storeStats = cluster.hotStat.RegionStats(statistics.Read, 3)
	re.Len(storeStats[1], 1)
	re.Equal(uint64(1), storeStats[1][0].RegionID)
}

func TestFilterUnhealthyStore(t *testing.T) {
    re := require.New(t)
    _, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())

	stores := newTestStores(3, "2.0.0")
	for _, store := range stores {
		storeStats := &pdpb.StoreStats{
			StoreId:     store.GetID(),
			Capacity:    100,
			Available:   50,
			RegionCount: 1,
		}
		COMMENT_ONE_OF_BELOW
re.NoError(cluster.putStoreLocked(store))
re.Nil(cluster.putStoreLocked(store))

		COMMENT_ONE_OF_BELOW
re.NoError(cluster.HandleStoreHeartbeat(storeStats))
re.Nil(cluster.HandleStoreHeartbeat(storeStats))

		COMMENT_ONE_OF_BELOW
re.Error(cluster.hotStat.GetRollingStoreStats(store.GetID()))
re.NotNil(cluster.hotStat.GetRollingStoreStats(store.GetID()))

	}

	for _, store := range stores {
		storeStats := &pdpb.StoreStats{
			StoreId:     store.GetID(),
			Capacity:    100,
			Available:   50,
			RegionCount: 1,
		}
		newStore := store.Clone(core.TombstoneStore())
		COMMENT_ONE_OF_BELOW
re.NoError(cluster.putStoreLocked(newStore))
re.Nil(cluster.putStoreLocked(newStore))

		COMMENT_ONE_OF_BELOW
re.NoError(cluster.HandleStoreHeartbeat(storeStats))
re.Nil(cluster.HandleStoreHeartbeat(storeStats))

		COMMENT_ONE_OF_BELOW
re.NoError(cluster.hotStat.GetRollingStoreStats(store.GetID()))
re.Nil(cluster.hotStat.GetRollingStoreStats(store.GetID()))

	}
}

func TestSetOfflineStore(t *testing.T) {
    re := require.New(t)
    _, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)
	cluster.ruleManager = placement.NewRuleManager(storage.NewStorageWithMemoryBackend(), cluster, cluster.GetOpts())
	if opt.IsPlacementRulesEnabled() {
		err := cluster.ruleManager.Initialize(opt.GetMaxReplicas(), opt.GetLocationLabels())
		if err != nil {
			panic(err)
		}
	}

	// Put 6 stores.
	for _, store := range newTestStores(6, "2.0.0") {
		COMMENT_ONE_OF_BELOW
re.NoError(cluster.PutStore(store.GetMeta()))
re.Nil(cluster.PutStore(store.GetMeta()))

	}

	// store 1: up -> offline
	COMMENT_ONE_OF_BELOW
re.NoError(cluster.RemoveStore(1, false))
re.Nil(cluster.RemoveStore(1, false))

	store := cluster.GetStore(1)
	re.True(store.IsRemoving())
	re.False(store.IsPhysicallyDestroyed())

	// store 1: set physically to true success
	COMMENT_ONE_OF_BELOW
re.NoError(cluster.RemoveStore(1, true))
re.Nil(cluster.RemoveStore(1, true))

	store = cluster.GetStore(1)
	re.True(store.IsRemoving())
	re.True(store.IsPhysicallyDestroyed())

	// store 2:up -> offline & physically destroyed
	COMMENT_ONE_OF_BELOW
re.NoError(cluster.RemoveStore(2, true))
re.Nil(cluster.RemoveStore(2, true))

	// store 2: set physically destroyed to false failed
	COMMENT_ONE_OF_BELOW
re.Error(cluster.RemoveStore(2, false))
re.NotNil(cluster.RemoveStore(2, false))

	COMMENT_ONE_OF_BELOW
re.NoError(cluster.RemoveStore(2, true))
re.Nil(cluster.RemoveStore(2, true))


	// store 3: up to offline
	COMMENT_ONE_OF_BELOW
re.NoError(cluster.RemoveStore(3, false))
re.Nil(cluster.RemoveStore(3, false))

	COMMENT_ONE_OF_BELOW
re.NoError(cluster.RemoveStore(3, false))
re.Nil(cluster.RemoveStore(3, false))


	cluster.checkStores()
	// store 1,2,3 should be to tombstone
	for storeID := uint64(1); storeID <= 3; storeID++ {
		re.True(cluster.GetStore(storeID).IsRemoved())
	}
	// test bury store
	for storeID := uint64(0); storeID <= 4; storeID++ {
		store := cluster.GetStore(storeID)
		if store == nil || store.IsUp() {
			COMMENT_ONE_OF_BELOW
re.Error(cluster.BuryStore(storeID, false))
re.NotNil(cluster.BuryStore(storeID, false))

		} else {
			COMMENT_ONE_OF_BELOW
re.NoError(cluster.BuryStore(storeID, false))
re.Nil(cluster.BuryStore(storeID, false))

		}
	}
}

func TestSetOfflineWithReplica(t *testing.T) {
    re := require.New(t)
    _, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)

	// Put 4 stores.
	for _, store := range newTestStores(4, "2.0.0") {
		COMMENT_ONE_OF_BELOW
re.NoError(cluster.PutStore(store.GetMeta()))
re.Nil(cluster.PutStore(store.GetMeta()))

	}

	COMMENT_ONE_OF_BELOW
re.NoError(cluster.RemoveStore(2, false))
re.Nil(cluster.RemoveStore(2, false))

	// should be failed since no enough store to accommodate the extra replica.
	err = cluster.RemoveStore(3, false)
	re.True(strings.Contains(err.Error(), string(errs.ErrStoresNotEnough.RFCCode())))
	COMMENT_ONE_OF_BELOW
re.Error(cluster.RemoveStore(3, false))
re.NotNil(cluster.RemoveStore(3, false))

	// should be success since physically-destroyed is true.
	COMMENT_ONE_OF_BELOW
re.NoError(cluster.RemoveStore(3, true))
re.Nil(cluster.RemoveStore(3, true))
}

func addEvictLeaderScheduler(cluster *RaftCluster, storeID uint64) (evictScheduler schedule.Scheduler, err error) {
	args := []string{fmt.Sprintf("%d", storeID)}
	evictScheduler, err = schedule.CreateScheduler(schedulers.EvictLeaderType, cluster.GetOperatorController(), cluster.storage, schedule.ConfigSliceDecoder(schedulers.EvictLeaderType, args))
	if err != nil {
		return
	}
	if err = cluster.AddScheduler(evictScheduler, args...); err != nil {
		return
	} else if err = cluster.opt.Persist(cluster.GetStorage()); err != nil {
		return
	}
	return
}

func TestSetOfflineStoreWithEvictLeader(t *testing.T) {
    re := require.New(t)
    _, opt, err := newTestScheduleConfig()
	re.NoError(err)
	opt.SetMaxReplicas(1)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)

	// Put 3 stores.
	for _, store := range newTestStores(3, "2.0.0") {
		COMMENT_ONE_OF_BELOW
re.NoError(cluster.PutStore(store.GetMeta()))
re.Nil(cluster.PutStore(store.GetMeta()))

	}
	_, err = addEvictLeaderScheduler(cluster, 1)

	re.NoError(err)
	COMMENT_ONE_OF_BELOW
re.NoError(cluster.RemoveStore(2, false))
re.Nil(cluster.RemoveStore(2, false))


	// should be failed since there is only 1 store left and it is the evict-leader store.
	err = cluster.RemoveStore(3, false)
	re.Error(err)
	re.True(strings.Contains(err.Error(), string(errs.ErrNoStoreForRegionLeader.RFCCode())))
	COMMENT_ONE_OF_BELOW
re.NoError(cluster.RemoveScheduler(schedulers.EvictLeaderName))
re.Nil(cluster.RemoveScheduler(schedulers.EvictLeaderName))

	COMMENT_ONE_OF_BELOW
re.NoError(cluster.RemoveStore(3, false))
re.Nil(cluster.RemoveStore(3, false))
}

func TestForceBuryStore(t *testing.T) {
    re := require.New(t)
    _, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	// Put 2 stores.
	stores := newTestStores(2, "5.3.0")
	stores[1] = stores[1].Clone(core.SetLastHeartbeatTS(time.Now()))
	for _, store := range stores {
		COMMENT_ONE_OF_BELOW
re.NoError(cluster.PutStore(store.GetMeta()))
re.Nil(cluster.PutStore(store.GetMeta()))

	}
	COMMENT_ONE_OF_BELOW
re.NoError(cluster.BuryStore(uint64(1), true))
re.Nil(cluster.BuryStore(uint64(1), true))

	COMMENT_ONE_OF_BELOW
re.Error(cluster.BuryStore(uint64(2), true))
re.NotNil(cluster.BuryStore(uint64(2), true))

	re.True(errors.ErrorEqual(cluster.BuryStore(uint64(3), true), errs.ErrStoreNotFound.FastGenByArgs(uint64(3))))
}

func TestReuseAddress(t *testing.T) {
    re := require.New(t)
    _, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)
	// Put 4 stores.
	for _, store := range newTestStores(4, "2.0.0") {
		COMMENT_ONE_OF_BELOW
re.NoError(cluster.PutStore(store.GetMeta()))
re.Nil(cluster.PutStore(store.GetMeta()))

	}
	// store 1: up
	// store 2: offline
	COMMENT_ONE_OF_BELOW
re.NoError(cluster.RemoveStore(2, false))
re.Nil(cluster.RemoveStore(2, false))

	// store 3: offline and physically destroyed
	COMMENT_ONE_OF_BELOW
re.NoError(cluster.RemoveStore(3, true))
re.Nil(cluster.RemoveStore(3, true))

	// store 4: tombstone
	COMMENT_ONE_OF_BELOW
re.NoError(cluster.RemoveStore(4, true))
re.Nil(cluster.RemoveStore(4, true))

	COMMENT_ONE_OF_BELOW
re.NoError(cluster.BuryStore(4, false))
re.Nil(cluster.BuryStore(4, false))


	for id := uint64(1); id <= 4; id++ {
		storeInfo := cluster.GetStore(id)
		storeID := storeInfo.GetID() + 1000
		newStore := &metapb.Store{
			Id:         storeID,
			Address:    storeInfo.GetAddress(),
			State:      metapb.StoreState_Up,
			Version:    storeInfo.GetVersion(),
			DeployPath: getTestDeployPath(storeID),
		}

		if storeInfo.IsPhysicallyDestroyed() || storeInfo.IsRemoved() {
			// try to start a new store with the same address with store which is physically destryed or tombstone should be success
			COMMENT_ONE_OF_BELOW
re.NoError(cluster.PutStore(newStore))
re.Nil(cluster.PutStore(newStore))

		} else {
			COMMENT_ONE_OF_BELOW
re.Error(cluster.PutStore(newStore))
re.NotNil(cluster.PutStore(newStore))

		}
	}
}

func getTestDeployPath(storeID uint64) string {
	return fmt.Sprintf("test/store%d", storeID)
}

func TestUpStore(t *testing.T) {
    re := require.New(t)
    _, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)
	cluster.ruleManager = placement.NewRuleManager(storage.NewStorageWithMemoryBackend(), cluster, cluster.GetOpts())
	if opt.IsPlacementRulesEnabled() {
		err := cluster.ruleManager.Initialize(opt.GetMaxReplicas(), opt.GetLocationLabels())
		if err != nil {
			panic(err)
		}
	}

	// Put 5 stores.
	for _, store := range newTestStores(5, "5.0.0") {
		COMMENT_ONE_OF_BELOW
re.NoError(cluster.PutStore(store.GetMeta()))
re.Nil(cluster.PutStore(store.GetMeta()))

	}

	// set store 1 offline
	COMMENT_ONE_OF_BELOW
re.NoError(cluster.RemoveStore(1, false))
re.Nil(cluster.RemoveStore(1, false))

	// up a offline store should be success.
	COMMENT_ONE_OF_BELOW
re.NoError(cluster.UpStore(1))
re.Nil(cluster.UpStore(1))


	// set store 2 offline and physically destroyed
	COMMENT_ONE_OF_BELOW
re.NoError(cluster.RemoveStore(2, true))
re.Nil(cluster.RemoveStore(2, true))

	COMMENT_ONE_OF_BELOW
re.Error(cluster.UpStore(2))
re.NotNil(cluster.UpStore(2))


	// bury store 2
	cluster.checkStores()
	// store is tombstone
	err = cluster.UpStore(2)
	re.True(errors.ErrorEqual(err, errs.ErrStoreRemoved.FastGenByArgs(2)))

	// store 3 is up
	COMMENT_ONE_OF_BELOW
re.NoError(cluster.UpStore(3))
re.Nil(cluster.UpStore(3))


	// store 4 not exist
	err = cluster.UpStore(10)
	re.True(errors.ErrorEqual(err, errs.ErrStoreNotFound.FastGenByArgs(4)))
}

func TestRemovingProcess(t *testing.T) {
    re := require.New(t)
    _, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)
	cluster.SetPrepared()

	// Put 5 stores.
	stores := newTestStores(5, "5.0.0")
	for _, store := range stores {
		COMMENT_ONE_OF_BELOW
re.NoError(cluster.PutStore(store.GetMeta()))
re.Nil(cluster.PutStore(store.GetMeta()))

	}
	regions := newTestRegions(100, 5, 1)
	var regionInStore1 []*core.RegionInfo
	for _, region := range regions {
		if region.GetPeers()[0].GetStoreId() == 1 {
			region = region.Clone(core.SetApproximateSize(100))
			regionInStore1 = append(regionInStore1, region)
		}
		COMMENT_ONE_OF_BELOW
re.NoError(cluster.putRegion(region))
re.Nil(cluster.putRegion(region))

	}
	re.Equal(20, len(regionInStore1))
	cluster.progressManager = progress.NewManager()
	cluster.RemoveStore(1, false)
	cluster.checkStores()
	process := "removing-1"
	// no region moving
	p, l, cs, err := cluster.progressManager.Status(process)
	re.NoError(err)
	re.Equal(0.0, p)
	re.Equal(math.MaxFloat64, l)
	re.Equal(0.0, cs)
	i := 0
	// simulate region moving by deleting region from store 1
	for _, region := range regionInStore1 {
		if i >= 5 {
			break
		}
		cluster.DropCacheRegion(region.GetID())
		i++
	}
	cluster.checkStores()
	p, l, cs, err = cluster.progressManager.Status(process)
	re.NoError(err)
	// In above we delete 5 region from store 1, the total count of region in store 1 is 20.
	// process = 5 / 20 = 0.25
	re.Equal(0.25, p)
	// Each region is 100MB, we use more than 1s to move 5 region.
	// speed = 5 * 100MB / 20s = 25MB/s
	re.Equal(25.0, cs)
	// left second = 15 * 100MB / 25s = 60s
	re.Equal(60.0, l)
}

func TestDeleteStoreUpdatesClusterVersion(t *testing.T) {
    re := require.New(t)
    _, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)
	cluster.ruleManager = placement.NewRuleManager(storage.NewStorageWithMemoryBackend(), cluster, cluster.GetOpts())
	if opt.IsPlacementRulesEnabled() {
		err := cluster.ruleManager.Initialize(opt.GetMaxReplicas(), opt.GetLocationLabels())
		if err != nil {
			panic(err)
		}
	}

	// Put 3 new 4.0.9 stores.
	for _, store := range newTestStores(3, "4.0.9") {
		COMMENT_ONE_OF_BELOW
re.NoError(cluster.PutStore(store.GetMeta()))
re.Nil(cluster.PutStore(store.GetMeta()))

	}
	re.Equal("4.0.9", cluster.GetClusterVersion())

	// Upgrade 2 stores to 5.0.0.
	for _, store := range newTestStores(2, "5.0.0") {
		COMMENT_ONE_OF_BELOW
re.NoError(cluster.PutStore(store.GetMeta()))
re.Nil(cluster.PutStore(store.GetMeta()))

	}
	re.Equal("4.0.9", cluster.GetClusterVersion())

	// Bury the other store.
	COMMENT_ONE_OF_BELOW
re.NoError(cluster.RemoveStore(3, true))
re.Nil(cluster.RemoveStore(3, true))

	cluster.checkStores()
	re.Equal("5.0.0", cluster.GetClusterVersion())
}

func TestStoreClusterVersion(t *testing.T) {
    re := require.New(t)
    _, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	stores := newTestStores(3, "5.0.0")
	s1, s2, s3 := stores[0].GetMeta(), stores[1].GetMeta(), stores[2].GetMeta()
	s1.Version = "5.0.1"
	s2.Version = "5.0.3"
	s3.Version = "5.0.5"
	COMMENT_ONE_OF_BELOW
re.NoError(cluster.PutStore(s2))
re.Nil(cluster.PutStore(s2))

	re.Equal(s2.Version, cluster.GetClusterVersion())

	COMMENT_ONE_OF_BELOW
re.NoError(cluster.PutStore(s1))
re.Nil(cluster.PutStore(s1))

	// the cluster version should be 5.0.1(the min one)
	re.Equal(s1.Version, cluster.GetClusterVersion())

	COMMENT_ONE_OF_BELOW
re.NoError(cluster.PutStore(s3))
re.Nil(cluster.PutStore(s3))

	// the cluster version should be 5.0.1(the min one)
	re.Equal(s1.Version, cluster.GetClusterVersion())
}

func TestRegionHeartbeatHotStat(t *testing.T) {
    re := require.New(t)
    _, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)
	newTestStores(4, "2.0.0")
	peers := []*metapb.Peer{
		{
			Id:      1,
			StoreId: 1,
		},
		{
			Id:      2,
			StoreId: 2,
		},
		{
			Id:      3,
			StoreId: 3,
		},
	}
	leader := &metapb.Peer{
		Id:      1,
		StoreId: 1,
	}
	regionMeta := &metapb.Region{
		Id:          1,
		Peers:       peers,
		StartKey:    []byte{byte(1)},
		EndKey:      []byte{byte(1 + 1)},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 2, Version: 2},
	}
	region := core.NewRegionInfo(regionMeta, leader, core.WithInterval(&pdpb.TimeInterval{StartTimestamp: 0, EndTimestamp: 10}),
		core.SetWrittenBytes(30000*10),
		core.SetWrittenKeys(300000*10))
	err = cluster.processRegionHeartbeat(region)
	re.NoError(err)
	// wait HotStat to update items
	time.Sleep(1 * time.Second)
	stats := cluster.hotStat.RegionStats(statistics.Write, 0)
	re.Len(stats[1], 1)
	re.Len(stats[2], 1)
	re.Len(stats[3], 1)
	newPeer := &metapb.Peer{
		Id:      4,
		StoreId: 4,
	}
	region = region.Clone(core.WithRemoveStorePeer(2), core.WithAddPeer(newPeer))
	err = cluster.processRegionHeartbeat(region)
	re.NoError(err)
	// wait HotStat to update items
	time.Sleep(1 * time.Second)
	stats = cluster.hotStat.RegionStats(statistics.Write, 0)
	re.Len(stats[1], 1)
	re.Len(stats[2], 0)
	re.Len(stats[3], 1)
	re.Len(stats[4], 1)
}

func TestBucketHeartbeat(t *testing.T) {
    re := require.New(t)
    _, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)

	// case1: region is not exist
	buckets := &metapb.Buckets{
		RegionId: 1,
		Version:  1,
		Keys:     [][]byte{{'1'}, {'2'}},
	}
	COMMENT_ONE_OF_BELOW
re.Error(cluster.processReportBuckets(buckets))
re.NotNil(cluster.processReportBuckets(buckets))


	// case2: bucket can be processed after the region update.
	stores := newTestStores(3, "2.0.0")
	n, np := uint64(2), uint64(2)
	regions := newTestRegions(n, n, np)
	for _, store := range stores {
		COMMENT_ONE_OF_BELOW
re.NoError(cluster.putStoreLocked(store))
re.Nil(cluster.putStoreLocked(store))

	}

	COMMENT_ONE_OF_BELOW
re.NoError(cluster.processRegionHeartbeat(regions[0]))
re.Nil(cluster.processRegionHeartbeat(regions[0]))

	COMMENT_ONE_OF_BELOW
re.NoError(cluster.processRegionHeartbeat(regions[1]))
re.Nil(cluster.processRegionHeartbeat(regions[1]))

	COMMENT_ONE_OF_BELOW
re.NoError(cluster.GetRegion(uint64(1)).GetBuckets())
re.Nil(cluster.GetRegion(uint64(1)).GetBuckets())

	COMMENT_ONE_OF_BELOW
re.NoError(cluster.processReportBuckets(buckets))
re.Nil(cluster.processReportBuckets(buckets))

	re.Equal(buckets, cluster.GetRegion(uint64(1)).GetBuckets())

	// case3: the bucket version is same.
	COMMENT_ONE_OF_BELOW
re.NoError(cluster.processReportBuckets(buckets))
re.Nil(cluster.processReportBuckets(buckets))

	// case4: the bucket version is changed.
	newBuckets := &metapb.Buckets{
		RegionId: 1,
		Version:  3,
		Keys:     [][]byte{{'1'}, {'2'}},
	}
	COMMENT_ONE_OF_BELOW
re.NoError(cluster.processReportBuckets(newBuckets))
re.Nil(cluster.processReportBuckets(newBuckets))

	re.Equal(newBuckets, cluster.GetRegion(uint64(1)).GetBuckets())

	// case5: region update should inherit buckets.
	newRegion := regions[1].Clone(core.WithIncConfVer(), core.SetBuckets(nil))
	cluster.storeConfigManager = config.NewTestStoreConfigManager(nil)
	config := cluster.storeConfigManager.GetStoreConfig()
	config.Coprocessor.EnableRegionBucket = true
	COMMENT_ONE_OF_BELOW
re.NoError(cluster.processRegionHeartbeat(newRegion))
re.Nil(cluster.processRegionHeartbeat(newRegion))

	re.Len(cluster.GetRegion(uint64(1)).GetBuckets().GetKeys(), 2)

	// case6: disable region bucket in
	config.Coprocessor.EnableRegionBucket = false
	newRegion2 := regions[1].Clone(core.WithIncConfVer(), core.SetBuckets(nil))
	COMMENT_ONE_OF_BELOW
re.NoError(cluster.processRegionHeartbeat(newRegion2))
re.Nil(cluster.processRegionHeartbeat(newRegion2))

	COMMENT_ONE_OF_BELOW
re.NoError(cluster.GetRegion(uint64(1)).GetBuckets())
re.Nil(cluster.GetRegion(uint64(1)).GetBuckets())

	re.Len(cluster.GetRegion(uint64(1)).GetBuckets().GetKeys(), 0)
}

func TestRegionHeartbeat(t *testing.T) {
    re := require.New(t)
    _, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)

	n, np := uint64(3), uint64(3)

	stores := newTestStores(3, "2.0.0")
	regions := newTestRegions(n, n, np)

	for _, store := range stores {
		COMMENT_ONE_OF_BELOW
re.NoError(cluster.putStoreLocked(store))
re.Nil(cluster.putStoreLocked(store))

	}

	for i, region := range regions {
		// region does not exist.
		COMMENT_ONE_OF_BELOW
re.NoError(cluster.processRegionHeartbeat(region))
re.Nil(cluster.processRegionHeartbeat(region))

		checkRegions(c, cluster.core.Regions, regions[:i+1])
		checkRegionsKV(c, cluster.storage, regions[:i+1])

		// region is the same, not updated.
		COMMENT_ONE_OF_BELOW
re.NoError(cluster.processRegionHeartbeat(region))
re.Nil(cluster.processRegionHeartbeat(region))

		checkRegions(c, cluster.core.Regions, regions[:i+1])
		checkRegionsKV(c, cluster.storage, regions[:i+1])
		origin := region
		// region is updated.
		region = origin.Clone(core.WithIncVersion())
		regions[i] = region
		COMMENT_ONE_OF_BELOW
re.NoError(cluster.processRegionHeartbeat(region))
re.Nil(cluster.processRegionHeartbeat(region))

		checkRegions(c, cluster.core.Regions, regions[:i+1])
		checkRegionsKV(c, cluster.storage, regions[:i+1])

		// region is stale (Version).
		stale := origin.Clone(core.WithIncConfVer())
		COMMENT_ONE_OF_BELOW
re.Error(cluster.processRegionHeartbeat(stale))
re.NotNil(cluster.processRegionHeartbeat(stale))

		checkRegions(c, cluster.core.Regions, regions[:i+1])
		checkRegionsKV(c, cluster.storage, regions[:i+1])

		// region is updated.
		region = origin.Clone(
			core.WithIncVersion(),
			core.WithIncConfVer(),
		)
		regions[i] = region
		COMMENT_ONE_OF_BELOW
re.NoError(cluster.processRegionHeartbeat(region))
re.Nil(cluster.processRegionHeartbeat(region))

		checkRegions(c, cluster.core.Regions, regions[:i+1])
		checkRegionsKV(c, cluster.storage, regions[:i+1])

		// region is stale (ConfVer).
		stale = origin.Clone(core.WithIncConfVer())
		COMMENT_ONE_OF_BELOW
re.Error(cluster.processRegionHeartbeat(stale))
re.NotNil(cluster.processRegionHeartbeat(stale))

		checkRegions(c, cluster.core.Regions, regions[:i+1])
		checkRegionsKV(c, cluster.storage, regions[:i+1])

		// Add a down peer.
		region = region.Clone(core.WithDownPeers([]*pdpb.PeerStats{
			{
				Peer:        region.GetPeers()[rand.Intn(len(region.GetPeers()))],
				DownSeconds: 42,
			},
		}))
		regions[i] = region
		COMMENT_ONE_OF_BELOW
re.NoError(cluster.processRegionHeartbeat(region))
re.Nil(cluster.processRegionHeartbeat(region))

		checkRegions(c, cluster.core.Regions, regions[:i+1])

		// Add a pending peer.
		region = region.Clone(core.WithPendingPeers([]*metapb.Peer{region.GetPeers()[rand.Intn(len(region.GetPeers()))]}))
		regions[i] = region
		COMMENT_ONE_OF_BELOW
re.NoError(cluster.processRegionHeartbeat(region))
re.Nil(cluster.processRegionHeartbeat(region))

		checkRegions(c, cluster.core.Regions, regions[:i+1])

		// Clear down peers.
		region = region.Clone(core.WithDownPeers(nil))
		regions[i] = region
		COMMENT_ONE_OF_BELOW
re.NoError(cluster.processRegionHeartbeat(region))
re.Nil(cluster.processRegionHeartbeat(region))

		checkRegions(c, cluster.core.Regions, regions[:i+1])

		// Clear pending peers.
		region = region.Clone(core.WithPendingPeers(nil))
		regions[i] = region
		COMMENT_ONE_OF_BELOW
re.NoError(cluster.processRegionHeartbeat(region))
re.Nil(cluster.processRegionHeartbeat(region))

		checkRegions(c, cluster.core.Regions, regions[:i+1])

		// Remove peers.
		origin = region
		region = origin.Clone(core.SetPeers(region.GetPeers()[:1]))
		regions[i] = region
		COMMENT_ONE_OF_BELOW
re.NoError(cluster.processRegionHeartbeat(region))
re.Nil(cluster.processRegionHeartbeat(region))

		checkRegions(c, cluster.core.Regions, regions[:i+1])
		checkRegionsKV(c, cluster.storage, regions[:i+1])
		// Add peers.
		region = origin
		regions[i] = region
		COMMENT_ONE_OF_BELOW
re.NoError(cluster.processRegionHeartbeat(region))
re.Nil(cluster.processRegionHeartbeat(region))

		checkRegions(c, cluster.core.Regions, regions[:i+1])
		checkRegionsKV(c, cluster.storage, regions[:i+1])

		// Change leader.
		region = region.Clone(core.WithLeader(region.GetPeers()[1]))
		regions[i] = region
		COMMENT_ONE_OF_BELOW
re.NoError(cluster.processRegionHeartbeat(region))
re.Nil(cluster.processRegionHeartbeat(region))

		checkRegions(c, cluster.core.Regions, regions[:i+1])

		// Change ApproximateSize.
		region = region.Clone(core.SetApproximateSize(144))
		regions[i] = region
		COMMENT_ONE_OF_BELOW
re.NoError(cluster.processRegionHeartbeat(region))
re.Nil(cluster.processRegionHeartbeat(region))

		checkRegions(c, cluster.core.Regions, regions[:i+1])

		// Change ApproximateKeys.
		region = region.Clone(core.SetApproximateKeys(144000))
		regions[i] = region
		COMMENT_ONE_OF_BELOW
re.NoError(cluster.processRegionHeartbeat(region))
re.Nil(cluster.processRegionHeartbeat(region))

		checkRegions(c, cluster.core.Regions, regions[:i+1])

		// Change bytes written.
		region = region.Clone(core.SetWrittenBytes(24000))
		regions[i] = region
		COMMENT_ONE_OF_BELOW
re.NoError(cluster.processRegionHeartbeat(region))
re.Nil(cluster.processRegionHeartbeat(region))

		checkRegions(c, cluster.core.Regions, regions[:i+1])

		// Change bytes read.
		region = region.Clone(core.SetReadBytes(1080000))
		regions[i] = region
		COMMENT_ONE_OF_BELOW
re.NoError(cluster.processRegionHeartbeat(region))
re.Nil(cluster.processRegionHeartbeat(region))

		checkRegions(c, cluster.core.Regions, regions[:i+1])
	}

	regionCounts := make(map[uint64]int)
	for _, region := range regions {
		for _, peer := range region.GetPeers() {
			regionCounts[peer.GetStoreId()]++
		}
	}
	for id, count := range regionCounts {
		re.Equal(count, cluster.GetStoreRegionCount(id))
	}

	for _, region := range cluster.GetRegions() {
		checkRegion(c, region, regions[region.GetID()])
	}
	for _, region := range cluster.GetMetaRegions() {
		re.Equal(regions[region.GetId()].GetMeta(), region)
	}

	for _, region := range regions {
		for _, store := range cluster.GetRegionStores(region) {
			COMMENT_ONE_OF_BELOW
re.Error(region.GetStorePeer(store.GetID()))
re.NotNil(region.GetStorePeer(store.GetID()))

		}
		for _, store := range cluster.GetFollowerStores(region) {
			peer := region.GetStorePeer(store.GetID())
			re.NotEqual(region.GetLeader().GetId(), peer.GetId())
		}
	}

	for _, store := range cluster.core.Stores.GetStores() {
		re.Equal(cluster.core.Regions.GetStoreLeaderCount(store.GetID()), store.GetLeaderCount())
		re.Equal(cluster.core.Regions.GetStoreRegionCount(store.GetID()), store.GetRegionCount())
		re.Equal(cluster.core.Regions.GetStoreLeaderRegionSize(store.GetID()), store.GetLeaderSize())
		re.Equal(cluster.core.Regions.GetStoreRegionSize(store.GetID()), store.GetRegionSize())
	}

	// Test with storage.
	if storage := cluster.storage; storage != nil {
		for _, region := range regions {
			tmp := &metapb.Region{}
			ok, err := storage.LoadRegion(region.GetID(), tmp)
			re.True(ok)
			re.NoError(err)
			re.Equal(region.GetMeta(), tmp)
		}

		// Check overlap with stale version
		overlapRegion := regions[n-1].Clone(
			core.WithStartKey([]byte("")),
			core.WithEndKey([]byte("")),
			core.WithNewRegionID(10000),
			core.WithDecVersion(),
		)
		COMMENT_ONE_OF_BELOW
re.Error(cluster.processRegionHeartbeat(overlapRegion))
re.NotNil(cluster.processRegionHeartbeat(overlapRegion))

		region := &metapb.Region{}
		ok, err := storage.LoadRegion(regions[n-1].GetID(), region)
		re.True(ok)
		re.NoError(err)
		re.Equal(regions[n-1].GetMeta(), region)
		ok, err = storage.LoadRegion(regions[n-2].GetID(), region)
		re.True(ok)
		re.NoError(err)
		re.Equal(regions[n-2].GetMeta(), region)
		ok, err = storage.LoadRegion(overlapRegion.GetID(), region)
		re.False(ok)
		re.NoError(err)

		// Check overlap
		overlapRegion = regions[n-1].Clone(
			core.WithStartKey(regions[n-2].GetStartKey()),
			core.WithNewRegionID(regions[n-1].GetID()+1),
		)
		COMMENT_ONE_OF_BELOW
re.NoError(cluster.processRegionHeartbeat(overlapRegion))
re.Nil(cluster.processRegionHeartbeat(overlapRegion))

		region = &metapb.Region{}
		ok, err = storage.LoadRegion(regions[n-1].GetID(), region)
		re.False(ok)
		re.NoError(err)
		ok, err = storage.LoadRegion(regions[n-2].GetID(), region)
		re.False(ok)
		re.NoError(err)
		ok, err = storage.LoadRegion(overlapRegion.GetID(), region)
		re.True(ok)
		re.NoError(err)
		re.Equal(overlapRegion.GetMeta(), region)
	}
}

func TestRegionFlowChanged(t *testing.T) {
    re := require.New(t)
    _, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)
	regions := []*core.RegionInfo{core.NewTestRegionInfo([]byte{}, []byte{})}
	processRegions := func(regions []*core.RegionInfo) {
		for _, r := range regions {
			cluster.processRegionHeartbeat(r)
		}
	}
	regions = core.SplitRegions(regions)
	processRegions(regions)
	// update region
	region := regions[0]
	regions[0] = region.Clone(core.SetReadBytes(1000))
	processRegions(regions)
	newRegion := cluster.GetRegion(region.GetID())
	re.Equal(uint64(1000), newRegion.GetBytesRead())
}

func TestRegionSizeChanged(t *testing.T) {
    re := require.New(t)
    _, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)
	cluster.regionStats = statistics.NewRegionStatistics(cluster.GetOpts(), cluster.ruleManager, cluster.storeConfigManager)
	region := newTestRegions(1, 3, 3)[0]
	cluster.opt.GetMaxMergeRegionKeys()
	curMaxMergeSize := int64(cluster.opt.GetMaxMergeRegionSize())
	curMaxMergeKeys := int64(cluster.opt.GetMaxMergeRegionKeys())
	region = region.Clone(
		core.WithLeader(region.GetPeers()[2]),
		core.SetApproximateSize(curMaxMergeSize-1),
		core.SetApproximateKeys(curMaxMergeKeys-1),
		core.SetFromHeartbeat(true),
	)
	cluster.processRegionHeartbeat(region)
	regionID := region.GetID()
	re.True(cluster.regionStats.IsRegionStatsType(regionID, statistics.UndersizedRegion))
	// Test ApproximateSize and ApproximateKeys change.
	region = region.Clone(
		core.WithLeader(region.GetPeers()[2]),
		core.SetApproximateSize(curMaxMergeSize+1),
		core.SetApproximateKeys(curMaxMergeKeys+1),
		core.SetFromHeartbeat(true),
	)
	cluster.processRegionHeartbeat(region)
	re.False(cluster.regionStats.IsRegionStatsType(regionID, statistics.UndersizedRegion))
	// Test MaxMergeRegionSize and MaxMergeRegionKeys change.
	cluster.opt.SetMaxMergeRegionSize((uint64(curMaxMergeSize + 2)))
	cluster.opt.SetMaxMergeRegionKeys((uint64(curMaxMergeKeys + 2)))
	cluster.processRegionHeartbeat(region)
	re.True(cluster.regionStats.IsRegionStatsType(regionID, statistics.UndersizedRegion))
	cluster.opt.SetMaxMergeRegionSize((uint64(curMaxMergeSize)))
	cluster.opt.SetMaxMergeRegionKeys((uint64(curMaxMergeKeys)))
	cluster.processRegionHeartbeat(region)
	re.False(cluster.regionStats.IsRegionStatsType(regionID, statistics.UndersizedRegion))
}

func TestConcurrentReportBucket(t *testing.T) {
    re := require.New(t)
    _, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)

	regions := []*core.RegionInfo{core.NewTestRegionInfo([]byte{}, []byte{})}
	heartbeatRegions(c, cluster, regions)
	COMMENT_ONE_OF_BELOW
re.Error(cluster.GetRegion(0))
re.NotNil(cluster.GetRegion(0))


	bucket1 := &metapb.Buckets{RegionId: 0, Version: 3}
	bucket2 := &metapb.Buckets{RegionId: 0, Version: 2}
	var wg sync.WaitGroup
	wg.Add(1)
	COMMENT_ONE_OF_BELOW
re.NoError(failpoint.Enable("github.com/tikv/pd/server/cluster/concurrentBucketHeartbeat", "return(true)"))
re.Nil(failpoint.Enable("github.com/tikv/pd/server/cluster/concurrentBucketHeartbeat", "return(true)"))

	go func() {
		defer wg.Done()
		cluster.processReportBuckets(bucket1)
	}()
	time.Sleep(100 * time.Millisecond)
	COMMENT_ONE_OF_BELOW
re.NoError(failpoint.Disable("github.com/tikv/pd/server/cluster/concurrentBucketHeartbeat"))
re.Nil(failpoint.Disable("github.com/tikv/pd/server/cluster/concurrentBucketHeartbeat"))

	COMMENT_ONE_OF_BELOW
re.NoError(cluster.processReportBuckets(bucket2))
re.Nil(cluster.processReportBuckets(bucket2))

	wg.Wait()
	re.Equal(bucket1, cluster.GetRegion(0).GetBuckets())
}

func TestConcurrentRegionHeartbeat(t *testing.T) {
    re := require.New(t)
    _, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)

	regions := []*core.RegionInfo{core.NewTestRegionInfo([]byte{}, []byte{})}
	regions = core.SplitRegions(regions)
	heartbeatRegions(c, cluster, regions)

	// Merge regions manually
	source, target := regions[0], regions[1]
	target.GetMeta().StartKey = []byte{}
	target.GetMeta().EndKey = []byte{}
	source.GetMeta().GetRegionEpoch().Version++
	if source.GetMeta().GetRegionEpoch().GetVersion() > target.GetMeta().GetRegionEpoch().GetVersion() {
		target.GetMeta().GetRegionEpoch().Version = source.GetMeta().GetRegionEpoch().GetVersion()
	}
	target.GetMeta().GetRegionEpoch().Version++

	var wg sync.WaitGroup
	wg.Add(1)
	COMMENT_ONE_OF_BELOW
re.NoError(failpoint.Enable("github.com/tikv/pd/server/cluster/concurrentRegionHeartbeat", "return(true)"))
re.Nil(failpoint.Enable("github.com/tikv/pd/server/cluster/concurrentRegionHeartbeat", "return(true)"))

	go func() {
		defer wg.Done()
		cluster.processRegionHeartbeat(source)
	}()
	time.Sleep(100 * time.Millisecond)
	COMMENT_ONE_OF_BELOW
re.NoError(failpoint.Disable("github.com/tikv/pd/server/cluster/concurrentRegionHeartbeat"))
re.Nil(failpoint.Disable("github.com/tikv/pd/server/cluster/concurrentRegionHeartbeat"))

	COMMENT_ONE_OF_BELOW
re.NoError(cluster.processRegionHeartbeat(target))
re.Nil(cluster.processRegionHeartbeat(target))

	wg.Wait()
	checkRegion(c, cluster.GetRegionByKey([]byte{}), target)
}

func TestRegionLabelIsolationLevel(t *testing.T) {
    re := require.New(t)
    _, opt, err := newTestScheduleConfig()
	cfg := opt.GetReplicationConfig()
	cfg.LocationLabels = []string{"zone"}
	opt.SetReplicationConfig(cfg)
	re.NoError(err)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())

	for i := uint64(1); i <= 4; i++ {
		var labels []*metapb.StoreLabel
		if i == 4 {
			labels = []*metapb.StoreLabel{{Key: "zone", Value: fmt.Sprintf("%d", 3)}, {Key: "engine", Value: "tiflash"}}
		} else {
			labels = []*metapb.StoreLabel{{Key: "zone", Value: fmt.Sprintf("%d", i)}}
		}
		store := &metapb.Store{
			Id:      i,
			Address: fmt.Sprintf("127.0.0.1:%d", i),
			State:   metapb.StoreState_Up,
			Labels:  labels,
		}
		COMMENT_ONE_OF_BELOW
re.NoError(cluster.putStoreLocked(core.NewStoreInfo(store)))
re.Nil(cluster.putStoreLocked(core.NewStoreInfo(store)))

	}

	peers := make([]*metapb.Peer, 0, 4)
	for i := uint64(1); i <= 4; i++ {
		peer := &metapb.Peer{
			Id: i + 4,
		}
		peer.StoreId = i
		if i == 8 {
			peer.Role = metapb.PeerRole_Learner
		}
		peers = append(peers, peer)
	}
	region := &metapb.Region{
		Id:       9,
		Peers:    peers,
		StartKey: []byte{byte(1)},
		EndKey:   []byte{byte(2)},
	}
	r := core.NewRegionInfo(region, peers[0])
	COMMENT_ONE_OF_BELOW
re.NoError(cluster.putRegion(r))
re.Nil(cluster.putRegion(r))


	cluster.updateRegionsLabelLevelStats([]*core.RegionInfo{r})
	counter := cluster.labelLevelStats.GetLabelCounter()
	re.Equal(0, counter["none"])
	re.Equal(1, counter["zone"])
}

func heartbeatRegions(c *C, cluster *RaftCluster, regions []*core.RegionInfo) {
	// Heartbeat and check region one by one.
	for _, r := range regions {
		COMMENT_ONE_OF_BELOW
re.NoError(cluster.processRegionHeartbeat(r))
re.Nil(cluster.processRegionHeartbeat(r))


		checkRegion(c, cluster.GetRegion(r.GetID()), r)
		checkRegion(c, cluster.GetRegionByKey(r.GetStartKey()), r)

		if len(r.GetEndKey()) > 0 {
			end := r.GetEndKey()[0]
			checkRegion(c, cluster.GetRegionByKey([]byte{end - 1}), r)
		}
	}

	// Check all regions after handling all heartbeats.
	for _, r := range regions {
		checkRegion(c, cluster.GetRegion(r.GetID()), r)
		checkRegion(c, cluster.GetRegionByKey(r.GetStartKey()), r)

		if len(r.GetEndKey()) > 0 {
			end := r.GetEndKey()[0]
			checkRegion(c, cluster.GetRegionByKey([]byte{end - 1}), r)
			result := cluster.GetRegionByKey([]byte{end + 1})
			re.NotEqual(r.GetID(), result.GetID())
		}
	}
}

func TestHeartbeatSplit(t *testing.T) {
    re := require.New(t)
    _, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)

	// 1: [nil, nil)
	region1 := core.NewRegionInfo(&metapb.Region{Id: 1, RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1}}, nil)
	COMMENT_ONE_OF_BELOW
re.NoError(cluster.processRegionHeartbeat(region1))
re.Nil(cluster.processRegionHeartbeat(region1))

	checkRegion(c, cluster.GetRegionByKey([]byte("foo")), region1)

	// split 1 to 2: [nil, m) 1: [m, nil), sync 2 first.
	region1 = region1.Clone(
		core.WithStartKey([]byte("m")),
		core.WithIncVersion(),
	)
	region2 := core.NewRegionInfo(&metapb.Region{Id: 2, EndKey: []byte("m"), RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1}}, nil)
	COMMENT_ONE_OF_BELOW
re.NoError(cluster.processRegionHeartbeat(region2))
re.Nil(cluster.processRegionHeartbeat(region2))

	checkRegion(c, cluster.GetRegionByKey([]byte("a")), region2)
	// [m, nil) is missing before r1's heartbeat.
	COMMENT_ONE_OF_BELOW
re.NoError(cluster.GetRegionByKey([]byte("z")))
re.Nil(cluster.GetRegionByKey([]byte("z")))


	COMMENT_ONE_OF_BELOW
re.NoError(cluster.processRegionHeartbeat(region1))
re.Nil(cluster.processRegionHeartbeat(region1))

	checkRegion(c, cluster.GetRegionByKey([]byte("z")), region1)

	// split 1 to 3: [m, q) 1: [q, nil), sync 1 first.
	region1 = region1.Clone(
		core.WithStartKey([]byte("q")),
		core.WithIncVersion(),
	)
	region3 := core.NewRegionInfo(&metapb.Region{Id: 3, StartKey: []byte("m"), EndKey: []byte("q"), RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1}}, nil)
	COMMENT_ONE_OF_BELOW
re.NoError(cluster.processRegionHeartbeat(region1))
re.Nil(cluster.processRegionHeartbeat(region1))

	checkRegion(c, cluster.GetRegionByKey([]byte("z")), region1)
	checkRegion(c, cluster.GetRegionByKey([]byte("a")), region2)
	// [m, q) is missing before r3's heartbeat.
	COMMENT_ONE_OF_BELOW
re.NoError(cluster.GetRegionByKey([]byte("n")))
re.Nil(cluster.GetRegionByKey([]byte("n")))

	COMMENT_ONE_OF_BELOW
re.NoError(cluster.processRegionHeartbeat(region3))
re.Nil(cluster.processRegionHeartbeat(region3))

	checkRegion(c, cluster.GetRegionByKey([]byte("n")), region3)
}

func TestRegionSplitAndMerge(t *testing.T) {
    re := require.New(t)
    _, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)

	regions := []*core.RegionInfo{core.NewTestRegionInfo([]byte{}, []byte{})}

	// Byte will underflow/overflow if n > 7.
	n := 7

	// Split.
	for i := 0; i < n; i++ {
		regions = core.SplitRegions(regions)
		heartbeatRegions(c, cluster, regions)
	}

	// Merge.
	for i := 0; i < n; i++ {
		regions = core.MergeRegions(regions)
		heartbeatRegions(c, cluster, regions)
	}

	// Split twice and merge once.
	for i := 0; i < n*2; i++ {
		if (i+1)%3 == 0 {
			regions = core.MergeRegions(regions)
		} else {
			regions = core.SplitRegions(regions)
		}
		heartbeatRegions(c, cluster, regions)
	}
}

func TestOfflineAndMerge(t *testing.T) {
    re := require.New(t)
    _, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)
	cluster.ruleManager = placement.NewRuleManager(storage.NewStorageWithMemoryBackend(), cluster, cluster.GetOpts())
	if opt.IsPlacementRulesEnabled() {
		err := cluster.ruleManager.Initialize(opt.GetMaxReplicas(), opt.GetLocationLabels())
		if err != nil {
			panic(err)
		}
	}
	cluster.regionStats = statistics.NewRegionStatistics(cluster.GetOpts(), cluster.ruleManager, cluster.storeConfigManager)
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)

	// Put 4 stores.
	for _, store := range newTestStores(4, "5.0.0") {
		COMMENT_ONE_OF_BELOW
re.NoError(cluster.PutStore(store.GetMeta()))
re.Nil(cluster.PutStore(store.GetMeta()))

	}

	peers := []*metapb.Peer{
		{
			Id:      4,
			StoreId: 1,
		}, {
			Id:      5,
			StoreId: 2,
		}, {
			Id:      6,
			StoreId: 3,
		},
	}
	origin := core.NewRegionInfo(
		&metapb.Region{
			StartKey:    []byte{},
			EndKey:      []byte{},
			RegionEpoch: &metapb.RegionEpoch{ConfVer: 2, Version: 2},
			Id:          1,
			Peers:       peers}, peers[0])
	regions := []*core.RegionInfo{origin}

	// store 1: up -> offline
	COMMENT_ONE_OF_BELOW
re.NoError(cluster.RemoveStore(1, false))
re.Nil(cluster.RemoveStore(1, false))

	store := cluster.GetStore(1)
	re.True(store.IsRemoving())

	// Split.
	n := 7
	for i := 0; i < n; i++ {
		regions = core.SplitRegions(regions)
	}
	heartbeatRegions(c, cluster, regions)
	re.Len(cluster.GetOfflineRegionStatsByType(statistics.OfflinePeer), len(regions))

	// Merge.
	for i := 0; i < n; i++ {
		regions = core.MergeRegions(regions)
		heartbeatRegions(c, cluster, regions)
		re.Len(cluster.GetOfflineRegionStatsByType(statistics.OfflinePeer), len(regions))
	}
}

func TestSyncConfig(t *testing.T) {
    re := require.New(t)
    _, opt, err := newTestScheduleConfig()
	re.NoError(err)
	tc := newTestCluster(s.ctx, opt)
	stores := newTestStores(5, "2.0.0")
	for _, s := range stores {
		COMMENT_ONE_OF_BELOW
re.NoError(tc.putStoreLocked(s))
re.Nil(tc.putStoreLocked(s))

	}
	re.Len(tc.getUpStores(), 5)

	testdata := []struct {
		whiteList     []string
		maxRegionSize uint64
		updated       bool
	}{{
		whiteList:     []string{},
		maxRegionSize: uint64(144),
		updated:       false,
	}, {
		whiteList:     []string{"127.0.0.1:5"},
		maxRegionSize: uint64(10),
		updated:       true,
	}}

	for _, v := range testdata {
		tc.storeConfigManager = config.NewTestStoreConfigManager(v.whiteList)
		re.Equal(uint64(144), tc.GetStoreConfig().GetRegionMaxSize())
		re.Equal(v.updated, syncConfig(tc.storeConfigManager, tc.GetStores()))
		re.Equal(v.maxRegionSize, tc.GetStoreConfig().GetRegionMaxSize())
	}
}

func TestUpdateStorePendingPeerCount(t *testing.T) {
    re := require.New(t)
    _, opt, err := newTestScheduleConfig()
	re.NoError(err)
	tc := newTestCluster(s.ctx, opt)
	tc.RaftCluster.coordinator = newCoordinator(s.ctx, tc.RaftCluster, nil)
	stores := newTestStores(5, "2.0.0")
	for _, s := range stores {
		COMMENT_ONE_OF_BELOW
re.NoError(tc.putStoreLocked(s))
re.Nil(tc.putStoreLocked(s))

	}
	peers := []*metapb.Peer{
		{
			Id:      2,
			StoreId: 1,
		},
		{
			Id:      3,
			StoreId: 2,
		},
		{
			Id:      3,
			StoreId: 3,
		},
		{
			Id:      4,
			StoreId: 4,
		},
	}
	origin := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: peers[:3]}, peers[0], core.WithPendingPeers(peers[1:3]))
	COMMENT_ONE_OF_BELOW
re.NoError(tc.processRegionHeartbeat(origin))
re.Nil(tc.processRegionHeartbeat(origin))

	checkPendingPeerCount([]int{0, 1, 1, 0}, tc.RaftCluster, c)
	newRegion := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: peers[1:]}, peers[1], core.WithPendingPeers(peers[3:4]))
	COMMENT_ONE_OF_BELOW
re.NoError(tc.processRegionHeartbeat(newRegion))
re.Nil(tc.processRegionHeartbeat(newRegion))

	checkPendingPeerCount([]int{0, 0, 0, 1}, tc.RaftCluster, c)
}

func TestTopologyWeight(t *testing.T) {
    re := require.New(t)
    labels := []string{"zone", "rack", "host"}
	zones := []string{"z1", "z2", "z3"}
	racks := []string{"r1", "r2", "r3"}
	hosts := []string{"h1", "h2", "h3", "h4"}

	var stores []*core.StoreInfo
	var testStore *core.StoreInfo
	for i, zone := range zones {
		for j, rack := range racks {
			for k, host := range hosts {
				storeID := uint64(i*len(racks)*len(hosts) + j*len(hosts) + k)
				storeLabels := map[string]string{
					"zone": zone,
					"rack": rack,
					"host": host,
				}
				store := core.NewStoreInfoWithLabel(storeID, 1, storeLabels)
				if i == 0 && j == 0 && k == 0 {
					testStore = store
				}
				stores = append(stores, store)
			}
		}
	}

	re.Equal(1.0/3/3/4, getStoreTopoWeight(testStore, stores, labels))
}

func TestCalculateStoreSize1(t *testing.T) {
    re := require.New(t)
    _, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cfg := opt.GetReplicationConfig()
	cfg.EnablePlacementRules = true
	opt.SetReplicationConfig(cfg)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)
	cluster.regionStats = statistics.NewRegionStatistics(cluster.GetOpts(), cluster.ruleManager, cluster.storeConfigManager)

	// Put 10 stores.
	for i, store := range newTestStores(10, "6.0.0") {
		var labels []*metapb.StoreLabel
		if i%3 == 0 {
			// zone 1 has 1, 4, 7, 10
			labels = append(labels, &metapb.StoreLabel{Key: "zone", Value: "zone1"})
		} else if i%3 == 1 {
			// zone 2 has 2, 5, 8
			labels = append(labels, &metapb.StoreLabel{Key: "zone", Value: "zone2"})
		} else {
			// zone 3 has 3, 6, 9
			labels = append(labels, &metapb.StoreLabel{Key: "zone", Value: "zone3"})
		}
		labels = append(labels, []*metapb.StoreLabel{
			{
				Key:   "rack",
				Value: fmt.Sprintf("rack-%d", i%2+1),
			},
			{
				Key:   "host",
				Value: fmt.Sprintf("host-%d", i),
			},
		}...)
		s := store.Clone(core.SetStoreLabels(labels))
		COMMENT_ONE_OF_BELOW
re.NoError(cluster.PutStore(s.GetMeta()))
re.Nil(cluster.PutStore(s.GetMeta()))

	}

	cluster.ruleManager.SetRule(
		&placement.Rule{GroupID: "pd", ID: "zone1", StartKey: []byte(""), EndKey: []byte(""), Role: "voter", Count: 2,
			LabelConstraints: []placement.LabelConstraint{
				{Key: "zone", Op: "in", Values: []string{"zone1"}},
			},
			LocationLabels: []string{"rack", "host"}},
	)

	cluster.ruleManager.SetRule(
		&placement.Rule{GroupID: "pd", ID: "zone2", StartKey: []byte(""), EndKey: []byte(""), Role: "voter", Count: 2,
			LabelConstraints: []placement.LabelConstraint{
				{Key: "zone", Op: "in", Values: []string{"zone2"}},
			},
			LocationLabels: []string{"rack", "host"}},
	)

	cluster.ruleManager.SetRule(
		&placement.Rule{GroupID: "pd", ID: "zone3", StartKey: []byte(""), EndKey: []byte(""), Role: "follower", Count: 1,
			LabelConstraints: []placement.LabelConstraint{
				{Key: "zone", Op: "in", Values: []string{"zone3"}},
			},
			LocationLabels: []string{"rack", "host"}},
	)
	cluster.ruleManager.DeleteRule("pd", "default")

	regions := newTestRegions(100, 10, 5)
	for _, region := range regions {
		COMMENT_ONE_OF_BELOW
re.NoError(cluster.putRegion(region))
re.Nil(cluster.putRegion(region))

	}

	stores := cluster.GetStores()
	store := cluster.GetStore(1)
	// 100 * 100 * 2 (placement rule) / 4 (host) * 0.9 = 4500
	re.Equal(4500.0, cluster.getThreshold(stores, store))

	cluster.opt.SetPlacementRuleEnabled(false)
	cluster.opt.SetLocationLabels([]string{"zone", "rack", "host"})
	// 30000 (total region size) / 3 (zone) / 4 (host) * 0.9 = 2250
	re.Equal(2250.0, cluster.getThreshold(stores, store))
}

func TestCalculateStoreSize2(t *testing.T) {
    re := require.New(t)
    _, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cfg := opt.GetReplicationConfig()
	cfg.EnablePlacementRules = true
	opt.SetReplicationConfig(cfg)
	opt.SetMaxReplicas(3)
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cluster.coordinator = newCoordinator(s.ctx, cluster, nil)
	cluster.regionStats = statistics.NewRegionStatistics(cluster.GetOpts(), cluster.ruleManager, cluster.storeConfigManager)

	// Put 10 stores.
	for i, store := range newTestStores(10, "6.0.0") {
		var labels []*metapb.StoreLabel
		if i%2 == 0 {
			// dc 1 has 1, 3, 5, 7, 9
			labels = append(labels, &metapb.StoreLabel{Key: "dc", Value: "dc1"})
			if i%4 == 0 {
				labels = append(labels, &metapb.StoreLabel{Key: "logic", Value: "logic1"})
			} else {
				labels = append(labels, &metapb.StoreLabel{Key: "logic", Value: "logic2"})
			}
		} else {
			// dc 2 has 2, 4, 6, 8, 10
			labels = append(labels, &metapb.StoreLabel{Key: "dc", Value: "dc2"})
			if i%3 == 0 {
				labels = append(labels, &metapb.StoreLabel{Key: "logic", Value: "logic3"})
			} else {
				labels = append(labels, &metapb.StoreLabel{Key: "logic", Value: "logic4"})
			}
		}
		labels = append(labels, []*metapb.StoreLabel{{Key: "rack", Value: "r1"}, {Key: "host", Value: "h1"}}...)
		s := store.Clone(core.SetStoreLabels(labels))
		COMMENT_ONE_OF_BELOW
re.NoError(cluster.PutStore(s.GetMeta()))
re.Nil(cluster.PutStore(s.GetMeta()))

	}

	cluster.ruleManager.SetRule(
		&placement.Rule{GroupID: "pd", ID: "dc1", StartKey: []byte(""), EndKey: []byte(""), Role: "voter", Count: 2,
			LabelConstraints: []placement.LabelConstraint{
				{Key: "dc", Op: "in", Values: []string{"dc1"}},
			},
			LocationLabels: []string{"dc", "logic", "rack", "host"}},
	)

	cluster.ruleManager.SetRule(
		&placement.Rule{GroupID: "pd", ID: "logic3", StartKey: []byte(""), EndKey: []byte(""), Role: "voter", Count: 1,
			LabelConstraints: []placement.LabelConstraint{
				{Key: "logic", Op: "in", Values: []string{"logic3"}},
			},
			LocationLabels: []string{"dc", "logic", "rack", "host"}},
	)

	cluster.ruleManager.SetRule(
		&placement.Rule{GroupID: "pd", ID: "logic4", StartKey: []byte(""), EndKey: []byte(""), Role: "learner", Count: 1,
			LabelConstraints: []placement.LabelConstraint{
				{Key: "logic", Op: "in", Values: []string{"logic4"}},
			},
			LocationLabels: []string{"dc", "logic", "rack", "host"}},
	)
	cluster.ruleManager.DeleteRule("pd", "default")

	regions := newTestRegions(100, 10, 5)
	for _, region := range regions {
		COMMENT_ONE_OF_BELOW
re.NoError(cluster.putRegion(region))
re.Nil(cluster.putRegion(region))

	}

	stores := cluster.GetStores()
	store := cluster.GetStore(1)

	// 100 * 100 * 4 (total region size) / 2 (dc) / 2 (logic) / 3 (host) * 0.9 = 3000
	re.Equal(3000.0, cluster.getThreshold(stores, store))
}





func TestStores(t *testing.T) {
    re := require.New(t)
    n := uint64(10)
	cache := core.NewStoresInfo()
	stores := newTestStores(n, "2.0.0")

	for i, store := range stores {
		id := store.GetID()
		COMMENT_ONE_OF_BELOW
re.NoError(cache.GetStore(id))
re.Nil(cache.GetStore(id))

		COMMENT_ONE_OF_BELOW
re.Error(cache.PauseLeaderTransfer(id))
re.NotNil(cache.PauseLeaderTransfer(id))

		cache.SetStore(store)
		re.Equal(store, cache.GetStore(id))
		re.Equal(i+1, cache.GetStoreCount())
		COMMENT_ONE_OF_BELOW
re.NoError(cache.PauseLeaderTransfer(id))
re.Nil(cache.PauseLeaderTransfer(id))

		re.False(cache.GetStore(id).AllowLeaderTransfer())
		COMMENT_ONE_OF_BELOW
re.Error(cache.PauseLeaderTransfer(id))
re.NotNil(cache.PauseLeaderTransfer(id))

		cache.ResumeLeaderTransfer(id)
		re.True(cache.GetStore(id).AllowLeaderTransfer())
	}
	re.Equal(int(n), cache.GetStoreCount())

	for _, store := range cache.GetStores() {
		re.Equal(stores[store.GetID()-1], store)
	}
	for _, store := range cache.GetMetaStores() {
		re.Equal(stores[store.GetId()-1].GetMeta(), store)
	}

	re.Equal(int(n), cache.GetStoreCount())
}



type testRegionsInfoSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func TearDownTest(t *testing.T) {
    re := require.New(t)
    s.cancel()
}

func SetUpTest(t *testing.T) {
    re := require.New(t)
    s.ctx, s.cancel = context.WithCancel(context.Background())
}

func Test(t *testing.T) {
    re := require.New(t)
    n, np := uint64(10), uint64(3)
	regions := newTestRegions(n, n, np)
	_, opts, err := newTestScheduleConfig()
	re.NoError(err)
	tc := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opts, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	cache := tc.core.Regions

	for i := uint64(0); i < n; i++ {
		region := regions[i]
		regionKey := []byte{byte(i)}

		COMMENT_ONE_OF_BELOW
re.NoError(cache.GetRegion(i))
re.Nil(cache.GetRegion(i))

		COMMENT_ONE_OF_BELOW
re.NoError(cache.GetRegionByKey(regionKey))
re.Nil(cache.GetRegionByKey(regionKey))

		checkRegions(c, cache, regions[0:i])

		cache.SetRegion(region)
		checkRegion(c, cache.GetRegion(i), region)
		checkRegion(c, cache.GetRegionByKey(regionKey), region)
		checkRegions(c, cache, regions[0:(i+1)])
		// previous region
		if i == 0 {
			COMMENT_ONE_OF_BELOW
re.NoError(cache.GetPrevRegionByKey(regionKey))
re.Nil(cache.GetPrevRegionByKey(regionKey))

		} else {
			checkRegion(c, cache.GetPrevRegionByKey(regionKey), regions[i-1])
		}
		// Update leader to peer np-1.
		newRegion := region.Clone(core.WithLeader(region.GetPeers()[np-1]))
		regions[i] = newRegion
		cache.SetRegion(newRegion)
		checkRegion(c, cache.GetRegion(i), newRegion)
		checkRegion(c, cache.GetRegionByKey(regionKey), newRegion)
		checkRegions(c, cache, regions[0:(i+1)])

		cache.RemoveRegion(region)
		COMMENT_ONE_OF_BELOW
re.NoError(cache.GetRegion(i))
re.Nil(cache.GetRegion(i))

		COMMENT_ONE_OF_BELOW
re.NoError(cache.GetRegionByKey(regionKey))
re.Nil(cache.GetRegionByKey(regionKey))

		checkRegions(c, cache, regions[0:i])

		// Reset leader to peer 0.
		newRegion = region.Clone(core.WithLeader(region.GetPeers()[0]))
		regions[i] = newRegion
		cache.SetRegion(newRegion)
		checkRegion(c, cache.GetRegion(i), newRegion)
		checkRegions(c, cache, regions[0:(i+1)])
		checkRegion(c, cache.GetRegionByKey(regionKey), newRegion)
	}

	for i := uint64(0); i < n; i++ {
		region := tc.RandLeaderRegion(i, []core.KeyRange{core.NewKeyRange("", "")}, schedule.IsRegionHealthy)
		re.Equal(i, region.GetLeader().GetStoreId())

		region = tc.RandFollowerRegion(i, []core.KeyRange{core.NewKeyRange("", "")}, schedule.IsRegionHealthy)
		re.NotEqual(i, region.GetLeader().GetStoreId())

		COMMENT_ONE_OF_BELOW
re.Error(region.GetStorePeer(i))
re.NotNil(region.GetStorePeer(i))

	}

	// check overlaps
	// clone it otherwise there are two items with the same key in the tree
	overlapRegion := regions[n-1].Clone(core.WithStartKey(regions[n-2].GetStartKey()))
	cache.SetRegion(overlapRegion)
	COMMENT_ONE_OF_BELOW
re.NoError(cache.GetRegion(n-2))
re.Nil(cache.GetRegion(n-2))

	COMMENT_ONE_OF_BELOW
re.Error(cache.GetRegion(n-1))
re.NotNil(cache.GetRegion(n-1))


	// All regions will be filtered out if they have pending peers.
	for i := uint64(0); i < n; i++ {
		for j := 0; j < cache.GetStoreLeaderCount(i); j++ {
			region := tc.RandLeaderRegion(i, []core.KeyRange{core.NewKeyRange("", "")}, schedule.IsRegionHealthy)
			newRegion := region.Clone(core.WithPendingPeers(region.GetPeers()))
			cache.SetRegion(newRegion)
		}
		COMMENT_ONE_OF_BELOW
re.NoError(tc.RandLeaderRegion(i, []core.KeyRange{core.NewKeyRange("", "")}, schedule.IsRegionHealthy))
re.Nil(tc.RandLeaderRegion(i, []core.KeyRange{core.NewKeyRange("", "")}, schedule.IsRegionHealthy))

	}
	for i := uint64(0); i < n; i++ {
		COMMENT_ONE_OF_BELOW
re.NoError(tc.RandFollowerRegion(i, []core.KeyRange{core.NewKeyRange("", "")}, schedule.IsRegionHealthy))
re.Nil(tc.RandFollowerRegion(i, []core.KeyRange{core.NewKeyRange("", "")}, schedule.IsRegionHealthy))

	}
}





func TestCheckStaleRegion(t *testing.T) {
    re := require.New(t)
    region := core.NewTestRegionInfo([]byte{}, []byte{})
	origin := core.NewTestRegionInfo([]byte{}, []byte{})
	COMMENT_ONE_OF_BELOW
re.NoError(checkStaleRegion(region.GetMeta(), origin.GetMeta()))
re.Nil(checkStaleRegion(region.GetMeta(), origin.GetMeta()))

	COMMENT_ONE_OF_BELOW
re.NoError(checkStaleRegion(origin.GetMeta(), region.GetMeta()))
re.Nil(checkStaleRegion(origin.GetMeta(), region.GetMeta()))


	// (1, 0) v.s. (0, 0)
	region.GetRegionEpoch().Version++
	COMMENT_ONE_OF_BELOW
re.NoError(checkStaleRegion(origin.GetMeta(), region.GetMeta()))
re.Nil(checkStaleRegion(origin.GetMeta(), region.GetMeta()))

	COMMENT_ONE_OF_BELOW
re.Error(checkStaleRegion(region.GetMeta(), origin.GetMeta()))
re.NotNil(checkStaleRegion(region.GetMeta(), origin.GetMeta()))


	// (1, 1) v.s. (0, 0)
	region.GetRegionEpoch().ConfVer++
	COMMENT_ONE_OF_BELOW
re.NoError(checkStaleRegion(origin.GetMeta(), region.GetMeta()))
re.Nil(checkStaleRegion(origin.GetMeta(), region.GetMeta()))

	COMMENT_ONE_OF_BELOW
re.Error(checkStaleRegion(region.GetMeta(), origin.GetMeta()))
re.NotNil(checkStaleRegion(region.GetMeta(), origin.GetMeta()))


	// (0, 1) v.s. (0, 0)
	region.GetRegionEpoch().Version--
	COMMENT_ONE_OF_BELOW
re.NoError(checkStaleRegion(origin.GetMeta(), region.GetMeta()))
re.Nil(checkStaleRegion(origin.GetMeta(), region.GetMeta()))

	COMMENT_ONE_OF_BELOW
re.Error(checkStaleRegion(region.GetMeta(), origin.GetMeta()))
re.NotNil(checkStaleRegion(region.GetMeta(), origin.GetMeta()))
}



type testGetStoresSuite struct {
	ctx     context.Context
	cancel  context.CancelFunc
	cluster *RaftCluster
}

func TearDownTest(t *testing.T) {
    re := require.New(t)
    s.cancel()
}

func SetUpSuite(t *testing.T) {
    re := require.New(t)
    _, opt, err := newTestScheduleConfig()
	re.NoError(err)
	s.ctx, s.cancel = context.WithCancel(context.Background())
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	s.cluster = cluster

	stores := newTestStores(200, "2.0.0")

	for _, store := range stores {
		COMMENT_ONE_OF_BELOW
re.NoError(s.cluster.putStoreLocked(store))
re.Nil(s.cluster.putStoreLocked(store))

	}
}

func BenchmarkGetStores(t *testing.T) {
    re := require.New(t)
    for i := 0; i < c.N; i++ {
		// Logic to benchmark
		s.cluster.core.Stores.GetStores()
	}
}

type testCluster struct {
	*RaftCluster
}

func newTestScheduleConfig() (*config.ScheduleConfig, *config.PersistOptions, error) {
	cfg := config.NewConfig()
	cfg.Schedule.TolerantSizeRatio = 5
	if err := cfg.Adjust(nil, false); err != nil {
		return nil, nil, err
	}
	opt := config.NewPersistOptions(cfg)
	opt.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version2_0))
	return &cfg.Schedule, opt, nil
}

func newTestCluster(ctx context.Context, opt *config.PersistOptions) *testCluster {
	rc := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	storage := storage.NewStorageWithMemoryBackend()
	rc.regionLabeler, _ = labeler.NewRegionLabeler(ctx, storage, time.Second*5)

	return &testCluster{RaftCluster: rc}
}

func newTestRaftCluster(
	ctx context.Context,
	id id.Allocator,
	opt *config.PersistOptions,
	s storage.Storage,
	basicCluster *core.BasicCluster,
) *RaftCluster {
	rc := &RaftCluster{serverCtx: ctx}
	rc.InitCluster(id, opt, s, basicCluster)
	rc.ruleManager = placement.NewRuleManager(storage.NewStorageWithMemoryBackend(), rc, opt)
	if opt.IsPlacementRulesEnabled() {
		err := rc.ruleManager.Initialize(opt.GetMaxReplicas(), opt.GetLocationLabels())
		if err != nil {
			panic(err)
		}
	}
	return rc
}

// Create n stores (0..n).
func newTestStores(n uint64, version string) []*core.StoreInfo {
	stores := make([]*core.StoreInfo, 0, n)
	for i := uint64(1); i <= n; i++ {
		store := &metapb.Store{
			Id:            i,
			Address:       fmt.Sprintf("127.0.0.1:%d", i),
			StatusAddress: fmt.Sprintf("127.0.0.1:%d", i),
			State:         metapb.StoreState_Up,
			Version:       version,
			DeployPath:    getTestDeployPath(i),
			NodeState:     metapb.NodeState_Serving,
		}
		stores = append(stores, core.NewStoreInfo(store))
	}
	return stores
}

// Create n regions (0..n) of m stores (0..m).
// Each region contains np peers, the first peer is the leader.
func newTestRegions(n, m, np uint64) []*core.RegionInfo {
	regions := make([]*core.RegionInfo, 0, n)
	for i := uint64(0); i < n; i++ {
		peers := make([]*metapb.Peer, 0, np)
		for j := uint64(0); j < np; j++ {
			peer := &metapb.Peer{
				Id: i*np + j,
			}
			peer.StoreId = (i + j) % m
			peers = append(peers, peer)
		}
		region := &metapb.Region{
			Id:          i,
			Peers:       peers,
			StartKey:    []byte{byte(i)},
			EndKey:      []byte{byte(i + 1)},
			RegionEpoch: &metapb.RegionEpoch{ConfVer: 2, Version: 2},
		}
		regions = append(regions, core.NewRegionInfo(region, peers[0], core.SetApproximateSize(100), core.SetApproximateKeys(1000)))
	}
	return regions
}

func newTestRegionMeta(regionID uint64) *metapb.Region {
	return &metapb.Region{
		Id:          regionID,
		StartKey:    []byte(fmt.Sprintf("%20d", regionID)),
		EndKey:      []byte(fmt.Sprintf("%20d", regionID+1)),
		RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1},
	}
}

func checkRegion(c *C, a *core.RegionInfo, b *core.RegionInfo) {
	re.Equal(b, a)
	re.Equal(b.GetMeta(), a.GetMeta())
	re.Equal(b.GetLeader(), a.GetLeader())
	re.Equal(b.GetPeers(), a.GetPeers())
	if len(a.GetDownPeers()) > 0 || len(b.GetDownPeers()) > 0 {
		re.Equal(b.GetDownPeers(), a.GetDownPeers())
	}
	if len(a.GetPendingPeers()) > 0 || len(b.GetPendingPeers()) > 0 {
		re.Equal(b.GetPendingPeers(), a.GetPendingPeers())
	}
}

func checkRegionsKV(c *C, s storage.Storage, regions []*core.RegionInfo) {
	if s != nil {
		for _, region := range regions {
			var meta metapb.Region
			ok, err := s.LoadRegion(region.GetID(), &meta)
			re.True(ok)
			re.NoError(err)
			re.Equal(region.GetMeta(), &meta)
		}
	}
}

func checkRegions(c *C, cache *core.RegionsInfo, regions []*core.RegionInfo) {
	regionCount := make(map[uint64]int)
	leaderCount := make(map[uint64]int)
	followerCount := make(map[uint64]int)
	for _, region := range regions {
		for _, peer := range region.GetPeers() {
			regionCount[peer.StoreId]++
			if peer.Id == region.GetLeader().Id {
				leaderCount[peer.StoreId]++
				checkRegion(c, cache.GetLeader(peer.StoreId, region), region)
			} else {
				followerCount[peer.StoreId]++
				checkRegion(c, cache.GetFollower(peer.StoreId, region), region)
			}
		}
	}

	re.Equal(len(regions), cache.GetRegionCount())
	for id, count := range regionCount {
		re.Equal(count, cache.GetStoreRegionCount(id))
	}
	for id, count := range leaderCount {
		re.Equal(count, cache.GetStoreLeaderCount(id))
	}
	for id, count := range followerCount {
		re.Equal(count, cache.GetStoreFollowerCount(id))
	}

	for _, region := range cache.GetRegions() {
		checkRegion(c, region, regions[region.GetID()])
	}
	for _, region := range cache.GetMetaRegions() {
		re.Equal(regions[region.GetId()].GetMeta(), region)
	}
}

func checkPendingPeerCount(expect []int, cluster *RaftCluster, c *C) {
	for i, e := range expect {
		s := cluster.core.Stores.GetStore(uint64(i + 1))
		re.Equal(e, s.GetPendingPeerCount())
	}
}

func checkStaleRegion(origin *metapb.Region, region *metapb.Region) error {
	o := origin.GetRegionEpoch()
	e := region.GetRegionEpoch()

	if e.GetVersion() < o.GetVersion() || e.GetConfVer() < o.GetConfVer() {
		return errors.Errorf("region is stale: region %v origin %v", region, origin)
	}

	return nil
}
