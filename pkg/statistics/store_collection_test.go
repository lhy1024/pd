// Copyright 2018 TiKV Project Authors.
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

package statistics

import (
	"fmt"
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/statistics/utils"
)

func TestStoreStatistics(t *testing.T) {
	re := require.New(t)
	opt := mockconfig.NewTestOptions()
	rep := opt.GetReplicationConfig().Clone()
	rep.LocationLabels = []string{"zone", "host"}
	opt.SetReplicationConfig(rep)

	metaStores := []*metapb.Store{
		{Id: 1, Address: "mock://tikv-1:1", Labels: []*metapb.StoreLabel{{Key: "zone", Value: "z1"}, {Key: "host", Value: "h1"}}},
		{Id: 2, Address: "mock://tikv-2:2", Labels: []*metapb.StoreLabel{{Key: "zone", Value: "z1"}, {Key: "host", Value: "h2"}}},
		{Id: 3, Address: "mock://tikv-3:3", Labels: []*metapb.StoreLabel{{Key: "zone", Value: "z2"}, {Key: "host", Value: "h1"}}},
		{Id: 4, Address: "mock://tikv-4:4", Labels: []*metapb.StoreLabel{{Key: "zone", Value: "z2"}, {Key: "host", Value: "h2"}}},
		{Id: 5, Address: "mock://tikv-5:5", Labels: []*metapb.StoreLabel{{Key: "zone", Value: "z3"}, {Key: "host", Value: "h1"}}},
		{Id: 6, Address: "mock://tikv-6:6", Labels: []*metapb.StoreLabel{{Key: "zone", Value: "z3"}, {Key: "host", Value: "h2"}}},
		{Id: 7, Address: "mock://tikv-7:7", Labels: []*metapb.StoreLabel{{Key: "host", Value: "h1"}}},
		{Id: 8, Address: "mock://tikv-8:8", Labels: []*metapb.StoreLabel{{Key: "host", Value: "h2"}}},
		{Id: 8, Address: "mock://tikv-9:9", Labels: []*metapb.StoreLabel{{Key: "host", Value: "h3"}}, State: metapb.StoreState_Tombstone, NodeState: metapb.NodeState_Removed},
		{Id: 10, Address: "mock://tikv-10:10", Labels: []*metapb.StoreLabel{{Key: "zone", Value: "z4"}, {Key: "host", Value: "h4"}, {Key: core.EngineKey, Value: core.EngineTiFlash}}},
	}
	storesStats := NewStoresStats()
	stores := make([]*core.StoreInfo, 0, len(metaStores))
	for _, m := range metaStores {
		s := core.NewStoreInfo(m, core.SetLastHeartbeatTS(time.Now()))
		storesStats.GetOrCreateRollingStoreStats(m.GetId())
		stores = append(stores, s)
	}

	store3 := stores[3].Clone(core.SetStoreState(metapb.StoreState_Offline, false))
	stores[3] = store3
	store4 := stores[4].Clone(core.SetLastHeartbeatTS(stores[4].GetLastHeartbeatTS().Add(-time.Hour)))
	stores[4] = store4
	store5 := stores[5].Clone(core.SetStoreStats(&pdpb.StoreStats{
		Capacity:  512 * units.MiB,
		Available: 100 * units.MiB,
		UsedSize:  0,
	}))
	stores[5] = store5
	stores[8] = stores[8].Clone(core.SetLeaderCount(100))

	storeStats := NewStoreStatisticsMap(opt)
	for _, store := range stores {
		storeStats.Observe(store)
		ObserveHotStat(store, storesStats)
	}
	stats := storeStats.stats

	re.Len(stats.LabelCounter["zone:z1"], 2)
	re.Equal([]uint64{1, 2}, stats.LabelCounter["zone:z1"])
	re.Len(stats.LabelCounter["zone:z2"], 2)
	re.Len(stats.LabelCounter["zone:z3"], 2)
	re.Len(stats.LabelCounter["host:h1"], 4)
	re.Equal([]uint64{1, 3, 5, 7}, stats.LabelCounter["host:h1"])
	re.Len(stats.LabelCounter["host:h2"], 4)
	re.Len(stats.LabelCounter["zone:unknown"], 2)
}

func TestSummaryStoreInfos(t *testing.T) {
	re := require.New(t)
	rw := utils.Read
	kind := constant.LeaderKind
	collector := newTikvCollector()
	storeHistoryLoad := NewStoreHistoryLoads(DefaultHistorySampleDuration, DefaultHistorySampleInterval)
	storeInfos := make(map[uint64]*StoreSummaryInfo)
	storeLoads := make(map[uint64]StoreKindLoads)
	for _, storeID := range []uint64{1, 3} {
		storeInfos[storeID] = &StoreSummaryInfo{
			StoreInfo: core.NewStoreInfo(
				&metapb.Store{
					Id:      storeID,
					Address: fmt.Sprintf("mock://tikv-%d:%d", storeID, storeID),
				},
				core.SetLastHeartbeatTS(time.Now()),
			),
		}
		storeStats := StoreKindLoads{1, 2, 0, 0, 5}
		for i, v := range storeStats {
			storeStats[i] = v * float64(storeID)
		}
		storeLoads[storeID] = storeStats
	}

	// case 1: put one element into history load
	details := summaryStoresLoadByEngine(storeInfos, storeLoads, storeHistoryLoad, nil, rw, kind, collector)
	re.Len(details, 2)
	re.Empty(details[0].LoadPred.Current.HistoryLoads)
	re.Empty(details[1].LoadPred.Current.HistoryLoads)
	expectHistoryLoads := []float64{1, 2, 5, 0}
	for _, storeID := range []uint64{1, 3} {
		loads := storeHistoryLoad.Get(storeID, rw, kind)
		for i := range loads {
			for j := range loads[0] {
				if loads[i][j] != 0 {
					re.Equal(loads[i][j]/float64(storeID), expectHistoryLoads[i])
				}
			}
		}
	}

	// case 2: put many elements into history load
	storeHistoryLoad.sampleDuration = 0
	for i := 1; i < 10; i++ {
		details = summaryStoresLoadByEngine(storeInfos, storeLoads, storeHistoryLoad, nil, rw, kind, collector)
		expect := []float64{2, 4, 10, 0}
		for _, detail := range details {
			loads := detail.LoadPred.Current.HistoryLoads
			re.Len(loads, len(expectHistoryLoads))
			storeID := detail.GetID()
			for i := range loads {
				for j := range loads[0] {
					if loads[i][j] != 0 {
						re.Equal(loads[i][j]/float64(storeID), expectHistoryLoads[i])
					}
				}
			}

			re.Len(detail.LoadPred.Expect.HistoryLoads, len(expectHistoryLoads))
			for i, loads := range detail.LoadPred.Expect.HistoryLoads {
				for _, load := range loads {
					if load != 0 {
						re.Equal(load, expect[i])
					}
				}
			}
		}
	}
}

func TestTiKVReadUsesPeerLoadSumCPU(t *testing.T) {
	re := require.New(t)
	rw := utils.Read
	kind := constant.LeaderKind
	collector := newTikvCollector()
	storeInfos := make(map[uint64]*StoreSummaryInfo)
	storeLoads := make(map[uint64]StoreKindLoads)
	storeHotPeers := make(map[uint64][]*HotPeerStat)

	for _, storeID := range []uint64{1, 2} {
		storeInfos[storeID] = &StoreSummaryInfo{
			StoreInfo: core.NewStoreInfo(
				&metapb.Store{
					Id:      storeID,
					Address: fmt.Sprintf("mock://tikv-%d:%d", storeID, storeID),
				},
				core.SetLastHeartbeatTS(time.Now()),
			),
		}
	}

	storeLoads[1] = StoreKindLoads{
		utils.StoreReadBytes: 1000,
		utils.StoreReadKeys:  2000,
		utils.StoreReadQuery: 3000,
		utils.StoreReadCPU:   4000,
	}
	storeLoads[2] = StoreKindLoads{
		utils.StoreReadBytes: 5000,
		utils.StoreReadKeys:  6000,
		utils.StoreReadQuery: 7000,
		utils.StoreReadCPU:   8000,
	}

	storeHotPeers[1] = []*HotPeerStat{
		{
			StoreID:  1,
			RegionID: 101,
			Loads:    []float64{10, 20, 30, 40},
			isLeader: true,
		},
		{
			StoreID:  1,
			RegionID: 102,
			Loads:    []float64{1, 2, 3, 4},
			isLeader: true,
		},
	}
	storeHotPeers[2] = []*HotPeerStat{
		{
			StoreID:  2,
			RegionID: 201,
			Loads:    []float64{5, 6, 7, 8},
			isLeader: true,
		},
	}

	details := summaryStoresLoadByEngine(storeInfos, storeLoads, nil, storeHotPeers, rw, kind, collector)
	re.Len(details, 2)

	var store1, store2 *StoreLoadDetail
	for _, detail := range details {
		switch detail.GetID() {
		case 1:
			store1 = detail
		case 2:
			store2 = detail
		}
	}

	re.NotNil(store1)
	re.NotNil(store2)

	re.Equal(1000.0, store1.LoadPred.Current.Loads[utils.ByteDim])
	re.Equal(2000.0, store1.LoadPred.Current.Loads[utils.KeyDim])
	re.Equal(3000.0, store1.LoadPred.Current.Loads[utils.QueryDim])
	re.Equal(44.0, store1.LoadPred.Current.Loads[utils.CPUDim])

	re.Equal(5000.0, store2.LoadPred.Current.Loads[utils.ByteDim])
	re.Equal(6000.0, store2.LoadPred.Current.Loads[utils.KeyDim])
	re.Equal(7000.0, store2.LoadPred.Current.Loads[utils.QueryDim])
	re.Equal(8.0, store2.LoadPred.Current.Loads[utils.CPUDim])

	re.Equal(3000.0, store1.LoadPred.Expect.Loads[utils.ByteDim])
	re.Equal(4000.0, store1.LoadPred.Expect.Loads[utils.KeyDim])
	re.Equal(5000.0, store1.LoadPred.Expect.Loads[utils.QueryDim])
	re.Equal(26.0, store1.LoadPred.Expect.Loads[utils.CPUDim])
	re.Equal(store1.LoadPred.Expect.Loads, store2.LoadPred.Expect.Loads)
}

func TestTiFlashComputeExcludedFromExpectation(t *testing.T) {
	re := require.New(t)
	rw := utils.Write
	kind := constant.RegionKind
	// Use isTraceRegionFlow=true to use StoreRegionsWriteBytes instead of peerLoadSum
	collector := newTiFlashCollector(true)
	storeHistoryLoad := NewStoreHistoryLoads(DefaultHistorySampleDuration, DefaultHistorySampleInterval)
	storeInfos := make(map[uint64]*StoreSummaryInfo)
	storeLoads := make(map[uint64]StoreKindLoads)

	// Create 3 TiFlash Write nodes with 5 MB/s each
	for _, storeID := range []uint64{1, 2, 3} {
		storeInfos[storeID] = &StoreSummaryInfo{
			StoreInfo: core.NewStoreInfo(
				&metapb.Store{
					Id:      storeID,
					Address: fmt.Sprintf("mock://tiflash-%d:%d", storeID, storeID),
					Labels:  []*metapb.StoreLabel{{Key: core.EngineKey, Value: core.EngineTiFlash}},
				},
				core.SetLastHeartbeatTS(time.Now()),
			),
		}
		// Simulate 5 MB/s write load
		storeLoads[storeID] = StoreKindLoads{
			utils.StoreWriteBytes:        5 * units.MiB,
			utils.StoreWriteKeys:         1000,
			utils.StoreRegionsWriteBytes: 5 * units.MiB,
			utils.StoreRegionsWriteKeys:  1000,
		}
	}

	// Create 2 TiFlash Compute nodes with 0 MB/s (or very low load)
	for _, storeID := range []uint64{4, 5} {
		storeInfos[storeID] = &StoreSummaryInfo{
			StoreInfo: core.NewStoreInfo(
				&metapb.Store{
					Id:      storeID,
					Address: fmt.Sprintf("mock://tiflash-compute-%d:%d", storeID, storeID),
					Labels:  []*metapb.StoreLabel{{Key: core.EngineKey, Value: core.EngineTiFlashCompute}},
				},
				core.SetLastHeartbeatTS(time.Now()),
			),
		}
		// TiFlash Compute has no write load
		storeLoads[storeID] = StoreKindLoads{
			utils.StoreWriteBytes:        0,
			utils.StoreWriteKeys:         0,
			utils.StoreRegionsWriteBytes: 0,
			utils.StoreRegionsWriteKeys:  0,
		}
	}

	details := summaryStoresLoadByEngine(storeInfos, storeLoads, storeHistoryLoad, nil, rw, kind, collector)

	// Should only include 3 TiFlash Write nodes, not the 2 TiFlash Compute nodes
	re.Len(details, 3, "Should only include TiFlash Write nodes, not TiFlash Compute nodes")

	// Verify that all returned details are TiFlash Write nodes
	for _, detail := range details {
		re.True(detail.GetID() >= 1 && detail.GetID() <= 3, "Should only include TiFlash Write nodes (ID 1-3)")
		re.True(detail.IsTiFlashWrite())
	}

	// Verify expectation is calculated based only on TiFlash Write nodes
	// Expected byte load should be (5+5+5)/3 = 5 MB/s, NOT (5+5+5+0+0)/5 = 3 MB/s
	for _, detail := range details {
		expectByteLoad := detail.LoadPred.Expect.Loads[utils.ByteDim]
		// Should be close to 5 MB/s (average of the 3 TiFlash Write nodes)
		re.InDelta(5*units.MiB, expectByteLoad, 0.1*units.MiB,
			"Expectation should be 5 MB/s (average of TiFlash Write nodes only), not lowered by TiFlash Compute nodes")

		// Verify current load for TiFlash Write nodes
		currentByteLoad := detail.LoadPred.Current.Loads[utils.ByteDim]
		re.InDelta(5*units.MiB, currentByteLoad, 0.1*units.MiB,
			"TiFlash Write nodes should have 5 MB/s load")
	}

	expectCount := details[0].LoadPred.Expect.HotPeerCount
	re.Equal(0.0, expectCount, "No hot peers in this test, expect count should be 0")
}

func TestTiKVNotAffectedByTiFlashCompute(t *testing.T) {
	re := require.New(t)
	rw := utils.Write
	// Use RegionKind for TiKV to use StoreWriteBytes instead of peerLoadSum
	kind := constant.RegionKind
	tikvCollector := newTikvCollector()
	tiflashCollector := newTiFlashCollector(true)
	storeHistoryLoad := NewStoreHistoryLoads(DefaultHistorySampleDuration, DefaultHistorySampleInterval)
	storeInfos := make(map[uint64]*StoreSummaryInfo)
	storeLoads := make(map[uint64]StoreKindLoads)

	// Create 3 TiKV nodes with 10 MB/s each
	for _, storeID := range []uint64{1, 2, 3} {
		storeInfos[storeID] = &StoreSummaryInfo{
			StoreInfo: core.NewStoreInfo(
				&metapb.Store{
					Id:      storeID,
					Address: fmt.Sprintf("mock://tikv-%d:%d", storeID, storeID),
				},
				core.SetLastHeartbeatTS(time.Now()),
			),
		}
		// Simulate 10 MB/s write load
		storeLoads[storeID] = StoreKindLoads{
			utils.StoreWriteBytes: 10 * units.MiB,
			utils.StoreWriteKeys:  2000,
			utils.StoreWriteQuery: 100,
		}
	}

	// Create 2 TiFlash Compute nodes with 0 MB/s
	for _, storeID := range []uint64{4, 5} {
		storeInfos[storeID] = &StoreSummaryInfo{
			StoreInfo: core.NewStoreInfo(
				&metapb.Store{
					Id:      storeID,
					Address: fmt.Sprintf("mock://tiflash-compute-%d:%d", storeID, storeID),
					Labels:  []*metapb.StoreLabel{{Key: core.EngineKey, Value: core.EngineTiFlashCompute}},
				},
				core.SetLastHeartbeatTS(time.Now()),
			),
		}
		// TiFlash Compute has no write load
		storeLoads[storeID] = StoreKindLoads{
			utils.StoreWriteBytes:        0,
			utils.StoreWriteKeys:         0,
			utils.StoreRegionsWriteBytes: 0,
			utils.StoreRegionsWriteKeys:  0,
		}
	}

	// Test TiKV expectation calculation
	tikvDetails := summaryStoresLoadByEngine(storeInfos, storeLoads, storeHistoryLoad, nil, rw, kind, tikvCollector)

	// Should only include 3 TiKV nodes
	re.Len(tikvDetails, 3, "Should only include TiKV nodes")

	// Verify that all returned details are TiKV nodes
	for _, detail := range tikvDetails {
		re.True(detail.GetID() >= 1 && detail.GetID() <= 3, "Should only include TiKV nodes (ID 1-3)")
		re.True(detail.IsTiKV(), "TiKV nodes should not be marked as TiFlash")
	}

	// Verify TiKV expectation is calculated based only on TiKV nodes
	// Expected byte load should be (10+10+10)/3 = 10 MB/s
	// Should NOT be affected by TiFlash Compute nodes
	for _, detail := range tikvDetails {
		expectByteLoad := detail.LoadPred.Expect.Loads[utils.ByteDim]
		re.InDelta(10*units.MiB, expectByteLoad, 0.1*units.MiB,
			"TiKV expectation should be 10 MB/s (average of TiKV nodes only), not affected by TiFlash Compute")

		currentByteLoad := detail.LoadPred.Current.Loads[utils.ByteDim]
		re.InDelta(10*units.MiB, currentByteLoad, 0.1*units.MiB,
			"TiKV nodes should have 10 MB/s load")
	}

	// Test TiFlash expectation calculation - should get empty result because no TiFlash Write nodes
	tiflashDetails := summaryStoresLoadByEngine(storeInfos, storeLoads, storeHistoryLoad, nil, rw, constant.RegionKind, tiflashCollector)
	re.Empty(tiflashDetails)
}
