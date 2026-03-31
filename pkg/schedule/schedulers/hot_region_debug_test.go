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

package schedulers

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/statistics/utils"
)

func TestBuildHotStoreDebugSnapshotAndRank(t *testing.T) {
	re := require.New(t)
	loads := make([]float64, utils.DimLen)
	loads[utils.ByteDim] = 321
	loads[utils.QueryDim] = 654
	loads[utils.CPUDim] = 987
	detail := &statistics.StoreLoadDetail{
		StoreSummaryInfo: &statistics.StoreSummaryInfo{StoreInfo: core.NewStoreInfoWithLabel(1, map[string]string{})},
		LoadPred: &statistics.StoreLoadPred{
			Current: statistics.StoreLoad{
				Loads: loads,
			},
		},
		HotPeers: []*statistics.HotPeerStat{
			{StoreID: 1, RegionID: 11, HotDegree: 4, AntiCount: 1, Loads: []float64{10, 0, 90, 200}},
			{StoreID: 1, RegionID: 12, HotDegree: 3, AntiCount: 2, Loads: []float64{20, 0, 80, 90}},
			{StoreID: 1, RegionID: 13, HotDegree: 2, AntiCount: 3, Loads: []float64{30, 0, 70, 160}},
		},
	}

	snapshot := buildHotStoreDebugSnapshot(detail, 2)
	re.Equal(uint64(1), snapshot.StoreID)
	re.Equal(987.0, snapshot.StoreCPU)
	re.Equal(654.0, snapshot.StoreQuery)
	re.Equal(321.0, snapshot.StoreByte)
	re.Equal(3, snapshot.HotPeerCount)
	re.Len(snapshot.TopN, 2)
	re.Equal(uint64(11), snapshot.TopN[0].RegionID)
	re.Equal(200.0, snapshot.TopN[0].CPU)
	re.Equal(uint64(13), snapshot.TopN[1].RegionID)
	re.Equal(160.0, snapshot.TopN[1].CPU)

	rank, cpu := findHotPeerRankByCPU(detail.HotPeers, 13, 10)
	re.Equal(2, rank)
	re.Equal(160.0, cpu)

	rank, cpu = findHotPeerRankByCPU(detail.HotPeers, 12, 1)
	re.Zero(rank)
	re.Zero(cpu)
}

func TestDiffHotStoreDebugSnapshot(t *testing.T) {
	re := require.New(t)
	prev := hotStoreDebugSnapshot{
		StoreID: 1,
		TopN: []hotStoreDebugPeer{
			{RegionID: 11, CPU: 200},
			{RegionID: 12, CPU: 150},
			{RegionID: 13, CPU: 100},
		},
	}
	curr := hotStoreDebugSnapshot{
		StoreID: 1,
		TopN: []hotStoreDebugPeer{
			{RegionID: 12, CPU: 190},
			{RegionID: 14, CPU: 180},
			{RegionID: 11, CPU: 120},
		},
	}

	delta := diffHotStoreDebugSnapshot(prev, curr)
	re.Len(delta.EnterTopN, 1)
	re.Equal(uint64(14), delta.EnterTopN[0].RegionID)
	re.Len(delta.ExitTopN, 1)
	re.Equal(uint64(13), delta.ExitTopN[0].RegionID)
	re.Len(delta.CPUUpTop5, 1)
	re.Equal(uint64(12), delta.CPUUpTop5[0].RegionID)
	re.Equal(150.0, delta.CPUUpTop5[0].OldCPU)
	re.Equal(190.0, delta.CPUUpTop5[0].NewCPU)
	re.Equal(2, delta.CPUUpTop5[0].OldRank)
	re.Equal(1, delta.CPUUpTop5[0].NewRank)
	re.Len(delta.CPUDownTop5, 1)
	re.Equal(uint64(11), delta.CPUDownTop5[0].RegionID)
	re.Equal(200.0, delta.CPUDownTop5[0].OldCPU)
	re.Equal(120.0, delta.CPUDownTop5[0].NewCPU)
	re.Equal(1, delta.CPUDownTop5[0].OldRank)
	re.Equal(3, delta.CPUDownTop5[0].NewRank)
}

func TestBuildHotStoreDebugPeersKeepTopNOrHighCPU(t *testing.T) {
	re := require.New(t)
	peers := []*statistics.HotPeerStat{
		{RegionID: 1, HotDegree: 1, Loads: []float64{0, 0, 0, 210}},
		{RegionID: 2, HotDegree: 1, Loads: []float64{0, 0, 0, 180}},
		{RegionID: 3, HotDegree: 1, Loads: []float64{0, 0, 0, 150}},
		{RegionID: 4, HotDegree: 1, Loads: []float64{0, 0, 0, 120}},
		{RegionID: 5, HotDegree: 1, Loads: []float64{0, 0, 0, 95}},
	}

	ret := buildHotStoreDebugPeers(peers, 2)
	re.Len(ret, 4)
	re.Equal([]uint64{1, 2, 3, 4}, []uint64{ret[0].RegionID, ret[1].RegionID, ret[2].RegionID, ret[3].RegionID})
}

func TestHotReadDebugTracerLogsAllStoresByDefault(t *testing.T) {
	re := require.New(t)
	tracer := &hotReadDebugTracer{}
	re.True(tracer.shouldLogStore(1))
	re.True(tracer.shouldLogStore(14))
	re.True(tracer.shouldLogStore(999))
}
