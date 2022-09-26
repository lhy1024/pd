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

package cases

import (
	"encoding/json"
	"os"

	"github.com/docker/go-units"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/statistics"
	"github.com/tikv/pd/tools/pd-simulator/simulator/info"
	"go.uber.org/zap"
)

func newHotReadFromFile() *Case {
	var simCase Case
	return &simCase
}

func newHotRead() *Case {
	//todo add hot region flag to avoid schedule them
	var simCase Case
	log.Info("start to load case")
	// load file
	path := "/data2/lhy1024/hot_pdctl/2.txt" //todo
	file, err := os.ReadFile(path)
	if err != nil {
		log.Fatal("open file error", zap.Error(err))
		return nil
	}
	var hotReadInfos statistics.StoreHotPeersInfos
	err = json.Unmarshal(file, &hotReadInfos)
	if err != nil {
		log.Fatal("json unmarshal error", zap.Error(err))
		return nil
	}

	// remap store id
	storeMap := make(map[uint64]*storeInfo)
	for storeID := range hotReadInfos.AsLeader {
		simulateID := IDAllocator.nextID()
		storeMap[storeID] = &storeInfo{
			storeID:    storeID,
			simulateID: simulateID,
		}
	}

	// build case
	storeHeartBeatPeriod := 10
	factor := 0.3
	minHotReadByte := statistics.MinHotThresholds[statistics.RegionReadBytes] * float64(storeHeartBeatPeriod) * factor
	minHotReadKey := statistics.MinHotThresholds[statistics.RegionReadKeys] * float64(storeHeartBeatPeriod) * factor
	minHotReadQuery := statistics.MinHotThresholds[statistics.RegionReadQueryNum] * float64(storeHeartBeatPeriod) * factor
	for storeID, store := range hotReadInfos.AsLeader {
		simulateID := storeMap[storeID].simulateID
		loads := make([]float64, statistics.StoreStatCount)
		loads[statistics.StoreReadBytes] = store.StoreByteRate * float64(storeHeartBeatPeriod)
		loads[statistics.StoreReadKeys] = store.StoreKeyRate * float64(storeHeartBeatPeriod)
		loads[statistics.StoreReadQuery] = store.StoreQueryRate * float64(storeHeartBeatPeriod)
		simCase.Stores = append(simCase.Stores, &Store{
			ID:        simulateID,
			Status:    metapb.StoreState_Up,
			Capacity:  6 * units.TiB,
			Available: 6 * units.TiB,
			Version:   "6.0.0",
			Loads:     loads,
		})
		hotPeerSum := make([]float64, statistics.RegionStatCount)
		for _, region := range store.Stats {
			var leader *metapb.Peer
			var peers []*metapb.Peer
			// todo fix rand perm
			for _, peerStoreID := range region.Stores {
				peer := &metapb.Peer{
					Id:      IDAllocator.nextID(),
					StoreId: storeMap[peerStoreID].simulateID,
				}
				if peerStoreID == storeID {
					leader = peer
					storeMap[peerStoreID].leaderNum += 1
				}
				peers = append(peers, peer)
				storeMap[peerStoreID].peerNum += 1
			}
			if leader == nil {
				log.Error("region doesn't have leader", zap.Uint64("region", region.RegionID))
			}
			regionID := IDAllocator.nextID()
			loads := make([]float64, statistics.RegionStatCount)
			loads[statistics.RegionReadBytes] = region.ByteRate * float64(storeHeartBeatPeriod)
			loads[statistics.RegionReadKeys] = region.KeyRate * float64(storeHeartBeatPeriod)
			loads[statistics.RegionReadQueryNum] = region.QueryRate * float64(storeHeartBeatPeriod)
			simCase.Regions = append(simCase.Regions, Region{
				ID:     regionID,
				Peers:  peers,
				Leader: leader,
				Size:   96 * units.MiB,
				Keys:   960000,
				Loads:  loads,
			})
			hotPeerSum[statistics.RegionReadBytes] += loads[statistics.RegionReadBytes]
			hotPeerSum[statistics.RegionReadKeys] += loads[statistics.RegionReadKeys]
			hotPeerSum[statistics.RegionReadQueryNum] += loads[statistics.RegionReadQueryNum]
		}
		// generate cold peer
		storeByte := loads[statistics.StoreReadBytes] - hotPeerSum[statistics.RegionReadBytes]
		storeKey := loads[statistics.StoreReadKeys] - hotPeerSum[statistics.RegionReadKeys]
		storeQueryNum := loads[statistics.StoreReadQuery] - hotPeerSum[statistics.RegionReadQueryNum]
		for {
			loads := make([]float64, statistics.RegionStatCount)
			if storeByte > 0 {
				loads[statistics.RegionReadBytes] = minHotReadByte
				storeByte -= minHotReadByte
			}
			if storeKey > 0 {
				loads[statistics.RegionReadKeys] = minHotReadKey
				storeKey -= minHotReadKey
			}
			if storeQueryNum > 0 {
				loads[statistics.RegionReadQueryNum] = minHotReadQuery
				storeQueryNum -= minHotReadQuery
			}
			if !slice.AnyOf(loads, func(i int) bool {
				return loads[i] > 0
			}) {
				break
			}
			peers := []*metapb.Peer{
				{Id: IDAllocator.nextID(), StoreId: simulateID},
			}
			storeMap[storeID].leaderNum += 1
			storeMap[storeID].peerNum += 1
			for _, s := range storeMap {
				if slice.NoneOf(peers, func(i int) bool {
					return peers[i].StoreId == s.storeID
				}) {
					peers = append(peers, &metapb.Peer{
						Id:      IDAllocator.nextID(),
						StoreId: s.simulateID,
					})
					storeMap[storeID].peerNum += 1
				}
				if len(peers) == 3 {
					break
				}
			}
			simCase.Regions = append(simCase.Regions, Region{
				ID:     IDAllocator.nextID(),
				Peers:  peers,
				Leader: peers[0],
				Size:   96 * units.MiB,
				Keys:   960000,
				Loads:  loads,
			})
		}
	}
	// add region to avoid balance region scheduler and balance leader scheduler
	maxLeaderNum, maxPeerNum := 0, 0
	for _, s := range storeMap {
		if s.leaderNum > maxLeaderNum {
			maxLeaderNum = s.leaderNum
		}
		if s.peerNum > maxPeerNum {
			maxPeerNum = s.peerNum
		}
	}
	for _, s := range storeMap {
		for {
			if s.leaderNum >= maxLeaderNum {
				break
			}
			peers := []*metapb.Peer{
				{Id: IDAllocator.nextID(), StoreId: s.simulateID},
			}
			storeMap[s.storeID].leaderNum++
			storeMap[s.storeID].peerNum++
			for _, candidate := range sortStoreInfos(storeMap) {
				if slice.NoneOf(peers, func(i int) bool {
					return peers[i].StoreId == candidate.storeID && candidate.peerNum <= maxPeerNum
				}) {
					peers = append(peers, &metapb.Peer{
						Id:      IDAllocator.nextID(),
						StoreId: s.simulateID,
					})
					storeMap[candidate.storeID].peerNum++
				}
				if len(peers) == 3 {
					break
				}
			}
			simCase.Regions = append(simCase.Regions, Region{
				ID:     IDAllocator.nextID(),
				Peers:  peers,
				Leader: peers[0],
				Size:   96 * units.MiB,
				Keys:   960000,
				Loads:  make([]float64, statistics.RegionStatCount),
			})
		}
	}
	for _, s := range storeMap {
		log.Info("store info", zap.Uint64("store id", s.storeID), zap.Uint64("simulator id", s.simulateID),
			zap.Int("leader num", s.leaderNum), zap.Int("peer num", s.peerNum))
	}
	log.Info("loading info", zap.Int("store", len(simCase.Stores)), zap.Int("region", len(simCase.Regions)), zap.Uint64("used id", IDAllocator.id))

	e := &CheckSchedulerDescriptor{}
	simCase.Events = []EventDescriptor{e}
	simCase.Checker = func(regions *core.RegionsInfo, stats []info.StoreStats) bool {
		// observe schedule for developer
		return false
	}

	return &simCase
}
