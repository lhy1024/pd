// Copyright 2022 TiKV Project Authors.
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
	"context"
	"encoding/json"
	"fmt"
	"os"

	. "github.com/pingcap/check"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/statistics"
	"github.com/tikv/pd/server/storage"
	"go.uber.org/zap"
)

var _ = Suite(&testHotSchedulerSuite{})

func (s *testHotSchedulerSuite) TestFromFile(c *C) {
	// input
	path := "/data2/lhy1024/2.txt"
	rw := statistics.Read
	op := transferLeader
	src := uint64(1)
	dst := uint64(6)
	region := uint64(47404166)
	// load file
	b, err := os.ReadFile(path)
	if err != nil {
		fmt.Println(err)
		return
	}
	var infos statistics.StoreHotPeersInfos
	err = json.Unmarshal(b, &infos)
	if err != nil {
		fmt.Println(err)
		return
	}

	// start cluster
	statistics.Denoising = false
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := config.NewTestOptions()
	hb, err := schedule.CreateScheduler(statistics.Read.String(), schedule.NewOperatorController(ctx, nil, nil), storage.NewStorageWithMemoryBackend(), nil)
	c.Assert(err, IsNil)
	hb.(*hotScheduler).conf.SetDstToleranceRatio(1)
	hb.(*hotScheduler).conf.SetSrcToleranceRatio(1)
	hb.(*hotScheduler).conf.SetStrictPickingStore(false)
	hb.(*hotScheduler).conf.ReadPriorities = []string{dimToString(statistics.QueryDim), dimToString(statistics.ByteDim)}

	tc := mockcluster.NewCluster(ctx, opt)
	tc.SetHotRegionCacheHitsThreshold(0)

	var regions []testRegionInfo
	var stores statistics.StoreHotPeersStat
	if op == transferLeader {
		stores = infos.AsLeader
	} else {
		stores = infos.AsPeer
	}

	for storeID, store := range stores {
		tc.AddRegionStore(storeID, store.Count)
		if rw == statistics.Read {
			tc.UpdateStorageReadBytes(storeID, uint64(store.StoreByteRate)*statistics.StoreHeartBeatReportInterval)
			tc.UpdateStorageReadKeys(storeID, uint64(store.StoreKeyRate)*statistics.StoreHeartBeatReportInterval)
			tc.UpdateStorageReadQuery(storeID, uint64(store.StoreQueryRate)*statistics.StoreHeartBeatReportInterval)
		}
		for _, region := range store.Stats {
			stores := []uint64{ // leaderID
				storeID,
			}
			for _, peerStoreID := range region.Stores {
				if peerStoreID != storeID {
					stores = append(stores, peerStoreID)
				}
			}
			regions = append(regions, testRegionInfo{
				id:        region.RegionID,
				peers:     region.Stores,
				byteRate:  region.ByteRate,
				keyRate:   region.KeyRate,
				queryRate: region.QueryRate,
			})
		}
	}
	addRegionInfo(tc, rw, regions)
	hb.Schedule(tc, false) // prepare
	clearPendingInfluence(hb.(*hotScheduler))
	log.Info("==================================")
	// check input
	bs := newBalanceSolver(hb.(*hotScheduler), tc, rw, op)
	bs.cur = bs.buildSolution()
	srcStore, ok := bs.filterSrcStores()[src]
	if !ok {
		log.Info("src store not found in available stores", zap.Uint64s("available", toSlice(bs.filterSrcStores())))
		return
	}
	srcPeerStat, ok := bs.filterHotPeers(srcStore)[region]
	if !ok {
		log.Info("region not found in hot peers of src store")
		return
	}
	bs.cur.region = bs.getRegion(srcPeerStat, src)
	bs.cur.srcStore = srcStore
	bs.cur.hotPeerStat = srcPeerStat
	dstStore, ok := bs.filterDstStores()[dst]
	if !ok {
		log.Info("dst store not found in available stores", zap.Uint64s("available", toSlice(bs.filterDstStores())))
		return
	}
	bs.cur.dstStore = dstStore
	// calculate
	bs.cur.calcProgressiveRank()
	if bs.filterUniformStore() {
		log.Info("uniform store")
		return
	}
	log.Info("result:", zap.Uint64("src", src), zap.Uint64("dst", dst), zap.Uint64("region", region), zap.Int64("rank", bs.cur.progressiveRank))
	bs.cur.logPriority(statistics.QueryDim, true /*is more*/)
	bs.cur.logPriority(statistics.ByteDim, true /*is more*/)

}

func toSlice(stores map[uint64]*statistics.StoreLoadDetail) []uint64 {
	ret := make([]uint64, 0)
	for store := range stores {
		ret = append(ret, store)
	}
	return ret
}
