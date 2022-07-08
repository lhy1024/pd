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
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/statistics"
	"github.com/tikv/pd/server/storage"
	"github.com/tikv/pd/server/versioninfo"
)

var _ = Suite(&testHotSchedulerSuite{})

func (s *testHotSchedulerSuite) TestFromFile(c *C) {
	// load file
	path := "/data2/lhy1024/1.txt"
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
	fmt.Println(len(infos.AsLeader))

	// start cluster
	rw := statistics.Read
	statistics.Denoising = false
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := config.NewTestOptions()
	hb, err := schedule.CreateScheduler(statistics.Write.String(), schedule.NewOperatorController(ctx, nil, nil), storage.NewStorageWithMemoryBackend(), nil)
	c.Assert(err, IsNil)
	hb.(*hotScheduler).conf.SetDstToleranceRatio(1)
	hb.(*hotScheduler).conf.SetSrcToleranceRatio(1)

	tc := mockcluster.NewCluster(ctx, opt)
	tc.SetHotRegionCacheHitsThreshold(0)
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))

	var regions []testRegionInfo
	for storeID, store := range infos.AsLeader {
		tc.AddRegionStore(storeID, store.Count)
		if rw == statistics.Read {
			tc.UpdateStorageReadBytes(storeID, uint64(store.StoreByteRate))
			tc.UpdateStorageReadKeys(storeID, uint64(store.StoreKeyRate))
			tc.UpdateStorageReadQuery(storeID, uint64(store.StoreQueryRate))
		}
		for _, region := range store.Stats {
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
	ops, _ := hb.Schedule(tc, false)
	c.Check(ops, HasLen, 0)
}
