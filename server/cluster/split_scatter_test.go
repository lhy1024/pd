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

package cluster

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/schedule/scatter"
	"github.com/tikv/pd/pkg/storage"
)

func TestHandleAskBatchSplitSchedulesSplitScatterInPatrol(t *testing.T) {
	re := require.New(t)
	cluster := newSplitScatterTestCluster(t)

	request := &pdpb.AskBatchSplitRequest{
		Region:     cluster.GetRegion(100).GetMeta(),
		SplitCount: 2,
	}
	resp, err := cluster.HandleAskBatchSplit(request)
	re.NoError(err)
	re.Len(resp.GetIds(), 2)

	splitRegionIDs := []uint64{
		resp.GetIds()[0].GetNewRegionId(),
		resp.GetIds()[1].GetNewRegionId(),
	}
	re.NoError(cluster.processRegionHeartbeat(core.ContextTODO(), newSplitScatterRegion(splitRegionIDs[0], []byte("m"), []byte("t"), 120)))
	re.NoError(cluster.processRegionHeartbeat(core.ContextTODO(), newSplitScatterRegion(splitRegionIDs[1], []byte("t"), []byte(""), 80)))

	cluster.GetCoordinator().GetCheckerController().DispatchSplitScatterRegions()

	group := ""
	for _, regionID := range splitRegionIDs {
		op := cluster.GetOperatorController().GetOperator(regionID)
		if op == nil {
			continue
		}
		re.Equal(scatter.InternalScatterOperatorDesc, op.Desc())
		opGroup, ok := op.GetAdditionalInfo("group")
		re.True(ok)
		if group == "" {
			group = opGroup
			re.True(strings.HasPrefix(group, "split-scatter-100-"))
			continue
		}
		re.Equal(group, opGroup)
	}
	re.NotEmpty(group)
	re.Nil(cluster.GetOperatorController().GetOperator(100))
}

func TestHandleAskBatchSplitSeedsIndexBaselineForFirstSplitRegion(t *testing.T) {
	re := require.New(t)
	cluster := newSplitScatterTestCluster(t)

	re.NoError(cluster.putRegion(newSplitScatterRegion(90, newSplitScatterIndexKey("a"), newSplitScatterIndexKey("j"), 0)))
	re.NoError(cluster.putRegion(newSplitScatterRegion(91, newSplitScatterIndexKey("j"), newSplitScatterIndexKey("t"), 0)))
	re.NoError(cluster.putRegion(newSplitScatterRegion(100, newSplitScatterIndexKey("t"), newSplitScatterIndexKey("z"), 0)))

	request := &pdpb.AskBatchSplitRequest{
		Region:     cluster.GetRegion(100).GetMeta(),
		SplitCount: 1,
	}
	resp, err := cluster.HandleAskBatchSplit(request)
	re.NoError(err)
	re.Len(resp.GetIds(), 1)

	splitRegionID := resp.GetIds()[0].GetNewRegionId()
	re.NoError(cluster.processRegionHeartbeat(
		core.ContextTODO(),
		newSplitScatterRegion(splitRegionID, newSplitScatterIndexKey("t"), newSplitScatterIndexKey("w"), 120),
	))

	cluster.GetCoordinator().GetCheckerController().DispatchSplitScatterRegions()

	op := cluster.GetOperatorController().GetOperator(splitRegionID)
	re.NotNil(op)
	re.Equal(scatter.InternalScatterOperatorDesc, op.Desc())
	opGroup, ok := op.GetAdditionalInfo("group")
	re.True(ok)
	re.Equal("split-scatter-100-1", opGroup)
}

func newSplitScatterTestCluster(t *testing.T) *RaftCluster {
	t.Helper()
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	hbStreams := hbstream.NewTestHeartbeatStreams(ctx, cluster.BasicCluster, false)
	cluster.initCoordinator(ctx, cluster, hbStreams)
	t.Cleanup(func() {
		hbStreams.Close()
	})

	for _, store := range newTestStores(4, "6.0.0") {
		re.NoError(cluster.setStore(store))
	}

	re.NoError(cluster.putRegion(newSplitScatterRegion(100, []byte(""), []byte("m"), 0)))
	return cluster
}

func newSplitScatterRegion(regionID uint64, start, end []byte, cpu uint64) *core.RegionInfo {
	return newSplitScatterRegionWithStores(regionID, start, end, cpu, 1, 2, 3)
}

func newSplitScatterRegionWithStores(regionID uint64, start, end []byte, cpu uint64, stores ...uint64) *core.RegionInfo {
	peers := []*metapb.Peer{
	}
	for i, storeID := range stores {
		peers = append(peers, &metapb.Peer{
			Id:      regionID*10 + uint64(i) + 1,
			StoreId: storeID,
		})
	}
	region := &metapb.Region{
		Id:       regionID,
		StartKey: start,
		EndKey:   end,
		Peers:    peers,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
	}
	return core.NewRegionInfo(
		region,
		peers[0],
		core.SetCPUUsage(cpu),
	)
}

func newSplitScatterIndexKey(suffix string) []byte {
	key := []byte{'t'}
	key = codec.EncodeInt(key, 42)
	key = append(key, '_', 'i')
	key = codec.EncodeInt(key, 7)
	key = append(key, suffix...)
	return codec.EncodeBytes(key)
}
