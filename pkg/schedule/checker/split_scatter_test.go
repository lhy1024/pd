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

package checker

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/scatter"
)

func TestRecordSplitScatterBatchAndObserveQueueByReadCPU(t *testing.T) {
	re := require.New(t)
	controller, tc, _, cleanup := newTestSplitScatterController(t)
	defer cleanup()

	controller.RecordSplitScatterBatch(100, []uint64{101, 102})
	re.Equal(3, controller.splitScatter.pendingCount())

	sourceGroup, ok := controller.splitScatter.getPendingGroup(100)
	re.True(ok)
	for _, regionID := range []uint64{101, 102} {
		group, ok := controller.splitScatter.getPendingGroup(regionID)
		re.True(ok)
		re.Equal(sourceGroup, group)
	}

	putSplitScatterRegion(tc, 101, "m", "t", 120)
	putSplitScatterRegion(tc, 102, "t", "", 80)

	controller.ObserveSplitScatterRegion(tc.GetRegion(101))
	controller.ObserveSplitScatterRegion(tc.GetRegion(102))

	candidates := controller.splitScatter.getCandidates(2)
	re.Len(candidates, 2)
	re.Equal(uint64(101), candidates[0].regionID)
	re.Equal(uint64(102), candidates[1].regionID)
}

func TestCheckSplitScatterRegionsCreatesScatterOperator(t *testing.T) {
	re := require.New(t)
	controller, tc, oc, cleanup := newTestSplitScatterController(t)
	defer cleanup()

	controller.RecordSplitScatterBatch(100, []uint64{101, 102})
	putSplitScatterRegion(tc, 101, "m", "t", 120)
	putSplitScatterRegion(tc, 102, "t", "", 80)

	controller.ObserveSplitScatterRegion(tc.GetRegion(101))
	controller.ObserveSplitScatterRegion(tc.GetRegion(102))

	group, ok := controller.splitScatter.getPendingGroup(101)
	re.True(ok)

	controller.checkSplitScatterRegions()

	op := oc.GetOperator(101)
	if op == nil {
		op = oc.GetOperator(102)
	}
	re.NotNil(op)
	re.Equal("scatter-region", op.Desc())
	opGroup, ok := op.GetAdditionalInfo("group")
	re.True(ok)
	re.Equal(group, opGroup)
	re.Equal(1, controller.splitScatter.pendingCount())
}

func TestObserveSplitScatterRegionUsesIndexGroupAndRangeHint(t *testing.T) {
	re := require.New(t)
	controller, tc, _, cleanup := newTestSplitScatterController(t)
	defer cleanup()

	controller.RecordSplitScatterBatch(100, []uint64{101})
	putSplitScatterRegionWithKeys(tc, 101, newSplitScatterIndexKey(42, 7, "a"), newSplitScatterIndexKey(42, 7, "m"), 120)

	controller.ObserveSplitScatterRegion(tc.GetRegion(101))

	group, ok := controller.splitScatter.getPendingGroup(101)
	re.True(ok)
	re.Equal("split-scatter-index-42-7", group)

	candidates := controller.splitScatter.getCandidates(1)
	re.Len(candidates, 1)
	expectedRange := splitScatterPrefixRange(splitScatterIndexKeyPrefix(42, 7))
	re.Equal(expectedRange.startKey, candidates[0].rangeHint.startKey)
	re.Equal(expectedRange.endKey, candidates[0].rangeHint.endKey)
}

func newTestSplitScatterController(t *testing.T) (*Controller, *mockcluster.Cluster, *operator.Controller, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	for storeID := uint64(1); storeID <= 4; storeID++ {
		tc.AddRegionStore(storeID, 0)
	}
	putSplitScatterRegion(tc, 100, "", "m", 0)

	stream := hbstream.NewTestHeartbeatStreams(ctx, tc, false)
	oc := operator.NewController(ctx, tc.GetBasicCluster(), tc.GetSharedConfig(), stream)
	controller := NewController(ctx, tc, tc.GetCheckerConfig(), oc)
	controller.SetSplitScatterer(scatter.NewRegionScatterer(ctx, tc, oc, controller.AddPendingProcessedRegions))

	cleanup := func() {
		stream.Close()
		cancel()
	}
	return controller, tc, oc, cleanup
}

func putSplitScatterRegion(tc *mockcluster.Cluster, regionID uint64, startKey, endKey string, cpu uint64) {
	tc.AddLeaderRegionWithRange(regionID, startKey, endKey, 1, 2, 3)
	region := tc.GetRegion(regionID).Clone(core.SetCPUUsage(cpu))
	tc.PutRegion(region)
}

func putSplitScatterRegionWithKeys(tc *mockcluster.Cluster, regionID uint64, startKey, endKey []byte, cpu uint64) {
	region := tc.AddLeaderRegion(regionID, 1, 2, 3).Clone(
		core.WithStartKey(startKey),
		core.WithEndKey(endKey),
		core.SetCPUUsage(cpu),
	)
	tc.PutRegion(region)
}

func splitScatterIndexKeyPrefix(tableID, indexID int64) []byte {
	key := []byte{'t'}
	key = codec.EncodeInt(key, tableID)
	key = append(key, '_', 'i')
	key = codec.EncodeInt(key, indexID)
	return key
}

func newSplitScatterIndexKey(tableID, indexID int64, suffix string) []byte {
	key := append([]byte(nil), splitScatterIndexKeyPrefix(tableID, indexID)...)
	key = append(key, suffix...)
	return codec.EncodeBytes(key)
}
