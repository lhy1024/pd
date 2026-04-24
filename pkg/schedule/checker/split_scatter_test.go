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

const (
	splitScatterObservedRegionID uint64 = 101
	splitScatterTestTableID      int64  = 42
	splitScatterTestIndexID      int64  = 7
)

func TestRecordSplitScatterBatchCollectsByLegacyCPUScore(t *testing.T) {
	re := require.New(t)
	controller, tc, _, cleanup := newTestSplitScatterController(t)
	defer cleanup()

	controller.RecordSplitScatterBatch(100, []uint64{101, 102, 103})
	re.Equal(4, splitScatterPendingCount(controller))

	sourceGroup := splitScatterPendingGroup(t, controller, 100)
	for _, regionID := range []uint64{101, 102} {
		re.Equal(sourceGroup, splitScatterPendingGroup(t, controller, regionID))
	}

	putSplitScatterRegion(tc, 101, "m", "n", 0)
	putSplitScatterRegionWithLegacyOnlyCPU(tc, 102, "n", "o", 120)
	putSplitScatterRegion(tc, 103, "o", "", 999)

	re.Equal([]uint64{103, 102, 100}, pendingRegionIDs(controller.collectTopPendingSplitScatter(3)))
}

func TestCheckSplitScatterRegionsCreatesScatterOperator(t *testing.T) {
	re := require.New(t)
	controller, tc, oc, cleanup := newTestSplitScatterController(t)
	defer cleanup()

	controller.RecordSplitScatterBatch(100, []uint64{101, 102})
	putSplitScatterRegion(tc, 101, "m", "t", 120)
	putSplitScatterRegion(tc, 102, "t", "", 80)

	group := splitScatterPendingGroup(t, controller, 101)

	controller.DispatchSplitScatterRegions()

	var op *operator.Operator
	for _, regionID := range []uint64{100, 101, 102} {
		op = oc.GetOperator(regionID)
		if op != nil {
			break
		}
	}
	re.NotNil(op)
	re.Equal(scatter.InternalScatterOperatorDesc, op.Desc())
	opGroup, ok := op.GetAdditionalInfo("group")
	re.True(ok)
	re.Equal(group, opGroup)
	re.Equal(0, splitScatterPendingCount(controller))
}

func TestCollectTopPendingResolvesRangeHint(t *testing.T) {
	testCases := []struct {
		name      string
		startKey  []byte
		endKey    []byte
		wantRange splitScatterRangeHint
	}{
		{
			name:      "index region",
			startKey:  newSplitScatterIndexKey("a"),
			endKey:    newSplitScatterIndexKey("m"),
			wantRange: splitScatterPrefixRange(splitScatterIndexKeyPrefix()),
		},
		{
			name:      "record region",
			startKey:  newSplitScatterRecordKey(42, "a"),
			endKey:    newSplitScatterRecordKey(42, "m"),
			wantRange: splitScatterPrefixRange(codec.GenerateTableKey(42)),
		},
		{
			name:      "bare table boundary",
			startKey:  newSplitScatterTableBoundaryKey(42),
			endKey:    newSplitScatterIndexKey("m"),
			wantRange: splitScatterPrefixRange(codec.GenerateTableKey(42)),
		},
		{
			name:      "cross entity falls back to table",
			startKey:  newSplitScatterIndexKey("a"),
			endKey:    newSplitScatterRecordKey(42, "m"),
			wantRange: splitScatterPrefixRange(codec.GenerateTableKey(42)),
		},
		{
			name:      "cross table uses start table",
			startKey:  newSplitScatterRecordKey(42, "a"),
			endKey:    newSplitScatterRecordKey(43, "m"),
			wantRange: splitScatterPrefixRange(codec.GenerateTableKey(42)),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			re := require.New(t)
			controller, tc, _, cleanup := newTestSplitScatterController(t)
			defer cleanup()

			controller.RecordSplitScatterBatch(100, []uint64{101})
			putSplitScatterRegionWithKeys(tc, testCase.startKey, testCase.endKey, 120)

			re.Equal(makeSplitScatterGroup(100, 101), splitScatterPendingGroup(t, controller, 101))
			re.Equal([]uint64{101}, pendingRegionIDs(controller.collectTopPendingSplitScatter(1)))
			rangeHint := resolveSplitScatterRangeHint(tc.GetRegion(101))
			re.Equal(testCase.wantRange.startKey, rangeHint.startKey)
			re.Equal(testCase.wantRange.endKey, rangeHint.endKey)
		})
	}
}

func TestCollectTopPendingUsesLegacyCPUUsage(t *testing.T) {
	re := require.New(t)
	controller, tc, _, cleanup := newTestSplitScatterController(t)
	defer cleanup()

	controller.RecordSplitScatterBatch(100, []uint64{101, 102})
	putSplitScatterRegionWithLegacyOnlyCPU(tc, 101, "m", "t", 120)
	putSplitScatterRegionWithLegacyOnlyCPU(tc, 102, "t", "", 999)

	re.Equal([]uint64{102, 101}, pendingRegionIDs(controller.collectTopPendingSplitScatter(2)))
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

func putSplitScatterRegionWithLegacyOnlyCPU(tc *mockcluster.Cluster, regionID uint64, startKey, endKey string, legacyCPU uint64) {
	tc.AddLeaderRegionWithRange(regionID, startKey, endKey, 1, 2, 3)
	region := tc.GetRegion(regionID).Clone(core.SetCPUUsage(legacyCPU))
	tc.PutRegion(region)
}

func putSplitScatterRegionWithKeys(tc *mockcluster.Cluster, startKey, endKey []byte, cpu uint64) {
	region := tc.AddLeaderRegion(splitScatterObservedRegionID, 1, 2, 3).Clone(
		core.WithStartKey(startKey),
		core.WithEndKey(endKey),
		core.SetCPUUsage(cpu),
	)
	tc.PutRegion(region)
}

func splitScatterPendingCount(controller *Controller) int {
	controller.splitScatterPendingMu.RLock()
	defer controller.splitScatterPendingMu.RUnlock()
	return len(controller.splitScatterPending.GetAllID())
}

func splitScatterPendingGroup(t *testing.T, controller *Controller, regionID uint64) string {
	t.Helper()
	controller.splitScatterPendingMu.RLock()
	defer controller.splitScatterPendingMu.RUnlock()
	value, ok := controller.splitScatterPending.Get(regionID)
	require.True(t, ok)
	group, ok := value.(string)
	require.True(t, ok)
	return group
}

func pendingRegionIDs(regions []splitScatterPendingItem) []uint64 {
	ids := make([]uint64, 0, len(regions))
	for _, region := range regions {
		ids = append(ids, region.regionID)
	}
	return ids
}

func splitScatterIndexKeyPrefix() []byte {
	key := []byte{'t'}
	key = codec.EncodeInt(key, splitScatterTestTableID)
	key = append(key, '_', 'i')
	key = codec.EncodeInt(key, splitScatterTestIndexID)
	return key
}

func newSplitScatterIndexKey(suffix string) []byte {
	key := append([]byte(nil), splitScatterIndexKeyPrefix()...)
	key = append(key, suffix...)
	return codec.EncodeBytes(key)
}

func splitScatterRecordKeyPrefix(tableID int64) []byte {
	key := []byte{'t'}
	key = codec.EncodeInt(key, tableID)
	key = append(key, '_', 'r')
	return key
}

func newSplitScatterRecordKey(tableID int64, suffix string) []byte {
	key := append([]byte(nil), splitScatterRecordKeyPrefix(tableID)...)
	key = append(key, suffix...)
	return codec.EncodeBytes(key)
}

func newSplitScatterTableBoundaryKey(tableID int64) []byte {
	return codec.EncodeBytes(codec.GenerateTableKey(tableID))
}
