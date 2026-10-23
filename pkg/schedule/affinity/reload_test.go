// Copyright 2025 TiKV Project Authors.
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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

func TestReloadRejectsStaleRegionObservation(t *testing.T) {
	m := newMetadataTestManager(t)
	id := "_tidb_pt_10_p1"
	r1 := keyutil.KeyRange{StartKey: []byte{1}, EndKey: []byte{2}}
	r2 := keyutil.KeyRange{StartKey: []byte{3}, EndKey: []byte{4}}
	require.NoError(t, m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: id, KeyRanges: []keyutil.KeyRange{r1}}}))
	peers := []*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}, {Id: 3, StoreId: 3}}
	region := core.NewRegionInfo(&metapb.Region{Id: 10, StartKey: r1.StartKey, EndKey: r1.EndKey, Peers: peers}, peers[0])
	stale, _ := m.GetAndCacheRegionAffinityGroupState(region)
	require.NotNil(t, stale)
	require.Equal(t, 1, stale.RangeCount)
	require.NoError(t, m.UpdateAffinityGroupKeyRanges(nil, []GroupKeyRanges{{GroupID: id, KeyRanges: []keyutil.KeyRange{r1}}}))
	require.NoError(t, m.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{{GroupID: id, KeyRanges: []keyutil.KeyRange{r2}}}, nil))
	require.NoError(t, m.ReloadAffinityMetadataFromStorage())
	current := m.GetAffinityGroupState(id)
	require.Equal(t, 1, current.RangeCount)
	require.Equal(t, []byte{3}, m.keyRanges[id].KeyRanges[0].StartKey)
	require.Equal(t, stale.affinityVer, current.affinityVer, "reload reused the stale observation's version")
	belongs, _ := m.GetRegionAffinityGroupState(region)
	require.Nil(t, belongs, "the observed Region is outside the current range")
	m.ObserveAvailableRegion(region, stale)
	require.Zero(t, loadStoredGroup(t, m, id).LeaderStoreID, "an old Region observation must not initialize the new range's target")
	freshRegion := region.Clone(core.WithStartKey(r2.StartKey), core.WithEndKey(r2.EndKey))
	fresh, _ := m.GetAndCacheRegionAffinityGroupState(freshRegion)
	require.NotNil(t, fresh)
	m.ObserveAvailableRegion(freshRegion, fresh)
	require.Equal(t, uint64(1), loadStoredGroup(t, m, id).LeaderStoreID)
}

func TestReloadValidationFailurePreservesPublishedState(t *testing.T) {
	for _, failure := range []string{"group-json", "label-json", "label-data", "overlap"} {
		t.Run(failure, func(t *testing.T) {
			m := newMetadataTestManager(t)
			ids := seedMetadataTestGroups(t, m, []uint64{1, 2})
			before := m.GetAffinityGroupState(ids[0])
			rangesBefore := m.keyRanges
			labelsBefore := m.regionLabeler.GetAllLabelRules()
			require.NoError(t, m.storage.RunInTxn(t.Context(), func(txn kv.Txn) error {
				// Stage a valid change as well, so failure cannot partially
				// publish metadata loaded before the invalid object.
				updated := before.Group
				updated.LeaderStoreID = 3
				if err := m.storage.SaveAffinityGroup(txn, ids[0], &updated); err != nil {
					return err
				}
				switch failure {
				case "group-json":
					return txn.Save(keypath.AffinityGroupPath(ids[1]), "{")
				case "label-json":
					return txn.Save(keypath.RegionLabelKeyPath(GetLabelRuleID(ids[1])), "{")
				case "label-data":
					return txn.Save(keypath.RegionLabelKeyPath(GetLabelRuleID(ids[1])), `{"id":"affinity_group/bad","rule_type":"invalid"}`)
				default:
					rule := MakeLabelRule(GroupKeyRanges{GroupID: ids[1], KeyRanges: m.keyRanges[ids[0]].KeyRanges})
					return m.storage.(endpoint.RuleStorage).SaveRegionRule(txn, rule.ID, rule)
				}
			}))

			require.Error(t, m.ReloadAffinityMetadataFromStorage())
			require.Equal(t, before, m.GetAffinityGroupState(ids[0]))
			require.Equal(t, rangesBefore, m.keyRanges)
			require.ElementsMatch(t, labelsBefore, m.regionLabeler.GetAllLabelRules())
		})
	}
}

func TestReloadPreservesOrdinaryLabels(t *testing.T) {
	m := newMetadataTestManager(t)
	seedMetadataTestGroups(t, m, []uint64{1})
	rule := &labeler.LabelRule{ID: "ordinary", RuleType: labeler.KeyRange,
		Labels: []labeler.RegionLabel{{Key: "schedule", Value: "deny"}},
		Data:   []any{map[string]any{"start_key": "10", "end_key": "20"}}}
	require.NoError(t, m.regionLabeler.SetLabelRule(rule))
	require.NoError(t, m.ReloadAffinityMetadataFromStorage())
	require.Equal(t, rule, m.regionLabeler.GetLabelRule(rule.ID))
}
