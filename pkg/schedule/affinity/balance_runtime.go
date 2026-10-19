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

package affinity

import (
	"bytes"
	"encoding/json"
	"hash/fnv"
	"maps"
	"slices"
	"strings"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/config"
)

// BalanceVersion identifies the local inputs of a table search. Affinity
// metadata changes advance the table generation. Placement changes use the
// local RuleManager revision; ordinary labels are checked at submission.
type BalanceVersion struct {
	Table, Stores, Rules uint64
}

// BalanceSnapshot owns a copy of the table metadata and primary term
// conditions. The checker owns the search; Manager never retains its mutable
// progress.
type BalanceSnapshot struct {
	TableID            string
	Version            BalanceVersion
	Groups             []*GroupState
	Ranges             map[string]GroupKeyRanges
	Stores             []uint64
	GlobalLeaderCounts map[uint64]int
	GlobalVoterCounts  map[uint64]int
	conditions         []clientv3.Cmp
}

func (m *Manager) balanceVersionLocked(state *metadataTableState) BalanceVersion {
	var rules uint64
	if m.balanceRuleRevision != nil {
		rules = m.balanceRuleRevision()
	}
	return BalanceVersion{Table: state.version, Stores: m.balanceStoreRevision, Rules: rules}
}

// GetBalanceVersion reads generations without taking the metadata mutex or
// scanning ranges. It is suitable for the Region check's enqueue fast path.
func (m *Manager) GetBalanceVersion(groupID string) (BalanceVersion, bool) {
	m.RLock()
	defer m.RUnlock()
	state := m.metadataTables[TableGroupPrefix(groupID)]
	if state == nil {
		return BalanceVersion{}, false
	}
	return m.balanceVersionLocked(state), true
}

// GetBalanceSnapshot copies only this table and the incremental global counts.
// It performs no placement checks and starts no background work.
func (m *Manager) GetBalanceSnapshot(groupID string) *BalanceSnapshot {
	stores, fingerprint := m.loadBalanceStoreSnapshot()
	m.metaMutex.Lock()
	if err := m.recoverWritesLocked(m.ctx); err != nil {
		m.metaMutex.Unlock()
		return nil
	}
	m.Lock()
	m.updateBalanceStoreFingerprintLocked(fingerprint)
	state := m.metadataTables[TableGroupPrefix(groupID)]
	if state == nil {
		m.Unlock()
		m.metaMutex.Unlock()
		return nil
	}
	result := &BalanceSnapshot{
		TableID: TableGroupPrefix(groupID), Version: m.balanceVersionLocked(state),
		Groups: make([]*GroupState, 0, len(state.groups)), Ranges: make(map[string]GroupKeyRanges, len(state.groups)),
		Stores:             balanceCandidateStores(stores),
		GlobalLeaderCounts: maps.Clone(m.balanceLeaderCounts), GlobalVoterCounts: maps.Clone(m.balanceVoterCounts),
	}
	for id := range state.groups {
		if group := m.groups[id]; group != nil {
			result.Groups = append(result.Groups, newGroupState(group))
			result.Ranges[id] = cloneBalanceRanges(m.keyRanges[id])
		}
	}
	m.Unlock()
	m.metaMutex.Unlock()
	slices.SortFunc(result.Groups, func(a, b *GroupState) int { return strings.Compare(a.ID, b.ID) })
	return result
}

// ObserveBalanceRegion invalidates searches on topology, health and scheduling
// changes. Approximate size is read directly while validating a candidate.
func (m *Manager) ObserveBalanceRegion(region *core.RegionInfo) {
	m.RLock()
	_, observed := m.regions[region.GetID()]
	hasGroups := len(m.groups) > 0
	m.RUnlock()
	if !hasGroups {
		return
	}
	var groupID string
	if !observed {
		groupID = m.regionLabeler.GetRegionLabel(region, labelKey)
		if groupID == "" {
			return
		}
	}
	scheduleDenied := m.regionLabeler.ScheduleDisabled(region)
	m.Lock()
	defer m.Unlock()
	if cached, ok := m.regions[region.GetID()]; ok {
		if balanceRegionChanged(cached.balanceInput(), region) || cached.balanceScheduleDenied != scheduleDenied {
			m.invalidateMetadataLocked(cached.groupInfo.ID)
			cached.balanceRegion, cached.balanceScheduleDenied = region, scheduleDenied
			m.regions[region.GetID()] = cached
			cached.groupInfo.Regions[region.GetID()] = cached
		}
	} else if _, ok := m.groups[groupID]; ok {
		m.invalidateMetadataLocked(groupID)
	}
}

func (cache regionCache) balanceInput() *core.RegionInfo {
	if cache.balanceRegion != nil {
		return cache.balanceRegion
	}
	return cache.region
}

func balanceRegionChanged(a, b *core.RegionInfo) bool {
	leaderStoreID := func(region *core.RegionInfo) uint64 {
		if region.GetLeader() == nil {
			return 0
		}
		return region.GetLeader().GetStoreId()
	}
	return a.GetRegionEpoch().GetVersion() != b.GetRegionEpoch().GetVersion() ||
		a.GetRegionEpoch().GetConfVer() != b.GetRegionEpoch().GetConfVer() ||
		leaderStoreID(a) != leaderStoreID(b) ||
		!bytes.Equal(a.GetStartKey(), b.GetStartKey()) || !bytes.Equal(a.GetEndKey(), b.GetEndKey()) ||
		!core.SortedPeersEqual(a.GetPeers(), b.GetPeers()) ||
		!core.SortedPeersEqual(a.GetPendingPeers(), b.GetPendingPeers()) ||
		!core.SortedPeersStatsEqual(a.GetDownPeers(), b.GetDownPeers())
}

func cloneBalanceRanges(ranges GroupKeyRanges) GroupKeyRanges {
	ranges.KeyRanges = slices.Clone(ranges.KeyRanges)
	for i := range ranges.KeyRanges {
		ranges.KeyRanges[i].StartKey = slices.Clone(ranges.KeyRanges[i].StartKey)
		ranges.KeyRanges[i].EndKey = slices.Clone(ranges.KeyRanges[i].EndKey)
	}
	return ranges
}

// refreshBalanceStores runs in the existing availability loop, rather than
// hashing all stores on every Region heartbeat.
func (m *Manager) refreshBalanceStores() {
	_, fingerprint := m.loadBalanceStoreSnapshot()
	m.Lock()
	defer m.Unlock()
	m.updateBalanceStoreFingerprintLocked(fingerprint)
}

func (m *Manager) loadBalanceStoreSnapshot() ([]*core.StoreInfo, uint64) {
	if m.storeSetInformer == nil {
		return nil, 0
	}
	stores := slices.DeleteFunc(slices.Clone(m.storeSetInformer.GetStores()), func(store *core.StoreInfo) bool { return store == nil })
	slices.SortFunc(stores, func(a, b *core.StoreInfo) int {
		if a.GetID() < b.GetID() {
			return -1
		}
		if a.GetID() > b.GetID() {
			return 1
		}
		return 0
	})
	h := fnv.New64a()
	for _, store := range stores {
		// Match StoreStateFilter's down threshold so recovery also invalidates
		// completed searches for tables that have no peers on this store.
		input := struct {
			ID        uint64
			State     int32
			NodeState int32
			Labels    []*metapb.StoreLabel
			Flags     [8]bool
		}{ID: store.GetID(), State: int32(store.GetMeta().GetState()), NodeState: int32(store.GetMeta().GetNodeState()), Labels: store.GetLabels(),
			Flags: [8]bool{store.DownTime() > m.conf.GetMaxStoreDownTime(), store.IsLowSpace(m.conf.GetLowSpaceRatio()),
				store.AllowLeaderTransferIn(), store.AllowLeaderTransferOut(), store.EvictedAsSlowStore(),
				store.EvictedAsStoppingStore(), store.IsEvictedAsSlowTrend(), m.conf.CheckLabelProperty(config.RejectLeader, store.GetLabels())}}
		data, _ := json.Marshal(input)
		_, _ = h.Write(data)
	}
	return stores, h.Sum64()
}

func (m *Manager) updateBalanceStoreFingerprintLocked(fingerprint uint64) {
	if fingerprint != m.balanceStoreFingerprint {
		m.balanceStoreFingerprint = fingerprint
		m.balanceStoreRevision++
	}
}

// Temporary restrictions stay in the candidate search so an unavailable store
// cannot turn an incomplete search into a stable result.
func balanceCandidateStores(stores []*core.StoreInfo) []uint64 {
	ids := make([]uint64, 0, len(stores))
	for _, store := range stores {
		if !store.IsUp() || !store.IsTiKV() {
			continue
		}
		ids = append(ids, store.GetID())
	}
	return ids
}

func (m *Manager) addBalanceCountsLocked(group *Group) {
	if group == nil {
		return
	}
	if group.LeaderStoreID != 0 {
		m.balanceLeaderCounts[group.LeaderStoreID]++
	}
	for _, storeID := range group.VoterStoreIDs {
		m.balanceVoterCounts[storeID]++
	}
}

func (m *Manager) removeBalanceCountsLocked(group *Group) {
	if group == nil {
		return
	}
	if group.LeaderStoreID != 0 {
		if count := m.balanceLeaderCounts[group.LeaderStoreID] - 1; count > 0 {
			m.balanceLeaderCounts[group.LeaderStoreID] = count
		} else {
			delete(m.balanceLeaderCounts, group.LeaderStoreID)
		}
	}
	for _, storeID := range group.VoterStoreIDs {
		if count := m.balanceVoterCounts[storeID] - 1; count > 0 {
			m.balanceVoterCounts[storeID] = count
		} else {
			delete(m.balanceVoterCounts, storeID)
		}
	}
}
