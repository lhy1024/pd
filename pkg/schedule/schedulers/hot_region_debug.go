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
	"os"
	"sort"
	"strconv"
	"strings"

	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const (
	hotReadDebugStoreIDsEnv      = "PD_HOT_DEBUG_STORE_IDS"
	hotReadDebugTopNEnv          = "PD_HOT_DEBUG_TOPN"
	hotReadDebugDefaultTopN      = 10
	hotReadDebugDefaultCPUMin    = 100
	hotReadDebugDispatchTopN     = 3
	hotReadDebugDeltaChangeLimit = 5
)

type hotStoreDebugPeer struct {
	RegionID  uint64  `json:"region_id"`
	CPU       float64 `json:"cpu"`
	Query     float64 `json:"query"`
	Byte      float64 `json:"byte"`
	HotDegree int     `json:"hot_degree"`
	AntiCount int     `json:"anti_count"`
	IsLeader  bool    `json:"is_leader"`
}

type hotStoreDebugSnapshot struct {
	StoreID       uint64              `json:"store_id"`
	StoreCPU      float64             `json:"store_cpu"`
	StoreQuery    float64             `json:"store_query"`
	StoreByte     float64             `json:"store_byte"`
	TotalHotCPU   float64             `json:"total_hot_cpu"`
	TotalHotQuery float64             `json:"total_hot_query"`
	TotalHotByte  float64             `json:"total_hot_byte"`
	HotPeerCount  int                 `json:"hot_peer_count"`
	TopN          []hotStoreDebugPeer `json:"topn"`
}

type hotStoreDebugPeerChange struct {
	RegionID uint64  `json:"region_id"`
	OldCPU   float64 `json:"old_cpu"`
	NewCPU   float64 `json:"new_cpu"`
	OldRank  int     `json:"old_rank"`
	NewRank  int     `json:"new_rank"`
}

type hotStoreDebugDelta struct {
	EnterTopN   []hotStoreDebugPeer       `json:"enter_topn,omitempty"`
	ExitTopN    []hotStoreDebugPeer       `json:"exit_topn,omitempty"`
	CPUUpTop5   []hotStoreDebugPeerChange `json:"cpu_up_top5,omitempty"`
	CPUDownTop5 []hotStoreDebugPeerChange `json:"cpu_down_top5,omitempty"`
}

type hotReadDebugTracer struct {
	topN     int
	storeIDs map[uint64]struct{}

	mu        syncutil.Mutex
	snapshots map[uint64]hotStoreDebugSnapshot
}

func newHotReadDebugTracerFromEnv() *hotReadDebugTracer {
	storeIDs := parseHotReadDebugStoreIDs(os.Getenv(hotReadDebugStoreIDsEnv))
	topN := hotReadDebugDefaultTopN
	if raw := strings.TrimSpace(os.Getenv(hotReadDebugTopNEnv)); raw != "" {
		if value, err := strconv.Atoi(raw); err == nil && value > 0 {
			topN = value
		} else {
			log.Warn("invalid hot read debug topn, fallback to default",
				zap.String("env", hotReadDebugTopNEnv),
				zap.String("value", raw),
				zap.Int("default", hotReadDebugDefaultTopN))
		}
	}
	return &hotReadDebugTracer{
		topN:      topN,
		storeIDs:  storeIDs,
		snapshots: make(map[uint64]hotStoreDebugSnapshot, len(storeIDs)),
	}
}

func parseHotReadDebugStoreIDs(raw string) map[uint64]struct{} {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	storeIDs := make(map[uint64]struct{})
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		storeID, err := strconv.ParseUint(item, 10, 64)
		if err != nil {
			log.Warn("skip invalid hot read debug store id",
				zap.String("env", hotReadDebugStoreIDsEnv),
				zap.String("value", item),
				zap.Error(err))
			continue
		}
		storeIDs[storeID] = struct{}{}
	}
	return storeIDs
}

func (t *hotReadDebugTracer) shouldLogStore(storeID uint64) bool {
	if t == nil {
		return false
	}
	if len(t.storeIDs) == 0 {
		return true
	}
	_, ok := t.storeIDs[storeID]
	return ok
}

func (t *hotReadDebugTracer) topNLimit() int {
	if t == nil || t.topN <= 0 {
		return hotReadDebugDefaultTopN
	}
	return t.topN
}

func (t *hotReadDebugTracer) logReadLeaderStoreSnapshot(detail *statistics.StoreLoadDetail) {
	if t == nil || detail == nil || detail.LoadPred == nil || !t.shouldLogStore(detail.GetID()) {
		return
	}
	snapshot := buildHotStoreDebugSnapshot(detail, t.topNLimit())
	t.mu.Lock()
	prev, hasPrev := t.snapshots[detail.GetID()]
	t.snapshots[detail.GetID()] = snapshot
	t.mu.Unlock()

	fields := []zap.Field{
		zap.Uint64("store-id", snapshot.StoreID),
		zap.String("resource-type", readLeader.String()),
		zap.Int("topn-limit", t.topNLimit()),
		zap.Float64("store-cpu", snapshot.StoreCPU),
		zap.Float64("store-query", snapshot.StoreQuery),
		zap.Float64("store-byte", snapshot.StoreByte),
		zap.Float64("total-hot-cpu", snapshot.TotalHotCPU),
		zap.Float64("total-hot-query", snapshot.TotalHotQuery),
		zap.Float64("total-hot-byte", snapshot.TotalHotByte),
		zap.Int("hot-peer-count", snapshot.HotPeerCount),
		zap.Any("topn", snapshot.TopN),
	}
	log.Info("read hot store topn snapshot", fields...)
	if !hasPrev {
		return
	}
	log.Info("read hot store topn delta",
		append(fields[:0:0],
			zap.Uint64("store-id", snapshot.StoreID),
			zap.String("resource-type", readLeader.String()),
			zap.Int("topn-limit", t.topNLimit()),
			zap.Any("delta", diffHotStoreDebugSnapshot(prev, snapshot)))...)
}

func buildHotStoreDebugSnapshot(detail *statistics.StoreLoadDetail, limit int) hotStoreDebugSnapshot {
	topN := buildHotStoreDebugPeers(detail.HotPeers, limit)
	summary := detail.ToHotPeersStat()
	snapshot := hotStoreDebugSnapshot{
		StoreID: detail.GetID(),
		TopN:    topN,
	}
	if detail.LoadPred != nil {
		snapshot.StoreByte = detail.LoadPred.Current.Loads[utils.ByteDim]
		snapshot.StoreQuery = detail.LoadPred.Current.Loads[utils.QueryDim]
		snapshot.StoreCPU = detail.LoadPred.Current.Loads[utils.CPUDim]
	}
	if summary != nil {
		snapshot.HotPeerCount = summary.Count
		snapshot.TotalHotByte = summary.TotalBytesRate
		snapshot.TotalHotQuery = summary.TotalQueryRate
		snapshot.TotalHotCPU = summary.TotalCPURate
	}
	return snapshot
}

func buildHotStoreDebugPeers(peers []*statistics.HotPeerStat, limit int) []hotStoreDebugPeer {
	if limit <= 0 {
		limit = hotReadDebugDefaultTopN
	}
	sorted := sortHotPeersByCPU(peers)
	ret := make([]hotStoreDebugPeer, 0, min(limit, len(sorted)))
	for idx, peer := range sorted {
		if idx >= limit && peer.GetLoad(utils.CPUDim) < hotReadDebugDefaultCPUMin {
			break
		}
		ret = append(ret, newHotStoreDebugPeer(peer))
	}
	return ret
}

func sortHotPeersByCPU(peers []*statistics.HotPeerStat) []*statistics.HotPeerStat {
	ret := make([]*statistics.HotPeerStat, 0, len(peers))
	for _, peer := range peers {
		if peer == nil || peer.HotDegree <= 0 {
			continue
		}
		ret = append(ret, peer)
	}
	sort.Slice(ret, func(i, j int) bool {
		left, right := ret[i], ret[j]
		if left.GetLoad(utils.CPUDim) != right.GetLoad(utils.CPUDim) {
			return left.GetLoad(utils.CPUDim) > right.GetLoad(utils.CPUDim)
		}
		if left.GetLoad(utils.QueryDim) != right.GetLoad(utils.QueryDim) {
			return left.GetLoad(utils.QueryDim) > right.GetLoad(utils.QueryDim)
		}
		if left.GetLoad(utils.ByteDim) != right.GetLoad(utils.ByteDim) {
			return left.GetLoad(utils.ByteDim) > right.GetLoad(utils.ByteDim)
		}
		return left.RegionID < right.RegionID
	})
	return ret
}

func newHotStoreDebugPeer(peer *statistics.HotPeerStat) hotStoreDebugPeer {
	return hotStoreDebugPeer{
		RegionID:  peer.RegionID,
		CPU:       peer.GetLoad(utils.CPUDim),
		Query:     peer.GetLoad(utils.QueryDim),
		Byte:      peer.GetLoad(utils.ByteDim),
		HotDegree: peer.HotDegree,
		AntiCount: peer.AntiCount,
		IsLeader:  peer.IsLeader(),
	}
}

func findHotPeerRankByCPU(peers []*statistics.HotPeerStat, regionID uint64, limit int) (int, float64) {
	if limit <= 0 {
		limit = hotReadDebugDefaultTopN
	}
	for idx, peer := range sortHotPeersByCPU(peers) {
		if idx >= limit {
			return 0, 0
		}
		if peer.RegionID == regionID {
			return idx + 1, peer.GetLoad(utils.CPUDim)
		}
	}
	return 0, 0
}

func diffHotStoreDebugSnapshot(prev, curr hotStoreDebugSnapshot) hotStoreDebugDelta {
	prevRanks := make(map[uint64]int, len(prev.TopN))
	prevPeers := make(map[uint64]hotStoreDebugPeer, len(prev.TopN))
	for idx, peer := range prev.TopN {
		prevRanks[peer.RegionID] = idx + 1
		prevPeers[peer.RegionID] = peer
	}
	currRanks := make(map[uint64]int, len(curr.TopN))
	currPeers := make(map[uint64]hotStoreDebugPeer, len(curr.TopN))
	for idx, peer := range curr.TopN {
		currRanks[peer.RegionID] = idx + 1
		currPeers[peer.RegionID] = peer
	}

	var delta hotStoreDebugDelta
	for _, peer := range curr.TopN {
		if _, ok := prevPeers[peer.RegionID]; !ok {
			delta.EnterTopN = append(delta.EnterTopN, peer)
		}
	}
	for _, peer := range prev.TopN {
		if _, ok := currPeers[peer.RegionID]; !ok {
			delta.ExitTopN = append(delta.ExitTopN, peer)
		}
	}

	cpuUp := make([]hotStoreDebugPeerChange, 0, len(curr.TopN))
	cpuDown := make([]hotStoreDebugPeerChange, 0, len(curr.TopN))
	for regionID, currPeer := range currPeers {
		prevPeer, ok := prevPeers[regionID]
		if !ok {
			continue
		}
		change := hotStoreDebugPeerChange{
			RegionID: regionID,
			OldCPU:   prevPeer.CPU,
			NewCPU:   currPeer.CPU,
			OldRank:  prevRanks[regionID],
			NewRank:  currRanks[regionID],
		}
		switch {
		case currPeer.CPU > prevPeer.CPU:
			cpuUp = append(cpuUp, change)
		case currPeer.CPU < prevPeer.CPU:
			cpuDown = append(cpuDown, change)
		}
	}
	sort.Slice(cpuUp, func(i, j int) bool {
		left := cpuUp[i].NewCPU - cpuUp[i].OldCPU
		right := cpuUp[j].NewCPU - cpuUp[j].OldCPU
		if left != right {
			return left > right
		}
		return cpuUp[i].RegionID < cpuUp[j].RegionID
	})
	sort.Slice(cpuDown, func(i, j int) bool {
		left := cpuDown[i].OldCPU - cpuDown[i].NewCPU
		right := cpuDown[j].OldCPU - cpuDown[j].NewCPU
		if left != right {
			return left > right
		}
		return cpuDown[i].RegionID < cpuDown[j].RegionID
	})
	if len(cpuUp) > hotReadDebugDeltaChangeLimit {
		cpuUp = cpuUp[:hotReadDebugDeltaChangeLimit]
	}
	if len(cpuDown) > hotReadDebugDeltaChangeLimit {
		cpuDown = cpuDown[:hotReadDebugDeltaChangeLimit]
	}
	delta.CPUUpTop5 = cpuUp
	delta.CPUDownTop5 = cpuDown
	return delta
}
