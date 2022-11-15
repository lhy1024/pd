// Copyright 2019 TiKV Project Authors.
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

package statistics

import (
	"fmt"
	"math"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/server/core"
	"go.uber.org/zap"
)

const (
	// TopNN is the threshold which means we can get hot threshold from store.
	TopNN = 60
	// HotThresholdRatio is used to calculate hot thresholds
	HotThresholdRatio = 0.8
	// WriteReportInterval indicates the interval between write interval
	WriteReportInterval = RegionHeartBeatReportInterval
	// ReadReportInterval indicates the interval between read stats report
	ReadReportInterval = StoreHeartBeatReportInterval

	rollingWindowsSize = 5

	// HotRegionReportMinInterval is used for the simulator and test
	HotRegionReportMinInterval = 3

	hotRegionAntiCount = 2

	queueCap = 20000
)

// Denoising is an option to calculate flow base on the real heartbeats. Should
// only turn off by the simulator and the test.
var Denoising = true

// MinHotThresholds is the threshold at which this dimension is recorded as a hot spot.
var MinHotThresholds = [RegionStatCount]float64{
	RegionReadBytes:     8 * units.KiB,
	RegionReadKeys:      128,
	RegionReadQueryNum:  128,
	RegionWriteBytes:    1 * units.KiB,
	RegionWriteKeys:     32,
	RegionWriteQueryNum: 32,
}

// hotPeerCache saves the hot peer's statistics.
type hotPeerCache struct {
	kind               RWType
	peersOfStore       map[uint64]*TopN               // storeID -> hot peers
	storesOfRegion     map[uint64]map[uint64]struct{} // regionID -> storeIDs
	regionsOfStore     map[uint64]map[uint64]struct{} // storeID -> regionIDs
	topNTTL            time.Duration
	reportIntervalSecs int
	taskQueue          chan FlowItemTask
	isUpdated          bool
}

// NewHotPeerCache creates a hotPeerCache
func NewHotPeerCache(kind RWType) *hotPeerCache {
	c := &hotPeerCache{
		kind:           kind,
		peersOfStore:   make(map[uint64]*TopN),
		storesOfRegion: make(map[uint64]map[uint64]struct{}),
		regionsOfStore: make(map[uint64]map[uint64]struct{}),
		taskQueue:      make(chan FlowItemTask, queueCap),
		isUpdated:      true,
	}
	if kind == Write {
		c.reportIntervalSecs = WriteReportInterval
	} else {
		c.reportIntervalSecs = ReadReportInterval
	}
	c.topNTTL = time.Duration(3*c.reportIntervalSecs) * time.Second
	return c
}

// TODO: rename RegionStats as PeerStats
// RegionStats returns hot items
func (f *hotPeerCache) RegionStats(minHotDegree int) (map[uint64][]*HotPeerStat, bool) {
	res := make(map[uint64][]*HotPeerStat)
	for storeID, peers := range f.peersOfStore {
		values := peers.GetAll()
		stat := make([]*HotPeerStat, 0, len(values))
		for _, v := range values {
			if peer := v.(*HotPeerStat); peer.HotDegree >= minHotDegree && !peer.inCold && peer.AntiCount == peer.defaultAntiCount() {
				stat = append(stat, peer)
			}
		}
		res[storeID] = stat
	}
	defer func() {
		f.isUpdated = false
	}()
	//  todo 拆分 ram-2 和 ram-3，2 只负责 sync.pool（包括 dimstat 和 监控无 copy），3 只负责 isupdated 缓存
	return res, f.isUpdated
}

func (f *hotPeerCache) updateStat(item *HotPeerStat) {
	switch item.actionType {
	case Remove:
		f.removeItem(item)
		f.isUpdated = true
		item.Log("region heartbeat remove from cache", log.Debug)
		incMetrics("remove_item", item.StoreID, item.Kind)
		return
	case Add:
		incMetrics("add_item", item.StoreID, item.Kind)
	case Update:
		incMetrics("update_item", item.StoreID, item.Kind)
	}
	// for add and update
	f.putItem(item)
	f.isUpdated = true
	item.Log("region heartbeat update", log.Debug)
}

func (f *hotPeerCache) collectPeerMetrics(loads []float64, interval uint64) {
	regionHeartbeatIntervalHist.Observe(float64(interval))
	if interval == 0 {
		return
	}
	// TODO: use unified metrics. (keep backward compatibility at the same time)
	for _, k := range f.kind.RegionStats() {
		switch k {
		case RegionReadBytes:
			readByteHist.Observe(loads[int(k)])
		case RegionReadKeys:
			readKeyHist.Observe(loads[int(k)])
		case RegionWriteBytes:
			writeByteHist.Observe(loads[int(k)])
		case RegionWriteKeys:
			writeKeyHist.Observe(loads[int(k)])
		case RegionWriteQueryNum:
			writeQueryHist.Observe(loads[int(k)])
		case RegionReadQueryNum:
			readQueryHist.Observe(loads[int(k)])
		}
	}
}

// collectExpiredItems collects expired items, mark them as needDelete and puts them into inherit items
func (f *hotPeerCache) collectExpiredItems(region *core.RegionInfo) []*HotPeerStat {
	regionID := region.GetID()
	items := make([]*HotPeerStat, 0)
	for _, storeID := range f.getAllStoreIDs(region) {
		if region.GetStorePeer(storeID) == nil {
			item := f.getOldHotPeerStat(regionID, storeID)
			if item != nil {
				item.actionType = Remove
				items = append(items, item)
			}
		}
	}
	return items
}

// checkPeerFlow checks the flow information of a peer.
// Notice: checkPeerFlow couldn't be used concurrently.
// checkPeerFlow will update oldItem's rollingLoads into newItem, thus we should use write lock here.
func (f *hotPeerCache) checkPeerFlow(peer *core.PeerInfo, region *core.RegionInfo) *HotPeerStat {
	interval := peer.GetInterval()
	// for test or simulator purpose
	if Denoising && interval < HotRegionReportMinInterval {
		return nil
	}
	storeID := peer.GetStoreID()
	deltaLoads := peer.GetLoads()
	f.collectPeerMetrics(deltaLoads, interval) // update metrics
	regionID := region.GetID()
	oldItem := f.getOldHotPeerStat(regionID, storeID)

	newItem := hotPeerStatPool.Get().(*HotPeerStat)
	hotPool.WithLabelValues("peer_statistic", "get", "check_peer").Inc()
	if !newItem.fromPool {
		newItem.alloc()
	}
	newItem.LogDebug("get")
	newItem.StoreID = storeID
	newItem.RegionID = regionID
	newItem.HotDegree = 0
	newItem.AntiCount = 0
	newItem.Kind = f.kind
	newItem.rollingLoads = nil
	newItem.LastUpdateTime = time.Now()
	newItem.actionType = Update
	newItem.isLeader = region.GetLeader().GetStoreId() == storeID
	newItem.isLearner = core.IsLearner(region.GetPeer(storeID))
	newItem.interval = interval
	newItem.peers = region.GetPeers()
	newItem.inCold = false
	newItem.allowInherited = false
	newItem.source = direct
	newItem.updateLoads(f.kind, deltaLoads)
	if len(newItem.thresholds) != DimLen {
		newItem.thresholds = make([]float64, DimLen)
	}
	f.calcHotThresholds(storeID, newItem.thresholds)

	if oldItem == nil {
		for _, storeID := range f.getAllStoreIDs(region) {
			oldItem = f.getOldHotPeerStat(regionID, storeID)
			if oldItem != nil && oldItem.allowInherited {
				newItem.source = inherit
				break
			}
		}
	}
	newItem.LogDebug("update")
	return f.updateHotPeerStat(region, newItem, oldItem, deltaLoads, time.Duration(interval)*time.Second)
}

// checkColdPeer checks the collect the un-heartbeat peer and maintain it.
func (f *hotPeerCache) checkColdPeer(storeID uint64, reportRegions map[uint64]*core.RegionInfo, interval uint64) (ret []*HotPeerStat) {
	// for test or simulator purpose
	if Denoising && interval < HotRegionReportMinInterval {
		return
	}
	previousHotStat, ok := f.regionsOfStore[storeID]
	// There is no need to continue since the store doesn't have any hot regions.
	if !ok {
		return
	}
	// Check if the original hot regions are still reported by the store heartbeat.
	for regionID := range previousHotStat {
		// If it's not reported, we need to update the original information.
		if region, ok := reportRegions[regionID]; !ok {
			oldItem := f.getOldHotPeerStat(regionID, storeID)
			// The region is not hot in the store, do nothing.
			if oldItem == nil {
				continue
			}
			// update the original hot peer, and mark it as cold.
			newItem := hotPeerStatPool.Get().(*HotPeerStat)
			hotPool.WithLabelValues("peer_statistic", "get", "check_cold_peer").Inc()
			newItem.StoreID = storeID
			newItem.RegionID = regionID
			newItem.Kind = f.kind
			newItem.LastUpdateTime = time.Now()
			newItem.isLeader = oldItem.isLeader
			newItem.isLearner = oldItem.isLearner
			newItem.interval = interval
			newItem.peers = oldItem.peers
			newItem.actionType = Update
			newItem.thresholds = oldItem.thresholds
			newItem.inCold = true
			newItem.rollingLoads = nil
			newItem.updateLoads(f.kind, nil)
			// deltaLoads := make([]float64, RegionStatCount)
			// for i, loads := range oldItem.thresholds { // todo thresholds?
			// 	deltaLoads[i] = loads * float64(interval)
			// }
			stat := f.updateHotPeerStat(region, newItem, oldItem, newItem.Loads, time.Duration(interval)*time.Second)
			if stat != nil {
				ret = append(ret, stat)
			}
		}
	}
	return
}

func (f *hotPeerCache) collectMetrics(typ string) {
	for storeID, peers := range f.peersOfStore {
		store := storeTag(storeID)
		thresholds := make([]float64, DimLen)
		f.calcHotThresholds(storeID, thresholds)
		hotCacheStatusGauge.WithLabelValues("total_length", store, typ).Set(float64(peers.Len()))
		hotCacheStatusGauge.WithLabelValues("byte-rate-threshold", store, typ).Set(thresholds[ByteDim])
		hotCacheStatusGauge.WithLabelValues("key-rate-threshold", store, typ).Set(thresholds[KeyDim])
		hotCacheStatusGauge.WithLabelValues("query-rate-threshold", store, typ).Set(thresholds[QueryDim])
		// for compatibility
		hotCacheStatusGauge.WithLabelValues("hotThreshold", store, typ).Set(thresholds[ByteDim])
	}
}

func (f *hotPeerCache) getOldHotPeerStat(regionID, storeID uint64) *HotPeerStat {
	if hotPeers, ok := f.peersOfStore[storeID]; ok {
		if v := hotPeers.Get(regionID); v != nil {
			return v.(*HotPeerStat)
		}
	}
	return nil
}

func (f *hotPeerCache) calcHotThresholds(storeID uint64, ret []float64) {
	statKinds := f.kind.RegionStats()
	for dim, kind := range statKinds {
		ret[dim] = MinHotThresholds[kind]
	}
	tn, ok := f.peersOfStore[storeID]
	if !ok || tn.Len() < TopNN {
		return
	}
	for i := range ret {
		ret[i] = math.Max(tn.GetTopNMin(i).(*HotPeerStat).GetLoad(i)*HotThresholdRatio, ret[i])
	}
}

// gets the storeIDs, including old region and new region
func (f *hotPeerCache) getAllStoreIDs(region *core.RegionInfo) []uint64 {
	storeIDs := make(map[uint64]struct{})
	regionPeers := region.GetPeers()
	ret := make([]uint64, 0, len(regionPeers))
	// old stores
	ids, ok := f.storesOfRegion[region.GetID()]
	if ok {
		for storeID := range ids {
			storeIDs[storeID] = struct{}{}
			ret = append(ret, storeID)
		}
	}

	// new stores
	for _, peer := range region.GetPeers() {
		if _, ok := storeIDs[peer.GetStoreId()]; !ok {
			storeIDs[peer.GetStoreId()] = struct{}{}
			ret = append(ret, peer.GetStoreId())
		}
	}

	return ret
}

func (f *hotPeerCache) isOldColdPeer(oldItem *HotPeerStat, storeID uint64) bool {
	isOldPeer := func() bool {
		for _, peer := range oldItem.peers {
			if peer.GetStoreId() == storeID {
				return true
			}
		}
		return false
	}
	noInCache := func() bool {
		ids, ok := f.storesOfRegion[oldItem.RegionID]
		if ok {
			for id := range ids {
				if id == storeID {
					return false
				}
			}
		}
		return true
	}
	return isOldPeer() && noInCache()
}

func (f *hotPeerCache) justTransferLeader(region *core.RegionInfo) bool {
	if region == nil {
		return false
	}
	ids, ok := f.storesOfRegion[region.GetID()]
	if ok {
		for storeID := range ids {
			oldItem := f.getOldHotPeerStat(region.GetID(), storeID)
			if oldItem == nil {
				continue
			}
			if oldItem.isLeader {
				return oldItem.StoreID != region.GetLeader().GetStoreId()
			}
		}
	}
	return false
}

func (f *hotPeerCache) isRegionHotWithAnyPeers(region *core.RegionInfo, hotDegree int) bool {
	for _, peer := range region.GetPeers() {
		if f.isRegionHotWithPeer(region, peer, hotDegree) {
			return true
		}
	}
	return false
}

func (f *hotPeerCache) isRegionHotWithPeer(region *core.RegionInfo, peer *metapb.Peer, hotDegree int) bool {
	if peer == nil {
		return false
	}
	if stat := f.getHotPeerStat(region.GetID(), peer.GetStoreId()); stat != nil {
		return stat.HotDegree >= hotDegree
	}
	return false
}

func (f *hotPeerCache) getHotPeerStat(regionID, storeID uint64) *HotPeerStat {
	if peers, ok := f.peersOfStore[storeID]; ok {
		if stat := peers.Get(regionID); stat != nil {
			return stat.(*HotPeerStat)
		}
	}
	return nil
}

func (f *hotPeerCache) updateHotPeerStat(region *core.RegionInfo, newItem, oldItem *HotPeerStat, deltaLoads []float64, interval time.Duration) *HotPeerStat {
	regionStats := f.kind.RegionStats()
	if oldItem == nil {
		return f.updateNewHotPeerStat(regionStats, newItem, deltaLoads, interval)
	}

	if newItem.source == inherit && oldItem.isHot() {
		for _, dim := range oldItem.rollingLoads {
			newItem.rollingLoads = append(newItem.rollingLoads, dim.Clone())
		}
		newItem.allowInherited = false
	} else {
		newItem.rollingLoads = oldItem.rollingLoads
		newItem.allowInherited = oldItem.allowInherited
	}
	if len(newItem.rollingLoads) < DimLen {
		oldItem.LogDebug("fatal")
		log.Fatal("rollingLoads len is less than DimLen",
			zap.String("oldItem address", fmt.Sprintf("%p", oldItem)),
			zap.Int("newItem", len(newItem.rollingLoads)),
			zap.Bool("source", newItem.source == inherit),
			zap.Bool("inCold", newItem.inCold),
		)
	}

	if f.justTransferLeader(region) {
		newItem.lastTransferLeaderTime = time.Now()
		// skip the first heartbeat flow statistic after transfer leader, because its statistics are calculated by the last leader in this store and are inaccurate
		// maintain anticount and hotdegree to avoid store threshold and hot peer are unstable.
		// For write stat, as the stat is send by region heartbeat, the first heartbeat will be skipped.
		// For read stat, as the stat is send by store heartbeat, the first heartbeat won't be skipped.
		if newItem.Kind == Write {
			inheritItem(newItem, oldItem)
			return newItem
		}
	} else {
		newItem.lastTransferLeaderTime = oldItem.lastTransferLeaderTime
	}

	for i, k := range regionStats {
		newItem.rollingLoads[i].Add(deltaLoads[k], interval)
	}

	isFull := newItem.rollingLoads[0].isFull() // The intervals of dims are the same, so it is only necessary to determine whether any of them
	if !isFull {
		// not update hot degree and anti count
		inheritItem(newItem, oldItem)
	} else {
		// If item is inCold, it means the pd didn't recv this item in the store heartbeat,
		// thus we make it colder
		if newItem.inCold {
			coldItem(newItem, oldItem)
		} else {
			if f.isOldColdPeer(oldItem, newItem.StoreID) {
				if newItem.isHot() {
					initItem(newItem)
				} else {
					newItem.actionType = Remove
				}
			} else {
				if newItem.isHot() {
					hotItem(newItem, oldItem)
				} else {
					coldItem(newItem, oldItem)
				}
			}
		}
		newItem.clearLastAverage()
	}
	return newItem
}

func (f *hotPeerCache) updateNewHotPeerStat(regionStats []RegionStatKind, newItem *HotPeerStat, deltaLoads []float64, interval time.Duration) *HotPeerStat {
	// interval is not 0 which is guaranteed by the caller.
	isHot := slice.AnyOf(regionStats, func(i int) bool {
		return deltaLoads[regionStats[i]]/interval.Seconds() >= newItem.thresholds[i]
	})
	if !isHot {
		hotPeerStatPool.Put(newItem)
		hotPool.WithLabelValues("peer_statistic", "put", "cold").Inc()
		return nil
	}
	if interval.Seconds() >= float64(f.reportIntervalSecs) {
		initItem(newItem)
	}
	newItem.actionType = Add
	newItem.rollingLoads = make([]*dimStat, DimLen)
	for i, k := range regionStats {
		ds := newDimStat(k, time.Duration(newItem.hotStatReportInterval())*time.Second)
		ds.Add(deltaLoads[k], interval)
		if ds.isFull() {
			ds.clearLastAverage()
		}
		newItem.rollingLoads[i] = ds
	}
	return newItem
}

func (f *hotPeerCache) putItem(item *HotPeerStat) {
	peers, ok := f.peersOfStore[item.StoreID]
	if !ok {
		peers = NewTopN(DimLen, TopNN, f.topNTTL)
		f.peersOfStore[item.StoreID] = peers
	}
	oldItem := peers.Put(item)
	hotPeerStatPool.Put(oldItem)
	hotPool.WithLabelValues("peer_statistic", "put", "topn_put").Inc()
	stores, ok := f.storesOfRegion[item.RegionID]
	if !ok {
		stores = make(map[uint64]struct{})
		f.storesOfRegion[item.RegionID] = stores
	}
	stores[item.StoreID] = struct{}{}
	regions, ok := f.regionsOfStore[item.StoreID]
	if !ok {
		regions = make(map[uint64]struct{})
		f.regionsOfStore[item.StoreID] = regions
	}
	regions[item.RegionID] = struct{}{}
}

func (f *hotPeerCache) removeItem(item *HotPeerStat) {
	if peers, ok := f.peersOfStore[item.StoreID]; ok {
		item := peers.Remove(item.RegionID)
		hotPeerStatPool.Put(item)
		hotPool.WithLabelValues("peer_statistic", "put", "topn_remove").Inc()
	}
	if stores, ok := f.storesOfRegion[item.RegionID]; ok {
		delete(stores, item.StoreID)
	}
	if regions, ok := f.regionsOfStore[item.StoreID]; ok {
		delete(regions, item.RegionID)
	}
}

func coldItem(newItem, oldItem *HotPeerStat) {
	newItem.HotDegree = oldItem.HotDegree - 1
	newItem.AntiCount = oldItem.AntiCount - 1
	if newItem.AntiCount <= 0 {
		newItem.actionType = Remove
	} else {
		newItem.allowInherited = true
	}
}

func hotItem(newItem, oldItem *HotPeerStat) {
	newItem.HotDegree = oldItem.HotDegree + 1
	if oldItem.AntiCount < oldItem.defaultAntiCount() {
		newItem.AntiCount = oldItem.AntiCount + 1
	} else {
		newItem.AntiCount = oldItem.AntiCount
	}
	newItem.allowInherited = true
}

func initItem(item *HotPeerStat) {
	item.HotDegree = 1
	item.AntiCount = item.defaultAntiCount()
	item.allowInherited = true
}

func inheritItem(newItem, oldItem *HotPeerStat) {
	newItem.HotDegree = oldItem.HotDegree
	newItem.AntiCount = oldItem.AntiCount
}
