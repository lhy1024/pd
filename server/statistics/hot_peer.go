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
	"math"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/movingaverage"
	"github.com/tikv/pd/pkg/slice"
	"go.uber.org/zap"
)

type dimStat struct {
	typ         RegionStatKind
	rolling     *movingaverage.TimeMedian  // it's used to statistic hot degree and average speed.
	lastAverage *movingaverage.AvgOverTime // it's used to obtain the average speed in last second as instantaneous speed.
}

func newDimStat(typ RegionStatKind, reportInterval time.Duration) *dimStat {
	return &dimStat{
		typ:         typ,
		rolling:     movingaverage.NewTimeMedian(DefaultAotSize, rollingWindowsSize, reportInterval),
		lastAverage: movingaverage.NewAvgOverTime(reportInterval),
	}
}

func (d *dimStat) Add(delta float64, interval time.Duration) {
	d.lastAverage.Add(delta, interval)
	d.rolling.Add(delta, interval)
}

func (d *dimStat) isLastAverageHot(threshold float64) bool {
	return d.lastAverage.Get() >= threshold
}

func (d *dimStat) isHot(threshold float64) bool {
	return d.rolling.Get() >= threshold
}

func (d *dimStat) isFull() bool {
	return d.lastAverage.IsFull()
}

func (d *dimStat) clearLastAverage() {
	d.lastAverage.Clear()
}

func (d *dimStat) Get() float64 {
	return d.rolling.Get()
}

func (d *dimStat) Clone() *dimStat {
	return &dimStat{
		typ:         d.typ,
		rolling:     d.rolling.Clone(),
		lastAverage: d.lastAverage.Clone(),
	}
}

// HotPeerStat records each hot peer's statistics
type HotPeerStat struct {
	StoreID  uint64 `json:"store_id"`
	RegionID uint64 `json:"region_id"`

	// HotDegree records the times for the region considered as hot spot during each HandleRegionHeartbeat
	HotDegree int `json:"hot_degree"`
	// AntiCount used to eliminate some noise when remove region in cache
	AntiCount int `json:"anti_count"`

	Kind RWType `json:"-"`
	// Loads contains only Kind-related statistics and is DimLen in length.
	Loads []float64 `json:"loads"`

	// rolling statistics, recording some recently added records.
	rollingLoads []*dimStat

	// LastUpdateTime used to calculate average write
	LastUpdateTime time.Time `json:"last_update_time"`

	actionType             ActionType
	isLeader               bool
	isLearner              bool
	interval               uint64
	thresholds             []float64
	peers                  []*metapb.Peer
	lastTransferLeaderTime time.Time
	// If the peer didn't been send by store heartbeat when it is already stored as hot peer stat,
	// we will handle it as cold peer and mark the inCold flag
	inCold bool
	// source represents the statistics item source, such as direct, inherit.
	source sourceKind
	// If the item in storeA is just inherited from storeB,
	// then other store, such as storeC, will be forbidden to inherit from storeA until the item in storeA is hot.
	allowInherited bool
}

// ID returns region ID. Implementing TopNItem.
func (stat *HotPeerStat) ID() uint64 {
	return stat.RegionID
}

// Less compares two HotPeerStat.Implementing TopNItem.
func (stat *HotPeerStat) Less(dim int, than TopNItem) bool {
	return stat.GetLoad(dim) < than.(*HotPeerStat).GetLoad(dim)
}

// Log is used to output some info
func (stat *HotPeerStat) Log(str string, level func(msg string, fields ...zap.Field)) {
	level(str,
		zap.Uint64("interval", stat.interval),
		zap.Uint64("region-id", stat.RegionID),
		zap.Uint64("store", stat.StoreID),
		zap.Bool("is-leader", stat.isLeader),
		zap.Bool("is-learner", stat.isLearner),
		zap.String("type", stat.Kind.String()),
		zap.Float64s("loads", stat.GetLoads()),
		zap.Float64s("loads-instant", stat.Loads),
		zap.Float64s("thresholds", stat.thresholds),
		zap.Int("hot-degree", stat.HotDegree),
		zap.Int("hot-anti-count", stat.AntiCount),
		zap.Duration("sum-interval", stat.getIntervalSum()),
		zap.String("source", stat.source.String()),
		zap.Bool("allow-inherited", stat.allowInherited),
		zap.String("action-type", stat.actionType.String()),
		zap.Time("last-transfer-leader-time", stat.lastTransferLeaderTime))
}

// IsNeedCoolDownTransferLeader use cooldown time after transfer leader to avoid unnecessary schedule
func (stat *HotPeerStat) IsNeedCoolDownTransferLeader(minHotDegree int) bool {
	return time.Since(stat.lastTransferLeaderTime).Seconds() < float64(minHotDegree*stat.hotStatReportInterval())
}

// IsLeader indicates the item belong to the leader.
func (stat *HotPeerStat) IsLeader() bool {
	return stat.isLeader
}

// GetActionType returns the item action type.
func (stat *HotPeerStat) GetActionType() ActionType {
	return stat.actionType
}

// GetLoad returns denoising load if possible.
func (stat *HotPeerStat) GetLoad(dim int) float64 {
	if stat.rollingLoads != nil {
		return math.Round(stat.rollingLoads[dim].Get())
	}
	return math.Round(stat.Loads[dim])
}

// GetLoads returns denoising loads if possible.
func (stat *HotPeerStat) GetLoads() []float64 {
	if stat.rollingLoads != nil {
		ret := make([]float64, len(stat.rollingLoads))
		for dim := range ret {
			ret[dim] = math.Round(stat.rollingLoads[dim].Get())
		}
		return ret
	}
	return stat.Loads
}

// GetThresholds returns thresholds.
// Only for test purpose.
func (stat *HotPeerStat) GetThresholds() []float64 {
	return stat.thresholds
}

// Clone clones the HotPeerStat.
func (stat *HotPeerStat) Clone() *HotPeerStat {
	ret := *stat
	ret.Loads = make([]float64, DimLen)
	for i := 0; i < DimLen; i++ {
		ret.Loads[i] = stat.GetLoad(i) // replace with denoising loads
	}
	ret.rollingLoads = nil
	return &ret
}

func (stat *HotPeerStat) isHot() bool {
	return slice.AnyOf(stat.rollingLoads, func(i int) bool {
		return stat.rollingLoads[i].isLastAverageHot(stat.thresholds[i])
	})
}

func (stat *HotPeerStat) clearLastAverage() {
	for _, l := range stat.rollingLoads {
		l.clearLastAverage()
	}
}

func (stat *HotPeerStat) hotStatReportInterval() int {
	if stat.Kind == Read {
		return ReadReportInterval
	}
	return WriteReportInterval
}

func (stat *HotPeerStat) getIntervalSum() time.Duration {
	if len(stat.rollingLoads) == 0 || stat.rollingLoads[0] == nil {
		return 0
	}
	return stat.rollingLoads[0].lastAverage.GetIntervalSum()
}

// GetStores returns stores of the item.
func (stat *HotPeerStat) GetStores() []uint64 {
	stores := []uint64{}
	for _, peer := range stat.peers {
		stores = append(stores, peer.StoreId)
	}
	return stores
}

// IsLearner indicates whether the item is learner.
func (stat *HotPeerStat) IsLearner() bool {
	return stat.isLearner
}

func (stat *HotPeerStat) defaultAntiCount() int {
	if stat.Kind == Read {
		return hotRegionAntiCount * (RegionHeartBeatReportInterval / StoreHeartBeatReportInterval)
	}
	return hotRegionAntiCount
}

// Warm makes the item warm. It is only used for test.
func (stat *HotPeerStat) Warm() {
	stat.HotDegree = 20
	stat.AntiCount = stat.defaultAntiCount()
}
