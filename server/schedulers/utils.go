// Copyright 2017 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"fmt"
	"math"
	"net/url"
	"strconv"
	"time"

	"github.com/montanaflynn/stats"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
	"github.com/tikv/pd/server/statistics"
	"go.uber.org/zap"
)

const (
	// adjustRatio is used to adjust TolerantSizeRatio according to region count.
	adjustRatio             float64 = 0.005
	leaderTolerantSizeRatio float64 = 5.0
	minTolerantSizeRatio    float64 = 1.0
	influenceAmp            int64   = 100
)

type balancePlan struct {
	kind              core.ScheduleKind
	cluster           opt.Cluster
	opInfluence       operator.OpInfluence
	tolerantSizeRatio float64

	source *core.StoreInfo
	target *core.StoreInfo
	region *core.RegionInfo

	sourceScore float64
	targetScore float64
}

func newBalancePlan(kind core.ScheduleKind, cluster opt.Cluster, opInfluence operator.OpInfluence) *balancePlan {
	return &balancePlan{
		kind:              kind,
		cluster:           cluster,
		opInfluence:       opInfluence,
		tolerantSizeRatio: adjustTolerantRatio(cluster, kind),
	}
}

func (p *balancePlan) GetOpInfluence(storeID uint64) int64 {
	return p.opInfluence.GetStoreInfluence(storeID).ResourceProperty(p.kind)
}

func (p *balancePlan) SourceStoreID() uint64 {
	return p.source.GetID()
}

func (p *balancePlan) SourceMetricLabel() string {
	return strconv.FormatUint(p.SourceStoreID(), 10)
}

func (p *balancePlan) TargetStoreID() uint64 {
	return p.target.GetID()
}

func (p *balancePlan) TargetMetricLabel() string {
	return strconv.FormatUint(p.TargetStoreID(), 10)
}

func (p *balancePlan) shouldBalance(scheduleName string) bool {
	// The reason we use max(regionSize, averageRegionSize) to check is:
	// 1. prevent moving small regions between stores with close scores, leading to unnecessary balance.
	// 2. prevent moving huge regions, leading to over balance.
	sourceID := p.source.GetID()
	targetID := p.target.GetID()
	tolerantResource := p.getTolerantResource()
	// to avoid schedule too much, if A's core greater than B and C a little
	// we want that A should be moved out one region not two
	sourceInfluence := p.GetOpInfluence(sourceID)
	// A->B, B's influence is positive , so B can become source schedule, it will move region from B to C
	if sourceInfluence > 0 {
		sourceInfluence = -sourceInfluence
	}
	// to avoid schedule too much, if A's score less than B and C in small range,
	// we want that A can be moved in one region not two
	targetInfluence := p.GetOpInfluence(targetID)
	// to avoid schedule call back
	// A->B, A's influence is negative，so A will be target,C may move region to A
	if targetInfluence < 0 {
		targetInfluence = -targetInfluence
	}
	opts := p.cluster.GetOpts()
	switch p.kind.Resource {
	case core.LeaderKind:
		sourceDelta, targetDelta := sourceInfluence-tolerantResource, targetInfluence+tolerantResource
		p.sourceScore = p.source.LeaderScore(p.kind.Policy, sourceDelta)
		p.targetScore = p.target.LeaderScore(p.kind.Policy, targetDelta)
	case core.RegionKind:
		sourceDelta, targetDelta := sourceInfluence*influenceAmp-tolerantResource, targetInfluence*influenceAmp+tolerantResource
		p.sourceScore = p.source.RegionScore(opts.GetRegionScoreFormulaVersion(), opts.GetHighSpaceRatio(), opts.GetLowSpaceRatio(), sourceDelta)
		p.targetScore = p.target.RegionScore(opts.GetRegionScoreFormulaVersion(), opts.GetHighSpaceRatio(), opts.GetLowSpaceRatio(), targetDelta)
	}
	if opts.IsDebugMetricsEnabled() {
		opInfluenceStatus.WithLabelValues(scheduleName, strconv.FormatUint(sourceID, 10), "source").Set(float64(sourceInfluence))
		opInfluenceStatus.WithLabelValues(scheduleName, strconv.FormatUint(targetID, 10), "target").Set(float64(targetInfluence))
		tolerantResourceStatus.WithLabelValues(scheduleName, strconv.FormatUint(sourceID, 10), strconv.FormatUint(targetID, 10)).Set(float64(tolerantResource))
	}
	// Make sure after move, source score is still greater than target score.
	shouldBalance := p.sourceScore > p.targetScore

	if !shouldBalance {
		log.Debug("skip balance "+p.kind.Resource.String(),
			zap.String("scheduler", scheduleName), zap.Uint64("region-id", p.region.GetID()), zap.Uint64("source-store", sourceID), zap.Uint64("target-store", targetID),
			zap.Int64("source-size", p.source.GetRegionSize()), zap.Float64("source-score", p.sourceScore),
			zap.Int64("source-influence", sourceInfluence),
			zap.Int64("target-size", p.target.GetRegionSize()), zap.Float64("target-score", p.targetScore),
			zap.Int64("target-influence", targetInfluence),
			zap.Int64("average-region-size", p.cluster.GetAverageRegionSize()),
			zap.Int64("tolerant-resource", tolerantResource))
	}
	return shouldBalance
}

func (p *balancePlan) getTolerantResource() int64 {
	if p.kind.Resource == core.LeaderKind && p.kind.Policy == core.ByCount {
		return int64(p.tolerantSizeRatio)
	}
	regionSize := p.region.GetApproximateSize()
	if regionSize < p.cluster.GetAverageRegionSize() {
		regionSize = p.cluster.GetAverageRegionSize()
	}
	return int64(float64(regionSize) * p.tolerantSizeRatio)
}

func adjustTolerantRatio(cluster opt.Cluster, kind core.ScheduleKind) float64 {
	tolerantSizeRatio := cluster.GetOpts().GetTolerantSizeRatio()
	if kind.Resource == core.LeaderKind && kind.Policy == core.ByCount {
		if tolerantSizeRatio == 0 {
			return leaderTolerantSizeRatio
		}
		return tolerantSizeRatio
	}

	if tolerantSizeRatio == 0 {
		var maxRegionCount float64
		stores := cluster.GetStores()
		for _, store := range stores {
			regionCount := float64(cluster.GetStoreRegionCount(store.GetID()))
			if maxRegionCount < regionCount {
				maxRegionCount = regionCount
			}
		}
		tolerantSizeRatio = maxRegionCount * adjustRatio
		if tolerantSizeRatio < minTolerantSizeRatio {
			tolerantSizeRatio = minTolerantSizeRatio
		}
	}
	return tolerantSizeRatio
}

func adjustBalanceLimit(cluster opt.Cluster, kind core.ResourceKind) uint64 {
	stores := cluster.GetStores()
	counts := make([]float64, 0, len(stores))
	for _, s := range stores {
		if s.IsUp() {
			counts = append(counts, float64(s.ResourceCount(kind)))
		}
	}
	limit, _ := stats.StandardDeviation(counts)
	return typeutil.MaxUint64(1, uint64(limit))
}

func getKeyRanges(args []string) ([]core.KeyRange, error) {
	var ranges []core.KeyRange
	for len(args) > 1 {
		startKey, err := url.QueryUnescape(args[0])
		if err != nil {
			return nil, errs.ErrQueryUnescape.Wrap(err).FastGenWithCause()
		}
		endKey, err := url.QueryUnescape(args[1])
		if err != nil {
			return nil, errs.ErrQueryUnescape.Wrap(err).FastGenWithCause()
		}
		args = args[2:]
		ranges = append(ranges, core.NewKeyRange(startKey, endKey))
	}
	if len(ranges) == 0 {
		return []core.KeyRange{core.NewKeyRange("", "")}, nil
	}
	return ranges, nil
}

// Influence records operator influence.
type Influence struct {
	Loads []float64
	Count float64
}

type pendingInfluence struct {
	op                *operator.Operator
	from, to          uint64
	origin            Influence
	maxZombieDuration time.Duration
}

func newPendingInfluence(op *operator.Operator, from, to uint64, infl Influence, maxZombieDur time.Duration) *pendingInfluence {
	return &pendingInfluence{
		op:                op,
		from:              from,
		to:                to,
		origin:            infl,
		maxZombieDuration: maxZombieDur,
	}
}

type storeLoad struct {
	Loads []float64
	Count float64
}

func (load storeLoad) ToLoadPred(rwTy rwType, infl *Influence) *storeLoadPred {
	future := storeLoad{
		Loads: append(load.Loads[:0:0], load.Loads...),
		Count: load.Count,
	}
	if infl != nil {
		switch rwTy {
		case read:
			future.Loads[statistics.ByteDim] += infl.Loads[statistics.RegionReadBytes]
			future.Loads[statistics.KeyDim] += infl.Loads[statistics.RegionReadKeys]
			future.Loads[statistics.QueryDim] += infl.Loads[statistics.RegionReadQuery]
		case write:
			future.Loads[statistics.ByteDim] += infl.Loads[statistics.RegionWriteBytes]
			future.Loads[statistics.KeyDim] += infl.Loads[statistics.RegionWriteKeys]
			future.Loads[statistics.QueryDim] += infl.Loads[statistics.RegionWriteQuery]
		}
		future.Count += infl.Count
	}
	return &storeLoadPred{
		Current: load,
		Future:  future,
	}
}

func stLdRate(dim int) func(ld *storeLoad) float64 {
	return func(ld *storeLoad) float64 {
		return ld.Loads[dim]
	}
}

func stLdCount(ld *storeLoad) float64 {
	return ld.Count
}

type storeLoadCmp func(ld1, ld2 *storeLoad) int

func negLoadCmp(cmp storeLoadCmp) storeLoadCmp {
	return func(ld1, ld2 *storeLoad) int {
		return -cmp(ld1, ld2)
	}
}

func sliceLoadCmp(cmps ...storeLoadCmp) storeLoadCmp {
	return func(ld1, ld2 *storeLoad) int {
		for _, cmp := range cmps {
			if r := cmp(ld1, ld2); r != 0 {
				return r
			}
		}
		return 0
	}
}

func stLdRankCmp(dim func(ld *storeLoad) float64, rank func(value float64) int64) storeLoadCmp {
	return func(ld1, ld2 *storeLoad) int {
		return rankCmp(dim(ld1), dim(ld2), rank)
	}
}

func rankCmp(a, b float64, rank func(value float64) int64) int {
	aRk, bRk := rank(a), rank(b)
	if aRk < bRk {
		return -1
	} else if aRk > bRk {
		return 1
	}
	return 0
}

// store load prediction
type storeLoadPred struct {
	Current storeLoad
	Future  storeLoad
	Expect  storeLoad
}

func (lp *storeLoadPred) min() *storeLoad {
	return minLoad(&lp.Current, &lp.Future)
}

func (lp *storeLoadPred) max() *storeLoad {
	return maxLoad(&lp.Current, &lp.Future)
}

func (lp *storeLoadPred) diff() *storeLoad {
	mx, mn := lp.max(), lp.min()
	loads := make([]float64, len(mx.Loads))
	for i := range loads {
		loads[i] = mx.Loads[i] - mn.Loads[i]
	}
	return &storeLoad{
		Loads: loads,
		Count: mx.Count - mn.Count,
	}
}

type storeLPCmp func(lp1, lp2 *storeLoadPred) int

func sliceLPCmp(cmps ...storeLPCmp) storeLPCmp {
	return func(lp1, lp2 *storeLoadPred) int {
		for _, cmp := range cmps {
			if r := cmp(lp1, lp2); r != 0 {
				return r
			}
		}
		return 0
	}
}

func minLPCmp(ldCmp storeLoadCmp) storeLPCmp {
	return func(lp1, lp2 *storeLoadPred) int {
		return ldCmp(lp1.min(), lp2.min())
	}
}

func maxLPCmp(ldCmp storeLoadCmp) storeLPCmp {
	return func(lp1, lp2 *storeLoadPred) int {
		return ldCmp(lp1.max(), lp2.max())
	}
}

func diffCmp(ldCmp storeLoadCmp) storeLPCmp {
	return func(lp1, lp2 *storeLoadPred) int {
		return ldCmp(lp1.diff(), lp2.diff())
	}
}

func minLoad(a, b *storeLoad) *storeLoad {
	loads := make([]float64, len(a.Loads))
	for i := range loads {
		loads[i] = math.Min(a.Loads[i], b.Loads[i])
	}
	return &storeLoad{
		Loads: loads,
		Count: math.Min(a.Count, b.Count),
	}
}

func maxLoad(a, b *storeLoad) *storeLoad {
	loads := make([]float64, len(a.Loads))
	for i := range loads {
		loads[i] = math.Max(a.Loads[i], b.Loads[i])
	}
	return &storeLoad{
		Loads: loads,
		Count: math.Max(a.Count, b.Count),
	}
}

type storeInfo struct {
	Store      *core.StoreInfo
	IsTiFlash  bool
	PendingSum *Influence
}

func summaryStoreInfos(cluster opt.Cluster) map[uint64]*storeInfo {
	stores := cluster.GetStores()
	infos := make(map[uint64]*storeInfo, len(stores))
	for _, store := range stores {
		info := &storeInfo{
			Store:      store,
			IsTiFlash:  core.IsTiFlashStore(store.GetMeta()),
			PendingSum: nil,
		}
		infos[store.GetID()] = info
	}
	return infos
}

func (s *storeInfo) addInfluence(infl *Influence, w float64) {
	if infl == nil || w == 0 {
		return
	}
	if s.PendingSum == nil {
		s.PendingSum = &Influence{
			Loads: make([]float64, len(infl.Loads)),
			Count: 0,
		}
	}
	for i, load := range infl.Loads {
		s.PendingSum.Loads[i] += load * w
	}
	s.PendingSum.Count += infl.Count * w
}

type storeLoadDetail struct {
	Info     *storeInfo
	LoadPred *storeLoadPred
	HotPeers []*statistics.HotPeerStat
}

func (li *storeLoadDetail) toHotPeersStat() *statistics.HotPeersStat {
	totalLoads := make([]float64, statistics.RegionStatCount)
	if len(li.HotPeers) == 0 {
		return &statistics.HotPeersStat{
			TotalLoads:     totalLoads,
			TotalBytesRate: 0.0,
			TotalKeysRate:  0.0,
			TotalQueryRate: 0.0,
			Count:          0,
			Stats:          make([]statistics.HotPeerStatShow, 0),
		}
	}
	kind := write
	if li.HotPeers[0].Kind == statistics.ReadFlow {
		kind = read
	}

	peers := make([]statistics.HotPeerStatShow, 0, len(li.HotPeers))
	for _, peer := range li.HotPeers {
		if peer.HotDegree > 0 {
			peers = append(peers, toHotPeerStatShow(peer, kind))
			for i := range totalLoads {
				totalLoads[i] += peer.GetLoad(statistics.RegionStatKind(i))
			}
		}
	}

	b, k, q := getRegionStatKind(kind, statistics.ByteDim), getRegionStatKind(kind, statistics.KeyDim), getRegionStatKind(kind, statistics.QueryDim)
	byteRate, keyRate, queryRate := totalLoads[b], totalLoads[k], totalLoads[q]
	storeByteRate, storeKeyRate, storeQueryRate := li.LoadPred.Current.Loads[statistics.ByteDim],
		li.LoadPred.Current.Loads[statistics.KeyDim], li.LoadPred.Current.Loads[statistics.QueryDim]

	return &statistics.HotPeersStat{
		TotalLoads:     totalLoads,
		TotalBytesRate: byteRate,
		TotalKeysRate:  keyRate,
		TotalQueryRate: queryRate,
		StoreByteRate:  storeByteRate,
		StoreKeyRate:   storeKeyRate,
		StoreQueryRate: storeQueryRate,
		Count:          len(peers),
		Stats:          peers,
	}
}

func toHotPeerStatShow(p *statistics.HotPeerStat, kind rwType) statistics.HotPeerStatShow {
	b, k, q := getRegionStatKind(kind, statistics.ByteDim), getRegionStatKind(kind, statistics.KeyDim), getRegionStatKind(kind, statistics.QueryDim)
	byteRate := p.Loads[b]
	keyRate := p.Loads[k]
	queryRate := p.Loads[q]
	return statistics.HotPeerStatShow{
		StoreID:        p.StoreID,
		RegionID:       p.RegionID,
		HotDegree:      p.HotDegree,
		ByteRate:       byteRate,
		KeyRate:        keyRate,
		QueryRate:      queryRate,
		AntiCount:      p.AntiCount,
		LastUpdateTime: p.LastUpdateTime,
	}
}

// summaryStoresLoad Load information of all available stores.
// it will filter the hot peer and calculate the current and future stat(rate,count) for each store
func summaryStoresLoad(
	storeInfos map[uint64]*storeInfo,
	storesLoads map[uint64][]float64,
	storeHotPeers map[uint64][]*statistics.HotPeerStat,
	rwTy rwType,
	kind core.ResourceKind,
) map[uint64]*storeLoadDetail {
	// loadDetail stores the storeID -> hotPeers stat and its current and future stat(rate,count)
	loadDetail := make(map[uint64]*storeLoadDetail, len(storesLoads))
	allTiKVLoadSum := make([]float64, statistics.DimLen)
	allTiFlashLoadSum := make([]float64, statistics.DimLen)
	allTiKVCount := 0
	allTiFlashCount := 0
	allTiKVHotPeersCount := 0
	allTiFlashHotPeersCount := 0

	// Stores without byte rate statistics is not available to schedule.
	for _, info := range storeInfos {
		store := info.Store
		id := store.GetID()
		storeLoads, ok := storesLoads[id]
		if !ok {
			continue
		}
		isTiFlash := core.IsTiFlashStore(store.GetMeta())

		loads := make([]float64, statistics.DimLen)
		switch rwTy {
		case read:
			loads[statistics.ByteDim] = storeLoads[statistics.StoreReadBytes]
			loads[statistics.KeyDim] = storeLoads[statistics.StoreReadKeys]
			loads[statistics.QueryDim] = storeLoads[statistics.StoreReadQuery]
		case write:
			if isTiFlash {
				// TiFlash is currently unable to report statistics in the same unit as Region,
				// so it uses the sum of Regions.
				loads[statistics.ByteDim] = storeLoads[statistics.StoreRegionsWriteBytes]
				loads[statistics.KeyDim] = storeLoads[statistics.StoreRegionsWriteKeys]
			} else {
				loads[statistics.ByteDim] = storeLoads[statistics.StoreWriteBytes]
				loads[statistics.KeyDim] = storeLoads[statistics.StoreWriteKeys]
			}
			loads[statistics.QueryDim] = storeLoads[statistics.StoreWriteQuery]
		}

		// Find all hot peers first
		var hotPeers []*statistics.HotPeerStat
		{
			peerLoadSum := make([]float64, statistics.DimLen)
			// TODO: To remove `filterHotPeers`, we need to:
			// HotLeaders consider `Write{Bytes,Keys}`, so when we schedule `writeLeader`, all peers are leader.
			for _, peer := range filterHotPeers(kind, storeHotPeers[id]) {
				for i := range peerLoadSum {
					peerLoadSum[i] += peer.GetLoad(getRegionStatKind(rwTy, i))
				}
				hotPeers = append(hotPeers, peer.Clone())
			}
			// Use sum of hot peers to estimate leader-only byte rate.
			// For write requests, Write{Bytes, Keys} is applied to all Peers at the same time,
			// while the Leader and Follower are under different loads (usually the Leader consumes more CPU).
			// Write{QPS} does not require such processing.
			if kind == core.LeaderKind && rwTy == write {
				loads[statistics.ByteDim] = peerLoadSum[statistics.ByteDim]
				loads[statistics.KeyDim] = peerLoadSum[statistics.KeyDim]
			}

			// Metric for debug.
			{
				ty := "byte-rate-" + rwTy.String() + "-" + kind.String()
				hotPeerSummary.WithLabelValues(ty, fmt.Sprintf("%v", id)).Set(peerLoadSum[statistics.ByteDim])
			}
			{
				ty := "key-rate-" + rwTy.String() + "-" + kind.String()
				hotPeerSummary.WithLabelValues(ty, fmt.Sprintf("%v", id)).Set(peerLoadSum[statistics.KeyDim])
			}
			{
				ty := "query-rate-" + rwTy.String() + "-" + kind.String()
				hotPeerSummary.WithLabelValues(ty, fmt.Sprintf("%v", id)).Set(peerLoadSum[statistics.QueryDim])
			}
		}

		if isTiFlash {
			for i := range allTiFlashLoadSum {
				allTiFlashLoadSum[i] += loads[i]
			}
			allTiFlashCount += 1
			allTiFlashHotPeersCount += len(hotPeers)
		} else {
			for i := range allTiKVLoadSum {
				allTiKVLoadSum[i] += loads[i]
			}
			allTiKVCount += 1
			allTiKVHotPeersCount += len(hotPeers)
		}

		// Build store load prediction from current load and pending influence.
		stLoadPred := (&storeLoad{
			Loads: loads,
			Count: float64(len(hotPeers)),
		}).ToLoadPred(rwTy, info.PendingSum)

		// Construct store load info.
		loadDetail[id] = &storeLoadDetail{
			Info:     info,
			LoadPred: stLoadPred,
			HotPeers: hotPeers,
		}
	}

	// store expectation rate and count for each store-load detail.
	for id, detail := range loadDetail {
		var allLoadSum []float64
		var allStoreCount float64
		var allHotPeersCount float64
		if detail.Info.IsTiFlash {
			allLoadSum = allTiFlashLoadSum
			allStoreCount = float64(allTiFlashCount)
			allHotPeersCount = float64(allTiFlashHotPeersCount)
		} else {
			allLoadSum = allTiKVLoadSum
			allStoreCount = float64(allTiKVCount)
			allHotPeersCount = float64(allTiKVHotPeersCount)
		}
		expectLoads := make([]float64, len(allLoadSum))
		for i := range expectLoads {
			expectLoads[i] = allLoadSum[i] / allStoreCount
		}
		expectCount := allHotPeersCount / allStoreCount
		detail.LoadPred.Expect.Loads = expectLoads
		detail.LoadPred.Expect.Count = expectCount
		// Debug
		{
			ty := "exp-byte-rate-" + rwTy.String() + "-" + kind.String()
			hotPeerSummary.WithLabelValues(ty, fmt.Sprintf("%v", id)).Set(expectLoads[statistics.ByteDim])
		}
		{
			ty := "exp-key-rate-" + rwTy.String() + "-" + kind.String()
			hotPeerSummary.WithLabelValues(ty, fmt.Sprintf("%v", id)).Set(expectLoads[statistics.KeyDim])
		}
		{
			ty := "exp-query-rate-" + rwTy.String() + "-" + kind.String()
			hotPeerSummary.WithLabelValues(ty, fmt.Sprintf("%v", id)).Set(expectLoads[statistics.QueryDim])
		}
		{
			ty := "exp-count-rate-" + rwTy.String() + "-" + kind.String()
			hotPeerSummary.WithLabelValues(ty, fmt.Sprintf("%v", id)).Set(expectCount)
		}
	}
	return loadDetail
}

// filterHotPeers filter the peer whose hot degree is less than minHotDegress
func filterHotPeers(
	kind core.ResourceKind,
	peers []*statistics.HotPeerStat,
) []*statistics.HotPeerStat {
	ret := make([]*statistics.HotPeerStat, 0, len(peers))
	for _, peer := range peers {
		if kind == core.LeaderKind && !peer.IsLeader() {
			continue
		}
		ret = append(ret, peer)
	}
	return ret
}
