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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scatter

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/config"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
)

const regionScatterName = "region-scatter"

var (
	gcInterval            = time.Minute
	gcTTL                 = time.Minute * 3
	operatorPriorityLevel = constant.High

	// WithLabelValues is a heavy operation, define variable to avoid call it every time.
	scatterSkipEmptyRegionCounter   = scatterCounter.WithLabelValues("skip", "empty-region")
	scatterSkipNoRegionCounter      = scatterCounter.WithLabelValues("skip", "no-region")
	scatterSkipNoLeaderCounter      = scatterCounter.WithLabelValues("skip", "no-leader")
	scatterSkipHotRegionCounter     = scatterCounter.WithLabelValues("skip", "hot")
	scatterSkipNotReplicatedCounter = scatterCounter.WithLabelValues("skip", "not-replicated")
	scatterSkipAffinityCounter      = scatterCounter.WithLabelValues("skip", "affinity")
	scatterUnnecessaryCounter       = scatterCounter.WithLabelValues("unnecessary", "")
	scatterFailCounter              = scatterCounter.WithLabelValues("fail", "")
	scatterSuccessCounter           = scatterCounter.WithLabelValues("success", "")
	scatterOperatorRunningCounter   = scatterCounter.WithLabelValues("skip", "running")
	scatterOperatorExistedCounter   = scatterCounter.WithLabelValues("fail", "other-existed")
)

const (
	maxSleepDuration     = time.Minute
	initialSleepDuration = 100 * time.Millisecond
	maxRetryLimit        = 30
	// AdminScatterOperatorDesc is used by external admin/API scatter requests.
	AdminScatterOperatorDesc = "scatter-region"
	// InternalScatterOperatorDesc is used by PD-internal split-scatter dispatch.
	InternalScatterOperatorDesc = "internal-scatter-region"
)

type selectedStores struct {
	mu                syncutil.RWMutex
	groupDistribution *cache.TTLString // value type: map[uint64]uint64, group -> StoreID -> count
	seededGroups      *cache.TTLString // value type: bool, group -> seeded baseline marker
}

func newSelectedStores(ctx context.Context) *selectedStores {
	return &selectedStores{
		groupDistribution: cache.NewStringTTL(ctx, gcInterval, gcTTL),
		seededGroups:      cache.NewStringTTL(ctx, gcInterval, gcTTL),
	}
}

func cloneDistribution(distribution map[uint64]uint64) map[uint64]uint64 {
	cloned := make(map[uint64]uint64, len(distribution))
	for id, count := range distribution {
		cloned[id] = count
	}
	return cloned
}

func sortedStoreIDsFromCounts(counts map[uint64]uint64) []uint64 {
	ids := make([]uint64, 0, len(counts))
	for id := range counts {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
	return ids
}

func formatStoreCounts(counts map[uint64]uint64) string {
	if len(counts) == 0 {
		return ""
	}
	parts := make([]string, 0, len(counts))
	for _, id := range sortedStoreIDsFromCounts(counts) {
		parts = append(parts, fmt.Sprintf("%d:%d", id, counts[id]))
	}
	return strings.Join(parts, ",")
}

func formatStoreReasons(reasons map[uint64][]string) string {
	if len(reasons) == 0 {
		return ""
	}
	ids := make([]uint64, 0, len(reasons))
	for id := range reasons {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, fmt.Sprintf("%d:[%s]", id, strings.Join(reasons[id], "|")))
	}
	return strings.Join(parts, ",")
}

func formatStoreIDs(ids []uint64) string {
	if len(ids) == 0 {
		return ""
	}
	sorted := append([]uint64(nil), ids...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})
	parts := make([]string, 0, len(sorted))
	for _, id := range sorted {
		parts = append(parts, strconv.FormatUint(id, 10))
	}
	return strings.Join(parts, ",")
}

func decrementDistribution(distribution map[uint64]uint64, id uint64) {
	count, ok := distribution[id]
	if !ok {
		return
	}
	if count <= 1 {
		delete(distribution, id)
		return
	}
	distribution[id] = count - 1
}

// Get the count by storeID and group
func (s *selectedStores) Get(id uint64, group string) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	distribution, ok := s.getDistributionByGroupLocked(group)
	if !ok {
		return 0
	}
	count, ok := distribution[id]
	if !ok {
		return 0
	}
	return count
}

// GetGroupDistribution get distribution group by `group`
func (s *selectedStores) GetGroupDistribution(group string) (map[uint64]uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.getDistributionByGroupLocked(group)
}

// InitGroupDistribution seeds the distribution for a group if the group has not
// been tracked yet.
func (s *selectedStores) InitGroupDistribution(group string, distribution map[uint64]uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.getDistributionByGroupLocked(group); ok {
		return false
	}
	s.groupDistribution.Put(group, cloneDistribution(distribution))
	s.seededGroups.Put(group, true)
	return true
}

// Update records the group distribution after scattering one more region.
// For seeded groups it applies the net old->new change. Otherwise it keeps the
// historical scatter behavior and only counts the new placement.
func (s *selectedStores) Update(group string, oldIDs, newIDs []uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	distribution, ok := s.getDistributionByGroupLocked(group)
	if !ok {
		distribution = map[uint64]uint64{}
	}
	if s.isSeededGroupLocked(group) {
		for _, id := range oldIDs {
			decrementDistribution(distribution, id)
		}
		s.seededGroups.Put(group, true)
	}
	for _, id := range newIDs {
		distribution[id]++
	}
	s.groupDistribution.Put(group, distribution)
}

// getDistributionByGroupLocked should be called with lock
func (s *selectedStores) getDistributionByGroupLocked(group string) (map[uint64]uint64, bool) {
	if result, ok := s.groupDistribution.Get(group); ok {
		return result.(map[uint64]uint64), true
	}
	return nil, false
}

func (s *selectedStores) isSeededGroupLocked(group string) bool {
	_, ok := s.seededGroups.Get(group)
	return ok
}

// IsSeededGroup returns whether the group has been seeded with a baseline
// distribution.
func (s *selectedStores) IsSeededGroup(group string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.isSeededGroupLocked(group)
}

// RegionScatterer scatters regions.
type RegionScatterer struct {
	ctx               context.Context
	name              string
	cluster           sche.SharedCluster
	ordinaryEngine    engineContext
	specialEngines    sync.Map
	opController      *operator.Controller
	addSuspectRegions func(bool, ...uint64)
	affinityFilter    filter.RegionFilter
}

// NewRegionScatterer creates a region scatterer.
// RegionScatter is used for the `Lightning`, it will scatter the specified regions before import data.
func NewRegionScatterer(ctx context.Context, cluster sche.SharedCluster, opController *operator.Controller, addSuspectRegions func(bool, ...uint64)) *RegionScatterer {
	return &RegionScatterer{
		ctx:               ctx,
		name:              regionScatterName,
		cluster:           cluster,
		opController:      opController,
		addSuspectRegions: addSuspectRegions,
		affinityFilter:    filter.NewAffinityFilter(cluster),
		ordinaryEngine: newEngineContext(ctx, func() filter.Filter {
			return filter.NewEngineFilter(regionScatterName, filter.NotSpecialEngines)
		}),
	}
}

type filterFunc func() filter.Filter

type engineContext struct {
	filterFuncs    []filterFunc
	selectedPeer   *selectedStores
	selectedLeader *selectedStores
}

func newEngineContext(ctx context.Context, filterFuncs ...filterFunc) engineContext {
	filterFuncs = append(filterFuncs, func() filter.Filter {
		return &filter.StoreStateFilter{ActionScope: regionScatterName, MoveRegion: true, ScatterRegion: true, OperatorLevel: operatorPriorityLevel}
	})
	return engineContext{
		filterFuncs:    filterFuncs,
		selectedPeer:   newSelectedStores(ctx),
		selectedLeader: newSelectedStores(ctx),
	}
}

func (r *RegionScatterer) getOrCreateSpecialEngineContext(engine string) engineContext {
	if ctx, ok := r.specialEngines.Load(engine); ok {
		return ctx.(engineContext)
	}
	ctx := newEngineContext(r.ctx, func() filter.Filter {
		return filter.NewEngineFilter(r.name, placement.LabelConstraint{Key: core.EngineKey, Op: placement.In, Values: []string{engine}})
	})
	r.specialEngines.Store(engine, ctx)
	return ctx
}

// SeedGroupDistributionByRange seeds the scatter group with the current peer and
// leader distribution of the specified key range. Existing group history is kept.
func (r *RegionScatterer) SeedGroupDistributionByRange(group string, startKey, endKey []byte) {
	if group == "" || len(startKey) == 0 {
		return
	}
	if r.ordinaryEngine.selectedPeer.IsSeededGroup(group) {
		return
	}

	engineFilter := filter.NewEngineFilter(r.name, filter.NotSpecialEngines)
	ordinaryPeer := make(map[uint64]uint64)
	ordinaryLeader := make(map[uint64]uint64)
	specialPeer := make(map[string]map[uint64]uint64)
	for _, store := range r.cluster.GetStores() {
		if store == nil {
			continue
		}
		storeID := store.GetID()
		peerCount := uint64(r.cluster.GetStorePeerCountByRange(storeID, startKey, endKey))
		if engineFilter.Target(r.cluster.GetSharedConfig(), store).IsOK() {
			ordinaryPeer[storeID] = peerCount
			ordinaryLeader[storeID] = uint64(r.cluster.GetStoreLeaderCountByRange(storeID, startKey, endKey))
			continue
		}

		engine := store.GetLabelValue(core.EngineKey)
		if _, ok := specialPeer[engine]; !ok {
			specialPeer[engine] = make(map[uint64]uint64)
		}
		specialPeer[engine][storeID] = peerCount
	}

	r.ordinaryEngine.selectedPeer.InitGroupDistribution(group, ordinaryPeer)
	r.ordinaryEngine.selectedLeader.InitGroupDistribution(group, ordinaryLeader)
	for engine, distribution := range specialPeer {
		ctx := r.getOrCreateSpecialEngineContext(engine)
		ctx.selectedPeer.InitGroupDistribution(group, distribution)
	}
	log.Info("seed internal scatter distribution by range",
		zap.String("group", group),
		zap.Binary("start-key", startKey),
		zap.Binary("end-key", endKey),
		zap.String("ordinary-peer-distribution", formatStoreCounts(ordinaryPeer)),
		zap.String("ordinary-leader-distribution", formatStoreCounts(ordinaryLeader)))
}

// ScatterRegionsByRange directly scatter regions by ScatterRegions
func (r *RegionScatterer) ScatterRegionsByRange(startKey, endKey []byte, group string, retryLimit int) (int, map[uint64]error, error) {
	regions := r.cluster.ScanRegions(startKey, endKey, -1)
	if len(regions) < 1 {
		scatterSkipEmptyRegionCounter.Inc()
		return 0, nil, errs.ErrEmptyRegion
	}
	failures := make(map[uint64]error, len(regions))
	regionMap := make(map[uint64]*core.RegionInfo, len(regions))
	for _, region := range regions {
		regionMap[region.GetID()] = region
	}
	// If there existed any region failed to relocated after retry, add it into unProcessedRegions
	opsCount, err := r.scatterRegions(regionMap, failures, group, retryLimit, false)
	if err != nil {
		return 0, nil, err
	}
	return opsCount, failures, nil
}

// ScatterRegionsByID directly scatter regions by ScatterRegions
func (r *RegionScatterer) ScatterRegionsByID(regionsID []uint64, group string, retryLimit int, skipStoreLimit bool) (int, map[uint64]error, error) {
	if len(regionsID) < 1 {
		scatterSkipEmptyRegionCounter.Inc()
		return 0, nil, errs.ErrEmptyRegion
	}
	if len(regionsID) == 1 {
		region := r.cluster.GetRegion(regionsID[0])
		if region == nil {
			scatterSkipNoRegionCounter.Inc()
			return 0, nil, errs.ErrRegionNotFound
		}
	}
	failures := make(map[uint64]error, len(regionsID))
	regions := make([]*core.RegionInfo, 0, len(regionsID))
	for _, id := range regionsID {
		region := r.cluster.GetRegion(id)
		if region == nil {
			scatterSkipNoRegionCounter.Inc()
			log.Warn("failed to find region during scatter", zap.Uint64("region-id", id))
			failures[id] = errors.New(fmt.Sprintf("failed to find region %v", id))
			continue
		}
		regions = append(regions, region)
	}
	regionMap := make(map[uint64]*core.RegionInfo, len(regions))
	for _, region := range regions {
		regionMap[region.GetID()] = region
	}
	// If there existed any region failed to relocated after retry, add it into unProcessedRegions
	opsCount, err := r.scatterRegions(regionMap, failures, group, retryLimit, skipStoreLimit)
	if err != nil {
		return 0, nil, err
	}
	return opsCount, failures, nil
}

// scatterRegions relocates the regions. If the group is defined, the regions' leader with the same group would be scattered
// in a group level instead of cluster level.
// RetryTimes indicates the retry times if any of the regions failed to relocate during scattering. There will be
// time.Sleep between each retry.
// Failures indicates the regions which are failed to be relocated, the key of the failures indicates the regionID
// and the value of the failures indicates the failure error.
func (r *RegionScatterer) scatterRegions(regions map[uint64]*core.RegionInfo, failures map[uint64]error, group string, retryLimit int, skipStoreLimit bool) (int, error) {
	if len(regions) < 1 {
		scatterSkipEmptyRegionCounter.Inc()
		return 0, errs.ErrEmptyRegion
	}
	if retryLimit > maxRetryLimit {
		retryLimit = maxRetryLimit
	}
	// opsCount represents the number of regions successfully processed (not necessarily
	// with operators created). This includes regions skipped due to affinity or already
	// in ideal distribution.
	opsCount := 0
	for currentRetry := 0; currentRetry <= retryLimit; currentRetry++ {
		for _, region := range regions {
			op, err := r.Scatter(region, group, skipStoreLimit)
			failpoint.Inject("scatterFail", func() {
				if region.GetID() == 1 {
					err = errors.New("mock error")
				}
			})
			if err != nil {
				failures[region.GetID()] = err
				continue
			}
			delete(regions, region.GetID())
			opsCount++
			if op != nil {
				if ok := r.opController.AddOperator(op); !ok {
					// If there existed any operator failed to be added into Operator Controller, add its regions into unProcessedRegions
					failures[op.RegionID()] = fmt.Errorf("region %v failed to add operator", op.RegionID())
					continue
				}
				r.Commit(region, op, group)
				failpoint.Inject("scatterHbStreamsDrain", func() {
					_ = r.opController.GetHBStreams().Drain(1)
					r.opController.RemoveOperator(op, operator.AdminStop)
				})
			}
			delete(failures, region.GetID())
		}
		// all regions have been relocated, break the loop.
		if len(regions) < 1 {
			break
		}
		// Wait for a while if there are some regions failed to be relocated
		time.Sleep(typeutil.MinDuration(maxSleepDuration, time.Duration(math.Pow(2, float64(currentRetry)))*initialSleepDuration))
	}
	return opsCount, nil
}

// Scatter relocates the region. If the group is defined, the regions' leader with the same group would be scattered
// in a group level instead of cluster level.
func (r *RegionScatterer) Scatter(region *core.RegionInfo, group string, skipStoreLimit bool) (*operator.Operator, error) {
	return r.ScatterWithDesc(region, group, skipStoreLimit, AdminScatterOperatorDesc)
}

// ScatterWithDesc relocates the region and tags the created operator with the
// provided desc so external/admin scatter and internal split-scatter can be
// distinguished in operator metrics.
func (r *RegionScatterer) ScatterWithDesc(region *core.RegionInfo, group string, skipStoreLimit bool, desc string) (*operator.Operator, error) {
	if desc == "" {
		desc = AdminScatterOperatorDesc
	}
	if !filter.IsRegionReplicated(r.cluster, region) {
		r.addSuspectRegions(false, region.GetID())
		scatterSkipNotReplicatedCounter.Inc()
		log.Warn("region not replicated during scatter", zap.Uint64("region-id", region.GetID()))
		return nil, errors.Errorf("region %d is not fully replicated", region.GetID())
	}

	// Check if there is any existing operator for the region.
	// if the exist operator level is higher than scatter operator level, give up to create new scatter operator new.
	// otherwise, create new scatter operator to replace the existing one.
	if op := r.opController.GetOperator(region.GetID()); op != nil && op.GetPriorityLevel() >= operatorPriorityLevel {
		val, exist := op.GetAdditionalInfo("group")
		// If the existing operator is created by the same group scatterer, just skip creating a new one.
		if strings.Contains(op.Desc(), "scatter-region") && exist && val == group {
			scatterOperatorRunningCounter.Inc()
			log.Debug("scatter operator is already running",
				zap.Uint64("region-id", region.GetID()))
			return nil, nil
		}
		scatterOperatorExistedCounter.Inc()
		log.Debug("the operator exist, but it does not meet requirement",
			zap.Uint64("region-id", region.GetID()),
			zap.String("additional-info-group", val),
			zap.String("operator-des", op.Desc()),
			zap.Bool("group-exist", exist),
		)
		return nil, errors.Errorf("the operator of region %d already exist", region.GetID())
	}

	if region.GetLeader() == nil {
		scatterSkipNoLeaderCounter.Inc()
		log.Warn("region no leader during scatter", zap.Uint64("region-id", region.GetID()))
		return nil, errors.Errorf("region %d has no leader", region.GetID())
	}

	// Check if region is in an affinity group that doesn't allow regular scheduling.
	// Unlike hot regions or regions without leaders (which are temporary states),
	// affinity is a configured persistent state. Returning nil error prevents the
	// client from retrying, as retrying won't change the affinity configuration.
	// Note: Returning (nil, nil) means:
	//   - The region will not be retried in scatterRegions loop
	//   - opsCount will still increment (representing "successfully processed", not "operator created")
	//   - The region won't appear in the failures map (client considers it successful)
	if !r.affinityFilter.Select(region).IsOK() {
		scatterSkipAffinityCounter.Inc()
		return nil, nil
	}

	if desc == AdminScatterOperatorDesc && r.cluster.IsRegionHot(region) {
		scatterSkipHotRegionCounter.Inc()
		log.Warn("region too hot during scatter", zap.Uint64("region-id", region.GetID()))
		return nil, errors.Errorf("region %d is hot", region.GetID())
	}

	return r.scatterRegionWithDesc(region, group, skipStoreLimit, desc)
}

func (r *RegionScatterer) scatterRegion(region *core.RegionInfo, group string, skipStoreLimit bool) (*operator.Operator, error) {
	return r.scatterRegionWithDesc(region, group, skipStoreLimit, AdminScatterOperatorDesc)
}

func (r *RegionScatterer) scatterRegionWithDesc(region *core.RegionInfo, group string, skipStoreLimit bool, desc string) (*operator.Operator, error) {
	logInternalScatter := desc == InternalScatterOperatorDesc
	engineFilter := filter.NewEngineFilter(r.name, filter.NotSpecialEngines)
	ordinaryPeers := make(map[uint64]*metapb.Peer, len(region.GetPeers()))
	specialPeers := make(map[string]map[uint64]*metapb.Peer)
	oldFit := r.cluster.GetRuleManager().FitRegion(r.cluster, region)
	// Group peers by the engine of their stores
	for _, peer := range region.GetPeers() {
		store := r.cluster.GetStore(peer.GetStoreId())
		if store == nil {
			return nil, errs.ErrGetSourceStore.FastGenByArgs(fmt.Sprintf("store not found, peer: %v, region id: %d", peer, region.GetID()))
		}
		if engineFilter.Target(r.cluster.GetSharedConfig(), store).IsOK() {
			ordinaryPeers[peer.GetStoreId()] = peer
		} else {
			engine := store.GetLabelValue(core.EngineKey)
			if _, ok := specialPeers[engine]; !ok {
				specialPeers[engine] = make(map[uint64]*metapb.Peer)
			}
			specialPeers[engine][peer.GetStoreId()] = peer
		}
	}

	targetPeers := make(map[uint64]*metapb.Peer, len(region.GetPeers()))                  // StoreID -> Peer
	selectedStores := make(map[uint64]struct{}, len(region.GetPeers()))                   // selected StoreID set
	leaderCandidateStores := make([]uint64, 0, len(region.GetPeers()))                    // StoreID allowed to become Leader
	scatterWithSameEngine := func(peers map[uint64]*metapb.Peer, context engineContext) { // peers: StoreID -> Peer
		filterLen := len(context.filterFuncs) + 2
		filters := make([]filter.Filter, filterLen)
		for i, filterFunc := range context.filterFuncs {
			filters[i] = filterFunc()
		}
		filters[filterLen-2] = filter.NewExcludedFilter(r.name, nil, selectedStores)
		for _, peer := range peers {
			if _, ok := selectedStores[peer.GetStoreId()]; ok {
				if allowLeader(oldFit, peer) {
					leaderCandidateStores = append(leaderCandidateStores, peer.GetStoreId())
				}
				// It is both sourcePeer and targetPeer itself, no need to select.
				continue
			}
			sourceStore := r.cluster.GetStore(peer.GetStoreId())
			if sourceStore == nil {
				log.Error("failed to get the store", zap.Uint64("store-id", peer.GetStoreId()), errs.ZapError(errs.ErrGetSourceStore))
				continue
			}
			filters[filterLen-1] = filter.NewPlacementSafeguard(r.name, r.cluster.GetSharedConfig(), r.cluster.GetBasicCluster(), r.cluster.GetRuleManager(), region, sourceStore, oldFit)
			for {
				newPeer, peerTrace := r.selectNewPeerWithTrace(context, group, peer, filters)
				targetPeers[newPeer.GetStoreId()] = newPeer
				selectedStores[newPeer.GetStoreId()] = struct{}{}
				if logInternalScatter {
					log.Info("internal scatter peer selection",
						zap.Uint64("region-id", region.GetID()),
						zap.String("group", group),
						zap.Uint64("source-store", peer.GetStoreId()),
						zap.Uint64("target-store", newPeer.GetStoreId()),
						zap.Bool("kept-origin", newPeer.GetStoreId() == peer.GetStoreId()),
						zap.Uint64("max-picked-count", peerTrace.maxStorePickedCount),
						zap.Uint64("min-picked-count", peerTrace.minStorePickedCount),
						zap.Uint64("origin-picked-count", peerTrace.originStorePickedCount),
						zap.Strings("candidates", peerTrace.candidates),
					)
				}
				// If the selected peer is a peer other than origin peer in this region,
				// it is considered that the selected peer select itself.
				// This origin peer re-selects.
				if _, ok := peers[newPeer.GetStoreId()]; !ok || peer.GetStoreId() == newPeer.GetStoreId() {
					selectedStores[peer.GetStoreId()] = struct{}{}
					if allowLeader(oldFit, peer) {
						leaderCandidateStores = append(leaderCandidateStores, newPeer.GetStoreId())
					}
					break
				}
			}
		}
	}

	scatterWithSameEngine(ordinaryPeers, r.ordinaryEngine)
	// FIXME: target leader only considers the ordinary stores, maybe we need to consider the
	// special engine stores if the engine supports to become a leader. But now there is only
	// one engine, tiflash, which does not support the leader, so don't consider it for now.
	targetLeader, leaderStorePickedCount, leaderTrace := r.selectAvailableLeaderStoreWithTrace(group, region, leaderCandidateStores, r.ordinaryEngine)
	if targetLeader == 0 {
		scatterSkipNoLeaderCounter.Inc()
		return nil, errs.ErrGetTargetStore.FastGenByArgs(fmt.Sprintf("no target leader store found, region: %v", region))
	}
	if logInternalScatter {
		log.Info("internal scatter leader selection",
			zap.Uint64("region-id", region.GetID()),
			zap.String("group", group),
			zap.Uint64("target-leader", targetLeader),
			zap.Uint64("leader-picked-count", leaderStorePickedCount),
			zap.Strings("leader-candidates", leaderTrace),
		)
	}

	for engine, peers := range specialPeers {
		scatterWithSameEngine(peers, r.getOrCreateSpecialEngineContext(engine))
	}

	if isSameDistribution(region, targetPeers, targetLeader) {
		if logInternalScatter {
			log.Info("internal scatter keeps original placement",
				zap.Uint64("region-id", region.GetID()),
				zap.String("group", group),
				zap.Strings("current-peers", formatScatterTargetPeers(scatterPlacementPeers(region))),
				zap.Uint64("current-leader", region.GetLeader().GetStoreId()))
		}
		scatterUnnecessaryCounter.Inc()
		r.Update(region, targetPeers, targetLeader, group)
		return nil, nil
	}
	op, err := operator.CreateScatterRegionOperator(desc, r.cluster, region, targetPeers, targetLeader, skipStoreLimit)
	if err != nil {
		scatterFailCounter.Inc()
		currentPeers := make(map[uint64]*metapb.Peer, len(region.GetPeers()))
		for _, peer := range region.GetPeers() {
			currentPeers[peer.GetStoreId()] = peer
		}
		r.Update(region, currentPeers, region.GetLeader().GetStoreId(), group)
		log.Debug("fail to create scatter region operator", errs.ZapError(err))
		return nil, errs.ErrCreateOperator.FastGenByArgs(fmt.Sprintf("failed to create scatter region operator for region %v", region.GetID()))
	}
	if op != nil {
		scatterSuccessCounter.Inc()
		op.SetAdditionalInfo("group", group)
		op.SetAdditionalInfo("leader-picked-count", strconv.FormatUint(leaderStorePickedCount, 10))
		op.SetPriorityLevel(operatorPriorityLevel)
		if logInternalScatter {
			log.Info("internal scatter operator created",
				zap.Uint64("region-id", region.GetID()),
				zap.String("group", group),
				zap.Strings("target-peers", formatScatterTargetPeers(targetPeers)),
				zap.Uint64("target-leader", targetLeader),
				zap.String("operator-desc", desc),
			)
		}
	}
	return op, nil
}

func allowLeader(fit *placement.RegionFit, peer *metapb.Peer) bool {
	switch peer.GetRole() {
	case metapb.PeerRole_Learner, metapb.PeerRole_DemotingVoter:
		return false
	}
	if peer.IsWitness {
		return false
	}
	peerFit := fit.GetRuleFit(peer.GetId())
	if peerFit == nil || peerFit.Rule == nil || peerFit.Rule.IsWitness {
		return false
	}
	switch peerFit.Rule.Role {
	case placement.Voter, placement.Leader:
		return true
	}
	return false
}

func isSameDistribution(region *core.RegionInfo, targetPeers map[uint64]*metapb.Peer, targetLeader uint64) bool {
	peers := region.GetPeers()
	for _, peer := range peers {
		if _, ok := targetPeers[peer.GetStoreId()]; !ok {
			return false
		}
	}
	return region.GetLeader().GetStoreId() == targetLeader
}

// selectNewPeer return the new peer which pick the fewest picked count.
// it keeps the origin peer if the origin store's pick count is equal the fewest pick.
// it can be divided into three steps:
// 1. found the max pick count and the min pick count.
// 2. if max pick count equals min pick count, it means all store picked count are some, return the origin peer.
// 3. otherwise, select the store which pick count is the min pick count and pass all filter.
func (r *RegionScatterer) selectNewPeer(context engineContext, group string, peer *metapb.Peer, filters []filter.Filter) *metapb.Peer {
	newPeer, _ := r.selectNewPeerWithTrace(context, group, peer, filters)
	return newPeer
}

type peerSelectionTrace struct {
	maxStorePickedCount    uint64
	minStorePickedCount    uint64
	originStorePickedCount uint64
	candidates             []string
}

func (r *RegionScatterer) selectNewPeerWithTrace(context engineContext, group string, peer *metapb.Peer, filters []filter.Filter) (*metapb.Peer, peerSelectionTrace) {
	stores := r.cluster.GetStores()
	maxStoreTotalCount := uint64(0)
	minStoreTotalCount := uint64(math.MaxUint64)
	for _, store := range stores {
		count := context.selectedPeer.Get(store.GetID(), group)
		if count > maxStoreTotalCount {
			maxStoreTotalCount = count
		}
		if count < minStoreTotalCount {
			minStoreTotalCount = count
		}
	}

	var newPeer *metapb.Peer
	minCount := uint64(math.MaxUint64)
	originStorePickedCount := uint64(math.MaxUint64)
	trace := peerSelectionTrace{
		maxStorePickedCount: maxStoreTotalCount,
		minStorePickedCount: minStoreTotalCount,
		candidates:          make([]string, 0, len(stores)),
	}
	for _, store := range stores {
		storeCount := context.selectedPeer.Get(store.GetID(), group)
		if store.GetID() == peer.GetStoreId() {
			originStorePickedCount = storeCount
		}
		allowedByCount := storeCount < maxStoreTotalCount || maxStoreTotalCount == minStoreTotalCount
		filterReasons := make([]string, 0, len(filters))
		passedFilters := false
		// If storeCount is equal to the maxStoreTotalCount, we should skip this store as candidate.
		// If the storeCount are all the same for the whole cluster(maxStoreTotalCount == minStoreTotalCount), any store
		// could be selected as candidate.
		if allowedByCount {
			filterReasons = collectTargetFilterReasons(r.cluster.GetSharedConfig(), store, filters)
			passedFilters = len(filterReasons) == 0
			if passedFilters && storeCount < minCount {
				minCount = storeCount
				newPeer = &metapb.Peer{
					StoreId: store.GetID(),
					Role:    peer.GetRole(),
				}
			}
		}
		trace.candidates = append(trace.candidates, formatPeerCandidateTrace(store.GetID(), storeCount, allowedByCount, passedFilters, filterReasons, peer.GetStoreId(), newPeer))
	}
	trace.originStorePickedCount = originStorePickedCount
	if originStorePickedCount <= minCount {
		return peer, trace
	}
	if newPeer == nil {
		return peer, trace
	}
	return newPeer, trace
}

// selectAvailableLeaderStore select the target leader store from the candidates. The candidates would be collected by
// the existed peers store depended on the leader counts in the group level. Please use this func before scatter spacial engines.
func (r *RegionScatterer) selectAvailableLeaderStore(group string, region *core.RegionInfo,
	leaderCandidateStores []uint64, context engineContext) (leaderID uint64, leaderStorePickedCount uint64) {
	leaderID, leaderStorePickedCount, _ = r.selectAvailableLeaderStoreWithTrace(group, region, leaderCandidateStores, context)
	return leaderID, leaderStorePickedCount
}

func (r *RegionScatterer) selectAvailableLeaderStoreWithTrace(group string, region *core.RegionInfo,
	leaderCandidateStores []uint64, context engineContext) (leaderID uint64, leaderStorePickedCount uint64, trace []string) {
	sourceStore := r.cluster.GetStore(region.GetLeader().GetStoreId())
	if sourceStore == nil {
		log.Error("failed to get the store", zap.Uint64("store-id", region.GetLeader().GetStoreId()), errs.ZapError(errs.ErrGetSourceStore))
		return 0, 0, nil
	}
	minStoreGroupLeader := uint64(math.MaxUint64)
	id := uint64(0)
	trace = make([]string, 0, len(leaderCandidateStores))
	for _, storeID := range leaderCandidateStores {
		store := r.cluster.GetStore(storeID)
		if store == nil {
			trace = append(trace, fmt.Sprintf("store=%d,missing=true", storeID))
			continue
		}
		storeGroupLeaderCount := context.selectedLeader.Get(storeID, group)
		trace = append(trace, fmt.Sprintf("store=%d,picked=%d,selected=%t", storeID, storeGroupLeaderCount, id == 0 || minStoreGroupLeader > storeGroupLeaderCount))
		if minStoreGroupLeader > storeGroupLeaderCount {
			minStoreGroupLeader = storeGroupLeaderCount
			id = storeID
		}
	}
	return id, minStoreGroupLeader, trace
}

func collectTargetFilterReasons(conf config.SharedConfigProvider, store *core.StoreInfo, filters []filter.Filter) []string {
	reasons := make([]string, 0, len(filters))
	for _, f := range filters {
		status := f.Target(conf, store)
		if status.IsOK() {
			continue
		}
		reason := fmt.Sprintf("%s:%s", f.Scope(), status.String())
		if status.DetailedReason != "" {
			reason = reason + "(" + status.DetailedReason + ")"
		}
		reasons = append(reasons, reason)
	}
	return reasons
}

func formatPeerCandidateTrace(storeID, pickedCount uint64, allowedByCount, passedFilters bool, filterReasons []string, originStoreID uint64, selectedPeer *metapb.Peer) string {
	selected := selectedPeer != nil && selectedPeer.GetStoreId() == storeID
	parts := []string{
		fmt.Sprintf("store=%d", storeID),
		fmt.Sprintf("picked=%d", pickedCount),
		fmt.Sprintf("allowed-by-count=%t", allowedByCount),
		fmt.Sprintf("passed-filters=%t", passedFilters),
		fmt.Sprintf("origin=%t", storeID == originStoreID),
		fmt.Sprintf("selected=%t", selected),
	}
	if len(filterReasons) > 0 {
		parts = append(parts, "filter-reasons="+strings.Join(filterReasons, "|"))
	}
	return strings.Join(parts, ",")
}

func formatScatterTargetPeers(targetPeers map[uint64]*metapb.Peer) []string {
	peers := make([]string, 0, len(targetPeers))
	for storeID, peer := range targetPeers {
		peers = append(peers, fmt.Sprintf("store=%d,role=%s,witness=%t", storeID, peer.GetRole().String(), peer.GetIsWitness()))
	}
	sort.Strings(peers)
	return peers
}

func scatterPlacementPeers(region *core.RegionInfo) map[uint64]*metapb.Peer {
	peers := make(map[uint64]*metapb.Peer, len(region.GetPeers()))
	for _, peer := range region.GetPeers() {
		peers[peer.GetStoreId()] = peer
	}
	return peers
}

func (r *RegionScatterer) classifyPlacementStores(region *core.RegionInfo, targetPeers map[uint64]*metapb.Peer) ([]uint64, []uint64, map[string][]uint64, map[string][]uint64) {
	engineFilter := filter.NewEngineFilter(r.name, filter.NotSpecialEngines)
	ordinaryOldStores := make([]uint64, 0, len(region.GetPeers()))
	ordinaryNewStores := make([]uint64, 0, len(targetPeers))
	specialOldStores := make(map[string][]uint64)
	specialNewStores := make(map[string][]uint64)

	classifyStore := func(storeID uint64, ordinary *[]uint64, special map[string][]uint64) {
		store := r.cluster.GetStore(storeID)
		if store == nil {
			return
		}
		if engineFilter.Target(r.cluster.GetSharedConfig(), store).IsOK() {
			*ordinary = append(*ordinary, storeID)
			return
		}
		engine := store.GetLabelValue(core.EngineKey)
		special[engine] = append(special[engine], storeID)
	}

	for _, peer := range region.GetPeers() {
		classifyStore(peer.GetStoreId(), &ordinaryOldStores, specialOldStores)
	}
	for _, peer := range targetPeers {
		classifyStore(peer.GetStoreId(), &ordinaryNewStores, specialNewStores)
	}
	return ordinaryOldStores, ordinaryNewStores, specialOldStores, specialNewStores
}

func scatterPlacementAfterOperator(region *core.RegionInfo, op *operator.Operator) (map[uint64]*metapb.Peer, uint64) {
	targetPeers := make(map[uint64]*metapb.Peer, len(region.GetPeers()))
	for _, peer := range region.GetPeers() {
		targetPeers[peer.GetStoreId()] = peer
	}
	targetLeader := region.GetLeader().GetStoreId()
	if op == nil {
		return targetPeers, targetLeader
	}
	for i := range op.Len() {
		switch step := op.Step(i).(type) {
		case operator.TransferLeader:
			targetLeader = step.ToStore
		case operator.AddPeer:
			targetPeers[step.ToStore] = &metapb.Peer{StoreId: step.ToStore}
		case operator.AddLearner:
			targetPeers[step.ToStore] = &metapb.Peer{StoreId: step.ToStore}
		case operator.RemovePeer:
			delete(targetPeers, step.FromStore)
		}
	}
	return targetPeers, targetLeader
}

// Commit updates the group distribution after the scatter operator has been
// accepted by the operator controller.
func (r *RegionScatterer) Commit(region *core.RegionInfo, op *operator.Operator, group string) {
	if op == nil || region == nil || region.GetLeader() == nil {
		return
	}
	targetPeers, targetLeader := scatterPlacementAfterOperator(region, op)
	if op.Desc() == InternalScatterOperatorDesc {
		log.Info("internal scatter commit placement",
			zap.Uint64("region-id", region.GetID()),
			zap.String("group", group),
			zap.Strings("old-peers", formatScatterTargetPeers(scatterPlacementPeers(region))),
			zap.Uint64("old-leader", region.GetLeader().GetStoreId()),
			zap.Strings("new-peers", formatScatterTargetPeers(targetPeers)),
			zap.Uint64("new-leader", targetLeader))
	}
	r.Update(region, targetPeers, targetLeader, group)
}

// Update records the group distribution after scattering a region to the target placement.
func (r *RegionScatterer) Update(region *core.RegionInfo, targetPeers map[uint64]*metapb.Peer, targetLeader uint64, group string) {
	engineFilter := filter.NewEngineFilter(r.name, filter.NotSpecialEngines)
	ordinaryOldStores, ordinaryNewStores, specialOldStores, specialNewStores := r.classifyPlacementStores(region, targetPeers)
	for _, peer := range targetPeers {
		storeID := peer.GetStoreId()
		store := r.cluster.GetStore(storeID)
		if store == nil {
			continue
		}
		engine := core.EngineTiKV
		if !engineFilter.Target(r.cluster.GetSharedConfig(), store).IsOK() {
			engine = store.GetLabelValue(core.EngineKey)
		}
		scatterDistributionCounter.WithLabelValues(
			strconv.FormatUint(storeID, 10),
			strconv.FormatBool(false),
			engine).Inc()
	}

	r.ordinaryEngine.selectedPeer.Update(group, ordinaryOldStores, ordinaryNewStores)
	specialEngines := make(map[string]struct{}, len(specialOldStores)+len(specialNewStores))
	for engine := range specialOldStores {
		specialEngines[engine] = struct{}{}
	}
	for engine := range specialNewStores {
		specialEngines[engine] = struct{}{}
	}
	for engine := range specialEngines {
		ctx := r.getOrCreateSpecialEngineContext(engine)
		ctx.selectedPeer.Update(group, specialOldStores[engine], specialNewStores[engine])
	}

	oldLeaderStoreID := region.GetLeader().GetStoreId()
	r.ordinaryEngine.selectedLeader.Update(group, []uint64{oldLeaderStoreID}, []uint64{targetLeader})
	scatterDistributionCounter.WithLabelValues(
		strconv.FormatUint(targetLeader, 10),
		strconv.FormatBool(true),
		core.EngineTiKV).Inc()
}
