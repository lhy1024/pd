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
	"context"
	"encoding/json"
	"maps"
	"slices"
	"strings"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const (
	// labelKey is the key for affinity group id in region label.
	labelKey = "affinity_group"
	// AffinityLabelKey is the reserved region-label key used by affinity ranges.
	AffinityLabelKey = labelKey
	// defaultDegradedExpirationSeconds is the default expiration time for a degraded group.
	// After a group becomes degraded , it will automatically
	// expire after this duration if the stores don't recover.
	defaultDegradedExpirationSeconds = 600 // 10 minutes
)

type regionCache struct {
	region      *core.RegionInfo
	groupInfo   *runtimeGroupInfo
	affinityVer uint64
	isAffinity  bool
}

// Manager is the manager of all affinity information.
type Manager struct {
	// RWMutex protects only in-memory data. (Does not include auxiliary data needed for meta updates, such as keyRanges)
	// It can be acquired on its own or while already holding metaMutex,
	// but metaMutex must never be acquired while RWMutex is held to avoid deadlocks.
	syncutil.RWMutex
	// metaMutex protects both in-memory and storage metadata.
	// Metadata updates typically hold metaMutex for the whole operation,
	// and may acquire RWMutex only for the final in-memory update.
	// The only strict rule is that metaMutex must not be taken inside RWMutex.
	metaMutex syncutil.Mutex

	ctx              context.Context
	storage          endpoint.AffinityStorage
	storeSetInformer core.StoreSetInformer
	conf             config.SharedConfigProvider
	regionLabeler    *labeler.RegionLabeler // region labeler for syncing key ranges

	// The following members are protected by RWMutex.
	affinityRegionCount     int
	groups                  map[string]*runtimeGroupInfo    // {group_id} -> runtimeGroupInfo
	regions                 map[uint64]regionCache          // {region_id} -> regionCache
	unavailableStores       map[uint64]storeCondition       // {store_id} -> storeCondition
	metadataTables          map[TableID]*metadataTableState // {table_id} -> metadata generations
	metadataGeneration      uint64
	balanceRuleRevision     func() uint64
	affinityWriteTerm       func() ([]clientv3.Cmp, bool)
	balanceStoreRevision    uint64
	balanceStoreFingerprint uint64

	// The following members are protected by metaMutex only, not protected by RWMutex.
	// keyRanges cached in memory to reduce labeler lock contention
	keyRanges map[string]GroupKeyRanges // {group_id} -> key ranges
	// affinityRangeVersion is the last globally fenced range-write version
	// observed by this Manager. It is protected by metaMutex and is compared
	// before every range mutation.
	affinityRangeVersion uint64
	// labelRuleBuffer is a buffer used during etcd synchronization.
	// When synchronizing via etcd, LabelRule information may arrive earlier than group information.
	labelRuleBuffer map[string]*labeler.LabelRule // {group_id} -> labelRule
	// metadataNeedsReload is set when a write's local baseline may be stale.
	// The next explicit write refreshes the authoritative baseline before
	// validating its request.
	metadataNeedsReload bool
}

// NewManager creates a new affinity Manager.
func NewManager(ctx context.Context, storage endpoint.AffinityStorage, storeSetInformer core.StoreSetInformer, conf config.SharedConfigProvider, regionLabeler *labeler.RegionLabeler) (*Manager, error) {
	if regionLabeler == nil {
		return nil, errs.ErrAffinityInternal
	}
	m := &Manager{
		ctx:                 ctx,
		storage:             storage,
		storeSetInformer:    storeSetInformer,
		conf:                conf,
		regionLabeler:       regionLabeler,
		affinityRegionCount: 0,
		groups:              make(map[string]*runtimeGroupInfo),
		regions:             make(map[uint64]regionCache),
		unavailableStores:   make(map[uint64]storeCondition),
		metadataTables:      make(map[TableID]*metadataTableState),
		keyRanges:           make(map[string]GroupKeyRanges),
		labelRuleBuffer:     make(map[string]*labeler.LabelRule),
	}
	if err := m.initialize(); err != nil {
		return nil, err
	}
	return m, nil
}

// initialize loads affinity groups from storage and rebuilds the group-label mapping.
func (m *Manager) initialize() error {
	m.metaMutex.Lock()
	defer m.metaMutex.Unlock()
	if err := m.reloadAffinityMetadataLocked(m.ctx); err != nil {
		return err
	}

	m.startAvailabilityCheckLoop()
	log.Info("affinity manager initialized", zap.Int("group-count", len(m.groups)))
	return nil
}

// refreshMetadataBeforeWriteLocked refreshes a stale authoritative baseline.
// The caller must hold metaMutex and keep it through validation and commit.
func (m *Manager) refreshMetadataBeforeWriteLocked() error {
	if !m.metadataNeedsReload {
		return nil
	}
	m.metadataNeedsReload = false
	if err := m.reloadAffinityMetadataUnconditionallyLocked(m.ctx); err != nil {
		m.metadataNeedsReload = true
		return err
	}
	return nil
}

// loadAffinityLabelRule reads one affinity label from the authoritative
// storage. AffinityStorage deliberately exposes only the affinity operations;
// production storage also implements RuleStorage, while the fallback keeps
// lightweight test/storage implementations compatible.
func (m *Manager) loadAffinityLabelRule(ruleID string) (string, error) {
	if loader, ok := m.storage.(interface {
		LoadRegionRule(string) (string, error)
	}); ok {
		return loader.LoadRegionRule(ruleID)
	}
	var value string
	err := m.storage.LoadAllAffinityLabels(func(key, candidate string) {
		if key == ruleID || strings.HasSuffix(key, "/"+ruleID) {
			value = candidate
		}
	})
	return value, err
}

// IsAvailable checks that the Manager contains at least one Group.
func (m *Manager) IsAvailable() bool {
	m.RLock()
	defer m.RUnlock()
	return len(m.groups) > 0
}

func (m *Manager) initGroupLocked(group *Group) {
	if _, ok := m.groups[group.ID]; ok {
		log.Error("group already initialized", zap.String("group-id", group.ID))
		return
	}
	m.groups[group.ID] = &runtimeGroupInfo{
		Group: Group{
			ID:              group.ID,
			CreateTimestamp: group.CreateTimestamp,
			LeaderStoreID:   group.LeaderStoreID,
			VoterStoreIDs:   normalizeVoters(group.VoterStoreIDs),
			BalancePolicy:   group.BalancePolicy,
		},
		availability:        groupDegraded,
		degradedExpiredAt:   newDegradedExpiredAtFromNow(),
		AffinityVer:         1,
		AffinityRegionCount: 0,
		Regions:             make(map[uint64]regionCache),
		LabelRule:           nil,
		RangeCount:          0,
	}
	m.invalidateMetadataLocked(group.ID)
}

func (m *Manager) noGroupsExist(groups []*Group) error {
	m.RLock()
	defer m.RUnlock()
	for _, group := range groups {
		if _, ok := m.groups[group.ID]; ok {
			return errs.ErrAffinityGroupExist.GenWithStackByArgs(group.ID)
		}
	}
	return nil
}

func (m *Manager) allGroupsExist(groupIDs []string) error {
	m.RLock()
	defer m.RUnlock()
	for _, groupID := range groupIDs {
		if _, ok := m.groups[groupID]; !ok {
			return errs.ErrAffinityGroupNotFound.GenWithStackByArgs(groupID)
		}
	}
	return nil
}

func (m *Manager) createGroups(groups []*Group, labelRules []*labeler.LabelRule) {
	m.Lock()
	defer m.Unlock()
	for i, group := range groups {
		m.initGroupLocked(group)
		m.updateGroupLabelRuleLocked(group.ID, labelRules[i], false)
	}
}

func (m *Manager) resetCountLocked(groupInfo *runtimeGroupInfo) {
	m.affinityRegionCount -= groupInfo.AffinityRegionCount
	groupInfo.AffinityRegionCount = 0
	groupInfo.AffinityVer++
	m.invalidateMetadataLocked(groupInfo.ID)
}

func (m *Manager) updateGroupTarget(group Group) (*GroupState, error) {
	m.Lock()
	defer m.Unlock()

	groupInfo, ok := m.groups[group.ID]
	if !ok {
		return nil, errs.ErrAffinityGroupNotFound.GenWithStackByArgs(group.ID)
	}

	groupInfo.SetAvailability(groupAvailable)
	groupInfo.Group = group
	groupInfo.VoterStoreIDs = slices.Clone(group.VoterStoreIDs)
	m.resetCountLocked(groupInfo)

	return newGroupState(groupInfo), nil
}

func (m *Manager) updateGroupAvailabilityLocked(groupID string, availability groupAvailability) {
	groupInfo, ok := m.groups[groupID]
	if !ok {
		return
	}
	groupInfo.SetAvailability(availability)
	m.resetCountLocked(groupInfo)
}

// ExpireAffinityGroup changes the Group availability to groupExpired.
func (m *Manager) ExpireAffinityGroup(groupID string) {
	m.Lock()
	defer m.Unlock()
	m.updateGroupAvailabilityLocked(groupID, groupExpired)
}

// DegradeAffinityGroup changes the Group availability to groupDegraded.
func (m *Manager) DegradeAffinityGroup(groupID string) {
	m.Lock()
	defer m.Unlock()
	m.updateGroupAvailabilityLocked(groupID, groupDegraded)
}

// RestoreAffinityGroup changes the Group availability to groupAvailable.
func (m *Manager) RestoreAffinityGroup(groupID string) {
	m.Lock()
	defer m.Unlock()
	m.updateGroupAvailabilityLocked(groupID, groupAvailable)
}

func (m *Manager) updateGroupLabelRuleLocked(groupID string, labelRule *labeler.LabelRule, needClear bool) {
	rangeCount := 0
	if labelRule != nil {
		if ranges, ok := labelRule.Data.([]*labeler.KeyRangeRule); ok {
			rangeCount = len(ranges)
		}
	}
	m.updateGroupLabelRuleLockedWithCount(groupID, labelRule, rangeCount, needClear)
}

func (m *Manager) updateGroupLabelRuleLockedWithCount(groupID string, labelRule *labeler.LabelRule, rangeCount int, needClear bool) {
	groupInfo, ok := m.groups[groupID]
	if !ok {
		log.Error("group not initialized", zap.String("group-id", groupID))
		return
	}
	if needClear {
		m.clearGroupCacheLocked(groupID)
	} else {
		m.resetCountLocked(groupInfo)
	}
	// Set LabelRule
	groupInfo.LabelRule = labelRule
	groupInfo.RangeCount = rangeCount
	if state := m.metadataTables[TableIDForGroup(groupID)]; state != nil {
		m.metadataGeneration++
		state.version = m.metadataGeneration
	}
}

func (m *Manager) updateGroupLabelRules(labels map[string]*labeler.LabelRule, needClear bool) {
	m.Lock()
	defer m.Unlock()
	for groupID, labelRule := range labels {
		m.updateGroupLabelRuleLocked(groupID, labelRule, needClear)
	}
}

func (m *Manager) clearGroupCacheLocked(groupID string) {
	groupInfo, ok := m.groups[groupID]
	if !ok {
		return
	}

	m.resetCountLocked(groupInfo)
	for regionID := range groupInfo.Regions {
		delete(m.regions, regionID)
	}
	groupInfo.Regions = make(map[uint64]regionCache)
}

func (m *Manager) deleteGroupLocked(groupID string) {
	m.clearGroupCacheLocked(groupID)
	delete(m.groups, groupID)
	if state := m.metadataTables[TableIDForGroup(groupID)]; state != nil {
		delete(state.groups, groupID)
		if len(state.groups) == 0 {
			delete(m.metadataTables, TableIDForGroup(groupID))
		}
	}
}

func (m *Manager) deleteGroups(groupIDs []string) {
	m.Lock()
	defer m.Unlock()
	for _, groupID := range groupIDs {
		m.deleteGroupLocked(groupID)
	}
}

func (m *Manager) deleteCacheLocked(regionID uint64) {
	cache, ok := m.regions[regionID]
	if !ok {
		return
	}
	if cache.isAffinity && cache.affinityVer == cache.groupInfo.AffinityVer {
		cache.groupInfo.AffinityRegionCount--
		m.affinityRegionCount--
	}
	delete(m.regions, regionID)
	delete(cache.groupInfo.Regions, regionID)
}

func (m *Manager) saveCache(cache *regionCache) {
	regionID := cache.region.GetID()
	m.Lock()
	defer m.Unlock()
	// If the Group has changed, update it but do not save it afterward.
	groupInfo, ok := m.groups[cache.groupInfo.ID]
	if ok && groupInfo == cache.groupInfo && groupInfo.AffinityVer == cache.affinityVer {
		old, exists := m.regions[regionID]
		if !exists || balanceRegionChanged(old.region, cache.region) {
			m.invalidateMetadataLocked(groupInfo.ID)
		}
		m.deleteCacheLocked(regionID)
		m.regions[regionID] = *cache
		groupInfo.Regions[regionID] = *cache
		if cache.isAffinity {
			m.affinityRegionCount++
			groupInfo.AffinityRegionCount++
		}
	}
}

// InvalidCache invalidates the cache of the corresponding Region in the manager by its Region ID.
// Since cache misses for Region are more likely when InvalidCache is called, check for existence under a read lock first.
func (m *Manager) InvalidCache(regionID uint64) {
	m.RLock()
	_, ok := m.regions[regionID]
	m.RUnlock()
	if !ok {
		return
	}

	m.Lock()
	defer m.Unlock()
	if cached, ok := m.regions[regionID]; ok {
		m.invalidateMetadataLocked(cached.groupInfo.ID)
	}
	m.deleteCacheLocked(regionID)
}

func (m *Manager) getCache(region *core.RegionInfo) (*regionCache, *GroupState) {
	m.RLock()
	defer m.RUnlock()
	cache, ok := m.regions[region.GetID()]
	if ok {
		return &cache, newGroupState(cache.groupInfo)
	}
	return nil, nil
}

// GetRegionAffinityGroupState returns the affinity group state and isAffinity for a region.
// This is a read-only operation that does not modify cache and we use this for temporary check
func (m *Manager) GetRegionAffinityGroupState(region *core.RegionInfo) (group *GroupState, isAffinity bool) {
	return m.calcRegionAffinityGroupState(region, false)
}

// GetAndCacheRegionAffinityGroupState returns the affinity group state and saves it to cache.
// The caller must call InvalidCache() when the region is deleted or merged.
// Currently only used by AffinityChecker.Check.
func (m *Manager) GetAndCacheRegionAffinityGroupState(region *core.RegionInfo) (group *GroupState, isAffinity bool) {
	return m.calcRegionAffinityGroupState(region, true)
}

func (m *Manager) calcRegionAffinityGroupState(region *core.RegionInfo, saveCache bool) (group *GroupState, isAffinity bool) {
	if region == nil || !m.IsAvailable() {
		return nil, false
	}
	var cache *regionCache
	cache, group = m.getCache(region)
	if cache == nil || group == nil || cache.affinityVer != group.affinityVer || region != cache.region {
		groupID := m.regionLabeler.GetRegionLabel(region, labelKey)
		group = m.GetAffinityGroupState(groupID)
		if group == nil {
			return nil, false
		}
		cache = &regionCache{
			region:      region,
			groupInfo:   group.groupInfoPtr,
			affinityVer: group.affinityVer,
			isAffinity:  group.isRegionAffinity(region),
		}
		if saveCache {
			m.saveCache(cache)
		}
	}

	return group, cache.isAffinity
}

// IsGroupExist checks if a group exists.
func (m *Manager) IsGroupExist(id string) bool {
	m.RLock()
	defer m.RUnlock()
	_, ok := m.groups[id]
	return ok
}

// GetAffinityGroupState gets the runtime state of an affinity group.
func (m *Manager) GetAffinityGroupState(id string) *GroupState {
	if id == "" {
		return nil
	}
	m.RLock()
	defer m.RUnlock()
	groupInfo, ok := m.groups[id]
	if ok {
		return newGroupState(groupInfo)
	}
	return nil
}

// CheckAndGetAffinityGroupState returns the group state after validating the ID and presence.
func (m *Manager) CheckAndGetAffinityGroupState(groupID string) (*GroupState, error) {
	if err := ValidateGroupID(groupID); err != nil {
		return nil, err
	}
	state := m.GetAffinityGroupState(groupID)
	if state == nil {
		return nil, errs.ErrAffinityGroupNotFound.GenWithStackByArgs(groupID)
	}
	return state, nil
}

// GetAffinityGroupCount returns the number of affinity groups.
func (m *Manager) GetAffinityGroupCount() int {
	m.RLock()
	defer m.RUnlock()
	return len(m.groups)
}

// GetAllAffinityGroupStates returns all affinity groups.
func (m *Manager) GetAllAffinityGroupStates() []*GroupState {
	m.RLock()
	defer m.RUnlock()
	result := make([]*GroupState, 0, len(m.groups))
	for _, groupInfo := range m.groups {
		result = append(result, newGroupState(groupInfo))
	}
	return result
}

// These methods are called by the watcher in microservice.
// They are called by the watcher when it receives events from etcd.

// SyncGroupFromEtcd synchronizes a group from etcd to the in-memory state.
func (m *Manager) SyncGroupFromEtcd(group *Group) error {
	m.metaMutex.Lock()
	defer m.metaMutex.Unlock()
	return m.syncGroupFromEtcdLocked(group)
}

func (m *Manager) syncGroupFromEtcdLocked(group *Group) error {
	if group == nil {
		return nil
	}
	if err := validateBalancePolicy(group.BalancePolicy); err != nil {
		log.Warn("invalid affinity group balance policy from etcd, skipping",
			zap.String("group-id", group.ID),
			zap.Error(err))
		return nil
	}
	// Voter order has no meaning, but affinity comparisons use canonical order.
	clonedGroup := *group
	clonedGroup.VoterStoreIDs = normalizeVoters(group.VoterStoreIDs)
	group = &clonedGroup

	// Fast path: avoid taking write lock if nothing changes.
	m.RLock()
	existingGroup, exists := m.groups[group.ID]
	if exists &&
		existingGroup.LeaderStoreID == group.LeaderStoreID &&
		slices.Equal(existingGroup.VoterStoreIDs, group.VoterStoreIDs) &&
		existingGroup.BalancePolicy == group.BalancePolicy &&
		existingGroup.CreateTimestamp == group.CreateTimestamp {
		m.RUnlock()
		return nil
	}
	m.RUnlock()

	// If the group is newly created, try to attach existing label rule without holding the write lock.
	var (
		labelRule *labeler.LabelRule
		gkr       GroupKeyRanges
		labelErr  error
	)
	if !exists {
		labelRule = m.regionLabeler.GetLabelRule(GetLabelRuleID(group.ID))

		// If regionLabeler has not been synchronized but SyncKeyRangesFromEtcd has already run, read from the buffer.
		if labelRule == nil {
			labelRule = m.labelRuleBuffer[group.ID]
		}

		if labelRule != nil {
			gkr, labelErr = extractKeyRangesFromLabelRule(labelRule)
		}
	}

	m.Lock()
	defer m.Unlock()
	// Check if the group exists again to avoid other threads modifying the group.
	groupInfo, exists := m.groups[group.ID]
	if !exists {
		m.initGroupLocked(group)
		if labelRule != nil {
			if labelErr != nil {
				log.Warn("failed to attach existing label rule to new affinity group",
					zap.String("group-id", group.ID),
					zap.Error(labelErr))
				delete(m.labelRuleBuffer, group.ID)
				return nil
			}
			// Attach only non-empty label rules to avoid marking the group as having ranges when it does not.
			if len(gkr.KeyRanges) > 0 {
				// Pass the rangeCount directly instead of letting updateGroupLabelRuleLocked calculate it
				// because labelRule.Data might be []any (from watcher) instead of []*labeler.KeyRangeRule
				m.keyRanges[group.ID] = gkr
				m.updateGroupLabelRuleLockedWithCount(group.ID, labelRule, len(gkr.KeyRanges), false)
			}
		}
		// Once group created, the buffer must be deleted.
		delete(m.labelRuleBuffer, group.ID)
	} else {
		changed := false
		if groupInfo.LeaderStoreID != group.LeaderStoreID {
			groupInfo.LeaderStoreID = group.LeaderStoreID
			changed = true
		}
		if !slices.Equal(groupInfo.VoterStoreIDs, group.VoterStoreIDs) {
			groupInfo.VoterStoreIDs = slices.Clone(group.VoterStoreIDs)
			changed = true
		}
		if groupInfo.BalancePolicy != group.BalancePolicy {
			groupInfo.BalancePolicy = group.BalancePolicy
			// Policy changes invalidate planning inputs, not Region affinity.
			m.invalidateMetadataLocked(group.ID)
		}
		if groupInfo.CreateTimestamp != group.CreateTimestamp {
			groupInfo.CreateTimestamp = group.CreateTimestamp
			changed = true
		}

		if changed {
			m.resetCountLocked(groupInfo)
		}
	}
	return nil
}

// ReloadAffinityMetadataFromStorage replaces the local group/range view with
// an authoritative snapshot before this process takes over scheduling. It
// intentionally drops cached Region associations; the checker will rebuild
// them from current Region state after the new primary starts.
func (m *Manager) ReloadAffinityMetadataFromStorage() error {
	m.metaMutex.Lock()
	defer m.metaMutex.Unlock()
	conditions, err := m.captureAffinityWriteConditions()
	if err != nil {
		return err
	}
	return m.reloadAffinityMetadataWithConditions(m.ctx, conditions)
}

// reloadAffinityMetadataLocked checks the captured authority before publishing
// the loaded group/range metadata.
func (m *Manager) reloadAffinityMetadataLocked(ctx context.Context) error {
	conditions, err := m.captureAffinityWriteConditions()
	if err != nil {
		return err
	}
	return m.reloadAffinityMetadataWithConditions(ctx, conditions)
}

// reloadAffinityMetadataUnconditionallyLocked refreshes the local metadata
// after an ambiguous write. This read does not authorize a write and is not
// rejected merely because the caller's primary term is stale. The group and
// label prefixes are loaded separately, without a cross-prefix revision fence.
func (m *Manager) reloadAffinityMetadataUnconditionallyLocked(ctx context.Context) error {
	return m.reloadAffinityMetadataWithConditions(ctx, nil)
}

// reloadAffinityMetadataWithConditions stages and publishes one authoritative
// affinity snapshot under the supplied authority conditions. Takeover passes
// the new primary conditions; ordinary initialization captures the current
// primary conditions through reloadAffinityMetadataLocked.
func (m *Manager) reloadAffinityMetadataWithConditions(ctx context.Context, conditions []clientv3.Cmp) error {
	loaded := make(map[string]Group)
	rules := make(map[string]*labeler.LabelRule)
	ranges := make(map[string]GroupKeyRanges)
	var rangeVersion uint64
	plan := m.regionLabeler.NewPlan()
	for _, rule := range m.regionLabeler.GetAllLabelRules() {
		if strings.HasPrefix(rule.ID, LabelRuleIDPrefix) {
			if err := plan.DeleteLabelRule(rule.ID); err != nil {
				return err
			}
		}
	}
	err := m.runAffinityTxn(ctx, conditions, func(txn kv.Txn) error {
		var err error
		rangeVersion, err = loadAffinityRangeVersion(txn)
		if err != nil {
			return err
		}
		var parseErr error
		if err := m.storage.LoadAllAffinityGroups(func(_, value string) {
			if parseErr != nil {
				return
			}
			var group Group
			if parseErr = json.Unmarshal([]byte(value), &group); parseErr != nil {
				return
			}
			// Preserve the load/watch contract: invalid policies are isolated.
			if validateBalancePolicy(group.BalancePolicy) != nil {
				return
			}
			if parseErr = ValidateGroupID(group.ID); parseErr == nil {
				loaded[group.ID] = group
			}
		}); err != nil {
			return err
		}
		if parseErr != nil {
			return parseErr
		}
		if err := m.storage.LoadAllAffinityLabels(func(_, value string) {
			if parseErr != nil {
				return
			}
			rule := &labeler.LabelRule{}
			parseErr = json.Unmarshal([]byte(value), rule)
			if parseErr != nil {
				return
			}
			if parseErr = plan.SetLabelRule(rule); parseErr != nil {
				return
			}
			id, ok := parseAffinityGroupIDFromLabelRule(rule)
			if !ok {
				parseErr = errs.ErrAffinityGroupContent.FastGenByArgs("invalid affinity label identity")
				return
			}
			var gkr GroupKeyRanges
			gkr, parseErr = extractKeyRangesFromLabelRule(rule)
			if parseErr != nil {
				return
			}
			rules[id] = rule
			if _, exists := loaded[id]; exists && len(gkr.KeyRanges) > 0 {
				ranges[id] = gkr
			}
		}); err != nil {
			return err
		}
		return parseErr
	})
	if err != nil {
		return err
	}
	if err := validateNoOverlap(slices.Collect(maps.Values(ranges))); err != nil {
		return err
	}
	// All fallible work is complete. Publish the full affinity view, preserving
	// ordinary labels. The prefixes were read separately, not at one revision.
	plan.Apply()
	m.Lock()
	defer m.Unlock()
	m.affinityRangeVersion = rangeVersion
	m.groups = make(map[string]*runtimeGroupInfo, len(loaded))
	m.regions = make(map[uint64]regionCache)
	m.metadataTables = make(map[TableID]*metadataTableState)
	m.affinityRegionCount = 0
	m.keyRanges = ranges
	m.labelRuleBuffer = make(map[string]*labeler.LabelRule)
	for _, group := range loaded {
		m.initGroupLocked(&group)
		if rule := rules[group.ID]; rule != nil {
			m.updateGroupLabelRuleLocked(group.ID, rule, false)
		}
	}
	return nil
}

// SyncGroupDeleteFromEtcd synchronizes a group deletion from etcd.
func (m *Manager) SyncGroupDeleteFromEtcd(groupID string) error {
	m.metaMutex.Lock()
	defer m.metaMutex.Unlock()
	return m.syncGroupDeleteFromEtcdLocked(groupID)
}

func (m *Manager) syncGroupDeleteFromEtcdLocked(groupID string) error {
	// A delayed delete event must not remove a group that was recreated under
	// the same ID after the event's revision. Read the current authoritative
	// value while holding the same metadata lock used by range validation.
	value, err := m.storage.LoadAffinityGroup(groupID)
	if err != nil {
		return err
	}
	if value != "" {
		return nil
	}
	delete(m.keyRanges, groupID)
	delete(m.labelRuleBuffer, groupID)

	m.Lock()
	defer m.Unlock()
	if _, exists := m.groups[groupID]; !exists {
		log.Warn("affinity group not found during etcd sync delete", zap.String("group-id", groupID))
		return nil
	}

	m.deleteGroupLocked(groupID)
	return nil
}

// SyncKeyRangesFromEtcd synchronizes key ranges from a label rule.
func (m *Manager) SyncKeyRangesFromEtcd(labelRule *labeler.LabelRule) error {
	m.metaMutex.Lock()
	defer m.metaMutex.Unlock()
	return m.syncKeyRangesFromEtcdLocked(labelRule)
}

// SyncKeyRangesFromEtcdEvent applies a watcher event only when its value still
// matches storage. This prevents a delayed old event from replacing a newer
// local API range view used by cross-group overlap validation.
func (m *Manager) SyncKeyRangesFromEtcdEvent(labelRule *labeler.LabelRule) error {
	m.metaMutex.Lock()
	defer m.metaMutex.Unlock()
	// A watcher event can be delayed after a local API commit. Read the
	// authoritative label before publishing it; an old event must never replace
	// the range view used by the next cross-group overlap check.
	if labelRule != nil {
		value, err := m.loadAffinityLabelRule(labelRule.ID)
		if err != nil {
			return err
		}
		if value == "" {
			return nil
		}
		current, err := labeler.NewLabelRuleFromJSON([]byte(value))
		if err != nil {
			return err
		}
		currentRanges, err := extractKeyRangesFromLabelRule(current)
		if err != nil {
			return err
		}
		eventRanges, err := extractKeyRangesFromLabelRule(labelRule)
		if err != nil {
			return err
		}
		if !affinityRangesEqual(currentRanges, eventRanges) {
			return nil
		}
	}
	return m.syncKeyRangesFromEtcdLocked(labelRule)
}

func (m *Manager) syncKeyRangesFromEtcdLocked(labelRule *labeler.LabelRule) error {
	groupID, isAffinity := parseAffinityGroupIDFromLabelRule(labelRule)
	if !isAffinity {
		return nil
	}
	// Extract key ranges from label rule
	gkr, err := extractKeyRangesFromLabelRule(labelRule)
	if err != nil {
		log.Error("failed to extract key ranges from label rule during sync",
			zap.String("rule-id", labelRule.ID),
			zap.Error(err))
		return err
	}
	m.regionLabeler.ApplyLabelRule(labelRule.ID, labelRule)
	m.Lock()
	defer m.Unlock()

	// Only buffer non-empty rules
	if _, exists := m.groups[groupID]; !exists {
		if len(gkr.KeyRanges) == 0 {
			delete(m.labelRuleBuffer, groupID)
			return nil
		}
		// Store LabelRule information in the buffer when it is synchronized before the group.
		m.labelRuleBuffer[groupID] = labelRule
		return nil
	}
	// Once group created, the buffer must be deleted.
	delete(m.labelRuleBuffer, groupID)

	// Keep cache in sync: store when ranges exist, clear when they do not.
	if len(gkr.KeyRanges) > 0 {
		m.keyRanges[groupID] = gkr
	} else {
		delete(m.keyRanges, groupID)
	}

	// Pass the rangeCount directly instead of letting updateGroupLabelRuleLocked calculate it
	// because labelRule.Data might be []any (from watcher) instead of []*labeler.KeyRangeRule
	m.updateGroupLabelRuleLockedWithCount(groupID, labelRule, len(gkr.KeyRanges), false)

	return nil
}

// SyncKeyRangesDeleteFromEtcd synchronizes key ranges deletion.
func (m *Manager) SyncKeyRangesDeleteFromEtcd(ruleID string) error {
	m.metaMutex.Lock()
	defer m.metaMutex.Unlock()
	return m.syncKeyRangesDeleteFromEtcdLocked(ruleID)
}

// SyncKeyRangesDeleteFromEtcdEvent ignores a delayed delete when the rule has
// already been recreated or replaced by a newer API write.
func (m *Manager) SyncKeyRangesDeleteFromEtcdEvent(ruleID string) error {
	m.metaMutex.Lock()
	defer m.metaMutex.Unlock()
	// Ignore a delayed delete event when the rule has already been recreated or
	// replaced by a newer API write.
	if value, err := m.loadAffinityLabelRule(ruleID); err != nil {
		return err
	} else if value != "" {
		return nil
	}
	return m.syncKeyRangesDeleteFromEtcdLocked(ruleID)
}

func (m *Manager) syncKeyRangesDeleteFromEtcdLocked(ruleID string) error {
	groupID := strings.TrimPrefix(ruleID, LabelRuleIDPrefix)
	if groupID == "" || groupID == ruleID {
		return nil
	}
	// Clear keyRanges cache (needs metaMutex)
	m.regionLabeler.ApplyLabelRule(ruleID, nil)

	delete(m.keyRanges, groupID)
	delete(m.labelRuleBuffer, groupID)

	// Update group label rule (needs RWMutex)
	m.Lock()
	defer m.Unlock()
	if _, exists := m.groups[groupID]; exists {
		m.updateGroupLabelRuleLocked(groupID, nil, true)
	}
	return nil
}
