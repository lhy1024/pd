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
	"context"
	"regexp"
	"slices"
	"strings"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

// metadataTableState is shared management state, independent of any planner.
type metadataTableState struct {
	version uint64
	groups  map[string]struct{}
}

func (m *Manager) invalidateMetadataLocked(groupID string) {
	tableID := TableIDForGroup(groupID)
	state := m.metadataTables[tableID]
	if state == nil {
		state = &metadataTableState{groups: make(map[string]struct{})}
		m.metadataTables[tableID] = state
	}
	state.groups[groupID] = struct{}{}
	// Generations cannot be reused when a deleted table is recreated.
	m.metadataGeneration++
	state.version = m.metadataGeneration
}

func affinityRangesEqual(a, b GroupKeyRanges) bool {
	return a.GroupID == b.GroupID && slices.EqualFunc(a.KeyRanges, b.KeyRanges, func(a, b keyutil.KeyRange) bool {
		return bytes.Equal(a.StartKey, b.StartKey) && bytes.Equal(a.EndKey, b.EndKey)
	})
}

var partitionGroupIDRegexp = regexp.MustCompile(`^_tidb_pt_[0-9]+_p[0-9]+$`)

type groupTargetUpdate struct {
	original Group
	target   Group
	ranges   GroupKeyRanges
}

// TableID identifies an internal metadata table, separate from user group IDs.
type TableID string

// TableIDForGroup groups numeric TiDB partitions by table. Every custom group
// has a separate identity, even when its ID looks like a partition table prefix.
// Convert a group ID once; APIs accepting TableID never parse it as a group ID.
func TableIDForGroup(groupID string) TableID {
	if partitionGroupIDRegexp.MatchString(groupID) {
		return TableID("partition:" + groupID[:strings.LastIndex(groupID, "_p")])
	}
	return TableID("group:" + groupID)
}

// SetAffinityWriteGuard installs the authority provider for all metadata writes.
// An operation captures its term before validation and never renews it in flight.
func (m *Manager) SetAffinityWriteGuard(term func() ([]clientv3.Cmp, bool)) {
	m.Lock()
	defer m.Unlock()
	m.affinityWriteTerm = term
}

func checkOriginalGroup(txn kv.Txn, original *Group) error {
	persisted, err := loadAffinityGroupInTxn(txn, original.ID)
	if err != nil {
		return err
	}
	if persisted == nil || persisted.ID != original.ID || persisted.CreateTimestamp != original.CreateTimestamp ||
		persisted.BalancePolicy != original.BalancePolicy || persisted.LeaderStoreID != original.LeaderStoreID ||
		!slices.Equal(normalizeVoters(persisted.VoterStoreIDs), original.VoterStoreIDs) {
		return affinityConflict()
	}
	return nil
}

func affinityConflict() error { return errs.ErrEtcdTxnConflict.FastGenByArgs() }

func cloneGroupTargetUpdate(change groupTargetUpdate) groupTargetUpdate {
	change.original.VoterStoreIDs = normalizeVoters(change.original.VoterStoreIDs)
	change.target.VoterStoreIDs = normalizeVoters(change.target.VoterStoreIDs)
	change.ranges.KeyRanges = slices.Clone(change.ranges.KeyRanges)
	return change
}

func sameGroupTarget(a, b Group) bool {
	return a.LeaderStoreID == b.LeaderStoreID && slices.Equal(a.VoterStoreIDs, b.VoterStoreIDs)
}

func normalizeGroupTargetUpdates(changes []groupTargetUpdate) ([]groupTargetUpdate, error) {
	updates := make([]groupTargetUpdate, 0, len(changes))
	seen := make(map[string]struct{}, len(changes))
	for _, change := range changes {
		if err := ValidateGroupID(change.original.ID); err != nil {
			return nil, err
		}
		if change.original.ID != change.target.ID || change.ranges.GroupID != change.original.ID ||
			change.target.CreateTimestamp != change.original.CreateTimestamp ||
			change.target.BalancePolicy != change.original.BalancePolicy {
			return nil, errs.ErrAffinityGroupContent.GenWithStackByArgs("invalid target update for group " + change.original.ID)
		}
		if _, ok := seen[change.original.ID]; ok {
			return nil, errs.ErrAffinityGroupContent.GenWithStackByArgs("duplicate target update for group " + change.original.ID)
		}
		seen[change.original.ID] = struct{}{}
		change = cloneGroupTargetUpdate(change)
		updates = append(updates, change)
	}
	return updates, nil
}

// commitGroupTarget is kept for callers that produce one target at a time.

//nolint:unused // consumed by planner integration built on PR2A.
func (m *Manager) commitGroupTarget(ctx context.Context, original, target Group, ranges GroupKeyRanges, conditions []clientv3.Cmp, current func() bool) (bool, error) {
	return m.commitGroupTargets(ctx, []groupTargetUpdate{{original: original, target: target, ranges: ranges}}, conditions, current)
}

// commitGroupTargets commits one baseline-to-final target per changed group.
// The caller captures authority and validates planner state; Manager owns the
// conditional transaction, primary fencing, unknown-result reload and single
// publication of the resulting targets.
func (m *Manager) commitGroupTargets(ctx context.Context, changes []groupTargetUpdate, conditions []clientv3.Cmp, current func() bool) (bool, error) {
	updates, err := normalizeGroupTargetUpdates(changes)
	if err != nil {
		return false, err
	}
	changed := make([]groupTargetUpdate, 0, len(updates))
	targets := make([]Group, 0, len(updates))
	groupIDs := make([]string, 0, len(updates))
	for _, change := range updates {
		if sameGroupTarget(change.original, change.target) {
			continue
		}
		changed = append(changed, change)
		targets = append(targets, change.target)
		groupIDs = append(groupIDs, change.target.ID)
	}
	checkCurrent := func() bool {
		return current == nil || current()
	}

	m.metaMutex.Lock()
	defer m.metaMutex.Unlock()
	if err := m.refreshMetadataBeforeWriteLocked(); err != nil {
		return false, err
	}
	if len(changed) == 0 {
		return false, nil
	}
	if err := m.allGroupsExist(groupIDs); err != nil {
		return false, err
	}
	if !checkCurrent() {
		return false, affinityConflict()
	}
	currentValid := true
	err = m.runAffinityMutationLocked(ctx, conditions, func(txn kv.Txn) error {
		for i := range changed {
			if err := checkOriginalGroup(txn, &changed[i].original); err != nil {
				return err
			}
			if err := guardAffinityLabelRevision(txn, changed[i].ranges); err != nil {
				return err
			}
		}
		for i := range changed {
			if err := m.storage.SaveAffinityGroup(txn, changed[i].target.ID, &changed[i].target); err != nil {
				return err
			}
		}
		currentValid = checkCurrent()
		if !currentValid {
			return affinityConflict()
		}
		return nil
	})
	if err != nil {
		if errs.ErrEtcdTxnConflict.Equal(err) {
			if currentValid {
				m.metadataNeedsReload = true
			}
			return false, nil
		}
		return false, err
	}
	if err := m.updateGroupTargets(targets); err != nil {
		m.metadataNeedsReload = true
		return false, err
	}
	return true, nil
}
