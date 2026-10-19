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
	"context"
	"fmt"
	"slices"

	"github.com/tikv/pd/pkg/errs"
)

// SetBalanceRuleRevision installs the local placement-rule generation used to
// invalidate stable searches and reject candidates built under older rules.
func (m *Manager) SetBalanceRuleRevision(revision func() uint64) {
	m.Lock()
	defer m.Unlock()
	m.balanceRuleRevision = revision
}

// PrepareBalanceCommit validates local metadata and captures the table/range
// and authority conditions for a subsequent write. Placement rules are read
// from the local RuleManager; this path deliberately does not perform a
// remote rules/rule-groups/ordinary-label JSON reconciliation.
func (m *Manager) PrepareBalanceCommit(ctx context.Context, snapshot *BalanceSnapshot) error {
	if !m.BalanceSnapshotCurrent(snapshot) {
		return affinityConflict()
	}
	m.refreshBalanceStores()
	if !m.BalanceSnapshotCurrent(snapshot) {
		return affinityConflict()
	}
	m.RLock()
	term := m.affinityWriteTerm
	m.RUnlock()
	if term != nil {
		var ok bool
		snapshot.conditions, ok = term()
		if !ok {
			return affinityConflict()
		}
	}
	return nil
}

// BalanceSnapshotCurrent reports whether local search inputs are unchanged.
func (m *Manager) BalanceSnapshotCurrent(snapshot *BalanceSnapshot) bool {
	version, exists := m.GetBalanceVersion(snapshot.TableID)
	return exists && version == snapshot.Version
}

// CommitBalanceTarget updates one target using the captured table version and
// primary term. Candidate/Region scans happen before this method; the final
// value check, etcd commit, and in-memory publication stay under metaMutex.
func (m *Manager) CommitBalanceTarget(ctx context.Context, snapshot *BalanceSnapshot, target *Group) (bool, error) {
	index := slices.IndexFunc(snapshot.Groups, func(group *GroupState) bool { return group.ID == target.ID })
	if index < 0 || snapshot.Groups[index].BalancePolicy == BalancePolicyFixed || !m.BalanceSnapshotCurrent(snapshot) {
		return false, affinityConflict()
	}
	original := snapshot.Groups[index]
	group := original.Group
	group.LeaderStoreID, group.VoterStoreIDs = target.LeaderStoreID, normalizeVoters(target.VoterStoreIDs)
	if err := m.AdjustGroup(&group); err != nil {
		return false, err
	}
	if err := m.checkBalanceTargetStores(group.LeaderStoreID, group.VoterStoreIDs); err != nil {
		// The worker logs the rejection and retries with backoff. Do not hide
		// a temporary target failure as a successful no-op or stable result.
		return false, fmt.Errorf("affinity target %s rejected: %w", group.ID, err)
	}
	if err := m.checkSpecialUseTargetStores(original.Group.VoterStoreIDs, group.VoterStoreIDs); err != nil {
		return false, fmt.Errorf("affinity target %s rejected: %w", group.ID, err)
	}
	m.refreshBalanceStores()
	if !m.BalanceSnapshotCurrent(snapshot) {
		return false, affinityConflict()
	}
	return m.commitGroupTarget(ctx, original.Group, group, snapshot.Ranges[original.ID], snapshot.conditions,
		func() bool { return m.BalanceSnapshotCurrent(snapshot) })
}

func (m *Manager) checkBalanceTargetStores(leaderStoreID uint64, voterStoreIDs []uint64) error {
	if m.storeSetInformer != nil {
		return checkUnavailableTargetStores(m.collectUnavailableStores(), leaderStoreID, voterStoreIDs)
	}
	return m.checkHasUnavailableStore(leaderStoreID, voterStoreIDs)
}

// checkSpecialUseTargetStores checks only newly introduced voters.
func (m *Manager) checkSpecialUseTargetStores(originalVoterStoreIDs, voterStoreIDs []uint64) error {
	if m.storeSetInformer == nil {
		return nil
	}
	original := make(map[uint64]struct{}, len(originalVoterStoreIDs))
	for _, storeID := range originalVoterStoreIDs {
		original[storeID] = struct{}{}
	}
	for _, storeID := range voterStoreIDs {
		if _, existed := original[storeID]; existed {
			continue
		}
		store := m.storeSetInformer.GetStore(storeID)
		if store != nil {
			specialUse := store.GetLabelValue("specialUse")
			if specialUse == "hotRegion" || specialUse == "reserved" {
				return errs.ErrAffinityGroupContent.GenWithStackByArgs(fmt.Sprintf("store %d has special use %q", storeID, specialUse))
			}
		}
	}
	return nil
}
