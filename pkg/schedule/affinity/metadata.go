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
	tableID := TableGroupPrefix(groupID)
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

// TableGroupPrefix identifies the table to which an affinity group belongs.
func TableGroupPrefix(groupID string) string {
	if strings.HasPrefix(groupID, "_tidb_pt_") {
		if index := strings.LastIndex(groupID, "_p"); index >= len("_tidb_pt_") {
			return groupID[:index]
		}
	}
	return groupID
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

// commitGroupTarget is the metadata boundary for conditional target changes.
// The caller supplies its input-validity check; Manager owns serialization,
// revision CAS, primary fencing and publication. This follows PD's existing
// metadata write contract; ambiguous errors are reconciled by a reload and
// do not create a second durable write-fencing protocol.
//
//nolint:unused // consumed by the planner integration in this branch.
func (m *Manager) commitGroupTarget(ctx context.Context, original, target Group, ranges GroupKeyRanges, conditions []clientv3.Cmp, current func() bool) (bool, error) {
	m.metaMutex.Lock()
	defer m.metaMutex.Unlock()
	if !current() {
		return false, affinityConflict()
	}
	err := m.runAffinityMutationLocked(ctx, conditions, func(txn kv.Txn) error {
		if !current() {
			return affinityConflict()
		}
		if err := checkOriginalGroup(txn, &original); err != nil {
			return err
		}
		if err := guardAffinityLabelRevision(txn, ranges); err != nil {
			return err
		}
		return m.storage.SaveAffinityGroup(txn, target.ID, &target)
	})
	if err != nil {
		if errs.ErrEtcdTxnConflict.Equal(err) {
			return false, nil
		}
		return false, err
	}
	_, err = m.updateGroupTarget(target)
	return true, err
}
