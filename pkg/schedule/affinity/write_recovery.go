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

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
)

// pendingAffinityMutation is shared by every Manager writer.
type pendingAffinityMutation struct {
	ids        []string
	creates    map[string]bool
	conditions []clientv3.Cmp
	uncertain  bool
}

// affinityMutationTxn preserves native revision comparisons and caches reads,
// so the shared fence and business checks use the same observation.
type affinityMutationTxn struct {
	kv.Txn
	values map[string]string
	saved  map[string]bool
	ops    int
}

func (txn *affinityMutationTxn) Load(key string) (string, error) {
	return txn.LoadWithRevision(key)
}

func (txn *affinityMutationTxn) LoadWithRevision(key string) (string, error) {
	if value, ok := txn.values[key]; ok {
		return value, nil
	}
	var value string
	var err error
	if revisionTxn, ok := txn.Txn.(kv.RevisionTxn); ok {
		value, err = revisionTxn.LoadWithRevision(key)
	} else {
		value, err = txn.Txn.Load(key)
	}
	if err == nil {
		txn.values[key] = value
	}
	return value, err
}

func (txn *affinityMutationTxn) Save(key, value string) error {
	txn.saved[key] = true
	txn.ops++
	return txn.Txn.Save(key, value)
}

func (txn *affinityMutationTxn) Remove(key string) error {
	txn.ops++
	return txn.Txn.Remove(key)
}

// runAffinityMutationLocked is the single transaction/recovery boundary for
// management and planner writes. The caller holds metaMutex through publication.
func (m *Manager) runAffinityMutationLocked(ctx context.Context, conditions []clientv3.Cmp, ids []string, f func(kv.Txn) error) error {
	if err := m.recoverWritesLocked(ctx); err != nil {
		return err
	}
	pending := &pendingAffinityMutation{ids: slices.Clone(ids), conditions: slices.Clone(conditions), creates: make(map[string]bool)}
	attempted := false
	err := m.runAffinityTxn(ctx, conditions, func(inner kv.Txn) error {
		txn := &affinityMutationTxn{Txn: inner, values: make(map[string]string), saved: make(map[string]bool)}
		for _, id := range ids {
			if _, err := txn.LoadWithRevision(keypath.AffinityGroupPath(id)); err != nil {
				return err
			}
		}
		if err := f(txn); err != nil {
			return err
		}
		if len(txn.values)+txn.ops+len(conditions) > etcdutil.MaxEtcdTxnOps {
			return errs.ErrExceedMaxEtcdTxnOps.GenWithStackByArgs()
		}
		for _, id := range ids {
			key := keypath.AffinityGroupPath(id)
			pending.creates[id] = txn.saved[key] && txn.values[key] == ""
		}
		attempted = txn.ops > 0
		return nil
	})
	if err == nil || (!attempted && !errs.ErrEtcdTxnConflict.Equal(err)) {
		return err
	}
	pending.uncertain = !errs.ErrEtcdTxnConflict.Equal(err)
	m.pendingMutation = pending
	m.Lock()
	for _, id := range ids {
		m.invalidateMetadataLocked(id)
	}
	m.Unlock()
	if reloadErr := m.recoverWritesLocked(m.ctx); reloadErr != nil {
		return fmt.Errorf("%w (authoritative reload failed: %v)", err, reloadErr)
	}
	return err
}

// recoverWritesLocked never replays the original operation. Existing groups
// are rewritten unchanged to advance native ModRevision and fence late commits.
// A missing create has no revision to advance: keep the Manager blocked until
// its sole attempt is visible or its original primary term is fenced.
func (m *Manager) recoverWritesLocked(ctx context.Context) error {
	pending := m.pendingMutation
	if pending == nil {
		return nil
	}
	if pending.uncertain && len(pending.conditions) > 0 {
		err := m.runAffinityTxn(ctx, pending.conditions, func(kv.Txn) error { return nil })
		if errs.ErrEtcdTxnConflict.Equal(err) {
			pending.uncertain = false
		} else if err != nil {
			return err
		}
	}
	for _, id := range pending.ids {
		var group *Group
		var rule *labeler.LabelRule
		var conditions []clientv3.Cmp
		if pending.uncertain {
			conditions = pending.conditions
		}
		err := m.runAffinityTxn(ctx, conditions, func(txn kv.Txn) error {
			var err error
			group, err = loadAffinityGroupInTxn(txn, id)
			if err != nil {
				return err
			}
			key := keypath.RegionLabelKeyPath(GetLabelRuleID(id))
			var value string
			if revisionTxn, ok := txn.(kv.RevisionTxn); ok {
				value, err = revisionTxn.LoadWithRevision(key)
			} else {
				value, err = txn.Load(key)
			}
			if err != nil {
				return err
			}
			if value != "" {
				rule, err = labeler.NewLabelRuleFromJSON([]byte(value))
				if err != nil {
					return err
				}
			}
			if pending.uncertain {
				if group == nil && pending.creates[id] {
					return fmt.Errorf("affinity mutation outcome is unresolved: %w", affinityConflict())
				}
				if group != nil {
					return m.storage.SaveAffinityGroup(txn, id, group)
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		if group == nil {
			err = m.syncGroupDeleteFromEtcdLocked(id, 0)
		} else {
			err = m.syncGroupFromEtcdLocked(group, 0)
		}
		if err != nil {
			return err
		}
		if rule == nil {
			err = m.syncKeyRangesDeleteFromEtcdLocked(GetLabelRuleID(id), 0)
		} else {
			err = m.syncKeyRangesFromEtcdLocked(rule, 0)
		}
		if err != nil {
			return err
		}
	}
	m.pendingMutation = nil
	return nil
}
