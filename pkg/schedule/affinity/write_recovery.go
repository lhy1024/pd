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

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
)

// affinityMutationTxn counts transaction operations and uses native revision
// comparisons for reads. Each business callback reads its affected keys once.
type affinityMutationTxn struct {
	kv.Txn
	reads int
	ops   int
}

// Load reads a value with a native revision comparison.
func (txn *affinityMutationTxn) Load(key string) (string, error) {
	txn.reads++
	return loadAffinityValue(txn.Txn, key)
}

// Save records a value write in the transaction operation count.
func (txn *affinityMutationTxn) Save(key, value string) error {
	txn.ops++
	return txn.Txn.Save(key, value)
}

// Remove records a value deletion in the transaction operation count.
func (txn *affinityMutationTxn) Remove(key string) error {
	txn.ops++
	return txn.Txn.Remove(key)
}

// runAffinityMutationLocked is the single transaction boundary for all writers.
// It follows PD's existing metadata contract: native revision/CAS and primary
// term fence the transaction; an ambiguous result is reconciled by an
// authoritative reload, without replaying the business operation or adding a
// durable write sequence. A late same-term ABA remains the documented PD-level
// tradeoff for this low-frequency metadata path.
func (m *Manager) runAffinityMutationLocked(ctx context.Context, conditions []clientv3.Cmp, f func(kv.Txn) error) error {
	attempted := false
	err := m.runAffinityTxn(ctx, conditions, func(inner kv.Txn) error {
		txn := &affinityMutationTxn{Txn: inner}
		if err := f(txn); err != nil {
			return err
		}
		if txn.reads+txn.ops+len(conditions) > etcdutil.MaxEtcdTxnOps {
			return errs.ErrExceedMaxEtcdTxnOps.GenWithStackByArgs()
		}
		attempted = txn.ops > 0
		return nil
	})
	if err == nil {
		return nil
	}
	// A failed compare is a definitive rejection: the transaction did not
	// commit. Match the other PD metadata writers and return the conflict
	// directly. The watcher will apply the winning value; a full reload here
	// would add a second read path and scan unrelated affinity metadata.
	if errs.ErrEtcdTxnConflict.Equal(err) {
		m.RLock()
		noWatcher := m.affinityWriteTerm == nil
		m.RUnlock()
		if noWatcher {
			m.metadataNeedsReload = true
		}
		return err
	}
	// Callback failures cannot have committed. A submission error after writes
	// were queued is ambiguous. Reconcile the local cache once like other PD
	// metadata writers, then return the original error; callers retry explicitly.
	if !attempted {
		return err
	}
	if reloadErr := m.reloadAffinityMetadataUnconditionallyLocked(ctx); reloadErr != nil {
		m.RLock()
		noWatcher := m.affinityWriteTerm == nil
		m.RUnlock()
		if noWatcher {
			// The durable write may have committed, but the one reconciliation
			// read also failed. Make the next explicit request refresh the
			// authoritative baseline before constructing its CAS conditions.
			m.metadataNeedsReload = true
		}
		return fmt.Errorf("%w (authoritative reload failed: %v)", err, reloadErr)
	}
	return err
}
