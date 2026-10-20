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
// An ambiguous committed write blocks this Manager until a new primary term
// reloads metadata. That is the deliberate low-frequency affinity contract.
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
	// commit. Refresh the local view once before returning the conflict. This
	// path deliberately omits the caller's primary condition because a stale
	// cache is exactly what a deterministic CAS conflict reports; the read is
	// not an authorization to write.
	if errs.ErrEtcdTxnConflict.Equal(err) {
		if reloadErr := m.reloadAffinityMetadataUnconditionallyLocked(ctx); reloadErr != nil {
			return fmt.Errorf("%w (authoritative reload failed: %v)", err, reloadErr)
		}
		return err
	}
	// Callback failures cannot have committed. A submission error after writes
	// were queued is ambiguous, so freeze this Manager until a new term reloads
	// it instead of replaying the business operation.
	if !attempted {
		return err
	}
	m.writeBlocked = true
	m.blockedConditions = slices.Clone(conditions)
	return err
}

func (m *Manager) ensureAffinityWritesReadyLocked() error {
	if m.writeBlocked {
		return errAffinityWriteBlocked
	}
	return nil
}
