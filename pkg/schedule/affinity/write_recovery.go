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
	"strconv"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/errors"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
)

// affinityMutationTxn counts transaction operations and uses native revision
// comparisons for reads. Each business callback reads its affected keys once.
type affinityMutationTxn struct {
	kv.Txn
	reads int
	ops   int
}

func loadAffinityRangeVersion(txn kv.Txn) (uint64, error) {
	key := keypath.AffinityRangeVersionPath()
	value, err := loadAffinityValue(txn, key)
	if err != nil {
		return 0, err
	}
	version := uint64(0)
	if value != "" {
		version, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid affinity range version %q: %w", value, err)
		}
	}
	return version, nil
}

// bumpAffinityRangeVersion adds the shared CAS fence used by every mutation
// that changes affinity ranges or their region-label rules. The expected value
// comes from this Manager's last authoritative reload or successful mutation;
// stale managers therefore fail before their cross-group overlap check can be
// committed.
func bumpAffinityRangeVersion(txn kv.Txn, expected uint64) error {
	version, err := loadAffinityRangeVersion(txn)
	if err != nil {
		return err
	}
	if version != expected {
		return affinityConflict()
	}
	if version == ^uint64(0) {
		return errors.New("affinity range version overflow")
	}
	return txn.Save(keypath.AffinityRangeVersionPath(), strconv.FormatUint(version+1, 10))
}

func (m *Manager) runAffinityRangeMutationLocked(ctx context.Context, conditions []clientv3.Cmp, f func(kv.Txn) error) error {
	expected := m.affinityRangeVersion
	err := m.runAffinityMutationLocked(ctx, conditions, func(txn kv.Txn) error {
		if err := bumpAffinityRangeVersion(txn, expected); err != nil {
			return err
		}
		return f(txn)
	})
	if err == nil {
		m.affinityRangeVersion = expected + 1
	}
	return err
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
// authoritative reload without replaying the business operation. Range/label
// mutations add a shared version fence through runAffinityRangeMutationLocked
// so cross-group overlap checks remain serialized even across an unknown
// response.
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
		return err
	}
	// Callback failures cannot have committed. A submission error after writes
	// were queued is ambiguous. Reconcile the local cache once like other PD
	// metadata writers, then return the original error; callers retry explicitly.
	if !attempted {
		return err
	}
	if reloadErr := m.reloadAffinityMetadataUnconditionallyLocked(ctx); reloadErr != nil {
		// The durable write may have committed, but the one reconciliation
		// read also failed. Make the next explicit request refresh the
		// authoritative baseline before constructing its CAS conditions.
		m.metadataNeedsReload = true
		return fmt.Errorf("%w (authoritative reload failed: %v)", err, reloadErr)
	}
	return err
}
