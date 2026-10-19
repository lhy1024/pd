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
	"errors"
	"fmt"
	"math"
	"strconv"

	clientv3 "go.etcd.io/etcd/client/v3"

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

func loadAffinityWriteSequence(txn kv.Txn) (uint64, error) {
	value, err := loadAffinityValue(txn, keypath.AffinityWriteSequencePath())
	if err != nil || value == "" {
		return 0, err
	}
	return strconv.ParseUint(value, 10, 64)
}

func advanceAffinityWriteSequence(txn kv.Txn, sequence uint64) error {
	if sequence == math.MaxUint64 {
		return errors.New("affinity write sequence exhausted")
	}
	return txn.Save(keypath.AffinityWriteSequencePath(), strconv.FormatUint(sequence+1, 10))
}

// runAffinityMutationLocked is the single transaction boundary for all writers.
// Callers recover before reading business state and hold metaMutex through
// publication. The cached sequence binds that state to the shared write fence.
func (m *Manager) runAffinityMutationLocked(ctx context.Context, conditions []clientv3.Cmp, f func(kv.Txn) error) error {
	sequence := m.writeSequence
	attempted := false
	err := m.runAffinityTxn(ctx, conditions, func(inner kv.Txn) error {
		txn := &affinityMutationTxn{Txn: inner}
		current, err := loadAffinityWriteSequence(txn)
		if err != nil {
			return err
		}
		if current != sequence {
			return affinityConflict()
		}
		if err := f(txn); err != nil {
			return err
		}
		if txn.ops > 0 {
			if err := advanceAffinityWriteSequence(txn, sequence); err != nil {
				return err
			}
		}
		if txn.reads+txn.ops+len(conditions) > etcdutil.MaxEtcdTxnOps {
			return errs.ErrExceedMaxEtcdTxnOps.GenWithStackByArgs()
		}
		attempted = txn.ops > 0
		return nil
	})
	if err == nil {
		if attempted {
			m.writeSequence = sequence + 1
		}
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
	// Callback failures cannot have committed. All submission errors and CAS
	// conflicts share one recovery path; none replay the business operation.
	if !attempted {
		return err
	}
	m.pendingSequence = &sequence
	m.Lock()
	for _, state := range m.metadataTables {
		m.metadataGeneration++
		state.version = m.metadataGeneration
	}
	m.Unlock()
	if reloadErr := m.recoverWritesLocked(m.ctx); reloadErr != nil {
		return fmt.Errorf("%w (authoritative reload failed: %v)", err, reloadErr)
	}
	return err
}

// recoverWritesLocked fences the original sequence before loading actual
// metadata. The original transaction and the fence can never both succeed.
// If the fence also times out, retry the same sequence, never a new one.
func (m *Manager) recoverWritesLocked(ctx context.Context) error {
	if m.pendingSequence == nil {
		return nil
	}
	conditions, err := m.captureAffinityWriteConditions()
	if err != nil {
		return err
	}
	err = m.runAffinityTxn(ctx, conditions, func(txn kv.Txn) error {
		sequence, err := loadAffinityWriteSequence(txn)
		if err != nil {
			return err
		}
		if sequence < *m.pendingSequence {
			return errors.New("affinity write sequence regressed")
		}
		if sequence == *m.pendingSequence {
			return advanceAffinityWriteSequence(txn, sequence)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err := m.reloadAffinityMetadataWithConditions(ctx, conditions); err != nil {
		return err
	}
	m.pendingSequence = nil
	return nil
}
