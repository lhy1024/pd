// Copyright 2016 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package id

import (
	"path"

	"github.com/pingcap/log"
	"github.com/sasha-s/go-deadlock"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/etcdutil"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server/kv"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

// Allocator is the allocator to generate unique ID.
type Allocator interface {
	Alloc() (uint64, error)
}

const allocStep = uint64(1000)

// AllocatorImpl is used to allocate ID.
type AllocatorImpl struct {
	mu   deadlock.Mutex
	base uint64
	end  uint64

	client   *clientv3.Client
	rootPath string
	member   string
}

// NewAllocatorImpl creates a new IDAllocator.
func NewAllocatorImpl(client *clientv3.Client, rootPath string, member string) *AllocatorImpl {
	return &AllocatorImpl{client: client, rootPath: rootPath, member: member}
}

// Alloc returns a new id.
func (alloc *AllocatorImpl) Alloc() (uint64, error) {
	alloc.mu.Lock()
	defer alloc.mu.Unlock()

	if alloc.base == alloc.end {
		if err := alloc.generateLocked(); err != nil {
			return 0, err
		}
	}

	alloc.base++

	return alloc.base, nil
}

// Generate synchronizes and generates id range.
func (alloc *AllocatorImpl) Generate() error {
	alloc.mu.Lock()
	defer alloc.mu.Unlock()

	return alloc.generateLocked()
}

func (alloc *AllocatorImpl) generateLocked() error {
	key := alloc.getAllocIDPath()
	value, err := etcdutil.GetValue(alloc.client, key)
	if err != nil {
		return err
	}

	var (
		cmp clientv3.Cmp
		end uint64
	)

	if value == nil {
		// create the key
		cmp = clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
	} else {
		// update the key
		end, err = typeutil.BytesToUint64(value)
		if err != nil {
			return err
		}

		cmp = clientv3.Compare(clientv3.Value(key), "=", string(value))
	}

	end += allocStep
	value = typeutil.Uint64ToBytes(end)
	txn := kv.NewSlowLogTxn(alloc.client)
	leaderPath := path.Join(alloc.rootPath, "leader")
	t := txn.If(append([]clientv3.Cmp{cmp}, clientv3.Compare(clientv3.Value(leaderPath), "=", alloc.member))...)
	resp, err := t.Then(clientv3.OpPut(key, string(value))).Commit()
	if err != nil {
		return errs.ErrEtcdTxn.Wrap(err).GenWithStackByArgs()
	}
	if !resp.Succeeded {
		return errs.ErrEtcdTxn.FastGenByArgs()
	}

	log.Info("idAllocator allocates a new id", zap.Uint64("alloc-id", end))
	idGauge.WithLabelValues("idalloc").Set(float64(end))
	alloc.end = end
	alloc.base = end - allocStep
	return nil
}

func (alloc *AllocatorImpl) getAllocIDPath() string {
	return path.Join(alloc.rootPath, "alloc_id")
}
