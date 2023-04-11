// Copyright 2023 TiKV Project Authors.
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

package keyspace

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/balancer"
	"github.com/tikv/pd/pkg/mcs/discovery"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

const (
	defaultBalancerPolicy = balancer.PolicyRoundRobin
	allocNodeTimeout      = 1 * time.Second
	allocNodeInterval     = 10 * time.Millisecond
	// TODO: move it to etcdutil
	watchEtcdChangeRetryInterval = 1 * time.Second
	maxRetryTimes                = 25
	retryInterval                = 100 * time.Millisecond
)

// GroupManager is the manager of keyspace group related data.
type GroupManager struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	// store is the storage for keyspace group related information.
	store endpoint.KeyspaceGroupStorage

	client *clientv3.Client

	// tsoServiceKey is the path of TSO service in etcd.
	tsoServiceKey string
	// tsoServiceEndKey is the end key of TSO service in etcd.
	tsoServiceEndKey string

	policy balancer.Policy

	// TODO: add user kind with different balancer
	// when we ensure where the correspondence between tso node and user kind will be found
	nodesBalancer balancer.Balancer[string]
}

// NewKeyspaceGroupManager creates a Manager of keyspace group related data.
func NewKeyspaceGroupManager(ctx context.Context, store endpoint.KeyspaceGroupStorage, client *clientv3.Client, clusterID uint64) *GroupManager {
	ctx, cancel := context.WithCancel(ctx)
	key := discovery.TSOPath(clusterID)
	return &GroupManager{
		ctx:              ctx,
		cancel:           cancel,
		store:            store,
		client:           client,
		tsoServiceKey:    key,
		tsoServiceEndKey: clientv3.GetPrefixRangeEnd(key) + "/",
		policy:           defaultBalancerPolicy,
	}
}

// Bootstrap saves default keyspace group info.
func (m *GroupManager) Bootstrap() error {
	defaultKeyspaceGroup := &endpoint.KeyspaceGroup{
		ID:       utils.DefaultKeySpaceGroupID,
		UserKind: endpoint.Basic.String(),
	}
	err := m.saveKeyspaceGroups([]*endpoint.KeyspaceGroup{defaultKeyspaceGroup})
	// It's possible that default keyspace group already exists in the storage (e.g. PD restart/recover),
	// so we ignore the ErrKeyspaceGroupExists.
	if err != nil && err != ErrKeyspaceGroupExists {
		return err
	}
	if m.client != nil {
		m.nodesBalancer = balancer.GenByPolicy[string](m.policy)
		m.wg.Add(1)
		go m.startWatchLoop()
	}
	return nil
}

// Close closes the manager.
func (m *GroupManager) Close() {
	m.cancel()
	m.wg.Wait()
}

func (m *GroupManager) startWatchLoop() {
	defer m.wg.Done()
	ctx, cancel := context.WithCancel(m.ctx)
	defer cancel()
	var (
		revision int64
		err      error
	)
	for i := 0; i < maxRetryTimes; i++ {
		select {
		case <-ctx.Done():
			return
		case <-time.After(retryInterval):
		}
		resp, err := etcdutil.EtcdKVGet(m.client, m.tsoServiceKey, clientv3.WithRange(m.tsoServiceEndKey))
		if err == nil { // success
			break
		}
		for _, item := range resp.Kvs {
			m.nodesBalancer.Put(string(item.Value))
		}
		revision = resp.Header.GetRevision()
	}
	if err != nil {
		log.Warn("failed to get tso service addrs from etcd", zap.Error(err))
	}
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		nextRevision, err := m.watchServiceAddrs(ctx, revision)
		if err != nil {
			log.Error("watcher canceled unexpectedly and a new watcher will start after a while",
				zap.Int64("next-revision", nextRevision),
				zap.Time("retry-at", time.Now().Add(watchEtcdChangeRetryInterval)),
				zap.Error(err))
			revision = nextRevision
			time.Sleep(watchEtcdChangeRetryInterval)
		}
	}
}

func (m *GroupManager) watchServiceAddrs(ctx context.Context, revision int64) (int64, error) {
	watcher := clientv3.NewWatcher(m.client)
	defer watcher.Close()
	for {
	WatchChan:
		watchChan := watcher.Watch(ctx, m.tsoServiceKey, clientv3.WithRange(m.tsoServiceEndKey), clientv3.WithRev(revision))
		select {
		case <-ctx.Done():
			return revision, nil
		case wresp := <-watchChan:
			if wresp.CompactRevision != 0 {
				log.Warn("required revision has been compacted, the watcher will watch again with the compact revision",
					zap.Int64("required-revision", revision),
					zap.Int64("compact-revision", wresp.CompactRevision))
				revision = wresp.CompactRevision
				goto WatchChan
			}
			if wresp.Err() != nil {
				log.Error("watch is canceled or closed",
					zap.Int64("required-revision", revision),
					zap.Error(wresp.Err()))
				return revision, wresp.Err()
			}
			for _, event := range wresp.Events {
				addr := string(event.Kv.Value)
				switch event.Type {
				case clientv3.EventTypePut:
					m.nodesBalancer.Put(addr)
				case clientv3.EventTypeDelete:
					m.nodesBalancer.Delete(addr)
				}
			}
		}

	}
}

// CreateKeyspaceGroups creates keyspace groups.
func (m *GroupManager) CreateKeyspaceGroups(keyspaceGroups []*endpoint.KeyspaceGroup) error {
	return m.saveKeyspaceGroups(keyspaceGroups)
}

// GetKeyspaceGroups gets keyspace groups from the start ID with limit.
// If limit is 0, it will load all keyspace groups from the start ID.
func (m *GroupManager) GetKeyspaceGroups(startID uint32, limit int) ([]*endpoint.KeyspaceGroup, error) {
	return m.store.LoadKeyspaceGroups(startID, limit)
}

// GetKeyspaceGroupByID returns the keyspace group by id.
func (m *GroupManager) GetKeyspaceGroupByID(id uint32) (*endpoint.KeyspaceGroup, error) {
	var (
		kg  *endpoint.KeyspaceGroup
		err error
	)

	m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
		kg, err = m.store.LoadKeyspaceGroup(txn, id)
		if err != nil {
			return err
		}
		return nil
	})
	return kg, nil
}

// DeleteKeyspaceGroupByID deletes the keyspace group by id.
func (m *GroupManager) DeleteKeyspaceGroupByID(id uint32) error {
	m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
		return m.store.DeleteKeyspaceGroup(txn, id)
	})
	return nil
}

func (m *GroupManager) saveKeyspaceGroups(keyspaceGroups []*endpoint.KeyspaceGroup) error {
	return m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
		for _, keyspaceGroup := range keyspaceGroups {
			// Check if keyspace group has already existed.
			oldKG, err := m.store.LoadKeyspaceGroup(txn, keyspaceGroup.ID)
			if err != nil {
				return err
			}
			if oldKG != nil {
				return ErrKeyspaceGroupExists
			}
			newKG := &endpoint.KeyspaceGroup{
				ID:       keyspaceGroup.ID,
				UserKind: keyspaceGroup.UserKind,
			}
			m.store.SaveKeyspaceGroup(txn, newKG)
		}
		return nil
	})
}

// GetAvailableKeyspaceGroupIDByKind returns the available keyspace group id by user kind.
func (m *GroupManager) GetAvailableKeyspaceGroupIDByKind(userKind endpoint.UserKind) (string, error) {
	// TODO: implement it
	return "0", nil
}

// GetNodesNum returns the number of nodes.
func (m *GroupManager) GetNodesNum() int {
	return len(m.nodesBalancer.GetAll())
}

// AllocNodesForGroup allocates nodes for the keyspace group.
func (m *GroupManager) AllocNodesForKeyspaceGroup(id uint32, replica int) ([]endpoint.KeyspaceGroupMember, error) {
	ctx, cancel := context.WithTimeout(m.ctx, allocNodeTimeout)
	defer cancel()
	ticker := time.NewTicker(allocNodeInterval)
	defer ticker.Stop()
	nodes := make([]endpoint.KeyspaceGroupMember, 0, replica)
	m.store.RunInTxn(m.ctx, func(txn kv.Txn) error {
		kg, err := m.store.LoadKeyspaceGroup(txn, id)
		if err != nil {
			return err
		}
		if kg == nil {
			return ErrKeyspaceGroupNotExists
		}
		exists := make(map[string]struct{})
		for _, member := range kg.Members {
			exists[member.Address] = struct{}{}
			nodes = append(nodes, member)
		}
		for len(exists) < replica {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
			}
			num := len(m.nodesBalancer.GetAll())
			if num < replica || num == 0 { // double check
				return nil
			}
			addr := m.nodesBalancer.Next()
			if addr == "" {
				return nil
			}
			if _, ok := exists[addr]; ok {
				continue
			}
			exists[addr] = struct{}{}
			nodes = append(nodes, endpoint.KeyspaceGroupMember{Address: addr})
		}
		kg.Members = nodes
		m.store.SaveKeyspaceGroup(txn, kg)
		return nil
	})
	return nodes, nil
}
