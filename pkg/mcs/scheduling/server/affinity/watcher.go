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
	"encoding/json"
	"strings"
	"sync"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
)

// Watcher is used to watch the affinity group and label rule changes from PD.
// It watches two paths:
// affinityGroupsPrefix:
//   - Key: /pd/{cluster_id}/affinity_groups/{group_id}
//   - Value: affinity.Group
//
// regionLabelPathPrefix:
//   - Key: /pd/{cluster_id}/region_label/affinity_group/{group_id}
//   - Value: labeler.LabelRule
//   - Filtered to only process rules with ID prefix "affinity_group/"
type Watcher struct {
	cancel     context.CancelFunc
	wg         *sync.WaitGroup
	etcdClient *clientv3.Client

	// affinityManager is used to manage the affinity groups in the scheduling server.
	affinityManager *affinity.Manager

	groupWatcher *etcdutil.LoopWatcher
	labelWatcher *etcdutil.LoopWatcher
	groupPending []groupSyncEvent
	labelPending []labelSyncEvent
}

type groupSyncEvent struct {
	group    *affinity.Group
	id       string
	revision int64
	deleted  bool
}

type labelSyncEvent struct {
	rule     *labeler.LabelRule
	id       string
	revision int64
	deleted  bool
}

// NewWatcher creates a new watcher to watch the affinity changes from PD.
func NewWatcher(
	ctx context.Context,
	etcdClient *clientv3.Client,
	affinityManager *affinity.Manager,
) (*Watcher, error) {
	ctx, cancel := context.WithCancel(ctx)
	w := &Watcher{
		cancel:          cancel,
		wg:              &sync.WaitGroup{},
		etcdClient:      etcdClient,
		affinityManager: affinityManager,
	}
	err := w.initializeGroupWatcher(ctx)
	if err != nil {
		w.Close()
		return nil, err
	}
	err = w.initializeAffinityLabelWatcher(ctx)
	if err != nil {
		w.Close()
		return nil, err
	}
	return w, nil
}

// initializeGroupWatcher initializes the watcher for affinity group changes.
func (w *Watcher) initializeGroupWatcher(ctx context.Context) error {
	// The scheduling Manager is initialized from storage before this watcher is
	// created. Seed deletion reconciliation with that snapshot so a group
	// deleted in the gap between the two loads is removed from memory even
	// though its delete event is older than the watch revision.
	initialKeys := make([]string, 0)
	for _, group := range w.affinityManager.GetAllAffinityGroupStates() {
		initialKeys = append(initialKeys, keypath.AffinityGroupPath(group.ID))
	}
	putFn := func(kv *mvccpb.KeyValue) error {
		log.Info("update affinity group", zap.String("key", string(kv.Key)), zap.String("value", string(kv.Value)))
		group := &affinity.Group{}
		if err := json.Unmarshal(kv.Value, group); err != nil {
			log.Warn("failed to unmarshal affinity group", zap.String("key", string(kv.Key)), zap.Error(err))
			return err
		}
		w.groupPending = append(w.groupPending, groupSyncEvent{group: group, revision: kv.ModRevision})
		return nil
	}

	deleteFn := func(kv *mvccpb.KeyValue) error {
		key := string(kv.Key)
		log.Info("delete affinity group", zap.String("key", key))
		groupID := strings.TrimPrefix(key, keypath.AffinityGroupsPrefix())
		w.groupPending = append(w.groupPending, groupSyncEvent{id: groupID, revision: kv.ModRevision, deleted: true})
		return nil
	}
	applyPending := func() error {
		pending := w.groupPending
		w.groupPending = nil
		var revision int64
		for _, event := range pending {
			if event.deleted {
				if err := w.affinityManager.SyncGroupDeleteFromEtcdAtRevision(event.id, event.revision); err != nil {
					return err
				}
			} else {
				if err := w.affinityManager.SyncGroupFromEtcdAtRevision(event.group, event.revision); err != nil {
					return err
				}
			}
			revision = max(revision, event.revision)
		}
		w.affinityManager.AdvanceWatchRevisions(revision, 0)
		return nil
	}

	w.groupWatcher = etcdutil.NewLoopWatcher(
		ctx, w.wg,
		w.etcdClient,
		"scheduling-affinity-group-watcher",
		strings.TrimSuffix(keypath.AffinityGroupsPrefix(), "/"),
		func([]*clientv3.Event) error { w.groupPending = nil; return nil },
		putFn, deleteFn,
		func([]*clientv3.Event) error { return applyPending() },
		true, /* withPrefix */
	)
	w.groupWatcher.SetReconcileDeletedKeys()
	w.groupWatcher.SetReconcileDeletedKeyRevision()
	w.groupWatcher.SetInitialLoadedKeys(initialKeys)
	w.groupWatcher.SetConsistentLoad()
	w.groupWatcher.SetAtomicLoadCallbacks()
	w.groupWatcher.StartWatchLoop()
	return w.groupWatcher.WaitLoad()
}

// initializeAffinityLabelWatcher initializes the watcher for affinity label rule changes.
// It watches the region_label path but only processes rules with affinity group prefix.
func (w *Watcher) initializeAffinityLabelWatcher(ctx context.Context) error {
	// Note: labelWatcher does not need preEventsFn/postEventsFn locking
	// because the sync methods will handle locking internally
	initialKeys := make([]string, 0)
	w.affinityManager.GetRegionLabeler().IterateLabelRules(func(rule *labeler.LabelRule) bool {
		if strings.HasPrefix(rule.ID, affinity.LabelRuleIDPrefix) {
			initialKeys = append(initialKeys, keypath.RegionLabelKeyPath(rule.ID))
		}
		return true
	})

	putFn := func(kv *mvccpb.KeyValue) error {
		key := string(kv.Key)
		log.Info("update affinity label rule", zap.String("key", key), zap.String("value", string(kv.Value)))
		rule, err := labeler.NewLabelRuleFromJSON(kv.Value)
		if err != nil {
			log.Warn("failed to unmarshal affinity label rule", zap.String("key", key), zap.Error(err))
			return err
		}
		w.labelPending = append(w.labelPending, labelSyncEvent{rule: rule, revision: kv.ModRevision})
		return nil
	}

	deleteFn := func(kv *mvccpb.KeyValue) error {
		key := string(kv.Key)
		log.Info("delete affinity label rule", zap.String("key", key))
		ruleID := strings.TrimPrefix(key, keypath.RegionLabelPathPrefix())
		w.labelPending = append(w.labelPending, labelSyncEvent{id: ruleID, revision: kv.ModRevision, deleted: true})
		return nil
	}
	applyPending := func() error {
		pending := w.labelPending
		w.labelPending = nil
		var revision int64
		for _, event := range pending {
			if event.deleted {
				if err := w.affinityManager.SyncKeyRangesDeleteFromEtcdAtRevision(event.id, event.revision); err != nil {
					return err
				}
			} else if err := w.affinityManager.SyncKeyRangesFromEtcdAtRevision(event.rule, event.revision); err != nil {
				return err
			}
			revision = max(revision, event.revision)
		}
		w.affinityManager.AdvanceWatchRevisions(0, revision)
		return nil
	}

	w.labelWatcher = etcdutil.NewLoopWatcher(
		ctx, w.wg,
		w.etcdClient,
		"scheduling-affinity-label-watcher",
		strings.TrimSuffix(keypath.RegionLabelKeyPath(affinity.LabelRuleIDPrefix), "/"), // Filter: only process affinity group label rules
		func([]*clientv3.Event) error { w.labelPending = nil; return nil },
		putFn, deleteFn,
		func([]*clientv3.Event) error { return applyPending() },
		true, /* withPrefix */
	)
	w.labelWatcher.SetConsistentLoad()
	w.labelWatcher.SetAtomicLoadCallbacks()
	w.labelWatcher.SetReconcileDeletedKeys()
	w.labelWatcher.SetReconcileDeletedKeyRevision()
	w.labelWatcher.SetInitialLoadedKeys(initialKeys)
	w.labelWatcher.StartWatchLoop()
	return w.labelWatcher.WaitLoad()
}

// Close closes the watcher.
func (w *Watcher) Close() {
	if w.cancel != nil {
		w.cancel()
	}
	if w.wg != nil {
		w.wg.Wait()
	}
}
