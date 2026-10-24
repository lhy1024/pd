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
}

// reconcile captures the watch position before reading metadata. Events committed
// during the load will be replayed, rather than skipped by a later revision.
func (w *Watcher) reconcile(ctx context.Context) (int64, error) {
	resp, err := etcdutil.EtcdKVGetWithContext(ctx, w.etcdClient, keypath.AffinityRangeVersionPath())
	if err != nil {
		return 0, err
	}
	if err := w.affinityManager.ReloadAffinityMetadataFromStorage(); err != nil {
		return 0, err
	}
	return resp.Header.Revision + 1, nil
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

// newLoopWatcher shares the same recovery contract for both metadata streams.
// The first default load discards its values and reconciles in postEventsFn;
// subsequent recovery uses reconcile directly, without a second prefix scan.
func (w *Watcher) newLoopWatcher(ctx context.Context, name, prefix string, apply func([]*clientv3.Event) error) *etcdutil.LoopWatcher {
	ignoreKV := func(*mvccpb.KeyValue) error { return nil }
	lw := etcdutil.NewLoopWatcher(ctx, w.wg, w.etcdClient, name, strings.TrimSuffix(prefix, "/"),
		func([]*clientv3.Event) error { return nil }, ignoreKV, ignoreKV,
		func(events []*clientv3.Event) error {
			if len(events) == 0 {
				_, err := w.reconcile(ctx)
				return err
			}
			return apply(events)
		}, true)
	lw.SetConsistentLoad()
	lw.SetAtomicLoadCallbacks()
	lw.SetInitialLoadRetryFn(w.reconcile)
	lw.SetCompactionReloadFn(w.reconcile)
	lw.SetRetryOnPostEventError()
	lw.StartWatchLoop()
	return lw
}

func (w *Watcher) initializeGroupWatcher(ctx context.Context) error {
	w.groupWatcher = w.newLoopWatcher(ctx, "scheduling-affinity-group-watcher", keypath.AffinityGroupsPrefix(),
		func(events []*clientv3.Event) error {
			for _, event := range events {
				if event.Type == clientv3.EventTypeDelete {
					id := strings.TrimPrefix(string(event.Kv.Key), keypath.AffinityGroupsPrefix())
					if err := w.affinityManager.SyncGroupDeleteFromEtcd(id); err != nil {
						return err
					}
				} else {
					var group affinity.Group
					if err := json.Unmarshal(event.Kv.Value, &group); err != nil {
						return err
					}
					if err := w.affinityManager.SyncGroupFromEtcd(&group); err != nil {
						return err
					}
				}
			}
			return nil
		})
	return w.groupWatcher.WaitLoad()
}

func (w *Watcher) initializeAffinityLabelWatcher(ctx context.Context) error {
	w.labelWatcher = w.newLoopWatcher(ctx, "scheduling-affinity-label-watcher", keypath.RegionLabelKeyPath(affinity.LabelRuleIDPrefix),
		func(events []*clientv3.Event) error {
			for _, event := range events {
				if event.Type == clientv3.EventTypeDelete {
					id := strings.TrimPrefix(string(event.Kv.Key), keypath.RegionLabelPathPrefix())
					if err := w.affinityManager.SyncKeyRangesDeleteFromEtcdEvent(id); err != nil {
						return err
					}
				} else {
					rule, err := labeler.NewLabelRuleFromJSON(event.Kv.Value)
					if err != nil {
						return err
					}
					if err := w.affinityManager.SyncKeyRangesFromEtcdEvent(rule); err != nil {
						return err
					}
				}
			}
			return nil
		})
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
