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
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/errors"

	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

type targetWriterFunc func(context.Context, []GroupTarget) ([]string, error)

func (f targetWriterFunc) WriteGroupTargets(ctx context.Context, targets []GroupTarget) ([]string, error) {
	return f(ctx, targets)
}

func TestFirstWinWithOverlappingRanges(t *testing.T) {
	for _, mode := range []string{"classic", "mcs"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				re := require.New(t)
				m, store := newTargetTestManager(t)
				re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}, {GroupID: "h"}}))
				_, err := m.UpdateAffinityGroupPeers("g", 1, []uint64{1, 2, 3})
				re.NoError(err)
				m.ExpireAffinityGroup("g")
				before := *loadStoredGroup(re, store, "g")
				for _, id := range []string{"g", "h"} {
					data, err := json.Marshal(MakeLabelRule(GroupKeyRanges{GroupID: id, KeyRanges: []keyutil.KeyRange{nonOverlappingRange}}))
					re.NoError(err)
					rule, err := labeler.NewLabelRuleFromJSON(data)
					re.NoError(err)
					re.NoError(m.SyncKeyRangesFromEtcd(rule))
				}
				m.ReconcileRangeConflicts()
				re.True(m.GetAffinityGroupState("g").rangeConflict)
				calls := 0
				if mode == "mcs" {
					m.SetTargetWriter(targetWriterFunc(func(context.Context, []GroupTarget) ([]string, error) {
						calls++
						return nil, nil
					}))
				}
				observe := func() {
					m.ObserveAvailableRegion(generateRegionForTest(10, []uint64{2, 1, 3}, nonOverlappingRange), m.GetAffinityGroupState("g"))
					synctest.Wait()
				}
				observe()
				re.Zero(calls)
				re.Equal(before, *loadStoredGroup(re, store, "g"))
				re.Equal(before, m.GetAffinityGroupState("g").Group)

				m.SyncKeyRangesDeleteFromEtcd(GetLabelRuleID("h"))
				m.ReconcileRangeConflicts()
				re.False(m.GetAffinityGroupState("g").rangeConflict)
				observe()
				if mode == "mcs" {
					re.Equal(1, calls)
					re.Equal(before, *loadStoredGroup(re, store, "g"))
				} else {
					re.Equal(uint64(2), loadStoredGroup(re, store, "g").LeaderStoreID)
				}
			})
		})
	}
}

func TestFirstWinSingleFlightAndGlobalBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		m, _ := newTargetTestManager(t)
		re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}, {GroupID: "h"}}))
		calls := 0
		m.SetTargetWriter(targetWriterFunc(func(_ context.Context, targets []GroupTarget) ([]string, error) {
			calls++
			if calls == 1 {
				time.Sleep(3 * time.Second)
				return nil, errors.New("PD unavailable")
			}
			m.SyncGroupFromEtcd(targets[0].result())
			return []string{targets[0].Expected.ID}, nil
		}))
		region := generateRegionForTest(10, []uint64{1, 2}, nonOverlappingRange)
		observe := func(id string) { m.ObserveAvailableRegion(region, m.GetAffinityGroupState(id)) }
		observe("g")
		synctest.Wait()
		for range 10 {
			observe("g")
			observe("h")
		}
		synctest.Wait()
		re.Equal(1, calls, "other groups and repeated Regions are discarded while busy")
		time.Sleep(3 * time.Second)
		synctest.Wait()
		observe("h")
		synctest.Wait()
		re.Equal(1, calls, "failure delays all groups")
		time.Sleep(time.Second)
		region = generateRegionForTest(10, []uint64{1, 3}, nonOverlappingRange)
		observe("h")
		synctest.Wait()
		re.Equal(2, calls)
		re.True(m.GetAffinityGroupState("h").AffinitySchedulingAllowed)
		re.Equal([]uint64{1, 3}, m.GetAffinityGroupState("h").VoterStoreIDs, "patrol replaces the discarded observation")
		re.Zero(m.GetAffinityGroupState("g").LeaderStoreID, "discarded observations are never replayed")
		observe("g")
		synctest.Wait()
		re.Equal(3, calls, "success has no global cooldown")
		re.True(m.GetAffinityGroupState("g").AffinitySchedulingAllowed)
	})
}

func TestFirstWinRecovery(t *testing.T) {
	for _, fault := range []string{"timeout", "lost reply", "delayed watcher", "primary restart", "late reply"} {
		t.Run(fault, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				re := require.New(t)
				pd, store := newTargetTestManager(t)
				re.NoError(pd.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				m := newTargetTestManagerWith(t, ctx, storage.NewStorageWithMemoryBackend(), time.Now())
				m.applyStoreConditions(map[uint64]storeCondition{})
				initial := pd.GetAffinityGroupState("g").Group
				m.SyncGroupFromEtcd(&initial)
				calls := 0
				var sent []GroupTarget
				writer := targetWriterFunc(func(ctx context.Context, targets []GroupTarget) ([]string, error) {
					calls++
					sent = append(sent, targets[0])
					if calls == 1 {
						switch fault {
						case "timeout":
							request, stop := context.WithTimeout(ctx, 3*time.Second)
							defer stop()
							<-request.Done()
							return nil, request.Err()
						case "primary restart":
							<-ctx.Done()
							return nil, ctx.Err()
						}
					}
					written, err := pd.UpdateAffinityGroupTargets(targets)
					if calls == 1 && fault == "lost reply" {
						return nil, errors.New("response lost after PD commit")
					}
					if calls == 1 && fault == "late reply" {
						newer, updateErr := pd.UpdateAffinityGroupPeers("g", 2, []uint64{1, 2})
						if updateErr != nil {
							return nil, updateErr
						}
						m.SyncGroupFromEtcd(&newer.Group)
					}
					return written, err
				})
				m.SetTargetWriter(writer)
				observe := func(voters []uint64) {
					m.ObserveAvailableRegion(generateRegionForTest(10, voters, nonOverlappingRange), m.GetAffinityGroupState("g"))
					synctest.Wait()
				}
				observe([]uint64{1, 2})
				re.Equal(1, calls)
				if fault == "timeout" {
					time.Sleep(3 * time.Second)
					synctest.Wait()
				}
				if fault == "primary restart" {
					cancel()
					synctest.Wait()
					re.False(m.targetWriteInFlight, "the old primary's sender has exited")
					time.Sleep(time.Second)
					observe([]uint64{1, 3})
					re.Equal(1, calls, "a canceled primary cannot start another write")
					m, _ = newTargetTestManager(t)
					m.applyStoreConditions(map[uint64]storeCondition{})
					m.SyncGroupFromEtcd(loadStoredGroup(re, store, "g"))
					m.SetTargetWriter(writer)
				}
				if fault != "late reply" {
					re.Zero(m.GetAffinityGroupState("g").LeaderStoreID, "HTTP completion cannot publish a target")
				}
				time.Sleep(time.Second)
				observe([]uint64{1, 3})
				stored := loadStoredGroup(re, store, "g")
				switch fault {
				case "timeout", "primary restart":
					re.Equal([]uint64{1, 3}, stored.VoterStoreIDs, "a later patrol supplies the new observation")
				case "lost reply", "delayed watcher":
					re.Equal([]uint64{1, 2}, stored.VoterStoreIDs, "an observation with stale Expected cannot overwrite a commit")
				case "late reply":
					re.Equal(uint64(2), m.GetAffinityGroupState("g").LeaderStoreID)
					re.Equal(1, calls, "the watcher already published the admin target")
				}
				written, err := pd.UpdateAffinityGroupTargets([]GroupTarget{sent[len(sent)-1]})
				re.NoError(err)
				re.Equal(fault == "timeout" || fault == "primary restart", len(written) == 1)
				m.SyncGroupFromEtcd(stored)
				re.Equal(*stored, m.GetAffinityGroupState("g").Group)
				re.True(m.GetAffinityGroupState("g").AffinitySchedulingAllowed)
				re.False(m.targetWriteInFlight)
				before := calls
				observe([]uint64{1, 3})
				re.Equal(before, calls, "patrol stops initializing a published healthy group")
			})
		})
	}
}
