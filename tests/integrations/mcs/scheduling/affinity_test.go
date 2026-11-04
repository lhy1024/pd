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

package scheduling

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

func TestAffinityFirstWinTargetWriteUnavailable(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()
	const groupID = "c08-target-write"
	const targetsPath = "/pd/api/v2/affinity-groups/targets"

	// Install before starting servers, since their clients can already be in use
	// once scheduling becomes primary. Only targets use the proxy; heartbeats,
	// etcd watches and other HTTP requests keep their route.
	original := http.DefaultTransport
	transport := original.(*http.Transport).Clone()
	var blocked atomic.Bool
	var failures atomic.Int64
	blocked.Store(true)
	forward := &httputil.ReverseProxy{Rewrite: func(r *httputil.ProxyRequest) { r.Out.URL = r.In.URL }, Transport: original}
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if blocked.Load() {
			failures.Add(1)
			http.Error(w, "C08 target writes blocked", http.StatusServiceUnavailable)
			return
		}
		forward.ServeHTTP(w, r)
	}))
	t.Cleanup(proxy.Close)
	proxyURL, err := url.Parse(proxy.URL)
	re.NoError(err)
	transport.Proxy = func(r *http.Request) (*url.URL, error) {
		if r.Method == http.MethodPost && r.URL.Path == targetsPath {
			return proxyURL, nil
		}
		if original.(*http.Transport).Proxy != nil {
			return original.(*http.Transport).Proxy(r)
		}
		return nil, nil
	}
	http.DefaultTransport = transport
	t.Cleanup(func() {
		http.DefaultTransport = original
		transport.CloseIdleConnections()
	})
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/changeCoordinatorTicker", "return(true)"))
	t.Cleanup(func() { re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/changeCoordinatorTicker")) })
	pd, err := tests.NewTestClusterWithKeyspaceGroup(ctx, 1, func(c *config.Config, _ string) {
		c.Schedule.AffinityScheduleLimit = 4
	})
	re.NoError(err)
	t.Cleanup(pd.Destroy)
	re.NoError(pd.RunInitialServers())
	re.NotEmpty(pd.WaitLeader())
	leader := pd.GetLeaderServer()
	re.NoError(leader.BootstrapCluster())
	tc, err := tests.NewTestSchedulingCluster(ctx, 1, pd)
	re.NoError(err)
	t.Cleanup(tc.Destroy)
	primary := tc.WaitForPrimaryServing(re)
	pd.SetSchedulingCluster(tc)
	cluster := primary.GetCluster()
	cluster.SetPrepared()
	for id := uint64(1); id <= 3; id++ {
		tests.MustPutStore(re, pd, &metapb.Store{Id: id, State: metapb.StoreState_Up, NodeState: metapb.NodeState_Serving})
		// MustPutStore's default statistics are low-space. Keep first-win
		// independent of when the availability check first inspects the group.
		tests.MustHandleStoreHeartbeat(re, pd, &pdpb.StoreHeartbeatRequest{
			Header: testutil.NewRequestHeader(leader.GetClusterID()),
			Stats:  &pdpb.StoreStats{StoreId: id, Capacity: 10 * units.GiB, Available: 9 * units.GiB, UsedSize: units.GiB},
		})
	}
	pdManager, err := leader.GetServer().GetAffinityManager()
	re.NoError(err)
	// Avoid bootstrap keyspace label boundaries, which would make the split
	// checker wait for the simulated TiKV to execute a split before first-win.
	re.NoError(pdManager.CreateAffinityGroups([]affinity.GroupKeyRanges{{GroupID: groupID,
		KeyRanges: []keyutil.KeyRange{{StartKey: []byte("b"), EndKey: []byte("c")}},
	}}))
	manager := cluster.GetAffinityManager()
	testutil.Eventually(re, func() bool {
		g := manager.GetAffinityGroupState(groupID)
		return g != nil && g.RangeCount == 1 && cluster.GetStore(3) != nil
	})
	observerCore, logs := observer.New(zap.WarnLevel)
	restore := log.ReplaceGlobals(zap.New(observerCore), &log.ZapProperties{Level: zap.NewAtomicLevelAt(zap.WarnLevel)})
	t.Cleanup(restore)
	counter := func() float64 {
		metrics, err := prometheus.DefaultGatherer.Gather()
		re.NoError(err)
		for _, metric := range metrics {
			if metric.GetName() == "pd_affinity_target_write_failures_total" {
				return metric.GetMetric()[0].GetCounter().GetValue()
			}
		}
		re.FailNow("target write failure metric not registered")
		return 0
	}
	before := counter()

	client, conn := testutil.MustNewGrpcClient(re, leader.GetAddr())
	t.Cleanup(func() { re.NoError(conn.Close()) })
	stream, err := client.RegionHeartbeat(ctx)
	re.NoError(err)
	t.Cleanup(func() { _ = stream.CloseSend() })
	peers := []*metapb.Peer{{Id: 11, StoreId: 1}, {Id: 12, StoreId: 2}, {Id: 13, StoreId: 3}}
	heartbeat := func() {
		re.NoError(stream.Send(&pdpb.RegionHeartbeatRequest{
			Header: testutil.NewRequestHeader(leader.GetClusterID()),
			Region: &metapb.Region{Id: 100, StartKey: []byte("b"), EndKey: []byte("c"),
				RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1}, Peers: peers},
			Leader: peers[0], ApproximateSize: 1, ApproximateKeys: 1,
		}))
	}
	loadStored := func() *affinity.Group {
		value, err := leader.GetServer().GetStorage().LoadAffinityGroup(groupID)
		re.NoError(err)
		var group affinity.Group
		re.NoError(json.Unmarshal([]byte(value), &group))
		return &group
	}
	testutil.Eventually(re, func() bool {
		heartbeat()
		return failures.Load() > 0 && counter() > before &&
			logs.FilterLevelExact(zap.WarnLevel).FilterMessage("failed to write affinity group target").FilterField(zap.String("group-id", groupID)).Len() > 0
	})
	re.NotNil(cluster.GetRegion(100), "Region heartbeats must still reach scheduling")
	response, err := primary.GetHTTPClient().Get(leader.GetAddr() + "/health")
	re.NoError(err)
	re.NoError(response.Body.Close())
	re.Equal(http.StatusOK, response.StatusCode, "other scheduling-to-PD HTTP requests must still work")
	for _, group := range []*affinity.GroupState{pdManager.GetAffinityGroupState(groupID), manager.GetAffinityGroupState(groupID)} {
		re.NotNil(group)
		re.Zero(group.LeaderStoreID, "failed target writes must not be published")
		re.Empty(group.VoterStoreIDs)
		re.False(group.AffinitySchedulingAllowed)
		re.Equal(affinity.PhasePending, group.Phase)
	}
	re.Zero(loadStored().LeaderStoreID)
	re.Empty(loadStored().VoterStoreIDs)

	blocked.Store(false)
	testutil.Eventually(re, func() bool {
		heartbeat()
		stored := loadStored()
		pdGroup := pdManager.GetAffinityGroupState(groupID)
		schedulingGroup := manager.GetAffinityGroupState(groupID)
		return stored.LeaderStoreID == 1 && len(stored.VoterStoreIDs) == 3 &&
			pdGroup != nil && schedulingGroup != nil && pdGroup.AffinitySchedulingAllowed && schedulingGroup.AffinitySchedulingAllowed &&
			stored.String() == pdGroup.String() && stored.String() == schedulingGroup.String()
	}, testutil.WithWaitFor(30*time.Second))
	re.Equal([]uint64{1, 2, 3}, loadStored().VoterStoreIDs)
}

func TestAffinityFirstWinAfterPDLeaderStops(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/changeCoordinatorTicker", "return(true)"))
	t.Cleanup(func() { re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/changeCoordinatorTicker")) })
	pd, err := tests.NewTestClusterWithKeyspaceGroup(ctx, 3, func(c *config.Config, _ string) {
		c.Schedule.AffinityScheduleLimit = 4
	})
	re.NoError(err)
	t.Cleanup(pd.Destroy)
	re.NoError(pd.RunInitialServers())
	leader := firstMemberPDLeader(t, pd)
	leaderName := pd.WaitLeader()
	re.NotEmpty(leaderName)
	re.NoError(leader.BootstrapCluster())
	tc, err := tests.NewTestSchedulingCluster(ctx, 1, pd)
	re.NoError(err)
	t.Cleanup(tc.Destroy)
	primary := tc.WaitForPrimaryServing(re)
	pd.SetSchedulingCluster(tc)
	cluster := primary.GetCluster()
	cluster.SetPrepared()
	for id := uint64(1); id <= 3; id++ {
		tests.MustPutStore(re, pd, &metapb.Store{Id: id, State: metapb.StoreState_Up, NodeState: metapb.NodeState_Serving})
		// MustPutStore's default statistics are low-space. Keep first-win
		// independent of when the availability check first inspects the group.
		tests.MustHandleStoreHeartbeat(re, pd, &pdpb.StoreHeartbeatRequest{
			Header: testutil.NewRequestHeader(leader.GetClusterID()),
			Stats:  &pdpb.StoreStats{StoreId: id, Capacity: 10 * units.GiB, Available: 9 * units.GiB, UsedSize: units.GiB},
		})
	}
	manager := cluster.GetAffinityManager()
	// Establish membership before the experiment, independently of a failed
	// target write that might otherwise discover the initial leader.
	testutil.Eventually(re, func() bool {
		_, _, err := cluster.AllocID(1)
		return err == nil
	})
	firstWin := func(id uint64, start, end string) {
		groupID := fmt.Sprintf("leader-switch-%d", id)
		pdManager, err := leader.GetServer().GetAffinityManager()
		re.NoError(err)
		// As in C08, avoid the bootstrap keyspace label boundaries.
		re.NoError(pdManager.CreateAffinityGroups([]affinity.GroupKeyRanges{{GroupID: groupID,
			KeyRanges: []keyutil.KeyRange{{StartKey: []byte(start), EndKey: []byte(end)}},
		}}))
		testutil.Eventually(re, func() bool {
			g := manager.GetAffinityGroupState(groupID)
			return g != nil && g.RangeCount == 1 && cluster.GetStore(3) != nil
		})
		// Reconnect to the new PD leader after stopping the previous one.
		client, conn := testutil.MustNewGrpcClient(re, leader.GetAddr())
		defer func() { re.NoError(conn.Close()) }()
		stream, err := client.RegionHeartbeat(ctx)
		re.NoError(err)
		defer func() { _ = stream.CloseSend() }()
		peers := []*metapb.Peer{{Id: id*10 + 1, StoreId: 1}, {Id: id*10 + 2, StoreId: 2}, {Id: id*10 + 3, StoreId: 3}}
		testutil.Eventually(re, func() bool {
			re.NoError(stream.Send(&pdpb.RegionHeartbeatRequest{
				Header: testutil.NewRequestHeader(leader.GetClusterID()),
				Region: &metapb.Region{Id: id, StartKey: []byte(start), EndKey: []byte(end),
					RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1}, Peers: peers},
				Leader: peers[0], ApproximateSize: 1, ApproximateKeys: 1,
			}))
			value, err := leader.GetServer().GetStorage().LoadAffinityGroup(groupID)
			re.NoError(err)
			var stored affinity.Group
			re.NoError(json.Unmarshal([]byte(value), &stored))
			pdGroup := pdManager.GetAffinityGroupState(groupID)
			schedulingGroup := manager.GetAffinityGroupState(groupID)
			return stored.LeaderStoreID == 1 && len(stored.VoterStoreIDs) == 3 &&
				pdGroup != nil && schedulingGroup != nil && pdGroup.AffinitySchedulingAllowed && schedulingGroup.AffinitySchedulingAllowed &&
				stored.String() == pdGroup.String() && stored.String() == schedulingGroup.String()
		}, testutil.WithWaitFor(20*time.Second))
	}
	// A successful write establishes that scheduling has learned the old URL.
	firstWin(100, "b", "c")
	writeFailures := func() float64 {
		metrics, err := prometheus.DefaultGatherer.Gather()
		re.NoError(err)
		for _, metric := range metrics {
			if metric.GetName() == "pd_affinity_target_write_failures_total" {
				return metric.GetMetric()[0].GetCounter().GetValue()
			}
		}
		re.FailNow("target write failure metric not registered")
		return 0
	}
	before := writeFailures()
	re.NoError(leader.Stop())
	newLeaderName := pd.WaitLeaderChange(leaderName)
	re.NotEmpty(newLeaderName)
	leader = pd.GetServer(newLeaderName)
	// Do not enable fastUpdateMember: failed writes must refresh membership
	// before the default one-minute membership ticker can rescue the write.
	firstWin(101, "d", "e")
	re.Greater(writeFailures(), before, "writing to the stopped leader must trigger membership refresh")
}

// TestAffinitySplitRecovery exercises the store-facing PD stream in both modes,
// including forwarding ChangeSplit from scheduling through PD in MCS mode.
func TestAffinitySplitRecovery(t *testing.T) {
	for _, mcs := range []bool{false, true} {
		name := "classic"
		if mcs {
			name = "mcs"
		}
		t.Run(name, func(t *testing.T) {
			re := require.New(t)
			configure := func(cfg *config.Config, _ string) {
				cfg.Schedule.AffinityScheduleLimit = 4
				cfg.Schedule.MaxAffinityMergeRegionSize = 10
				cfg.Schedule.Schedulers = nil
			}
			newCluster := tests.NewTestCluster
			if mcs {
				newCluster = tests.NewTestClusterWithKeyspaceGroup
			}
			pd, err := newCluster(t.Context(), 1, configure)
			re.NoError(err)
			t.Cleanup(pd.Destroy)
			re.NoError(pd.RunInitialServers())
			re.NotEmpty(pd.WaitLeader())
			leader := pd.GetLeaderServer()
			re.NoError(leader.BootstrapCluster())
			rc := leader.GetRaftCluster()
			manager := rc.GetAffinityManager()
			streams := rc.GetHeartbeatStreams()
			getRegion := rc.GetRegion
			if mcs {
				scheduling, err := tests.NewTestSchedulingCluster(t.Context(), 1, pd)
				re.NoError(err)
				t.Cleanup(scheduling.Destroy)
				scheduling.WaitForPrimaryServing(re)
				cluster := scheduling.GetPrimaryServer().GetCluster()
				manager = cluster.GetAffinityManager()
				streams = cluster.GetHeartbeatStreams()
				getRegion = cluster.GetRegion
				testutil.Eventually(re, func() bool {
					return cluster.GetSharedConfig().GetAffinityScheduleLimit() == 4 &&
						cluster.GetSharedConfig().GetMaxAffinityMergeRegionSize() == 10
				})
			}

			client, conn := testutil.MustNewGrpcClient(re, leader.GetAddr())
			t.Cleanup(func() { re.NoError(conn.Close()) })
			header := testutil.NewRequestHeader(leader.GetClusterID())
			for id := uint64(1); id <= 3; id++ {
				resp, err := client.PutStore(t.Context(), &pdpb.PutStoreRequest{
					Header: header,
					Store: &metapb.Store{
						Id: id, Address: fmt.Sprintf("mock://tikv-%d", id), Version: "7.0.0",
						NodeState: metapb.NodeState_Serving, LastHeartbeat: time.Now().UnixNano(),
					},
				})
				re.NoError(err)
				re.Nil(resp.GetHeader().GetError())
				hb, err := client.StoreHeartbeat(t.Context(), &pdpb.StoreHeartbeatRequest{
					Header: header, Stats: &pdpb.StoreStats{StoreId: id, Capacity: 100 * units.GiB, Available: 90 * units.GiB},
				})
				re.NoError(err)
				re.Nil(hb.GetHeader().GetError())
			}
			// Keep the range below bootstrap keyspace label boundaries.
			re.NoError(rc.GetAffinityManager().CreateAffinityGroups([]affinity.GroupKeyRanges{{
				GroupID: "split-recovery", KeyRanges: []keyutil.KeyRange{{StartKey: []byte("a"), EndKey: []byte("c")}},
			}}))
			_, err = rc.GetAffinityManager().UpdateAffinityGroupPeers("split-recovery", 1, []uint64{1, 2, 3})
			re.NoError(err)
			testutil.Eventually(re, func() bool {
				g := manager.GetAffinityGroupState("split-recovery")
				return g != nil && g.RangeCount == 1 && !g.RegularSchedulingAllowed
			})

			ctx, cancel := context.WithCancel(t.Context())
			heartbeats, err := client.RegionHeartbeat(ctx)
			re.NoError(err)
			responses := make(chan *pdpb.RegionHeartbeatResponse, 16)
			receiveErrors := make(chan error, 1)
			done := make(chan struct{})
			go func() {
				defer close(done)
				for {
					resp, err := heartbeats.Recv()
					if err != nil {
						receiveErrors <- err
						return
					}
					select {
					case responses <- resp:
					case <-ctx.Done():
						return
					}
				}
			}()
			t.Cleanup(func() {
				cancel()
				<-done
			})
			peers := []*metapb.Peer{{Id: 11, StoreId: 1}, {Id: 12, StoreId: 2}, {Id: 13, StoreId: 3}}
			req := &pdpb.RegionHeartbeatRequest{
				Header: header,
				Region: &metapb.Region{
					Id: 100, StartKey: []byte("a"), EndKey: []byte("b"), Peers: peers,
					RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
				},
				Leader: peers[0], ApproximateSize: 5 * units.MiB, ApproximateKeys: 500,
			}
			re.NoError(heartbeats.Send(req))
			testutil.Eventually(re, func() bool {
				region := getRegion(100)
				return region != nil && !manager.AllowSplit(region, pdpb.SplitReason_SIZE)
			})
			// A round trip also ensures asynchronous stream binding has completed.
			testutil.Eventually(re, func() bool {
				streams.SendMsg(getRegion(100), &hbstream.Operation{})
				select {
				case resp := <-responses:
					return resp.GetRegionId() == 100
				case err := <-receiveErrors:
					re.NoError(err)
					return false
				default:
					return false
				}
			})
			split, err := client.AskBatchSplit(t.Context(), &pdpb.AskBatchSplitRequest{
				Header: header, Region: req.GetRegion(), SplitCount: 1, Reason: pdpb.SplitReason_SIZE,
			})
			re.NoError(err)
			re.Contains(split.GetHeader().GetError().GetMessage(), "cannot split affinity region")
			re.Empty(split.GetIds())
			waitChangeSplit := func(enabled bool) {
				t.Helper()
				testutil.Eventually(re, func() bool {
					// Reuse unchanged metadata: recovery must not depend on cache updates.
					re.NoError(heartbeats.Send(req))
					select {
					case resp := <-responses:
						if resp.GetRegionId() != 100 || resp.GetChangeSplit() == nil {
							return false
						}
						re.Equal(enabled, resp.GetChangeSplit().GetAutoSplitEnabled())
						re.Equal(req.GetRegion().GetRegionEpoch(), resp.GetRegionEpoch())
						re.Equal(req.GetLeader(), resp.GetTargetPeer())
						return true
					case err := <-receiveErrors:
						re.NoError(err)
						return false
					default:
						return false
					}
				}, testutil.WithWaitFor(10*time.Second))
			}
			waitChangeSplit(false)
			re.NoError(rc.GetAffinityManager().DeleteAffinityGroups([]string{"split-recovery"}, true))
			testutil.Eventually(re, func() bool { return manager.GetAffinityGroupState("split-recovery") == nil })
			waitChangeSplit(true)
		})
	}
}
