// Copyright 2023 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compatibility_test

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	pd "github.com/tikv/pd/client"
	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mcs/discovery"
	tso "github.com/tikv/pd/pkg/mcs/tso/server"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/tests"
)

// This part is to test the function of forwarding tso requests from the old client and the new server.
// After splitting tso in mcs, tso requests sent to api server will be forwarded directly to tso server
// in order to be compatible with the behavior of the old client. For the new client, it will connect
// to the tso server autonomously, so when testing, we need to test with the 6.6 version of the client
// to make sure we are using the api server forwarding.

type APIServerForwardTestSuite struct {
	suite.Suite
	ctx              context.Context
	cancel           context.CancelFunc
	cluster          *tests.TestCluster
	pdLeader         *tests.TestServer
	backendEndpoints string
	pdClient         pd.Client
}

func TestAPIServerForwardTestSuite(t *testing.T) {
	suite.Run(t, new(APIServerForwardTestSuite))
}

func (suite *APIServerForwardTestSuite) SetupSuite() {
	var err error
	re := suite.Require()
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.cluster, err = tests.NewTestAPICluster(suite.ctx, 1)
	re.NoError(err)

	err = suite.cluster.RunInitialServers()
	re.NoError(err)

	leaderName := suite.cluster.WaitLeader()
	suite.pdLeader = suite.cluster.GetServer(leaderName)
	suite.backendEndpoints = suite.pdLeader.GetAddr()
	suite.NoError(suite.pdLeader.BootstrapCluster())
	suite.addRegions()

	suite.pdClient, err = pd.NewClientWithContext(context.Background(),
		[]string{suite.backendEndpoints}, pd.SecurityOption{}, pd.WithMaxErrorRetry(1))
	suite.NoError(err)
}

func (suite *APIServerForwardTestSuite) TearDownSuite() {
	suite.pdClient.Close()
	etcdClient := suite.pdLeader.GetEtcdClient()
	clusterID := strconv.FormatUint(suite.pdLeader.GetClusterID(), 10)
	endpoints, err := discovery.Discover(etcdClient, clusterID, utils.TSOServiceName)
	suite.NoError(err)
	if len(endpoints) != 0 {
		endpoints, err = discovery.Discover(etcdClient, clusterID, utils.TSOServiceName)
		suite.NoError(err)
		suite.Empty(endpoints)
	}
	suite.cluster.Destroy()
	suite.cancel()
}

func (suite *APIServerForwardTestSuite) TestForwardTSORelated() {
	// Unable to use the tso-related interface without tso server
	suite.checkUnavailableTSO()
	// can use the tso-related interface with tso server
	s, cleanup := startSingleTSOTestServer(suite.ctx, suite.Require(), suite.backendEndpoints, tempurl.Alloc())
	serverMap := make(map[string]bs.Server)
	serverMap[s.GetAddr()] = s
	waitForPrimaryServing(suite.Require(), serverMap)
	suite.checkAvailableTSO()
	cleanup()
}

func (suite *APIServerForwardTestSuite) TestForwardTSOWhenPrimaryChanged() {
	serverMap := make(map[string]bs.Server)
	for i := 0; i < 3; i++ {
		s, cleanup := startSingleTSOTestServer(suite.ctx, suite.Require(), suite.backendEndpoints, tempurl.Alloc())
		defer cleanup()
		serverMap[s.GetAddr()] = s
	}
	waitForPrimaryServing(suite.Require(), serverMap)

	// can use the tso-related interface with new primary
	oldPrimary, exist := suite.pdLeader.GetServer().GetServicePrimaryAddr(suite.ctx, utils.TSOServiceName)
	suite.True(exist)
	serverMap[oldPrimary].Close()
	delete(serverMap, oldPrimary)
	time.Sleep(time.Duration(utils.DefaultLeaderLease) * time.Second) // wait for leader lease timeout
	waitForPrimaryServing(suite.Require(), serverMap)
	primary, exist := suite.pdLeader.GetServer().GetServicePrimaryAddr(suite.ctx, utils.TSOServiceName)
	suite.True(exist)
	suite.NotEqual(oldPrimary, primary)
	suite.checkAvailableTSO()

	// can use the tso-related interface with old primary again
	s, cleanup := startSingleTSOTestServer(suite.ctx, suite.Require(), suite.backendEndpoints, oldPrimary)
	defer cleanup()
	serverMap[oldPrimary] = s
	suite.checkAvailableTSO()
	for addr, s := range serverMap {
		if addr != oldPrimary {
			s.Close()
			delete(serverMap, addr)
		}
	}
	waitForPrimaryServing(suite.Require(), serverMap)
	time.Sleep(time.Duration(utils.DefaultLeaderLease) * time.Second) // wait for leader lease timeout
	primary, exist = suite.pdLeader.GetServer().GetServicePrimaryAddr(suite.ctx, utils.TSOServiceName)
	suite.True(exist)
	suite.Equal(oldPrimary, primary)
	suite.checkAvailableTSO()
}

func (suite *APIServerForwardTestSuite) TestForwardTSOUnexpectedToFollower() {
	serverMap := make(map[string]bs.Server)
	for i := 0; i < 3; i++ {
		s, cleanup := startSingleTSOTestServer(suite.ctx, suite.Require(), suite.backendEndpoints, tempurl.Alloc())
		defer cleanup()
		serverMap[s.GetAddr()] = s
	}
	waitForPrimaryServing(suite.Require(), serverMap)
	_, _, err := suite.pdClient.GetTS(suite.ctx)
	suite.NoError(err)

	// write follower's address to cache to simulate cache is not updated.
	oldPrimary, exist := suite.pdLeader.GetServer().GetServicePrimaryAddr(suite.ctx, utils.TSOServiceName)
	suite.True(exist)
	var follower string
	for addr := range serverMap {
		if addr != oldPrimary {
			follower = addr
			break
		}
	}
	suite.pdLeader.GetServer().SetServicePrimaryAddr(utils.TSOServiceName, follower)
	errorAddr, ok := suite.pdLeader.GetServer().GetServicePrimaryAddr(suite.ctx, utils.TSOServiceName)
	suite.True(ok)
	suite.Equal(follower, errorAddr)

	// test tso request will fail
	suite.pdClient.Close()
	suite.pdClient, err = pd.NewClientWithContext(context.Background(),
		[]string{suite.backendEndpoints}, pd.SecurityOption{}, pd.WithMaxErrorRetry(1))
	suite.NoError(err)
	_, _, err = suite.pdClient.GetTS(suite.ctx)
	suite.Error(err) // client error
	_, _, err = suite.pdClient.GetTS(suite.ctx)
	suite.NoError(err)
	newPrimary, exist2 := suite.pdLeader.GetServer().GetServicePrimaryAddr(suite.ctx, utils.TSOServiceName)
	suite.True(exist2)
	suite.Equal(oldPrimary, newPrimary)
}

func (suite *APIServerForwardTestSuite) addRegions() {
	leader := suite.cluster.GetServer(suite.cluster.WaitLeader())
	rc := leader.GetServer().GetRaftCluster()
	for i := 0; i < 3; i++ {
		region := &metapb.Region{
			Id:       uint64(i*4 + 1),
			Peers:    []*metapb.Peer{{Id: uint64(i*4 + 2), StoreId: uint64(i*4 + 3)}},
			StartKey: []byte{byte(i)},
			EndKey:   []byte{byte(i + 1)},
		}
		rc.HandleRegionHeartbeat(core.NewRegionInfo(region, region.Peers[0]))
	}
}

func (suite *APIServerForwardTestSuite) checkUnavailableTSO() {
	_, _, err := suite.pdClient.GetTS(suite.ctx)
	suite.Error(err)
	// try to update gc safe point
	_, err = suite.pdClient.UpdateServiceGCSafePoint(suite.ctx, "a", 1000, 1)
	suite.Error(err)
	// try to set external ts
	err = suite.pdClient.SetExternalTimestamp(suite.ctx, 1000)
	suite.Error(err)
}

func (suite *APIServerForwardTestSuite) checkAvailableTSO() {
	// try to get ts
	_, _, err := suite.pdClient.GetTS(suite.ctx)
	suite.NoError(err)
	// try to update gc safe point
	min, err := suite.pdClient.UpdateServiceGCSafePoint(context.Background(), "a", 1000, 1)
	suite.NoError(err)
	suite.Equal(uint64(0), min)
	// try to set external ts
	ts, err := suite.pdClient.GetExternalTimestamp(suite.ctx)
	suite.NoError(err)
	err = suite.pdClient.SetExternalTimestamp(suite.ctx, ts+1)
	suite.NoError(err)
}

// waitForPrimaryServing waits for one of servers being elected to be the primary/leader
func waitForPrimaryServing(re *require.Assertions, serverMap map[string]bs.Server) {
	testutil.Eventually(re, func() bool {
		for _, s := range serverMap {
			if s.IsServing() {
				return true
			}
		}
		return false
	}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))
}

// startSingleTSOTestServer creates and starts a tso server with default config for testing.
func startSingleTSOTestServer(ctx context.Context, re *require.Assertions, backendEndpoints, listenAddrs string) (*tso.Server, func()) {
	cfg := tso.NewConfig()
	cfg.BackendEndpoints = backendEndpoints
	cfg.ListenAddr = listenAddrs
	cfg, err := tso.GenerateConfig(cfg)
	re.NoError(err)

	s, cleanup, err := newTSOTestServer(ctx, cfg)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		return !s.IsClosed()
	}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))

	return s, cleanup
}

func newTSOTestServer(ctx context.Context, cfg *tso.Config) (*tso.Server, testutil.CleanupFunc, error) {
	s := tso.CreateServer(ctx, cfg)
	if err := s.Run(); err != nil {
		return nil, nil, err
	}
	cleanup := func() {
		s.Close()
		os.RemoveAll(cfg.DataDir)
	}
	return s, cleanup, nil
}
