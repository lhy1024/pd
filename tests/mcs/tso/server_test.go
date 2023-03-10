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

package tso

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mcs/discovery"
	tsosvr "github.com/tikv/pd/pkg/mcs/tso/server"
	tsoapi "github.com/tikv/pd/pkg/mcs/tso/server/apis/v1"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/tests"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

type tsoServerTestSuite struct {
	suite.Suite
	ctx              context.Context
	cancel           context.CancelFunc
	cluster          *tests.TestCluster
	pdLeader         *tests.TestServer
	backendEndpoints string
}

func TestTSOServerTestSuite(t *testing.T) {
	suite.Run(t, new(tsoServerTestSuite))
}

func (suite *tsoServerTestSuite) SetupSuite() {
	var err error
	re := suite.Require()

	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.cluster, err = tests.NewTestCluster(suite.ctx, 1)
	re.NoError(err)

	err = suite.cluster.RunInitialServers()
	re.NoError(err)

	leaderName := suite.cluster.WaitLeader()
	suite.pdLeader = suite.cluster.GetServer(leaderName)
	suite.backendEndpoints = suite.pdLeader.GetAddr()
}

func (suite *tsoServerTestSuite) TearDownSuite() {
	suite.cluster.Destroy()
	suite.cancel()
}

func (suite *tsoServerTestSuite) TestTSOServerStartAndStopNormally() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered from an unexpected panic", r)
			suite.T().Errorf("Expected no panic, but something bad occurred with")
		}
	}()

	re := suite.Require()
	s, cleanup, err := startSingleTSOTestServer(suite.ctx, re, suite.backendEndpoints)
	re.NoError(err)

	defer cleanup()
	testutil.Eventually(re, func() bool {
		return s.IsServing()
	}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))

	// Test registered GRPC Service
	cc, err := grpc.DialContext(suite.ctx, s.GetListenURL().Host, grpc.WithInsecure())
	re.NoError(err)
	cc.Close()
	url := s.GetConfig().ListenAddr + tsoapi.APIPathPrefix
	{
		resetJSON := `{"tso":"121312", "force-use-larger":true}`
		re.NoError(err)
		resp, err := http.Post(url+"/admin/reset-ts", "application/json", strings.NewReader(resetJSON))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)
	}
	{
		resetJSON := `{}`
		re.NoError(err)
		resp, err := http.Post(url+"/admin/reset-ts", "application/json", strings.NewReader(resetJSON))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusBadRequest, resp.StatusCode)
	}
}

func (suite *tsoServerTestSuite) TestTSOServerRegister() {
	re := suite.Require()
	s, cleanup, err := startSingleTSOTestServer(suite.ctx, re, suite.backendEndpoints)
	re.NoError(err)

	serviceName := "tso"
	client := suite.pdLeader.GetEtcdClient()
	endpoints, err := discovery.Discover(client, serviceName)
	re.NoError(err)
	re.Equal(s.GetConfig().ListenAddr, endpoints[0])

	// test API server discovery
	exist, addr, err := suite.pdLeader.GetServer().GetServicePrimaryAddr(suite.ctx, serviceName)
	re.NoError(err)
	re.True(exist)
	re.Equal(s.GetConfig().ListenAddr, addr)

	cleanup()
	endpoints, err = discovery.Discover(client, serviceName)
	re.NoError(err)
	re.Empty(endpoints)
}

func (suite *tsoServerTestSuite) TestTSOPath() {
	re := suite.Require()

	client := suite.pdLeader.GetEtcdClient()
	re.Equal(1, getEtcdTimestampKeyNum(re, client))

	_, cleanup, err := startSingleTSOTestServer(suite.ctx, re, suite.backendEndpoints)
	re.NoError(err)
	defer cleanup()

	cli := setupCli(re, suite.ctx, []string{suite.backendEndpoints})
	physical, logical, err := cli.GetTS(suite.ctx)
	re.NoError(err)
	ts := tsoutil.ComposeTS(physical, logical)
	re.NotEmpty(ts)
	// After we request the tso server, etcd still has only one key related to the timestamp.
	re.Equal(1, getEtcdTimestampKeyNum(re, client))
}

func getEtcdTimestampKeyNum(re *require.Assertions, client *clientv3.Client) int {
	resp, err := etcdutil.EtcdKVGet(client, "/", clientv3.WithPrefix())
	re.NoError(err)
	var count int
	for _, kv := range resp.Kvs {
		key := strings.TrimSpace(string(kv.Key))
		if !strings.HasSuffix(key, "timestamp") {
			continue
		}
		count++
	}
	return count
}

type APIServerForwardTestSuite struct {
	suite.Suite
	ctx              context.Context
	cancel           context.CancelFunc
	cluster          *tests.TestCluster
	pdLeader         *tests.TestServer
	backendEndpoints string
	pdClient         pd.Client
	cleanup          CleanupFunc
}

func TestAPIServerForwardTestSuite(t *testing.T) {
	suite.Run(t, new(APIServerForwardTestSuite))
}

func (suite *APIServerForwardTestSuite) SetupTest() {
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

	suite.pdClient, err = pd.NewClientWithContext(suite.ctx, []string{suite.backendEndpoints}, pd.SecurityOption{})
	re.NoError(err)
}

func (suite *APIServerForwardTestSuite) TearDownTest() {
	etcdClient := suite.pdLeader.GetEtcdClient()
	endpoints, err := discovery.Discover(etcdClient, "tso")
	suite.NoError(err)
	if len(endpoints) != 0 {
		suite.cleanup()
		endpoints, err = discovery.Discover(etcdClient, "tso")
		suite.NoError(err)
		suite.Empty(endpoints)
	}
	suite.cluster.Destroy()
	suite.cancel()
}

func (suite *APIServerForwardTestSuite) TestForwardTSORelated() {
	var err error
	leader := suite.cluster.GetServer(suite.cluster.WaitLeader())
	suite.NoError(leader.BootstrapCluster())
	suite.addRegions()
	// Unable to use the tso-related interface without tso server
	{
		// try to get ts
		_, _, err = suite.pdClient.GetTS(suite.ctx)
		suite.Error(err)
		suite.Contains(err.Error(), "not found tso address")
		// try to update gc safe point
		_, err = suite.pdClient.UpdateServiceGCSafePoint(suite.ctx, "a", 1000, 1)
		suite.Contains(err.Error(), "not found tso address")
		// try to set external ts
		err = suite.pdClient.SetExternalTimestamp(suite.ctx, 1000)
		suite.Contains(err.Error(), "not found tso address")
	}
	// can use the tso-related interface with tso server
	{
		suite.addTSOService()
		// try to get ts
		_, _, err = suite.pdClient.GetTS(suite.ctx)
		suite.NoError(err)
		// try to update gc safe point
		min, err := suite.pdClient.UpdateServiceGCSafePoint(context.Background(),
			"a", 1000, 1)
		suite.NoError(err)
		suite.Equal(uint64(0), min)
		// try to set external ts
		err = suite.pdClient.SetExternalTimestamp(suite.ctx, 1000)
		suite.NoError(err)
	}
}

func (suite *APIServerForwardTestSuite) addTSOService() {
	var s *tsosvr.Server
	var err error
	re := suite.Require()
	s, suite.cleanup, err = startSingleTSOTestServer(suite.ctx, re, suite.backendEndpoints)
	suite.NoError(err)
	etcdClient := suite.pdLeader.GetEtcdClient()
	endpoints, err := discovery.Discover(etcdClient, "tso")
	suite.NoError(err)
	suite.Equal(s.GetConfig().ListenAddr, endpoints[0])
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
