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

package pd

import (
	"context"
	"crypto/tls"
	"errors"
	"log"
	"net"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/grpcutil"
	"github.com/tikv/pd/client/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "google.golang.org/grpc/examples/helloworld/helloworld"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
)

type testGRPCServer struct {
	pb.UnimplementedGreeterServer
	leaderConn   *grpc.ClientConn
	leaderAddr   string
	handleCount  atomic.Int32
	forwardCount atomic.Int32
	isLeader     bool
}

// SayHello implements helloworld.GreeterServer
func (s *testGRPCServer) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	if !s.isLeader {
		if !grpcutil.IsFollowerHandleEnabled(ctx, metadata.FromIncomingContext) {
			if addr := grpcutil.GetForwardedHost(ctx, metadata.FromIncomingContext); addr == s.leaderAddr {
				s.forwardCount.Add(1)
				return pb.NewGreeterClient(s.leaderConn).SayHello(ctx, in)
			}
			return nil, errors.New("not leader")
		}
	}
	s.handleCount.Add(1)
	return &pb.HelloReply{Message: "Hello " + in.GetName()}, nil
}

func (s *testGRPCServer) resetCount() {
	s.handleCount.Store(0)
	s.forwardCount.Store(0)
}

func (s *testGRPCServer) getHandleCount() int32 {
	return s.handleCount.Load()
}

func (s *testGRPCServer) getForwardCount() int32 {
	return s.forwardCount.Load()
}

type testServer struct {
	server     *testGRPCServer
	grpcServer *grpc.Server
	addr       string
}

func newTestServer(isLeader bool) *testServer {
	addr := testutil.Alloc()
	u, err := url.Parse(addr)
	if err != nil {
		return nil
	}
	grpcServer := grpc.NewServer()
	server := &testGRPCServer{
		isLeader: isLeader,
	}
	pb.RegisterGreeterServer(grpcServer, server)
	hsrv := health.NewServer()
	hsrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, hsrv)
	return &testServer{
		server:     server,
		grpcServer: grpcServer,
		addr:       u.Host,
	}
}

func (s *testServer) run() {
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
	if err := s.grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

type serviceClientTestSuite struct {
	suite.Suite
	ctx   context.Context
	clean context.CancelFunc

	leaderServer   *testServer
	followerServer *testServer

	leaderClient   ServiceClient
	followerClient ServiceClient
}

func TestServiceClientClientTestSuite(t *testing.T) {
	suite.Run(t, new(serviceClientTestSuite))
}

func (suite *serviceClientTestSuite) SetupSuite() {
	suite.ctx, suite.clean = context.WithCancel(context.Background())

	suite.leaderServer = newTestServer(true)
	suite.followerServer = newTestServer(false)
	go suite.leaderServer.run()
	go suite.followerServer.run()
	for i := 0; i < 10; i++ {
		leaderConn, err1 := grpc.Dial(suite.leaderServer.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		followerConn, err2 := grpc.Dial(suite.followerServer.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err1 == nil && err2 == nil {
			suite.followerClient = newPDServiceClient(
				modifyURLScheme(suite.followerServer.addr, nil),
				modifyURLScheme(suite.leaderServer.addr, nil),
				followerConn, false)
			suite.leaderClient = newPDServiceClient(
				modifyURLScheme(suite.leaderServer.addr, nil),
				modifyURLScheme(suite.leaderServer.addr, nil),
				leaderConn, true)
			suite.followerServer.server.leaderConn = suite.leaderClient.GetClientConn()
			suite.followerServer.server.leaderAddr = suite.leaderClient.GetURL()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	suite.NotNil(suite.leaderClient)
}

func (suite *serviceClientTestSuite) TearDownTest() {
	suite.leaderServer.server.resetCount()
	suite.followerServer.server.resetCount()
}

func (suite *serviceClientTestSuite) TearDownSuite() {
	suite.leaderServer.grpcServer.GracefulStop()
	suite.followerServer.grpcServer.GracefulStop()
	suite.leaderClient.GetClientConn().Close()
	suite.followerClient.GetClientConn().Close()
	suite.clean()
}

func (suite *serviceClientTestSuite) TestServiceClient() {
	re := suite.Require()
	leaderAddress := modifyURLScheme(suite.leaderServer.addr, nil)
	followerAddress := modifyURLScheme(suite.followerServer.addr, nil)

	follower := suite.followerClient
	leader := suite.leaderClient

	re.Equal(follower.GetURL(), followerAddress)
	re.Equal(leader.GetURL(), leaderAddress)

	re.True(follower.Available())
	re.True(leader.Available())

	re.False(follower.IsConnectedToLeader())
	re.True(leader.IsConnectedToLeader())

	re.NoError(failpoint.Enable("github.com/tikv/pd/client/unreachableNetwork1", "return(true)"))
	follower.(*pdServiceClient).checkNetworkAvailable(suite.ctx)
	leader.(*pdServiceClient).checkNetworkAvailable(suite.ctx)
	re.False(follower.Available())
	re.False(leader.Available())
	re.NoError(failpoint.Disable("github.com/tikv/pd/client/unreachableNetwork1"))

	follower.(*pdServiceClient).checkNetworkAvailable(suite.ctx)
	leader.(*pdServiceClient).checkNetworkAvailable(suite.ctx)
	re.True(follower.Available())
	re.True(leader.Available())

	followerConn := follower.GetClientConn()
	leaderConn := leader.GetClientConn()
	re.NotNil(followerConn)
	re.NotNil(leaderConn)

	_, err := pb.NewGreeterClient(followerConn).SayHello(suite.ctx, &pb.HelloRequest{Name: "pd"})
	re.ErrorContains(err, errs.NotLeaderErr)
	resp, err := pb.NewGreeterClient(leaderConn).SayHello(suite.ctx, &pb.HelloRequest{Name: "pd"})
	re.NoError(err)
	re.Equal("Hello pd", resp.GetMessage())

	re.False(follower.NeedRetry(nil, nil))
	re.False(leader.NeedRetry(nil, nil))

	ctx1 := context.WithoutCancel(suite.ctx)
	ctx1 = follower.BuildGRPCTargetContext(ctx1, false)
	re.True(grpcutil.IsFollowerHandleEnabled(ctx1, metadata.FromOutgoingContext))
	re.Empty(grpcutil.GetForwardedHost(ctx1, metadata.FromOutgoingContext))
	ctx2 := context.WithoutCancel(suite.ctx)
	ctx2 = follower.BuildGRPCTargetContext(ctx2, true)
	re.False(grpcutil.IsFollowerHandleEnabled(ctx2, metadata.FromOutgoingContext))
	re.Equal(grpcutil.GetForwardedHost(ctx2, metadata.FromOutgoingContext), leaderAddress)

	ctx3 := context.WithoutCancel(suite.ctx)
	ctx3 = leader.BuildGRPCTargetContext(ctx3, false)
	re.False(grpcutil.IsFollowerHandleEnabled(ctx3, metadata.FromOutgoingContext))
	re.Empty(grpcutil.GetForwardedHost(ctx3, metadata.FromOutgoingContext))
	ctx4 := context.WithoutCancel(suite.ctx)
	ctx4 = leader.BuildGRPCTargetContext(ctx4, true)
	re.False(grpcutil.IsFollowerHandleEnabled(ctx4, metadata.FromOutgoingContext))
	re.Empty(grpcutil.GetForwardedHost(ctx4, metadata.FromOutgoingContext))

	followerAPIClient := newPDServiceAPIClient(follower, regionAPIErrorFn)
	leaderAPIClient := newPDServiceAPIClient(leader, regionAPIErrorFn)

	re.NoError(failpoint.Enable("github.com/tikv/pd/client/fastCheckAvailable", "return(true)"))

	re.True(followerAPIClient.Available())
	re.True(leaderAPIClient.Available())
	pdErr1 := &pdpb.Error{
		Type: pdpb.ErrorType_UNKNOWN,
	}
	pdErr2 := &pdpb.Error{
		Type: pdpb.ErrorType_REGION_NOT_FOUND,
	}
	err = errors.New("error")
	re.True(followerAPIClient.NeedRetry(pdErr1, nil))
	re.False(leaderAPIClient.NeedRetry(pdErr1, nil))
	re.True(followerAPIClient.Available())
	re.True(leaderAPIClient.Available())

	re.True(followerAPIClient.NeedRetry(pdErr2, nil))
	re.False(leaderAPIClient.NeedRetry(pdErr2, nil))
	re.False(followerAPIClient.Available())
	re.True(leaderAPIClient.Available())
	followerAPIClient.(*pdServiceAPIClient).markAsAvailable()
	leaderAPIClient.(*pdServiceAPIClient).markAsAvailable()
	re.False(followerAPIClient.Available())
	time.Sleep(time.Millisecond * 100)
	followerAPIClient.(*pdServiceAPIClient).markAsAvailable()
	re.True(followerAPIClient.Available())

	re.True(followerAPIClient.NeedRetry(nil, err))
	re.False(leaderAPIClient.NeedRetry(nil, err))
	re.True(followerAPIClient.Available())
	re.True(leaderAPIClient.Available())

	re.NoError(failpoint.Disable("github.com/tikv/pd/client/fastCheckAvailable"))
}

func (suite *serviceClientTestSuite) TestServiceClientBalancer() {
	re := suite.Require()
	follower := suite.followerClient
	leader := suite.leaderClient
	b := &pdServiceBalancer{}
	b.set([]ServiceClient{leader, follower})
	re.Equal(2, b.totalNode)

	for i := 0; i < 10; i++ {
		client := b.get()
		ctx := client.BuildGRPCTargetContext(suite.ctx, false)
		conn := client.GetClientConn()
		re.NotNil(conn)
		resp, err := pb.NewGreeterClient(conn).SayHello(ctx, &pb.HelloRequest{Name: "pd"})
		re.NoError(err)
		re.Equal("Hello pd", resp.GetMessage())
	}
	re.Equal(int32(5), suite.leaderServer.server.getHandleCount())
	re.Equal(int32(5), suite.followerServer.server.getHandleCount())
	suite.followerServer.server.resetCount()
	suite.leaderServer.server.resetCount()

	for i := 0; i < 10; i++ {
		client := b.get()
		ctx := client.BuildGRPCTargetContext(suite.ctx, true)
		conn := client.GetClientConn()
		re.NotNil(conn)
		resp, err := pb.NewGreeterClient(conn).SayHello(ctx, &pb.HelloRequest{Name: "pd"})
		re.NoError(err)
		re.Equal("Hello pd", resp.GetMessage())
	}
	re.Equal(int32(10), suite.leaderServer.server.getHandleCount())
	re.Equal(int32(0), suite.followerServer.server.getHandleCount())
	re.Equal(int32(5), suite.followerServer.server.getForwardCount())
}

func TestServiceClientScheme(t *testing.T) {
	re := require.New(t)
	cli := newPDServiceClient(modifyURLScheme("127.0.0.1:2379", nil), modifyURLScheme("127.0.0.1:2379", nil), nil, false)
	re.Equal("http://127.0.0.1:2379", cli.GetURL())
	cli = newPDServiceClient(modifyURLScheme("https://127.0.0.1:2379", nil), modifyURLScheme("127.0.0.1:2379", nil), nil, false)
	re.Equal("http://127.0.0.1:2379", cli.GetURL())
	cli = newPDServiceClient(modifyURLScheme("http://127.0.0.1:2379", nil), modifyURLScheme("127.0.0.1:2379", nil), nil, false)
	re.Equal("http://127.0.0.1:2379", cli.GetURL())
	cli = newPDServiceClient(modifyURLScheme("127.0.0.1:2379", &tls.Config{}), modifyURLScheme("127.0.0.1:2379", &tls.Config{}), nil, false)
	re.Equal("https://127.0.0.1:2379", cli.GetURL())
	cli = newPDServiceClient(modifyURLScheme("https://127.0.0.1:2379", &tls.Config{}), modifyURLScheme("127.0.0.1:2379", &tls.Config{}), nil, false)
	re.Equal("https://127.0.0.1:2379", cli.GetURL())
	cli = newPDServiceClient(modifyURLScheme("http://127.0.0.1:2379", &tls.Config{}), modifyURLScheme("127.0.0.1:2379", &tls.Config{}), nil, false)
	re.Equal("https://127.0.0.1:2379", cli.GetURL())
}

func TestSchemeFunction(t *testing.T) {
	re := require.New(t)
	tlsCfg := &tls.Config{}

	endpoints1 := []string{
		"http://tc-pd:2379",
		"tc-pd:2379",
		"https://tc-pd:2379",
	}
	endpoints2 := []string{
		"127.0.0.1:2379",
		"http://127.0.0.1:2379",
		"https://127.0.0.1:2379",
	}
	urls := addrsToURLs(endpoints1, tlsCfg)
	for _, u := range urls {
		re.Equal("https://tc-pd:2379", u)
	}
	urls = addrsToURLs(endpoints2, tlsCfg)
	for _, u := range urls {
		re.Equal("https://127.0.0.1:2379", u)
	}
	urls = addrsToURLs(endpoints1, nil)
	for _, u := range urls {
		re.Equal("http://tc-pd:2379", u)
	}
	urls = addrsToURLs(endpoints2, nil)
	for _, u := range urls {
		re.Equal("http://127.0.0.1:2379", u)
	}

	re.Equal("https://127.0.0.1:2379", modifyURLScheme("https://127.0.0.1:2379", tlsCfg))
	re.Equal("https://127.0.0.1:2379", modifyURLScheme("http://127.0.0.1:2379", tlsCfg))
	re.Equal("https://127.0.0.1:2379", modifyURLScheme("127.0.0.1:2379", tlsCfg))
	re.Equal("https://tc-pd:2379", modifyURLScheme("tc-pd:2379", tlsCfg))
	re.Equal("http://127.0.0.1:2379", modifyURLScheme("https://127.0.0.1:2379", nil))
	re.Equal("http://127.0.0.1:2379", modifyURLScheme("http://127.0.0.1:2379", nil))
	re.Equal("http://127.0.0.1:2379", modifyURLScheme("127.0.0.1:2379", nil))
	re.Equal("http://tc-pd:2379", modifyURLScheme("tc-pd:2379", nil))

	urls = []string{
		"http://127.0.0.1:2379",
		"https://127.0.0.1:2379",
	}
	re.Equal("https://127.0.0.1:2379", pickMatchedURL(urls, tlsCfg))
	urls = []string{
		"http://127.0.0.1:2379",
	}
	re.Equal("https://127.0.0.1:2379", pickMatchedURL(urls, tlsCfg))
	urls = []string{
		"http://127.0.0.1:2379",
		"https://127.0.0.1:2379",
	}
	re.Equal("http://127.0.0.1:2379", pickMatchedURL(urls, nil))
	urls = []string{
		"https://127.0.0.1:2379",
	}
	re.Equal("http://127.0.0.1:2379", pickMatchedURL(urls, nil))
}
