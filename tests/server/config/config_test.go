// Copyright 2022 TiKV Project Authors.
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

package config

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/ratelimit"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/tests"
)

// dialClient used to dial http request.
var dialClient = &http.Client{
	Transport: &http.Transport{
		DisableKeepAlives: true,
	},
}

func TestRateLimitConfigReload(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 3)
	defer cluster.Destroy()
	re.NoError(err)
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())

	leader := cluster.GetServer(cluster.GetLeader())

	re.Empty(leader.GetServer().GetServiceMiddlewareConfig().RateLimitConfig.LimiterConfig)
	limitCfg := make(map[string]ratelimit.DimensionConfig)
	limitCfg["GetRegions"] = ratelimit.DimensionConfig{QPS: 1}

	input := map[string]interface{}{
		"enable-rate-limit": "true",
		"limiter-config":    limitCfg,
	}
	data, err := json.Marshal(input)
	re.NoError(err)
	req, _ := http.NewRequest("POST", leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	resp, err := dialClient.Do(req)
	re.NoError(err)
	resp.Body.Close()
	re.True(leader.GetServer().GetServiceMiddlewarePersistOptions().IsRateLimitEnabled())
	re.Len(leader.GetServer().GetServiceMiddlewarePersistOptions().GetRateLimitConfig().LimiterConfig, 1)

	oldLeaderName := leader.GetServer().Name()
	leader.GetServer().GetMember().ResignEtcdLeader(leader.GetServer().Context(), oldLeaderName, "")
	mustWaitLeader(re, cluster.GetServers())
	leader = cluster.GetServer(cluster.GetLeader())

	re.True(leader.GetServer().GetServiceMiddlewarePersistOptions().IsRateLimitEnabled())
	re.Len(leader.GetServer().GetServiceMiddlewarePersistOptions().GetRateLimitConfig().LimiterConfig, 1)
}

func mustWaitLeader(re *require.Assertions, svrs map[string]*tests.TestServer) *server.Server {
	var leader *server.Server
	testutil.Eventually(re, func() bool {
<<<<<<< HEAD
		for _, s := range svrs {
			if !s.GetServer().IsClosed() && s.GetServer().GetMember().IsLeader() {
				leader = s.GetServer()
=======
		for _, svr := range svrs {
			if !svr.GetServer().IsClosed() && svr.GetServer().GetMember().IsLeader() {
				leader = svr.GetServer()
>>>>>>> 2aa6049c691be21c67768ad9a74859891a6ac8b3
				return true
			}
		}
		return false
	})
	return leader
}
