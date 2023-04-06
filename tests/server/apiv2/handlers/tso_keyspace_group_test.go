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

package handlers_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/tests"
)

const keyspaceGroupsPrefix = "/pd/api/v2/tso/keyspace-groups"

type keyspaceGroupTestSuite struct {
	suite.Suite
	ctx     context.Context
	cancel  context.CancelFunc
	cluster *tests.TestCluster
	server  *tests.TestServer
}

func TestKeyspaceGroupTestSuite(t *testing.T) {
	suite.Run(t, new(keyspaceGroupTestSuite))
}

func (suite *keyspaceGroupTestSuite) SetupTest() {
	suite.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/disableAllocate", "return(true)"))
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	cluster, err := tests.NewTestCluster(suite.ctx, 1)
	suite.cluster = cluster
	suite.NoError(err)
	suite.NoError(cluster.RunInitialServers())
	suite.NotEmpty(cluster.WaitLeader())
	suite.server = cluster.GetServer(cluster.GetLeader())
	suite.NoError(suite.server.BootstrapCluster())
}

func (suite *keyspaceGroupTestSuite) TearDownTest() {
	suite.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/disableAllocate"))
	suite.cancel()
	suite.cluster.Destroy()
}

func (suite *keyspaceGroupTestSuite) TestCreateKeyspaceGroups() {
	re := suite.Require()
	kgs := &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID:       uint32(1),
			UserKind: endpoint.Standard.String(),
			Replica:  1,
		},
		{
			ID:       uint32(2),
			UserKind: endpoint.Standard.String(),
			Replica:  1,
		},
	}}
	code := tryCreateKeyspaceGroup(re, suite.server, kgs)
	re.Equal(http.StatusOK, code)

	// miss user kind.
	kgs = &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID:      uint32(3),
			Replica: 1,
		},
	}}
	code = tryCreateKeyspaceGroup(re, suite.server, kgs)
	re.Equal(http.StatusBadRequest, code)

	// invalid user kind.
	kgs = &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID:       uint32(3),
			UserKind: "invalid",
			Replica:  1,
		},
	}}
	code = tryCreateKeyspaceGroup(re, suite.server, kgs)
	re.Equal(http.StatusBadRequest, code)

	// miss ID.
	kgs = &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			UserKind: endpoint.Standard.String(),
			Replica:  1,
		},
	}}
	code = tryCreateKeyspaceGroup(re, suite.server, kgs)
	re.Equal(http.StatusInternalServerError, code)

	// invalid ID.
	kgs = &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID:       utils.MaxKeyspaceGroupCount + 1,
			UserKind: endpoint.Standard.String(),
			Replica:  1,
		},
	}}
	code = tryCreateKeyspaceGroup(re, suite.server, kgs)
	re.Equal(http.StatusBadRequest, code)

	// repeated ID.
	kgs = &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID:       uint32(2),
			UserKind: endpoint.Standard.String(),
			Replica:  1,
		},
	}}
	code = tryCreateKeyspaceGroup(re, suite.server, kgs)
	re.Equal(http.StatusInternalServerError, code)

	// miss replica.
	kgs = &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID:       uint32(4),
			UserKind: endpoint.Standard.String(),
		},
	}}
	code = tryCreateKeyspaceGroup(re, suite.server, kgs)
	re.Equal(http.StatusBadRequest, code)

	// replica is negative.
	kgs = &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID:       uint32(5),
			UserKind: endpoint.Standard.String(),
			Replica:  -1,
		},
	}}
	code = tryCreateKeyspaceGroup(re, suite.server, kgs)
	re.Equal(http.StatusBadRequest, code)

	// replica is more than the num of nodes.
	kgs = &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID:       uint32(5),
			UserKind: endpoint.Standard.String(),
			Replica:  2,
		},
	}}
	code = tryCreateKeyspaceGroup(re, suite.server, kgs)
	re.Equal(http.StatusBadRequest, code)
}

func (suite *keyspaceGroupTestSuite) TestLoadKeyspaceGroup() {
	re := suite.Require()
	kgs := &handlers.CreateKeyspaceGroupParams{KeyspaceGroups: []*endpoint.KeyspaceGroup{
		{
			ID:       uint32(1),
			UserKind: endpoint.Standard.String(),
			Replica:  1,
		},
		{
			ID:       uint32(2),
			UserKind: endpoint.Standard.String(),
			Replica:  1,
		},
	}}

	code := tryCreateKeyspaceGroup(re, suite.server, kgs)
	re.Equal(http.StatusOK, code)
	resp := sendLoadKeyspaceGroupRequest(re, suite.server, "0", "0")
	re.Equal(3, len(resp))
}

func sendLoadKeyspaceGroupRequest(re *require.Assertions, server *tests.TestServer, token, limit string) []*endpoint.KeyspaceGroup {
	// Construct load range request.
	httpReq, err := http.NewRequest(http.MethodGet, server.GetAddr()+keyspaceGroupsPrefix, nil)
	re.NoError(err)
	query := httpReq.URL.Query()
	query.Add("page_token", token)
	query.Add("limit", limit)
	httpReq.URL.RawQuery = query.Encode()
	// Send request.
	httpResp, err := dialClient.Do(httpReq)
	re.NoError(err)
	defer httpResp.Body.Close()
	re.Equal(http.StatusOK, httpResp.StatusCode)
	// Receive & decode response.
	data, err := io.ReadAll(httpResp.Body)
	re.NoError(err)
	var resp []*endpoint.KeyspaceGroup
	re.NoError(json.Unmarshal(data, &resp))
	return resp
}

func tryCreateKeyspaceGroup(re *require.Assertions, server *tests.TestServer, request *handlers.CreateKeyspaceGroupParams) int {
	data, err := json.Marshal(request)
	re.NoError(err)
	httpReq, err := http.NewRequest(http.MethodPost, server.GetAddr()+keyspaceGroupsPrefix, bytes.NewBuffer(data))
	re.NoError(err)
	resp, err := dialClient.Do(httpReq)
	re.NoError(err)
	defer resp.Body.Close()
	return resp.StatusCode
}
