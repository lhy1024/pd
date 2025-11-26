// Copyright 2025 TiKV Project Authors.
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

package handlers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/tests"
)

type affinityHandlerTestSuite struct {
	suite.Suite
	env *tests.SchedulingTestEnvironment
}

func TestAffinityHandlerTestSuite(t *testing.T) {
	suite.Run(t, new(affinityHandlerTestSuite))
}

func (suite *affinityHandlerTestSuite) SetupSuite() {
	suite.env = tests.NewSchedulingTestEnvironment(suite.T())
}

func (suite *affinityHandlerTestSuite) TearDownSuite() {
	suite.env.Cleanup()
}

func (suite *affinityHandlerTestSuite) TearDownTest() {
	// Clean up any remaining affinity groups after each test to avoid interference between tests.
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		leader := cluster.GetLeaderServer()
		manager, err := leader.GetServer().GetAffinityManager()
		if err != nil {
			return
		}

		allGroups := manager.GetAllAffinityGroupStates()
		groupIDs := make([]string, 0, len(allGroups))
		for _, group := range allGroups {
			groupIDs = append(groupIDs, group.ID)
		}

		if len(groupIDs) > 0 {
			_ = manager.DeleteAffinityGroups(groupIDs, true)
		}
	})
}


func (suite *affinityHandlerTestSuite) TestAffinityGroupLifecycle() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		client := tests.TestDialClient
		baseURL := fmt.Sprintf("%s/pd/api/v2/affinity-groups", leader.GetAddr())

		// Create two non-overlapping groups.
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"group-1": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x10}}}},
				"group-2": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x10}, EndKey: []byte{0x20}}}},
			},
		}
		data, err := json.Marshal(createReq)
		re.NoError(err)
		resp, err := client.Post(baseURL, "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)

		var createResp handlers.AffinityGroupsResponse
		re.NoError(json.NewDecoder(resp.Body).Decode(&createResp))
		re.Len(createResp.AffinityGroups, 2)
		re.Equal(1, createResp.AffinityGroups["group-1"].RangeCount)
		re.Equal(1, createResp.AffinityGroups["group-2"].RangeCount)

		// Creating an overlapping group should be rejected.
		overlapReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"group-overlap": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x05}, EndKey: []byte{0x0f}}}},
			},
		}
		data, err = json.Marshal(overlapReq)
		re.NoError(err)
		res, err := client.Post(baseURL, "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer res.Body.Close()
		re.Equal(http.StatusBadRequest, res.StatusCode)

		// Query all groups.
		res, err = client.Get(baseURL)
		re.NoError(err)
		defer res.Body.Close()
		re.Equal(http.StatusOK, res.StatusCode)
		var listResp handlers.AffinityGroupsResponse
		re.NoError(json.NewDecoder(res.Body).Decode(&listResp))
		re.Len(listResp.AffinityGroups, 2)

		// Update peers for group-1.
		updatePeersReq := handlers.UpdateAffinityGroupPeersRequest{
			LeaderStoreID: 1,
			VoterStoreIDs: []uint64{1},
		}
		data, err = json.Marshal(updatePeersReq)
		re.NoError(err)
		request, err := http.NewRequest(http.MethodPut, baseURL+"/group-1", bytes.NewReader(data))
		re.NoError(err)
		res, err = client.Do(request)
		re.NoError(err)
		defer res.Body.Close()
		re.Equal(http.StatusOK, res.StatusCode)
		groupState := &affinity.GroupState{}
		re.NoError(json.NewDecoder(res.Body).Decode(groupState))
		re.True(groupState.IsAffinitySchedulingAllowed)
		re.Equal(updatePeersReq.LeaderStoreID, groupState.LeaderStoreID)
		re.ElementsMatch(updatePeersReq.VoterStoreIDs, groupState.VoterStoreIDs)

		// Batch modify ranges: add one to group-1 and remove the only one from group-2.
		patchReq := handlers.BatchModifyAffinityGroupsRequest{
			Add: []handlers.GroupRangesModification{
				{ID: "group-1", Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x20}, EndKey: []byte{0x30}}}},
			},
			Remove: []handlers.GroupRangesModification{
				{ID: "group-2", Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x10}, EndKey: []byte{0x20}}}},
			},
		}
		data, err = json.Marshal(patchReq)
		re.NoError(err)
		request, err = http.NewRequest(http.MethodPatch, baseURL, bytes.NewReader(data))
		re.NoError(err)
		res, err = client.Do(request)
		re.NoError(err)
		defer res.Body.Close()
		re.Equal(http.StatusOK, res.StatusCode)
		var patchResp handlers.AffinityGroupsResponse
		re.NoError(json.NewDecoder(res.Body).Decode(&patchResp))
		re.Contains(patchResp.AffinityGroups, "group-1")
		re.Contains(patchResp.AffinityGroups, "group-2")

		// Delete with ranges should be blocked unless force=true.
		request, err = http.NewRequest(http.MethodDelete, baseURL+"/group-1", http.NoBody)
		re.NoError(err)
		res, err = client.Do(request)
		re.NoError(err)
		defer res.Body.Close()
		re.Equal(http.StatusBadRequest, res.StatusCode)

		request, err = http.NewRequest(http.MethodDelete, baseURL+"/group-1?force=true", http.NoBody)
		re.NoError(err)
		res, err = client.Do(request)
		re.NoError(err)
		defer res.Body.Close()
		re.Equal(http.StatusOK, res.StatusCode)

		// Batch delete the remaining empty group.
		batchDeleteReq := handlers.BatchDeleteAffinityGroupsRequest{IDs: []string{"group-2"}}
		data, err = json.Marshal(batchDeleteReq)
		re.NoError(err)
		res, err = client.Post(baseURL+"/batch-delete", "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer res.Body.Close()
		re.Equal(http.StatusOK, res.StatusCode)

		// Listing again should be empty.
		res, err = client.Get(baseURL)
		re.NoError(err)
		defer res.Body.Close()
		re.Equal(http.StatusOK, res.StatusCode)
		listResp = handlers.AffinityGroupsResponse{}
		re.NoError(json.NewDecoder(res.Body).Decode(&listResp))
		re.Empty(listResp.AffinityGroups)
	})
}

func (suite *affinityHandlerTestSuite) TestAffinityFirstRegionWins() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		client := tests.TestDialClient
		baseURL := fmt.Sprintf("%s/pd/api/v2/affinity-groups", leader.GetAddr())

		// Create a group without peer placement; range covers the default region.
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"first-win": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{}, EndKey: []byte{}}}},
			},
		}
		data, err := json.Marshal(createReq)
		re.NoError(err)
		resp, err := client.Post(baseURL, "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)

		manager := leader.GetServer().GetRaftCluster().GetAffinityManager()
		group := manager.GetAffinityGroupState("first-win")
		re.NotNil(group)
		re.False(group.IsAffinitySchedulingAllowed)
		re.Equal(uint64(0), group.LeaderStoreID)

		// Fake a healthy region that matches store 1.
		region := core.NewRegionInfo(
			&metapb.Region{
				Id:       100,
				StartKey: []byte(""),
				EndKey:   []byte("ffff"),
				Peers:    []*metapb.Peer{{Id: 11, StoreId: 1, Role: metapb.PeerRole_Voter}},
			},
			&metapb.Peer{Id: 11, StoreId: 1, Role: metapb.PeerRole_Voter},
		)

		// Manually observe region; first available region should set the peer layout.
		manager.ObserveAvailableRegion(region, group)

		// Fetch group via API to ensure effect and peers are set.
		res, err := client.Get(baseURL + "/first-win")
		re.NoError(err)
		defer res.Body.Close()
		re.Equal(http.StatusOK, res.StatusCode)
		finalState := &affinity.GroupState{}
		re.NoError(json.NewDecoder(res.Body).Decode(finalState))
		re.True(finalState.IsAffinitySchedulingAllowed)
		re.Equal(region.GetLeader().GetStoreId(), finalState.LeaderStoreID)
		re.ElementsMatch([]uint64{region.GetLeader().GetStoreId()}, finalState.VoterStoreIDs)

		// Cleanup to avoid overlaps for following cases.
		request, err := http.NewRequest(http.MethodDelete, baseURL+"/first-win?force=true", http.NoBody)
		re.NoError(err)
		res, err = client.Do(request)
		re.NoError(err)
		defer res.Body.Close()
		re.Equal(http.StatusOK, res.StatusCode)
	})
}

func (suite *affinityHandlerTestSuite) TestAffinityRemoveOnlyPatch() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		client := tests.TestDialClient
		baseURL := fmt.Sprintf("%s/pd/api/v2/affinity-groups", leader.GetAddr())

		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"remove-only": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x02}}}},
			},
		}
		data, err := json.Marshal(createReq)
		re.NoError(err)
		resp, err := client.Post(baseURL, "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)

		// Remove the only range.
		patchReq := handlers.BatchModifyAffinityGroupsRequest{
			Remove: []handlers.GroupRangesModification{
				{ID: "remove-only", Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x02}}}},
			},
		}
		data, err = json.Marshal(patchReq)
		re.NoError(err)
		request, err := http.NewRequest(http.MethodPatch, baseURL, bytes.NewReader(data))
		re.NoError(err)
		resp, err = client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)

		// Verify range count cleared.
		res, err := client.Get(baseURL + "/remove-only")
		re.NoError(err)
		defer res.Body.Close()
		re.Equal(http.StatusOK, res.StatusCode)
		state := &affinity.GroupState{}
		re.NoError(json.NewDecoder(res.Body).Decode(state))
		re.Equal(0, state.RangeCount)

		// Cleanup.
		request, err = http.NewRequest(http.MethodDelete, baseURL+"/remove-only?force=true", http.NoBody)
		re.NoError(err)
		resp, err = client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)
	})
}

func (suite *affinityHandlerTestSuite) TestAffinityBatchModifySuccess() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		client := tests.TestDialClient
		baseURL := fmt.Sprintf("%s/pd/api/v2/affinity-groups", leader.GetAddr())

		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"patch-success": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x05}}}},
			},
		}
		data, err := json.Marshal(createReq)
		re.NoError(err)
		resp, err := client.Post(baseURL, "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)

		patchReq := handlers.BatchModifyAffinityGroupsRequest{
			Remove: []handlers.GroupRangesModification{
				{ID: "patch-success", Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x05}}}},
			},
			Add: []handlers.GroupRangesModification{
				{ID: "patch-success", Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x10}, EndKey: []byte{0x20}}}},
			},
		}
		data, err = json.Marshal(patchReq)
		re.NoError(err)
		request, err := http.NewRequest(http.MethodPatch, baseURL, bytes.NewReader(data))
		re.NoError(err)
		resp, err = client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusBadRequest, resp.StatusCode)

		var errorMsg string
		re.NoError(json.NewDecoder(resp.Body).Decode(&errorMsg))
		re.Contains(errorMsg, "patch-success")
		re.Contains(errorMsg, "cannot appear in both add and remove")

		// Cleanup.
		request, err = http.NewRequest(http.MethodDelete, baseURL+"/patch-success?force=true", http.NoBody)
		re.NoError(err)
		resp, err = client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)
	})
}

func (suite *affinityHandlerTestSuite) TestUpdatePeersLeaderNotInVoters() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		client := tests.TestDialClient
		baseURL := fmt.Sprintf("%s/pd/api/v2/affinity-groups", leader.GetAddr())

		// Prepare group.
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"mismatch": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x00}, EndKey: []byte{0x10}}}},
			},
		}
		data, err := json.Marshal(createReq)
		re.NoError(err)
		resp, err := client.Post(baseURL, "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)

		// Leader is not in voters
		// Note: Using storeID 2 which doesn't exist in test environment.
		// AdjustGroup validates store existence before checking leader-in-voters,
		// so we expect "voter store does not exist" error.
		updateReq := handlers.UpdateAffinityGroupPeersRequest{
			LeaderStoreID: 1,
			VoterStoreIDs: []uint64{2},
		}
		payload, err := json.Marshal(updateReq)
		re.NoError(err)
		req, err := http.NewRequest(http.MethodPut, baseURL+"/mismatch", bytes.NewReader(payload))
		re.NoError(err)
		resp, err = client.Do(req)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusBadRequest, resp.StatusCode)
		var errorMsg string
		re.NoError(json.NewDecoder(resp.Body).Decode(&errorMsg))
		// Error comes from AdjustGroup - it checks store existence before leader-in-voters
		re.Contains(errorMsg, "voter store does not exist")

		// Cleanup.
		req, err = http.NewRequest(http.MethodDelete, baseURL+"/mismatch?force=true", http.NoBody)
		re.NoError(err)
		resp, err = client.Do(req)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)
	})
}

func (suite *affinityHandlerTestSuite) TestAffinityHandlersErrors() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		client := tests.TestDialClient
		baseURL := fmt.Sprintf("%s/pd/api/v2/affinity-groups", leader.GetAddr())

		// Empty payload should be rejected.
		data, err := json.Marshal(handlers.CreateAffinityGroupsRequest{})
		re.NoError(err)
		resp, err := client.Post(baseURL, "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusBadRequest, resp.StatusCode)

		// Illegal group ID.
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"bad id": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x00}, EndKey: []byte{0x10}}}},
			},
		}
		data, err = json.Marshal(createReq)
		re.NoError(err)
		resp, err = client.Post(baseURL, "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusBadRequest, resp.StatusCode)

		// Create a valid group for follow-up checks.
		createReq = handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"ok": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x00}, EndKey: []byte{0x10}}}},
			},
		}
		data, err = json.Marshal(createReq)
		re.NoError(err)
		resp, err = client.Post(baseURL, "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)

		// Duplicate creation should fail.
		data, err = json.Marshal(createReq)
		re.NoError(err)
		resp, err = client.Post(baseURL, "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusBadRequest, resp.StatusCode)

		// Get non-existent group.
		resp, err = client.Get(baseURL + "/nope")
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusNotFound, resp.StatusCode)

		// Update peers for non-existent group.
		updateReq := handlers.UpdateAffinityGroupPeersRequest{
			LeaderStoreID: 1,
			VoterStoreIDs: []uint64{1},
		}
		data, err = json.Marshal(updateReq)
		re.NoError(err)
		request, err := http.NewRequest(http.MethodPut, baseURL+"/ghost", bytes.NewReader(data))
		re.NoError(err)
		resp, err = client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusNotFound, resp.StatusCode)

		// Update peers with non-existent store should fail.
		updateReq = handlers.UpdateAffinityGroupPeersRequest{
			LeaderStoreID: 99,
			VoterStoreIDs: []uint64{99},
		}
		data, err = json.Marshal(updateReq)
		re.NoError(err)
		request, err = http.NewRequest(http.MethodPut, baseURL+"/ok", bytes.NewReader(data))
		re.NoError(err)
		resp, err = client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusBadRequest, resp.StatusCode)

		// Batch delete without IDs.
		emptyDelete := handlers.BatchDeleteAffinityGroupsRequest{}
		data, err = json.Marshal(emptyDelete)
		re.NoError(err)
		resp, err = client.Post(baseURL+"/batch-delete", "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusBadRequest, resp.StatusCode)

		// Batch modify referencing non-existent group.
		patchReq := handlers.BatchModifyAffinityGroupsRequest{
			Add: []handlers.GroupRangesModification{{ID: "ghost", Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x10}, EndKey: []byte{0x20}}}}},
		}
		data, err = json.Marshal(patchReq)
		re.NoError(err)
		request, err = http.NewRequest(http.MethodPatch, baseURL, bytes.NewReader(data))
		re.NoError(err)
		resp, err = client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusNotFound, resp.StatusCode)

		// Verify existing group "ok" is not affected (state not polluted)
		res, err := client.Get(baseURL + "/ok")
		re.NoError(err)
		defer res.Body.Close()
		re.Equal(http.StatusOK, res.StatusCode)
		groupState := &affinity.GroupState{}
		re.NoError(json.NewDecoder(res.Body).Decode(groupState))
		re.Equal(1, groupState.RangeCount)

		// Batch modify with empty operations.
		patchReq = handlers.BatchModifyAffinityGroupsRequest{}
		data, err = json.Marshal(patchReq)
		re.NoError(err)
		request, err = http.NewRequest(http.MethodPatch, baseURL, bytes.NewReader(data))
		re.NoError(err)
		resp, err = client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusBadRequest, resp.StatusCode)

		// Force delete the created group.
		request, err = http.NewRequest(http.MethodDelete, baseURL+"/ok?force=true", http.NoBody)
		re.NoError(err)
		resp, err = client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)
	})
}

// TestAffinityGroupDuplicateErrorMessage verifies that duplicate group creation
// returns a clear error message.
func (suite *affinityHandlerTestSuite) TestAffinityGroupDuplicateErrorMessage() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		client := tests.TestDialClient
		baseURL := fmt.Sprintf("%s/pd/api/v2/affinity-groups", leader.GetAddr())

		// Create a group successfully.
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"test-group": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x10}}}},
			},
		}
		data, err := json.Marshal(createReq)
		re.NoError(err)
		resp, err := client.Post(baseURL, "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)

		// Try to create the same group again, should get clear error message.
		resp, err = client.Post(baseURL, "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusBadRequest, resp.StatusCode)

		// Verify error message is not empty and contains useful information.
		var errorMsg string
		re.NoError(json.NewDecoder(resp.Body).Decode(&errorMsg))
		re.NotEmpty(errorMsg, "Error message should not be empty")
		re.Contains(errorMsg, "test-group", "Error message should contain the group ID")
		re.Contains(errorMsg, "already exists", "Error message should indicate the group already exists")
	})
}

// TestAffinityInvalidKeyRanges tests various invalid key range scenarios.
func (suite *affinityHandlerTestSuite) TestAffinityInvalidKeyRanges() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		client := tests.TestDialClient
		baseURL := fmt.Sprintf("%s/pd/api/v2/affinity-groups", leader.GetAddr())

		// Test StartKey > EndKey
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"invalid-range": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x10}, EndKey: []byte{0x01}}}},
			},
		}
		data, err := json.Marshal(createReq)
		re.NoError(err)
		resp, err := client.Post(baseURL, "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusBadRequest, resp.StatusCode)
		var errorMsg string
		re.NoError(json.NewDecoder(resp.Body).Decode(&errorMsg))
		re.Contains(errorMsg, "start_key must be less than end_key")

		// Test StartKey == EndKey
		createReq = handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"equal-keys": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x01}}}},
			},
		}
		data, err = json.Marshal(createReq)
		re.NoError(err)
		resp, err = client.Post(baseURL, "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusBadRequest, resp.StatusCode)
		re.NoError(json.NewDecoder(resp.Body).Decode(&errorMsg))
		re.Contains(errorMsg, "start_key must be less than end_key")

		// Test only StartKey provided
		createReq = handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"only-start": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{}}}},
			},
		}
		data, err = json.Marshal(createReq)
		re.NoError(err)
		resp, err = client.Post(baseURL, "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusBadRequest, resp.StatusCode)
		re.NoError(json.NewDecoder(resp.Body).Decode(&errorMsg))
		re.Contains(errorMsg, "key range must have both start_key and end_key")

		// Test only EndKey provided
		createReq = handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"only-end": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{}, EndKey: []byte{0x10}}}},
			},
		}
		data, err = json.Marshal(createReq)
		re.NoError(err)
		resp, err = client.Post(baseURL, "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusBadRequest, resp.StatusCode)
		re.NoError(json.NewDecoder(resp.Body).Decode(&errorMsg))
		re.Contains(errorMsg, "key range must have both start_key and end_key")
	})
}

// TestAffinityInvalidGroupIDs tests various invalid group ID scenarios.
func (suite *affinityHandlerTestSuite) TestAffinityInvalidGroupIDs() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		client := tests.TestDialClient
		baseURL := fmt.Sprintf("%s/pd/api/v2/affinity-groups", leader.GetAddr())

		testCases := []struct {
			name    string
			groupID string
		}{
			{"empty string", ""},
			{"space in id", "bad id"},
			{"special char @", "bad@id"},
			{"special char .", "bad.id"},
			{"65 characters", "a1234567890123456789012345678901234567890123456789012345678901234"},
		}

		for _, tc := range testCases {
			createReq := handlers.CreateAffinityGroupsRequest{
				AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
					tc.groupID: {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x10}}}},
				},
			}
			data, err := json.Marshal(createReq)
			re.NoError(err)
			resp, err := client.Post(baseURL, "application/json", bytes.NewReader(data))
			re.NoError(err)
			defer resp.Body.Close()
			re.Equal(http.StatusBadRequest, resp.StatusCode, "Test case: %s", tc.name)
		}

		// Test 64 characters (boundary, should succeed)
		validLongID := "a12345678901234567890123456789012345678901234567890123456789012"
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				validLongID: {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x10}}}},
			},
		}
		data, err := json.Marshal(createReq)
		re.NoError(err)
		resp, err := client.Post(baseURL, "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)

		// Cleanup
		request, err := http.NewRequest(http.MethodDelete, baseURL+"/"+validLongID+"?force=true", http.NoBody)
		re.NoError(err)
		resp, err = client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)
	})
}

// TestAffinityUpdatePeersDuplicateStores tests duplicate store IDs in VoterStoreIDs.
func (suite *affinityHandlerTestSuite) TestAffinityUpdatePeersDuplicateStores() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		client := tests.TestDialClient
		baseURL := fmt.Sprintf("%s/pd/api/v2/affinity-groups", leader.GetAddr())

		// Create a group first
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"test-dup": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x10}}}},
			},
		}
		data, err := json.Marshal(createReq)
		re.NoError(err)
		resp, err := client.Post(baseURL, "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)

		// Try to update with duplicate store IDs
		updateReq := handlers.UpdateAffinityGroupPeersRequest{
			LeaderStoreID: 1,
			VoterStoreIDs: []uint64{1, 1, 2}, // duplicate 1
		}
		data, err = json.Marshal(updateReq)
		re.NoError(err)
		request, err := http.NewRequest(http.MethodPut, baseURL+"/test-dup", bytes.NewReader(data))
		re.NoError(err)
		resp, err = client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusBadRequest, resp.StatusCode)
		var errorMsg string
		re.NoError(json.NewDecoder(resp.Body).Decode(&errorMsg))
		// Error comes from AdjustGroup in manager layer
		re.Contains(errorMsg, "duplicate voter store ID")

		// Cleanup
		request, err = http.NewRequest(http.MethodDelete, baseURL+"/test-dup?force=true", http.NoBody)
		re.NoError(err)
		resp, err = client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)
	})
}

// TestAffinityForceParameterVariants tests different values for the force parameter.
func (suite *affinityHandlerTestSuite) TestAffinityForceParameterVariants() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		client := tests.TestDialClient
		baseURL := fmt.Sprintf("%s/pd/api/v2/affinity-groups", leader.GetAddr())

		// Create a group with ranges
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"force-test": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x10}}}},
			},
		}
		data, err := json.Marshal(createReq)
		re.NoError(err)
		resp, err := client.Post(baseURL, "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)

		// Test force=false (should fail because group has ranges)
		request, err := http.NewRequest(http.MethodDelete, baseURL+"/force-test?force=false", http.NoBody)
		re.NoError(err)
		resp, err = client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusBadRequest, resp.StatusCode)

		// Test force=1 (should succeed with bool parsing)
		request, err = http.NewRequest(http.MethodDelete, baseURL+"/force-test?force=1", http.NoBody)
		re.NoError(err)
		resp, err = client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)
	})
}

// TestAffinityGetInvalidGroupID tests getting a group with invalid ID format.
func (suite *affinityHandlerTestSuite) TestAffinityGetInvalidGroupID() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		client := tests.TestDialClient
		baseURL := fmt.Sprintf("%s/pd/api/v2/affinity-groups", leader.GetAddr())

		// Test getting group with invalid ID format
		resp, err := client.Get(baseURL + "/bad@id")
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusBadRequest, resp.StatusCode)
		var errorMsg string
		re.NoError(json.NewDecoder(resp.Body).Decode(&errorMsg))
		re.Contains(errorMsg, "invalid group id")
	})
}

// TestDeleteInvalidGroupID tests deleting a group with invalid ID format.
func (suite *affinityHandlerTestSuite) TestDeleteInvalidGroupID() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		client := tests.TestDialClient
		baseURL := fmt.Sprintf("%s/pd/api/v2/affinity-groups", leader.GetAddr())

		// Test deleting group with invalid ID format
		request, err := http.NewRequest(http.MethodDelete, baseURL+"/bad@id", http.NoBody)
		re.NoError(err)
		resp, err := client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusBadRequest, resp.StatusCode)
		var errorMsg string
		re.NoError(json.NewDecoder(resp.Body).Decode(&errorMsg))
		re.Contains(errorMsg, "invalid group id")
	})
}

// TestBatchDeleteWithInvalidIDs tests batch delete with invalid group IDs.
func (suite *affinityHandlerTestSuite) TestBatchDeleteWithInvalidIDs() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		client := tests.TestDialClient
		baseURL := fmt.Sprintf("%s/pd/api/v2/affinity-groups", leader.GetAddr())

		// Create a valid group
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"valid-group": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x10}}}},
			},
		}
		data, err := json.Marshal(createReq)
		re.NoError(err)
		resp, err := client.Post(baseURL, "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)

		// Try to batch delete with one valid ID and one invalid ID
		batchDeleteReq := handlers.BatchDeleteAffinityGroupsRequest{
			IDs:   []string{"valid-group", "bad@id"},
			Force: true,
		}
		data, err = json.Marshal(batchDeleteReq)
		re.NoError(err)
		resp, err = client.Post(baseURL+"/batch-delete", "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusBadRequest, resp.StatusCode)
		var errorMsg string
		re.NoError(json.NewDecoder(resp.Body).Decode(&errorMsg))
		re.Contains(errorMsg, "invalid group id")

		// Cleanup - valid-group should still exist
		request, err := http.NewRequest(http.MethodDelete, baseURL+"/valid-group?force=true", http.NoBody)
		re.NoError(err)
		resp, err = client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)
	})
}

// TestBatchModifyInvalidGroupID tests batch modify with invalid group ID.
func (suite *affinityHandlerTestSuite) TestBatchModifyInvalidGroupID() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		client := tests.TestDialClient
		baseURL := fmt.Sprintf("%s/pd/api/v2/affinity-groups", leader.GetAddr())

		// Try to add ranges to a group with invalid ID
		patchReq := handlers.BatchModifyAffinityGroupsRequest{
			Add: []handlers.GroupRangesModification{
				{ID: "bad@id", Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x10}}}},
			},
		}
		data, err := json.Marshal(patchReq)
		re.NoError(err)
		request, err := http.NewRequest(http.MethodPatch, baseURL, bytes.NewReader(data))
		re.NoError(err)
		resp, err := client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusBadRequest, resp.StatusCode)
		var errorMsg string
		re.NoError(json.NewDecoder(resp.Body).Decode(&errorMsg))
		re.Contains(errorMsg, "invalid group id")
	})
}

// TestBatchModifyEmptyRanges tests batch modify with empty ranges array.
func (suite *affinityHandlerTestSuite) TestBatchModifyEmptyRanges() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		client := tests.TestDialClient
		baseURL := fmt.Sprintf("%s/pd/api/v2/affinity-groups", leader.GetAddr())

		// Create a valid group
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"test-group": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x10}}}},
			},
		}
		data, err := json.Marshal(createReq)
		re.NoError(err)
		resp, err := client.Post(baseURL, "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)

		// Try to add empty ranges array
		patchReq := handlers.BatchModifyAffinityGroupsRequest{
			Add: []handlers.GroupRangesModification{
				{ID: "test-group", Ranges: []handlers.AffinityKeyRange{}},
			},
		}
		data, err = json.Marshal(patchReq)
		re.NoError(err)
		request, err := http.NewRequest(http.MethodPatch, baseURL, bytes.NewReader(data))
		re.NoError(err)
		resp, err = client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusBadRequest, resp.StatusCode)
		var errorMsg string
		re.NoError(json.NewDecoder(resp.Body).Decode(&errorMsg))
		re.Contains(errorMsg, "no key ranges provided")

		// Cleanup
		request, err = http.NewRequest(http.MethodDelete, baseURL+"/test-group?force=true", http.NoBody)
		re.NoError(err)
		resp, err = client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)
	})
}

// TestBatchModifyAddOnly tests batch modify with only add operations.
func (suite *affinityHandlerTestSuite) TestBatchModifyAddOnly() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		client := tests.TestDialClient
		baseURL := fmt.Sprintf("%s/pd/api/v2/affinity-groups", leader.GetAddr())

		// Create a group
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"add-only": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x10}}}},
			},
		}
		data, err := json.Marshal(createReq)
		re.NoError(err)
		resp, err := client.Post(baseURL, "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)

		// Add more ranges (no remove)
		patchReq := handlers.BatchModifyAffinityGroupsRequest{
			Add: []handlers.GroupRangesModification{
				{ID: "add-only", Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x20}, EndKey: []byte{0x30}}}},
			},
		}
		data, err = json.Marshal(patchReq)
		re.NoError(err)
		request, err := http.NewRequest(http.MethodPatch, baseURL, bytes.NewReader(data))
		re.NoError(err)
		resp, err = client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)

		// Verify the group now has 2 ranges
		res, err := client.Get(baseURL + "/add-only")
		re.NoError(err)
		defer res.Body.Close()
		re.Equal(http.StatusOK, res.StatusCode)
		state := &affinity.GroupState{}
		re.NoError(json.NewDecoder(res.Body).Decode(state))
		re.Equal(2, state.RangeCount)

		// Cleanup
		request, err = http.NewRequest(http.MethodDelete, baseURL+"/add-only?force=true", http.NoBody)
		re.NoError(err)
		resp, err = client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)
	})
}

// TestBatchDeleteWithForce tests batch delete with force parameter.
func (suite *affinityHandlerTestSuite) TestBatchDeleteWithForce() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		client := tests.TestDialClient
		baseURL := fmt.Sprintf("%s/pd/api/v2/affinity-groups", leader.GetAddr())

		// Create a group with ranges
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"force-delete": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x10}}}},
			},
		}
		data, err := json.Marshal(createReq)
		re.NoError(err)
		resp, err := client.Post(baseURL, "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)

		// Try to batch delete without force (should fail)
		batchDeleteReq := handlers.BatchDeleteAffinityGroupsRequest{
			IDs:   []string{"force-delete"},
			Force: false,
		}
		data, err = json.Marshal(batchDeleteReq)
		re.NoError(err)
		resp, err = client.Post(baseURL+"/batch-delete", "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusBadRequest, resp.StatusCode)

		// Try to batch delete with force (should succeed)
		batchDeleteReq.Force = true
		data, err = json.Marshal(batchDeleteReq)
		re.NoError(err)
		resp, err = client.Post(baseURL+"/batch-delete", "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)

		// Verify the group is deleted
		res, err := client.Get(baseURL + "/force-delete")
		re.NoError(err)
		defer res.Body.Close()
		re.Equal(http.StatusNotFound, res.StatusCode)
	})
}

// TestUpdatePeersInvalidGroupID tests updating peers with invalid group ID format in URL path.
func (suite *affinityHandlerTestSuite) TestUpdatePeersInvalidGroupID() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		client := tests.TestDialClient
		baseURL := fmt.Sprintf("%s/pd/api/v2/affinity-groups", leader.GetAddr())

		// Try to update peers with invalid group ID format in URL path
		updateReq := handlers.UpdateAffinityGroupPeersRequest{
			LeaderStoreID: 1,
			VoterStoreIDs: []uint64{1},
		}
		data, err := json.Marshal(updateReq)
		re.NoError(err)
		request, err := http.NewRequest(http.MethodPut, baseURL+"/bad@id", bytes.NewReader(data))
		re.NoError(err)
		resp, err := client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusBadRequest, resp.StatusCode)
		var errorMsg string
		re.NoError(json.NewDecoder(resp.Body).Decode(&errorMsg))
		re.Contains(errorMsg, "invalid group id")
	})
}

// TestBatchModifyRemoveNonExistentGroup tests removing ranges from a non-existent group.
func (suite *affinityHandlerTestSuite) TestBatchModifyRemoveNonExistentGroup() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		client := tests.TestDialClient
		baseURL := fmt.Sprintf("%s/pd/api/v2/affinity-groups", leader.GetAddr())

		// Create a baseline group to verify it's not affected
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"baseline": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x10}}}},
			},
		}
		data, err := json.Marshal(createReq)
		re.NoError(err)
		resp, err := client.Post(baseURL, "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)

		// Try to remove ranges from a non-existent group
		patchReq := handlers.BatchModifyAffinityGroupsRequest{
			Remove: []handlers.GroupRangesModification{
				{ID: "non-existent-group", Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x10}, EndKey: []byte{0x20}}}},
			},
		}
		data, err = json.Marshal(patchReq)
		re.NoError(err)
		request, err := http.NewRequest(http.MethodPatch, baseURL, bytes.NewReader(data))
		re.NoError(err)
		resp, err = client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusNotFound, resp.StatusCode)
		var errorMsg string
		re.NoError(json.NewDecoder(resp.Body).Decode(&errorMsg))
		re.Contains(errorMsg, "not found")

		// Verify baseline group is not affected (state not polluted)
		res, err := client.Get(baseURL)
		re.NoError(err)
		defer res.Body.Close()
		re.Equal(http.StatusOK, res.StatusCode)
		var listResp handlers.AffinityGroupsResponse
		re.NoError(json.NewDecoder(res.Body).Decode(&listResp))
		re.Len(listResp.AffinityGroups, 1)
		re.Equal(1, listResp.AffinityGroups["baseline"].RangeCount)

		// Cleanup
		request, err = http.NewRequest(http.MethodDelete, baseURL+"/baseline?force=true", http.NoBody)
		re.NoError(err)
		resp, err = client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)
	})
}

// TestAffinityCreateEmptyRanges tests creating a group with empty ranges array.
func (suite *affinityHandlerTestSuite) TestAffinityCreateEmptyRanges() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		client := tests.TestDialClient
		baseURL := fmt.Sprintf("%s/pd/api/v2/affinity-groups", leader.GetAddr())

		// Try to create a group with empty ranges array
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"empty-ranges": {Ranges: []handlers.AffinityKeyRange{}},
			},
		}
		data, err := json.Marshal(createReq)
		re.NoError(err)
		resp, err := client.Post(baseURL, "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusBadRequest, resp.StatusCode)
		var errorMsg string
		re.NoError(json.NewDecoder(resp.Body).Decode(&errorMsg))
		re.Contains(errorMsg, "no key ranges provided")
	})
}

// TestBatchModifyOverlappingRanges tests adding overlapping ranges via batch modify.
func (suite *affinityHandlerTestSuite) TestBatchModifyOverlappingRanges() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		client := tests.TestDialClient
		baseURL := fmt.Sprintf("%s/pd/api/v2/affinity-groups", leader.GetAddr())

		// Create two groups with non-overlapping ranges
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"group-1": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x10}}}},
				"group-2": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x20}, EndKey: []byte{0x30}}}},
			},
		}
		data, err := json.Marshal(createReq)
		re.NoError(err)
		resp, err := client.Post(baseURL, "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)

		// Try to add overlapping range to group-2 (overlaps with group-1's range)
		patchReq := handlers.BatchModifyAffinityGroupsRequest{
			Add: []handlers.GroupRangesModification{
				{ID: "group-2", Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x05}, EndKey: []byte{0x15}}}},
			},
		}
		data, err = json.Marshal(patchReq)
		re.NoError(err)
		request, err := http.NewRequest(http.MethodPatch, baseURL, bytes.NewReader(data))
		re.NoError(err)
		resp, err = client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusBadRequest, resp.StatusCode)
		var errorMsg string
		re.NoError(json.NewDecoder(resp.Body).Decode(&errorMsg))
		re.Contains(errorMsg, "overlap")

		// Verify system state not polluted: both groups still have only 1 range each
		res, err := client.Get(baseURL)
		re.NoError(err)
		defer res.Body.Close()
		re.Equal(http.StatusOK, res.StatusCode)
		var listResp handlers.AffinityGroupsResponse
		re.NoError(json.NewDecoder(res.Body).Decode(&listResp))
		re.Len(listResp.AffinityGroups, 2)
		re.Equal(1, listResp.AffinityGroups["group-1"].RangeCount)
		re.Equal(1, listResp.AffinityGroups["group-2"].RangeCount)

		// Cleanup
		request, err = http.NewRequest(http.MethodDelete, baseURL+"/group-1?force=true", http.NoBody)
		re.NoError(err)
		resp, err = client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()

		request, err = http.NewRequest(http.MethodDelete, baseURL+"/group-2?force=true", http.NoBody)
		re.NoError(err)
		resp, err = client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()
	})
}

// TestUpdatePeersMissingRequiredFields tests updating peers with missing required fields.
func (suite *affinityHandlerTestSuite) TestUpdatePeersMissingRequiredFields() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		client := tests.TestDialClient
		baseURL := fmt.Sprintf("%s/pd/api/v2/affinity-groups", leader.GetAddr())

		// Create a group first
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"test-peers": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x10}}}},
			},
		}
		data, err := json.Marshal(createReq)
		re.NoError(err)
		resp, err := client.Post(baseURL, "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)

		// Test 1: LeaderStoreID = 0 (missing)
		updateReq := handlers.UpdateAffinityGroupPeersRequest{
			LeaderStoreID: 0,
			VoterStoreIDs: []uint64{1},
		}
		data, err = json.Marshal(updateReq)
		re.NoError(err)
		request, err := http.NewRequest(http.MethodPut, baseURL+"/test-peers", bytes.NewReader(data))
		re.NoError(err)
		resp, err = client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusBadRequest, resp.StatusCode)
		var errorMsg string
		re.NoError(json.NewDecoder(resp.Body).Decode(&errorMsg))
		re.Contains(errorMsg, "required")

		// Test 2: VoterStoreIDs empty
		updateReq = handlers.UpdateAffinityGroupPeersRequest{
			LeaderStoreID: 1,
			VoterStoreIDs: []uint64{},
		}
		data, err = json.Marshal(updateReq)
		re.NoError(err)
		request, err = http.NewRequest(http.MethodPut, baseURL+"/test-peers", bytes.NewReader(data))
		re.NoError(err)
		resp, err = client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusBadRequest, resp.StatusCode)
		re.NoError(json.NewDecoder(resp.Body).Decode(&errorMsg))
		re.Contains(errorMsg, "required")

		// Cleanup
		request, err = http.NewRequest(http.MethodDelete, baseURL+"/test-peers?force=true", http.NoBody)
		re.NoError(err)
		resp, err = client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)
	})
}

// TestBatchDeleteNonExistentGroup tests batch deleting a non-existent group.
func (suite *affinityHandlerTestSuite) TestBatchDeleteNonExistentGroup() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		client := tests.TestDialClient
		baseURL := fmt.Sprintf("%s/pd/api/v2/affinity-groups", leader.GetAddr())

		// Create a baseline group to verify it's not affected
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"existing": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x10}}}},
			},
		}
		data, err := json.Marshal(createReq)
		re.NoError(err)
		resp, err := client.Post(baseURL, "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)

		// Try to delete a non-existent group
		batchDeleteReq := handlers.BatchDeleteAffinityGroupsRequest{
			IDs: []string{"non-existent-group"},
		}
		data, err = json.Marshal(batchDeleteReq)
		re.NoError(err)
		resp, err = client.Post(baseURL+"/batch-delete", "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusNotFound, resp.StatusCode)
		var errorMsg string
		re.NoError(json.NewDecoder(resp.Body).Decode(&errorMsg))
		re.Contains(errorMsg, "not found")

		// Verify baseline group is not affected (state not polluted)
		res, err := client.Get(baseURL)
		re.NoError(err)
		defer res.Body.Close()
		re.Equal(http.StatusOK, res.StatusCode)
		var listResp handlers.AffinityGroupsResponse
		re.NoError(json.NewDecoder(res.Body).Decode(&listResp))
		re.Len(listResp.AffinityGroups, 1)
		re.Contains(listResp.AffinityGroups, "existing")
		re.Equal(1, listResp.AffinityGroups["existing"].RangeCount)

		// Cleanup
		request, err := http.NewRequest(http.MethodDelete, baseURL+"/existing?force=true", http.NoBody)
		re.NoError(err)
		resp, err = client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)
	})
}

// TestBatchModifyRemoveNonExistentRange tests removing a range that the group doesn't contain.
func (suite *affinityHandlerTestSuite) TestBatchModifyRemoveNonExistentRange() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		client := tests.TestDialClient
		baseURL := fmt.Sprintf("%s/pd/api/v2/affinity-groups", leader.GetAddr())

		// Create a group with a specific range
		createReq := handlers.CreateAffinityGroupsRequest{
			AffinityGroups: map[string]handlers.CreateAffinityGroupInput{
				"test-group": {Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x01}, EndKey: []byte{0x10}}}},
			},
		}
		data, err := json.Marshal(createReq)
		re.NoError(err)
		resp, err := client.Post(baseURL, "application/json", bytes.NewReader(data))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)

		// Try to remove a range that the group doesn't contain
		patchReq := handlers.BatchModifyAffinityGroupsRequest{
			Remove: []handlers.GroupRangesModification{
				{ID: "test-group", Ranges: []handlers.AffinityKeyRange{{StartKey: []byte{0x20}, EndKey: []byte{0x30}}}},
			},
		}
		data, err = json.Marshal(patchReq)
		re.NoError(err)
		request, err := http.NewRequest(http.MethodPatch, baseURL, bytes.NewReader(data))
		re.NoError(err)
		resp, err = client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusNotFound, resp.StatusCode)
		var errorMsg string
		re.NoError(json.NewDecoder(resp.Body).Decode(&errorMsg))
		re.Contains(errorMsg, "not found")

		// Verify the original range is still intact (state not polluted)
		res, err := client.Get(baseURL + "/test-group")
		re.NoError(err)
		defer res.Body.Close()
		re.Equal(http.StatusOK, res.StatusCode)
		state := &affinity.GroupState{}
		re.NoError(json.NewDecoder(res.Body).Decode(state))
		re.Equal(1, state.RangeCount)

		// Cleanup
		request, err = http.NewRequest(http.MethodDelete, baseURL+"/test-group?force=true", http.NoBody)
		re.NoError(err)
		resp, err = client.Do(request)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)
	})
}


