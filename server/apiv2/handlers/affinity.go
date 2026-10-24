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
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/handler"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/apiv2/middlewares"
)

var affinityHandler = handler.AffinityHandler{GetManager: func(c *gin.Context) (*affinity.Manager, bool) {
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager, err := svr.GetAffinityManager()
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return nil, false
	}
	return manager, true
}}

// RegisterAffinity registers affinity group related handlers to router paths.
func RegisterAffinity(r *gin.RouterGroup) {
	router := r.Group("affinity-groups")
	router.Use(middlewares.BootstrapChecker())
	router.POST("", PostAffinityGroups)
	router.PATCH("", BatchModifyAffinityGroups)
	router.PATCH("/:group_id/policy", UpdateAffinityGroupPolicy)
	router.PUT("/:group_id", UpdateAffinityGroupPeers)
	router.DELETE("/:group_id", DeleteAffinityGroup)
	router.GET("", GetAllAffinityGroups)
	router.GET("/:group_id", GetAffinityGroup)
}

// AffinityKeyRange is the shared affinity API payload.
type AffinityKeyRange = handler.AffinityKeyRange

// CreateAffinityGroupInput is the shared affinity API payload.
type CreateAffinityGroupInput = handler.CreateAffinityGroupInput

// CreateAffinityGroupsRequest is the shared affinity API payload.
type CreateAffinityGroupsRequest = handler.CreateAffinityGroupsRequest

// AffinityGroupsResponse is the shared affinity API payload.
type AffinityGroupsResponse = handler.AffinityGroupsResponse

// BatchDeleteAffinityGroupsRequest is the shared affinity API payload.
type BatchDeleteAffinityGroupsRequest = handler.BatchDeleteAffinityGroupsRequest

// GroupRangesModification is the shared affinity API payload.
type GroupRangesModification = handler.GroupRangesModification

// BatchModifyAffinityGroupsRequest is the shared affinity API payload.
type BatchModifyAffinityGroupsRequest = handler.BatchModifyAffinityGroupsRequest

// UpdateAffinityGroupPeersRequest is the shared affinity API payload.
type UpdateAffinityGroupPeersRequest = handler.UpdateAffinityGroupPeersRequest

// UpdateAffinityGroupPolicyRequest is the shared affinity API payload.
type UpdateAffinityGroupPolicyRequest = handler.UpdateAffinityGroupPolicyRequest

// --- Handlers ---

// PostAffinityGroups handles POST requests for affinity groups.
//
//	@Tags			affinity-groups
//	@Summary		Create or delete affinity groups.
//	@Description	Create affinity groups (default), or delete multiple groups if ?delete query parameter is present. When delete parameter is absent, expects CreateAffinityGroupsRequest in body. When delete parameter is present, expects BatchDeleteAffinityGroupsRequest in body.
//	@Param			delete				query	string	false	"If present, triggers batch deletion instead of creation"
//	@Param			skip_exist_check	query	bool	false	"If true, skip existing groups and create the rest"
//	@Param			body				body	object	true	"Request body (type depends on delete parameter)"
//	@Produce		json
//	@Success		200	{object}	AffinityGroupsResponse	"Response for create or delete operation"
//	@Failure		400	{string}	string					"The input is invalid."
//	@Failure		500	{string}	string					"PD server failed to proceed the request."
//	@Router			/affinity-groups [post]
func PostAffinityGroups(c *gin.Context) {
	affinityHandler.PostAffinityGroups(c)
}

// BatchModifyAffinityGroups batch modifies key ranges for multiple affinity groups.
//
//	@Tags		affinity-groups
//	@Summary	Batch modify key ranges for affinity groups. Add and remove ranges are validated against the current view; moving an overlapping range between groups requires separate requests.
//	@Param		body	body	BatchModifyAffinityGroupsRequest	true	"Batch modify request with add and remove operations"
//	@Produce	json
//	@Success	200	{object}	AffinityGroupsResponse	"Updated affinity groups"
//	@Failure	400	{string}	string					"The input is invalid."
//	@Failure	500	{string}	string					"PD server failed to proceed the request."
//	@Router		/affinity-groups [patch]
func BatchModifyAffinityGroups(c *gin.Context) {
	affinityHandler.BatchModifyAffinityGroups(c)
}

// UpdateAffinityGroupPeers updates leader and voter stores of an affinity group.
//
//	@Tags		affinity-groups
//	@Summary	Update leader and voter stores of an affinity group.
//	@Param		group_id	path	string							true	"The group id of the affinity group"
//	@Param		body		body	UpdateAffinityGroupPeersRequest	true	"New leader and voter store IDs"
//	@Produce	json
//	@Success	200	{object}	affinity.GroupState	"Updated affinity group state"
//	@Failure	400	{string}	string				"The input is invalid."
//	@Failure	404	{string}	string				"Affinity group not found."
//	@Failure	500	{string}	string				"PD server failed to proceed the request."
//	@Router		/affinity-groups/{group_id} [put]
func UpdateAffinityGroupPeers(c *gin.Context) {
	affinityHandler.UpdateAffinityGroupPeers(c)
}

// UpdateAffinityGroupPolicy updates the balance policy of an affinity group.
//
//	@Tags		affinity-groups
//	@Summary	Update the balance policy of an affinity group.
//	@Param		group_id	path	string						true	"The group id of the affinity group"
//	@Param		body		body	UpdateAffinityGroupPolicyRequest	true	"New balance policy: auto or fixed"
//	@Produce	json
//	@Success	200	{object}	affinity.GroupState	"Updated affinity group state"
//	@Failure	400	{string}	string						"The input is invalid."
//	@Failure	404	{string}	string						"Affinity group not found."
//	@Failure	409	{string}	string						"Affinity group changed during the update."
//	@Failure	500	{string}	string						"PD server failed to proceed the request."
//	@Router		/affinity-groups/{group_id}/policy [patch]
func UpdateAffinityGroupPolicy(c *gin.Context) {
	affinityHandler.UpdateAffinityGroupPolicy(c)
}

// DeleteAffinityGroup deletes a specific affinity group by group id.
//
//	@Tags			affinity-groups
//	@Summary		Delete an affinity group by group id.
//	@Param			group_id	path	string	true	"The group id of the affinity group"
//	@Param			force		query	bool	false	"Force delete even if the group has key ranges"
//	@Produce		json
//	@Success		200	{string}	string	"Affinity group deleted successfully."
//	@Failure		400	{string}	string	"The input is invalid or group has ranges without force flag."
//	@Failure		404	{string}	string	"Affinity group not found."
//	@Failure		500	{string}	string	"PD server failed to proceed the request."
//	@Router			/affinity-groups/{group_id} [delete]
//	@Description	force=false: missing group returns 404; force=true: missing group is ignored, existing group is deleted.
func DeleteAffinityGroup(c *gin.Context) {
	affinityHandler.DeleteAffinityGroup(c)
}

// GetAllAffinityGroups lists affinity groups, with optional range details.
//
//	@Tags		affinity-groups
//	@Summary	List all affinity groups.
//	@Param		ids	query	[]string	false	"Optional affinity group IDs. Repeat as ids=a&ids=b."
//	@Produce	json
//	@Success	200	{object}	AffinityGroupsResponse
//	@Failure	400	{string}	string	"The input is invalid."
//	@Failure	500	{string}	string	"PD server failed to proceed the request."
//	@Router		/affinity-groups [get]
func GetAllAffinityGroups(c *gin.Context) {
	affinityHandler.GetAllAffinityGroups(c)
}

// GetAffinityGroup gets a specific affinity group by group id, with optional range details.
//
//	@Tags		affinity-groups
//	@Summary	Get an affinity group by group id.
//	@Param		group_id	path	string	true	"The group id of the affinity group"
//	@Produce	json
//	@Success	200	{object}	*affinity.GroupState
//	@Failure	404	{string}	string	"Affinity group not found."
//	@Failure	500	{string}	string	"PD server failed to proceed the request."
//	@Router		/affinity-groups/{group_id} [get]
func GetAffinityGroup(c *gin.Context) {
	affinityHandler.GetAffinityGroup(c)
}
