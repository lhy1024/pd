// Copyright 2018 TiKV Project Authors.
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

package api

import (
	"net/http"

	"github.com/unrolled/render"

	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/cluster"
)

type healthHandler struct {
	svr *server.Server
	rd  *render.Render
}

// Health reflects the cluster's health.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type Health struct {
	Name       string   `json:"name"`
	MemberID   uint64   `json:"member_id"`
	ClientUrls []string `json:"client_urls"`
	Health     bool     `json:"health"`
}

func newHealthHandler(svr *server.Server, rd *render.Render) *healthHandler {
	return &healthHandler{
		svr: svr,
		rd:  rd,
	}
}

// @Summary  Health status of PD servers.
// @Produce  json
// @Success  200  {array}   Health
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /health [get]
func (h *healthHandler) GetHealthStatus(w http.ResponseWriter, _ *http.Request) {
	client := h.svr.GetClient()
	members, err := cluster.GetMembers(client)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	healthMembers := cluster.CheckHealth(h.svr.GetHTTPClient(), members)
	healths := []Health{}
	for _, member := range members {
		h := Health{
			Name:       member.Name,
			MemberID:   member.MemberId,
			ClientUrls: member.ClientUrls,
			Health:     false,
		}
		if _, ok := healthMembers[member.GetMemberId()]; ok {
			h.Health = true
		}
		healths = append(healths, h)
	}
	h.rd.JSON(w, http.StatusOK, healths)
}

// @Summary  Ping PD servers.
// @Router   /ping [get]
func (*healthHandler) Ping(http.ResponseWriter, *http.Request) {}

// ReadyStatus reflects the cluster's ready status.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type ReadyStatus struct {
	RegionLoaded bool `json:"region_loaded"`
}

// @Summary  It will return whether pd follower is ready to became leader.
// @Router   /ready [get]
// @Param    verbose query  bool    false  "Whether to return details."
// @Success  200
// @Failure  500
func (h *healthHandler) Ready(w http.ResponseWriter, r *http.Request) {
	s := h.svr.GetStorage()
	regionLoaded := storage.AreRegionsLoaded(s)
	if regionLoaded {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}

	if _, ok := r.URL.Query()["verbose"]; !ok {
		return
	}
	resp := &ReadyStatus{
		RegionLoaded: regionLoaded,
	}
	if regionLoaded {
		h.rd.JSON(w, http.StatusOK, resp)
	} else {
		h.rd.JSON(w, http.StatusInternalServerError, resp)
	}
}
