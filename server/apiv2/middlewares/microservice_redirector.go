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

package middlewares

import (
	"net/http"
	"net/url"

	"github.com/gin-gonic/gin"

	"github.com/tikv/pd/pkg/errs"
	scheapi "github.com/tikv/pd/pkg/mcs/scheduling/server/apis/v1"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/apiutil/serverapi"
	"github.com/tikv/pd/server"
)

// MicroserviceRedirector creates a middleware to rewrite and forward requests to a microservice primary.
func MicroserviceRedirector(rules ...serverapi.RedirectRule) gin.HandlerFunc {
	return func(c *gin.Context) {
		if len(rules) == 0 {
			c.Next()
			return
		}
		svr := c.MustGet(ServerContextKey).(*server.Server)
		redirectToMicroservice, targetAddr := serverapi.MatchMicroserviceRedirect(
			c.Request,
			rules,
			svr.IsKeyspaceGroupEnabled(),
			svr.IsServiceIndependent,
			svr.GetServicePrimaryAddr)
		if !redirectToMicroservice {
			c.Next()
			return
		}
		if targetAddr == "" {
			c.AbortWithStatusJSON(http.StatusInternalServerError, errs.ErrRedirect.FastGenByArgs().Error())
			return
		}

		client := svr.GetHTTPClient()
		u, err := url.Parse(targetAddr)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, errs.ErrURLParse.Wrap(err).GenWithStackByCause().Error())
			return
		}
		forwardedIP, forwardedPort := apiutil.GetIPPortFromHTTPRequest(c.Request)
		if len(forwardedIP) > 0 {
			c.Request.Header.Add(apiutil.XForwardedForHeader, forwardedIP)
		} else {
			// Fallback if GetIPPortFromHTTPRequest failed to get the IP.
			c.Request.Header.Add(apiutil.XForwardedForHeader, c.Request.RemoteAddr)
		}
		if len(forwardedPort) > 0 {
			c.Request.Header.Add(apiutil.XForwardedPortHeader, forwardedPort)
		}
		c.Writer.Header().Add(apiutil.XForwardedToMicroserviceHeader, "true")
		apiutil.NewCustomReverseProxies(client, []url.URL{*u}).ServeHTTP(c.Writer, c.Request)
		c.Abort()
	}
}

// AffinityMicroserviceRedirector forwards every affinity read and write to the
// scheduling primary. The scheduling service owns the same Manager/storage
// path as the PD leader; keeping all methods under one redirect rule prevents
// a follower or a PD-side handler from becoming a second writer.
func AffinityMicroserviceRedirector() gin.HandlerFunc {
	pdAffinityPath := "/pd/api/v2/affinity-groups"
	targetAffinityPath := scheapi.APIPathPrefix + "/affinity-groups"
	redirect := MicroserviceRedirector(
		serverapi.RedirectRule{
			MatchPath:         pdAffinityPath,
			TargetPath:        targetAffinityPath,
			TargetServiceName: constant.SchedulingServiceName,
			MatchMethods: []string{
				http.MethodGet, http.MethodPost, http.MethodPatch,
				http.MethodPut, http.MethodDelete,
			},
		})
	return func(c *gin.Context) {
		// A caller must not use the generic forbidden-forward escape hatch to
		// write affinity metadata on PD while scheduling is independent. That
		// would create a second writer beside the scheduling primary.
		if c.Request.Header.Get(apiutil.XForbiddenForwardToMicroserviceHeader) == "true" &&
			c.Request.Method != http.MethodGet && c.Request.Method != http.MethodHead {
			svr := c.MustGet(ServerContextKey).(*server.Server)
			if svr.IsKeyspaceGroupEnabled() && svr.IsServiceIndependent(constant.SchedulingServiceName) {
				c.AbortWithStatusJSON(http.StatusServiceUnavailable, errs.ErrRedirectToNotPrimary.Error())
				return
			}
		}
		redirect(c)
	}
}
