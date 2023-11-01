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

package apis

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/gin-contrib/cors"
	"github.com/gin-contrib/gzip"
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	scheserver "github.com/tikv/pd/pkg/mcs/scheduling/server"
	mcsutils "github.com/tikv/pd/pkg/mcs/utils"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/handler"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/apiutil/multiservicesapi"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/unrolled/render"
)

// APIPathPrefix is the prefix of the API path.
const APIPathPrefix = "/scheduling/api/v1"
const handlerKey = "handler"

var (
	once            sync.Once
	apiServiceGroup = apiutil.APIServiceGroup{
		Name:       "scheduling",
		Version:    "v1",
		IsCore:     false,
		PathPrefix: APIPathPrefix,
	}
)

func init() {
	scheserver.SetUpRestHandler = func(srv *scheserver.Service) (http.Handler, apiutil.APIServiceGroup) {
		s := NewService(srv)
		return s.apiHandlerEngine, apiServiceGroup
	}
}

// Service is the tso service.
type Service struct {
	apiHandlerEngine *gin.Engine
	root             *gin.RouterGroup

	srv *scheserver.Service
	rd  *render.Render
}

type server struct {
	*scheserver.Server
}

func (s *server) GetCluster() sche.SchedulerCluster {
	return s.Server.GetCluster()
}

func createIndentRender() *render.Render {
	return render.New(render.Options{
		IndentJSON: true,
	})
}

// NewService returns a new Service.
func NewService(srv *scheserver.Service) *Service {
	once.Do(func() {
		// These global modification will be effective only for the first invoke.
		_ = godotenv.Load()
		gin.SetMode(gin.ReleaseMode)
	})
	apiHandlerEngine := gin.New()
	apiHandlerEngine.Use(gin.Recovery())
	apiHandlerEngine.Use(cors.Default())
	apiHandlerEngine.Use(gzip.Gzip(gzip.DefaultCompression))
	apiHandlerEngine.Use(func(c *gin.Context) {
		c.Set(multiservicesapi.ServiceContextKey, srv.Server)
		c.Set(handlerKey, handler.NewHandler(&server{srv.Server}))
		c.Next()
	})
	apiHandlerEngine.Use(multiservicesapi.ServiceRedirector())
	apiHandlerEngine.GET("metrics", mcsutils.PromHandler())
	pprof.Register(apiHandlerEngine)
	root := apiHandlerEngine.Group(APIPathPrefix)
	s := &Service{
		srv:              srv,
		apiHandlerEngine: apiHandlerEngine,
		root:             root,
		rd:               createIndentRender(),
	}
	s.RegisterAdminRouter()
	s.RegisterOperatorsRouter()
	s.RegisterSchedulersRouter()
	s.RegisterCheckersRouter()
	s.RegisterHotspotRouter()
	s.RegisterRegionsRouter()
	s.RegisterRegionLabelRouter()
	return s
}

// RegisterAdminRouter registers the router of the admin handler.
func (s *Service) RegisterAdminRouter() {
	router := s.root.Group("admin")
	router.PUT("/log", changeLogLevel)
}

// RegisterSchedulersRouter registers the router of the schedulers handler.
func (s *Service) RegisterSchedulersRouter() {
	router := s.root.Group("schedulers")
	router.GET("", getSchedulers)
	router.GET("/diagnostic/:name", getDiagnosticResult)
	// TODO: in the future, we should split pauseOrResumeScheduler to two different APIs.
	// And we need to do one-to-two forwarding in the API middleware.
	router.POST("/:name", pauseOrResumeScheduler)
}

// RegisterCheckersRouter registers the router of the checkers handler.
func (s *Service) RegisterCheckersRouter() {
	router := s.root.Group("checkers")
	router.GET("/:name", getCheckerByName)
	router.POST("/:name", pauseOrResumeChecker)
}

// RegisterHotspotRouter registers the router of the hotspot handler.
func (s *Service) RegisterHotspotRouter() {
	router := s.root.Group("hotspot")
	router.GET("/regions/write", getHotWriteRegions)
	router.GET("/regions/read", getHotReadRegions)
	router.GET("/regions/history", getHistoryHotRegions)
	router.GET("/stores", getHotStores)
	router.GET("/buckets", getHotBuckets)
}

// RegisterOperatorsRouter registers the router of the operators handler.
func (s *Service) RegisterOperatorsRouter() {
	router := s.root.Group("operators")
	router.GET("", getOperators)
	router.POST("", createOperator)
	router.GET("/:id", getOperatorByRegion)
	router.DELETE("/:id", deleteOperatorByRegion)
	router.GET("/records", getOperatorRecords)
}

// RegisterRegionsRouter registers the router of the regions handler.
func (s *Service) RegisterRegionsRouter() {
	router := s.root.Group("regions")
	router.GET("/:id/label/:key", getRegionLabelByKey)
	router.GET("/:id/labels", getRegionLabels)
	router.POST("/accelerate-schedule", accelerateRegionsScheduleInRange)
	router.POST("/accelerate-schedule/batch", accelerateRegionsScheduleInRanges)
	router.POST("/scatter", scatterRegions)
	router.POST("/split", splitRegions)
	router.GET("/replicated", checkRegionsReplicated)
}

// RegisterRegionLabelRouter registers the router of the region label handler.
func (s *Service) RegisterRegionLabelRouter() {
	router := s.root.Group("config/region-label")
	router.GET("rules", getAllRegionLabelRules)
	router.GET("rules/ids", getRegionLabelRulesByIDs)
	router.GET("rule/:id", getRegionLabelRuleByID)
}

func changeLogLevel(c *gin.Context) {
	svr := c.MustGet(multiservicesapi.ServiceContextKey).(*scheserver.Server)
	var level string
	if err := c.Bind(&level); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	if err := svr.SetLogLevel(level); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	log.SetLevel(logutil.StringToZapLogLevel(level))
	c.String(http.StatusOK, "The log level is updated.")
}

// @Tags     operators
// @Summary  Get an operator by ID.
// @Param    region_id  path  int  true  "A Region's Id"
// @Produce  json
// @Success  200  {object}  operator.OpWithStatus
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /operators/{id} [GET]
func getOperatorByRegion(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)
	id := c.Param("id")

	regionID, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	op, err := handler.GetOperatorStatus(regionID)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	c.IndentedJSON(http.StatusOK, op)
}

// @Tags     operators
// @Summary  List operators.
// @Param    kind  query  string  false  "Specify the operator kind."  Enums(admin, leader, region, waiting)
// @Produce  json
// @Success  200  {array}   operator.Operator
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /operators [GET]
func getOperators(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)
	var (
		results []*operator.Operator
		err     error
	)

	kinds := c.QueryArray("kind")
	if len(kinds) == 0 {
		results, err = handler.GetOperators()
	} else {
		results, err = handler.GetOperatorsByKinds(kinds)
	}

	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, results)
}

// @Tags     operator
// @Summary  Cancel a Region's pending operator.
// @Param    region_id  path  int  true  "A Region's Id"
// @Produce  json
// @Success  200  {string}  string  "The pending operator is canceled."
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /operators/{region_id} [delete]
func deleteOperatorByRegion(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)
	id := c.Param("id")

	regionID, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	if err = handler.RemoveOperator(regionID); err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	c.String(http.StatusOK, "The pending operator is canceled.")
}

// @Tags     operator
// @Summary  lists the finished operators since the given timestamp in second.
// @Param    from  query  integer  false  "From Unix timestamp"
// @Produce  json
// @Success  200  {object}  []operator.OpRecord
// @Failure  400  {string}  string  "The request is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /operators/records [get]
func getOperatorRecords(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)
	from, err := apiutil.ParseTime(c.Query("from"))
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	records, err := handler.GetRecords(from)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, records)
}

// FIXME: details of input json body params
// @Tags     operator
// @Summary  Create an operator.
// @Accept   json
// @Param    body  body  object  true  "json params"
// @Produce  json
// @Success  200  {string}  string  "The operator is created."
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /operators [post]
func createOperator(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)
	var input map[string]interface{}
	if err := c.BindJSON(&input); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	statusCode, result, err := handler.HandleOperatorCreation(input)
	if err != nil {
		c.String(statusCode, err.Error())
		return
	}
	if statusCode == http.StatusOK && result == nil {
		c.String(http.StatusOK, "The operator is created.")
		return
	}
	c.IndentedJSON(statusCode, result)
}

// @Tags     checkers
// @Summary  Get checker by name
// @Param    name  path  string  true  "The name of the checker."
// @Produce  json
// @Success  200  {string}  string  "The checker's status."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /checkers/{name} [get]
func getCheckerByName(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)
	name := c.Param("name")
	output, err := handler.GetCheckerStatus(name)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, output)
}

// FIXME: details of input json body params
// @Tags     checker
// @Summary  Pause or resume region merge.
// @Accept   json
// @Param    name  path  string  true  "The name of the checker."
// @Param    body  body  object  true  "json params"
// @Produce  json
// @Success  200  {string}  string  "Pause or resume the scheduler successfully."
// @Failure  400  {string}  string  "Bad format request."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /checker/{name} [post]
func pauseOrResumeChecker(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)
	var input map[string]int
	if err := c.BindJSON(&input); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	name := c.Param("name")
	t, ok := input["delay"]
	if !ok {
		c.String(http.StatusBadRequest, "missing pause time")
		return
	}
	if t < 0 {
		c.String(http.StatusBadRequest, "delay cannot be negative")
		return
	}
	if err := handler.PauseOrResumeChecker(name, int64(t)); err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	if t == 0 {
		c.String(http.StatusOK, "Resume the checker successfully.")
	} else {
		c.String(http.StatusOK, "Pause the checker successfully.")
	}
}

// @Tags     schedulers
// @Summary  List all created schedulers by status.
// @Produce  json
// @Success  200  {array}   string
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /schedulers [get]
func getSchedulers(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)
	status := c.Query("status")
	_, needTS := c.GetQuery("timestamp")
	output, err := handler.GetSchedulerByStatus(status, needTS)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, output)
}

// @Tags     schedulers
// @Summary  List schedulers diagnostic result.
// @Produce  json
// @Success  200  {array}   string
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /schedulers/diagnostic/{name} [get]
func getDiagnosticResult(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)
	name := c.Param("name")
	result, err := handler.GetDiagnosticResult(name)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, result)
}

// FIXME: details of input json body params
// @Tags     scheduler
// @Summary  Pause or resume a scheduler.
// @Accept   json
// @Param    name  path  string  true  "The name of the scheduler."
// @Param    body  body  object  true  "json params"
// @Produce  json
// @Success  200  {string}  string  "Pause or resume the scheduler successfully."
// @Failure  400  {string}  string  "Bad format request."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /schedulers/{name} [post]
func pauseOrResumeScheduler(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)

	var input map[string]int64
	if err := c.BindJSON(&input); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	name := c.Param("name")
	t, ok := input["delay"]
	if !ok {
		c.String(http.StatusBadRequest, "missing pause time")
		return
	}
	if err := handler.PauseOrResumeScheduler(name, t); err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.String(http.StatusOK, "Pause or resume the scheduler successfully.")
}

// @Tags     hotspot
// @Summary  List the hot write regions.
// @Produce  json
// @Success  200  {object}  statistics.StoreHotPeersInfos
// @Failure  400  {string}  string  "The request is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /hotspot/regions/write [get]
func getHotWriteRegions(c *gin.Context) {
	getHotRegions(utils.Write, c)
}

// @Tags     hotspot
// @Summary  List the hot read regions.
// @Produce  json
// @Success  200  {object}  statistics.StoreHotPeersInfos
// @Failure  400  {string}  string  "The request is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /hotspot/regions/read [get]
func getHotReadRegions(c *gin.Context) {
	getHotRegions(utils.Read, c)
}

func getHotRegions(typ utils.RWType, c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)

	storeIDs := c.QueryArray("store_id")
	if len(storeIDs) < 1 {
		hotRegions, err := handler.GetHotRegions(typ)
		if err != nil {
			c.String(http.StatusInternalServerError, err.Error())
			return
		}
		c.IndentedJSON(http.StatusOK, hotRegions)
		return
	}

	var ids []uint64
	for _, storeID := range storeIDs {
		id, err := strconv.ParseUint(storeID, 10, 64)
		if err != nil {
			c.String(http.StatusBadRequest, fmt.Sprintf("invalid store id: %s", storeID))
			return
		}
		_, err = handler.GetStore(id)
		if err != nil {
			c.String(http.StatusInternalServerError, err.Error())
			return
		}
		ids = append(ids, id)
	}

	hotRegions, err := handler.GetHotRegions(typ, ids...)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, hotRegions)
}

// @Tags     hotspot
// @Summary  List the hot stores.
// @Produce  json
// @Success  200  {object}  handler.HotStoreStats
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /hotspot/stores [get]
func getHotStores(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)
	stores, err := handler.GetHotStores()
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, stores)
}

// @Tags     hotspot
// @Summary  List the hot buckets.
// @Produce  json
// @Success  200  {object}  handler.HotBucketsResponse
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /hotspot/buckets [get]
func getHotBuckets(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)

	regionIDs := c.QueryArray("region_id")
	ids := make([]uint64, len(regionIDs))
	for i, regionID := range regionIDs {
		if id, err := strconv.ParseUint(regionID, 10, 64); err == nil {
			ids[i] = id
		}
	}
	ret, err := handler.GetHotBuckets(ids...)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, ret)
}

// @Tags     hotspot
// @Summary  List the history hot regions.
// @Accept   json
// @Produce  json
// @Success  200  {object}  storage.HistoryHotRegions
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /hotspot/regions/history [get]
func getHistoryHotRegions(c *gin.Context) {
	// TODO: support history hotspot in scheduling server with stateless in the future.
	// Ref: https://github.com/tikv/pd/pull/7183
	var res storage.HistoryHotRegions
	c.IndentedJSON(http.StatusOK, res)
}

// @Tags     region_label
// @Summary  Get label of a region.
// @Param    id   path  integer  true  "Region Id"
// @Param    key  path  string   true  "Label key"
// @Produce  json
// @Success  200  {string}  string
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  404  {string}  string  "The region does not exist."
// @Router   /regions/{id}/label/{key} [get]
func getRegionLabelByKey(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)

	idStr := c.Param("id")
	labelKey := c.Param("key") // TODO: test https://github.com/tikv/pd/pull/4004

	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	region := handler.GetRegion(id)
	if region == nil {
		c.String(http.StatusNotFound, errs.ErrRegionNotFound.FastGenByArgs().Error())
		return
	}

	l, err := handler.GetRegionLabeler()
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	labelValue := l.GetRegionLabel(region, labelKey)
	c.IndentedJSON(http.StatusOK, labelValue)
}

// @Tags     region_label
// @Summary  Get labels of a region.
// @Param    id  path  integer  true  "Region Id"
// @Produce  json
// @Success  200  {string}  string
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  404  {string}  string  "The region does not exist."
// @Router   /regions/{id}/labels [get]
func getRegionLabels(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)

	idStr := c.Param("id")
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	region := handler.GetRegion(id)
	if region == nil {
		c.String(http.StatusNotFound, errs.ErrRegionNotFound.FastGenByArgs().Error())
		return
	}
	l, err := handler.GetRegionLabeler()
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	labels := l.GetRegionLabels(region)
	c.IndentedJSON(http.StatusOK, labels)
}

// @Tags     region_label
// @Summary  List all label rules of cluster.
// @Produce  json
// @Success  200  {array}  labeler.LabelRule
// @Router   /config/region-label/rules [get]
func getAllRegionLabelRules(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)
	l, err := handler.GetRegionLabeler()
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	rules := l.GetAllLabelRules()
	c.IndentedJSON(http.StatusOK, rules)
}

// @Tags     region_label
// @Summary  Get label rules of cluster by ids.
// @Param    body  body  []string  true  "IDs of query rules"
// @Produce  json
// @Success  200  {array}   labeler.LabelRule
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/region-label/rules/ids [get]
func getRegionLabelRulesByIDs(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)
	l, err := handler.GetRegionLabeler()
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	var ids []string
	if err := c.BindJSON(&ids); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	rules, err := l.GetLabelRules(ids)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, rules)
}

// @Tags     region_label
// @Summary  Get label rule of cluster by id.
// @Param    id  path  string  true  "Rule Id"
// @Produce  json
// @Success  200  {object}  labeler.LabelRule
// @Failure  404  {string}  string  "The rule does not exist."
// @Router   /config/region-label/rule/{id} [get]
func getRegionLabelRuleByID(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)

	id, err := url.PathUnescape(c.Param("id"))
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	l, err := handler.GetRegionLabeler()
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	rule := l.GetLabelRule(id)
	if rule == nil {
		c.String(http.StatusNotFound, errs.ErrRegionRuleNotFound.FastGenByArgs().Error())
		return
	}
	c.IndentedJSON(http.StatusOK, rule)
}

// @Tags     region
// @Summary  Accelerate regions scheduling a in given range, only receive hex format for keys
// @Accept   json
// @Param    body   body   object   true   "json params"
// @Param    limit  query  integer  false  "Limit count"  default(256)
// @Produce  json
// @Success  200  {string}  string  "Accelerate regions scheduling in a given range [startKey, endKey)"
// @Failure  400  {string}  string  "The input is invalid."
// @Router   /regions/accelerate-schedule [post]
func accelerateRegionsScheduleInRange(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)

	var input map[string]interface{}
	if err := c.BindJSON(&input); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	rawStartKey, ok1 := input["start_key"].(string)
	rawEndKey, ok2 := input["end_key"].(string)
	if !ok1 || !ok2 {
		c.String(http.StatusBadRequest, "start_key or end_key is not string")
		return
	}

	limitStr, _ := c.GetQuery("limit")
	limit, err := handler.AdjustLimit(limitStr, 256 /*default limit*/)
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	err = handler.AccelerateRegionsScheduleInRange(rawStartKey, rawEndKey, limit)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.String(http.StatusOK, fmt.Sprintf("Accelerate regions scheduling in a given range [%s,%s)", rawStartKey, rawEndKey))
}

// @Tags     region
// @Summary  Accelerate regions scheduling in given ranges, only receive hex format for keys
// @Accept   json
// @Param    body   body   object   true   "json params"
// @Param    limit  query  integer  false  "Limit count"  default(256)
// @Produce  json
// @Success  200  {string}  string  "Accelerate regions scheduling in given ranges [startKey1, endKey1), [startKey2, endKey2), ..."
// @Failure  400  {string}  string  "The input is invalid."
// @Router   /regions/accelerate-schedule/batch [post]
func accelerateRegionsScheduleInRanges(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)

	var input []map[string]interface{}
	if err := c.BindJSON(&input); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	limitStr, _ := c.GetQuery("limit")
	limit, err := handler.AdjustLimit(limitStr, 256 /*default limit*/)
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	var msgBuilder strings.Builder
	msgBuilder.Grow(128)
	msgBuilder.WriteString("Accelerate regions scheduling in given ranges: ")
	var startKeys, endKeys [][]byte
	for _, rg := range input {
		startKey, rawStartKey, err := apiutil.ParseKey("start_key", rg)
		if err != nil {
			c.String(http.StatusBadRequest, err.Error())
			return
		}
		endKey, rawEndKey, err := apiutil.ParseKey("end_key", rg)
		if err != nil {
			c.String(http.StatusBadRequest, err.Error())
			return
		}
		startKeys = append(startKeys, startKey)
		endKeys = append(endKeys, endKey)
		msgBuilder.WriteString(fmt.Sprintf("[%s,%s), ", rawStartKey, rawEndKey))
	}
	err = handler.AccelerateRegionsScheduleInRanges(startKeys, endKeys, limit)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.String(http.StatusOK, msgBuilder.String())
}

// @Tags     region
// @Summary  Scatter regions by given key ranges or regions id distributed by given group with given retry limit
// @Accept   json
// @Param    body  body  object  true  "json params"
// @Produce  json
// @Success  200  {string}  string  "Scatter regions by given key ranges or regions id distributed by given group with given retry limit"
// @Failure  400  {string}  string  "The input is invalid."
// @Router   /regions/scatter [post]
func scatterRegions(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)

	var input map[string]interface{}
	if err := c.BindJSON(&input); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	rawStartKey, ok1 := input["start_key"].(string)
	rawEndKey, ok2 := input["end_key"].(string)
	group, _ := input["group"].(string)
	retryLimit := 5
	if rl, ok := input["retry_limit"].(float64); ok {
		retryLimit = int(rl)
	}

	opsCount, failures, err := func() (int, map[uint64]error, error) {
		if ok1 && ok2 {
			return handler.ScatterRegionsByRange(rawStartKey, rawEndKey, group, retryLimit)
		}
		ids, ok := typeutil.JSONToUint64Slice(input["regions_id"])
		if !ok {
			return 0, nil, errors.New("regions_id is invalid")
		}
		return handler.ScatterRegionsByID(ids, group, retryLimit, false)
	}()
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	s := handler.BuildScatterRegionsResp(opsCount, failures)
	c.IndentedJSON(http.StatusOK, &s)
}

// @Tags     region
// @Summary  Split regions with given split keys
// @Accept   json
// @Param    body  body  object  true  "json params"
// @Produce  json
// @Success  200  {string}  string  "Split regions with given split keys"
// @Failure  400  {string}  string  "The input is invalid."
// @Router   /regions/split [post]
func splitRegions(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)

	var input map[string]interface{}
	if err := c.BindJSON(&input); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	s, ok := input["split_keys"]
	if !ok {
		c.String(http.StatusBadRequest, "split_keys should be provided.")
		return
	}
	rawSplitKeys := s.([]string)
	if len(rawSplitKeys) < 1 {
		c.String(http.StatusBadRequest, "empty split keys.")
		return
	}
	fmt.Println(rawSplitKeys)
	retryLimit := 5
	if rl, ok := input["retry_limit"].(float64); ok {
		retryLimit = int(rl)
	}
	s, err := handler.SplitRegions(c.Request.Context(), rawSplitKeys, retryLimit)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, &s)
}

// @Tags     region
// @Summary  Check if regions in the given key ranges are replicated. Returns 'REPLICATED', 'INPROGRESS', or 'PENDING'. 'PENDING' means that there is at least one region pending for scheduling. Similarly, 'INPROGRESS' means there is at least one region in scheduling.
// @Param    startKey  query  string  true  "Regions start key, hex encoded"
// @Param    endKey    query  string  true  "Regions end key, hex encoded"
// @Produce  plain
// @Success  200  {string}  string  "INPROGRESS"
// @Failure  400  {string}  string  "The input is invalid."
// @Router   /regions/replicated [get]
func checkRegionsReplicated(c *gin.Context) {
	handler := c.MustGet(handlerKey).(*handler.Handler)
	rawStartKey, _ := c.GetQuery("start_key")
	rawEndKey, _ := c.GetQuery("end_key")
	state, err := handler.CheckRegionsReplicated(rawStartKey, rawEndKey)
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	c.String(http.StatusOK, state)
}
