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

// Package server provides a set of struct definitions for the resource group, can be imported.
package server

import (
	"encoding/json"
	"math"
	"time"

	"github.com/gogo/protobuf/proto"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

// overrideFillRateToleranceRate is the tolerance rate to override the fill rate, i.e., the fill rate will only
// be overridden if the new fill rate exceeds the original fill rate by this specified tolerance rate.
const overrideFillRateToleranceRate = 0.05

// ResourceGroup is the definition of a resource group, for REST API.
type ResourceGroup struct {
	syncutil.RWMutex
	Name string         `json:"name"`
	Mode rmpb.GroupMode `json:"mode"`
	// RU settings
	RUSettings *RequestUnitSettings     `json:"r_u_settings,omitempty"`
	Priority   uint32                   `json:"priority"`
	Runaway    *rmpb.RunawaySettings    `json:"runaway_settings,omitempty"`
	Background *rmpb.BackgroundSettings `json:"background_settings,omitempty"`
	// total ru consumption
	RUConsumption *rmpb.Consumption `json:"ru_consumption,omitempty"`
}

// RequestUnitSettings is the definition of the RU settings.
type RequestUnitSettings struct {
	RU *GroupTokenBucket `json:"r_u,omitempty"`
}

// Clone returns a deep copy of the RequestUnitSettings.
func (rus *RequestUnitSettings) Clone() *RequestUnitSettings {
	if rus == nil {
		return nil
	}
	var ru *GroupTokenBucket
	if rus.RU != nil {
		ru = rus.RU.clone()
	}
	return &RequestUnitSettings{
		RU: ru,
	}
}

// NewRequestUnitSettings creates a new RequestUnitSettings with the given token bucket.
func NewRequestUnitSettings(resourceGroupName string, tokenBucket *rmpb.TokenBucket) *RequestUnitSettings {
	return &RequestUnitSettings{
		RU: NewGroupTokenBucket(resourceGroupName, tokenBucket),
	}
}

func (rg *ResourceGroup) String() string {
	res, err := json.Marshal(rg)
	if err != nil {
		log.Error("marshal resource group failed", zap.Error(err))
		return ""
	}
	return string(res)
}

// Clone copies the resource group.
func (rg *ResourceGroup) Clone(withStats bool) *ResourceGroup {
	rg.RLock()
	defer rg.RUnlock()
	newRG := &ResourceGroup{
		Name:       rg.Name,
		Mode:       rg.Mode,
		Priority:   rg.Priority,
		RUSettings: rg.RUSettings.Clone(),
	}
	if rg.Runaway != nil {
		newRG.Runaway = proto.Clone(rg.Runaway).(*rmpb.RunawaySettings)
	}

	if rg.Background != nil {
		newRG.Background = proto.Clone(rg.Background).(*rmpb.BackgroundSettings)
	}

	if withStats && rg.RUConsumption != nil {
		newRG.RUConsumption = proto.Clone(rg.RUConsumption).(*rmpb.Consumption)
	}

	return newRG
}

func (rg *ResourceGroup) getRUToken() float64 {
	rg.Lock()
	defer rg.Unlock()
	return rg.RUSettings.RU.Tokens
}

func (rg *ResourceGroup) getPriority() float64 {
	rg.RLock()
	defer rg.RUnlock()
	return float64(rg.Priority)
}

// getFillRate returns the fill rate of the resource group.
// It will ignore the override fill rate and return the fill rate setting if the `ignoreOverride` is true.
func (rg *ResourceGroup) getFillRate(ignoreOverride ...bool) float64 {
	rg.RLock()
	defer rg.RUnlock()
	if len(ignoreOverride) > 0 && ignoreOverride[0] {
		return rg.RUSettings.RU.getFillRateSetting()
	}
	return rg.RUSettings.RU.getFillRate()
}

func (rg *ResourceGroup) getOverrideFillRate() float64 {
	rg.RLock()
	defer rg.RUnlock()
	return rg.RUSettings.RU.overrideFillRate
}

func (rg *ResourceGroup) overrideFillRate(new float64) {
	rg.Lock()
	defer rg.Unlock()
	rg.overrideFillRateLocked(new)
}

func (rg *ResourceGroup) overrideFillRateLocked(new float64) {
	original := rg.RUSettings.RU.overrideFillRate
	// If the fill rate has not been set before or the new value is negative,
	// set it to the new fill rate directly without checking the tolerance.
	if original < 0 || new < 0 {
		rg.RUSettings.RU.overrideFillRate = new
		return
	}
	// If the new fill rate exceeds the original by more than the allowed tolerance,
	// override it.
	if math.Abs(new-original) > original*overrideFillRateToleranceRate {
		rg.RUSettings.RU.overrideFillRate = new
		return
	}
}

// getBurstLimit returns the burst limit of the resource group.
// It will ignore the override burst limit and return the burst limit setting if the `ignoreOverride` is true.
func (rg *ResourceGroup) getBurstLimit(ignoreOverride ...bool) int64 {
	rg.RLock()
	defer rg.RUnlock()
	return rg.getBurstLimitLocked(ignoreOverride...)
}

func (rg *ResourceGroup) getBurstLimitLocked(ignoreOverride ...bool) int64 {
	if len(ignoreOverride) > 0 && ignoreOverride[0] {
		return rg.RUSettings.RU.getBurstLimitSetting()
	}
	return rg.RUSettings.RU.getBurstLimit()
}

func (rg *ResourceGroup) getOverrideBurstLimit() int64 {
	rg.RLock()
	defer rg.RUnlock()
	return rg.RUSettings.RU.overrideBurstLimit
}

func (rg *ResourceGroup) overrideBurstLimit(new int64) {
	rg.Lock()
	defer rg.Unlock()
	rg.overrideBurstLimitLocked(new)
}

func (rg *ResourceGroup) overrideBurstLimitLocked(new int64) {
	rg.RUSettings.RU.overrideBurstLimit = new
}

func (rg *ResourceGroup) overrideFillRateAndBurstLimit(fillRate float64, burstLimit int64) {
	rg.Lock()
	defer rg.Unlock()
	rg.overrideFillRateLocked(fillRate)
	rg.overrideBurstLimitLocked(burstLimit)
}

// PatchSettings patches the resource group settings.
// Only used to patch the resource group when updating.
// Note: the tokens is the delta value to patch.
func (rg *ResourceGroup) PatchSettings(metaGroup *rmpb.ResourceGroup) error {
	rg.Lock()
	defer rg.Unlock()

	if metaGroup.GetMode() != rg.Mode {
		return errors.New("only support reconfigure in same mode, maybe you should delete and create a new one")
	}
	if metaGroup.GetPriority() > 16 {
		return errors.New("invalid resource group priority, the value should be in [0,16]")
	}
	rg.Priority = metaGroup.Priority
	rg.Runaway = metaGroup.RunawaySettings
	rg.Background = metaGroup.BackgroundSettings
	switch rg.Mode {
	case rmpb.GroupMode_RUMode:
		settings := metaGroup.GetRUSettings()
		if settings == nil {
			return errors.New("invalid resource group settings, RU mode should set RU settings")
		}
		rg.RUSettings.RU.patch(settings.GetRU())
	case rmpb.GroupMode_RawMode:
		panic("no implementation")
	}
	log.Info("patch resource group settings", zap.String("name", rg.Name), zap.String("settings", rg.String()))
	return nil
}

// FromProtoResourceGroup converts a rmpb.ResourceGroup to a ResourceGroup.
func FromProtoResourceGroup(group *rmpb.ResourceGroup) *ResourceGroup {
	rg := &ResourceGroup{
		Name:          group.Name,
		Mode:          group.Mode,
		Priority:      group.Priority,
		Runaway:       group.RunawaySettings,
		Background:    group.BackgroundSettings,
		RUConsumption: &rmpb.Consumption{},
	}
	switch group.GetMode() {
	case rmpb.GroupMode_RUMode:
		if group.GetRUSettings() == nil {
			rg.RUSettings = NewRequestUnitSettings(rg.Name, nil)
		} else {
			rg.RUSettings = NewRequestUnitSettings(rg.Name, group.GetRUSettings().GetRU())
		}
		if group.RUStats != nil {
			rg.RUConsumption = group.RUStats
		}
	case rmpb.GroupMode_RawMode:
		panic("no implementation")
	}
	return rg
}

// RequestRU requests the RU of the resource group.
func (rg *ResourceGroup) RequestRU(
	now time.Time,
	requiredToken float64,
	targetPeriodMs, clientUniqueID uint64,
	sl *serviceLimiter,
) *rmpb.GrantedRUTokenBucket {
	rg.Lock()
	defer rg.Unlock()
	if rg.RUSettings == nil || rg.RUSettings.RU.Settings == nil {
		return nil
	}
	// First, try to get tokens from the resource group.
	tb, trickleTimeMs := rg.RUSettings.RU.request(now, requiredToken, targetPeriodMs, clientUniqueID)
	if tb == nil {
		return nil
	}
	// Then, try to apply the service limit.
	grantedTokens := tb.GetTokens()
	limitedTokens := sl.applyServiceLimit(now, grantedTokens)
	if limitedTokens < grantedTokens {
		tb.Tokens = limitedTokens
		// Retain the unused tokens for the later requests if it has a burst limit.
		if rg.getBurstLimitLocked() > 0 {
			rg.RUSettings.RU.lastLimitedTokens += grantedTokens - limitedTokens
		}
	}
	return &rmpb.GrantedRUTokenBucket{GrantedTokens: tb, TrickleTimeMs: trickleTimeMs}
}

// IntoProtoResourceGroup converts a ResourceGroup to a rmpb.ResourceGroup.
func (rg *ResourceGroup) IntoProtoResourceGroup(keyspaceID uint32) *rmpb.ResourceGroup {
	rg.RLock()
	defer rg.RUnlock()

	switch rg.Mode {
	case rmpb.GroupMode_RUMode: // RU mode
		group := &rmpb.ResourceGroup{
			Name:     rg.Name,
			Mode:     rmpb.GroupMode_RUMode,
			Priority: rg.Priority,
			RUSettings: &rmpb.GroupRequestUnitSettings{
				RU: rg.RUSettings.RU.GetTokenBucket(),
			},
			RunawaySettings:    rg.Runaway,
			BackgroundSettings: rg.Background,
			KeyspaceId:         &rmpb.KeyspaceIDValue{Value: keyspaceID},
		}

		if rg.RUConsumption != nil {
			consumption := *rg.RUConsumption
			group.RUStats = &consumption
		}
		return group
	case rmpb.GroupMode_RawMode: // Raw mode
		panic("no implementation")
	}
	return nil
}

// persistSettings persists the resource group settings.
// TODO: persist the state of the group separately.
func (rg *ResourceGroup) persistSettings(keyspaceID uint32, storage endpoint.ResourceGroupStorage) error {
	metaGroup := rg.IntoProtoResourceGroup(keyspaceID)
	return storage.SaveResourceGroupSetting(keyspaceID, rg.Name, metaGroup)
}

// GroupStates is the tokens set of a resource group.
type GroupStates struct {
	// RU tokens
	RU *GroupTokenBucketState `json:"r_u,omitempty"`
	// RU consumption
	RUConsumption *rmpb.Consumption `json:"ru_consumption,omitempty"`
	// raw resource tokens
	CPU     *GroupTokenBucketState `json:"cpu,omitempty"`
	IORead  *GroupTokenBucketState `json:"io_read,omitempty"`
	IOWrite *GroupTokenBucketState `json:"io_write,omitempty"`
}

// GetGroupStates get the token set of ResourceGroup.
func (rg *ResourceGroup) GetGroupStates() *GroupStates {
	rg.RLock()
	defer rg.RUnlock()

	switch rg.Mode {
	case rmpb.GroupMode_RUMode: // RU mode
		consumption := *rg.RUConsumption
		tokens := &GroupStates{
			RU:            rg.RUSettings.RU.GroupTokenBucketState.clone(),
			RUConsumption: &consumption,
		}
		return tokens
	case rmpb.GroupMode_RawMode: // Raw mode
		panic("no implementation")
	}
	return nil
}

// SetStatesIntoResourceGroup updates the state of resource group.
func (rg *ResourceGroup) SetStatesIntoResourceGroup(states *GroupStates) {
	switch rg.Mode {
	case rmpb.GroupMode_RUMode:
		if state := states.RU; state != nil {
			rg.RUSettings.RU.setState(state)
			log.Debug("update group token bucket state", zap.String("name", rg.Name), zap.Any("state", state))
		}
		if states.RUConsumption != nil {
			rg.UpdateRUConsumption(states.RUConsumption)
		}
	case rmpb.GroupMode_RawMode:
		panic("no implementation")
	}
}

// UpdateRUConsumption add delta consumption data to group ru statistics.
func (rg *ResourceGroup) UpdateRUConsumption(c *rmpb.Consumption) {
	rg.Lock()
	defer rg.Unlock()
	rc := rg.RUConsumption
	rc.RRU += c.RRU
	rc.WRU += c.WRU
	rc.ReadBytes += c.ReadBytes
	rc.WriteBytes += c.WriteBytes
	rc.TotalCpuTimeMs += c.TotalCpuTimeMs
	rc.SqlLayerCpuTimeMs += c.SqlLayerCpuTimeMs
	rc.KvReadRpcCount += c.KvReadRpcCount
	rc.KvWriteRpcCount += c.KvWriteRpcCount
	rc.ReadCrossAzTrafficBytes += c.ReadCrossAzTrafficBytes
	rc.WriteCrossAzTrafficBytes += c.WriteCrossAzTrafficBytes
}

// persistStates persists the resource group tokens.
func (rg *ResourceGroup) persistStates(keyspaceID uint32, storage endpoint.ResourceGroupStorage) error {
	states := rg.GetGroupStates()
	return storage.SaveResourceGroupStates(keyspaceID, rg.Name, states)
}
