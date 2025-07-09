// Copyright 2022 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,g
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"fmt"
	"math"
	"time"

	"github.com/gogo/protobuf/proto"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	defaultRefillRate    = 10000
	defaultInitialTokens = 10 * 10000
)

const (
	defaultReserveRatio    = 0.5
	defaultLoanCoefficient = 2
	maxAssignTokens        = math.MaxFloat64 / 1024 // assume max client connect is 1024
	slotExpireTimeout      = 10 * time.Minute
)

// GroupTokenBucket is a token bucket for a resource group.
// Now we don't save consumption in `GroupTokenBucket`, only statistics it in prometheus.
type GroupTokenBucket struct {
	// Settings is the setting of TokenBucket.
	// BurstLimit is used as below:
	//   - If b == 0, that means the limiter is unlimited capacity. default use in resource controller (burst with a rate within an unlimited capacity).
	//   - If b < 0, that means the limiter is unlimited capacity and fillrate(r) is ignored, can be seen as r == Inf (burst within an unlimited capacity).
	//   - If b > 0, that means the limiter is limited capacity.
	// MaxTokens limits the number of tokens that can be accumulated
	Settings              *rmpb.TokenLimitSettings `json:"settings,omitempty"`
	GroupTokenBucketState `json:"state,omitempty"`
}

// Clone returns the deep copy of GroupTokenBucket
func (gtb *GroupTokenBucket) Clone() *GroupTokenBucket {
	if gtb == nil {
		return nil
	}
	var settings *rmpb.TokenLimitSettings
	if gtb.Settings != nil {
		settings = proto.Clone(gtb.Settings).(*rmpb.TokenLimitSettings)
	}
	stateClone := *gtb.GroupTokenBucketState.Clone()
	return &GroupTokenBucket{
		Settings:              settings,
		GroupTokenBucketState: stateClone,
	}
}

func (gtb *GroupTokenBucket) setState(state *GroupTokenBucketState) {
	gtb.Tokens = state.Tokens
	gtb.LastUpdate = state.LastUpdate
	gtb.Initialized = state.Initialized
}

// TokenSlot is used to split a token bucket into multiple slots to
// server different clients within the same resource group.
type TokenSlot struct {
	// settings is the token limit settings for the slot.
	settings *rmpb.TokenLimitSettings
	// requireTokensSum is the number of tokens required.
	requireTokensSum float64
	// tokenCapacity is the number of tokens in the slot.
	tokenCapacity     float64
	lastTokenCapacity float64
	lastReqTime       time.Time
}

// GroupTokenBucketState is the running state of TokenBucket.
type GroupTokenBucketState struct {
	Tokens float64 `json:"tokens,omitempty"`
	// ClientUniqueID -> TokenSlot
	tokenSlots                 map[uint64]*TokenSlot
	clientConsumptionTokensSum float64
	lastBurstTokens            float64

	LastUpdate  *time.Time `json:"last_update,omitempty"`
	Initialized bool       `json:"initialized"`
	// settingChanged is used to avoid that the number of tokens returned is jitter because of changing fill rate.
	settingChanged      bool
	lastCheckExpireSlot time.Time
}

// Clone returns the copy of GroupTokenBucketState
func (gts *GroupTokenBucketState) Clone() *GroupTokenBucketState {
	var tokenSlots map[uint64]*TokenSlot
	if gts.tokenSlots != nil {
		tokenSlots = make(map[uint64]*TokenSlot)
		for id, tokens := range gts.tokenSlots {
			tokenSlots[id] = tokens
		}
	}

	var lastUpdate *time.Time
	if gts.LastUpdate != nil {
		newLastUpdate := *gts.LastUpdate
		lastUpdate = &newLastUpdate
	}
	return &GroupTokenBucketState{
		Tokens:                     gts.Tokens,
		LastUpdate:                 lastUpdate,
		Initialized:                gts.Initialized,
		tokenSlots:                 tokenSlots,
		clientConsumptionTokensSum: gts.clientConsumptionTokensSum,
		lastCheckExpireSlot:        gts.lastCheckExpireSlot,
	}
}

func (gts *GroupTokenBucketState) resetLoan(groupName string) {
	gts.settingChanged = false
	gts.Tokens = 0
	gts.clientConsumptionTokensSum = 0
	evenRatio := 1.0
	if l := len(gts.tokenSlots); l > 0 {
		evenRatio = 1 / float64(l)
	}
	checkAndLogNaN(evenRatio, groupName, "evenRatio", "in resetLoan")
	evenTokens := gts.Tokens * evenRatio
	checkAndLogNaN(evenTokens, groupName, "evenTokens", "in resetLoan")
	for _, slot := range gts.tokenSlots {
		slot.requireTokensSum = 0
		slot.tokenCapacity = evenTokens
		slot.lastTokenCapacity = evenTokens
	}
}

func (gts *GroupTokenBucketState) balanceSlotTokens(
	groupName string,
	clientUniqueID uint64,
	settings *rmpb.TokenLimitSettings,
	requiredToken, elapseTokens float64) {
	now := time.Now()
	slot, exist := gts.tokenSlots[clientUniqueID]
	if !exist {
		// Only slots that require a positive number will be considered alive,
		// but still need to allocate the elapsed tokens as well.
		if requiredToken != 0 {
			slot = &TokenSlot{lastReqTime: now}
			gts.tokenSlots[clientUniqueID] = slot
			gts.clientConsumptionTokensSum = 0
		}
	} else {
		slot.lastReqTime = now
		if gts.clientConsumptionTokensSum >= maxAssignTokens {
			gts.clientConsumptionTokensSum = 0
		}
		// Clean up slot that required 0.
		if requiredToken == 0 {
			delete(gts.tokenSlots, clientUniqueID)
			gts.clientConsumptionTokensSum = 0
		}
	}

	if time.Since(gts.lastCheckExpireSlot) >= slotExpireTimeout {
		gts.lastCheckExpireSlot = now
		for clientUniqueID, slot := range gts.tokenSlots {
			if time.Since(slot.lastReqTime) >= slotExpireTimeout {
				delete(gts.tokenSlots, clientUniqueID)
				log.Info("delete resource group slot because expire", zap.Time("last-req-time", slot.lastReqTime),
					zap.Any("expire timeout", slotExpireTimeout), zap.Any("del client id", clientUniqueID), zap.Any("len", len(gts.tokenSlots)))
			}
		}
	}
	if len(gts.tokenSlots) == 0 {
		return
	}
	evenRatio := 1 / float64(len(gts.tokenSlots))
	checkAndLogNaN(evenRatio, groupName, "evenRatio", "in balanceSlotTokens")
	if settings.GetBurstLimit() <= 0 {
		for _, slot := range gts.tokenSlots {
			fillRateVal := float64(settings.GetFillRate()) * evenRatio
			checkAndLogNaN(fillRateVal, groupName, "slot.settings.FillRate", "in balanceSlotTokens, unlimited burst")
			slot.settings = &rmpb.TokenLimitSettings{
				FillRate:   uint64(fillRateVal),
				BurstLimit: settings.GetBurstLimit(),
			}
		}
		return
	}

	for _, slot := range gts.tokenSlots {
		if gts.clientConsumptionTokensSum == 0 || len(gts.tokenSlots) == 1 {
			// Need to make each slot even.
			slot.tokenCapacity = evenRatio * gts.Tokens
			slot.lastTokenCapacity = evenRatio * gts.Tokens
			checkAndLogNaN(slot.tokenCapacity, groupName, "slot.tokenCapacity", "in balanceSlotTokens, even distribution")
			slot.requireTokensSum = 0
			gts.clientConsumptionTokensSum = 0

			var (
				fillRate   = float64(settings.GetFillRate()) * evenRatio
				burstLimit = float64(settings.GetBurstLimit()) * evenRatio
			)

			checkAndLogNaN(fillRate, groupName, "fillRate", "in balanceSlotTokens, even distribution")
			checkAndLogNaN(burstLimit, groupName, "burstLimit", "in balanceSlotTokens, even distribution")

			slot.settings = &rmpb.TokenLimitSettings{
				FillRate:   uint64(fillRate),
				BurstLimit: int64(burstLimit),
			}
		} else {
			// In order to have fewer tokens available to clients that are currently consuming more.
			// We have the following formula:
			// 		client1: (1 - a/N + 1/N) * 1/N
			// 		client2: (1 - b/N + 1/N) * 1/N
			// 		...
			// 		clientN: (1 - n/N + 1/N) * 1/N
			// Sum is:
			// 		(N - (a+b+...+n)/N +1) * 1/N => (N - 1 + 1) * 1/N => 1
			ratio := (1 - slot.requireTokensSum/gts.clientConsumptionTokensSum + evenRatio) * evenRatio
			checkAndLogNaN(gts.clientConsumptionTokensSum, groupName, "gts.clientConsumptionTokensSum", "in balanceSlotTokens, dynamic ratio")
			checkAndLogNaN(slot.requireTokensSum/gts.clientConsumptionTokensSum, groupName, "slot.requireTokensSum/gts.clientConsumptionTokensSum", "in balanceSlotTokens, dynamic ratio")
			checkAndLogNaN(ratio, groupName, "ratio", "in balanceSlotTokens, dynamic ratio")
			var (
				fillRate    = float64(settings.GetFillRate()) * ratio
				burstLimit  = float64(settings.GetBurstLimit()) * ratio
				assignToken = elapseTokens * ratio
			)

			checkAndLogNaN(fillRate, groupName, "fillRate", "in balanceSlotTokens, dynamic ratio")
			checkAndLogNaN(burstLimit, groupName, "burstLimit", "in balanceSlotTokens, dynamic ratio")
			checkAndLogNaN(assignToken, groupName, "assignToken", "in balanceSlotTokens, dynamic ratio")

			// Need to reserve burst limit to next balance.
			if burstLimit > 0 && slot.tokenCapacity > burstLimit {
				reservedTokens := slot.tokenCapacity - burstLimit
				gts.lastBurstTokens += reservedTokens
				gts.Tokens -= reservedTokens
				assignToken -= reservedTokens
				checkAndLogNaN(reservedTokens, groupName, "reservedTokens", "in balanceSlotTokens, dynamic ratio")
				checkAndLogNaN(gts.lastBurstTokens, groupName, "gts.lastBurstTokens", "in balanceSlotTokens, dynamic ratio")
				checkAndLogNaN(gts.Tokens, groupName, "gts.Tokens", "in balanceSlotTokens, dynamic ratio")
				checkAndLogNaN(assignToken, groupName, "assignToken_after_reserve", "in balanceSlotTokens, dynamic ratio")
			}

			slot.tokenCapacity += assignToken
			slot.lastTokenCapacity += assignToken
			checkAndLogNaN(slot.tokenCapacity, groupName, "slot.tokenCapacity", "in balanceSlotTokens, after assign")
			slot.settings = &rmpb.TokenLimitSettings{
				FillRate:   uint64(fillRate),
				BurstLimit: int64(burstLimit),
			}
		}
	}
	if requiredToken != 0 {
		// Only slots that require a positive number will be considered alive.
		slot.requireTokensSum += requiredToken
		gts.clientConsumptionTokensSum += requiredToken
	}
}

// NewGroupTokenBucket returns a new GroupTokenBucket
func NewGroupTokenBucket(tokenBucket *rmpb.TokenBucket) *GroupTokenBucket {
	if tokenBucket == nil || tokenBucket.Settings == nil {
		return &GroupTokenBucket{}
	}
	return &GroupTokenBucket{
		Settings: tokenBucket.GetSettings(),
		GroupTokenBucketState: GroupTokenBucketState{
			Tokens:     tokenBucket.GetTokens(),
			tokenSlots: make(map[uint64]*TokenSlot),
		},
	}
}

// GetTokenBucket returns the grpc protoc struct of GroupTokenBucket.
func (gtb *GroupTokenBucket) GetTokenBucket() *rmpb.TokenBucket {
	if gtb.Settings == nil {
		return nil
	}
	return &rmpb.TokenBucket{
		Settings: gtb.Settings,
		Tokens:   gtb.Tokens,
	}
}

// patch patches the token bucket settings.
func (gtb *GroupTokenBucket) patch(tb *rmpb.TokenBucket) {
	if tb == nil {
		return
	}
	if setting := proto.Clone(tb.GetSettings()).(*rmpb.TokenLimitSettings); setting != nil {
		gtb.Settings = setting
		gtb.settingChanged = true
	}

	// The settings in token is delta of the last update and now.
	gtb.Tokens += tb.GetTokens()
}

// init initializes the group token bucket.
func (gtb *GroupTokenBucket) init(now time.Time, clientID uint64) {
	if gtb.Settings.FillRate == 0 {
		gtb.Settings.FillRate = defaultRefillRate
	}
	if gtb.Tokens < defaultInitialTokens && gtb.Settings.BurstLimit > 0 {
		gtb.Tokens = defaultInitialTokens
	}
	// init slot
	gtb.tokenSlots[clientID] = &TokenSlot{
		settings:          gtb.Settings,
		tokenCapacity:     gtb.Tokens,
		lastTokenCapacity: gtb.Tokens,
	}
	gtb.LastUpdate = &now
	gtb.lastCheckExpireSlot = now
	gtb.Initialized = true
}

// updateTokens updates the tokens and settings.
func (gtb *GroupTokenBucket) updateTokens(groupName string, now time.Time, burstLimit int64, clientUniqueID uint64, consumptionToken float64) {
	var elapseTokens float64
	if !gtb.Initialized {
		gtb.init(now, clientUniqueID)
	} else if burst := float64(burstLimit); burst > 0 {
		if delta := now.Sub(*gtb.LastUpdate); delta > 0 {
			elapseTokens = float64(gtb.Settings.GetFillRate())*delta.Seconds() + gtb.lastBurstTokens
			checkAndLogNaN(elapseTokens, groupName, "elapseTokens", "in updateTokens")
			gtb.lastBurstTokens = 0
			gtb.Tokens += elapseTokens
		}
		if gtb.Tokens > burst {
			elapseTokens -= gtb.Tokens - burst
			gtb.Tokens = burst
			checkAndLogNaN(elapseTokens, groupName, "elapseTokens_after_burst_limit", "in updateTokens")
		}
	}
	gtb.LastUpdate = &now
	// Reloan when setting changed
	if gtb.settingChanged && gtb.Tokens <= 0 {
		elapseTokens = 0
		gtb.resetLoan(groupName)
	}
	// Balance each slots.
	gtb.balanceSlotTokens(groupName, clientUniqueID, gtb.Settings, consumptionToken, elapseTokens)
}

// request requests tokens from the corresponding slot.
func (gtb *GroupTokenBucket) request(name string, now time.Time,
	neededTokens float64,
	targetPeriodMs, clientUniqueID uint64,
) (*rmpb.TokenBucket, int64) {
	burstLimit := gtb.Settings.GetBurstLimit()
	gtb.updateTokens(name, now, burstLimit, clientUniqueID, neededTokens)
	slot, ok := gtb.tokenSlots[clientUniqueID]
	if !ok {
		return &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{BurstLimit: burstLimit}}, 0
	}
	res, trickleDuration := slot.assignSlotTokens(name, neededTokens, targetPeriodMs)
	// Update bucket to record all tokens.
	gtb.Tokens -= slot.lastTokenCapacity - slot.tokenCapacity
	checkAndLogNaN(gtb.Tokens, name, "gtb.Tokens", "in request, after assign")
	slot.lastTokenCapacity = slot.tokenCapacity

	return res, trickleDuration
}

func (ts *TokenSlot) assignSlotTokens(groupName string, neededTokens float64, targetPeriodMs uint64) (*rmpb.TokenBucket, int64) {
	var res rmpb.TokenBucket
	burstLimit := ts.settings.GetBurstLimit()
	res.Settings = &rmpb.TokenLimitSettings{BurstLimit: burstLimit}
	// If BurstLimit < 0, just return.
	if burstLimit < 0 {
		res.Tokens = neededTokens
		return &res, 0
	}
	// FillRate is used for the token server unavailable in abnormal situation.
	if neededTokens <= 0 {
		return &res, 0
	}
	checkAndLogNaN(ts.tokenCapacity, groupName, "ts.tokenCapacity", "start of assignSlotTokens")
	// If the current tokens can directly meet the requirement, returns the need token.
	if ts.tokenCapacity >= neededTokens {
		ts.tokenCapacity -= neededTokens
		// granted the total request tokens
		res.Tokens = neededTokens
		checkAndLogNaN(ts.tokenCapacity, groupName, "ts.tokenCapacity", "in assignSlotTokens, sufficient tokens")
		checkAndLogNaN(res.Tokens, groupName, "res.Tokens", "in assignSlotTokens, sufficient tokens")
		return &res, 0
	}

	// Firstly allocate the remaining tokens
	var grantedTokens float64
	hasRemaining := false
	if ts.tokenCapacity > 0 {
		grantedTokens = ts.tokenCapacity
		neededTokens -= grantedTokens
		ts.tokenCapacity = 0
		hasRemaining = true
		checkAndLogNaN(grantedTokens, groupName, "grantedTokens", "in assignSlotTokens, after allocating remaining")
		checkAndLogNaN(neededTokens, groupName, "neededTokens", "in assignSlotTokens, after allocating remaining")
	}

	var (
		targetPeriodTime    = time.Duration(targetPeriodMs) * time.Millisecond
		targetPeriodTimeSec = targetPeriodTime.Seconds()
		trickleTime         = 0.
		fillRate            = float64(ts.settings.GetFillRate())
	)
	checkAndLogNaN(fillRate, groupName, "fillRate", "in assignSlotTokens, start of loan calculation")

	loanCoefficient := defaultLoanCoefficient
	// When BurstLimit less or equal FillRate, the server does not accumulate a significant number of tokens.
	// So we don't need to smooth the token allocation speed.
	if burstLimit > 0 && burstLimit <= int64(fillRate) {
		loanCoefficient = 1
	}
	// When there are loan, the allotment will match the fill rate.
	// We will have k threshold, beyond which the token allocation will be a minimum.
	// The threshold unit is `fill rate * target period`.
	//               |
	// k*fill_rate   |* * * * * *     *
	//               |                        *
	//     ***       |                                 *
	//               |                                           *
	//               |                                                     *
	//   fill_rate   |                                                                 *
	// reserve_rate  |                                                                              *
	//               |
	// grant_rate 0  ------------------------------------------------------------------------------------
	//         loan      ***    k*period_token    (k+k-1)*period_token    ***      (k+k+1...+1)*period_token
	p := make([]float64, loanCoefficient)
	p[0] = float64(loanCoefficient) * float64(fillRate) * targetPeriodTimeSec
	checkAndLogNaN(p[0], groupName, "p[0]", "in assignSlotTokens, loan calculation")
	for i := 1; i < loanCoefficient; i++ {
		p[i] = float64(loanCoefficient-i)*float64(fillRate)*targetPeriodTimeSec + p[i-1]
		checkAndLogNaN(p[i], groupName, fmt.Sprintf("p[0],%d", i), "in assignSlotTokens, loan calculation")
	}
	for i := 0; i < loanCoefficient && neededTokens > 0 && trickleTime < targetPeriodTimeSec; i++ {
		loan := -ts.tokenCapacity
		checkAndLogNaN(loan, groupName, fmt.Sprintf("loan,%d", i), "in assignSlotTokens, loan loop")
		if loan >= p[i] {
			continue
		}
		roundReserveTokens := p[i] - loan
		fillRate := float64(loanCoefficient-i) * float64(fillRate)
		checkAndLogNaN(roundReserveTokens, groupName, fmt.Sprintf("roundReserveTokens,%d", i), "in assignSlotTokens, loan loop")
		checkAndLogNaN(fillRate, groupName, fmt.Sprintf("fillRate,%d", i), "in assignSlotTokens, loan loop")
		if roundReserveTokens > neededTokens {
			ts.tokenCapacity -= neededTokens
			grantedTokens += neededTokens
			trickleTime += grantedTokens / fillRate
			checkAndLogNaN(trickleTime, groupName, "trickleTime", "in assignSlotTokens, loan loop, roundReserve>needed")
			neededTokens = 0
		} else {
			roundReserveTime := roundReserveTokens / fillRate
			checkAndLogNaN(roundReserveTime, groupName, "roundReserveTime", "in assignSlotTokens, loan loop, roundReserve<=needed")
			if roundReserveTime+trickleTime >= targetPeriodTimeSec {
				roundTokens := (targetPeriodTimeSec - trickleTime) * fillRate
				neededTokens -= roundTokens
				ts.tokenCapacity -= roundTokens
				grantedTokens += roundTokens
				trickleTime = targetPeriodTimeSec
				checkAndLogNaN(roundTokens, groupName, "roundTokens", "in assignSlotTokens, loan loop, time exceeded")
			} else {
				grantedTokens += roundReserveTokens
				neededTokens -= roundReserveTokens
				ts.tokenCapacity -= roundReserveTokens
				trickleTime += roundReserveTime
			}
			checkAndLogNaN(ts.tokenCapacity, groupName, fmt.Sprintf("ts.tokenCapacity,%d", i), "in assignSlotTokens, end of loan loop")
			checkAndLogNaN(grantedTokens, groupName, fmt.Sprintf("neededTokensy,%d", i), "in assignSlotTokens, end of loan loop")
			checkAndLogNaN(neededTokens, groupName, fmt.Sprintf("grantedTokens,%d", i), "in assignSlotTokens, end of loan loop")
			checkAndLogNaN(trickleTime, groupName, fmt.Sprintf("trickleTime,%d", i), "in assignSlotTokens, end of loan loop")
		}
	}
	if neededTokens > 0 && grantedTokens < defaultReserveRatio*float64(fillRate)*targetPeriodTimeSec {
		reservedTokens := math.Min(neededTokens+grantedTokens, defaultReserveRatio*float64(fillRate)*targetPeriodTimeSec)
		ts.tokenCapacity -= reservedTokens - grantedTokens
		grantedTokens = reservedTokens
		checkAndLogNaN(reservedTokens, groupName, "reservedTokens", "in assignSlotTokens, final reservation")
		checkAndLogNaN(ts.tokenCapacity, groupName, "ts.tokenCapacity", "in assignSlotTokens, final reservation")
		checkAndLogNaN(grantedTokens, groupName, "grantedTokens", "in assignSlotTokens, final reservation")
	}
	res.Tokens = grantedTokens
	checkAndLogNaN(res.Tokens, groupName, "res.Tokens", "final value before return")

	var trickleDuration time.Duration
	// Can't directly treat targetPeriodTime as trickleTime when there is a token remaining.
	// If treated, client consumption will be slowed down (actually could be increased).
	if hasRemaining {
		trickleDuration = time.Duration(math.Min(trickleTime, targetPeriodTime.Seconds()) * float64(time.Second))
	} else {
		trickleDuration = targetPeriodTime
	}
	checkAndLogNaN(float64(trickleDuration.Milliseconds()), groupName, "trickleDuration", "final value before return")
	return &res, trickleDuration.Milliseconds()
}

func checkAndLogNaN(value float64, groupName string, varName string, contextMsg string) {
	if math.IsNaN(value) {
		log.Warn("[RC_NaN_CHECK] Detected NaN value during token calculation",
			zap.String("group", groupName),
			zap.String("variable", varName),
			zap.String("context", contextMsg),
		)
	}
}
