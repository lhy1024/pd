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

package server

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
)

const testResourceGroupName = "test"

func TestGroupTokenBucketUpdateAndPatch(t *testing.T) {
	re := require.New(t)
	tbSetting := &rmpb.TokenBucket{
		Tokens: 200000,
		Settings: &rmpb.TokenLimitSettings{
			FillRate:   2000,
			BurstLimit: 20000000,
		},
	}

	clientUniqueID := uint64(0)
	tb := NewGroupTokenBucket(testResourceGroupName, tbSetting)
	time1 := time.Now()
	tb.request(time1, 0, 0, clientUniqueID)
	re.LessOrEqual(math.Abs(tbSetting.Tokens-tb.Tokens), 1e-7)
	re.Equal(float64(tbSetting.Settings.FillRate), tb.getFillRate())

	tbSetting = &rmpb.TokenBucket{
		Tokens: -100000,
		Settings: &rmpb.TokenLimitSettings{
			FillRate:   1000,
			BurstLimit: 10000000,
		},
	}
	tb.patch(tbSetting)
	time.Sleep(10 * time.Millisecond)
	time2 := time.Now()
	tb.request(time2, 0, 0, clientUniqueID)
	re.LessOrEqual(math.Abs(100000-tb.Tokens), time2.Sub(time1).Seconds()*float64(tbSetting.Settings.FillRate)+1e7)
	re.Equal(float64(tbSetting.Settings.FillRate), tb.getFillRate())

	tbSetting = &rmpb.TokenBucket{
		Tokens: 0,
		Settings: &rmpb.TokenLimitSettings{
			FillRate:   2000,
			BurstLimit: -1,
		},
	}
	tb = NewGroupTokenBucket(testResourceGroupName, tbSetting)
	tb.request(time2, 0, 0, clientUniqueID)
	re.LessOrEqual(math.Abs(tbSetting.Tokens), 1e-7)
	time3 := time.Now()
	tb.request(time3, 0, 0, clientUniqueID)
	re.LessOrEqual(math.Abs(tbSetting.Tokens), 1e-7)

	tbSetting = &rmpb.TokenBucket{
		Tokens: 200000,
		Settings: &rmpb.TokenLimitSettings{
			FillRate:   2000,
			BurstLimit: -1,
		},
	}
	tb = NewGroupTokenBucket(testResourceGroupName, tbSetting)
	tb.request(time3, 0, 0, clientUniqueID)
	re.LessOrEqual(math.Abs(tbSetting.Tokens-200000), 1e-7)
	time.Sleep(10 * time.Millisecond)
	time4 := time.Now()
	tb.request(time4, 0, 0, clientUniqueID)
	re.LessOrEqual(math.Abs(tbSetting.Tokens-200000), 1e-7)
}

func TestGroupTokenBucketRequest(t *testing.T) {
	re := require.New(t)
	tbSetting := &rmpb.TokenBucket{
		Tokens: 200000,
		Settings: &rmpb.TokenLimitSettings{
			FillRate:   2000,
			BurstLimit: 20000000,
		},
	}

	gtb := NewGroupTokenBucket(testResourceGroupName, tbSetting)
	time1 := time.Now()
	clientUniqueID := uint64(0)
	tb, trickle := gtb.request(time1, 190000, uint64(time.Second)*10/uint64(time.Millisecond), clientUniqueID)
	re.LessOrEqual(math.Abs(tb.Tokens-190000), 1e-7)
	re.Zero(trickle)
	// need to lend token
	tb, trickle = gtb.request(time1, 11000, uint64(time.Second)*10/uint64(time.Millisecond), clientUniqueID)
	re.LessOrEqual(math.Abs(tb.Tokens-11000), 1e-7)
	re.Equal(int64(time.Second)*11000./4000./int64(time.Millisecond), trickle)
	tb, trickle = gtb.request(time1, 35000, uint64(time.Second)*10/uint64(time.Millisecond), clientUniqueID)
	re.LessOrEqual(math.Abs(tb.Tokens-35000), 1e-7)
	re.Equal(int64(time.Second)*10/int64(time.Millisecond), trickle)
	tb, trickle = gtb.request(time1, 60000, uint64(time.Second)*10/uint64(time.Millisecond), clientUniqueID)
	re.LessOrEqual(math.Abs(tb.Tokens-22000), 1e-7)
	re.Equal(int64(time.Second)*10/int64(time.Millisecond), trickle)
	// Get reserved 10000 tokens = fillrate(2000) * 10 * defaultReserveRatio(0.5)
	// Max loan tokens is 60000.
	tb, trickle = gtb.request(time1, 3000, uint64(time.Second)*10/uint64(time.Millisecond), clientUniqueID)
	re.LessOrEqual(math.Abs(tb.Tokens-3000), 1e-7)
	re.Equal(int64(time.Second)*10/int64(time.Millisecond), trickle)
	tb, trickle = gtb.request(time1, 12000, uint64(time.Second)*10/uint64(time.Millisecond), clientUniqueID)
	re.LessOrEqual(math.Abs(tb.Tokens-10000), 1e-7)
	re.Equal(int64(time.Second)*10/int64(time.Millisecond), trickle)
	time2 := time1.Add(20 * time.Second)
	tb, trickle = gtb.request(time2, 20000, uint64(time.Second)*10/uint64(time.Millisecond), clientUniqueID)
	re.LessOrEqual(math.Abs(tb.Tokens-20000), 1e-7)
	re.Equal(int64(time.Second)*10/int64(time.Millisecond), trickle)
}

func TestGroupTokenBucketRequestBurstLimit(t *testing.T) {
	re := require.New(t)
	testGroupSetting := func(tbSetting *rmpb.TokenBucket, expectedFillRate, expectedBurstLimit int64) {
		gtb := NewGroupTokenBucket(testResourceGroupName, tbSetting)
		time1 := time.Now()
		clientUniqueID := uint64(0)
		gtb.request(time1, 190000, uint64(time.Second)*10/uint64(time.Millisecond), clientUniqueID)
		re.Contains(gtb.tokenSlots, clientUniqueID)
		// it should not be able to change group settings
		groupSetting := gtb.tokenSlots[clientUniqueID]
		re.Equal(expectedBurstLimit, groupSetting.burstLimit)
		re.Equal(uint64(expectedFillRate), groupSetting.fillRate)
		// it should not be able to change gtb settings
		re.Equal(float64(tbSetting.GetSettings().FillRate), gtb.getFillRate())
		re.Equal(tbSetting.GetSettings().BurstLimit, gtb.getBurstLimitSetting())
	}

	// case 1: fillrate = 2000, burstLimit = 2000,0,-1,-2
	testGroupSetting(&rmpb.TokenBucket{
		Tokens: 200000,
		Settings: &rmpb.TokenLimitSettings{
			FillRate:   2000,
			BurstLimit: 2000,
		},
	}, 2000, 2000)

	testGroupSetting(&rmpb.TokenBucket{
		Tokens: 200000,
		Settings: &rmpb.TokenLimitSettings{
			FillRate:   2000,
			BurstLimit: 0,
		},
	}, 2000, 0)

	testGroupSetting(&rmpb.TokenBucket{
		Tokens: 200000,
		Settings: &rmpb.TokenLimitSettings{
			FillRate:   2000,
			BurstLimit: UnlimitedBurstLimit,
		},
	}, 2000, UnlimitedBurstLimit)

	testGroupSetting(&rmpb.TokenBucket{
		Tokens: 200000,
		Settings: &rmpb.TokenLimitSettings{
			FillRate:   2000,
			BurstLimit: -2,
		},
	}, 2000+defaultModeratedBurstRate, 2000+defaultModeratedBurstRate)

	// case 2: fillrate = unlimited, burstLimit = 2000,0,-1,-2
	testGroupSetting(&rmpb.TokenBucket{
		Tokens: 200000,
		Settings: &rmpb.TokenLimitSettings{
			FillRate:   UnlimitedRate,
			BurstLimit: 2000,
		},
	}, UnlimitedRate, 2000)

	testGroupSetting(&rmpb.TokenBucket{
		Tokens: 200000,
		Settings: &rmpb.TokenLimitSettings{
			FillRate:   UnlimitedRate,
			BurstLimit: 0,
		},
	}, UnlimitedRate, 0) // burstLimit = 0 is a special case

	testGroupSetting(&rmpb.TokenBucket{
		Tokens: 200000,
		Settings: &rmpb.TokenLimitSettings{
			FillRate:   UnlimitedRate,
			BurstLimit: UnlimitedBurstLimit,
		},
	}, UnlimitedRate, UnlimitedBurstLimit)

	testGroupSetting(&rmpb.TokenBucket{
		Tokens: 200000,
		Settings: &rmpb.TokenLimitSettings{
			FillRate:   UnlimitedRate,
			BurstLimit: -2,
		},
	}, UnlimitedRate, UnlimitedRate)
}

func TestGroupTokenBucketRequestLoop(t *testing.T) {
	re := require.New(t)
	tbSetting := &rmpb.TokenBucket{
		Tokens: 50000,
		Settings: &rmpb.TokenLimitSettings{
			FillRate:   2000,
			BurstLimit: 200000,
		},
	}

	gtb := NewGroupTokenBucket(testResourceGroupName, tbSetting)
	clientUniqueID := uint64(0)
	initialTime := time.Now()

	// Initialize the token bucket
	gtb.init(initialTime, clientUniqueID)
	gtb.Tokens = 50000

	const timeIncrement = 5 * time.Second
	const targetPeriod = 5 * time.Second
	const defaultTrickleMs = int64(targetPeriod) / int64(time.Millisecond)

	// Define the test cases in a table
	testCases := []struct {
		requestTokens                 float64
		assignedTokens                float64
		globalBucketTokensAfterAssign float64
		expectedTrickleMs             int64
	}{
		/* requestTokens, assignedTokens, globalBucketTokensAfterAssign, TrickleMs  */
		{50000, 50000, 0, 0},
		{50000, 30000, -20000, defaultTrickleMs},
		{30000, 15000, -25000, defaultTrickleMs},
		{15000, 12500, -27500, defaultTrickleMs},
		{12500, 11250, -28750, defaultTrickleMs},
		{11250, 10625, -29375, defaultTrickleMs},
		// RU_PER_SEC is close to 2000, RU_PER_SEC =  assignedTokens / TrickleMs / 1000.
		{10625, 10312.5, -29687.5, defaultTrickleMs},
		{10312.5, 10156.25, -29843.75, defaultTrickleMs},
		{10156.25, 10078.125, -29921.875, defaultTrickleMs},
		{10078.125, 10039.0625, -29960.9375, defaultTrickleMs},
		{10039.0625, 10019.53125, -29980.46875, defaultTrickleMs},
		{10019.53125, 10009.765625, -29990.234375, defaultTrickleMs},
		{10009.765625, 10004.8828125, -29995.1171875, defaultTrickleMs},
		{10004.8828125, 10002.44140625, -29997.55859375, defaultTrickleMs},
		{10002.44140625, 10001.220703125, -29998.779296875, defaultTrickleMs},
		{10001.220703125, 10000.6103515625, -29999.3896484375, defaultTrickleMs},
		{10000.6103515625, 10000.30517578125, -29999.69482421875, defaultTrickleMs},
		{10000.30517578125, 10000.152587890625, -29999.847412109375, defaultTrickleMs},
		{10000.152587890625, 10000.0762939453125, -29999.9237060546875, defaultTrickleMs},
		{10000.0762939453125, 10000.038146972656, -29999.961853027343, defaultTrickleMs},
	}

	currentTime := initialTime
	for i, tc := range testCases {
		tb, trickle := gtb.request(currentTime, tc.requestTokens, uint64(targetPeriod)/uint64(time.Millisecond), clientUniqueID)
		re.Equalf(tc.globalBucketTokensAfterAssign, gtb.GetTokenBucket().Tokens, "Test case %d failed: expected bucket tokens %f, got %f", i, tc.globalBucketTokensAfterAssign, gtb.GetTokenBucket().Tokens)
		re.LessOrEqualf(math.Abs(tb.Tokens-tc.assignedTokens), 1e-7, "Test case %d failed: expected tokens %f, got %f", i, tc.assignedTokens, tb.Tokens)
		re.Equalf(tc.expectedTrickleMs, trickle, "Test case %d failed: expected trickle %d, got %d", i, tc.expectedTrickleMs, trickle)
		currentTime = currentTime.Add(timeIncrement)
	}
}

// Regression for unfair penalty: heavier slot should not get less fill than lighter one.
func TestSlotBalancePenaltyReproduction(t *testing.T) {
	re := require.New(t)
	now := time.Now()
	fillRate := uint64(1000)
	burst := int64(1000)
	gtb := &GroupTokenBucket{
		Settings: &rmpb.TokenLimitSettings{FillRate: fillRate, BurstLimit: burst},
		GroupTokenBucketState: GroupTokenBucketState{
			Tokens:             float64(fillRate),
			resourceGroupName:  testResourceGroupName,
			tokenSlots:         make(map[uint64]*tokenSlot),
			overrideFillRate:   -1,
			overrideBurstLimit: -1,
			Initialized:        true,
		},
	}
	gtb.tokenSlots[1] = &tokenSlot{requireTokensSum: 900, lastReqTime: now}
	gtb.tokenSlots[2] = &tokenSlot{requireTokensSum: 100, lastReqTime: now}
	gtb.clientConsumptionTokensSum = 1000

	gtb.balanceSlotTokens(now, 1, 1, float64(fillRate))
	re.GreaterOrEqual(gtb.tokenSlots[1].fillRate, gtb.tokenSlots[2].fillRate)
}

func TestSlotBalanceHighWaterBypass(t *testing.T) {
	re := require.New(t)
	now := time.Now()
	fillRate := uint64(1000)
	burst := int64(1000)
	gtb := &GroupTokenBucket{
		Settings: &rmpb.TokenLimitSettings{FillRate: fillRate, BurstLimit: burst},
		GroupTokenBucketState: GroupTokenBucketState{
			Tokens:             float64(burst),
			resourceGroupName:  testResourceGroupName,
			tokenSlots:         make(map[uint64]*tokenSlot),
			overrideFillRate:   -1,
			overrideBurstLimit: -1,
			Initialized:        true,
		},
	}
	gtb.tokenSlots[1] = &tokenSlot{requireTokensSum: 900, lastReqTime: now}
	gtb.tokenSlots[2] = &tokenSlot{requireTokensSum: 100, lastReqTime: now}
	gtb.clientConsumptionTokensSum = 1000

	gtb.balanceSlotTokens(now, 1, 1, float64(fillRate))
	re.Equal(fillRate, gtb.tokenSlots[1].fillRate)
	re.Equal(fillRate, gtb.tokenSlots[2].fillRate)
	re.InDelta(gtb.Tokens/2, gtb.tokenSlots[1].tokenCapacity, 1e-6)
	re.InDelta(gtb.Tokens/2, gtb.tokenSlots[2].tokenCapacity, 1e-6)
}

// Sanity matrix comparing master-like, 9887, and new-decay in typical scenes.
func TestBalanceScenarioMatrix(t *testing.T) {
	type scenario struct {
		name          string
		requireTokens []float64
		tokens        float64
		fillRate      float64
		warmupDecays  int
		burstForS1    float64
	}
	scenarios := []scenario{
		{name: "high_water_hotspot", requireTokens: []float64{900, 50, 50}, tokens: 1000, fillRate: 1000},
		{name: "low_water_skewed", requireTokens: []float64{900, 100, 100}, tokens: 200, fillRate: 1000},
		{name: "low_water_single_hotspot", requireTokens: []float64{1000, 1, 1}, tokens: 200, fillRate: 1000},
		{name: "recovery_after_suppression", requireTokens: []float64{0, 5000}, tokens: 200, fillRate: 1000, warmupDecays: 20, burstForS1: 5000},
	}

	masterFill := func(req []float64, fillRate float64) []float64 {
		n := float64(len(req))
		sum := 0.0
		for _, r := range req {
			sum += r
		}
		out := make([]float64, len(req))
		for i, r := range req {
			ratio := (1 - r/sum + 1/n) * (1 / n)
			out[i] = fillRate * ratio
		}
		return out
	}

	pr9887Fill := func(req []float64, fillRate float64) []float64 {
		n := float64(len(req))
		even := 1 / n
		sum := 0.0
		for _, r := range req {
			sum += r
		}
		out := make([]float64, len(req))
		for i, r := range req {
			share := r / sum
			ratio := even*(1-defaultConsumptionBiasWeight) + share*defaultConsumptionBiasWeight
			out[i] = fillRate * ratio
		}
		return out
	}

	newFill := func(sc scenario) []float64 {
		now := time.Now()
		gtb := &GroupTokenBucket{
			Settings: &rmpb.TokenLimitSettings{FillRate: uint64(sc.fillRate), BurstLimit: int64(sc.fillRate)},
			GroupTokenBucketState: GroupTokenBucketState{
				Tokens:             sc.tokens,
				resourceGroupName:  testResourceGroupName,
				tokenSlots:         make(map[uint64]*tokenSlot),
				overrideFillRate:   -1,
				overrideBurstLimit: -1,
				Initialized:        true,
			},
		}
		sum := 0.0
		for i, r := range sc.requireTokens {
			gtb.tokenSlots[uint64(i+1)] = &tokenSlot{requireTokensSum: r, lastReqTime: now}
			sum += r
		}
		gtb.clientConsumptionTokensSum = sum
		// apply warmup decays (no new demand) to simulate time passing
		for range sc.warmupDecays {
			gtb.balanceSlotTokens(now, 0, 0, sc.fillRate)
		}
		burstReq := sc.burstForS1
		if burstReq == 0 {
			burstReq = 1
		}
		gtb.balanceSlotTokens(now, 1, burstReq, sc.fillRate)
		out := make([]float64, len(sc.requireTokens))
		for i := range sc.requireTokens {
			out[i] = float64(gtb.tokenSlots[uint64(i+1)].fillRate)
		}
		return out
	}

	for _, sc := range scenarios {
		m := masterFill(sc.requireTokens, sc.fillRate)
		p := pr9887Fill(sc.requireTokens, sc.fillRate)
		nf := newFill(sc)
		t.Logf("[%s] master=%v pr9887=%v new=%v", sc.name, m, p, nf)
		if len(sc.requireTokens) > 1 && sc.requireTokens[0] >= sc.requireTokens[1] {
			require.GreaterOrEqual(t, nf[0], nf[1], "[%s] heavy should be >= light", sc.name)
		}
		if sc.warmupDecays > 0 && sc.burstForS1 > 0 {
			require.GreaterOrEqual(t, nf[0], p[0], "[%s] decay+bypass should recover faster or equal to pr9887", sc.name)
		}
	}
}

// Ensure hysteresis and bypass do not reset tokenCapacity.
func TestHighWaterHysteresisAndCapacity(t *testing.T) {
	re := require.New(t)
	now := time.Now()
	fillRate := uint64(1000)
	burst := int64(1000)
	gtb := &GroupTokenBucket{
		Settings: &rmpb.TokenLimitSettings{FillRate: fillRate, BurstLimit: burst},
		GroupTokenBucketState: GroupTokenBucketState{
			Tokens:             800, // > enter (0.7)
			resourceGroupName:  testResourceGroupName,
			tokenSlots:         make(map[uint64]*tokenSlot),
			overrideFillRate:   -1,
			overrideBurstLimit: -1,
			Initialized:        true,
		},
	}
	// Custom capacities to verify they are not reset in bypass.
	gtb.tokenSlots[1] = &tokenSlot{requireTokensSum: 100, tokenCapacity: 300, lastReqTime: now}
	gtb.tokenSlots[2] = &tokenSlot{requireTokensSum: 50, tokenCapacity: 100, lastReqTime: now}
	gtb.clientConsumptionTokensSum = 150

	// Enter bypass (high water), capacities only get incremental balance.
	gtb.balanceSlotTokens(now, 1, 1, 200)
	re.True(gtb.highWaterMode)
	re.InDelta(300+100, gtb.tokenSlots[1].tokenCapacity, 1e-6)
	re.InDelta(100+100, gtb.tokenSlots[2].tokenCapacity, 1e-6)
	re.Equal(fillRate, gtb.tokenSlots[1].fillRate)
	re.Equal(int64(fillRate), gtb.tokenSlots[1].burstLimit)

	// Drop water level below exit threshold, should leave bypass.
	gtb.Tokens = 300 // < 0.4 * burst
	gtb.balanceSlotTokens(now, 1, 1, 0)
	re.False(gtb.highWaterMode)
}
