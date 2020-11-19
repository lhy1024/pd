// Copyright 2019 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package statistics

import (
	"time"

	"github.com/phf/go-queue/queue"
)

const (
	// StoreHeartBeatReportInterval is the heartbeat report interval of a store.
	StoreHeartBeatReportInterval = 10
)

type deltaWithInterval struct {
	delta    float64
	interval time.Duration
}

// AvgOverTime maintains change rate in the last avgInterval.
//
// AvgOverTime takes changes with their own intervals,
// stores recent changes that happened in the last avgInterval,
// then calculates the change rate by (sum of changes) / (sum of intervals).
type AvgOverTime struct {
	que         *queue.Queue
	deltaSum    float64
	intervalSum time.Duration
	avgInterval time.Duration
}

// NewAvgOverTime returns an AvgOverTime with given interval.
func NewAvgOverTime(interval time.Duration) *AvgOverTime {
	return &AvgOverTime{
		que:         queue.New(),
		deltaSum:    0,
		intervalSum: 0,
		avgInterval: interval,
	}
}

// Get returns change rate in the last interval.
func (aot *AvgOverTime) Get() float64 {
	return aot.deltaSum / aot.intervalSum.Seconds()
}

// Clear clears the AvgOverTime.
func (aot *AvgOverTime) Clear() {
	aot.que = queue.New()
	aot.intervalSum = 0
	aot.deltaSum = 0
}

// Add adds recent change to AvgOverTime.
func (aot *AvgOverTime) Add(delta float64, interval time.Duration) {
	aot.que.PushBack(deltaWithInterval{delta, interval})
	aot.deltaSum += delta
	aot.intervalSum += interval
	if in
}

// Set sets AvgOverTime to the given average.
func (aot *AvgOverTime) Set(avg float64) {
	aot.Clear()
	aot.deltaSum = avg * aot.avgInterval.Seconds()
	aot.intervalSum = aot.avgInterval
	aot.que.PushBack(deltaWithInterval{delta: aot.deltaSum, interval: aot.intervalSum})
}

// TimeMedian is AvgOverTime + MedianFilter
// Size of MedianFilter should be larger than double size of AvgOverTime to denoisy.
// Delay is aotSize * mfSize * StoreHeartBeatReportInterval/2
// and the min filled period is aotSize * StoreHeartBeatReportInterval, which is not related with mfSize
type TimeMedian struct {
	aotInterval    time.Duration
	reportInterval time.Duration
	aot            *AvgOverTime
	mf             *MedianFilter
	aotSize        int
	mfSize         int
	instantaneous  float64
	average_num    float64
	aotIsFull      bool
}

// NewTimeMedian returns a TimeMedian with given size.
func NewTimeMedian(aotSize, mfSize, reportInterval int) *TimeMedian {
	interval := time.Duration(aotSize*reportInterval) * time.Second
	return &TimeMedian{
		aotInterval:    interval,
		reportInterval: time.Duration(reportInterval) * time.Second,
		aot:            NewAvgOverTime(interval),
		mf:             NewMedianFilter(mfSize),
		aotSize:        aotSize,
		mfSize:         mfSize,
		instantaneous:  0.0,
	}
}

// Get returns change rate in the median of the several intervals.
func (t *TimeMedian) Get() float64 {
	return t.mf.Get()
}

// Add adds recent change to TimeMedian.
func (t *TimeMedian) Add(delta float64, interval time.Duration) {
	t.instantaneous = delta / interval.Seconds()
	t.aot.Add(delta, interval)
	t.aotIsFull = false

	if t.aot.intervalSum >= t.aotInterval {
		t.average_num = t.aot.Get()
		t.aotIsFull = true
		t.mf.Add(t.average_num)
		t.aot.Clear()
		return
	}
}

// Set sets the given average.
func (t *TimeMedian) Set(avg float64) {
	t.mf.Set(avg)
	t.instantaneous = avg
}

// GetFilledPeriod returns filled period.
func (t *TimeMedian) GetFilledPeriod() int { // it is unrelated with mfSize
	return t.aotSize
}

// GetInstantaneous returns the returns a instantaneous speed
func (t *TimeMedian) GetInstantaneous() float64 {
	return t.instantaneous
}

func (t *TimeMedian) IsAotFull() bool {
	return t.aotIsFull
}

func (t *TimeMedian) GetAverageNum() float64 {
	return t.average_num
}
