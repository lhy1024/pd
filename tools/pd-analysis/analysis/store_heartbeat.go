// Copyright 2020 TiKV Project Authors.
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

package analysis

import (
	"time"

	"github.com/tikv/pd/pkg/movingaverage"
)

type storeStats struct {
	storeID       uint64
	readByteRate  float64
	writeByteRate float64
	readKeyRate   float64
	writeKeyRate  float64
	interval      uint64
}

type deltaWithInterval struct {
	delta    float64
	interval time.Duration
}

type heartbeatCollector struct {
	storesStats []storeStats
	mvs         []*movingaverage.MovingAvg
}

func NewHeartbeatCollector() *heartbeatCollector {
	return &heartbeatCollector{}
}

//func (c *heartbeatCollector) ParseLog(filename, start, end, layout string, r *regexp.Regexp) error {
//	collectResult := func(content string) error {
//		results, err := c.parseLine(content, r)
//		for _, result := range results {
//			log.Info("results", zap.Uint64("storeID", result.storeID))
//		}
//		return err
//	}
//	return readLog(filename, start, end, layout, collectResult)
//}
//
//func (c *heartbeatCollector) parseLine(content string, r *regexp.Regexp) ([]storeStats, error) {
//	results := make([]uint64, 0, 4)
//	subStrings := r.FindStringSubmatch(content)
//	if len(subStrings) == 0 {
//
//	} else {
//		return results, errors.New("Can't parse Log, with " + content)
//	}
//}
