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
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"regexp"
	"strconv"
	"time"

	"github.com/tikv/pd/pkg/movingaverage"
)

type storeStats struct {
	storeID       int
	readByteRate  int
	writeByteRate int
	readKeyRate   int
	writeKeyRate  int
	interval      int
}

func (s *storeStats) log() {
	if s == nil {
		log.Info("store stats is nil")
		return
	}
	log.Info("store stats",
		zap.Int("key-write", s.writeKeyRate),
		zap.Int("key-read", s.readKeyRate),
		zap.Int("byte-write", s.writeKeyRate),
		zap.Int("byte-read", s.writeKeyRate),
		zap.Int("interval", s.writeKeyRate),
		zap.Int("store-id", s.writeKeyRate),
	)
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

// CompileRegex is to provide regexp for heartbeatCollector.
func (c *heartbeatCollector) CompileRegex() (*regexp.Regexp, error) {
	typs := []string{
		"key-write",
		"key-read",
		"byte-write",
		"byte-read",
		"interval",
		"store-id",
	}
	r := ".*?update store stats.*?"
	for _, typ := range typs {
		r += typ + "=([0-9]*).*?"
	}
	return regexp.Compile(r)
}

func (c *heartbeatCollector) ParseLog(filename, start, end, layout string, r *regexp.Regexp) error {
	collectResult := func(content string) error {
		s, err := c.parseLine(content, r)
		s.log()
		return err
	}
	return readLog(filename, start, end, layout, collectResult)
}

func (c *heartbeatCollector) parseLine(content string, r *regexp.Regexp) (*storeStats, error) {
	subStrings := r.FindStringSubmatch(content)
	if len(subStrings) == 6 {
		s := &storeStats{}
		s.writeKeyRate, _ = strconv.Atoi(subStrings[0])
		s.readKeyRate, _ = strconv.Atoi(subStrings[1])
		s.writeByteRate, _ = strconv.Atoi(subStrings[2])
		s.readByteRate, _ = strconv.Atoi(subStrings[3])
		s.interval, _ = strconv.Atoi(subStrings[4])
		s.storeID, _ = strconv.Atoi(subStrings[5])
		return s, nil
	}
	return nil, errors.New("Can't parse Log, with " + content)
}
