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
	"flag"
	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/opts"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/movingaverage"
	"go.uber.org/zap"
	"regexp"
	"strconv"
)

var port = flag.String("p", ":8086", "serving addr")

type rateKind int

const (
	readByte rateKind = iota
	writeByte
	readKey
	writeKey
)

type stat struct {
	id            int
	readByteRate  int
	writeByteRate int
	readKeyRate   int
	writeKeyRate  int
	interval      int
}

func (s *stat) log() {
	log.Info("store stats",
		zap.Int("key-write", s.writeKeyRate),
		zap.Int("key-read", s.readKeyRate),
		zap.Int("byte-write", s.writeKeyRate),
		zap.Int("byte-read", s.readByteRate),
		zap.Int("interval", s.interval),
	)
}

func (s *stat) getRate(kind rateKind) int {
	switch kind {
	case readByte:
		return s.readByteRate
	case writeByte:
		return s.writeByteRate
	case readKey:
		return s.readKeyRate
	case writeKey:
		return s.writeKeyRate
	default:
		return 0.0
	}
}

type heartbeats struct {
	stats map[int][]*stat
}

func newHeartbeats() *heartbeats {
	return &heartbeats{
		stats: make(map[int][]*stat),
	}
}

func (hb *heartbeats) add(s *stat) {
	if _, ok := hb.stats[s.id]; ok {
		hb.stats[s.id] = make([]*stat, 0, 0)
	}
	hb.stats[s.id] = append(hb.stats[s.id], s)
}

func (hb *heartbeats) get(id int) []*stat {
	if stats, ok := hb.stats[id]; ok {
		return stats
	}
	return nil
}

type heartbeatCollector struct {
	hb  *heartbeats
	mvs []*movingaverage.MovingAvg
}

func NewHeartbeatCollector() *heartbeatCollector {
	return &heartbeatCollector{
		hb: newHeartbeats(),
	}
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

func (c *heartbeatCollector) ParseLog(filename, start, end, layout string, r *regexp.Regexp) (*charts.Line, error) {
	collectResult := func(content string) error {
		s, err := c.parseLine(content, r)
		if err == nil {
			c.hb.add(s)
		}
		return err
	}
	id := 1
	stats := c.hb.get(id)
	//if stats == nil {
	//	return errors.New("Can't get hbs, with id " + strconv.FormatInt(int64(id), 10))
	//}
	readLog(filename, start, end, layout, collectResult)

	return c.draw(stats, readByte)

}

func (c *heartbeatCollector) draw(stats []*stat, kind rateKind) (*charts.Line, error) {
	size := charts.NewLine()
	size.SetGlobalOptions(
		charts.WithTitleOpts(opts.Title{
			Title: "Size",
		}),
	)

	//xAxis := make([]int, len(hbs))
	//xAxis[0] = hbs[0].interval
	//for i := 1; i < len(hbs); i++ {
	//	xAxis[i] = xAxis[i-1] + hbs[i].interval
	//}

	xAxis := make([]float64, 0, 0)
	for _, stat := range stats {
		if stat.interval == 0 {
			continue
		}
		rate := float64(stat.getRate(kind)) / float64(stat.interval)
		for j := 0; j < stat.interval; j++ {
			xAxis = append(xAxis, rate)
		}
	}

	size.SetXAxis(xAxis)
	return size, nil
}

func (c *heartbeatCollector) parseLine(content string, r *regexp.Regexp) (*stat, error) {
	subStrings := r.FindStringSubmatch(content)
	if len(subStrings) == 7 {
		s := &stat{}
		s.writeKeyRate, _ = strconv.Atoi(subStrings[1])
		s.readKeyRate, _ = strconv.Atoi(subStrings[2])
		s.writeByteRate, _ = strconv.Atoi(subStrings[3])
		s.readByteRate, _ = strconv.Atoi(subStrings[4])
		s.interval, _ = strconv.Atoi(subStrings[5])
		s.id, _ = strconv.Atoi(subStrings[6])
		return s, nil
	}
	return nil, errors.New("Can't parse Log, with " + content)
}
