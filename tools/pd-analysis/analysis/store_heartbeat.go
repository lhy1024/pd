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
	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/opts"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/movingaverage"
	"go.uber.org/zap"
	"regexp"
	"strconv"
)

type rateKind int

const (
	readByte rateKind = iota
	writeByte
	readKey
	writeKey
)

type idKind int

const (
	store idKind = iota
	region
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
	if _, ok := hb.stats[s.id]; !ok {
		hb.stats[s.id] = make([]*stat, 0, 0)
	}
	hb.stats[s.id] = append(hb.stats[s.id], s)
}

func (hb *heartbeats) get(id int) []*stat {
	if stats, ok := hb.stats[id]; ok {
		log.Info("add", zap.Int("len", len(stats)), zap.Int("id", id))
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

func (c *heartbeatCollector) ParseLog(filename, start, end, layout string, r *regexp.Regexp) ([]*charts.Line, error) {
	collectResult := func(content string) error {
		s, err := c.parseLine(content, r)
		if s != nil {
			c.hb.add(s)
		}
		return err
	}
	readLog(filename, start, end, layout, collectResult)
	id := 1
	stats := c.hb.get(id)

	line1, _ := c.draw(stats, []rateKind{readByte, writeByte})
	// line2, _ := c.draw(stats, writeByte)

	return []*charts.Line{line1}, nil

}

func (c *heartbeatCollector) draw(stats []*stat, kinds []rateKind) (*charts.Line, error) {
	line := charts.NewLine()
	line.SetGlobalOptions(
		charts.WithTitleOpts(opts.Title{
			Title: "line",
		}),
	)

	xAxis := make([]int, len(stats))
	xAxis[0] = stats[0].interval
	for i := 1; i < len(stats); i++ {
		xAxis[i] = xAxis[i-1] + stats[i].interval
	}
	l := line.SetXAxis(xAxis)
	for _, kind := range kinds {
		scoreData := make([]opts.LineData, 0, len(stats))
		for _, stat := range stats {
			scoreData = append(scoreData, opts.LineData{Value: stat.getRate(kind)})
		}
		l.AddSeries("", scoreData)
	}
	return line, nil
}

func (c *heartbeatCollector) parseLine(content string, r *regexp.Regexp) (*stat, error) {
	subStrings := r.FindStringSubmatch(content)
	switch len(subStrings) {
	case 0:
		return nil, nil
	case 7:
		s := &stat{}
		s.writeKeyRate, _ = strconv.Atoi(subStrings[1])
		s.readKeyRate, _ = strconv.Atoi(subStrings[2])
		s.writeByteRate, _ = strconv.Atoi(subStrings[3])
		s.readByteRate, _ = strconv.Atoi(subStrings[4])
		s.interval, _ = strconv.Atoi(subStrings[5])
		s.id, _ = strconv.Atoi(subStrings[6])
		return s, nil
	default:
		return nil, errors.New("Can't parse Log, with " + content)
	}
}
