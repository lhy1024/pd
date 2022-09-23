// Copyright 2017 TiKV Project Authors.
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

package simulator

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/tools/pd-simulator/simulator/cases"
	"github.com/tikv/pd/tools/pd-simulator/simulator/info"
	"github.com/tikv/pd/tools/pd-simulator/simulator/simutil"
	"go.uber.org/zap"
)

const (
	balanceRegionScheduler = "balance-region-scheduler"
	balanceLeaderScheduler = "balance-leader-scheduler"
)

// Driver promotes the cluster status change.
type Driver struct {
	wg          sync.WaitGroup
	pdAddr      string
	simCase     *cases.Case
	client      Client
	tickCount   int64
	eventRunner *EventRunner
	raftEngine  *RaftEngine
	conn        *Connection
	simConfig   *SimConfig
	pdConfig    *PDConfig
}

// NewDriver returns a driver.
func NewDriver(pdAddr string, caseName string, simConfig *SimConfig) (*Driver, error) {
	simCase := cases.NewCase(caseName)
	if simCase == nil {
		return nil, errors.Errorf("failed to create case %s", caseName)
	}
	pdConfig := &PDConfig{}
	pdConfig.PlacementRules = simCase.Rules
	pdConfig.LocationLabels = simCase.Labels
	return &Driver{
		pdAddr:    pdAddr,
		simCase:   simCase,
		simConfig: simConfig,
		pdConfig:  pdConfig,
	}, nil
}

// Prepare initializes cluster information, bootstraps cluster and starts nodes.
func (d *Driver) Prepare() error {
	conn, err := NewConnection(d.simCase, d.pdAddr, d.simConfig)
	if err != nil {
		return err
	}
	d.conn = conn

	d.raftEngine = NewRaftEngine(d.simCase, d.conn, d.simConfig)
	d.eventRunner = NewEventRunner(d.simCase.Events, d.raftEngine)

	// Bootstrap.
	store, region, err := d.GetBootstrapInfo(d.raftEngine)
	if err != nil {
		return err
	}
	d.client = d.conn.Nodes[store.GetId()].client
	d.raftEngine.client = d.client

	ctx, cancel := context.WithTimeout(context.Background(), pdTimeout)
	err = d.client.Bootstrap(ctx, store, region)
	cancel()
	if err != nil {
		simutil.Logger.Fatal("bootstrap error", zap.Error(err))
	} else {
		simutil.Logger.Debug("bootstrap success")
	}

	// Setup alloc id.
	start := time.Now()
	maxID := cases.IDAllocator.GetID()
	for {
		var id uint64
		id, err = d.client.AllocID(context.Background())
		if err != nil {
			return errors.WithStack(err)
		}
		if id > maxID {
			cases.IDAllocator.ResetID()
			log.Info("finish to alloc id", zap.Uint64("max id", maxID))
			break
		}
		if cost := time.Since(start); cost > 10*time.Second {
			log.Fatal("alloc id too slow", zap.Duration("duration", cost))
		}
	}

	err = d.Start()
	if err != nil {
		return err
	}
	d.raftEngine.startTime = time.Now()
	return nil
}

// Tick invokes nodes' Tick.
func (d *Driver) Tick() {
	d.tickCount++
	d.raftEngine.stepRegions()
	// simulator don't need any schedulers util all stores send their heartbeat.
	if d.tickCount <= 1 {
		d.client.RemoveScheduler(balanceRegionScheduler)
	} else if d.tickCount == storeHeartBeatPeriod {
		d.client.AddScheduler(balanceRegionScheduler, nil)
	}
	d.eventRunner.Tick(d.tickCount)
	for _, n := range d.conn.Nodes {
		n.reportRegionChange()
		d.wg.Add(1)
		go n.Tick(&d.wg)
	}
	d.wg.Wait()
}

// Check checks if the simulation is completed.
func (d *Driver) Check() bool {
	length := uint64(len(d.conn.Nodes) + 1)
	for index := range d.conn.Nodes {
		if index >= length {
			length = index + 1
		}
	}
	stats := make([]info.StoreStats, length)
	for index, node := range d.conn.Nodes {
		stats[index] = *node.stats
	}
	return d.simCase.Checker(d.raftEngine.regionsInfo, stats)
}

// PrintStatistics prints the statistics of the scheduler.
func (d *Driver) PrintStatistics() {
	d.raftEngine.schedulerStats.PrintStatistics()
}

// Start starts all nodes.
func (d *Driver) Start() error {
	for _, n := range d.conn.Nodes {
		err := n.Start()
		if err != nil {
			return err
		}
	}
	d.ChangePDConfig()
	return nil
}

// ChangePDConfig changes pd config
func (d *Driver) ChangePDConfig() error {
	d.client.PutPDConfig(d.pdConfig)
	return nil
}

// Stop stops all nodes.
func (d *Driver) Stop() {
	for _, n := range d.conn.Nodes {
		n.Stop()
	}
}

// TickCount returns the simulation's tick count.
func (d *Driver) TickCount() int64 {
	return d.tickCount
}

// GetBootstrapInfo returns a valid bootstrap store and region.
func (d *Driver) GetBootstrapInfo(r *RaftEngine) (*metapb.Store, *metapb.Region, error) {
	origin := r.BootstrapRegion()
	if origin == nil {
		return nil, nil, errors.New("no region found for bootstrap")
	}
	region := origin.Clone(
		core.WithStartKey([]byte("")),
		core.WithEndKey([]byte("")),
		core.SetRegionConfVer(1),
		core.SetRegionVersion(1),
		core.SetPeers([]*metapb.Peer{origin.GetLeader()}),
	)
	if region.GetLeader() == nil {
		return nil, nil, errors.New("bootstrap region has no leader")
	}
	store := d.conn.Nodes[region.GetLeader().GetStoreId()]
	if store == nil {
		return nil, nil, errors.Errorf("bootstrap store %v not found", region.GetLeader().GetStoreId())
	}
	return store.Store, region.GetMeta(), nil
}
