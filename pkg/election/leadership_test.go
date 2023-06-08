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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package election

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
)

const defaultLeaseTimeout = 1

func TestLeadership(t *testing.T) {
	re := require.New(t)
	cfg := etcdutil.NewTestSingleConfig(t)
	etcd, err := embed.StartEtcd(cfg)
	defer func() {
		etcd.Close()
	}()
	re.NoError(err)

	ep := cfg.LCUrls[0].String()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep},
	})
	re.NoError(err)

	<-etcd.Server.ReadyNotify()

	// Campaign the same leadership
	leadership1 := NewLeadership(client, "/test_leader", "test_leader_1")
	leadership2 := NewLeadership(client, "/test_leader", "test_leader_2")

	// leadership1 starts first and get the leadership
	err = leadership1.Campaign(defaultLeaseTimeout, "test_leader_1")
	re.NoError(err)
	// leadership2 starts then and can not get the leadership
	err = leadership2.Campaign(defaultLeaseTimeout, "test_leader_2")
	re.Error(err)

	re.True(leadership1.Check())
	// leadership2 failed, so the check should return false
	re.False(leadership2.Check())

	// Sleep longer than the defaultLeaseTimeout to wait for the lease expires
	time.Sleep((defaultLeaseTimeout + 1) * time.Second)

	re.False(leadership1.Check())
	re.False(leadership2.Check())

	// Delete the leader key and campaign for leadership1
	err = leadership1.DeleteLeaderKey()
	re.NoError(err)
	err = leadership1.Campaign(defaultLeaseTimeout, "test_leader_1")
	re.NoError(err)
	re.True(leadership1.Check())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leadership1.Keep(ctx)

	// Sleep longer than the defaultLeaseTimeout
	time.Sleep((defaultLeaseTimeout + 1) * time.Second)

	re.True(leadership1.Check())
	re.False(leadership2.Check())

	// Delete the leader key and re-campaign for leadership2
	err = leadership1.DeleteLeaderKey()
	re.NoError(err)
	err = leadership2.Campaign(defaultLeaseTimeout, "test_leader_2")
	re.NoError(err)
	re.True(leadership2.Check())
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	go leadership2.Keep(ctx)

	// Sleep longer than the defaultLeaseTimeout
	time.Sleep((defaultLeaseTimeout + 1) * time.Second)

	re.False(leadership1.Check())
	re.True(leadership2.Check())

	// Test resetting the leadership.
	leadership1.Reset()
	leadership2.Reset()
	re.False(leadership1.Check())
	re.False(leadership2.Check())

	// Try to keep the reset leadership.
	leadership1.Keep(ctx)
	leadership2.Keep(ctx)

	// Check the lease.
	lease1 := leadership1.getLease()
	re.NotNil(lease1)
	lease2 := leadership2.getLease()
	re.NotNil(lease2)

	re.True(lease1.IsExpired())
	re.True(lease2.IsExpired())
	re.NoError(lease1.Close())
	re.NoError(lease2.Close())
}

func TestRestartEtcd(t *testing.T) {
	re := require.New(t)
	cfg := etcdutil.NewTestSingleConfig(t)
	etcd0, err := embed.StartEtcd(cfg)
	defer func() {
		etcd0.Close()
	}()
	re.NoError(err)
	<-etcd0.Server.ReadyNotify()

	ep := cfg.LCUrls[0].String()
	client0, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{ep},
		DialTimeout: 1 * time.Second,
	})
	re.NoError(err)

	etcd1 := checkAddEtcdMember(t, cfg, client0)
	defer func() {
		etcd1.Close()
	}()
	cfg1 := etcd1.Config()
	<-etcd1.Server.ReadyNotify()

	etcd2 := checkAddEtcdMember(t, &cfg1, client0)
	cfg2 := etcd2.Config()
	<-etcd2.Server.ReadyNotify()

	ep1 := cfg1.LCUrls[0].String()
	client1, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{ep1},
		DialTimeout: 1 * time.Second,
	})
	re.NoError(err)

	ep2 := cfg2.LCUrls[0].String()
	client2, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{ep2},
		DialTimeout: 1 * time.Second,
	})
	re.NoError(err)

	// Campaign the same leadership
	leadership1 := NewLeadership(client1, "/test_leader", "test_leader_1")
	err = leadership1.Campaign(3, "test_leader_1")
	re.NoError(err)
	re.True(leadership1.Check())
	exitCh := make(chan struct{})
	go func() {
		leadership2 := NewLeadership(client2, "/test_leader", "test_leader_2")
		leadership2.Watch(context.Background(), 0)
		exitCh <- struct{}{}
	}()
	go func() {
		leadership2 := NewLeadership(client0, "/test_leader", "test_leader_0")
		leadership2.Watch(context.Background(), 0)
	}()

	time.Sleep(1 * time.Second)
	etcd2.Close()
	time.Sleep(10 * time.Second)
	etcd1.Close()
	time.Sleep(10 * time.Second)
	log.Info("close etcd1 and etcd2")
	etcd2, err = embed.StartEtcd(&cfg2)
	re.NoError(err)
	<-etcd2.Server.ReadyNotify()
	defer func() {
		etcd2.Close()
	}()
	log.Info("restart etcd2")
	time.Sleep(30 * time.Second)
	testutil.Eventually(re, func() bool {
		<-exitCh
		return true
	}, testutil.WithWaitFor(10*time.Second))
}

func checkAddEtcdMember(t *testing.T, cfg1 *embed.Config, client *clientv3.Client) *embed.Etcd {
	re := require.New(t)
	cfg2 := etcdutil.NewTestSingleConfig(t)
	cfg2.Name = genRandName()
	cfg2.InitialCluster = cfg1.InitialCluster + fmt.Sprintf(",%s=%s", cfg2.Name, &cfg2.LPUrls[0])
	cfg2.ClusterState = embed.ClusterStateFlagExisting
	peerURL := cfg2.LPUrls[0].String()
	addResp, err := etcdutil.AddEtcdMember(client, []string{peerURL})
	re.NoError(err)
	etcd2, err := embed.StartEtcd(cfg2)
	re.NoError(err)
	re.Equal(uint64(etcd2.Server.ID()), addResp.Member.ID)
	<-etcd2.Server.ReadyNotify()
	return etcd2
}

func genRandName() string {
	return "test_etcd_" + strconv.FormatInt(time.Now().UnixNano()%10000, 10)
}
