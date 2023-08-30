// Copyright 2023 TiKV Project Authors.
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

package etcdutil

import (
	"fmt"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/etcdserver/etcdserverpb"
)

// newTestSingleConfig is used to create a etcd config for the unit test purpose.
func newTestSingleConfig(t *testing.T) *embed.Config {
	cfg := embed.NewConfig()
	cfg.Name = genRandName()
	cfg.Dir = t.TempDir()
	cfg.WalDir = ""
	cfg.Logger = "zap"
	cfg.LogOutputs = []string{"stdout"}

	pu, _ := url.Parse(tempurl.Alloc())
	cfg.LPUrls = []url.URL{*pu}
	cfg.APUrls = cfg.LPUrls
	cu, _ := url.Parse(tempurl.Alloc())
	cfg.LCUrls = []url.URL{*cu}
	cfg.ACUrls = cfg.LCUrls

	cfg.StrictReconfigCheck = false
	cfg.InitialCluster = fmt.Sprintf("%s=%s", cfg.Name, &cfg.LPUrls[0])
	cfg.ClusterState = embed.ClusterStateFlagNew
	return cfg
}

func genRandName() string {
	return "test_etcd_" + strconv.FormatInt(time.Now().UnixNano()%10000, 10)
}

// NewTestEtcdCluster is used to create a etcd cluster for the unit test purpose.
func NewTestEtcdCluster(t *testing.T, count int) (servers []*embed.Etcd, etcdClient *clientv3.Client, clean func()) {
	re := require.New(t)
	servers = make([]*embed.Etcd, 0, count)

	cfg := newTestSingleConfig(t)
	etcd, err := embed.StartEtcd(cfg)
	re.NoError(err)
	etcdClient, err = CreateEtcdClient(nil, cfg.LCUrls)
	re.NoError(err)
	<-etcd.Server.ReadyNotify()
	servers = append(servers, etcd)

	for i := 1; i < count; i++ {
		// Check the client can get the new member.
		listResp, err := ListEtcdMembers(etcdClient)
		re.NoError(err)
		re.Len(listResp.Members, i)
		// Add a new member.
		etcd2 := MustAddEtcdMember(t, cfg, etcdClient)
		cfg2 := etcd2.Config()
		cfg = &cfg2
		<-etcd2.Server.ReadyNotify()
		servers = append(servers, etcd2)
	}

	checkMembers(re, etcdClient, servers)

	clean = func() {
		etcdClient.Close()
		for _, server := range servers {
			if server != nil {
				server.Close()
			}
		}
	}

	return
}

// MustAddEtcdMember is used to add a new etcd member to the cluster.
func MustAddEtcdMember(t *testing.T, cfg1 *embed.Config, client *clientv3.Client) *embed.Etcd {
	return addEtcdMemberWithRetry(t, cfg1, client, 3)
}

func addEtcdMemberWithRetry(t *testing.T, cfg1 *embed.Config, client *clientv3.Client, retry int) *embed.Etcd {
	re := require.New(t)
	cfg2 := newTestSingleConfig(t)
	cfg2.Name = genRandName()
	cfg2.InitialCluster = cfg1.InitialCluster + fmt.Sprintf(",%s=%s", cfg2.Name, &cfg2.LPUrls[0])
	cfg2.ClusterState = embed.ClusterStateFlagExisting
	peerURL := cfg2.LPUrls[0].String()
	addResp, err := AddEtcdMember(client, []string{peerURL})
	re.NoError(err)
	// Check the client can get the new member.
	members, err := ListEtcdMembers(client)
	re.NoError(err)
	re.Len(addResp.Members, len(members.Members))
	// Start the new etcd member.
	etcd2, err := embed.StartEtcd(cfg2)
	if err != nil {
		re.Contains(err.Error(), "error validating peerURLs")
		if retry > 0 {
			return addEtcdMemberWithRetry(t, cfg1, client, retry-1)
		}
	}
	re.NoError(err, "addEtcdMemberWithRetry failed after retry")
	re.Equal(uint64(etcd2.Server.ID()), addResp.Member.ID)
	<-etcd2.Server.ReadyNotify()
	return etcd2
}

func checkMembers(re *require.Assertions, client *clientv3.Client, etcds []*embed.Etcd) {
	// Check the client can get the new member.
	listResp, err := ListEtcdMembers(client)
	re.NoError(err)
	re.Len(listResp.Members, len(etcds))
	inList := func(m *etcdserverpb.Member) bool {
		for _, etcd := range etcds {
			if m.ID == uint64(etcd.Server.ID()) {
				return true
			}
		}
		return false
	}
	for _, m := range listResp.Members {
		re.True(inList(m))
	}
}
