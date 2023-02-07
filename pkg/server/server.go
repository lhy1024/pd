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

package server

import (
	"context"

	"github.com/tikv/pd/pkg/member"
	"go.etcd.io/etcd/clientv3"
)

// Server is the interface for the server.
type Server interface {
	Context() context.Context
	AddStartCallback(callbacks ...func())
	Name() string
	GetClient() *clientv3.Client
	GetMember() *member.Member
	AddLeaderCallback(callbacks ...func(context.Context))
	Run() error
	Close()
}
