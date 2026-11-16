// Copyright 2026 TiKV Project Authors.
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

package affinity

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage"
)

func TestCollectGroupStatesByQuery(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()
	store := storage.NewStorageWithMemoryBackend()
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)
	manager, err := NewManager(ctx, store, core.NewStoresInfo(), mockconfig.NewTestOptions(), regionLabeler)
	re.NoError(err)
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "p-1"}, {GroupID: "p-10"}, {GroupID: "q-1"}}))

	keys := func(states map[string]*GroupState) []string {
		ids := make([]string, 0, len(states))
		for id := range states {
			ids = append(ids, id)
		}
		slices.Sort(ids)
		return ids
	}
	for _, tc := range []struct {
		ids    []string
		prefix string
		want   []string
	}{
		{nil, "", []string{"p-1", "p-10", "q-1"}},
		{nil, "p-", []string{"p-1", "p-10"}},
		{nil, "p-1", []string{"p-1", "p-10"}},
		{nil, "r", []string{}},
		{[]string{"p-10", "q-1", "missing"}, "p-", []string{"p-10"}},
		{[]string{"q-1"}, "", []string{"q-1"}},
	} {
		states, err := CollectGroupStatesByQuery(manager, tc.ids, tc.prefix)
		re.NoError(err)
		re.Equal(tc.want, keys(states), "%v %q", tc.ids, tc.prefix)
	}
	for _, prefix := range []string{"bad prefix", strings.Repeat("a", 65)} {
		_, err := CollectGroupStatesByQuery(manager, nil, prefix)
		re.ErrorIs(err, errs.ErrAffinityGroupContent)
	}
}
