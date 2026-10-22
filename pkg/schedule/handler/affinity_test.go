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

package handler

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(testutil.WaitForEtcdConnections(m), testutil.LeakOptions...)
}

// lostAffinityResponseStorage commits a write but loses its response, then
// fails the first authoritative reload. Subsequent requests use normal storage.
type lostAffinityResponseStorage struct {
	storage.Storage
	failures int
}

func (s *lostAffinityResponseStorage) RunInTxn(ctx context.Context, f func(kv.Txn) error) error {
	if s.failures == 1 {
		s.failures--
		return errors.New("authoritative reload failed")
	}
	if err := s.Storage.RunInTxn(ctx, f); err != nil {
		return err
	}
	if s.failures == 2 {
		s.failures--
		return errors.New("commit response lost")
	}
	return nil
}

func TestAffinityHTTPWritesRefreshBeforeCheckingExistence(t *testing.T) {
	for _, operation := range []string{"recreate", "modify"} {
		t.Run(operation, func(t *testing.T) {
			store := &lostAffinityResponseStorage{Storage: storage.NewStorageWithMemoryBackend()}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second)
			require.NoError(t, err)
			manager, err := affinity.NewManager(ctx, store, core.NewStoresInfo(), mockconfig.NewTestOptions(), regionLabeler)
			require.NoError(t, err)
			h := AffinityHandler{GetManager: func(*gin.Context) (*affinity.Manager, bool) { return manager, true }}
			router := gin.New()
			router.POST("/", h.PostAffinityGroups)
			router.PATCH("/", h.BatchModifyAffinityGroups)
			request := func(method, body string) *httptest.ResponseRecorder {
				t.Helper()
				response := httptest.NewRecorder()
				req := httptest.NewRequest(method, "/", bytes.NewBufferString(body))
				req.Header.Set("Content-Type", "application/json")
				router.ServeHTTP(response, req)
				return response
			}
			const create = `{"affinity_groups":{"custom":{"ranges":[{"start_key":"AQ==","end_key":"Ag=="}]}}}`
			if operation == "recreate" {
				require.Equal(t, http.StatusOK, request(http.MethodPost, create).Code)
				store.failures = 2
				// Use the Manager delete entry to model Classic's failed delete
				// response while retaining its stale local group.
				require.Error(t, manager.DeleteAffinityGroups([]string{"custom"}, true))
				require.True(t, manager.IsGroupExist("custom"))
				response := request(http.MethodPost, create)
				require.Equal(t, http.StatusOK, response.Code, response.Body.String())
				require.Equal(t, http.StatusConflict, request(http.MethodPost, create).Code)
			} else {
				store.failures = 2
				require.Equal(t, http.StatusInternalServerError, request(http.MethodPost, create).Code)
				require.False(t, manager.IsGroupExist("custom"))
				response := request(http.MethodPatch, `{"add":[{"id":"custom","ranges":[{"start_key":"Ag==","end_key":"Aw=="}]}]}`)
				require.Equal(t, http.StatusOK, response.Code, response.Body.String())
				require.Equal(t, 2, manager.GetAffinityGroupState("custom").RangeCount)
				require.Equal(t, http.StatusNotFound, request(http.MethodPatch, `{"add":[{"id":"missing","ranges":[{"start_key":"Aw==","end_key":"BA=="}]}]}`).Code)
			}
		})
	}
}
