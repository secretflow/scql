// Copyright 2023 Ant Group Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/stretchr/testify/require"

	"github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/scdb/auth"
	"github.com/secretflow/scql/pkg/scdb/config"
	"github.com/secretflow/scql/pkg/scdb/storage"
	"github.com/secretflow/scql/pkg/util/mock"
)

const testSpuRuntimeCfg = `{"protocol":"SEMI2K","field":"FM64"}`

func newTestApp(t *testing.T) *App {
	t.Helper()

	store, err := storage.NewMemoryStorage()
	require.NoError(t, err)
	require.NoError(t, mock.MockStorage(store))

	conf := config.NewDefaultConfig()
	conf.Protocol = "http"
	conf.Engine.SpuRuntimeCfg = testSpuRuntimeCfg
	conf.PartyAuth = config.PartyAuthConf{
		Method:               config.PartyAuthMethodNone,
		EnableTimestampCheck: false,
		ValidityPeriod:       0,
	}

	return &App{
		config:             conf,
		storage:            store,
		sessions:           cache.New(time.Hour, time.Hour),
		queryDoneChan:      make(chan string, defaultCallbackQueueSize),
		partyAuthenticator: auth.NewPartyAuthenticator(conf.PartyAuth),
	}
}

func newTestCredential(user, password string) *scql.SCDBCredential {
	return &scql.SCDBCredential{
		User: &scql.User{
			AccountSystemType: scql.User_NATIVE_USER,
			User: &scql.User_NativeUser_{
				NativeUser: &scql.User_NativeUser{
					Name:     user,
					Password: password,
				},
			},
		},
	}
}

func TestSubmitAndGetDryRun(t *testing.T) {
	r := require.New(t)
	app := newTestApp(t)

	alice := newTestCredential("alice", "alice123")

	// Test 1: Valid DQL query should succeed in dryRun mode
	t.Run("valid DQL dryRun", func(t *testing.T) {
		resp := submitAndGetWithDryRun(t, app, alice, "select column1_1 from test.table_1", true)
		r.NotNil(resp)
		r.Equal(int32(scql.Code_OK), resp.GetStatus().GetCode())
	})

	// Test 2: Invalid SQL should fail in dryRun mode
	t.Run("invalid DQL dryRun", func(t *testing.T) {
		resp := submitAndGetWithDryRun(t, app, alice, "select not_exist from test.table_1", true)
		r.Equal(int32(scql.Code_INTERNAL), resp.GetStatus().GetCode())
	})

	// Test 3: dryRun with async mode should fail (using Submit handler)
	t.Run("async dryRun unsupported", func(t *testing.T) {
		req := &scql.SCDBQueryRequest{
			User:   alice,
			Query:  "select column1_1 from test.table_1",
			DbName: "test",
			DryRun: true,
		}
		resp := app.submit(context.Background(), req)
		r.Equal(int32(scql.Code_BAD_REQUEST), resp.GetStatus().GetCode())
	})

	// Test 4: CCL violation should fail in dryRun mode (bob accessing alice column)
	t.Run("dryRun CCL violation", func(t *testing.T) {
		bob := newTestCredential("bob", "bob123")
		resp := submitAndGetWithDryRun(t, app, bob, "select tb.column2_1 from test.table_2 as tb", true)
		r.NotEqual(int32(scql.Code_OK), resp.GetStatus().GetCode())
	})
}

func submitAndGetWithDryRun(t *testing.T, app *App, cred *scql.SCDBCredential, query string, dryRun bool) *scql.SCDBQueryResultResponse {
	t.Helper()
	req := &scql.SCDBQueryRequest{
		User:   cred,
		Query:  query,
		DbName: "test",
		DryRun: dryRun,
	}
	resp := app.submitAndGet(context.Background(), req)
	if resp == nil {
		return &scql.SCDBQueryResultResponse{
			Status: &scql.Status{
				Code:    int32(scql.Code_INTERNAL),
				Message: fmt.Sprintf("nil response for query %s", query),
			},
		}
	}
	return resp
}
