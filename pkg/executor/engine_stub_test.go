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

package executor

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/secretflow/scql/pkg/proto-gen/scql"
)

type TestBody struct {
	Name     string `json:"name"`
	ID       int    `json:"id"`
	FullName string `json:"full_name"`
}

func TestHttpClientPost(t *testing.T) {
	r := require.New(t)
	cli := NewEngineClient("HTTP", time.Second, nil, "application/json", "http")
	req := &scql.RunExecutionPlanRequest{
		Async:       false,
		CallbackUrl: "http://example.com/callback",
	}
	resp := &scql.RunExecutionPlanResponse{
		JobId:           "job_id_example",
		PartyCode:       "party_code_example",
		NumRowsAffected: 100,
	}

	respBody, err := protojson.Marshal(resp)
	r.NoError(err)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("Expected 'POST' request, got '%s'", r.Method)
		}
		var b scql.RunExecutionPlanRequest
		err := json.NewDecoder(r.Body).Decode(&b)
		if err != nil {
			t.Errorf("Error when unmarshaling json body: %v", err)
		}
		if b.Async != req.Async {
			t.Errorf("Expect %t got %t", req.Async, b.Async)
		}
		if b.CallbackUrl != req.CallbackUrl {
			t.Errorf("Expect %s got %s", req.CallbackUrl, b.CallbackUrl)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(respBody)
	}))
	defer ts.Close()

	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatalf("Error parsing URL: %v", err)
	}

	_, err = cli.RunExecutionPlan(u.Host, "credential", req)
	r.NoError(err)
}
