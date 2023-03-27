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
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type TestBody struct {
	Name     string `json:"name"`
	ID       int    `json:"id"`
	FullName string `json:"full_name"`
}

func TestHttpClientPost(t *testing.T) {
	r := require.New(t)
	cli := NewEngineClient(time.Second)
	body := TestBody{
		Name:     "name",
		ID:       123,
		FullName: "full_name",
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("Expected 'POST' request, got '%s'", r.Method)
		}
		var b TestBody
		err := json.NewDecoder(r.Body).Decode(&b)
		if err != nil {
			t.Errorf("Error when unmarshaling json body: %v", err)
		}
		if b.Name != body.Name {
			t.Errorf("Expect %s got %s", body.Name, b.Name)
		}
		if b.ID != body.ID {
			t.Errorf("Expect %d got %d", body.ID, b.ID)
		}
		if b.FullName != body.FullName {
			t.Errorf("Expect %s got %s", body.FullName, b.FullName)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()
	ctx := context.Background()
	b, err := json.Marshal(body)
	r.NoError(err)
	_, err = cli.Post(ctx, ts.URL, "credential", "application/json", string(b))
	r.NoError(err)
}
