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

package client

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/util/message"
)

func toJson(t *testing.T, pb proto.Message) string {
	content, err := message.SerializeTo(pb, message.EncodingTypeJson)
	assert.NoError(t, err)
	return content
}

func TestClient(t *testing.T) {
	a := require.New(t)
	// mock http client
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockhttpClient := NewMockhttpClientI(ctrl)

	serverHost := "127.0.0.1"
	maxFetchNum := 3
	client := NewClient(serverHost, mockhttpClient, maxFetchNum, time.Millisecond)

	// exceeds max fetch number
	{
		for i := 0; i < maxFetchNum+1; i++ {
			unavailableResp := &scql.SCDBQueryResultResponse{
				Status:        &scql.Status{Code: int32(scql.Code_NOT_READY)},
				ScdbSessionId: "some_id",
			}
			unavailableRespRecord := &httptest.ResponseRecorder{
				Code: http.StatusOK,
				Body: bytes.NewBuffer([]byte(toJson(t, unavailableResp))),
			}
			mockhttpClient.EXPECT().Post(serverHost+FetchPath, "application/json", gomock.Any()).
				Return(unavailableRespRecord.Result(), nil)
		}
		_, err := client.Fetch(nil, "some_id")
		a.Error(err)
	}

	// submit
	{
		expectResp := &scql.SCDBSubmitResponse{
			Status:        &scql.Status{},
			ScdbSessionId: "some_id",
		}
		respRecord := &httptest.ResponseRecorder{
			Code: http.StatusOK,
			Body: bytes.NewBuffer([]byte(toJson(t, expectResp))),
		}
		mockhttpClient.EXPECT().Post(serverHost+SubmitPath, "application/json", gomock.Any()).
			Return(respRecord.Result(), nil).Times(1)
		actualResp, err := client.Submit(nil, "some_sql")
		a.NoError(err)
		a.Equal(expectResp, actualResp)
	}

	// fetch
	{
		unavailableResp := &scql.SCDBQueryResultResponse{
			Status:        &scql.Status{Code: int32(scql.Code_NOT_READY)},
			ScdbSessionId: "some_id",
		}
		unavailableRespRecord := &httptest.ResponseRecorder{
			Code: http.StatusOK,
			Body: bytes.NewBuffer([]byte(toJson(t, unavailableResp))),
		}
		mockhttpClient.EXPECT().Post(serverHost+FetchPath, "application/json", gomock.Any()).
			Return(unavailableRespRecord.Result(), nil).Times(1)
		expectResp := &scql.SCDBQueryResultResponse{
			Status:        &scql.Status{},
			ScdbSessionId: "some_id",
		}
		respRecord := &httptest.ResponseRecorder{
			Code: http.StatusOK,
			Body: bytes.NewBuffer([]byte(toJson(t, expectResp))),
		}
		mockhttpClient.EXPECT().Post(serverHost+FetchPath, "application/json", gomock.Any()).
			Return(respRecord.Result(), nil).Times(1)
		actualResp, err := client.Fetch(nil, "some_id")
		a.NoError(err)
		a.Equal(expectResp, actualResp)
	}
}
