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

package stdgrm

import (
	"bytes"
	"fmt"
	"net/http/httptest"
	"testing"

	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/secretflow/scql/pkg/executor"
	"github.com/secretflow/scql/pkg/grm"
	grmproto "github.com/secretflow/scql/pkg/proto-gen/grm"
	"github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/util/message"
)

func toJson(t *testing.T, pb proto.Message) string {
	content, err := message.SerializeTo(pb, message.EncodingTypeJson)
	assert.NoError(t, err)
	return content
}

func TestGetTableSchema(t *testing.T) {
	r := require.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := executor.NewMockSimpleHttpClient(ctrl)
	client := New(m, "")
	httpresp404 := httptest.ResponseRecorder{
		Code: 404,
		Body: bytes.NewBuffer([]byte("")),
	}
	m.EXPECT().Do(gomock.Any()).Return(httpresp404.Result(), nil)
	_, err := client.GetTableMeta("fake id", "", "fake token")
	r.NotNil(err)
	m.EXPECT().Do(gomock.Any()).Return(nil, fmt.Errorf("%v", "timeout"))
	_, err = client.GetTableMeta("fake id", "", "fake token")
	r.NotNil(err)
	correctResponse := &grmproto.GetTableMetaResponse{
		Status: &scql.Status{
			Code:    int32(scql.Code_OK),
			Message: "",
		},
		Schema: &grmproto.TableSchema{
			DbName:    "d1",
			TableName: "t1",
			Columns: []*grmproto.ColumnDesc{
				&grmproto.ColumnDesc{
					Name:        "c1",
					Type:        "long",
					Description: "dddd",
				},
			},
		},
		DbType: grm.DBMySQL,
	}
	httpresp200 := httptest.ResponseRecorder{
		Code: 200,
		Body: bytes.NewBuffer([]byte(toJson(t, correctResponse))),
	}
	m.EXPECT().Do(gomock.Any()).Return(httpresp200.Result(), nil)
	res, err := client.GetTableMeta("fake id", "", "fake token")
	r.Nil(err)
	r.Equal("d1", res.DbName)
	r.Equal("t1", res.TableName)
	r.Equal(1, len(res.Columns))
	r.Equal("c1", res.Columns[0].Name)
	r.Equal("long", res.Columns[0].Type)
	r.Equal(grm.DBMySQL, res.DBType)

	NilDBTypeResponse := &grmproto.GetTableMetaResponse{
		Status: &scql.Status{
			Code:    int32(scql.Code_OK),
			Message: "",
		},
		Schema: &grmproto.TableSchema{
			DbName:    "d1",
			TableName: "t1",
			Columns: []*grmproto.ColumnDesc{
				&grmproto.ColumnDesc{
					Name:        "c1",
					Type:        "long",
					Description: "dddd",
				},
			},
		},
	}
	httpNilDBType := httptest.ResponseRecorder{
		Code: 200,
		Body: bytes.NewBuffer([]byte(toJson(t, NilDBTypeResponse))),
	}
	m.EXPECT().Do(gomock.Any()).Return(httpNilDBType.Result(), nil)
	res, err = client.GetTableMeta("fake id", "", "fake token")
	r.Nil(err)
	r.Equal(grm.DBUnknown, res.DBType)

}

func TestGetEngines(t *testing.T) {
	r := require.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := executor.NewMockSimpleHttpClient(ctrl)
	client := New(m, "")
	httpresp404 := httptest.ResponseRecorder{
		Code: 404,
		Body: bytes.NewBuffer([]byte("")),
	}
	m.EXPECT().Do(gomock.Any()).Return(httpresp404.Result(), nil)
	_, err := client.GetEngines([]string{"1", "2"}, "fake token")
	r.NotNil(err)
	m.EXPECT().Do(gomock.Any()).Return(nil, fmt.Errorf("%v", "timeout"))
	_, err = client.GetEngines([]string{"1", "2"}, "fake token")
	r.NotNil(err)
	correctResponse := &grmproto.GetEnginesResponse{
		Status: &scql.Status{
			Code:    int32(scql.Code_OK),
			Message: "",
		},
		EngineInfos: []*grmproto.GetEnginesResponse_EngineInfo{
			&grmproto.GetEnginesResponse_EngineInfo{
				Endpoints: []string{"1.com"},
			},
			&grmproto.GetEnginesResponse_EngineInfo{
				Endpoints: []string{"2.com"},
			},
		},
	}
	httpresp200 := httptest.ResponseRecorder{
		Code: 200,
		Body: bytes.NewBuffer([]byte(toJson(t, correctResponse))),
	}
	m.EXPECT().Do(gomock.Any()).Return(httpresp200.Result(), nil)
	res, err := client.GetEngines([]string{"1", "2"}, "fake token")
	r.Nil(err)
	r.Equal(2, len(res))
	r.Equal(1, len(res[0].Endpoints))
	r.Equal("1.com", res[0].Endpoints[0])
	r.Equal(1, len(res[1].Endpoints))
	r.Equal("2.com", res[1].Endpoints[0])
}

func TestVerifyTableOwnership(t *testing.T) {
	r := require.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := executor.NewMockSimpleHttpClient(ctrl)
	client := New(m, "")
	httpresp404 := httptest.ResponseRecorder{
		Code: 404,
		Body: bytes.NewBuffer([]byte("")),
	}
	m.EXPECT().Do(gomock.Any()).Return(httpresp404.Result(), nil)
	_, err := client.VerifyTableOwnership("tid", "fake token")
	r.NotNil(err)
	m.EXPECT().Do(gomock.Any()).Return(nil, fmt.Errorf("%v", "timeout"))
	_, err = client.VerifyTableOwnership("tid", "fake token")
	r.NotNil(err)
	correctResponse := &grmproto.VerifyTableOwnershipResponse{
		Status: &scql.Status{
			Code:    int32(scql.Code_OK),
			Message: "",
		},
		IsOwner: false,
	}
	httpresp200 := httptest.ResponseRecorder{
		Code: 200,
		Body: bytes.NewBuffer([]byte(toJson(t, correctResponse))),
	}
	m.EXPECT().Do(gomock.Any()).Return(httpresp200.Result(), nil)
	res, err := client.VerifyTableOwnership("tid", "fake token")
	r.Nil(err)
	r.Equal(false, res)
}
