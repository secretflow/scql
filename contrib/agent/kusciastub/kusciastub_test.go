// Copyright 2024 Ant Group Co., Ltd.
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

package kusciastub

import (
	"fmt"
	"testing"
	"time"

	v1alpha1 "github.com/secretflow/kuscia/proto/api/v1alpha1"
	"github.com/secretflow/kuscia/proto/api/v1alpha1/appconfig"
	"github.com/secretflow/kuscia/proto/api/v1alpha1/kusciaapi"
	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/secretflow/scql/contrib/agent/kusciastub/mock_kusciaapi"
	taskconfig "github.com/secretflow/scql/contrib/agent/proto"
	"github.com/secretflow/scql/pkg/proto-gen/scql"
)

const (
	queryServingOk       = `{"status":{"code":0, "message":"success", "details":[]}, "data":{"serving_input_config":"", "initiator":"alice", "parties":[{"domain_id":"alice", "role":"broker", "app_image":"scqlagent", "update_strategy":{"type":"RollingUpdate", "max_surge":"25%", "max_unavailable":"25%"}, "resources":[{"container_name":"broker", "min_cpu":"", "max_cpu":"", "min_memory":"", "max_memory":""}], "service_name_prefix":"scqlagent-broker"}, {"domain_id":"bob", "role":"broker", "app_image":"scqlagent", "update_strategy":{"type":"RollingUpdate", "max_surge":"25%", "max_unavailable":"25%"}, "resources":[{"container_name":"broker", "min_cpu":"", "max_cpu":"", "min_memory":"", "max_memory":""}], "service_name_prefix":"scqlagent-broker"}], "status":{"state":"Available", "reason":"", "message":"", "total_parties":2, "available_parties":2, "create_time":"2024-09-14T07:39:03Z", "party_statuses":[{"domain_id":"bob", "role":"broker", "state":"Available", "replicas":1, "available_replicas":1, "unavailable_replicas":0, "updatedReplicas":1, "create_time":"2024-09-14T07:39:03Z", "endpoints":[]}, {"domain_id":"alice", "role":"broker", "state":"Available", "replicas":1, "available_replicas":1, "unavailable_replicas":0, "updatedReplicas":1, "create_time":"2024-09-14T07:39:03Z", "endpoints":[{"port_name":"inter", "scope":"Cluster", "endpoint":"scqlagent-broker-inter.alice.svc"}, {"port_name":"intra", "scope":"Domain", "endpoint":"scqlagent-broker-intra.alice.svc:28823"}]}]}}}`
	queryServingNotFound = `{"status":{"code":11601, "message":"kusciadeployments.kuscia.secretflow \"scqlserving-broker\" not found", "details":[]}, "data":null}`
)

type KusciaTestSuit struct {
	suite.Suite
	stub          *KusciaStub
	servingClient *mock_kusciaapi.MockServingServiceClient
	dataClient    *mock_kusciaapi.MockDomainDataServiceClient
}

func (suite *KusciaTestSuit) SetupTest() {
	fmt.Println("SetupTest")
	ctrl := gomock.NewController(suite.T())

	suite.servingClient = mock_kusciaapi.NewMockServingServiceClient(ctrl)
	suite.dataClient = mock_kusciaapi.NewMockDomainDataServiceClient(ctrl)
	suite.stub = &KusciaStub{
		dataClient:    suite.dataClient,
		servingClient: suite.servingClient,
		selfParty:     "alice",
		servingId:     "test-servingid",
		taskConfig: &taskconfig.ScqlTaskInputConfig{
			Initiator: "alice",
			ProjectId: "mock-project-id",
			OutputIds: map[string]string{
				"alice": "mock-output-id",
			},
		},
		clusterDefine: &appconfig.ClusterDefine{},
		waitTimeout:   2500 * time.Millisecond,
	}
}

func (suite *KusciaTestSuit) TestCreateServing() {
	fmt.Println("TestCreateServing")

	suite.servingClient.EXPECT().CreateServing(gomock.Any(), gomock.Any()).Return(&kusciaapi.CreateServingResponse{
		Status: &v1alpha1.Status{
			Code:    1,
			Message: "failed",
		},
	}, nil)
	suite.Error(suite.stub.CreateServing())

	suite.servingClient.EXPECT().CreateServing(gomock.Any(), gomock.Any()).Return(&kusciaapi.CreateServingResponse{
		Status: &v1alpha1.Status{
			Code:    0,
			Message: "success",
		},
	}, nil)
	suite.NoError(suite.stub.CreateServing())
}

func (suite *KusciaTestSuit) TestWaitServingReady() {
	fmt.Println("TestWaitServingReady")

	resp := &kusciaapi.QueryServingResponse{}
	suite.NoError(protojson.Unmarshal([]byte(queryServingNotFound), resp))
	suite.servingClient.EXPECT().QueryServing(gomock.Any(), gomock.Any()).Return(resp, nil).Times(2)
	suite.Error(suite.stub.WaitServingReady())

	respOk := &kusciaapi.QueryServingResponse{}
	suite.NoError(protojson.Unmarshal([]byte(queryServingOk), respOk))
	suite.servingClient.EXPECT().QueryServing(gomock.Any(), gomock.Any()).Return(resp, nil)
	suite.servingClient.EXPECT().QueryServing(gomock.Any(), gomock.Any()).Return(respOk, nil)
	suite.NoError(suite.stub.WaitServingReady())
}

func (suite *KusciaTestSuit) TestDeleteServing() {
	fmt.Println("TestDeleteServing")
	suite.servingClient.EXPECT().DeleteServing(gomock.Any(), gomock.Any()).Return(&kusciaapi.DeleteServingResponse{
		Status: &v1alpha1.Status{
			Code:    0,
			Message: "success",
		},
	}, nil)
	suite.NoError(suite.stub.DeleteServing())
}

func (suite *KusciaTestSuit) TestGetBrokerUrl() {
	resp := &kusciaapi.QueryServingResponse{}
	suite.NoError(protojson.Unmarshal([]byte(queryServingOk), resp))
	suite.servingClient.EXPECT().QueryServing(gomock.Any(), gomock.Any()).Return(resp, nil)
	url, err := suite.stub.GetBrokerUrl()
	suite.NoError(err)
	suite.Equal("scqlagent-broker-intra.alice.svc:28823", url)

	suite.NoError(protojson.Unmarshal([]byte(queryServingNotFound), resp))
	suite.servingClient.EXPECT().QueryServing(gomock.Any(), gomock.Any()).Return(resp, nil)
	url, err = suite.stub.GetBrokerUrl()
	suite.Error(err)
	suite.Equal("", url)
}

func (suite *KusciaTestSuit) TestInitiatorSetDomaindata() {
	fmt.Println("TestInitiatorSetDomaindata begin")
	result := &scql.QueryResult{
		AffectedRows: 0,
		CostTimeS:    100,
	}

	//rpc failed
	suite.dataClient.EXPECT().CreateDomainData(gomock.Any(), gomock.Any()).Return(&kusciaapi.CreateDomainDataResponse{
		Status: &v1alpha1.Status{
			Code:    1,
			Message: "failed",
		},
	}, nil)

	suite.Error(suite.stub.SetDomainData(result))

	//rpc success
	suite.dataClient.EXPECT().CreateDomainData(gomock.Any(), gomock.Any()).Return(&kusciaapi.CreateDomainDataResponse{
		Status: &v1alpha1.Status{
			Code:    0,
			Message: "success",
		},
	}, nil)
	suite.NoError(suite.stub.SetDomainData(result))

	fmt.Println("TestInitiatorSetDomaindata end")
}

func (suite *KusciaTestSuit) TestParicipantsSetDomaindata() {
	fmt.Println("TestParicipantsSetDomaindata begin")
	suite.stub.selfParty = "bob"
	result := &scql.QueryResult{
		AffectedRows: 0,
		CostTimeS:    100,
	}

	suite.NoError(suite.stub.SetDomainData(result))
	fmt.Println("TestParicipantsSetDomaindata end")
}
func TestServerSuit(t *testing.T) {
	fmt.Println("test begin")
	suite.Run(t, new(KusciaTestSuit))
	fmt.Println("test end")
}
