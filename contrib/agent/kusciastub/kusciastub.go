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
	"context"
	"fmt"
	"time"

	"github.com/secretflow/scql/pkg/proto-gen/scql"

	"github.com/secretflow/kuscia/proto/api/v1alpha1/appconfig"
	"github.com/secretflow/kuscia/proto/api/v1alpha1/kusciaapi"
	"github.com/sirupsen/logrus"

	"github.com/secretflow/scql/contrib/agent/config"
	taskconfig "github.com/secretflow/scql/contrib/agent/proto"
	"github.com/secretflow/scql/pkg/util/kusciaclient"
	"github.com/secretflow/scql/pkg/util/message"
)

type KusciaStub struct {
	selfParty string
	servingId string

	taskConfig    *taskconfig.ScqlTaskInputConfig
	clusterDefine *appconfig.ClusterDefine

	servingClient kusciaapi.ServingServiceClient
	dataClient    kusciaapi.DomainDataServiceClient

	waitTimeout time.Duration
	scqlImage   string
}

func NewStub(conf *config.Config, clusterDefine *appconfig.ClusterDefine, taskConfig *taskconfig.ScqlTaskInputConfig, self string) (*KusciaStub, error) {
	if conf.Kuscia == nil {
		return nil, fmt.Errorf("NewStub: kuscia in config is nil")
	}
	conn, err := kusciaclient.NewKusciaClientConn(conf.Kuscia.Endpoint, conf.Kuscia.TLSMode, conf.Kuscia.Cert, conf.Kuscia.Key, conf.Kuscia.CaCert, conf.Kuscia.Token)
	if err != nil {
		return nil, fmt.Errorf("NewStub: failed to create kuscia client conn: %v", err)
	}

	stub := &KusciaStub{
		selfParty:     self,
		servingId:     fmt.Sprintf("sid-%s", taskConfig.ProjectId),
		servingClient: kusciaapi.NewServingServiceClient(conn),
		dataClient:    kusciaapi.NewDomainDataServiceClient(conn),
		taskConfig:    taskConfig,
		clusterDefine: clusterDefine,
		waitTimeout:   conf.WaitTimeout,
		scqlImage:     conf.ScqlImage,
	}
	return stub, nil
}

func (k *KusciaStub) CreateServing() error {
	req := &kusciaapi.CreateServingRequest{
		ServingId: k.servingId,
		Initiator: k.taskConfig.Initiator,
	}
	for _, p := range k.clusterDefine.Parties {
		req.Parties = append(req.Parties, &kusciaapi.ServingParty{
			DomainId:          p.Name,
			AppImage:          k.scqlImage,
			Role:              "broker",
			ServiceNamePrefix: fmt.Sprintf("broker-%s", k.servingId),
		})
	}

	logrus.Infof("CreateServing request: %v", req)
	resp, err := k.servingClient.CreateServing(context.Background(), req)
	if err != nil {
		return fmt.Errorf("CreateServing: failed to create serving: %v", err)
	}
	if resp.GetStatus().GetCode() != 0 {
		return fmt.Errorf("CreateServing: status error: %v", resp.GetStatus())
	}
	logrus.Infof("CreateServing succeed, resp: %v", resp)
	return nil
}

func (k *KusciaStub) WaitServingReady() error {
	timer := time.NewTimer(k.waitTimeout)
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-timer.C:
			return fmt.Errorf("WaitServingReady timeout, servingId: %s", k.servingId)
		case <-ticker.C:
			req := &kusciaapi.QueryServingRequest{
				ServingId: k.servingId,
			}
			resp, err := k.servingClient.QueryServing(context.Background(), req)
			if err != nil {
				return fmt.Errorf("WaitServingReady: failed to query serving: %v", err)
			}
			if resp.GetStatus().GetCode() != 0 {
				logrus.Warnf("WaitServingReady: query serving status error: %v", resp.GetStatus())
				continue
			}
			switch resp.GetData().GetStatus().GetState() {
			case "Available":
				logrus.Info("WaitServingReady succeed")
				return nil
			case "Failed":
				return fmt.Errorf("WaitServingReady: serving state Failed: %v", resp.GetData().GetStatus())
			default:
				continue
			}
		}
	}
}

func (k *KusciaStub) DeleteServing() error {
	req := &kusciaapi.DeleteServingRequest{
		ServingId: k.servingId,
	}
	resp, err := k.servingClient.DeleteServing(context.Background(), req)
	if err != nil {
		return fmt.Errorf("DeleteServing: failed to delete serving: %v", err)
	}
	if resp.GetStatus().GetCode() != 0 {
		return fmt.Errorf("DeleteServing: status error: %v", resp.GetStatus())
	}
	logrus.Info("DeleteServing succeed")
	return nil
}

// query serving to get the broker intra url
func (k *KusciaStub) GetBrokerUrl() (string, error) {
	req := &kusciaapi.QueryServingRequest{
		ServingId: k.servingId,
	}
	resp, err := k.servingClient.QueryServing(context.Background(), req)
	if err != nil {
		return "", fmt.Errorf("GetBrokerUrl: failed to query serving: %v", err)
	}
	if resp.GetStatus().GetCode() != 0 || resp.GetData().GetStatus().GetState() != "Available" {
		return "", fmt.Errorf("GetBrokerUrl: query serving status error: %v", resp.GetStatus())
	}

	for _, p := range resp.GetData().GetStatus().GetPartyStatuses() {
		if p.GetDomainId() == k.selfParty {
			for _, s := range p.GetEndpoints() {
				if s.GetPortName() == "intra" {
					logrus.Infof("GetBrokerUrl succeed: %s", s.GetEndpoint())
					return s.GetEndpoint(), nil
				}
			}
			return "", fmt.Errorf("GetBrokerUrl: failed to get broker intra url for domain %s in party status: %v", k.selfParty, p)
		}
	}
	return "", fmt.Errorf("GetBrokerUrl: failed to get broker url for %s in party statuses: %v", k.selfParty, resp.GetData().GetStatus().GetPartyStatuses())
}

func (k *KusciaStub) SetDomainData(result *scql.QueryResult) error {
	logrus.Infof("SetDomainData entry: %s", k.taskConfig.ProjectId)
	// save domaindata by outputIds
	if _, ok := k.taskConfig.GetOutputIds()[k.selfParty]; !ok {
		logrus.Infof("OutputIds not contain selfParty: %s, no need to save domaindata", k.selfParty)
		return nil
	}
	// makeup params
	resultJson, err := message.SerializeTo(result, message.EncodingTypeJson)
	if err != nil {
		return fmt.Errorf("SetDomainData: failed to marshal result to json : %v", err)
	}
	attributes := map[string]string{
		"dist_data": resultJson,
	}
	outputId := k.taskConfig.GetOutputIds()[k.selfParty]
	req := &kusciaapi.CreateDomainDataRequest{
		DomainId:     k.selfParty,
		DomaindataId: outputId,
		DatasourceId: "", // kuscia use default local datasource
		Name:         outputId,
		Type:         "report",
		RelativeUri:  outputId,
		Attributes:   attributes,
		Vendor:       k.selfParty,
	}

	resp, err := k.dataClient.CreateDomainData(context.Background(), req)
	if err != nil {
		return fmt.Errorf("SetDomainData: failed to set domaindata: %v", err)
	}
	if resp.GetStatus().GetCode() != 0 {
		return fmt.Errorf("SetDomainData: create domaindata status error: %v", resp.GetStatus())
	}
	logrus.Infof("SetDomainData end: %s", k.taskConfig.ProjectId)

	return nil
}

func (k *KusciaStub) WaitAllDomainDataReady() error {
	// NOTE: initiator should wait peers to set domainData(if exist) before delete broker serving.
	// temporarily sleep 5s here since initiator cannot check peers' domainData through the kuscia api.
	time.Sleep(time.Second * 5)
	return nil
}
