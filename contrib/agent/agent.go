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

package agent

import (
	"fmt"

	"github.com/secretflow/kuscia/proto/api/v1alpha1/appconfig"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/secretflow/scql/contrib/agent/config"
	"github.com/secretflow/scql/contrib/agent/kusciastub"
	taskconfig "github.com/secretflow/scql/contrib/agent/proto"
	"github.com/secretflow/scql/contrib/agent/scqlstub"
)

type Agent struct {
	config *config.Config
}

func NewAgent(config *config.Config) *Agent {
	return &Agent{
		config: config,
	}
}

func (a *Agent) Run() error {
	taskConfig := &taskconfig.ScqlTaskInputConfig{}
	err := protojson.Unmarshal([]byte(a.config.TaskConfig), taskConfig)
	if err != nil {
		return fmt.Errorf("failed to deserialize task config: %s, err: %v", a.config.TaskConfig, err)
	}
	if taskConfig.Initiator == "" {
		return fmt.Errorf("initiator is empty in task_config: %v", taskConfig)
	}

	clusterDefine := &appconfig.ClusterDefine{}
	err = protojson.Unmarshal([]byte(a.config.ClusterDefine), clusterDefine)
	if err != nil {
		return fmt.Errorf("failed to deserialize cluster define: %s, err: %v", a.config.ClusterDefine, err)
	}
	if clusterDefine.SelfPartyIdx < 0 || int(clusterDefine.SelfPartyIdx) >= len(clusterDefine.GetParties()) {
		return fmt.Errorf("self party idx '%d' out of boundary, party number in cluster define is %d", clusterDefine.SelfPartyIdx, len(clusterDefine.GetParties()))
	}

	selfParty := clusterDefine.Parties[clusterDefine.SelfPartyIdx].GetName()
	if selfParty == taskConfig.Initiator {
		logrus.Infof("self party: %s, run as initiator", selfParty)
		return a.InitiatorRun(clusterDefine, taskConfig, selfParty)
	} else {
		logrus.Infof("self party: %s, run as participants, initiator is %s", selfParty, taskConfig.Initiator)
		return a.participantsRun(clusterDefine, taskConfig, selfParty)
	}
}

func (a *Agent) InitiatorRun(clusterDefine *appconfig.ClusterDefine, taskConfig *taskconfig.ScqlTaskInputConfig, selfParty string) error {
	kusciaStub, err := kusciastub.NewStub(a.config, clusterDefine, taskConfig, selfParty)
	if err != nil {
		return err
	}

	err = kusciaStub.CreateServing()
	if err != nil {
		return err
	}
	defer func() {
		err := kusciaStub.DeleteServing()
		if err != nil {
			logrus.Warnf("failed to delete serving: %v", err)
		}
	}()

	err = kusciaStub.WaitServingReady()
	if err != nil {
		return err
	}

	brokerUrl, err := kusciaStub.GetBrokerUrl()
	if err != nil {
		return err
	}

	scqlStub, err := scqlstub.NewStub(a.config, clusterDefine, taskConfig, brokerUrl, selfParty)
	if err != nil {
		return err
	}

	err = scqlStub.CreateProject()
	if err != nil {
		return err
	}

	err = scqlStub.InviteMember()
	if err != nil {
		return err
	}

	err = scqlStub.WaitMemberReady()
	if err != nil {
		return err
	}

	err = scqlStub.CreateTable()
	if err != nil {
		return err
	}

	err = scqlStub.GrantCcl()
	if err != nil {
		return err
	}

	// wait for other parties to create table and grant ccl
	err = scqlStub.WaitCclReady()
	if err != nil {
		return err
	}

	err = scqlStub.CreateJob()
	if err != nil {
		return err
	}

	// wait for job to finish
	result, err := scqlStub.WaitResult(false)
	if err != nil {
		return err
	}

	// set domainData
	err = kusciaStub.SetDomainData(result)
	if err != nil {
		return err
	}

	// clear broker Serving after all broker job finished
	err = kusciaStub.WaitAllDomainDataReady()
	if err != nil {
		return err
	}

	return nil
}

func (a *Agent) participantsRun(clusterDefine *appconfig.ClusterDefine, taskConfig *taskconfig.ScqlTaskInputConfig, selfParty string) error {
	kusciaStub, err := kusciastub.NewStub(a.config, clusterDefine, taskConfig, selfParty)
	if err != nil {
		return err
	}
	err = kusciaStub.WaitServingReady()
	if err != nil {
		return err
	}

	brokerUrl, err := kusciaStub.GetBrokerUrl()
	if err != nil {
		return err
	}
	if brokerUrl == "" {
		return fmt.Errorf("broker url is empty")
	}

	scqlStub, err := scqlstub.NewStub(a.config, clusterDefine, taskConfig, brokerUrl, selfParty)
	if err != nil {
		return err
	}
	// wait for invitation
	err = scqlStub.WaitAndAcceptInvitation()
	if err != nil {
		return err
	}

	err = scqlStub.WaitMemberReady()
	if err != nil {
		return err
	}

	err = scqlStub.CreateTable()
	if err != nil {
		return err
	}

	err = scqlStub.GrantCcl()
	if err != nil {
		return err
	}

	// wait for job to finish
	result, err := scqlStub.WaitResult(true)
	if err != nil {
		return err
	}

	// set domainData if needed
	err = kusciaStub.SetDomainData(result)
	if err != nil {
		return err
	}

	return nil
}
