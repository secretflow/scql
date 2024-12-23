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

package scqlstub

import (
	"fmt"
	"time"

	"github.com/secretflow/kuscia/proto/api/v1alpha1/appconfig"
	"github.com/sirupsen/logrus"

	"github.com/secretflow/scql/contrib/agent/config"
	taskconfig "github.com/secretflow/scql/contrib/agent/proto"
	"github.com/secretflow/scql/pkg/broker/application"
	"github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/proto-gen/spu"
)

type ScqlStub struct {
	stub       *application.IntraStub
	url        string
	taskConfig *taskconfig.ScqlTaskInputConfig
	jobId      string // NOTE: scql need to support to specify jobId
	selfParty  string

	clusterDefine    *appconfig.ClusterDefine
	waitTimeout      time.Duration
	waitQueryTimeout time.Duration
}

func NewStub(conf *config.Config, clusterDefine *appconfig.ClusterDefine, taskConfig *taskconfig.ScqlTaskInputConfig, brokerUrl, selfParty string) (*ScqlStub, error) {
	if taskConfig.ProjectId == "" {
		return nil, fmt.Errorf("ProjectId is empty in task_config: %v", taskConfig)
	}

	return &ScqlStub{
		// TODO: support config for intra stub timeout
		stub:             &application.IntraStub{Timeout: 30 * time.Second},
		url:              fmt.Sprintf("http://%s", brokerUrl), // TODO: support tls when scql intra service enabled tls
		taskConfig:       taskConfig,
		jobId:            fmt.Sprintf("job-%s", taskConfig.ProjectId),
		selfParty:        selfParty,
		clusterDefine:    clusterDefine,
		waitTimeout:      conf.WaitTimeout,
		waitQueryTimeout: conf.WaitQueryTimeout,
	}, nil
}

func (s *ScqlStub) CreateProject() error {
	req := &scql.CreateProjectRequest{
		ProjectId: s.taskConfig.ProjectId,
		Conf: &scql.ProjectConfig{
			// NOTE: scql in secretPad not support specifying spu runtime config, using SEMI2k/FM64 temporarily
			SpuRuntimeCfg: &spu.RuntimeConfig{
				Protocol: spu.ProtocolKind_SEMI2K,
				Field:    spu.FieldType_FM64,
			},
		},
	}
	logrus.Infof("CreateProject req: %v", req)
	resp := &scql.CreateProjectResponse{}
	err := s.stub.CreateProject(s.url, req, resp)
	if err != nil {
		return fmt.Errorf("CreateProject failed: %v", err)
	}
	if resp.GetStatus().GetCode() != 0 {
		return fmt.Errorf("CreateProject status error: %v", resp.GetStatus())
	}
	logrus.Info("CreateProject succeed")
	return nil
}

func (s *ScqlStub) InviteMember() error {
	for idx, p := range s.clusterDefine.GetParties() {
		if idx == int(s.clusterDefine.GetSelfPartyIdx()) {
			continue
		}
		req := &scql.InviteMemberRequest{
			ProjectId: s.taskConfig.ProjectId,
			Invitee:   p.GetName(),
		}
		resp := &scql.InviteMemberResponse{}
		err := s.stub.InviteMember(s.url, req, resp)
		if err != nil {
			return fmt.Errorf("InviteMember failed: %v", err)
		}
		if resp.GetStatus().GetCode() != 0 {
			return fmt.Errorf("InviteMember: invite %s status error: %v", p.GetName(), resp.GetStatus())
		}
		logrus.Infof("InviteMember: invite %s successfully", p.GetName())
	}
	return nil
}

func (s *ScqlStub) WaitAndAcceptInvitation() error {
	logrus.Infof("WaitAndAcceptInvitation for project: %s", s.taskConfig.ProjectId)

	timer := time.NewTimer(s.waitTimeout)
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-timer.C:
			return fmt.Errorf("WaitAndAcceptInvitation timeout")
		case <-ticker.C:
			req := &scql.ListInvitationsRequest{}
			resp := &scql.ListInvitationsResponse{}
			err := s.stub.ListInvitations(s.url, req, resp)
			if err != nil {
				return fmt.Errorf("WaitAndAcceptInvitation: failed to list invitations: %v", err)
			}
			if resp.GetStatus().GetCode() != 0 {
				return fmt.Errorf("WaitAndAcceptInvitation: list invitations status error: %v", resp.GetStatus())
			}
			for _, invite := range resp.GetInvitations() {
				if invite.GetProject().GetProjectId() == s.taskConfig.ProjectId &&
					invite.GetStatus() == scql.InvitationStatus_UNDECIDED {
					logrus.Infof("WaitAndAcceptInvitation: received invitation from '%s', try to accept", invite.GetInviter())
					req := &scql.ProcessInvitationRequest{
						InvitationId: invite.GetInvitationId(),
						Respond:      scql.InvitationRespond_ACCEPT,
					}
					resp := &scql.ProcessInvitationResponse{}
					err := s.stub.ProcessInvitation(s.url, req, resp)
					if err != nil {
						return fmt.Errorf("WaitAndAcceptInvitation: accept invitation failed: %v", err)
					}
					if resp.GetStatus().GetCode() != 0 {
						return fmt.Errorf("WaitAndAcceptInvitation: accept invitation status error: %v", resp.GetStatus())
					}
					logrus.Info("WaitAndAcceptInvitation succeed")
					return nil
				}
			}
		}
	}
}

func (s *ScqlStub) WaitMemberReady() error {
	logrus.Info("WaitMemberReady start")
	timer := time.NewTimer(s.waitTimeout)
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-timer.C:
			return fmt.Errorf("WaitMemberReady timeout")
		case <-ticker.C:
			req := &scql.ListProjectsRequest{
				Ids: []string{s.taskConfig.ProjectId},
			}
			resp := &scql.ListProjectsResponse{}
			err := s.stub.ListProjects(s.url, req, resp)
			if err != nil {
				return fmt.Errorf("WaitMemberReady: failed to list projects: %v", err)
			}
			if resp.GetStatus().GetCode() != 0 {
				return fmt.Errorf("WaitMemberReady: list projects status error: %v", resp.GetStatus())
			}
			for _, p := range resp.GetProjects() {
				if p.GetProjectId() == s.taskConfig.ProjectId &&
					len(p.GetMembers()) == len(s.clusterDefine.GetParties()) {
					logrus.Infof("WaitMemberReady succeed: project '%s' cantains all memebers", s.taskConfig.ProjectId)
					return nil
				}
			}
		}
	}
}

func (s *ScqlStub) CreateTable() error {
	logrus.Infof("CreateTable for: %s", s.selfParty)
	if _, ok := s.taskConfig.Tables[s.selfParty]; !ok {
		logrus.Infof("CreateTable: no table need to create in party: %s", s.selfParty)
		return nil
	}
	for _, tb := range s.taskConfig.Tables[s.selfParty].GetTbls() {
		req := &scql.CreateTableRequest{
			ProjectId: s.taskConfig.ProjectId,
			TableName: tb.GetTableName(),
			RefTable:  tb.GetRefTable(),
			DbType:    tb.GetDbType(),
		}
		for _, col := range tb.GetColumns() {
			req.Columns = append(req.Columns, &scql.CreateTableRequest_ColumnDesc{
				Name:  col.GetName(),
				Dtype: col.GetDtype(),
			})
		}
		resp := &scql.CreateTableResponse{}
		err := s.stub.CreateTable(s.url, req, resp)
		if err != nil {
			return fmt.Errorf("CreateTable: failed: %v", err)
		}
		if resp.GetStatus().GetCode() != 0 {
			return fmt.Errorf("CreateTable: status error: %v", resp.GetStatus())
		}
		logrus.Infof("CreateTable: create table '%v' successfully", tb)
	}

	return nil
}

func (s *ScqlStub) GrantCcl() error {
	logrus.Infof("GrantCcl in party %s", s.selfParty)
	if _, ok := s.taskConfig.Ccls[s.selfParty]; !ok {
		logrus.Infof("GrantCcl: no ccl need to grant in party: %s", s.selfParty)
		return nil
	}
	req := &scql.GrantCCLRequest{
		ProjectId: s.taskConfig.ProjectId,
	}
	for _, ccl := range s.taskConfig.Ccls[s.selfParty].GetColumnControlList() {
		req.ColumnControlList = append(req.ColumnControlList, &scql.ColumnControl{
			Col: &scql.ColumnDef{
				ColumnName: ccl.GetCol().GetColumnName(),
				TableName:  ccl.GetCol().GetTableName(),
			},
			PartyCode:  ccl.GetPartyCode(),
			Constraint: scql.Constraint(ccl.GetConstraint()),
		})
	}
	resp := &scql.GrantCCLResponse{}
	err := s.stub.GrantCCL(s.url, req, resp)
	if err != nil {
		return fmt.Errorf("GrantCcl: failed: %v", err)
	}
	if resp.GetStatus().GetCode() != 0 {
		return fmt.Errorf("GrantCcl: status error: %v", resp.GetStatus())
	}
	logrus.Infof("GrantCcl: succeed, ccls: %v", req.GetColumnControlList())
	return nil
}

func (s *ScqlStub) WaitCclReady() error {
	// get ccl total count from task config
	totalCnt := 0
	for _, ccl := range s.taskConfig.Ccls {
		totalCnt += len(ccl.GetColumnControlList())
	}
	logrus.Infof("WaitCclReady: total cnt: %d", totalCnt)

	timer := time.NewTimer(s.waitTimeout)
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-timer.C:
			return fmt.Errorf("WaitCclReady timeout")
		case <-ticker.C:
			req := &scql.ShowCCLRequest{
				ProjectId: s.taskConfig.ProjectId,
			}
			resp := &scql.ShowCCLResponse{}
			err := s.stub.ListCCLs(s.url, req, resp)
			if err != nil {
				return fmt.Errorf("WaitCclReady: failed to show ccl: %v", err)
			}
			if resp.GetStatus().GetCode() != 0 {
				return fmt.Errorf("WaitCclReady: show ccl status error: %v", resp.GetStatus())
			}
			logrus.Infof("WaitCclReady: current ccl count: %d", len(resp.GetColumnControlList()))
			if len(resp.GetColumnControlList()) == totalCnt {
				logrus.Infof("WaitCclReady: all columns have been granted ccl")
				return nil
			}
		}
	}
}

func (s *ScqlStub) CreateJob() error {
	logrus.Infof("CreateJob: %s", s.taskConfig.Query)
	req := &scql.QueryRequest{
		ProjectId: s.taskConfig.ProjectId,
		Query:     s.taskConfig.Query,
		JobId:     s.jobId,
	}
	resp := &scql.SubmitResponse{}
	err := s.stub.CreateJob(s.url, req, resp)
	if err != nil {
		return fmt.Errorf("CreateJob: failed: %v", err)
	}
	if resp.GetStatus().GetCode() != 0 {
		return fmt.Errorf("CreateJob: status error: %v", resp.GetStatus())
	}
	logrus.Info("CreateJob succeed")
	return nil
}

func (s *ScqlStub) WaitResult(skipWait bool) (*scql.QueryResult, error) {
	logrus.Info("WaitResult start")
	if _, ok := s.taskConfig.GetOutputIds()[s.selfParty]; !ok {
		if skipWait {
			logrus.Infof("WaitResult: no output id for %s, skip wait", s.selfParty)
			return nil, nil
		} // else still waiting result
	}
	timer := time.NewTimer(s.waitTimeout)
	ticker := time.NewTicker(time.Second)
	jobRunning := false
	for {
		select {
		case <-timer.C:
			// TODO: engine may still running when timeout
			return nil, fmt.Errorf("WaitResult timeout: you may need to clear scql engine pods manually and modify the timeout")
		case <-ticker.C:
			req := &scql.FetchResultRequest{
				JobId: s.jobId,
			}
			resp := &scql.FetchResultResponse{}
			err := s.stub.GetResult(s.url, req, resp)
			if err != nil {
				return nil, fmt.Errorf("WaitResult: fetch result failed: %v", err)
			}
			switch resp.GetStatus().GetCode() {
			case int32(scql.Code_OK):
				logrus.Infof("WaitResult succeed: job '%s' is ready", s.jobId)
				return resp.GetResult(), nil
			case int32(scql.Code_NOT_FOUND):
				logrus.Infof("WaitResult: job '%s' not found", s.jobId)
				continue
			case int32(scql.Code_NOT_READY):
				if !jobRunning {
					timer.Reset(s.waitQueryTimeout)
					jobRunning = true
				}
				logrus.Infof("WaitResult: job '%s' is not ready, job status: %v", s.jobId, resp.GetJobStatus())
				continue
			default:
				return nil, fmt.Errorf("WaitResult: fetch result status error: %v", resp.GetStatus())
			}
		}
	}
}
