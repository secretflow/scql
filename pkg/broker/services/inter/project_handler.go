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

package inter

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"golang.org/x/exp/slices"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/secretflow/scql/pkg/broker/services/common"
	"github.com/secretflow/scql/pkg/broker/storage"
	"github.com/secretflow/scql/pkg/proto-gen/scql"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
)

func (svc *grpcInterSvc) InviteToProject(c context.Context, req *pb.InviteToProjectRequest) (resp *pb.InviteToProjectResponse, err error) {
	app := svc.app
	txn := app.MetaMgr.CreateMetaTransaction()
	defer func() {
		txn.Finish(err)
	}()

	project, err := txn.GetProject(req.GetProject().GetProjectId())
	if err == nil {
		return nil, fmt.Errorf("InviteToProject: project %v already exists", project.ID)
	}

	spuCfg, err := protojson.Marshal(req.GetProject().GetConf().GetSpuRuntimeCfg())
	if err != nil {
		return nil, fmt.Errorf("InviteToProject: %v", err)
	}
	invite := storage.Invitation{
		ProjectID:   req.GetProject().GetProjectId(),
		Name:        req.GetProject().GetName(),
		Description: req.GetProject().GetDescription(),
		Creator:     req.GetProject().GetCreator(),
		Member:      strings.Join(req.GetProject().GetMembers(), ";"),
		ProjectConf: storage.ProjectConfig{
			SpuConf: string(spuCfg),
		},
		Inviter:    req.GetInviter(),
		Invitee:    app.Conf.PartyCode,
		InviteTime: time.Now(),
	}
	err = txn.AddInvitations([]storage.Invitation{invite})
	if err != nil {
		return nil, fmt.Errorf("InviteToProject: %v", err)
	}

	return &pb.InviteToProjectResponse{
		Status: &scql.Status{
			Code:    int32(0),
			Message: fmt.Sprintf("deal with invitation for project %v succeed", req.GetProject().GetProjectId()),
		},
	}, nil
}

func (svc *grpcInterSvc) ReplyInvitation(c context.Context, req *pb.ReplyInvitationRequest) (resp *pb.ReplyInvitationResponse, err error) {
	if req.GetClientId().GetCode() == "" {
		return nil, fmt.Errorf("ReplyInvitation: missing client id in request: %v", req.String())
	}

	txn := svc.app.MetaMgr.CreateMetaTransaction()
	defer func() {
		txn.Finish(err)
	}()

	proj, err := txn.GetProject(req.GetProjectId())
	if err != nil {
		return nil, fmt.Errorf("ReplyInvitation: %v", err)
	}
	if proj.Creator != svc.app.Conf.PartyCode {
		return nil, fmt.Errorf("ReplyInvitation: project %v not owned by selfParty %v", proj.ID, svc.app.Conf.PartyCode)
	}

	// if already in project, return result directly
	if slices.Contains(strings.Split(proj.Member, ";"), req.GetClientId().GetCode()) {
		if req.GetRespond() == pb.InvitationRespond_ACCEPT {
			projBytes, err := json.Marshal(proj) // TODO: json is not effective enough
			if err != nil {
				return nil, fmt.Errorf("ReplyInvitation marshal proj err: %v", err)
			}
			return &pb.ReplyInvitationResponse{
				Status: &scql.Status{
					Code:    int32(0),
					Message: "already in project",
				},
				ProjectInfo: projBytes,
			}, nil
		}
		return nil, fmt.Errorf("ReplyInvitation: already in project")
	}

	var invitation storage.Invitation
	invitation, err = txn.GetUnhandledInvitation(req.GetProjectId(), svc.app.Conf.PartyCode, req.GetClientId().GetCode())
	if err != nil {
		return nil, fmt.Errorf("ReplyInvitation: GetUnhandledInvitation: %v", err)
	}

	needAddMember := false
	if req.GetRespond() == pb.InvitationRespond_ACCEPT {
		invitation.Accepted = 1
		needAddMember = true
	} else {
		invitation.Accepted = -1
	}
	err = txn.UpdateInvitation(invitation)
	if err != nil {
		return nil, fmt.Errorf("ReplyInvitation: %v", err)
	}

	if needAddMember {
		err = txn.AddProjectMember(req.GetProjectId(), req.GetClientId().GetCode())
		if err != nil {
			return nil, fmt.Errorf("ReplyInvitation: %v", err)
		}

		// Sync to other parties
		var targetParties []string
		for _, p := range strings.Split(proj.Member, ";") {
			if p != proj.Creator && p != req.GetClientId().GetCode() {
				targetParties = append(targetParties, p)
			}
		}
		go common.PostSyncInfo(svc.app, req.GetProjectId(), pb.ChangeEntry_AddProjectMember, req.GetClientId().GetCode(), targetParties)
	}

	proj, err = txn.GetProject(req.GetProjectId())
	if err != nil {
		return nil, fmt.Errorf("ReplyInvitation: %v", err)
	}
	projBytes, err := json.Marshal(proj)
	if err != nil {
		return nil, fmt.Errorf("ReplyInvitation marshal proj err: %v", err)
	}
	return &pb.ReplyInvitationResponse{
		Status: &scql.Status{
			Code:    int32(0),
			Message: fmt.Sprintf("deal with reply invitation for project %v succeed", req.GetProjectId()),
		},
		ProjectInfo: projBytes}, nil
}
