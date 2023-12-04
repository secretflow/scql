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

package intra

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"golang.org/x/exp/slices"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/secretflow/scql/pkg/broker/application"
	"github.com/secretflow/scql/pkg/broker/services/common"
	"github.com/secretflow/scql/pkg/broker/storage"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	spu "github.com/secretflow/scql/pkg/proto-gen/spu"
)

func (svc *grpcIntraSvc) CreateProject(c context.Context, req *pb.CreateProjectRequest) (resp *pb.CreateProjectResponse, err error) {
	if req == nil {
		return nil, fmt.Errorf("CreateProject: illegal empty request")
	}

	spuConf, err := protojson.Marshal(req.GetConf().GetSpuRuntimeCfg())
	if err != nil {
		return nil, fmt.Errorf("CreateProject: %v", err)
	}
	app := svc.app
	project := storage.Project{
		ID:          req.GetProjectId(),
		Name:        req.GetName(),
		Description: req.GetDescription(),
		Creator:     app.Conf.PartyCode,
		Member:      app.Conf.PartyCode,
		Archived:    false,
		ProjectConf: storage.ProjectConfig{
			SpuConf: string(spuConf),
		},
	}
	if project.ID == "" {
		id, err := application.GenerateProjectID()
		if err != nil {
			return nil, fmt.Errorf("CreateProject: %v", err)
		}
		project.ID = fmt.Sprint(id)
	}

	err = common.VerifyProjectID(project.ID)
	if err != nil {
		return nil, fmt.Errorf("CreateProject: %v", err)
	}

	txn := app.MetaMgr.CreateMetaTransaction()
	defer func() {
		txn.Finish(err)
	}()
	err = txn.CreateProject(project)
	if err != nil {
		return nil, fmt.Errorf("CreateProject: %v", err)
	}

	return &pb.CreateProjectResponse{
		Status: &pb.Status{
			Code:    int32(0),
			Message: fmt.Sprintf("create project succeed, id: %s", project.ID),
		},
		ProjectId: project.ID}, nil
}

func (svc *grpcIntraSvc) UpdateProject(context.Context, *pb.UpdateProjectRequest) (*pb.UpdateProjectResponse, error) {
	return nil, errors.New("method UpdateProject not implemented")
}

func (svc *grpcIntraSvc) ListProjects(c context.Context, req *pb.ListProjectsRequest) (resp *pb.ListProjectsResponse, err error) {
	app := svc.app
	txn := app.MetaMgr.CreateMetaTransaction()
	defer func() {
		txn.Finish(err)
	}()

	projects, err := txn.ListProjects(req.GetIds())
	if err != nil {
		return nil, fmt.Errorf("ListProjects: %v", err)
	}
	var projectsList []*pb.ProjectDesc
	for _, proj := range projects {
		var spuConf spu.RuntimeConfig
		err := protojson.Unmarshal([]byte(proj.ProjectConf.SpuConf), &spuConf)
		if err != nil {
			return nil, fmt.Errorf("ListProjects: unmarshal: %v", err)
		}
		projectsList = append(projectsList, &pb.ProjectDesc{
			ProjectId:   proj.ID,
			Name:        proj.Name,
			Description: proj.Description,
			Conf: &pb.ProjectConfig{
				SpuRuntimeCfg: &spuConf,
			},
			Creator: proj.Creator,
			Members: strings.Split(proj.Member, ";"),
		})
	}

	return &pb.ListProjectsResponse{
		Status: &pb.Status{
			Code:    int32(0),
			Message: "list projects succeed",
		},
		Projects: projectsList,
	}, nil
}

func (svc *grpcIntraSvc) ArchiveProject(c context.Context, req *pb.ArchiveProjectRequest) (resp *pb.ArchiveProjectResponse, err error) {
	return nil, errors.New("method ArchiveProject not implemented")
}

func (svc *grpcIntraSvc) InviteMember(c context.Context, req *pb.InviteMemberRequest) (resp *pb.InviteMemberResponse, err error) {
	if req.GetMethod() == pb.InviteMemberRequest_PULL {
		return nil, errors.New("InviteMember: not support PULL method")
	}
	if req.GetProjectId() == "" || req.GetInvitee() == "" {
		return nil, fmt.Errorf("InviteMember: request illegal, empty project id or invitee in request:%v", protojson.Format(req))
	}

	app := svc.app
	txn := app.MetaMgr.CreateMetaTransaction()
	defer func() {
		txn.Finish(err)
	}()

	var proj storage.Project
	proj, err = txn.GetProject(req.GetProjectId())
	if err != nil {
		return nil, fmt.Errorf("InviteMember: %v", err)
	}
	if proj.Creator != app.Conf.PartyCode {
		return nil, fmt.Errorf("InviteMember: project creator{%v} not equal to selfParty{%v}", proj.Creator, app.Conf.PartyCode)
	}
	members := strings.Split(proj.Member, ";")
	if slices.Contains(members, req.GetInvitee()) {
		return nil, fmt.Errorf("InviteMember: project already contains invitee{%v}", req.GetInvitee())
	}

	// Send Rpc request first
	url, err := app.PartyMgr.GetBrokerUrlByParty(req.GetInvitee())
	if err != nil {
		return nil, fmt.Errorf("InviteMember: %v", err)
	}

	var spuConf spu.RuntimeConfig
	err = protojson.Unmarshal([]byte(proj.ProjectConf.SpuConf), &spuConf)
	if err != nil {
		return nil, fmt.Errorf("InviteMember unmarshal: %v", err)
	}

	interReq := &pb.InviteToProjectRequest{
		ClientId: &pb.PartyId{
			Code: app.Conf.PartyCode,
		},
		Project: &pb.ProjectDesc{
			ProjectId:   proj.ID,
			Name:        proj.Name,
			Description: proj.Description,
			Conf: &pb.ProjectConfig{
				SpuRuntimeCfg: &spuConf,
			},
			Creator: proj.Creator,
			Members: strings.Split(proj.Member, ";"),
		},
		Inviter: app.Conf.PartyCode,
	}

	response := &pb.InviteToProjectResponse{}
	err = app.InterStub.InviteToProject(url, interReq, response)
	if err != nil {
		return nil, err
	}

	if response.GetStatus().GetCode() != 0 {
		return nil, fmt.Errorf("InviteMember not success: %v", response.GetStatus().String())
	}

	// add invitation to metaMgr
	invite := storage.Invitation{
		ProjectID:   proj.ID,
		Name:        proj.Name,
		Description: proj.Description,
		Creator:     proj.Creator,
		Member:      proj.Member,
		ProjectConf: proj.ProjectConf,
		Inviter:     proj.Creator,
		Invitee:     req.GetInvitee(),
		Accepted:    0,
		InviteTime:  time.Now(),
	}
	err = txn.AddInvitations([]storage.Invitation{invite})
	if err != nil {
		return nil, fmt.Errorf("InviteMember: %v", err)

	}

	return &pb.InviteMemberResponse{
		Status: &pb.Status{
			Code:    int32(0),
			Message: fmt.Sprintf("invite member %v succeed", req.GetInvitee()),
		},
	}, nil
}

func (svc *grpcIntraSvc) ListInvitations(c context.Context, req *pb.ListInvitationsRequest) (resp *pb.ListInvitationsResponse, err error) {
	txn := svc.app.MetaMgr.CreateMetaTransaction()
	defer func() {
		txn.Finish(err)
	}()

	invitations, err := txn.ListInvitations()
	if err != nil {
		return nil, fmt.Errorf("ListInvitations: %v", err)
	}
	var invitationsList []*pb.ProjectInvitation
	for _, inv := range invitations {
		var spuConf spu.RuntimeConfig
		err = protojson.Unmarshal([]byte(inv.ProjectConf.SpuConf), &spuConf)
		if err != nil {
			return nil, fmt.Errorf("ListInvitations: %v", err)
		}
		invitationsList = append(invitationsList, &pb.ProjectInvitation{
			InvitationId: inv.ID,
			Project: &pb.ProjectDesc{
				ProjectId:   inv.ProjectID,
				Name:        inv.Name,
				Description: inv.Description,
				Conf: &pb.ProjectConfig{
					SpuRuntimeCfg: &spuConf,
				},
				Creator: inv.Creator,
				Members: strings.Split(inv.Member, ";"),
			},
			Inviter: inv.Inviter,
			// postscript...
			Accepted: int32(inv.Accepted),
		})

	}

	return &pb.ListInvitationsResponse{
		Status: &pb.Status{
			Code:    int32(0),
			Message: "list invitations succeed",
		},
		Invitations: invitationsList,
	}, nil
}

func (svc *grpcIntraSvc) ProcessInvitation(c context.Context, req *pb.ProcessInvitationRequest) (resp *pb.ProcessInvitationResponse, err error) {
	txn := svc.app.MetaMgr.CreateMetaTransaction()
	defer func() {
		txn.Finish(err)
	}()

	var invitation storage.Invitation
	invitation, err = txn.GetUnhandledInvitationWithID(req.GetInvitationId())
	if err != nil {
		return nil, fmt.Errorf("ProcessInvitation: GetUnhandledInvitationWithID : %v", err)
	}
	if invitation.Invitee != svc.app.Conf.PartyCode {
		return nil, fmt.Errorf("ProcessInvitation: invitee{%v} != selfParty{%v}", invitation.Invitee, svc.app.Conf.PartyCode)
	}

	proj, err := txn.GetProject(invitation.ProjectID)
	if err == nil {
		return nil, fmt.Errorf("ProcessInvitation: existing project{%+v} conflicts with invitation{%+v}", proj, invitation)
	}
	if req.GetRespond() == pb.InvitationRespond_ACCEPT {
		// insert a blank project to avoid duplicate project and lock it
		err = txn.CreateProject(storage.Project{ID: invitation.ProjectID, Creator: invitation.Creator})
		if err != nil {
			return nil, fmt.Errorf("ProcessInvitation: CreateProject: %v", err)
		}
	}
	// 1. Send Rpc request to project owner
	url, err := svc.app.PartyMgr.GetBrokerUrlByParty(invitation.Inviter)
	if err != nil {
		return nil, fmt.Errorf("ProcessInvitation: %v", err)
	}

	interReq := &pb.ReplyInvitationRequest{
		ClientId: &pb.PartyId{
			Code: svc.app.Conf.PartyCode,
		},
		ProjectId:      invitation.ProjectID,
		Respond:        req.GetRespond(),
		RespondComment: req.GetRespondComment(),
	}
	response := &pb.ReplyInvitationResponse{}
	err = svc.app.InterStub.ReplyInvitation(url, interReq, response)
	if err != nil {
		return nil, err
	}

	if response.GetStatus().GetCode() != 0 {
		return nil, fmt.Errorf("ProcessInvitation not success: %v", response.GetStatus().String())
	}

	// 2. Update local invitations
	if req.GetRespond() == pb.InvitationRespond_ACCEPT {
		invitation.Accepted = 1
		// Add project
		var proj storage.Project
		err = json.Unmarshal(response.GetProjectInfo(), &proj)
		if err != nil {
			return nil, fmt.Errorf("ProcessInvitation: unmarshal project failed: %v", err)
		}
		err = txn.UpdateProject(proj)
		if err != nil {
			return nil, fmt.Errorf("ProcessInvitation: CreateProject: %v", err)
		}
	} else {
		invitation.Accepted = -1
	}
	err = txn.UpdateInvitation(invitation)
	if err != nil {
		return nil, fmt.Errorf("ProcessInvitation: %v", err)
	}

	// 3. TODO(jingshi): Ask and store more project information: table/ccl

	return &pb.ProcessInvitationResponse{
		Status: &pb.Status{
			Code:    int32(0),
			Message: fmt.Sprintf("process for invitation %v succeed", req.GetInvitationId()),
		},
	}, nil

}
