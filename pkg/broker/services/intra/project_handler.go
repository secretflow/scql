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
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/sirupsen/logrus"

	"github.com/secretflow/scql/pkg/broker/application"
	"github.com/secretflow/scql/pkg/broker/services/common"
	"github.com/secretflow/scql/pkg/broker/storage"
	"github.com/secretflow/scql/pkg/constant"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/status"
	"github.com/secretflow/scql/pkg/util/message"
)

func (svc *grpcIntraSvc) CreateProject(c context.Context, req *pb.CreateProjectRequest) (resp *pb.CreateProjectResponse, err error) {
	if req == nil {
		return nil, status.New(pb.Code_BAD_REQUEST, "CreateProject: illegal empty request")
	}

	app := svc.app

	projectConf := req.GetConf()
	if projectConf == nil {
		projectConf = &pb.ProjectConfig{}
	}

	if projectConf.GetSpuRuntimeCfg() == nil {
		return nil, fmt.Errorf("CreateProject: spu runtime config can not be null in project config")
	}

	if projectConf.SessionExpireSeconds <= 0 {
		projectConf.SessionExpireSeconds = int64(app.Conf.SessionExpireTime.Seconds())
	}
	// use value in config file if not set
	groupByThreshold := app.Conf.SecurityCompromise.GroupByThreshold
	if projectConf.GroupByThreshold != nil {
		groupByThreshold = projectConf.GetGroupByThreshold()
	}
	if groupByThreshold == 0 {
		groupByThreshold = constant.DefaultGroupByThreshold
	}

	revealGroupMark := app.Conf.SecurityCompromise.RevealGroupMark
	if projectConf.RevealGroupMark != nil {
		revealGroupMark = projectConf.GetRevealGroupMark()
	}

	projConf, err := message.ProtoMarshal(&pb.ProjectConfig{
		SpuRuntimeCfg:                             projectConf.GetSpuRuntimeCfg(),
		SessionExpireSeconds:                      projectConf.GetSessionExpireSeconds(),
		UnbalancePsiRatioThreshold:                projectConf.GetUnbalancePsiRatioThreshold(),
		UnbalancePsiLargerPartyRowsCountThreshold: projectConf.GetUnbalancePsiLargerPartyRowsCountThreshold(),
		PsiCurveType:                              projectConf.GetPsiCurveType(),
		HttpMaxPayloadSize:                        projectConf.GetHttpMaxPayloadSize(),
		LinkRecvTimeoutSec:                        projectConf.GetLinkRecvTimeoutSec(),
		LinkThrottleWindowSize:                    projectConf.GetLinkThrottleWindowSize(),
		LinkChunkedSendParallelSize:               projectConf.GetLinkChunkedSendParallelSize(),
		GroupByThreshold:                          &groupByThreshold,
		RevealGroupMark:                           &revealGroupMark,
	})

	if err != nil {
		return nil, fmt.Errorf("CreateProject: failed to serialize project conf: %v", err)
	}

	project := storage.Project{
		ID:          req.GetProjectId(),
		Name:        req.GetName(),
		Description: req.GetDescription(),
		Creator:     app.Conf.PartyCode,
		Archived:    false,
		ProjectConf: string(projConf),
	}
	if project.ID == "" {
		id, err := application.GenerateProjectID()
		if err != nil {
			return nil, fmt.Errorf("CreateProject: failed to generate project ID: %v", err)
		}
		project.ID = fmt.Sprint(id)
	}
	// check project ID validity when working as db name
	err = common.VerifyProjectID(project.ID)
	if err != nil {
		return nil, fmt.Errorf("CreateProject: failed to veriry project ID: %v", err)
	}

	err = app.MetaMgr.ExecInMetaTransaction(func(txn *storage.MetaTransaction) error {
		return txn.CreateProject(project)
	})
	if err != nil {
		return nil, fmt.Errorf("CreateProject: failed to create table in meta database: %v", err)
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

func (svc *grpcIntraSvc) ListProjects(c context.Context, req *pb.ListProjectsRequest) (*pb.ListProjectsResponse, error) {
	app := svc.app
	var projectWithMembers []storage.ProjectWithMember
	err := app.MetaMgr.ExecInMetaTransaction(func(txn *storage.MetaTransaction) error {
		var err error
		projectWithMembers, err = txn.ListProjects(req.GetIds())
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("ListProjects: failed to list project in meta database: %v", err)
	}
	var projectsList []*pb.ProjectDesc
	for _, projWithMember := range projectWithMembers {
		proj := projWithMember.Proj
		var projConf pb.ProjectConfig
		err := message.ProtoUnmarshal([]byte(proj.ProjectConf), &projConf)
		if err != nil {
			return nil, fmt.Errorf("ListProjects: unmarshal: %v", err)
		}

		projectsList = append(projectsList, &pb.ProjectDesc{
			ProjectId:   proj.ID,
			Name:        proj.Name,
			Description: proj.Description,
			Conf:        &projConf,
			Creator:     proj.Creator,
			Members:     projWithMember.Members,
			CreatedAt:   timestamppb.New(proj.CreatedAt),
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
	if req.GetMethod() != pb.InviteMemberRequest_PUSH {
		return nil, status.New(pb.Code_BAD_REQUEST, fmt.Sprintf("InviteMember: not support invite method %v", req.GetMethod()))
	}
	if req.GetProjectId() == "" || req.GetInvitee() == "" {
		return nil, status.New(pb.Code_BAD_REQUEST, fmt.Sprintf("InviteMember: request illegal, empty project id or invitee in request:%v", protojson.Format(req)))
	}

	app := svc.app
	txn := app.MetaMgr.CreateMetaTransaction()
	defer func() {
		err = txn.Finish(err)
	}()

	projWithMember, err := txn.GetProjectAndMembers(req.GetProjectId())
	if err != nil {
		return nil, fmt.Errorf("InviteMember: failed to list project info in meta database: %v", err)
	}
	proj := projWithMember.Proj
	if proj.Creator != app.Conf.PartyCode {
		return nil, fmt.Errorf("InviteMember: project creator{%v} not equal to selfParty{%v}", proj.Creator, app.Conf.PartyCode)
	}
	members := projWithMember.Members
	if slices.Contains(members, req.GetInvitee()) {
		return nil, fmt.Errorf("InviteMember: project already contains invitee{%v}", req.GetInvitee())
	}

	// Send Rpc request first
	url, err := app.PartyMgr.GetBrokerUrlByParty(req.GetInvitee())
	if err != nil {
		return nil, fmt.Errorf("InviteMember: failed to get invitee broker url: %v", err)
	}

	var projConf pb.ProjectConfig
	err = message.ProtoUnmarshal([]byte(proj.ProjectConf), &projConf)
	if err != nil {
		return nil, fmt.Errorf("InviteMember unmarshal: failed to deserialize project conf string: %v", err)
	}

	// add invitation to metaMgr
	invite := storage.Invitation{
		ProjectID:        proj.ID,
		Name:             proj.Name,
		Description:      proj.Description,
		Creator:          proj.Creator,
		ProjectCreatedAt: proj.CreatedAt,
		Member:           strings.Join(projWithMember.Members, ";"),
		ProjectConf:      proj.ProjectConf,
		Inviter:          proj.Creator,
		Invitee:          req.GetInvitee(),
		Status:           int8(pb.InvitationStatus_UNDECIDED),
		InviteTime:       time.Now(),
	}
	// set existed project invalid
	err = txn.SetUnhandledInvitationsInvalid(proj.ID, proj.Creator, req.GetInvitee())
	if err != nil {
		return nil, fmt.Errorf("InviteMember: failed to set existed invitations invalid with err %v", err)
	}
	err = txn.AddInvitations([]storage.Invitation{invite})
	if err != nil {
		return nil, fmt.Errorf("InviteMember: failed to add invitation in meta database: %v", err)
	}

	interReq := &pb.InviteToProjectRequest{
		ClientId: &pb.PartyId{
			Code: app.Conf.PartyCode,
		},
		Project: &pb.ProjectDesc{
			ProjectId:   proj.ID,
			Name:        proj.Name,
			Description: proj.Description,
			Conf:        &projConf,
			Creator:     proj.Creator,
			Members:     projWithMember.Members,
			CreatedAt:   timestamppb.New(proj.CreatedAt),
		},
		Inviter: app.Conf.PartyCode,
	}

	response := &pb.InviteToProjectResponse{}
	err = app.InterStub.InviteToProject(url, interReq, response)
	if err != nil {
		return nil, fmt.Errorf("InviteMember not success: error occur when inviting %s err %v", req.GetInvitee(), err)
	}

	if response.GetStatus().GetCode() != 0 {
		return nil, fmt.Errorf("InviteMember not success: %v", response.GetStatus().String())
	}
	return &pb.InviteMemberResponse{
		Status: &pb.Status{
			Code:    int32(0),
			Message: fmt.Sprintf("invite member %v succeed", req.GetInvitee()),
		},
	}, nil
}

func (svc *grpcIntraSvc) ListInvitations(c context.Context, req *pb.ListInvitationsRequest) (resp *pb.ListInvitationsResponse, err error) {
	targetInvitation := storage.Invitation{}
	selectUndecidedInvitation := false
	if req.Filter != nil {
		switch filter := req.Filter.(type) {
		case *pb.ListInvitationsRequest_Status:
			targetInvitation.Status = int8(filter.Status)
			selectUndecidedInvitation = filter.Status == pb.InvitationStatus_UNDECIDED
		case *pb.ListInvitationsRequest_Inviter:
			targetInvitation.Inviter = filter.Inviter
		}
	}
	var invitations []storage.Invitation
	err = svc.app.MetaMgr.ExecInMetaTransaction(func(txn *storage.MetaTransaction) error {
		var err error
		invitations, err = txn.GetInvitationsBy(targetInvitation, selectUndecidedInvitation)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("ListInvitations: failed to list invitations in meta database %v", err)
	}
	var invitationsList []*pb.ProjectInvitation
	for _, inv := range invitations {
		var projConf pb.ProjectConfig
		err = message.ProtoUnmarshal([]byte(inv.ProjectConf), &projConf)
		if err != nil {
			return nil, fmt.Errorf("ListInvitations: failed to deserialize spu config: %v", err)
		}

		invitationsList = append(invitationsList, &pb.ProjectInvitation{
			InvitationId: inv.ID,
			Project: &pb.ProjectDesc{
				ProjectId:   inv.ProjectID,
				Name:        inv.Name,
				Description: inv.Description,
				Conf:        &projConf,
				Creator:     inv.Creator,
				Members:     strings.Split(inv.Member, ";"),
			},
			Inviter: inv.Inviter,
			Invitee: inv.Invitee,
			// postscript...
			Status: pb.InvitationStatus(inv.Status),
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
	// check parameter
	if _, ok := pb.InvitationRespond_name[int32(req.GetRespond())]; !ok {
		return nil, status.New(pb.Code_BAD_REQUEST, fmt.Sprintf("ProcessInvitation: not support respond type: %d", req.GetRespond()))
	}

	txn := svc.app.MetaMgr.CreateMetaTransaction()
	var invitation storage.Invitation
	invalidInvitation := false
	var projectInfoBytes []byte
	var invitationMembers []storage.Member
	defer func() {
		finishErr := txn.Finish(err)
		// use new transaction to set invitation invalid
		// whether commit or rollback, we need to set invitation invalid if invalidInvitation is true
		if invalidInvitation {
			svc.app.MetaMgr.ExecInMetaTransaction(func(txn *storage.MetaTransaction) error {
				return txn.SetInvitationInvalidByID(invitation.ID)
			})
		}
		if finishErr != nil {
			// commit error, user should try to process again
			if err == nil {
				err = fmt.Errorf("ProcessInvitation: commit error, please try it again")
				return
			}
			// rollback, just return
			return
		}
		// add members not in invitations
		if len(projectInfoBytes) > 0 {
			projectAndMembers := storage.ProjectWithMember{}
			unmarshalErr := json.Unmarshal(projectInfoBytes, &projectAndMembers)
			// ignore error and return
			if unmarshalErr != nil {
				logrus.Warningf("ProcessInvitation: unable to unmarshal %s", unmarshalErr.Error())
				return
			}
			var newMembers []storage.Member
			for _, member := range projectAndMembers.Members {
				found := false
				if member == invitation.Creator || member == invitation.Invitee {
					continue
				}
				for _, projectMember := range invitationMembers {
					if member == projectMember.Member {
						found = true
					}
				}
				if !found {
					newMembers = append(newMembers, storage.Member{ProjectID: invitation.ProjectID, Member: member})
				}
			}
			if len(newMembers) > 0 {
				svc.app.MetaMgr.ExecInMetaTransaction(func(txn *storage.MetaTransaction) error {
					// lock project
					_, lockErr := storage.AddExclusiveLock(txn).GetProject(invitation.ProjectID)
					if lockErr != nil {
						logrus.Warningf("ProcessInvitation: unable to lock project %s", err.Error())
						return lockErr
					}
					return txn.AddProjectMembers(newMembers)
				})
			}
		}
	}()
	invitation, err = txn.GetUnhandledInvitationWithID(req.GetInvitationId())
	if err != nil {
		return nil, fmt.Errorf("ProcessInvitation: GetUnhandledInvitationWithID: %v", err)
	}
	if invitation.Invitee != svc.app.Conf.PartyCode {
		invalidInvitation = true
		return nil, fmt.Errorf("ProcessInvitation: invitee{%v} != selfParty{%v}", invitation.Invitee, svc.app.Conf.PartyCode)
	}
	// lock projection id
	proj, err := storage.AddExclusiveLock(txn).GetProject(invitation.ProjectID)
	if err == nil {
		return nil, fmt.Errorf("ProcessInvitation: existing project{%+v} conflicts with invitation{%+v}", proj, invitation)
	}

	status := pb.InvitationStatus_DECLINED
	if req.GetRespond() == pb.InvitationRespond_ACCEPT {
		logrus.Infof("ProcessInvitation: accept invitation invited by %s to project %s with conf %+v", invitation.Inviter, invitation.ProjectID, invitation.ProjectConf)
		err = txn.CreateProject(storage.Project{
			ID:          invitation.ProjectID,
			Name:        invitation.Name,
			Description: invitation.Description,
			Archived:    false,
			ProjectConf: string(invitation.ProjectConf),
			Creator:     invitation.Creator,
			CreatedAt:   invitation.ProjectCreatedAt,
		})
		if err != nil {
			return nil, fmt.Errorf("ProcessInvitation: CreateProject: failed to create project in meta database while processing invitation: %v", err)
		}
		for _, member := range append(strings.Split(invitation.Member, ";"), invitation.Invitee) {
			if member != "" && member != invitation.Creator {
				invitationMembers = append(invitationMembers, storage.Member{ProjectID: invitation.ProjectID, Member: member})
			}
		}
		err = txn.AddProjectMembers(invitationMembers)
		if err != nil {
			return nil, fmt.Errorf("ProcessInvitation: AddProjectMember: failed to add project member in meta database while processing invitation: %v", err)
		}
		status = pb.InvitationStatus_ACCEPTED
	}

	err = txn.ModifyInvitationStatus(invitation.ID, status)
	if err != nil {
		return nil, fmt.Errorf("ProcessInvitation: failed to modify invitation status: %v", err)
	}

	// 1. Send Rpc request to project owner
	url, err := svc.app.PartyMgr.GetBrokerUrlByParty(invitation.Inviter)
	if err != nil {
		return nil, fmt.Errorf("ProcessInvitation: failed to get inviter broker url: %v", err)
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
	// ignore project info in response for now
	err = svc.app.InterStub.ReplyInvitation(url, interReq, response)
	if err != nil {
		return nil, err
	}
	if response.GetStatus().GetCode() == int32(pb.Code_DATA_INCONSISTENCY) {
		invalidInvitation = true
		return nil, fmt.Errorf("ProcessInvitation: failed to reply invitation due to invitation %d is not same with project", invitation.ID)
	}
	if response.GetStatus().GetCode() != 0 {
		return nil, fmt.Errorf("ProcessInvitation not success: %v", response.GetStatus().String())
	}
	projectInfoBytes = response.GetProjectInfo()
	// 3. TODO(jingshi): Ask and store more project information: table/ccl
	return &pb.ProcessInvitationResponse{
		Status: &pb.Status{
			Code:    int32(0),
			Message: fmt.Sprintf("process for invitation %v succeed", req.GetInvitationId()),
		},
	}, nil

}
