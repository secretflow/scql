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
	"errors"
	"fmt"
	"strings"
	"time"

	"golang.org/x/exp/slices"

	"github.com/sirupsen/logrus"

	"github.com/secretflow/scql/pkg/broker/services/common"
	"github.com/secretflow/scql/pkg/broker/storage"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/status"
	"github.com/secretflow/scql/pkg/util/message"
)

func (svc *grpcInterSvc) InviteToProject(c context.Context, req *pb.InviteToProjectRequest) (resp *pb.InviteToProjectResponse, err error) {
	if req.GetInviter() == "" || req.GetInviter() != req.GetClientId().GetCode() {
		return nil, status.New(pb.Code_BAD_REQUEST, fmt.Sprintf("InviteToProject: Inviter=%s must be equal to ClientId.Code:%s", req.GetInviter(), req.GetClientId().GetCode()))
	}
	app := svc.app
	txn := app.MetaMgr.CreateMetaTransaction()
	defer func() {
		err = txn.Finish(err)
	}()

	project, err := txn.GetProject(req.GetProject().GetProjectId())
	if err == nil {
		return nil, status.New(pb.Code_BAD_REQUEST, fmt.Sprintf("InviteToProject: project %v already exists", project.ID))
	}

	reqProjectConf := req.GetProject().GetConf()
	if reqProjectConf == nil {
		reqProjectConf = &pb.ProjectConfig{}
	}

	if reqProjectConf.GetSpuRuntimeCfg() == nil {
		return nil, fmt.Errorf("InviteToProject: spu runtime config can not be null in project config")
	}

	projConf, err := message.ProtoMarshal(reqProjectConf)
	if err != nil {
		return nil, fmt.Errorf("InviteToProject: failed to serialize project conf: %v", err)
	}

	invite := storage.Invitation{
		ProjectID:        req.GetProject().GetProjectId(),
		Name:             req.GetProject().GetName(),
		Description:      req.GetProject().GetDescription(),
		Creator:          req.GetProject().GetCreator(),
		ProjectCreatedAt: req.GetProject().GetCreatedAt().AsTime(),
		Member:           strings.Join(req.GetProject().GetMembers(), ";"),
		ProjectConf:      string(projConf),
		Inviter:          req.GetInviter(),
		Invitee:          app.Conf.PartyCode,
		InviteTime:       time.Now(),
	}
	// set existed project invalid
	err = txn.SetUnhandledInvitationsInvalid(req.GetProject().GetProjectId(), req.GetInviter(), app.Conf.PartyCode)
	if err != nil {
		return nil, fmt.Errorf("InviteMember: failed to set existed invitations invalid with err %v", err)
	}
	err = txn.AddInvitations([]storage.Invitation{invite})
	if err != nil {
		return nil, status.New(pb.Code_INTERNAL, fmt.Sprintf("InviteToProject: %s", err.Error()))
	}

	return &pb.InviteToProjectResponse{
		Status: &pb.Status{
			Code:    int32(0),
			Message: fmt.Sprintf("deal with invitation for project %v succeed", req.GetProject().GetProjectId()),
		},
	}, nil
}

func (svc *grpcInterSvc) ReplyInvitation(c context.Context, req *pb.ReplyInvitationRequest) (resp *pb.ReplyInvitationResponse, err error) {
	if req.GetClientId().GetCode() == "" {
		return nil, status.New(pb.Code_BAD_REQUEST, fmt.Sprintf("ReplyInvitation: missing client id in request: %v", req.String()))
	}

	txn := svc.app.MetaMgr.CreateMetaTransaction()
	defer func() {
		err = txn.Finish(err)
	}()

	proj, err := storage.AddExclusiveLock(txn).GetProject(req.GetProjectId())
	if err != nil {
		return nil, fmt.Errorf("ReplyInvitation: %v", err)
	}
	if proj.Creator != svc.app.Conf.PartyCode {
		return nil, fmt.Errorf("ReplyInvitation: project %v not owned by self party %v", proj.ID, svc.app.Conf.PartyCode)
	}
	members, err := txn.GetProjectMembers(req.GetProjectId())
	if err != nil {
		return nil, fmt.Errorf("ReplyInvitation: %v", err)
	}
	// if already in project, return result directly
	if slices.Contains(members, req.GetClientId().GetCode()) {
		logrus.Infof("member %s already in project %s", req.GetClientId().GetCode(), req.GetProjectId())
		if req.GetRespond() == pb.InvitationRespond_ACCEPT {
			return &pb.ReplyInvitationResponse{
				Status: &pb.Status{
					Code:    int32(0),
					Message: "already in project",
				},
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
	status := pb.InvitationStatus_DECLINED
	if req.GetRespond() == pb.InvitationRespond_ACCEPT {
		err = common.CheckInvitationCompatibleWithProj(invitation, proj)
		if err != nil {
			newErr := txn.SetInvitationInvalidByID(invitation.ID)
			if newErr != nil {
				return nil, fmt.Errorf("ReplyInvitation: %s", newErr)
			}
			return &pb.ReplyInvitationResponse{
				Status: &pb.Status{
					Code:    int32(pb.Code_DATA_INCONSISTENCY),
					Message: fmt.Sprintf("ReplyInvitation: %s", errors.Join(err, newErr)),
				},
			}, nil
		}
		status = pb.InvitationStatus_ACCEPTED
		needAddMember = true
	}
	err = txn.ModifyInvitationStatus(invitation.ID, status)
	if err != nil {
		return nil, fmt.Errorf("ReplyInvitation: %v", err)
	}

	if needAddMember {
		logrus.Infof("add member %s to project %s", req.GetClientId().GetCode(), req.GetProjectId())
		err = txn.AddProjectMembers([]storage.Member{{ProjectID: req.GetProjectId(), Member: req.GetClientId().GetCode()}})
		if err != nil {
			return nil, fmt.Errorf("ReplyInvitation: %v", err)
		}

		// Sync to other parties
		var targetParties []string
		for _, p := range members {
			if p != proj.Creator && p != req.GetClientId().GetCode() {
				targetParties = append(targetParties, p)
			}
		}

		go func() {
			err := common.PostSyncInfo(svc.app, req.GetProjectId(), pb.ChangeEntry_AddProjectMember, req.GetClientId().GetCode(), targetParties)
			if err != nil {
				logrus.Warnf("ReplyInvitation: sync info to %v err %s", targetParties, err)
			}
		}()
	}
	// get newest project info
	projAndMembers, err := txn.GetProjectAndMembers(req.GetProjectId())
	logrus.Infof("newest project member: %v", projAndMembers.Members)
	if err != nil {
		return nil, fmt.Errorf("ReplyInvitation: %v", err)
	}
	projectInfo, err := json.Marshal(projAndMembers)
	if err != nil {
		return nil, fmt.Errorf("ReplyInvitation: unable to marshal %v", err)
	}
	return &pb.ReplyInvitationResponse{
		Status: &pb.Status{
			Code:    int32(0),
			Message: fmt.Sprintf("deal with reply invitation for project %v succeed", req.GetProjectId()),
		},
		ProjectInfo: projectInfo}, nil
}
