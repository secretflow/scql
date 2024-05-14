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
	"fmt"

	"golang.org/x/exp/slices"

	"github.com/secretflow/scql/pkg/broker/services/common"
	"github.com/secretflow/scql/pkg/broker/storage"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/status"
	"github.com/secretflow/scql/pkg/util/sliceutil"
)

func (svc *grpcIntraSvc) GrantCCL(ctx context.Context, req *pb.GrantCCLRequest) (resp *pb.GrantCCLResponse, err error) {
	if req == nil || req.GetProjectId() == "" || req.GetColumnControlList() == nil {
		return nil, status.New(pb.Code_BAD_REQUEST, "GrantCCL: illegal request")
	}

	app := svc.app
	txn := app.MetaMgr.CreateMetaTransaction()
	defer func() {
		err = txn.Finish(err)
	}()

	members, err := txn.GetProjectMembers(req.GetProjectId())
	if err != nil {
		return nil, fmt.Errorf("GrantCCL: get project %s err: %v", req.GetProjectId(), err)
	}
	if len(members) == 0 {
		return nil, fmt.Errorf("GrantCCL: project %s has no members or project doesn't exist", req.GetProjectId())
	}
	privs, err := ColumnControlList2ColumnPriv(req.GetProjectId(), req.GetColumnControlList())
	if err != nil {
		return nil, fmt.Errorf("GrantCCL: %v", err)
	}

	// only allow to grant ccl to project members
	for _, priv := range privs {
		if !slices.Contains(members, priv.DestParty) {
			return nil, fmt.Errorf("GrantCCL: member %v not in project members %v", priv.DestParty, members)
		}
	}

	err = common.GrantColumnConstraintsWithCheck(txn, req.GetProjectId(), privs, common.OwnerChecker{Owner: app.Conf.PartyCode})
	if err != nil {
		return nil, fmt.Errorf("GrantCCL: %v", err)
	}

	// Sync Info to other parties
	var targetParties []string
	for _, p := range members {
		if p != app.Conf.PartyCode {
			targetParties = append(targetParties, p)
		}
	}
	go common.PostSyncInfo(svc.app, req.GetProjectId(), pb.ChangeEntry_GrantCCL, privs, targetParties)

	return &pb.GrantCCLResponse{
		Status: &pb.Status{
			Code:    int32(0),
			Message: fmt.Sprintf("grant ccl for %v succeed", req.GetProjectId()),
		}}, nil

}

func (svc *grpcIntraSvc) RevokeCCL(c context.Context, req *pb.RevokeCCLRequest) (resp *pb.RevokeCCLResponse, err error) {
	if req == nil || req.GetProjectId() == "" || len(req.GetColumnControlList()) == 0 {
		return nil, status.New(pb.Code_BAD_REQUEST, "RevokeCCL: illegal request")
	}
	privIDs, err := ColumnControlList2ColumnPrivIdentifier(req.GetProjectId(), req.GetColumnControlList())
	if err != nil {
		return nil, fmt.Errorf("RevokeCCL: %v", err)
	}
	app := svc.app
	var members []string
	err = app.MetaMgr.ExecInMetaTransaction(func(txn *storage.MetaTransaction) error {
		var err error
		err = common.RevokeColumnConstraintsWithCheck(txn, req.GetProjectId(), app.Conf.PartyCode, privIDs)
		if err != nil {
			return fmt.Errorf("RevokeCCL: %v", err)
		}
		// get sync parties
		members, err = txn.GetProjectMembers(req.GetProjectId())
		if err != nil {
			return fmt.Errorf("RevokeCCL: GetProject: %v", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	// sync to other parties
	go func() {
		var targetParties []string
		for _, p := range members {
			if p != app.Conf.PartyCode {
				targetParties = append(targetParties, p)
			}
		}
		common.PostSyncInfo(svc.app, req.GetProjectId(), pb.ChangeEntry_RevokeCCL, privIDs, targetParties)
	}()

	return &pb.RevokeCCLResponse{
		Status: &pb.Status{
			Code:    int32(0),
			Message: "revoke ccls succeed",
		},
	}, nil

}

func (svc *grpcIntraSvc) ShowCCL(ctx context.Context, req *pb.ShowCCLRequest) (resp *pb.ShowCCLResponse, err error) {
	if req == nil || req.GetProjectId() == "" {
		return nil, status.New(pb.Code_BAD_REQUEST, "ShowCCL: no project id in request")
	}
	app := svc.app
	txn := app.MetaMgr.CreateMetaTransaction()
	defer func() {
		err = txn.Finish(err)
	}()
	// check project exist
	projectAndMembers, err := txn.GetProjectAndMembers(req.GetProjectId())
	if err != nil {
		return nil, fmt.Errorf("ShowCCL: GetProjectAndMembers err: %v", err)
	}
	// check member exist
	if !sliceutil.ContainsAll(projectAndMembers.Members, req.GetDestParties()) {
		return nil, fmt.Errorf("ShowCCL: dest parties %v not found in project members", sliceutil.Subtraction(req.GetDestParties(), projectAndMembers.Members))
	}
	// check table exist
	_, exist, err := txn.GetTables(req.GetProjectId(), req.GetTables())
	if err != nil {
		return nil, fmt.Errorf("ShowCCL: CheckTablesExist err: %v", err)
	}
	if !exist {
		return nil, fmt.Errorf("ShowCCL: tables %v not all exist", req.GetTables())
	}

	var privs []storage.ColumnPriv
	privs, err = txn.ListColumnConstraints(req.GetProjectId(), req.GetTables(), req.GetDestParties())
	if err != nil {
		return nil, fmt.Errorf("ShowCCL: ListColumnConstraints err: %v", err)
	}

	var ccls []*pb.ColumnControl
	for _, priv := range privs {
		columnIdf := priv.ColumnPrivIdentifier
		ccls = append(ccls, &pb.ColumnControl{
			Col: &pb.ColumnDef{
				ColumnName: columnIdf.ColumnName,
				TableName:  columnIdf.TableName,
			},
			PartyCode:  columnIdf.DestParty,
			Constraint: pb.Constraint(pb.Constraint_value[priv.Priv]),
		})
	}

	return &pb.ShowCCLResponse{
		Status: &pb.Status{
			Code:    int32(0),
			Message: fmt.Sprintf("show ccl for %v succeed", req.GetProjectId()),
		},
		ColumnControlList: ccls,
	}, nil

}

func ColumnControlList2ColumnPrivIdentifier(projectID string, ccls []*pb.ColumnControl) ([]storage.ColumnPrivIdentifier, error) {
	privs, err := ColumnControlList2ColumnPriv(projectID, ccls)
	if err != nil {
		return nil, fmt.Errorf("ColumnControlList2ColumnPrivIdentifier: %v", err)
	}
	var privIDs []storage.ColumnPrivIdentifier
	for _, priv := range privs {
		privIDs = append(privIDs, priv.ColumnPrivIdentifier)
	}
	return privIDs, nil
}

func ColumnControlList2ColumnPriv(projectID string, ccls []*pb.ColumnControl) ([]storage.ColumnPriv, error) {
	if len(ccls) == 0 {
		return []storage.ColumnPriv{}, nil
	}
	if projectID == "" {
		return nil, fmt.Errorf("ColumnControlList2ColumnPriv: empty projectID")
	}

	var privs []storage.ColumnPriv
	for _, ccl := range ccls {
		if ccl.GetCol().GetTableName() == "" || ccl.GetCol().GetColumnName() == "" || ccl.GetPartyCode() == "" {
			return nil, fmt.Errorf("ColumnControlList2ColumnPriv: contain empty table/column/party: %+v", ccl)
		}
		if _, ok := pb.Constraint_name[int32(ccl.Constraint)]; !ok {
			return nil, fmt.Errorf("ColumnControlList2ColumnPriv: illegal constraint: %d", ccl.GetConstraint())
		}
		privs = append(privs, storage.ColumnPriv{
			ColumnPrivIdentifier: storage.ColumnPrivIdentifier{
				ProjectID:  projectID,
				TableName:  ccl.GetCol().GetTableName(),
				ColumnName: ccl.GetCol().GetColumnName(),
				DestParty:  ccl.GetPartyCode(),
			},
			Priv: ccl.GetConstraint().String(),
		})
	}
	return privs, nil
}
