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
	"fmt"

	"github.com/sirupsen/logrus"

	"github.com/secretflow/scql/pkg/broker/application"
	"github.com/secretflow/scql/pkg/broker/services/common"
	"github.com/secretflow/scql/pkg/broker/storage"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/status"
	"github.com/secretflow/scql/pkg/util/sliceutil"
)

func (svc *grpcIntraSvc) CheckAndUpdateStatus(c context.Context, req *pb.CheckAndUpdateStatusRequest) (resp *pb.CheckAndUpdateStatusResponse, err error) {
	if req == nil {
		return nil, status.New(pb.Code_BAD_REQUEST, "CheckAndUpdateStatus: illegal empty request")
	}

	resp = &pb.CheckAndUpdateStatusResponse{
		Status:    &pb.Status{Code: int32(pb.Code_INTERNAL)},
		Conflicts: make(map[string]*pb.ProjectConflict),
	}

	app := svc.app
	// 1. get projects
	var projects []storage.ProjectWithMember
	err = app.MetaMgr.ExecInMetaTransaction(func(txn *storage.MetaTransaction) error {
		var tmpErr error
		projects, tmpErr = txn.ListProjects(req.GetProjectIds())
		return tmpErr
	})
	if err != nil {
		return nil, fmt.Errorf("CheckAndUpdateStatus: get projects err: %v", err)
	}

	// 2. check and update each project
	// TODO: limit the number of projects to avoid excessive network interactions
	for _, proj := range projects {
		members := proj.Members
		if proj.Proj.Creator != app.Conf.PartyCode {
			// check with project creator first to update members
			var conflict *pb.ProjectConflict
			members, conflict, err = checkAndUpdateStatusFor(app, proj.Proj.ID, proj.Proj.Creator)
			if err != nil {
				return nil, fmt.Errorf("CheckAndUpdateStatus: %v", err)
			}

			mergeConflict(resp, proj.Proj.ID, conflict)
		}
		// check with other members
		for _, member := range members {
			if member == proj.Proj.Creator || member == app.Conf.PartyCode {
				continue
			}
			var conflict *pb.ProjectConflict
			_, conflict, err = checkAndUpdateStatusFor(app, proj.Proj.ID, member)
			if err != nil {
				return nil, fmt.Errorf("CheckAndUpdateStatus: %v", err)
			}

			mergeConflict(resp, proj.Proj.ID, conflict)
		}
	}

	if len(resp.Conflicts) == 0 {
		resp.Status = &pb.Status{
			Code:    0,
			Message: "check and update status successfully",
		}
	}
	return
}

func checkAndUpdateStatusFor(app *application.App, projID, destParty string) ([]string, *pb.ProjectConflict, error) {
	localStatus, err := getLocalStatus(app, projID, destParty)
	if err != nil {
		return nil, nil, fmt.Errorf("checkAndUpdateStatusFor: get local status err: %v", err)
	}

	remoteStatus, err := getRemoteStatus(app, projID, destParty)
	if err != nil {
		return nil, nil, fmt.Errorf("checkAndUpdateStatusFor: get remote status err: %v", err)
	}

	conflict, err := checkAndUpdateStatus(app, localStatus, remoteStatus, destParty)
	if err != nil {
		return nil, nil, fmt.Errorf("checkAndUpdateStatusFor: %v", err)
	}

	return remoteStatus.Proj.Members, conflict, nil

}

func getLocalStatus(app *application.App, projID, owner string) (*storage.ProjectMeta, error) {
	var proj *storage.ProjectMeta
	err := app.MetaMgr.ExecInMetaTransaction(func(txn *storage.MetaTransaction) error {
		var tmpErr error
		proj, tmpErr = txn.GetProjectMeta(projID, nil, nil, owner)
		return tmpErr
	})
	if err != nil {
		return nil, fmt.Errorf("getLocalStatus failed: %v", err)
	}
	return proj, nil
}

func getRemoteStatus(app *application.App, projID, destParty string) (*storage.ProjectMeta, error) {
	destUrl, err := app.PartyMgr.GetBrokerUrlByParty(destParty)
	if err != nil {
		return nil, fmt.Errorf("getRemoteStatus: %v", err)
	}
	resourceSpec := []*pb.ResourceSpec{{Kind: pb.ResourceSpec_All, ProjectId: projID}}
	req := &pb.AskInfoRequest{ClientId: &pb.PartyId{Code: app.Conf.PartyCode}, ResourceSpecs: resourceSpec}
	response := &pb.AskInfoResponse{}
	err = app.InterStub.AskInfo(destUrl, req, response)
	if err != nil {
		return nil, fmt.Errorf("getRemoteStatus: %v", err)
	}
	if len(response.GetDatas()) != 1 {
		return nil, fmt.Errorf("getRemoteStatus: get wrong response")
	}

	meta := storage.ProjectMeta{}
	err = json.Unmarshal(response.GetDatas()[0], &meta)
	if err != nil {
		return nil, fmt.Errorf("getRemoteStatus: unmarshal response failed: %v", err)
	}
	err = checkStatusOwnedBy(&meta, destParty)
	if err != nil {
		return nil, fmt.Errorf("checkStatusOwnedBy: %v", err)
	}

	return &meta, nil
}

func checkStatusOwnedBy(meta *storage.ProjectMeta, owner string) error {
	ownedTable := make(map[string]bool)
	for _, tbl := range meta.Tables {
		if tbl.Table.Owner != owner {
			return fmt.Errorf("table {%v} not owned by party %v", tbl, owner)
		}
		ownedTable[tbl.Table.TableName] = true
	}

	for _, ccl := range meta.CCLs {
		if !ownedTable[ccl.TableName] {
			return fmt.Errorf("ccl {%v} not owned by party %v", ccl, owner)
		}
	}

	return nil
}

func checkAndUpdateStatus(app *application.App, local, remote *storage.ProjectMeta, remoteParty string) (*pb.ProjectConflict, error) {
	logrus.Debugf("check local: %v \n remote: %v", local, remote)
	conflict, err := checkConflict(app, local, remote, app.Conf.PartyCode, remoteParty)
	if err != nil || conflict != nil {
		return conflict, err
	}

	return nil, updateStatus(app, local, remote, remoteParty)
}

func mergeConflict(resp *pb.CheckAndUpdateStatusResponse, projID string, conflict *pb.ProjectConflict) {
	if conflict == nil || len(conflict.GetItems()) == 0 {
		return
	}

	if resp.Status.Code != int32(pb.Code_PROJECT_CONFLICT) {
		resp.Status = &pb.Status{
			Code:    int32(pb.Code_PROJECT_CONFLICT),
			Message: "exist conflicts in Projects, please check field: conflicts for details",
		}
	}

	if respConflict, ok := resp.Conflicts[projID]; ok {
		respConflict.Items = append(respConflict.Items, conflict.Items...)
	} else {
		resp.Conflicts[projID] = conflict
	}
}

func checkConflict(app *application.App, local, remote *storage.ProjectMeta, localParty, remoteParty string) (*pb.ProjectConflict, error) {
	var conflict pb.ProjectConflict

	// check project conflict
	localProj := local.Proj.Proj
	remoteProj := remote.Proj.Proj
	remoteProj.CreatedAt = localProj.CreatedAt // ignore comparing created time
	remoteProj.UpdatedAt = localProj.UpdatedAt // ignore comparing updated time
	eq, err := localProj.Equals(&remoteProj)
	if err != nil {
		return nil, fmt.Errorf("failed to compare project between local project and remote project, %v", err)
	}

	if !eq {
		conflict.Items = append(conflict.Items, &pb.ProjectConflict_ConflictItem{
			Message: fmt.Sprintf("project info conflict: '%+v' in party %s; '%+v' in party %s", localProj, localParty, remoteProj, remoteParty),
		})
		return &conflict, nil
	}

	// check table owner conflict
	var tableNames []string
	for _, tbl := range remote.Tables {
		tableNames = append(tableNames, tbl.Table.TableName)
	}
	// search from local storage, since param 'local' does not contains tables owned by others
	var localTables []storage.TableMeta
	err = app.MetaMgr.ExecInMetaTransaction(func(txn *storage.MetaTransaction) error {
		var tmpErr error
		localTables, _, tmpErr = txn.GetTableMetasByTableNames(localProj.ID, tableNames)
		return tmpErr
	})
	if err != nil {
		return nil, fmt.Errorf("checkConflict: get local tables err: %v", err)
	}

	for _, tbl := range localTables {
		if tbl.Table.Owner != remoteParty {
			conflict.Items = append(conflict.Items, &pb.ProjectConflict_ConflictItem{
				Message: fmt.Sprintf("table '%s' owner conflict: '%v' in party %s; '%v' in party %s", tbl.Table.TableName, tbl.Table.Owner, localParty, remoteParty, remoteParty),
			})
		}
	}

	if len(conflict.GetItems()) == 0 {
		return nil, nil
	}
	return &conflict, nil
}

func updateStatus(app *application.App, local, remote *storage.ProjectMeta, remoteParty string) (err error) {
	txn := app.MetaMgr.CreateMetaTransaction()
	defer func() {
		err = txn.Finish(err)
	}()

	// add project members
	proj := local.Proj.Proj
	if proj.Creator == remoteParty {
		var membersToAdd []storage.Member
		for _, member := range sliceutil.Subtraction(remote.Proj.Members, local.Proj.Members) {
			membersToAdd = append(membersToAdd, storage.Member{
				ProjectID: proj.ID,
				Member:    member,
			})
		}
		if len(membersToAdd) > 0 {
			err = txn.AddProjectMembers(membersToAdd)
			if err != nil {
				return fmt.Errorf("updateStatus: add project members err: %v", err)
			}
			logrus.Infof("updateStatus for project %s: add members '%+v'", proj.ID, membersToAdd)
		}
	}

	// update table and ccl
	var tsToUpdate []storage.TableMeta
	var psToUpdate []storage.ColumnPriv

	var localCCLs = make(map[string][]storage.ColumnPriv)
	for _, ccl := range local.CCLs {
		localCCLs[ccl.TableName] = append(localCCLs[ccl.TableName], ccl)
	}
	var remoteCCLs = make(map[string][]storage.ColumnPriv)
	for _, ccl := range remote.CCLs {
		remoteCCLs[ccl.TableName] = append(remoteCCLs[ccl.TableName], ccl)
	}
	localTable := make(map[string]storage.TableMeta)
	for _, tbl := range local.Tables {
		localTable[tbl.Table.TableName] = tbl
	}
	for _, rt := range remote.Tables {
		lt, ok := localTable[rt.Table.TableName]
		if ok {
			if application.GetTableChecksum(lt) != application.GetTableChecksum(rt) {
				tsToUpdate = append(tsToUpdate, rt)
			} else {
				lc := localCCLs[rt.Table.TableName]
				rc := remoteCCLs[rt.Table.TableName]
				if application.GetCCLsChecksum(lc) != application.GetCCLsChecksum(rc) {
					psToUpdate = append(psToUpdate, rc...)
				}
			}

			delete(localTable, rt.Table.TableName) // remove the table that exists locally and remotely
		} else {
			tsToUpdate = append(tsToUpdate, rt)
		}
	}

	// drop tables that exist locally but not remotely
	for _, tbl := range localTable {
		_, err = common.DropTableWithCheck(txn, proj.ID, remoteParty, storage.TableIdentifier{ProjectID: proj.ID, TableName: tbl.Table.TableName})
		if err != nil {
			return fmt.Errorf("updateStatus: drop local old tables err: %v", err)
		}
		logrus.Infof("updateStatus for project %s: drop table '%s', which was owned by %s but not existing now", proj.ID, tbl.Table.TableName, remoteParty)
	}

	// update table in storage
	for _, tbl := range tsToUpdate {
		// Drop table, then add table and ccl
		if _, ok := localTable[tbl.Table.TableName]; ok {
			_, err = common.DropTableWithCheck(txn, proj.ID, remoteParty, storage.TableIdentifier{ProjectID: proj.ID, TableName: tbl.Table.TableName})
			if err != nil {
				return fmt.Errorf("updateStatus: DropTableWithCheck: %v", err)
			}
		}
		err = common.AddTableWithCheck(txn, proj.ID, remoteParty, tbl)
		if err != nil {
			return fmt.Errorf("updateStatus: AddTableWithCheck: %v", err)
		}
		if len(remoteCCLs[tbl.Table.TableName]) > 0 {
			// skip owner check, which has been done in getRemoteStatus
			err = common.GrantColumnConstraintsWithCheck(txn, proj.ID, remoteCCLs[tbl.Table.TableName], common.OwnerChecker{SkipOwnerCheck: true})
			if err != nil {
				return fmt.Errorf("updateStatus: GrantColumnConstraintsWithCheck: %v", err)
			}
		}
		logrus.Infof("updateStatus for project %s: update table '%s'", proj.ID, tbl.Table.TableName)
	}

	// update ccl in storage
	if len(psToUpdate) > 0 {
		err = common.GrantColumnConstraintsWithCheck(txn, proj.ID, psToUpdate, common.OwnerChecker{SkipOwnerCheck: true})
		if err != nil {
			return fmt.Errorf("updateStatus: GrantColumnConstraintsWithCheck: %v", err)
		}
		logrus.Infof("updateStatus for project %s: update '%d' ccls", proj.ID, len(psToUpdate))
	}

	return nil
}
