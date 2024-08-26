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

package common

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"

	"golang.org/x/exp/slices"

	"github.com/sirupsen/logrus"

	"github.com/secretflow/scql/pkg/broker/application"
	"github.com/secretflow/scql/pkg/broker/storage"
	"github.com/secretflow/scql/pkg/planner/core"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
)

func PostSyncInfo(app *application.App, projectID string, action pb.ChangeEntry_Action, data any, targetParties []string) (err error) {
	if len(targetParties) == 0 {
		return
	}

	dataBytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("PostSyncInfo: marshal: %v", err)
	}

	retCh := make(chan error, len(targetParties))
	for _, p := range targetParties {
		logrus.Infof("PostSyncInfo: sync info to party %s, sync type %s", p, action.String())
		go func(p string) {
			syncReq := &pb.SyncInfoRequest{
				ClientId: &pb.PartyId{
					Code: app.Conf.PartyCode,
				},
				ProjectId: projectID,
				ChangeEntry: &pb.ChangeEntry{
					Action: action,
					Data:   dataBytes,
				},
			}
			targetUrl, err := app.PartyMgr.GetBrokerUrlByParty(p)
			if err != nil {
				retCh <- fmt.Errorf("PostSyncInfo: %v", err)
				return
			}
			response := &pb.SyncInfoResponse{}
			err = app.InterStub.SyncInfo(targetUrl, syncReq, response)
			if err != nil {
				if urlErr, ok := err.(*url.Error); ok && urlErr.Timeout() {
					// we ignore timeout for syncInfo
					logrus.Warnf("PostSyncInfo ignore http timeout err: %v", urlErr)
					retCh <- nil
					return
				}
				retCh <- fmt.Errorf("PostSyncInfo to %v: %v", p, err)
				return
			}
			if response.GetStatus().GetCode() != 0 {
				retCh <- fmt.Errorf("PostSyncInfo to %v: status: %+v", p, response.Status)
				return
			}
			retCh <- nil
		}(p)
	}

	for i := 0; i < len(targetParties); i++ {
		err = errors.Join(err, <-retCh)
	}

	return
}

// use this function if you know table owner
func AskInfoByChecksumResult(session *application.Session, compRes pb.ChecksumCompareResult, tables []core.DbTable, targetParty string) (askInfoTriggered bool, err error) {
	resources := createResourcesFrom(compRes, tables, session.ExecuteInfo.ProjectID, session.ExecuteInfo.WorkParties)
	if len(resources) != 0 {
		logrus.Infof("ask info from party %s for resources %+v in project %s", targetParty, resources, session.ExecuteInfo.ProjectID)
		askInfoTriggered = true
		updater := InfoAskerAndUpdater{sourceParties: []string{targetParty}, app: session.App, resources: resources}
		err = updater.askInfoAndUpdateStorage()
		if err != nil {
			return
		}
	}
	return askInfoTriggered, nil
}

// use this function if you don't know table owner and set source parties as project members except self
func AskProjectInfoFromParties(app *application.App, projectID string, tableNames []string, cclDestParties []string, sourceParties []string) (err error) {
	resource := &pb.ResourceSpec{Kind: pb.ResourceSpec_All, ProjectId: projectID, DestParties: cclDestParties}
	for _, tableName := range tableNames {
		resource.TableNames = append(resource.TableNames, tableName)
	}
	updater := InfoAskerAndUpdater{sourceParties: sourceParties, app: app, resources: []*pb.ResourceSpec{resource}}
	return updater.askInfoAndUpdateStorage()
}

func createResourcesFrom(checksumResult pb.ChecksumCompareResult, tables []core.DbTable, projectID string, workParties []string) (resources []*pb.ResourceSpec) {
	if checksumResult == pb.ChecksumCompareResult_EQUAL {
		return
	}
	if checksumResult == pb.ChecksumCompareResult_TABLE_SCHEMA_NOT_EQUAL || checksumResult == pb.ChecksumCompareResult_TABLE_CCL_NOT_EQUAL {
		resource := pb.ResourceSpec{Kind: pb.ResourceSpec_Table, ProjectId: projectID}
		for _, dt := range tables {
			resource.TableNames = append(resource.TableNames, dt.GetTableName())
		}
		resources = append(resources, &resource)
	}

	resource := pb.ResourceSpec{Kind: pb.ResourceSpec_CCL, ProjectId: projectID}
	for _, dt := range tables {
		resource.TableNames = append(resource.TableNames, dt.GetTableName())
	}
	resource.DestParties = workParties
	resources = append(resources, &resource)
	return
}

type InfoAskerAndUpdater struct {
	sourceParties   []string
	app             *application.App
	meta            storage.ProjectMeta
	foundTableNames []string
	resources       []*pb.ResourceSpec
}

func (updater *InfoAskerAndUpdater) updateTableMeta(metas []storage.TableMeta, sourceParty string) (err error) {
	tableNameToOwner := make(map[string]string)
	for _, meta := range metas {
		if slices.Contains(updater.foundTableNames, meta.Table.TableName) {
			return fmt.Errorf("duplicated table name %s from party %s and %s", meta.Table.TableName, tableNameToOwner[meta.Table.TableName], meta.Table.Owner)
		}
		if meta.Table.Owner != sourceParty {
			return fmt.Errorf("table %s not owned by %s", meta.Table.TableName, sourceParty)
		}
		updater.foundTableNames = append(updater.foundTableNames, meta.Table.TableName)
		updater.meta.Tables = append(updater.meta.Tables, meta)
		tableNameToOwner[meta.Table.TableName] = meta.Table.Owner
	}
	return nil
}

func (updater *InfoAskerAndUpdater) updateMetaFromData(datas [][]byte, sourceParty string) (err error) {
	for i, resource := range updater.resources {
		switch resource.Kind {
		case pb.ResourceSpec_Table:
			// Only update the table that is included in the resource
			var metas []storage.TableMeta
			err = json.Unmarshal(datas[i], &metas)
			if err != nil {
				return
			}
			err = updater.updateTableMeta(metas, sourceParty)
		case pb.ResourceSpec_CCL:
			var ccls []storage.ColumnPriv
			err = json.Unmarshal(datas[i], &ccls)
			if err != nil {
				return
			}
			updater.meta.CCLs = ccls
		case pb.ResourceSpec_All:
			// Only update the table that is included in the resource
			meta := storage.ProjectMeta{}
			err = json.Unmarshal(datas[i], &meta)
			if err != nil {
				return
			}
			err = updater.updateTableMeta(meta.Tables, sourceParty)
			if err != nil {
				return
			}
			tableNameToOwner := make(map[string]string)
			for _, meta := range updater.meta.Tables {
				tableNameToOwner[meta.Table.TableName] = meta.Table.Owner
			}
			for _, ccl := range meta.CCLs {
				if tableNameToOwner[ccl.TableName] != sourceParty {
					return fmt.Errorf("ccl for table %s is not owned by %s", ccl.TableName, sourceParty)
				}
				updater.meta.CCLs = append(updater.meta.CCLs, ccl)
			}
			logrus.Infof("updater.meta.CCLs: %+v", updater.meta.CCLs)
		default:
			return fmt.Errorf("unsupported resource type in ask info response: %+v", resource)
		}
		if err != nil {
			return
		}
	}
	return nil
}

func (updater *InfoAskerAndUpdater) updateTableMetaInStorage(txn *storage.MetaTransaction, resource *pb.ResourceSpec) (err error) {
	// TODO: fix scenario where len(resource.TableNames) == 0
	for _, tableName := range resource.TableNames {
		err := txn.DropTable(storage.TableIdentifier{ProjectID: resource.ProjectId, TableName: tableName})
		if err != nil {
			return err
		}
	}
	// create tables
	for _, meta := range updater.meta.Tables {
		// ignore table which is not in the resource
		if !slices.Contains(resource.TableNames, meta.Table.TableName) {
			continue
		}
		err = AddTableWithCheck(txn, resource.ProjectId, meta.Table.Owner, meta)
		if err != nil {
			return
		}
	}
	return nil
}

func (updater *InfoAskerAndUpdater) updateStorage(txn *storage.MetaTransaction) (err error) {
	for _, resource := range updater.resources {
		switch resource.Kind {
		case pb.ResourceSpec_Table:
			err = updater.updateTableMetaInStorage(txn, resource)
		case pb.ResourceSpec_CCL:
			err = GrantColumnConstraintsWithCheck(txn, resource.GetProjectId(), updater.meta.CCLs, OwnerChecker{Owner: updater.sourceParties[0]})
		case pb.ResourceSpec_All:
			// TODO: fix scenario where len(resource.TableNames) == 0
			err = updater.updateTableMetaInStorage(txn, resource)
			if err != nil {
				return
			}
			err = GrantColumnConstraintsWithCheck(txn, resource.GetProjectId(), updater.meta.CCLs, OwnerChecker{SkipOwnerCheck: true})
		default:
			return fmt.Errorf("unsupported resource type in ask info response: %+v", resource)
		}
		if err != nil {
			return
		}
	}
	return nil
}

func (updater *InfoAskerAndUpdater) askInfoAndUpdateStorage() error {
	app := updater.app
	type askInfoResult struct {
		resource    []*pb.ResourceSpec
		sourceParty string
		data        [][]byte
		err         error
	}
	// double check
	if len(updater.sourceParties) == 0 {
		return fmt.Errorf("askInfoAndUpdateStorage: no source party")
	}
	dataCh := make(chan askInfoResult, len(updater.sourceParties))
	for _, sourceParty := range updater.sourceParties {
		go func(p string) {
			for _, resource := range updater.resources {
				// ask ccl from other party meaning that you already got the table then you knew the table owner
				if resource.Kind == pb.ResourceSpec_CCL {
					if len(updater.sourceParties) != 1 {
						dataCh <- askInfoResult{nil, p, nil, fmt.Errorf("ask info only can get CCL from one party but got: %+v", updater.sourceParties)}
						return
					}
				}
			}
			req := pb.AskInfoRequest{ClientId: &pb.PartyId{Code: app.Conf.PartyCode}, ResourceSpecs: updater.resources}
			destUrl, err := app.PartyMgr.GetBrokerUrlByParty(p)
			if err != nil {
				dataCh <- askInfoResult{nil, p, nil, err}
				return
			}
			resp := pb.AskInfoResponse{}
			err = app.InterStub.AskInfo(destUrl, &req, &resp)
			if err != nil {
				dataCh <- askInfoResult{nil, p, nil, err}
				return
			}
			// check resp ok
			if resp.Status == nil || resp.Status.Code != 0 {
				dataCh <- askInfoResult{nil, p, nil, fmt.Errorf("ask info failed: %+v", resp.Status)}
				return
			}
			dataCh <- askInfoResult{updater.resources, p, resp.Datas, nil}
		}(sourceParty)
	}
	for range updater.sourceParties {
		result := <-dataCh
		if result.err != nil {
			logrus.Infof("ask info from %s failed: %v", result.sourceParty, result.err)
			continue
		}
		if err := updater.updateMetaFromData(result.data, result.sourceParty); err != nil {
			return err
		}
	}

	return app.MetaMgr.ExecInMetaTransaction(updater.updateStorage)
}
