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
	"fmt"
	"net/http"
	"net/url"

	"golang.org/x/exp/slices"
	"google.golang.org/protobuf/proto"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"

	"github.com/secretflow/scql/pkg/broker/application"
	"github.com/secretflow/scql/pkg/broker/storage"
	"github.com/secretflow/scql/pkg/constant"
	"github.com/secretflow/scql/pkg/parser"
	"github.com/secretflow/scql/pkg/planner/core"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/util/message"
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
		ret := <-retCh
		if ret != nil {
			err = fmt.Errorf("%v,{%v}", err, ret)
		}
	}

	return
}

// Only update the table that is included in the resources here to avoid side effects and no need to check privileges again
func askInfoAndUpdateStorage(session *application.Session, resources []*pb.ResourceSpec, targetParty string) (err error) {
	req := pb.AskInfoRequest{ClientId: &pb.PartyId{Code: session.GetSelfPartyCode()}, ResourceSpecs: resources}
	destUrl, err := session.PartyMgr.GetBrokerUrlByParty(targetParty)
	if err != nil {
		return err
	}
	resp := pb.AskInfoResponse{}
	err = session.ExecuteInfo.InterStub.AskInfo(destUrl, &req, &resp)
	if err != nil {
		return
	}
	// check resp ok
	if resp.Status == nil || resp.Status.Code != 0 {
		return fmt.Errorf("ask info failed: %+v", resp.Status)
	}
	txn := session.MetaMgr.CreateMetaTransaction()
	defer func() {
		txn.Finish(err)
	}()
	for i, resource := range resources {
		err = updateStorageFor(txn, resource, resp.Datas[i], targetParty)
		if err != nil {
			return
		}
	}
	return nil
}

func updateStorageFor(txn *storage.MetaTransaction, resource *pb.ResourceSpec, data []byte, targetParty string) (err error) {
	switch resource.Kind {
	case pb.ResourceSpec_Table:
		// Only update the table that is included in the resource
		var metas []storage.TableMeta
		err = json.Unmarshal(data, &metas)
		if err != nil {
			return
		}
		// remove all tables in the resource
		for _, tableName := range resource.TableNames {
			err = txn.DropTable(storage.TableIdentifier{ProjectID: resource.ProjectId, TableName: tableName})
			if err != nil {
				return
			}
		}
		// create tables
		for _, meta := range metas {
			// ignore table which is not in the resource
			if !slices.Contains(resource.TableNames, meta.Table.TableName) {
				continue
			}
			err = AddTableWithCheck(txn, resource.ProjectId, targetParty, meta)
			if err != nil {
				return
			}
		}
		return
	case pb.ResourceSpec_CCL:
		// only update columns owned by target party
		var columnPrivs []storage.ColumnPriv
		err = json.Unmarshal(data, &columnPrivs)
		if err != nil {
			return
		}
		// ignore column which is not in the resource
		var privs []storage.ColumnPriv
		for _, priv := range columnPrivs {
			if !slices.Contains(resource.TableNames, priv.TableName) {
				continue
			}
			privs = append(privs, priv)
		}
		err = GrantColumnConstraintsWithCheck(txn, resource.GetProjectId(), targetParty, privs)
		return
	default:
		return fmt.Errorf("unsupported resource type in ask info response: %+v", resource)
	}
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

func AskInfoByChecksumResult(session *application.Session, compRes pb.ChecksumCompareResult, tables []core.DbTable, targetParty string) (askInfoTriggerd bool, err error) {
	resources := createResourcesFrom(compRes, tables, session.ExecuteInfo.ProjectID, session.ExecuteInfo.WorkParties)
	if len(resources) != 0 {
		askInfoTriggerd = true
		err = askInfoAndUpdateStorage(session, resources, targetParty)
		if err != nil {
			return
		}
	}
	return askInfoTriggerd, nil
}

func VerifyTableMeta(meta storage.TableMeta) error {
	// 1. check DB type
	_, err := core.ParseDBType(meta.Table.DBType)
	if err != nil {
		return fmt.Errorf("VerifyTableMeta: %s", err)
	}
	// 2. check column type
	for _, col := range meta.Columns {
		if ok := constant.SupportTypes[col.DType]; !ok {
			return fmt.Errorf("VerifyTableMeta: unsupported data type %s", col.DType)
		}
	}
	// 3. check table/column name
	createFormat := `
	create table %s.%s (
		%s
	);
	`
	columnStr := ""
	for i := 0; i < len(meta.Columns); i++ {
		columnStr += fmt.Sprintf("%s int", meta.Columns[i].ColumnName)
		if i < len(meta.Columns)-1 {
			columnStr += ",\n"
		}
	}
	p := parser.New()
	_, err = p.ParseOneStmt(fmt.Sprintf(createFormat, meta.Table.ProjectID, meta.Table.TableName, columnStr), "", "")
	if err != nil {
		return fmt.Errorf("VerifyTableMeta: %s", err)
	}
	// 4. check ref table
	_, err = core.NewDbTableFromString(meta.Table.RefTable)
	if err != nil {
		return fmt.Errorf("VerifyTableMeta: %s", err)
	}
	return nil
}

func VerifyProjectID(projectID string) error {
	p := parser.New()
	// project id may work as db name
	_, err := p.ParseOneStmt(fmt.Sprintf("create database %s;", projectID), "", "")
	if err != nil {
		return fmt.Errorf("VerifyProjectID: %s", err)
	}
	return nil
}

func FeedResponse(c *gin.Context, response proto.Message, encodingType message.ContentEncodingType) {
	body, err := message.SerializeTo(response, encodingType)
	if err != nil {
		c.String(http.StatusInternalServerError, fmt.Sprintf("FeedResponse: unable to serialize response: %+v", response))
		return
	}
	c.String(http.StatusOK, body)
}
