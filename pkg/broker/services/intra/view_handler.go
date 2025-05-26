// Copyright 2025 Ant Group Co., Ltd.
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
	"time"

	"github.com/secretflow/scql/pkg/broker/application"
	"github.com/secretflow/scql/pkg/broker/executor"
	"github.com/secretflow/scql/pkg/broker/services/common"
	"github.com/secretflow/scql/pkg/broker/storage"
	"github.com/secretflow/scql/pkg/infoschema"
	"github.com/secretflow/scql/pkg/parser"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/planner/core"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/sessionctx"
	"github.com/secretflow/scql/pkg/sessionctx/stmtctx"
	"github.com/secretflow/scql/pkg/status"
)

func buildColumnDesc(lp core.Plan) ([]storage.ColumnMeta, error) {
	var columns []storage.ColumnMeta
	for i, field := range lp.OutputNames() {
		if field.ColName.String() == "" {
			return nil, fmt.Errorf("buildColumnDesc: %vth column name can not be empty", i)
		}

		t, err := infoschema.FieldTypeString(*lp.Schema().Columns[i].RetType)
		if err != nil {
			return nil, fmt.Errorf("buildColumnDesc: get column type error: %v", err)
		}
		columns = append(columns, storage.ColumnMeta{
			ColumnName: field.ColName.String(),
			DType:      t,
		})
	}

	return columns, nil
}

func buildPlan(txn *storage.MetaTransaction, session *application.Session, projectId string, selectionNode *ast.SelectStmt) (core.Plan, error) {
	r := executor.NewQueryRunner(session)

	usedTables, err := core.GetSourceTables(selectionNode.Text())
	if err != nil {
		return nil, fmt.Errorf("buildPlan: get used tables error: %v", err)
	}

	var tableNames []string
	for _, tbl := range usedTables {
		tableNames = append(tableNames, tbl.GetTableName())
	}

	tableEntries, notFound, err := txn.GetTableMetasByTableNames(projectId, tableNames)
	if err != nil {
		return nil, fmt.Errorf("buildPlan: get table metas error: %v", err)
	}
	if len(notFound) > 0 {
		return nil, fmt.Errorf("buildPlan: table not found: %v", notFound)
	}

	tableEntries, err = common.ExpandTablesInView(txn, projectId, tableEntries)
	if err != nil {
		return nil, fmt.Errorf("buildPlan: expand tables error: %v", err)
	}
	is, err := r.CreateInfoSchema(tableEntries)
	if err != nil {
		return nil, fmt.Errorf("buildPlan: create info schema error: %v", err)
	}
	sctx := sessionctx.NewContext()
	sctx.GetSessionVars().StmtCtx = &stmtctx.StatementContext{}
	sctx.GetSessionVars().PlanID = 0
	sctx.GetSessionVars().PlanColumnID = 0
	sctx.GetSessionVars().CurrentDB = projectId

	lp, _, err := core.BuildLogicalPlan(context.Background(), sctx, selectionNode, is)
	return lp, err
}

func (svc *grpcIntraSvc) CreateView(ctx context.Context, req *pb.CreateViewRequest) (resp *pb.CreateViewResponse, err error) {
	if req == nil || req.ViewName == "" || req.Query == "" || req.ProjectId == "" {
		return nil, fmt.Errorf("CreateView: invalid request")
	}
	p := parser.New()
	stmt, err := p.ParseOneStmt(req.GetQuery(), "", "")
	if err != nil {
		return nil, fmt.Errorf("CreateView: parse query error: %v", err)
	}

	selectionNode, ok := stmt.(*ast.SelectStmt)
	if !ok {
		return nil, fmt.Errorf("CreateView: query is not a select statement, only support query like 'SELECT ... FROM ..'")
	}
	app := svc.app
	info := &application.ExecutionInfo{
		ProjectID: req.GetProjectId(),
		JobID:     "",
		Query:     req.GetQuery(),
		Issuer: &pb.PartyId{
			Code: app.Conf.PartyCode,
		},
		EngineClient:   app.EngineClient,
		DebugOpts:      nil,
		SessionOptions: &application.SessionOptions{},
		CreatedAt:      time.Now(),
	}
	session, err := application.NewSession(ctx, info, app, false, true)
	txn := session.App.MetaMgr.CreateMetaTransaction()
	defer func() {
		err = txn.Finish(err)
	}()

	resp, err = common.CheckProjectArchived[pb.CreateViewResponse](txn, req.GetProjectId(), "CreateView")
	if err != nil || resp != nil {
		return resp, err
	}

	tables, err := txn.GetAllTables(req.GetProjectId())
	if err != nil {
		return nil, fmt.Errorf("CreateView: %s", err.Error())
	}

	for _, tbl := range tables {
		if tbl.TableName == req.GetViewName() {
			return nil, fmt.Errorf("CreateView: entity %s already exists", req.GetViewName())
		}
	}

	lp, err := buildPlan(txn, session, req.GetProjectId(), selectionNode)
	if err != nil {
		return nil, fmt.Errorf("CreateView: build logical plan error: %v", err)
	}

	columns, err := buildColumnDesc(lp)
	if err != nil {
		return nil, fmt.Errorf("CreateView: build column desc error: %v", err)
	}
	tableMeta := storage.TableMeta{
		Table: storage.Table{
			TableIdentifier: storage.TableIdentifier{
				ProjectID: req.GetProjectId(),
				TableName: req.GetViewName(),
			},
			RefTable:     "",
			Owner:        app.Conf.PartyCode,
			IsView:       true,
			SelectString: selectionNode.Text()},
		Columns: columns,
	}

	err = common.AddTableWithCheck(txn, req.GetProjectId(), app.Conf.PartyCode, tableMeta)
	if err != nil {
		return nil, fmt.Errorf("CreateView: AddTableWithCheck err: %v", err)
	}

	members, err := txn.GetProjectMembers(req.GetProjectId())
	if err != nil {
		return nil, fmt.Errorf("CreateView: get project %v err: %v", req.GetProjectId(), err)
	}
	// Sync Info to other parties
	var targetParties []string
	for _, p := range members {
		if p != app.Conf.PartyCode {
			targetParties = append(targetParties, p)
		}
	}
	err = common.PostSyncInfo(svc.app, req.GetProjectId(), pb.ChangeEntry_CreateTable, tableMeta, targetParties)
	if err != nil {
		rollbackErr := common.PostSyncInfo(svc.app, req.GetProjectId(), pb.ChangeEntry_DropTable,
			storage.TableIdentifier{ProjectID: req.GetProjectId(), TableName: req.GetViewName()},
			targetParties)
		if rollbackErr != nil {
			return nil, fmt.Errorf("CreateView: failed to sync: %v; rollback err: %v", err, rollbackErr)
		}

		return nil, fmt.Errorf("CreateView: failed to sync: %v", err)
	}

	return &pb.CreateViewResponse{
		Status: &pb.Status{
			Code:    int32(0),
			Message: fmt.Sprintf("create view for %v succeed", req.GetProjectId()),
		}}, nil
}

func (svc *grpcIntraSvc) DropView(ctx context.Context, req *pb.DropViewRequest) (resp *pb.DropViewResponse, err error) {
	if req == nil || req.GetProjectId() == "" || req.GetViewName() == "" {
		return nil, status.New(pb.Code_BAD_REQUEST, "DropView: illegal request")
	}

	app := svc.app
	txn := app.MetaMgr.CreateMetaTransaction()
	defer func() {
		err = txn.Finish(err)
	}()

	resp, err = common.CheckProjectArchived[pb.DropViewResponse](txn, req.GetProjectId(), "DropView")
	if err != nil || resp != nil {
		return resp, err
	}

	dropTableReq := &pb.DropTableRequest{
		ProjectId: req.GetProjectId(),
		TableName: req.GetViewName(),
	}
	dropTableResp, err := svc.DropTable(ctx, dropTableReq)
	if err != nil {
		return nil, fmt.Errorf("DropView: drop table err: %v", err)
	}

	if dropTableResp.Status == nil {
		return nil, fmt.Errorf("DropView: status is nil")
	}
	if dropTableResp.GetStatus().GetCode() == 0 {
		return &pb.DropViewResponse{
			Status: &pb.Status{
				Code:    int32(0),
				Message: "drop view succeed",
			},
		}, nil
	} else {
		return &pb.DropViewResponse{
			Status: &pb.Status{
				Code:    dropTableResp.GetStatus().GetCode(),
				Message: dropTableResp.GetStatus().GetMessage(),
			},
		}, nil
	}
}

func (svc *grpcIntraSvc) ListViews(ctx context.Context, req *pb.ListViewsRequest) (resp *pb.ListViewsResponse, err error) {
	if req == nil || req.GetProjectId() == "" {
		return nil, status.New(pb.Code_BAD_REQUEST, "ListViews: illegal request")
	}

	app := svc.app
	txn := app.MetaMgr.CreateMetaTransaction()
	defer func() {
		err = txn.Finish(err)
	}()
	_, err = txn.GetProject(req.GetProjectId())
	if err != nil {
		return nil, fmt.Errorf("ListViews: GetProject err: %v", err)
	}
	tables, notFoundTables, err := txn.GetTableMetasByTableNames(req.GetProjectId(), req.GetNames())
	if err != nil {
		return nil, fmt.Errorf("ListViews: %v", err)
	}
	if len(notFoundTables) > 0 {
		return nil, fmt.Errorf("ListViews: view %v not found", notFoundTables)
	}
	var viewList []*pb.TableMeta
	for _, table := range tables {
		if !table.Table.IsView {
			continue
		}
		var columns []*pb.TableMeta_Column
		for _, column := range table.Columns {
			columns = append(columns, &pb.TableMeta_Column{
				Name:  column.ColumnName,
				Dtype: column.DType,
			})
		}
		tbl := table.Table
		viewList = append(viewList, &pb.TableMeta{
			TableName:    tbl.TableName,
			RefTable:     tbl.RefTable,
			DbType:       tbl.DBType,
			TableOwner:   tbl.Owner,
			Columns:      columns,
			SelectString: table.Table.SelectString,
		})
	}

	return &pb.ListViewsResponse{
		Status: &pb.Status{
			Code:    int32(0),
			Message: "list tables succeed",
		},
		Views: viewList,
	}, nil
}
