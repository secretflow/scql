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

	"github.com/secretflow/scql/pkg/broker/services/common"
	"github.com/secretflow/scql/pkg/broker/storage"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/status"
	"github.com/secretflow/scql/pkg/util/sliceutil"
)

func (svc *grpcIntraSvc) CreateTable(c context.Context, req *pb.CreateTableRequest) (resp *pb.CreateTableResponse, err error) {
	if req == nil || req.GetProjectId() == "" || req.GetTableName() == "" || req.GetRefTable() == "" {
		return nil, status.New(pb.Code_BAD_REQUEST, "CreateTable: illegal request")
	}

	app := svc.app
	txn := app.MetaMgr.CreateMetaTransaction()
	defer func() {
		err = txn.Finish(err)
	}()

	members, err := txn.GetProjectMembers(req.GetProjectId())
	if err != nil {
		return nil, fmt.Errorf("CreateTable: get project %v err: %v", req.GetProjectId(), err)
	}
	tables, exist, err := txn.GetTables(req.GetProjectId(), []string{req.GetTableName()})
	if err != nil {
		return nil, fmt.Errorf("CreateTable: %s", err.Error())
	}
	if exist {
		return nil, fmt.Errorf("CreateTable: table %v already exists owned by %s", req.GetTableName(), tables[0].Owner)
	}

	var columns []storage.ColumnMeta
	for _, col := range req.GetColumns() {
		columns = append(columns, storage.ColumnMeta{
			ColumnName: col.GetName(),
			DType:      col.GetDtype(),
		})
	}
	tableMeta := storage.TableMeta{
		Table: storage.Table{
			TableIdentifier: storage.TableIdentifier{
				ProjectID: req.GetProjectId(),
				TableName: req.GetTableName(),
			},
			DBType:   req.GetDbType(),
			RefTable: req.GetRefTable(),
			Owner:    app.Conf.PartyCode},
		Columns: columns,
	}
	err = common.VerifyTableMeta(tableMeta)
	if err != nil {
		return nil, fmt.Errorf("CreateTable: Table schema err: %v", err)
	}
	err = common.AddTableWithCheck(txn, req.GetProjectId(), app.Conf.PartyCode, tableMeta)
	if err != nil {
		return nil, fmt.Errorf("CreateTable: AddTableWithCheck err: %v", err)
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
		// NOTICE: if sync failed, rollback operations are performed to prevent parties from creating tables with the same name at the same time:
		//    1) the AddTable will be rolled back by MetaTransaction
		//    2) call DropTable for peers to roll back, since some peers may successfully create the table
		rollbackErr := common.PostSyncInfo(svc.app, req.GetProjectId(), pb.ChangeEntry_DropTable,
			storage.TableIdentifier{ProjectID: req.GetProjectId(), TableName: req.GetTableName()},
			targetParties)
		if rollbackErr != nil {
			return nil, fmt.Errorf("CreateTable: failed to sync: %v; rollback err: %v", err, rollbackErr)
		}

		return nil, fmt.Errorf("CreateTable: failed to sync: %v", err)
	}

	return &pb.CreateTableResponse{
		Status: &pb.Status{
			Code:    int32(0),
			Message: fmt.Sprintf("create table for %v succeed", req.GetProjectId()),
		}}, nil

}

func (svc *grpcIntraSvc) ListTables(ctx context.Context, req *pb.ListTablesRequest) (resp *pb.ListTablesResponse, err error) {
	if req == nil || req.GetProjectId() == "" {
		return nil, status.New(pb.Code_BAD_REQUEST, "ListTables: illegal request")
	}

	app := svc.app
	txn := app.MetaMgr.CreateMetaTransaction()
	defer func() {
		err = txn.Finish(err)
	}()
	_, err = txn.GetProject(req.GetProjectId())
	if err != nil {
		return nil, fmt.Errorf("ListTables: GetProject err: %v", err)
	}
	tables, notFoundTables, err := txn.GetTableMetasByTableNames(req.GetProjectId(), req.GetNames())
	if err != nil {
		return nil, fmt.Errorf("ListTables: %v", err)
	}
	if len(notFoundTables) > 0 {
		return nil, fmt.Errorf("ListTables: table %v not found", notFoundTables)
	}
	var tableList []*pb.TableMeta
	for _, table := range tables {
		var columns []*pb.TableMeta_Column
		for _, column := range table.Columns {
			columns = append(columns, &pb.TableMeta_Column{
				Name:  column.ColumnName,
				Dtype: column.DType,
			})
		}
		tbl := table.Table
		tableList = append(tableList, &pb.TableMeta{
			TableName:  tbl.TableName,
			RefTable:   tbl.RefTable,
			DbType:     tbl.DBType,
			TableOwner: tbl.Owner,
			Columns:    columns,
		})
	}

	return &pb.ListTablesResponse{
		Status: &pb.Status{
			Code:    int32(0),
			Message: "list tables succeed",
		},
		Tables: tableList,
	}, nil
}

func (svc *grpcIntraSvc) DropTable(c context.Context, req *pb.DropTableRequest) (resp *pb.DropTableResponse, err error) {
	if req == nil || req.GetProjectId() == "" || req.GetTableName() == "" {
		return nil, status.New(pb.Code_BAD_REQUEST, "DropTable: illegal request")
	}

	app := svc.app
	txn := app.MetaMgr.CreateMetaTransaction()
	defer func() {
		err = txn.Finish(err)
	}()

	tableId := storage.TableIdentifier{
		ProjectID: req.GetProjectId(),
		TableName: req.GetTableName(),
	}
	exist, err := common.DropTableWithCheck(txn, req.GetProjectId(), app.Conf.PartyCode, tableId)
	if err != nil {
		return nil, fmt.Errorf("DropTable: DropTableWithCheck err %v", err)
	}
	if !exist {
		return nil, fmt.Errorf("DropTable: table %s not found", tableId.TableName)
	}
	// Sync to other parties
	members, err := txn.GetProjectMembers(req.GetProjectId())
	if err != nil {
		return nil, fmt.Errorf("DropTable: GetProject: %v", err)
	}
	go func() {
		targetParties := sliceutil.Subtraction(members, []string{app.Conf.PartyCode})
		common.PostSyncInfo(svc.app, req.GetProjectId(), pb.ChangeEntry_DropTable, tableId, targetParties)
	}()

	return &pb.DropTableResponse{
		Status: &pb.Status{
			Code:    int32(0),
			Message: "drop table succeed",
		},
	}, nil

}
