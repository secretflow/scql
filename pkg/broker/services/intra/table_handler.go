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
	"errors"
	"fmt"
	"strings"

	"github.com/secretflow/scql/pkg/broker/services/common"
	"github.com/secretflow/scql/pkg/broker/storage"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
)

func (svc *grpcIntraSvc) CreateTable(c context.Context, req *pb.CreateTableRequest) (resp *pb.CreateTableResponse, err error) {
	if req == nil || req.GetProjectId() == "" || req.GetTableName() == "" || req.GetRefTable() == "" {
		return nil, errors.New("CreateTable: illegal request")
	}

	app := svc.app
	txn := app.MetaMgr.CreateMetaTransaction()
	defer func() {
		txn.Finish(err)
	}()

	var proj storage.Project
	proj, err = txn.GetProject(req.GetProjectId())
	if err != nil {
		return nil, fmt.Errorf("CreateTable: get project %v err: %v", req.GetProjectId(), err)
	}

	tables, tmpErr := txn.GetTablesByTableNames(req.GetProjectId(), []string{req.GetTableName()})
	if tmpErr == nil && len(tables) > 0 {
		return nil, fmt.Errorf("CreateTable: already exist table with same name: %+v", tables)
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
	for _, p := range strings.Split(proj.Member, ";") {
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
		return nil, errors.New("ListTables: illegal request")
	}

	app := svc.app
	txn := app.MetaMgr.CreateMetaTransaction()
	defer func() {
		txn.Finish(err)
	}()

	tables, err := txn.GetTablesByTableNames(req.GetProjectId(), req.GetNames())
	if err != nil {
		return nil, fmt.Errorf("ListTables: %v", err)
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
		return nil, errors.New("DropTable: illegal request")
	}

	app := svc.app
	txn := app.MetaMgr.CreateMetaTransaction()
	defer func() {
		txn.Finish(err)
	}()

	tableMetas, err := txn.GetTablesByTableNames(req.GetProjectId(), []string{req.GetTableName()})
	if err != nil || len(tableMetas) != 1 {
		return nil, fmt.Errorf("DropTable: get table err: %v", err)
	}
	if tableMetas[0].Table.Owner != app.Conf.PartyCode {
		return nil, fmt.Errorf("DropTable: cannot drop table without ownership")
	}

	tableId := storage.TableIdentifier{
		ProjectID: req.GetProjectId(),
		TableName: req.GetTableName(),
	}
	_, err = common.DropTableWithCheck(txn, req.GetProjectId(), app.Conf.PartyCode, tableId)
	if err != nil {
		return nil, fmt.Errorf("DropTable: DropTableWithCheck err %v", err)
	}

	// Sync to other parties
	proj, err := txn.GetProject(req.GetProjectId())
	if err != nil {
		return nil, fmt.Errorf("DropTable: GetProject: %v", err)
	}
	go func() {
		var targetParties []string
		for _, p := range strings.Split(proj.Member, ";") {
			if p != app.Conf.PartyCode {
				targetParties = append(targetParties, p)
			}
		}
		common.PostSyncInfo(svc.app, req.GetProjectId(), pb.ChangeEntry_DropTable, tableId, targetParties)
	}()

	return &pb.DropTableResponse{
		Status: &pb.Status{
			Code:    int32(0),
			Message: "drop table succeed",
		},
	}, nil

}
