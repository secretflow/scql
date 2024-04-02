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
	"fmt"
	"strings"

	"golang.org/x/exp/slices"

	"github.com/sirupsen/logrus"

	"github.com/secretflow/scql/pkg/broker/storage"
	"github.com/secretflow/scql/pkg/util/sliceutil"
)

// return map[table name]map[lower(column name)]column name
func createMapLowerToOriginColName(tableMetas []storage.TableMeta) map[string]map[string]string {
	colMap := make(map[string]map[string]string)
	for _, meta := range tableMetas {
		if _, ok := colMap[meta.Table.TableName]; !ok {
			colMap[meta.Table.TableName] = make(map[string]string)
		}
		for _, col := range meta.Columns {
			colMap[meta.Table.TableName][strings.ToLower(col.ColumnName)] = col.ColumnName
		}
	}
	return colMap
}

type OwnerChecker struct {
	Owner          string
	SkipOwnerCheck bool
}

func GrantColumnConstraintsWithCheck(t *storage.MetaTransaction, projectID string, privs []storage.ColumnPriv, columnOwnerChecker OwnerChecker) error {
	// split info in privs
	privMap := make(map[string][]string)
	var toParties []string
	var tableNames []string
	for _, priv := range privs {
		if priv.ProjectID != projectID {
			return fmt.Errorf("GrantColumnConstraintsWithCheck: grant ccl to columns in project %s, expect %s", priv.ProjectID, projectID)
		}
		privMap[priv.TableName] = append(privMap[priv.TableName], priv.ColumnName)
		toParties = append(toParties, priv.DestParty)
		tableNames = append(tableNames, priv.TableName)
	}
	toParties = sliceutil.SliceDeDup(toParties)
	// check project
	members, err := t.GetProjectMembers(projectID)
	if err != nil {
		return fmt.Errorf("GrantColumnConstraintsWithCheck: %s", err)
	}
	// check members
	if !sliceutil.ContainsAll(members, toParties) {
		return fmt.Errorf("GrantColumnConstraintsWithCheck: project members %+v not found", sliceutil.Subtraction(toParties, members))
	}
	tableMetas, notFoundTables, err := t.GetTableMetasByTableNames(projectID, sliceutil.SliceDeDup(tableNames))
	if err != nil {
		return fmt.Errorf("GrantColumnConstraintsWithCheck: %s", err)
	}
	if len(notFoundTables) > 0 {
		return fmt.Errorf("GrantColumnConstraintsWithCheck: table %+v not found", notFoundTables)
	}
	// check table meta
	for _, tableMeta := range tableMetas {
		// check table owner if party is not empty string
		if !columnOwnerChecker.SkipOwnerCheck && tableMeta.Table.Owner != columnOwnerChecker.Owner {
			return fmt.Errorf("GrantColumnConstraintsWithCheck: grant ccl to columns in table %s by %s, but owner is %s", tableMeta.Table.TableName, columnOwnerChecker.Owner, tableMeta.Table.Owner)
		}
	}
	// transfer column case
	var newPrivs []storage.ColumnPriv
	colMap := createMapLowerToOriginColName(tableMetas)
	for _, priv := range privs {
		if _, ok := colMap[priv.TableName]; !ok {
			return fmt.Errorf("GrantColumnConstraintsWithCheck: unknown table %s", priv.TableName)
		}
		newColName, ok := colMap[priv.TableName][strings.ToLower(priv.ColumnName)]
		if !ok {
			return fmt.Errorf("GrantColumnConstraintsWithCheck: unknown column name %s in table %s", priv.ColumnName, priv.TableName)
		}
		priv.ColumnName = newColName
		newPrivs = append(newPrivs, priv)
	}
	return t.GrantColumnConstraints(newPrivs)
}

func RevokeColumnConstraintsWithCheck(t *storage.MetaTransaction, projectID, party string, privIDs []storage.ColumnPrivIdentifier) error {
	var tableNames []string
	// check project
	for _, priv := range privIDs {
		if priv.ProjectID != projectID {
			return fmt.Errorf("RevokeColumnConstraintsWithCheck: grant ccl to columns in project %s, expect %s", priv.ProjectID, projectID)
		}
		tableNames = append(tableNames, priv.TableName)
	}
	// check owner
	tableMetas, notFoundTables, err := t.GetTableMetasByTableNames(projectID, sliceutil.SliceDeDup(tableNames))
	if err != nil {
		return fmt.Errorf("RevokeColumnConstraintsWithCheck: %s", err)
	}
	if len(notFoundTables) > 0 {
		return fmt.Errorf("RevokeColumnConstraintsWithCheck: table %+v not found", notFoundTables)
	}
	// check table meta
	for _, tableMeta := range tableMetas {
		if tableMeta.Table.Owner != party {
			return fmt.Errorf("RevokeColumnConstraintsWithCheck: revoke ccl in table %s by %s, but owner is %s", tableMeta.Table.TableName, party, tableMeta.Table.Owner)
		}
	}
	colMap := createMapLowerToOriginColName(tableMetas)
	var newPrivIDs []storage.ColumnPrivIdentifier
	for _, privID := range privIDs {
		if _, ok := colMap[privID.TableName]; !ok {
			return fmt.Errorf("RevokeColumnConstraintsWithCheck: unknown table %s", privID.TableName)
		}
		newColName, ok := colMap[privID.TableName][strings.ToLower(privID.ColumnName)]
		if !ok {
			return fmt.Errorf("RevokeColumnConstraintsWithCheck: unknown column name %s in table %s", privID.ColumnName, privID.TableName)
		}
		privID.ColumnName = newColName
		newPrivIDs = append(newPrivIDs, privID)
	}
	err = t.RevokeColumnConstraints(newPrivIDs)
	if err != nil {
		return fmt.Errorf("RevokeColumnConstraintsWithCheck: %v", err)
	}
	return nil
}

func AddTableWithCheck(t *storage.MetaTransaction, projectID, party string, table storage.TableMeta) error {
	// check owner
	if table.Table.Owner != party {
		return fmt.Errorf("AddTableWithCheck: table %s is not owned by %s", table.Table.TableName, party)
	}
	// check project
	if table.Table.ProjectID != projectID {
		return fmt.Errorf("AddTableWithCheck: table %s is not in project %s", table.Table.TableName, projectID)
	}

	// check column case
	var lowerColumnNames []string
	for _, col := range table.Columns {
		lowerColumnNames = append(lowerColumnNames, strings.ToLower(col.ColumnName))
	}

	if len(lowerColumnNames) != len(sliceutil.SliceDeDup(lowerColumnNames)) {
		return fmt.Errorf("AddTableWithCheck: duplicate column names in table %s", table.Table.TableName)
	}

	members, err := t.GetProjectMembers(projectID)
	if err != nil {
		return fmt.Errorf("AddTableWithCheck: GetProject %v", err)
	}
	if !slices.Contains(members, party) {
		return fmt.Errorf("AddTableWithCheck: party %s is not in project %s", party, projectID)
	}

	// if table already exists, check whether the owner is the same.
	owners, err := t.ListDedupTableOwners(table.Table.ProjectID, []string{table.Table.TableName})
	if err != nil {
		return fmt.Errorf("AddTableWithCheck: ListDedupTableOwners %v", err)
	}
	if len(owners) > 1 {
		return fmt.Errorf("AddTableWithCheck: table %s owned by more than one party %+v", table.Table.TableName, owners)
	}
	if len(owners) == 1 {
		if owners[0] == table.Table.Owner {
			logrus.Warnf("AddTableWithCheck: already exist, dropping table %s first", table.Table.TableName)
			err := t.DropTable(table.Table.TableIdentifier)
			if err != nil {
				return fmt.Errorf("AddTableWithCheck: drop existing table err: %v", err)
			}
		} else {
			return fmt.Errorf("AddTableWithCheck: existing table owner{%s} not equal to {%s}", owners[0], table.Table.Owner)
		}
	}
	err = t.AddTable(table)
	if err != nil {
		return fmt.Errorf("AddTableWithCheck: AddTable %v", err)
	}
	return nil
}

// if table not exist, return false, nil
// if table exist and drop it successfully return true, nil
func DropTableWithCheck(t *storage.MetaTransaction, projectID, party string, tableId storage.TableIdentifier) (exist bool, err error) {
	if tableId.ProjectID != projectID {
		return false, fmt.Errorf("DropTableWithCheck: table %s is not in project %s", tableId.TableName, projectID)
	}
	_, exist, err = t.GetTables(tableId.ProjectID, []string{tableId.TableName})
	if err != nil {
		return false, fmt.Errorf("DropTableWithCheck: get table err: %v", err)
	}
	if !exist {
		return false, nil
	}
	// check project and owner
	owners, err := t.ListDedupTableOwners(projectID, []string{tableId.TableName})
	if err != nil {
		return false, fmt.Errorf("DropTableWithCheck: get owner err: %v", err)
	}
	if len(owners) != 1 || owners[0] != party {
		return false, fmt.Errorf("DropTableWithCheck: table %s is not owned by %s", tableId.TableName, party)
	}
	// table exist then drop it
	err = t.DropTable(tableId)
	if err != nil {
		return true, fmt.Errorf("DropTableWithCheck: %v", err)
	}
	return true, nil
}
