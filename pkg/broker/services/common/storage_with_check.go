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

	"github.com/secretflow/scql/pkg/broker/storage"
	"github.com/secretflow/scql/pkg/util/sliceutil"
)

func GrantColumnConstraintsWithCheck(t *storage.MetaTransaction, projectID, party string, privs []storage.ColumnPriv) error {
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
	proj, err := t.GetProject(projectID)
	if err != nil {
		return fmt.Errorf("GrantColumnConstraintsWithCheck: %s", err)
	}
	members := strings.Split(proj.Member, ";")
	// check members
	if !sliceutil.SubSet(members, toParties) {
		return fmt.Errorf("GrantColumnConstraintsWithCheck: grant ccl to parties %s, but project only has %s", toParties, members)
	}
	tableMetas, err := t.GetTablesByTableNames(projectID, sliceutil.SliceDeDup(tableNames))
	if err != nil {
		return fmt.Errorf("GrantColumnConstraintsWithCheck: %s", err)
	}
	// check table meta
	for _, tableMeta := range tableMetas {
		if tableMeta.Table.Owner != party {
			return fmt.Errorf("GrantColumnConstraintsWithCheck: grant ccl to columns in table %s by %s, but owner is %s", tableMeta.Table.TableName, party, tableMeta.Table.Owner)
		}
		var colNames []string
		for _, colMeta := range tableMeta.Columns {
			colNames = append(colNames, colMeta.ColumnName)
		}
		if !sliceutil.SubSet(colNames, privMap[tableMeta.Table.TableName]) {
			return fmt.Errorf("GrantColumnConstraintsWithCheck: grant ccl to columns %+v, but table columns %+v does not have these columns", privMap[tableMeta.Table.TableName], colNames)
		}
	}
	return t.GrantColumnConstraints(privs)
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
	owners, err := t.ListDedupTableOwners(sliceutil.SliceDeDup(tableNames))
	if err != nil {
		return fmt.Errorf("RevokeColumnConstraintsWithCheck: %s", err)
	}
	if len(owners) != 1 || owners[0] != party {
		return fmt.Errorf("RevokeColumnConstraintsWithCheck: privs contain tables whose owner expect %s but %+v", party, owners)
	}
	err = t.RevokeColumnConstraints(privIDs)
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
	proj, err := t.GetProject(projectID)
	if err != nil {
		return fmt.Errorf("AddTableWithCheck: GetProject %v", err)
	}
	if !slices.Contains(strings.Split(proj.Member, ";"), party) {
		return fmt.Errorf("AddTableWithCheck: party %s is not in project %s", party, projectID)
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
	tableMetas, err := t.GetTablesByTableNames(tableId.ProjectID, []string{tableId.TableName})
	if err != nil {
		return false, fmt.Errorf("DropTableWithCheck: get table err: %v", err)
	}
	if len(tableMetas) == 0 {
		// table not exist
		return false, nil
	}
	// check project and owner
	for _, tableMeta := range tableMetas {
		if tableMeta.Table.Owner != party {
			return false, fmt.Errorf("DropTableWithCheck: table %s is not owned by %s", tableMeta.Table.TableName, party)
		}
	}
	err = t.DropTable(tableId)
	if err != nil {
		return true, fmt.Errorf("DropTableWithCheck: %v", err)
	}
	// table exist and drop it successfully
	return true, nil
}
