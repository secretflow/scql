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

package server

import (
	"fmt"
	"strings"

	"gorm.io/gorm"

	"github.com/secretflow/scql/pkg/grm"
	"github.com/secretflow/scql/pkg/interpreter/translator"
	"github.com/secretflow/scql/pkg/parser/mysql"
	grmproto "github.com/secretflow/scql/pkg/proto-gen/grm"
	"github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/scdb/storage"
	"github.com/secretflow/scql/pkg/util/sliceutil"
)

func findColPriv(colPrivs []storage.ColumnPriv, colName string) (storage.ColumnPriv, bool) {
	for _, p := range colPrivs {
		// This is a temporary solution, we should unify case-insensitive impl.
		if strings.EqualFold(p.ColumnName, colName) {
			return p, true
		}
	}
	return storage.ColumnPriv{}, false
}

func collectCCLForUser(store *gorm.DB, user, host string, tables []*grm.TableSchema) ([]*scql.SecurityConfig_ColumnControl, error) {
	var ccl []*scql.SecurityConfig_ColumnControl

	partyCode, err := storage.QueryUserPartyCode(store, user, host)
	if err != nil {
		return nil, fmt.Errorf("failed to query party code for user %s: %v", user, err)
	}

	for _, tbl := range tables {
		// table level visibility
		tblPriv, err := tableVisibilityForUser(store, user, host, tbl.DbName, tbl.TableName)
		if err != nil {
			return nil, err
		}

		// column level visibility
		colPrivs, err := columnVisibilityForUser(store, user, host, tbl.DbName, tbl.TableName)
		if err != nil {
			return nil, err
		}

		for _, col := range tbl.Columns {
			colPriv, ok := findColPriv(colPrivs, col.Name)
			// if not found in column privileges table, use table level visibility
			if !ok {
				colPriv.VisibilityPriv = tblPriv.VisibilityPriv
			}
			cc := &scql.SecurityConfig_ColumnControl{
				PartyCode:    partyCode,
				DatabaseName: tbl.DbName,
				TableName:    tbl.TableName,
				ColumnName:   col.Name,
				Visibility:   mysqlPrivilegeType2CCLVisibility(colPriv.VisibilityPriv),
			}

			ccl = append(ccl, cc)
		}
	}
	return ccl, nil
}

func mysqlPrivilegeType2CCLVisibility(priv mysql.PrivilegeType) scql.SecurityConfig_ColumnControl_Visibility {
	switch priv {
	case mysql.PlaintextPriv:
		return scql.SecurityConfig_ColumnControl_PLAINTEXT
	case mysql.PlaintextAfterComparePriv:
		return scql.SecurityConfig_ColumnControl_PLAINTEXT_AFTER_COMPARE
	case mysql.PlaintextAfterAggregatePriv:
		return scql.SecurityConfig_ColumnControl_PLAINTEXT_AFTER_AGGREGATE
	case mysql.EncryptedOnlyPriv:
		return scql.SecurityConfig_ColumnControl_ENCRYPTED_ONLY
	case mysql.PlaintextAfterJoinPriv:
		return scql.SecurityConfig_ColumnControl_PLAINTEXT_AFTER_JOIN
	case mysql.PlaintextAfterGroupByPriv:
		return scql.SecurityConfig_ColumnControl_PLAINTEXT_AFTER_GROUP_BY
	}
	return scql.SecurityConfig_ColumnControl_UNKNOWN
}

func tableVisibilityForUser(store *gorm.DB, user, host, dbName, tableName string) (storage.TablePriv, error) {
	var tblPriv storage.TablePriv
	result := store.Where(&storage.TablePriv{User: user, Host: host, Db: dbName, TableName: tableName}).Find(&tblPriv)
	return tblPriv, result.Error
}

func columnVisibilityForUser(store *gorm.DB, user, host, dbName, tableName string) ([]storage.ColumnPriv, error) {
	var colPrivs []storage.ColumnPriv
	result := store.Where(&storage.ColumnPriv{User: user, Host: host, Db: dbName, TableName: tableName}).Find(&colPrivs)
	if result.Error != nil {
		return nil, result.Error
	}
	return colPrivs, nil
}

func (app *App) askEngineInfoByTables(s *session, dbName string, tableNames []string) (*translator.EnginesInfo, error) {
	store := s.GetSessionVars().Storage
	var tables []storage.Table
	result := store.Where("db = ? AND `table_name` in ?", dbName, tableNames).Find(&tables)
	if result.Error != nil {
		return nil, result.Error
	}

	var partyCodes []string
	party2Tables := map[string][]translator.DbTable{}
	tableToRefs := map[translator.DbTable]translator.DbTable{}
	for _, t := range tables {
		partyCode, err := storage.QueryUserPartyCode(store, t.Owner, t.Host)
		if err != nil {
			return nil, fmt.Errorf("askEngineInfo failed to query party code: %v", err)
		}
		partyCodes = append(partyCodes, partyCode)

		dbTable := translator.NewDbTable(t.Db, t.Table)
		if _, exist := party2Tables[partyCode]; !exist {
			party2Tables[partyCode] = []translator.DbTable{dbTable}
		} else {
			party2Tables[partyCode] = append(party2Tables[partyCode], dbTable)
		}

		refDbTable := translator.NewDbTable(t.RefDb, t.RefTable)
		refDbTable.SetDBType(grmproto.DataSourceKind(t.DBType))
		tableToRefs[dbTable] = refDbTable
	}

	grmToken := s.GetSessionVars().GrmToken
	grmClien := s.GetSessionVars().GrmClient

	// NOTE: sort engines by party code to enforce determinism
	partyCodes = sliceutil.SliceDeDup(partyCodes)

	engines, err := grmClien.GetEngines(partyCodes, grmToken)
	if err != nil {
		return nil, fmt.Errorf("askEngineInfo failed to get engineInfo: %v", err)
	}
	var partyHosts []string
	var partyCredentials []string
	for _, engineInfo := range engines {
		partyHosts = append(partyHosts, engineInfo.Endpoints[0])
		partyCredentials = append(partyCredentials, engineInfo.Credential[0])
	}
	partyInfo, err := translator.NewPartyInfo(partyCodes, partyHosts, partyCredentials)
	if err != nil {
		return nil, fmt.Errorf("askEngineInfo failed to get partyInfo: %v", err)
	}
	enginesInfo := translator.NewEnginesInfo(partyInfo, party2Tables)

	enginesInfo.UpdateTableToRefs(tableToRefs)

	return enginesInfo, nil
}
