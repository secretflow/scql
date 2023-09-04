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

	"github.com/secretflow/scql/pkg/infoschema"
	"github.com/secretflow/scql/pkg/interpreter/translator"
	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/planner/core"
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

func collectCCLForUser(store *gorm.DB, user, host string, tables []*infoschema.TableSchema) ([]*scql.SecurityConfig_ColumnControl, error) {
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
		refDbTable.SetDBType(core.DBType(t.DBType))
		tableToRefs[dbTable] = refDbTable
	}

	// NOTE: sort engines by party code to enforce determinism
	partyCodes = sliceutil.SliceDeDup(partyCodes)

	participants, err := GetParticipantInfos(store, partyCodes)
	if err != nil {
		return nil, fmt.Errorf("askEngineInfo failed to get participantInfos: %+v", err)
	}

	if err = ValidateParticipantInfos(participants); err != nil {
		return nil, fmt.Errorf("invalid participantsInfo: %+v", err)
	}

	partyInfo := translator.NewPartyInfo(participants)
	enginesInfo := translator.NewEnginesInfo(partyInfo, party2Tables)

	enginesInfo.UpdateTableToRefs(tableToRefs)

	return enginesInfo, nil
}

// one-to-one correspondence between in parameter `partyCodes` and return value `[]*translator.Participant` on success
func GetParticipantInfos(db *gorm.DB, partyCodes []string) ([]*translator.Participant, error) {
	var users []storage.User
	if result := db.Model(&storage.User{}).Where("party_code in ?", partyCodes).Find(&users); result.Error != nil {
		return nil, result.Error
	}
	partyMap := make(map[string]*translator.Participant)
	for _, user := range users {
		if _, exists := partyMap[user.PartyCode]; exists {
			return nil, fmt.Errorf("there exists multiply users belong to the same party %v", user.PartyCode)
		}
		p := &translator.Participant{
			PartyCode: user.PartyCode,
			Endpoints: strings.Split(user.EngineEndpoints, ";"),
			Token:     user.EngineToken,
			PubKey:    user.EnginePubKey,
		}
		partyMap[user.PartyCode] = p
	}
	result := make([]*translator.Participant, 0, len(partyCodes))
	for _, code := range partyCodes {
		p, exists := partyMap[code]
		if !exists {
			return nil, fmt.Errorf("party %v not found", code)
		}
		result = append(result, p)
	}
	return result, nil
}

func ValidateParticipantInfos(participants []*translator.Participant) error {
	for _, p := range participants {
		if len(p.Endpoints) == 0 {
			return fmt.Errorf("no endpoint for party %s", p.PartyCode)
		}
	}
	return nil
}
