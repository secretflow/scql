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

	"github.com/secretflow/scql/pkg/interpreter/graph"
	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/planner/core"
	"github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/scdb/storage"
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

func collectCCLForParty(store *gorm.DB, party string, tables []*scql.TableEntry) ([]*scql.SecurityConfig_ColumnControl, error) {
	var ccl []*scql.SecurityConfig_ColumnControl

	partyOwner, err := storage.FindUserByParty(store, party)
	if err != nil {
		return nil, err
	}

	for _, tbl := range tables {
		dbTable, err := core.NewDbTableFromString(tbl.TableName)
		if err != nil {
			return nil, err
		}
		// table level visibility
		tblPriv, err := tableVisibilityForUser(store, partyOwner.User, partyOwner.Host, dbTable.GetDbName(), dbTable.GetTableName())
		if err != nil {
			return nil, err
		}

		// column level visibility
		colPrivs, err := columnVisibilityForUser(store, partyOwner.User, partyOwner.Host, dbTable.GetDbName(), dbTable.GetTableName())
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
				PartyCode:    party,
				DatabaseName: dbTable.GetDbName(),
				TableName:    dbTable.GetTableName(),
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
	case mysql.PlaintextAsJoinPayloadPriv:
		return scql.SecurityConfig_ColumnControl_PLAINTEXT_AS_JOIN_PAYLOAD
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

// one-to-one correspondence between in parameter `partyCodes` and return value `[]*graph.Participant` on success
func GetParticipantInfos(db *gorm.DB, partyCodes []string) ([]*graph.Participant, error) {
	var users []storage.User
	if result := db.Model(&storage.User{}).Where("party_code in ?", partyCodes).Find(&users); result.Error != nil {
		return nil, result.Error
	}
	partyMap := make(map[string]*graph.Participant)
	for _, user := range users {
		if _, exists := partyMap[user.PartyCode]; exists {
			return nil, fmt.Errorf("there exists multiply users belong to the same party %v", user.PartyCode)
		}
		p := &graph.Participant{
			PartyCode: user.PartyCode,
			Endpoints: strings.Split(user.EngineEndpoints, ";"),
			Token:     user.EngineToken,
			PubKey:    user.EnginePubKey,
		}
		partyMap[user.PartyCode] = p
	}
	result := make([]*graph.Participant, 0, len(partyCodes))
	for _, code := range partyCodes {
		p, exists := partyMap[code]
		if !exists {
			return nil, fmt.Errorf("party %v not found", code)
		}
		result = append(result, p)
	}
	return result, nil
}

func ValidateParticipantInfos(participants []*graph.Participant) error {
	for _, p := range participants {
		if len(p.Endpoints) == 0 {
			return fmt.Errorf("no endpoint for party %s", p.PartyCode)
		}
	}
	return nil
}
