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

package regtest

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/secretflow/scql/pkg/parser/model"
	"github.com/secretflow/scql/pkg/proto-gen/scql"
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/scdb/client"
	"github.com/secretflow/scql/pkg/util/mock"
)

var (
	dbName = "scdb"
)

const (
	userNameRoot   = "root"
	userNameAlice  = "alice"
	userNameBob    = "bob"
	userNameCarol  = "carol"
	stubTimeoutMin = 5
)

var userMapPassword = map[string]string{
	userNameRoot:  "root",
	userNameAlice: "alice123",
	userNameBob:   "bob123",
	userNameCarol: "carol123",
}

var userMapPartyCode = map[string]string{
	userNameRoot:  "root",
	userNameAlice: "alice",
	userNameBob:   "bob",
	userNameCarol: "carol",
}

var partyCodeToUser = map[string]string{
	"root":  userNameRoot,
	"alice": userNameAlice,
	"bob":   userNameBob,
	"carol": userNameCarol,
}

var userMapGrmToken = map[string]string{
	userNameRoot:  "",
	userNameAlice: "token_alice",
	userNameBob:   "token_bob",
	userNameCarol: "token_carol",
}

var (
	tableToPartyCode map[string]string
)

func newUserCredential(user, pwd, grmToken string) *scql.SCDBCredential {
	return &scql.SCDBCredential{
		User: &scql.User{
			AccountSystemType: scql.User_NATIVE_USER,
			User: &scql.User_NativeUser_{
				NativeUser: &scql.User_NativeUser{
					Name:     user,
					Password: pwd,
				},
			},
		},
		GrmToken: grmToken,
	}
}

var userMapCredential = map[string]*scql.SCDBCredential{
	userNameRoot:  newUserCredential("root", "root", ""),
	userNameAlice: newUserCredential(userNameAlice, userMapPassword[userNameAlice], userMapGrmToken[userNameAlice]),
	userNameBob:   newUserCredential(userNameBob, userMapPassword[userNameBob], userMapGrmToken[userNameBob]),
	userNameCarol: newUserCredential(userNameCarol, userMapPassword[userNameCarol], userMapGrmToken[userNameCarol]),
}

var userNames = []string{userNameAlice, userNameBob, userNameCarol}

func fillTableToPartyCodeMap(dbTables map[string][]*model.TableInfo) {
	tableToPartyCode = make(map[string]string)
	for db, tables := range dbTables {
		for _, t := range tables {
			tableToPartyCode[fmt.Sprintf("%s_%s", db, t.Name)] = t.PartyCode
		}
	}
}

func runSql(user *scql.SCDBCredential, sql, addr string, sync bool) ([]*scql.Tensor, error) {
	httpClient := &http.Client{Timeout: stubTimeoutMin * time.Minute}
	stub := client.NewDefaultClient(addr, httpClient)
	if sync {
		resp, err := stub.SubmitAndGet(user, sql)
		if err != nil {
			return nil, err
		}
		if int32(scql.Code_OK) != resp.GetStatus().GetCode() {
			return nil, fmt.Errorf("error code %d, %+v", resp.Status.Code, resp)
		}
		return resp.OutColumns, nil
	} else {
		resp, err := stub.Submit(user, sql)
		if err != nil {
			return nil, err
		}
		if resp.Status.Code != int32(scql.Code_OK) {
			return nil, fmt.Errorf("error code expected %d, actual %d", int32(scql.Code_OK), resp.Status.Code)
		}

		if resp.ScdbSessionId == "" {
			return nil, fmt.Errorf("errorCode: %v, msg: %v", resp.Status.Code, resp.Status.Message)
		}
		fetchResp, err := stub.Fetch(user, resp.ScdbSessionId)
		if err != nil {
			return nil, err
		}

		if int32(scql.Code_OK) != fetchResp.Status.Code {
			return nil, fmt.Errorf("error code %d, %+v", fetchResp.Status.Code, fetchResp)
		}
		return fetchResp.OutColumns, nil
	}
}

func createUserAndCcl(cclList []*scql.SecurityConfig_ColumnControl, addr string, skipCreate bool) error {
	if skipCreate {
		fmt.Println("skip createUserAndCcl")
		return nil
	}
	// create user
	for _, user := range userNames {
		sql := fmt.Sprintf(`CREATE USER IF NOT EXISTS %s PARTY_CODE "%s" IDENTIFIED BY "%s"`, user, userMapPartyCode[user], userMapPassword[user])
		fmt.Println(sql)
		if _, err := runSql(userMapCredential[userNameRoot], sql, addr, true); err != nil {
			return err
		}
	}

	// create database
	{
		sql := fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS %s`, dbName)
		fmt.Println(sql)
		if _, err := runSql(userMapCredential[userNameRoot], sql, addr, true); err != nil {
			return err
		}
	}
	// grant base auth
	for _, user := range userNames {
		sql := fmt.Sprintf(`GRANT CREATE, CREATE VIEW, GRANT OPTION ON %s.* TO %s`, dbName, user)
		fmt.Println(sql)
		if _, err := runSql(userMapCredential[userNameRoot], sql, addr, true); err != nil {
			return err
		}
	}

	// grant ccl
	tableExistMap := map[string]bool{}
	// map db_table to party to ccl to columns
	grantMap := map[string]map[string]map[string][]string{}
	tableTIDs, err := mock.MockTableTIDs()
	if err != nil {
		return err
	}
	for _, ccl := range cclList {
		findTable := false
		dbTableName := fmt.Sprintf(`%s_%s`, ccl.DatabaseName, ccl.TableName)
		if _, exist := tableExistMap[dbTableName]; exist {
			findTable = true
		}
		tableOwner := partyCodeToUser[tableToPartyCode[dbTableName]]
		if !findTable {
			sql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s tid="%s"`, dbName, dbTableName, tableTIDs[dbTableName])
			fmt.Println(sql)
			if _, err := runSql(userMapCredential[tableOwner], sql, addr, true); err != nil {
				return err
			}
			tableExistMap[dbTableName] = true
		}
		grantTo := fmt.Sprintf("%s.%s", dbName, dbTableName)
		cclStr := proto.SecurityConfig_ColumnControl_Visibility_name[int32(ccl.Visibility)]
		if grantMap[grantTo] == nil {
			grantMap[grantTo] = map[string]map[string][]string{}
		}
		if grantMap[grantTo][partyCodeToUser[ccl.PartyCode]] == nil {
			grantMap[grantTo][partyCodeToUser[ccl.PartyCode]] = map[string][]string{}
		}
		if len(grantMap[grantTo][partyCodeToUser[ccl.PartyCode]][cclStr]) == 0 {
			grantMap[grantTo][partyCodeToUser[ccl.PartyCode]][cclStr] = []string{}
		}
		grantMap[grantTo][partyCodeToUser[ccl.PartyCode]][cclStr] = append(grantMap[grantTo][partyCodeToUser[ccl.PartyCode]][cclStr], ccl.ColumnName)
	}
	fmt.Println("Grant CCL...")
	for dbTable, userGrantMap := range grantMap {
		for user, cclGrantMap := range userGrantMap {
			var cclColumns []string
			for cc, columns := range cclGrantMap {
				cclColumns = append(cclColumns, fmt.Sprintf("SELECT %s(%s)", cc, strings.Join(columns, ", ")))
			}
			sql := fmt.Sprintf("GRANT %s ON %s TO %s", strings.Join(cclColumns, ", "), dbTable, user)
			tableOwner := partyCodeToUser[tableToPartyCode[strings.Split(dbTable, ".")[1]]]
			if _, err := runSql(userMapCredential[tableOwner], sql, addr, true); err != nil {
				return fmt.Errorf("query failed: %v, with error:%v", sql, err)
			}
		}
	}
	fmt.Println("Grant Complete!")
	fmt.Println()
	return nil
}
