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
	"time"

	"github.com/secretflow/scql/pkg/parser/model"
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/scdb/client"
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

var (
	tableToPartyCode map[string]string
)

func newUserCredential(user, pwd string) *proto.SCDBCredential {
	return &proto.SCDBCredential{
		User: &proto.User{
			AccountSystemType: proto.User_NATIVE_USER,
			User: &proto.User_NativeUser_{
				NativeUser: &proto.User_NativeUser{
					Name:     user,
					Password: pwd,
				},
			},
		},
	}
}

var userMapCredential = map[string]*proto.SCDBCredential{
	userNameRoot:  newUserCredential("root", "root"),
	userNameAlice: newUserCredential(userNameAlice, userMapPassword[userNameAlice]),
	userNameBob:   newUserCredential(userNameBob, userMapPassword[userNameBob]),
	userNameCarol: newUserCredential(userNameCarol, userMapPassword[userNameCarol]),
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

func runSql(user *proto.SCDBCredential, sql, addr string, sync bool) ([]*proto.Tensor, error) {
	httpClient := &http.Client{Timeout: stubTimeoutMin * time.Minute}
	stub := client.NewDefaultClient(addr, httpClient)
	if sync {
		resp, err := stub.SubmitAndGet(user, sql)
		if err != nil {
			return nil, err
		}
		if int32(proto.Code_OK) != resp.GetStatus().GetCode() {
			return nil, fmt.Errorf("error code %d, %+v", resp.Status.Code, resp)
		}
		return resp.OutColumns, nil
	} else {
		resp, err := stub.Submit(user, sql)
		if err != nil {
			return nil, err
		}
		if resp.Status.Code != int32(proto.Code_OK) {
			return nil, fmt.Errorf("error code expected %d, actual %d", int32(proto.Code_OK), resp.Status.Code)
		}

		if resp.ScdbSessionId == "" {
			return nil, fmt.Errorf("errorCode: %v, msg: %v", resp.Status.Code, resp.Status.Message)
		}
		fetchResp, err := stub.Fetch(user, resp.ScdbSessionId)
		if err != nil {
			return nil, err
		}

		if int32(proto.Code_OK) != fetchResp.Status.Code {
			return nil, fmt.Errorf("error code %d, %+v", fetchResp.Status.Code, fetchResp)
		}
		return fetchResp.OutColumns, nil
	}
}
