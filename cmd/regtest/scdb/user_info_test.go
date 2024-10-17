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

package scdb

import (
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
)

var (
	dbName = "scdb"
)

const (
	userNameRoot  = "root"
	userNameAlice = "alice"
	userNameBob   = "bob"
	userNameCarol = "carol"
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

var userMapCredential = map[string]*proto.SCDBCredential{
	userNameRoot:  newUserCredential("root", "root"),
	userNameAlice: newUserCredential(userNameAlice, userMapPassword[userNameAlice]),
	userNameBob:   newUserCredential(userNameBob, userMapPassword[userNameBob]),
	userNameCarol: newUserCredential(userNameCarol, userMapPassword[userNameCarol]),
}

var userNames = []string{userNameAlice, userNameBob, userNameCarol}

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
