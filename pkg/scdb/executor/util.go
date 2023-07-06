// Copyright 2023 Ant Group Co., Ltd.

// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Modified by Ant Group in 2023

package executor

import (
	"fmt"

	"github.com/pkg/errors"
	"gorm.io/gorm"

	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/parser/auth"
	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/scdb/storage"
)

func SetAllGrantPrivTo(level ast.GrantLevelType, attributes map[string]interface{}, value bool) error {
	var allPrivs []mysql.PrivilegeType
	switch level {
	case ast.GrantLevelDB:
		allPrivs = storage.AllDBPrivs
	case ast.GrantLevelTable:
		allPrivs = storage.AllTablePrivs
	case ast.GrantLevelGlobal:
		allPrivs = storage.AllGlobalPrivs
	default:
		return errors.Errorf("unknown grant level %v", level)
	}
	for _, p := range allPrivs {
		v, ok := mysql.Priv2UserCol[p]
		if !ok {
			return errors.Errorf("Unknown db privilege %v", p)
		}
		attributes[v] = value
	}
	return nil
}

func CheckUserGrantPriv(store *gorm.DB, issuerUser, curUser *auth.UserIdentity, privs []*ast.PrivElem) error {
	if curUser.Username == storage.DefaultRootName && curUser.Hostname == storage.DefaultHostName {
		return fmt.Errorf("you can't grant or revoke priv for root")
	}
	curPartyCode, err := storage.QueryUserPartyCode(store, curUser.Username, curUser.Hostname)
	if err != nil {
		return fmt.Errorf("check current user failed: %v", err)
	}

	if issuerUser.Username == storage.DefaultRootName && issuerUser.Hostname == storage.DefaultHostName {
		return nil
	}
	issuerPartyCode, err := storage.QueryUserPartyCode(store, issuerUser.Username, issuerUser.Hostname)
	if err != nil {
		return fmt.Errorf("check issuer user failed: %v", err)
	}

	if issuerPartyCode != curPartyCode {
		for _, priv := range privs {
			if _, exist := storage.VisibilityPrivColName[priv.Priv]; !exist {
				return fmt.Errorf("you can't grant or revoke priv %v for user %v in another party", mysql.Priv2Str[priv.Priv], curUser.Username)
			}
		}
	}
	return nil
}
