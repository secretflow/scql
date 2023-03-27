// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package privilege

import (
	"crypto/tls"

	"github.com/secretflow/scql/pkg/parser/auth"
	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/sessionctx"
)

type keyType int

func (k keyType) String() string {
	return "privilege-key"
}

// Manager is the interface for providing privilege related operations.
type Manager interface {

	// RequestVerification verifies user privilege for the request.
	// If table is "", only check global/db scope privileges.
	// If table is not "", check global/db/table scope privileges.
	// priv should be a defined constant like CreatePriv, if pass AllPrivMask to priv,
	// this means any privilege would be OK.
	RequestVerification(activeRole []*auth.RoleIdentity, db, table, column string, priv mysql.PrivilegeType) (bool, error)

	// RequestVerificationWithUser verifies specific user privilege for the request.
	RequestVerificationWithUser(db, table, column string, priv mysql.PrivilegeType, user *auth.UserIdentity) (bool, error)

	// ConnectionVerification verifies user privilege for connection.
	ConnectionVerification(user, host, auth string, salt []byte, tlsState *tls.ConnectionState) (string, string, bool)

	// DBIsVisible returns true is the database is visible to current user for privilege `priv`
	DBIsVisible(activeRole []*auth.RoleIdentity, db string, priv mysql.PrivilegeType) (bool, error)
}

const key keyType = 0

// BindPrivilegeManager binds Manager to context.
func BindPrivilegeManager(ctx sessionctx.Context, pc Manager) {
	ctx.SetValue(key, pc)
}

// GetPrivilegeManager gets Checker from context.
func GetPrivilegeManager(ctx sessionctx.Context) Manager {
	if v, ok := ctx.Value(key).(Manager); ok {
		return v
	}
	return nil
}
