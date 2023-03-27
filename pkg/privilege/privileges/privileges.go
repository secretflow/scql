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

// Modified by Ant Group in 2023

package privileges

import (
	"crypto/tls"

	log "github.com/sirupsen/logrus"

	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/parser/auth"
	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/privilege"
)

var _ privilege.Manager = (*UserPrivileges)(nil)

// UserPrivileges implements privilege.Manager interface.
// This is used to check privilege for the current user.
type UserPrivileges struct {
	user string
	host string

	*Handle
}

// RequestVerification implements the Manager interface.
func (p *UserPrivileges) RequestVerification(activeRoles []*auth.RoleIdentity, db, table, column string, priv mysql.PrivilegeType) (bool, error) {

	if p.user == "" && p.host == "" {
		return true, nil
	}

	scdbPriv := p.Handle.Get()
	return scdbPriv.RequestVerification(activeRoles, p.user, p.host, db, table, column, priv)
}

// RequestVerificationWithUser implements the Manager interface.
func (p *UserPrivileges) RequestVerificationWithUser(db, table, column string, priv mysql.PrivilegeType, user *auth.UserIdentity) (bool, error) {

	if user == nil {
		return false, nil
	}

	mysqlPriv := p.Handle.Get()
	return mysqlPriv.RequestVerification(nil, user.Username, user.Hostname, db, table, column, priv)
}

// ConnectionVerification implements the Manager interface.
func (p *UserPrivileges) ConnectionVerification(user, host, authentication string, salt []byte, tlsState *tls.ConnectionState) (u string, h string, success bool) {
	mysqlPriv := p.Handle.Get()
	record, err := mysqlPriv.matchUser(user, host)
	if err != nil {
		return "", "", false
	}
	if record == nil {
		log.Errorf("get user privilege record fail: user %s, host %s",
			user, host)
		return
	}

	u = record.User
	h = record.Host

	pwd := record.Password
	if len(pwd) != 0 && len(pwd) != mysql.PWDHashLen+1 {
		log.Errorf("user password from system DB not like sha1sum: user %s", user)
		return
	}

	// empty password
	if len(pwd) == 0 && len(authentication) == 0 {
		p.user = user
		p.host = h
		success = true
		return
	}

	if len(pwd) == 0 || len(authentication) == 0 {
		return
	}

	// NOTE(yang.y): Here we simply compare the encode password string
	// while the TiDB uses hand shake based on random number.
	// Please refer to auth.CheckScrambledPassword for details
	if epwd := EncodePassword(user, host, authentication); epwd != pwd {
		return
	}

	p.user = user
	p.host = h
	success = true
	return
}

// DBIsVisible implements the Manager interface.
func (p *UserPrivileges) DBIsVisible(activeRoles []*auth.RoleIdentity, db string, priv mysql.PrivilegeType) (bool, error) {
	mysqlPriv := p.Handle.Get()
	return mysqlPriv.DBIsVisible(p.user, p.host, db, priv)
	// Not support roles here
}

func EncodePassword(user, host, authentication string) string {
	spec := &ast.UserSpec{
		User: &auth.UserIdentity{
			Username: user,
			Hostname: host,
		},
		AuthOpt: &ast.AuthOption{
			ByAuthString: true,
			AuthString:   authentication,
		},
	}
	encodedPwd, _ := spec.EncodedPassword()
	return encodedPwd
}
