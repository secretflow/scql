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

package executor

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/secretflow/scql/pkg/infoschema"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/parser/auth"
	"github.com/secretflow/scql/pkg/parser/model"
	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/privilege"
	"github.com/secretflow/scql/pkg/privilege/privileges"
	"github.com/secretflow/scql/pkg/proto-gen/scql"
	partyAuth "github.com/secretflow/scql/pkg/scdb/auth"
	"github.com/secretflow/scql/pkg/scdb/config"
	"github.com/secretflow/scql/pkg/scdb/storage"
	"github.com/secretflow/scql/pkg/sessionctx"
	"github.com/secretflow/scql/pkg/sessionctx/stmtctx"
	"github.com/secretflow/scql/pkg/types"
)

type userAuth struct {
	hostName string
	userName string
	password string
}

var (
	userRoot = &userAuth{
		hostName: `%`,
		userName: `root`,
		password: `root`,
	}
	userAlice = &userAuth{
		hostName: `%`,
		userName: `alice`,
		password: `some_pwd`,
	}
	userBob = &userAuth{
		hostName: `%`,
		userName: `bob`,
		password: `some_pwd`,
	}
)

func (u *userAuth) encodedPassword() string {
	spec := &ast.UserSpec{
		User: &auth.UserIdentity{
			Username: u.userName,
			Hostname: u.hostName,
		},
		AuthOpt: &ast.AuthOption{
			ByAuthString: true,
			AuthString:   u.password,
		},
	}
	encodedPwd, _ := spec.EncodedPassword()
	return encodedPwd
}

func (u *userAuth) toUserIdentity() *auth.UserIdentity {
	return &auth.UserIdentity{Username: u.userName, Hostname: u.hostName}
}

func mockStorage() (*gorm.DB, error) {
	db, err := storage.NewMemoryStorage()
	if err != nil {
		return nil, err
	}

	// Bootstrap
	result := db.Create(&storage.User{
		Host:           `%`,
		User:           `root`,
		Password:       userRoot.encodedPassword(),
		CreatePriv:     true,
		CreateUserPriv: true,
		GrantPriv:      true,
		DropPriv:       true,
		DescribePriv:   true,
		ShowPriv:       true,
		CreateViewPriv: true,
	})
	if result.Error != nil {
		return nil, result.Error
	}

	// check if bootstrap succeeded
	if exist, err := storage.CheckUserExist(db, "root", `%`); err != nil || !exist {
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("bootstrap failed")
	}

	return db, nil
}

func mockUserSession(ctx sessionctx.Context, user *userAuth, storage *gorm.DB) error {
	ctx.GetSessionVars().Storage = storage
	return switchUser(ctx, user)
}

func mockContext() sessionctx.Context {
	ctx := sessionctx.NewContext()
	ctx.GetSessionVars().StmtCtx = &stmtctx.StatementContext{}
	return ctx
}

func switchUser(ctx sessionctx.Context, user *userAuth) error {
	pm := &privileges.UserPrivileges{
		Handle: privileges.NewHandle(ctx),
	}
	if _, _, ok := pm.ConnectionVerification(user.userName, user.hostName, user.password, nil, nil); !ok {
		return fmt.Errorf("user verification error")
	}
	privilege.BindPrivilegeManager(ctx, pm)

	ctx.GetSessionVars().User = user.toUserIdentity()
	return nil
}

func columnPrivilegesExists(store *gorm.DB, user, dbName, tblName, colName string, priv mysql.PrivilegeType) (bool, error) {
	var colPrivRecord storage.ColumnPriv
	result := store.Where(&storage.ColumnPriv{User: user, Host: "%", Db: dbName, TableName: tblName, ColumnName: colName}).Find(&colPrivRecord)
	if result.Error != nil {
		return false, result.Error
	}
	if result.RowsAffected != 1 {
		return false, nil
	}

	if colPrivRecord.User != user || colPrivRecord.Db != dbName || colPrivRecord.ColumnName != colName || colPrivRecord.VisibilityPriv != priv {
		return false, nil
	}
	return true, nil
}

// setupTestEnv setup test environment and login as root user
func setupTestEnv(t *testing.T) sessionctx.Context {
	r := require.New(t)

	db, err := mockStorage()
	r.NoError(err)

	ctx := mockContext()

	err = mockUserSession(ctx, userRoot, db)
	r.NoError(err)

	return ctx
}

func runSQL(ctx sessionctx.Context, stmt string) ([]*scql.Tensor, error) {
	is := infoschema.MockInfoSchema(map[string][]*model.TableInfo{})
	return Run(ctx, stmt, is)
}

func runSQLWithInfoschema(ctx sessionctx.Context, stmt string, is infoschema.InfoSchema) ([]*scql.Tensor, error) {
	return Run(ctx, stmt, is)
}

func TestCreateUser(t *testing.T) {
	r := require.New(t)

	db, err := mockStorage()
	r.NoError(err)

	t.Run("NormalCreateUserTest", func(t *testing.T) {
		ctx := mockContext()
		r.NoError(mockUserSession(ctx, userRoot, db))

		// success
		_, err := runSQL(ctx, `CREATE USER alice PARTY_CODE "party_A" IDENTIFIED BY "some_pwd"`)
		r.NoError(err)

		// success
		_, err = runSQL(ctx, `CREATE USER bob PARTY_CODE "party_B" IDENTIFIED BY "some_pwd"`)
		r.NoError(err)

		// failed due to user exists
		_, err = runSQL(ctx, `CREATE USER bob PARTY_CODE "party_B" IDENTIFIED BY "some_pwd"`)
		r.Error(err)

		// success
		_, err = runSQL(ctx, `CREATE USER IF NOT EXISTS bob PARTY_CODE "party_B" IDENTIFIED BY "some_pwd"`)
		r.NoError(err)

		// only one user is allowed to exist in a party
		_, err = runSQL(ctx, `CREATE USER another PARTY_CODE "party_B" IDENTIFIED BY "some_pwd"`)
		r.Error(err)

		r.NoError(switchUser(ctx, userAlice))

		// failed due to no privilege
		_, err = runSQL(ctx, `CREATE USER carol PARTY_CODE "party_C" IDENTIFIED BY "some_pwd"`)
		r.Error(err)

		users := []storage.User{}
		result := ctx.GetSessionVars().Storage.Order(`id`).Find(&users)
		r.NoError(result.Error)
		r.Equal(3, len(users))
		r.Equal("root", users[0].User)
		r.Equal("alice", users[1].User)
		r.Equal("party_A", users[1].PartyCode)
		r.Equal("bob", users[2].User)
	})

	t.Run("CreateUserFailedTest", func(t *testing.T) {
		ctx := mockContext()
		r.NoError(mockUserSession(ctx, userBob, db))

		// failed due to no create user privilege
		_, err := runSQL(ctx, `CREATE USER alice PARTY_CODE "party_A" IDENTIFIED BY "some_pwd"`)
		r.Error(err)
	})

	t.Run("CreateUserWithEngineOptionTest", func(t *testing.T) {
		// db isolation
		db, err := mockStorage()
		r.NoError(err)

		ctx := mockContext()

		r.NoError(mockUserSession(ctx, userRoot, db))

		// create user with token auth
		{
			pa := partyAuth.NewPartyAuthenticator(config.PartyAuthConf{
				Method: "token",
			})
			partyAuth.BindPartyAuthenticator(ctx, pa)

			_, err = runSQL(ctx, `CREATE USER alice PARTY_CODE "party_A" IDENTIFIED BY "some_pwd" WITH TOKEN "some_token"`)
			r.NoError(err)
		}

		// create user with ED25519 auth
		{
			pa := partyAuth.NewPartyAuthenticator(config.PartyAuthConf{
				Method:               "pubkey",
				EnableTimestampCheck: true,
				ValidityPeriod:       time.Second,
			})
			partyAuth.BindPartyAuthenticator(ctx, pa)

			pub, priv, err := ed25519.GenerateKey(nil)
			r.NoError(err)

			msg, err := time.Now().MarshalText()
			r.NoError(err)

			sig := ed25519.Sign(priv, msg)

			pubKeyInDER, err := x509.MarshalPKIXPublicKey(pub)
			r.NoError(err)
			query := fmt.Sprintf("CREATE USER bob PARTY_CODE 'party_B' IDENTIFIED BY 'some_pwd' WITH '%s' '%s' '%s'", string(msg), base64.StdEncoding.EncodeToString(sig), base64.StdEncoding.EncodeToString(pubKeyInDER))
			_, err = runSQL(ctx, query)
			r.NoError(err)
		}

		// create user with ED25519 auth with expired timestamp, it should fail
		{
			pa := partyAuth.NewPartyAuthenticator(config.PartyAuthConf{
				Method:               "pubkey",
				EnableTimestampCheck: true,
				ValidityPeriod:       time.Second,
			})
			partyAuth.BindPartyAuthenticator(ctx, pa)

			pub, priv, err := ed25519.GenerateKey(nil)
			r.NoError(err)

			msg, err := time.Now().Add(-time.Second).MarshalText()
			r.NoError(err)

			sig := ed25519.Sign(priv, msg)

			pubKeyInDER, err := x509.MarshalPKIXPublicKey(pub)
			r.NoError(err)
			query := fmt.Sprintf("CREATE USER carol PARTY_CODE 'party_C' IDENTIFIED BY 'some_pwd' WITH '%s' '%s' '%s'", string(msg), base64.StdEncoding.EncodeToString(sig), base64.StdEncoding.EncodeToString(pubKeyInDER))
			_, err = runSQL(ctx, query)
			r.Error(err)
		}

	})
}

func TestAlterUser(t *testing.T) {
	r := require.New(t)
	ctx := setupTestEnv(t)

	userCarol := &userAuth{
		hostName: `%`,
		userName: `carol`,
		password: `some_pwd`,
	}

	// root> create user carol
	_, err := runSQL(ctx, `CREATE USER carol PARTY_CODE "party_C" IDENTIFIED BY "some_pwd"`)
	r.NoError(err)

	// root> alter user carol, failed due to no privilege
	_, err = runSQL(ctx, `ALTER USER carol IDENTIFIED BY 'new-password'`)
	r.Equal(err.Error(), "only support user themselves to execute alter user")

	r.NoError(switchUser(ctx, userCarol))

	// carol> alter user carol, success
	_, err = runSQL(ctx, `ALTER USER carol IDENTIFIED BY 'new-password'`)
	r.NoError(err)

	// switch to user carol, failed due to password changed
	r.Error(switchUser(ctx, userCarol))

	// switch to user alice
	userCarol.password = "new-password"
	r.NoError(switchUser(ctx, userCarol))

	// carol> alter endpoint
	_, err = runSQL(ctx, `ALTER USER carol WITH ENDPOINT 'host1:port1'`)
	r.NoError(err)

	// carol> alter nothing, failed
	_, err = runSQL(ctx, `ALTER user carol`)
	r.Error(err)
}

func TestDropUser(t *testing.T) {
	r := require.New(t)

	db, err := mockStorage()
	r.NoError(err)

	t.Run("NormalDropUserTest", func(t *testing.T) {
		ctx := mockContext()
		r.NoError(mockUserSession(ctx, userRoot, db))

		// create user `bob` before dropping it
		_, err := runSQL(ctx, `CREATE USER bob PARTY_CODE "party_B" IDENTIFIED BY "some_pwd"`)
		r.NoError(err)

		// failed since user bob has no create user privilege
		r.NoError(switchUser(ctx, userBob))
		_, err = runSQL(ctx, `DROP USER alice`)
		r.Error(err)
		r.NoError(switchUser(ctx, userRoot))

		// success
		_, err = runSQL(ctx, `DROP USER bob`)
		r.NoError(err)

		// error since user bob no longer exists
		r.Error(mockUserSession(ctx, userBob, db))

		// failed due to drop an non-existed user
		_, err = runSQL(ctx, `DROP USER bob`)
		r.Error(err)

		// success to drop a non-existed user with `IF EXISTS`
		_, err = runSQL(ctx, `DROP USER IF EXISTS bob`)
		r.NoError(err)
	})

	t.Run("DropUserAndDeleteAllPriv", func(t *testing.T) {
		// root> create user alice
		// root> grant create on *.* to alice
		// alice> create database da;  -- success
		// root> grant delete on da.* to alice
		// root> drop user alice
		// root> create user alice
		ctx := mockContext()
		r.NoError(mockUserSession(ctx, userRoot, db))

		// root> create user alice
		_, err = runSQL(ctx, `CREATE USER alice PARTY_CODE "party_A" IDENTIFIED BY "some_pwd"`)
		r.NoError(err)

		// root> grant create on *.* to alice
		_, err = runSQL(ctx, `GRANT CREATE ON *.* TO alice`)
		r.NoError(err)

		// switch to user alice
		r.NoError(switchUser(ctx, userAlice))

		// alice> create database da
		_, err = runSQL(ctx, `CREATE DATABASE da`)
		r.NoError(err)

		// switch to user root
		r.NoError(switchUser(ctx, userRoot))

		// root> grant drop privilege
		_, err = runSQL(ctx, `GRANT DROP ON da.* TO alice`)
		r.NoError(err)

		// switch to user root
		r.NoError(switchUser(ctx, userRoot))

		// root> drop user alice
		_, err = runSQL(ctx, `DROP USER alice`)
		r.NoError(err)

		// root> create user alice
		_, err = runSQL(ctx, `CREATE USER alice PARTY_CODE "party_A" IDENTIFIED BY "some_pwd"`)
		r.NoError(err)

		// switch to user alice
		r.NoError(switchUser(ctx, userAlice))
	})
}

func TestCreateDatabase(t *testing.T) {
	r := require.New(t)

	db, err := mockStorage()
	r.NoError(err)

	t.Run("NormalCreateDatabaseTest", func(t *testing.T) {
		ctx := mockContext()
		r.NoError(mockUserSession(ctx, userRoot, db))

		// success
		_, err := runSQL(ctx, `CREATE DATABASE test`)
		r.NoError(err)

		// success
		_, err = runSQL(ctx, `CREATE DATABASE IF NOT EXISTS test`)
		r.NoError(err)

		// failed due to database exists
		_, err = runSQL(ctx, `CREATE DATABASE test`)
		r.Error(err)

	})

	t.Run("CreateDatabaseFailedTest", func(t *testing.T) {
		ctx := mockContext()
		r.NoError(mockUserSession(ctx, userRoot, db))

		// create user success
		_, err = runSQL(ctx, `CREATE USER alice PARTY_CODE "party_A" IDENTIFIED BY "some_pwd"`)
		r.NoError(err)

		ctx.GetSessionVars().User = &auth.UserIdentity{Username: `alice`}
		// failed due to lack of privilege
		_, err = runSQL(ctx, `CREATE DATABASE test2`)
		r.Error(err)
	})
}

func TestDropDatabase(t *testing.T) {
	r := require.New(t)

	db, err := mockStorage()
	r.NoError(err)

	t.Run("NormalDropDatabaseTest", func(t *testing.T) {
		ctx := mockContext()
		r.NoError(mockUserSession(ctx, userRoot, db))

		// success
		_, err := runSQL(ctx, `DROP DATABASE IF EXISTS test`)
		r.NoError(err)

		// failed due to database test not exists
		_, err = runSQL(ctx, `DROP DATABASE test`)
		r.Error(err)
	})

	t.Run("DropDatabaseAndDeletePrivTest", func(t *testing.T) {
		// testcase: Drop database will delete related privileges
		// root> CREATE DATABASE da;
		// root> CREATE USER alice...
		// root> GRANT CREATE ON da.* TO alice...
		// root> DROP DATABASE da;
		// root> CREATE DATABASE da;
		ctx := mockContext()
		r.NoError(mockUserSession(ctx, userRoot, db))

		// root> create database
		_, err := runSQL(ctx, `CREATE DATABASE da`)
		r.NoError(err)

		// root> create user alice
		_, err = runSQL(ctx, `CREATE USER alice PARTY_CODE "party_A" IDENTIFIED BY "some_pwd"`)
		r.NoError(err)

		// root> grant create on da.* to alice...
		_, err = runSQL(ctx, `GRANT CREATE ON da.* TO alice`)
		r.NoError(err)

		// switch to user root
		r.NoError(switchUser(ctx, userRoot))

		// root> drop database da
		_, err = runSQL(ctx, `DROP DATABASE da`)
		r.NoError(err)

		// root> create database da
		_, err = runSQL(ctx, `CREATE DATABASE da`)
		r.NoError(err)
	})

}

func TestCreateView(t *testing.T) {
	r := require.New(t)

	db, err := mockStorage()
	r.NoError(err)

	{
		ctx := mockContext()
		r.NoError(mockUserSession(ctx, userRoot, db))

		// root> create user alice
		_, err = runSQL(ctx, `CREATE USER alice PARTY_CODE "party_A" IDENTIFIED BY "some_pwd"`)
		r.NoError(err)

		// root> grant create on *.* to alice
		_, err = runSQL(ctx, `GRANT CREATE ON *.* TO alice`)
		r.NoError(err)

		// root> grant create view on *.* to alice
		_, err = runSQL(ctx, `GRANT CREATE VIEW ON *.* TO alice`)
		r.NoError(err)

		// root> grant drop view on *.* to alice
		_, err = runSQL(ctx, `GRANT DROP ON *.* TO alice`)
		r.NoError(err)

		// switch to user alice
		r.NoError(switchUser(ctx, userAlice))

		// success
		_, err = runSQL(ctx, `CREATE DATABASE test`)
		r.NoError(err)

		// success
		_, err = runSQL(ctx, `CREATE TABLE test.new_table (c1 int, c2 int) REF_TABLE=d1.c2 DB_TYPE='mysql'`)
		r.NoError(err)

		is1, err := storage.QueryDBInfoSchema(db, "test")
		r.NoError(err)

		// NOTE: create view does not support a table has two column like c1 and C1
		_, err = runSQLWithInfoschema(ctx, `CREATE VIEW test.view1 AS SELECT tn.c1 + tn.c2 as view1_c1 FROM test.new_table as tn`, is1)
		r.NoError(err)
		_, err = runSQLWithInfoschema(ctx, `CREATE VIEW test.view2 AS SELECT new_table.c2 FROM test.new_table`, is1)
		r.NoError(err)

		// create view with same name, failed
		_, err = runSQLWithInfoschema(ctx, `CREATE VIEW test.view2 AS SELECT new_table.c2 FROM test.new_table`, is1)
		r.Error(err)

		// create or replace view the same name, success
		_, err = runSQLWithInfoschema(ctx, `CREATE OR REPLACE VIEW test.view2 AS SELECT new_table.c2 as view2_c1 FROM test.new_table`, is1)
		r.NoError(err)

		// create view from views
		is2, err := storage.QueryDBInfoSchema(db, "test")
		r.NoError(err)
		_, err = runSQLWithInfoschema(ctx, `CREATE VIEW test.view3 AS SELECT view1.view1_c1 + view2.view2_c1 as c4 FROM test.view1 JOIN test.view2`, is2)
		r.NoError(err)

		// show full table
		rt, err := runSQL(ctx, `SHOW FULL TABLES FROM test`)
		r.NoError(err)
		r.Equal(int64(1), rt[0].GetShape().GetDim()[1].GetDimValue())
		r.Equal(int64(4), rt[0].GetShape().GetDim()[0].GetDimValue())
		r.Equal("Tables_in_test", rt[0].GetName())
		r.Equal("new_table", rt[0].GetStringData()[0])
		r.Equal("view1", rt[0].GetStringData()[1])
		r.Equal("view2", rt[0].GetStringData()[2])
		r.Equal("view3", rt[0].GetStringData()[3])
		r.Equal("Table_type", rt[1].GetName())
		r.Equal(TableTypeBase, rt[1].GetStringData()[0])
		r.Equal(TableTypeView, rt[1].GetStringData()[1])
		r.Equal(TableTypeView, rt[1].GetStringData()[2])
		r.Equal(TableTypeView, rt[1].GetStringData()[3])

		is3, err := storage.QueryDBInfoSchema(db, "test")
		r.NoError(err)

		// describe view
		rt, err = runSQLWithInfoschema(ctx, `DESCRIBE test.view3`, is3)
		r.NoError(err)
		r.Equal(int64(1), rt[0].GetShape().GetDim()[0].GetDimValue())
		r.Equal(int64(1), rt[0].GetShape().GetDim()[1].GetDimValue())
		r.Equal("Field", rt[0].GetName())
		r.Equal("c4", rt[0].GetStringData()[0])
		r.Equal("Type", rt[1].GetName())
		r.Equal("int", rt[1].GetStringData()[0])

		// drop view
		_, err = runSQL(ctx, `DROP VIEW test.view2`)
		r.NoError(err)

		// show full table again
		rt, err = runSQL(ctx, `SHOW FULL TABLES FROM test`)
		r.NoError(err)
		r.Equal(int64(3), rt[0].GetShape().GetDim()[0].GetDimValue())
	}
}

func TestCreateTable(t *testing.T) {
	r := require.New(t)

	db, err := mockStorage()
	r.NoError(err)

	{
		ctx := mockContext()
		r.NoError(mockUserSession(ctx, userRoot, db))

		// root> create user alice
		_, err = runSQL(ctx, `CREATE USER alice PARTY_CODE "party_A" IDENTIFIED BY "some_pwd"`)
		r.NoError(err)

		// root> grant create on *.* to alice
		_, err = runSQL(ctx, `GRANT CREATE ON *.* TO alice`)
		r.NoError(err)

		// switch to user alice
		r.NoError(switchUser(ctx, userAlice))

		// success
		_, err = runSQL(ctx, `CREATE DATABASE test`)
		r.NoError(err)

		// success
		_, err = runSQL(ctx, `CREATE TABLE test.new_table (
			c1 int,
			c2 int
		) REF_TABLE=d1.t1 DB_TYPE='MYSQL'`)
		r.NoError(err)

		// failed due to database doesn't exists
		_, err = runSQL(ctx, `CREATE TABLE i_dont_exists.new_table (
			c1 int,
			c2 int
		) REF_TABLE=d1.t1 DB_TYPE='MYSQL'`)
		r.Error(err)

		// failed due to table already exists
		_, err = runSQL(ctx, `CREATE TABLE test.new_table (
			c1 int,
			c2 int
		) REF_TABLE=d1.t1 DB_TYPE='MYSQL'`)
		r.Error(err)

		// success on if not exists
		_, err = runSQL(ctx, `CREATE TABLE IF NOT EXISTS test.new_table (
			c1 int,
			c2 int
		) REF_TABLE=d1.t1 DB_TYPE='MYSQL'`)
		r.NoError(err)

		// failed due to missing db_name
		_, err = runSQL(ctx, `CREATE TABLE new_table_2 (
			c1 string,
			c2 int,
		) REF_TABLE=d1.t2 DB_TYPE='POSTGRESQL'`)
		r.Error(err)

		// use database test
		ctx.GetSessionVars().CurrentDB = "test"
		_, err = runSQL(ctx, `CREATE TABLE new_table_2 (
			c1 string,
			c2 int
		) REF_TABLE=d1.t2 DB_TYPE='POSTGRESQL'`)
		r.NoError(err)
	}

	{
		ctx := mockContext()
		r.NoError(mockUserSession(ctx, userRoot, db))

		// create user success
		_, err = runSQL(ctx, `CREATE USER bob PARTY_CODE "party_B" IDENTIFIED BY "some_pwd"`)
		r.NoError(err)

		// switch to user bob
		r.NoError(switchUser(ctx, userBob))

		// failed due to lack of privilege
		_, err = runSQL(ctx, `CREATE TABLE test.ant_table (
			c1 int64,
			c2 float,
			c3 double
		) REF_TABLE=bob_db.tbl2 DB_TYPE='csvdb'`)
		r.Error(err)

		r.NoError(switchUser(ctx, userRoot))

		_, err = runSQL(ctx, `GRANT CREATE ON test.* TO bob`)
		r.NoError(err)

		r.NoError(switchUser(ctx, userBob))

		_, err = runSQL(ctx, `CREATE TABLE test.bob_table (
			c1 int64,
			c2 float,
			c3 double
		) REF_TABLE=bob_db.tbl2 DB_TYPE='csvdb'`)
		r.NoError(err)

	}
}

func TestDropTable(t *testing.T) {
	r := require.New(t)

	db, err := mockStorage()
	r.NoError(err)

	ctx := mockContext()
	r.NoError(mockUserSession(ctx, userRoot, db))

	// root> create user alice
	_, err = runSQL(ctx, `CREATE USER alice PARTY_CODE "party_A" IDENTIFIED BY "some_pwd"`)
	r.NoError(err)

	// root> create user alice
	_, err = runSQL(ctx, `CREATE USER bob PARTY_CODE "party_B" IDENTIFIED BY "some_pwd"`)
	r.NoError(err)

	// success
	_, err = runSQL(ctx, `CREATE DATABASE test`)
	r.NoError(err)

	// root> grant create priv on test.* to alice
	_, err = runSQL(ctx, `GRANT CREATE, GRANT OPTION, DROP ON test.* TO alice`)
	r.NoError(err)

	// root> grant create priv on test.* to bob
	_, err = runSQL(ctx, `GRANT CREATE, GRANT OPTION, DROP ON test.* TO bob`)
	r.NoError(err)

	// switch to user alice
	r.NoError(switchUser(ctx, userAlice))

	// success
	_, err = runSQL(ctx, `CREATE TABLE test.t1 (
		id str,
		c2 int
	) REF_TABLE=d1.t1 DB_TYPE='sqlite'`)
	r.NoError(err)

	// switch to user bob
	r.NoError(switchUser(ctx, userBob))

	// success
	_, err = runSQL(ctx, `CREATE TABLE test.t3 (
		id str,
		c2 int
	) REF_TABLE=d1.t1 DB_TYPE='sqlite'`)
	r.NoError(err)

	// switch to user alice
	r.NoError(switchUser(ctx, userAlice))

	// success
	_, err = runSQL(ctx, `DROP TABLE test.t1`)
	r.NoError(err)

	// failed due to alice is not the owner of test.t3
	_, err = runSQL(ctx, `DROP TABLE test.t3`)
	r.Error(err)

	// failed due to new_table doesn't exist
	_, err = runSQL(ctx, `DROP TABLE test.new_table`)
	r.Error(err)

	// success
	_, err = runSQL(ctx, `DROP TABLE IF EXISTS test.new_table`)
	r.NoError(err)
}

func TestGrantDbScope(t *testing.T) {

	t.Run("NormalGrantTest", func(t *testing.T) {
		r := require.New(t)
		ctx := setupTestEnv(t)

		// failed due to user `alice` not exists, not allowed create user with GRANT
		_, err := runSQL(ctx, "GRANT CREATE ON da.* TO alice")
		r.Error(err)

		_, err = runSQL(ctx, "GRANT UNKNOWN ON da.* TO alice")
		r.Error(err)

		_, err = runSQL(ctx, `CREATE USER alice PARTY_CODE "party_A" IDENTIFIED BY "some_pwd"`)
		r.NoError(err)

		_, err = runSQL(ctx, `CREATE USER bob PARTY_CODE "party_B" IDENTIFIED BY "some_pwd"`)
		r.NoError(err)

		// failed due to grant priv to not exist database
		_, err = runSQL(ctx, "GRANT CREATE ON not_exist.* TO alice")
		r.Error(err)

		_, err = runSQL(ctx, "CREATE DATABASE da")
		r.NoError(err)

		_, err = runSQL(ctx, "GRANT CREATE ON da.* TO alice")
		r.NoError(err)

		_, err = runSQL(ctx, "GRANT CREATE, DROP, GRANT OPTION, DESCRIBE, SHOW ON da.* TO alice")
		r.NoError(err)

		r.NoError(switchUser(ctx, userAlice))

		// failed due to grant priv to root who doesn't belong to any party
		_, err = runSQL(ctx, "GRANT CREATE ON da.* TO root")
		r.Error(err)

		// failed due to grant priv to another party
		_, err = runSQL(ctx, "GRANT CREATE ON da.* TO bob")
		r.Error(err)

	})

	t.Run("CreateTableBeforeAndAfterGrantCreatePrivilege", func(t *testing.T) {
		r := require.New(t)
		ctx := setupTestEnv(t)

		// root> create user alice
		_, err := runSQL(ctx, `CREATE USER alice PARTY_CODE "party_A" IDENTIFIED BY "some_pwd"`)
		r.NoError(err)

		// success
		_, err = runSQL(ctx, `CREATE DATABASE test`)
		r.NoError(err)

		// switch to user alice
		r.NoError(switchUser(ctx, userAlice))

		// switch back to user root
		r.NoError(switchUser(ctx, userRoot))

		// grant create privilege to alice on database test
		_, err = runSQL(ctx, `GRANT CREATE ON test.* TO alice`)
		r.NoError(err)

		// switch to user alice
		r.NoError(switchUser(ctx, userAlice))

	})

	t.Run("GrantALL", func(t *testing.T) {
		r := require.New(t)
		ctx := setupTestEnv(t)

		// root> create user alice
		_, err := runSQL(ctx, `CREATE USER alice PARTY_CODE "party_A" IDENTIFIED BY "some_pwd"`)
		r.NoError(err)

		// root> create database da.*
		_, err = runSQL(ctx, `CREATE DATABASE da`)
		r.NoError(err)

		// root> grant all on da.* to alice
		_, err = runSQL(ctx, `GRANT ALL ON da.* to alice`)
		r.NoError(err)
	})
}

func TestGrantGlobalScope(t *testing.T) {
	t.Run("GrantAll", func(t *testing.T) {
		r := require.New(t)
		ctx := setupTestEnv(t)

		// root> create user alice
		_, err := runSQL(ctx, `CREATE USER alice PARTY_CODE "party_A" IDENTIFIED BY "some_pwd"`)
		r.NoError(err)

		_, err = runSQL(ctx, `CREATE USER bob PARTY_CODE "party_B" IDENTIFIED BY "some_pwd"`)
		r.NoError(err)

		// switch to user alice
		r.NoError(switchUser(ctx, userAlice))

		// alice> create database da -- failed due to no create privilege
		_, err = runSQL(ctx, `CREATE DATABASE da`)
		r.Error(err)

		// switch to user root
		r.NoError(switchUser(ctx, userRoot))

		// root> GRANT ALL on *.* to alice
		_, err = runSQL(ctx, `GRANT ALL ON *.* to alice`)
		r.NoError(err)

		// switch to user alice
		r.NoError(switchUser(ctx, userAlice))

		// failed due to grant to root who doesn't belong to any party
		_, err = runSQL(ctx, "GRANT CREATE ON *.* TO root")
		r.Error(err)

		// failed due to grant priv to another party
		_, err = runSQL(ctx, "GRANT CREATE ON *.* TO bob")
		r.Error(err)

		// alice> create database da -- success
		_, err = runSQL(ctx, `CREATE DATABASE da`)
		r.NoError(err)

		// alice> create table da.t1 -- success
		_, err = runSQL(ctx, `CREATE TABLE da.t1 (id string) REF_TABLE=d1.t1 DB_TYPE='mysql'`)
		r.NoError(err)

		// alice> delete table da.t1 -- success
		_, err = runSQL(ctx, `DROP TABLE da.t1`)
		r.NoError(err)

		// alice> create user bob -- success
		_, err = runSQL(ctx, `CREATE USER carol PARTY_CODE "party_c" IDENTIFIED BY "some_pwd"`)
		r.NoError(err)
	})
}

func TestGrantTableScope(t *testing.T) {
	t.Run("GrantCreateDropOnTable", func(t *testing.T) {
		r := require.New(t)
		ctx := setupTestEnv(t)

		// root> create database da
		_, err := runSQL(ctx, `CREATE DATABASE da`)
		r.NoError(err)

		// root> create user alice
		_, err = runSQL(ctx, `CREATE USER alice PARTY_CODE "party_A" IDENTIFIED BY "some_pwd"`)
		r.NoError(err)

		// root> create user bob
		_, err = runSQL(ctx, `CREATE USER bob PARTY_CODE "party_B" IDENTIFIED BY "some_pwd"`)
		r.NoError(err)

		// root> grant create, grant option priv on *.* to alice
		_, err = runSQL(ctx, `GRANT CREATE, DROP, GRANT OPTION ON da.* TO alice`)
		r.NoError(err)

		// switch to user alice
		r.NoError(switchUser(ctx, userAlice))

		// alice> create table da.t1
		_, err = runSQL(ctx, `CREATE TABLE da.t1 (id str) REF_TABLE=dba.tbl1 DB_TYPE='mysql'`)
		r.NoError(err)

		// switch to user Bob
		r.NoError(switchUser(ctx, userBob))

		// alice> drop table da.t1 -- failed due to no drop privilege on da.t1
		_, err = runSQL(ctx, `DROP TABLE da.t1`)
		r.Error(err)

		// switch to user alice
		r.NoError(switchUser(ctx, userAlice))

		// failed due to grant priv to another party
		_, err = runSQL(ctx, `GRANT CREATE, DROP ON da.t1 TO bob`)
		r.Error(err)
	})
	t.Run("GrantVisibilityPrivilege", func(t *testing.T) {
		r := require.New(t)
		ctx := setupTestEnv(t)

		// root> create user alice
		_, err := runSQL(ctx, `CREATE USER alice PARTY_CODE "party_A" IDENTIFIED BY "some_pwd"`)
		r.NoError(err)

		// root> create user bob
		_, err = runSQL(ctx, `CREATE USER bob PARTY_CODE "party_B" IDENTIFIED BY "some_pwd"`)
		r.NoError(err)

		// root> create database da
		_, err = runSQL(ctx, `CREATE DATABASE da`)
		r.NoError(err)

		// root> grant create, grant option priv on *.* to alice
		_, err = runSQL(ctx, `GRANT CREATE, GRANT OPTION ON *.* TO alice`)
		r.NoError(err)

		// switch to user alice
		r.NoError(switchUser(ctx, userAlice))

		// alice> create table da.t1
		_, err = runSQL(ctx, `CREATE TABLE da.t1 (c1 int) REF_TABLE=dba.tbl1 DB_TYPE='mysql'`)
		r.NoError(err)

		// alice> GRANT SELECT PLAINTEXT on da.t1 to bob
		_, err = runSQL(ctx, `GRANT SELECT PLAINTEXT ON da.t1 TO bob`)
		r.NoError(err)

		_, err = runSQL(ctx, `GRANT SELECT PLAINTEXT(c1) ON da.t1 TO bob`)
		r.NoError(err)

		_, err = runSQL(ctx, `GRANT SELECT PLAINTEXT, SELECT PLAINTEXT(c1) ON da.t1 TO bob`)
		r.NoError(err)

		// failed due to grant priv to another party
		_, err = runSQL(ctx, `GRANT CREATE, SELECT PLAINTEXT(c1) ON da.t1 TO bob`)
		r.Error(err)

		// failed due to grant priv to another party
		_, err = runSQL(ctx, `GRANT CREATE, SELECT ENCRYPTED_ONLY ON da.t1 TO bob`)
		r.Error(err)

		// switch to user root
		r.NoError(switchUser(ctx, userRoot))

		// root grant multiple privileges to `bob`
		_, err = runSQL(ctx, "GRANT CREATE, DROP, GRANT OPTION, DESCRIBE, SHOW ON da.* TO bob")
		r.NoError(err)

		// root> (switch to user bob)
		r.NoError(switchUser(ctx, userBob))

		// bob> GRANT SELECT PLAINTEXT(c1) on da.t1 to bob
		_, err = runSQL(ctx, `GRANT SELECT PLAINTEXT(c1) ON da.t1 TO bob`)
		r.Error(err) // error: bob is not the table owner

		// bob> GRANT SELECT PLAINTEXT on da.t1 to bob
		_, err = runSQL(ctx, `GRANT SELECT PLAINTEXT ON da.t1 TO bob`)
		r.Error(err) // error: bob is not the table owner
	})
}

func TestRevokeColumnScope(t *testing.T) {
	t.Run("RevokeColumnPrivilege", func(t *testing.T) {
		r := require.New(t)
		ctx := setupTestEnv(t)

		// root> create user alice
		_, err := runSQL(ctx, `CREATE USER alice PARTY_CODE "party_A" IDENTIFIED BY "some_pwd"`)
		r.NoError(err)

		// root> create user bob
		_, err = runSQL(ctx, `CREATE USER bob PARTY_CODE "party_B" IDENTIFIED BY "some_pwd"`)
		r.NoError(err)

		// root> create database da
		_, err = runSQL(ctx, `CREATE DATABASE da`)
		r.NoError(err)

		// root> grant create, grant option priv on *.* to alice
		_, err = runSQL(ctx, `GRANT CREATE, GRANT OPTION ON *.* TO alice`)
		r.NoError(err)

		// switch to user alice
		r.NoError(switchUser(ctx, userAlice))

		// alice> create table da.t1
		_, err = runSQL(ctx, `CREATE TABLE da.t1 (c1 int) REF_TABLE=dba.tbl1 DB_TYPE='mysql'`)
		r.NoError(err)

		_, err = runSQL(ctx, `GRANT SELECT PLAINTEXT(c1) ON da.t1 TO bob`)
		r.NoError(err)

		_, err = runSQL(ctx, `REVOKE SELECT PLAINTEXT_AFTER_AGGREGATE(c1) ON da.t1 FROM bob`)
		r.Error(err) // error: there is no SELECT PLAINTEXT_AFTER_AGGREGATE defined for user 'bob' on host '%' on table 't1'

		rt, err := runSQL(ctx, `SHOW GRANTS ON da FOR bob`)
		r.NoError(err)
		r.Equal(int64(1), rt[0].GetShape().GetDim()[1].GetDimValue())
		r.Equal(int64(1), rt[0].GetShape().GetDim()[0].GetDimValue())
		r.Equal("Grants on da for bob@%", rt[0].GetName())
		r.Equal("GRANT SELECT PLAINTEXT(c1) ON da.t1 TO bob", rt[0].GetStringData()[0])

		_, err = runSQL(ctx, `REVOKE SELECT PLAINTEXT(c1) ON da.t1 FROM bob`)
		r.NoError(err)
		rt, err = runSQL(ctx, `SHOW GRANTS ON da FOR bob`)
		r.NoError(err)
		r.Equal(int64(1), rt[0].GetShape().GetDim()[1].GetDimValue())
		r.Equal(int64(0), rt[0].GetShape().GetDim()[0].GetDimValue())
		r.Equal("Grants on da for bob@%", rt[0].GetName())
	})
	t.Run("RevokeColumnPrivilegeCheckTableOwner", func(t *testing.T) {
		r := require.New(t)
		ctx := setupTestEnv(t)

		// root> create user alice
		_, err := runSQL(ctx, `CREATE USER alice PARTY_CODE "party_A" IDENTIFIED BY "some_pwd"`)
		r.NoError(err)

		// root> create user bob
		_, err = runSQL(ctx, `CREATE USER bob PARTY_CODE "party_B" IDENTIFIED BY "some_pwd"`)
		r.NoError(err)

		// root> create database da
		_, err = runSQL(ctx, `CREATE DATABASE da`)
		r.NoError(err)

		// root> grant create, grant priv on *.* to alice
		_, err = runSQL(ctx, `GRANT CREATE, GRANT OPTION ON da.* TO alice`)
		r.NoError(err)

		// switch to user alice
		r.NoError(switchUser(ctx, userAlice))

		// alice> create table da.t1
		_, err = runSQL(ctx, `CREATE TABLE da.t1 (c1 int) REF_TABLE=dba.tbl1 DB_TYPE='mysql'`)
		r.NoError(err)
		_, err = runSQL(ctx, `GRANT SELECT PLAINTEXT(c1) ON da.t1 TO bob`)
		r.NoError(err)

		// alice> (switch to user root)
		r.NoError(switchUser(ctx, userRoot))

		_, err = runSQL(ctx, `GRANT CREATE, DROP, GRANT OPTION ON da.* TO bob`)
		r.NoError(err)

		// alice> (switch to user bob)
		r.NoError(switchUser(ctx, userBob))

		// bob> REVOKE SELECT PLAINTEXT(c1) ON da.t1 FROM bob
		_, err = runSQL(ctx, `REVOKE SELECT PLAINTEXT(c1) ON da.t1 FROM bob`)
		r.Error(err) // error: user bob is not the owner of table da.t1
	})
}

func TestRevokeTableScope(t *testing.T) {
	r := require.New(t)
	ctx := setupTestEnv(t)

	// root> create user alice
	_, err := runSQL(ctx, `CREATE USER alice PARTY_CODE "party_A" IDENTIFIED BY "some_pwd"`)
	r.NoError(err)

	// root> create user bob
	_, err = runSQL(ctx, `CREATE USER bob PARTY_CODE "party_B" IDENTIFIED BY "some_pwd"`)
	r.NoError(err)

	// root> create database da
	_, err = runSQL(ctx, `CREATE DATABASE da`)
	r.NoError(err)

	// root> grant create, grant option priv on da.* to alice
	_, err = runSQL(ctx, `GRANT ALL ON da.* TO alice`)
	r.NoError(err)

	// switch to user alice
	r.NoError(switchUser(ctx, userAlice))

	// alice> create table da.t1
	_, err = runSQL(ctx, `CREATE TABLE da.t1 (c1 int, c2 int) REF_TABLE=d1.t1 DB_TYPE='mysql'`)
	r.NoError(err)

	// alice> grant SELECT PLAINTEXT priv to alice
	_, err = runSQL(ctx, `GRANT ALL, SELECT PLAINTEXT on da.t1 TO alice`)
	r.NoError(err)

	// alice> grant SELECT PLAINTEXT priv to bob
	_, err = runSQL(ctx, `GRANT SELECT PLAINTEXT on da.t1 TO bob`)
	r.NoError(err)

	rt, err := runSQL(ctx, `SHOW GRANTS ON da FOR alice`)
	r.NoError(err)
	r.Equal(int64(1), rt[0].GetShape().GetDim()[1].GetDimValue())
	r.Equal(int64(2), rt[0].GetShape().GetDim()[0].GetDimValue())
	r.Equal("Grants on da for alice@%", rt[0].GetName())
	r.Equal("GRANT CREATE, DROP, GRANT OPTION ON da.* TO alice", rt[0].GetStringData()[0])
	r.Equal("GRANT CREATE, GRANT, DROP, SELECT PLAINTEXT ON da.t1 TO alice", rt[0].GetStringData()[1])

	// alice> revoke CREATE from alice, success
	_, err = runSQL(ctx, `REVOKE CREATE ON da.t1 FROM alice`)
	r.NoError(err)

	// alice> revoke CREATE from alice again, success even it's not exist
	_, err = runSQL(ctx, `REVOKE CREATE ON da.t1 FROM alice`)
	r.NoError(err)

	// alice> revoke ENCRYPTED_ONLY from alice again, success even it's not exist
	_, err = runSQL(ctx, `REVOKE SELECT ENCRYPTED_ONLY ON da.t1 FROM alice`)
	r.NoError(err)

	// alice> revoke priv from bob, failed due to not support revoke priv from another party
	_, err = runSQL(ctx, `REVOKE CREATE ON da.t1 FROM bob`)
	r.Error(err)

	// alice> revoke SELECT PLAINTEXT priv from bob
	_, err = runSQL(ctx, `REVOKE SELECT PLAINTEXT ON da.t1 FROM bob`)
	r.NoError(err)
}

func TestRevokeDBScope(t *testing.T) {
	r := require.New(t)
	ctx := setupTestEnv(t)

	// root> create user alice
	_, err := runSQL(ctx, `CREATE USER alice PARTY_CODE "party_A" IDENTIFIED BY "some_pwd"`)
	r.NoError(err)

	// root> create user bob
	_, err = runSQL(ctx, `CREATE USER bob PARTY_CODE "party_B" IDENTIFIED BY "some_pwd"`)
	r.NoError(err)

	// root> create database da
	_, err = runSQL(ctx, `CREATE DATABASE da`)
	r.NoError(err)

	// root> grant create, grant option priv on da.* to alice
	_, err = runSQL(ctx, `GRANT ALL ON da.* TO alice`)
	r.NoError(err)

	// root> grant create, grant option priv on da.* to  bob
	_, err = runSQL(ctx, `GRANT ALL ON da.* TO bob`)
	r.NoError(err)

	rt, err := runSQL(ctx, `SHOW GRANTS ON da FOR alice`)
	r.NoError(err)
	r.Equal(int64(1), rt[0].GetShape().GetDim()[1].GetDimValue())
	r.Equal(int64(1), rt[0].GetShape().GetDim()[0].GetDimValue())
	r.Equal("Grants on da for alice@%", rt[0].GetName())
	r.Equal("GRANT CREATE, DROP, GRANT OPTION ON da.* TO alice", rt[0].GetStringData()[0])

	_, err = runSQL(ctx, `REVOKE CREATE ON da.* FROM alice`)
	r.NoError(err)
	rt, err = runSQL(ctx, `SHOW GRANTS ON da FOR alice`)
	r.NoError(err)
	r.Equal(int64(1), rt[0].GetShape().GetDim()[1].GetDimValue())
	r.Equal(int64(1), rt[0].GetShape().GetDim()[0].GetDimValue())
	r.Equal("GRANT DROP, GRANT OPTION ON da.* TO alice", rt[0].GetStringData()[0])

	r.NoError(switchUser(ctx, userAlice))

	// failed due to not allowed to revoke priv from another party
	_, err = runSQL(ctx, `REVOKE GRANT OPTION ON da.* FROM bob`)
	r.Error(err)

	r.NoError(switchUser(ctx, userRoot))
	_, err = runSQL(ctx, `REVOKE ALL ON da.* FROM alice`)
	r.NoError(err)

	rt, err = runSQL(ctx, `SHOW GRANTS ON da FOR alice`)
	r.NoError(err)
	r.Equal(int64(1), rt[0].GetShape().GetDim()[1].GetDimValue())
	r.Equal(int64(0), rt[0].GetShape().GetDim()[0].GetDimValue())

}

func TestRevokeGlobalScope(t *testing.T) {
	r := require.New(t)
	ctx := setupTestEnv(t)

	// root> create user alice
	_, err := runSQL(ctx, `CREATE USER alice PARTY_CODE "party_A" IDENTIFIED BY "some_pwd"`)
	r.NoError(err)

	// root> create user alice
	_, err = runSQL(ctx, `CREATE USER bob PARTY_CODE "party_B" IDENTIFIED BY "some_pwd"`)
	r.NoError(err)

	// root> create database da
	_, err = runSQL(ctx, `CREATE DATABASE da`)
	r.NoError(err)

	// root> grant create, create user, grant option, drop priv on *.* to alice
	_, err = runSQL(ctx, `GRANT ALL ON *.* TO alice`)
	r.NoError(err)

	// root> grant create, grant option, drop priv on da.* to alice
	_, err = runSQL(ctx, `GRANT ALL ON da.* TO alice`)
	r.NoError(err)

	// root> grant create, grant option, drop priv on da.* to bob
	_, err = runSQL(ctx, `GRANT ALL ON *.* TO bob`)
	r.NoError(err)

	// switch to user alice
	r.NoError(switchUser(ctx, userAlice))

	rt, err := runSQL(ctx, `SHOW GRANTS ON da FOR alice`)
	r.NoError(err)
	r.Equal(int64(1), rt[0].GetShape().GetDim()[1].GetDimValue())
	r.Equal(int64(2), rt[0].GetShape().GetDim()[0].GetDimValue())
	r.Equal("Grants on da for alice@%", rt[0].GetName())
	r.Equal("GRANT CREATE, CREATE USER, DROP, GRANT OPTION ON *.* TO alice", rt[0].GetStringData()[0])
	r.Equal("GRANT CREATE, DROP, GRANT OPTION ON da.* TO alice", rt[0].GetStringData()[1])

	_, err = runSQL(ctx, `REVOKE CREATE, CREATE USER ON *.* FROM alice`)
	r.NoError(err)
	rt, err = runSQL(ctx, `SHOW GRANTS ON da FOR alice`)
	r.NoError(err)
	r.Equal(int64(1), rt[0].GetShape().GetDim()[1].GetDimValue())
	r.Equal(int64(2), rt[0].GetShape().GetDim()[0].GetDimValue())
	r.Equal("GRANT DROP, GRANT OPTION ON *.* TO alice", rt[0].GetStringData()[0])
	r.Equal("GRANT CREATE, DROP, GRANT OPTION ON da.* TO alice", rt[0].GetStringData()[1])

	// alice revoke global priv from bob, failed due to no privilege
	_, err = runSQL(ctx, `REVOKE CREATE ON *.* FROM bob`)
	r.Error(err)

	// failed due to not allowed to revoke priv from another party
	_, err = runSQL(ctx, `REVOKE DROP ON *.* FROM bob`)
	r.Error(err)

	// failed due to alice has no CREATE, CREATE USER  privilege
	_, err = runSQL(ctx, `REVOKE CREATE, CREATE USER ON *.* FROM alice`)
	r.Error(err)

	// failed due to not allowed to revoke priv from root
	_, err = runSQL(ctx, `REVOKE DROP ON *.* FROM root`)
	r.Error(err)

	// switch to user root
	r.NoError(switchUser(ctx, userRoot))

	// success even alice has no CREATE, CREATE USER  privilege
	_, err = runSQL(ctx, `REVOKE CREATE, CREATE USER ON *.* FROM alice`)
	r.NoError(err)

	_, err = runSQL(ctx, `REVOKE ALL ON *.* FROM alice`)
	r.NoError(err)

	rt, err = runSQL(ctx, `SHOW GRANTS ON da FOR alice`)
	r.NoError(err)
	r.Equal(int64(1), rt[0].GetShape().GetDim()[1].GetDimValue())
	r.Equal(int64(1), rt[0].GetShape().GetDim()[0].GetDimValue())
	r.Equal("GRANT CREATE, DROP, GRANT OPTION ON da.* TO alice", rt[0].GetStringData()[0])
}

func TestShowDatabase(t *testing.T) {
	r := require.New(t)
	ctx := setupTestEnv(t)

	rt, err := runSQL(ctx, `SHOW DATABASES`)
	r.NoError(err)
	r.Equal(int64(0), rt[0].GetShape().GetDim()[0].GetDimValue())

	_, err = runSQL(ctx, `CREATE DATABASE da`)
	r.NoError(err)
	_, err = runSQL(ctx, `CREATE DATABASE db`)
	r.NoError(err)

	rt, err = runSQL(ctx, `SHOW DATABASES`)
	r.NoError(err)
	r.Equal(int64(2), rt[0].GetShape().GetDim()[0].GetDimValue())
	r.Equal("Database", rt[0].GetName())
	r.Equal("da", rt[0].GetStringData()[0])
	r.Equal("db", rt[0].GetStringData()[1])

	// root> (create user alice before granting privilege)
	_, err = runSQL(ctx, `CREATE USER alice PARTY_CODE "party_A" IDENTIFIED BY "some_pwd"`)
	r.NoError(err)
	_, err = runSQL(ctx, `GRANT CREATE ON da.* to alice`)
	r.NoError(err)

	// root> (switch to user alice)
	r.NoError(switchUser(ctx, userAlice))

	// alice>
	rt, err = runSQL(ctx, `SHOW DATABASES`)
	r.NoError(err)
	r.Equal(int64(1), rt[0].GetShape().GetDim()[0].GetDimValue())
	r.Equal("Database", rt[0].GetName())
	r.Equal("da", rt[0].GetStringData()[0])
}

func TestShow(t *testing.T) {
	r := require.New(t)
	ctx := setupTestEnv(t)

	_, err := runSQL(ctx, `CREATE DATABASE da`)
	r.NoError(err)
	_, err = runSQL(ctx, `CREATE DATABASE db`)
	r.NoError(err)

	_, err = runSQL(ctx, `CREATE USER alice PARTY_CODE "party_A" IDENTIFIED BY "some_pwd"`)
	r.NoError(err)
	_, err = runSQL(ctx, `CREATE USER bob PARTY_CODE "party_B" IDENTIFIED BY "some_pwd"`)
	r.NoError(err)

	_, err = runSQL(ctx, `GRANT CREATE ON da.* to alice`)
	r.NoError(err)
	_, err = runSQL(ctx, `GRANT SHOW ON db.* to bob`)
	r.NoError(err)

	// root> (switch to user alice)
	r.NoError(switchUser(ctx, userAlice))

	rt, err := runSQL(ctx, `SHOW DATABASES`)
	r.NoError(err)
	r.Equal(int64(1), rt[0].GetShape().GetDim()[0].GetDimValue())
	r.Equal("Database", rt[0].GetName())
	r.Equal("da", rt[0].GetStringData()[0])

	rt, err = runSQL(ctx, `SHOW GRANTS ON da;`)
	r.NoError(err)
	r.Equal(int64(1), rt[0].GetShape().GetDim()[0].GetDimValue())

	rt, err = runSQL(ctx, `SHOW GRANTS ON db;`)
	r.NoError(err)
	r.Equal(int64(0), rt[0].GetShape().GetDim()[0].GetDimValue())

	// root> (switch to user bob)
	r.NoError(switchUser(ctx, userBob))

	// bob>
	rt, err = runSQL(ctx, `SHOW DATABASES`)
	r.NoError(err)
	r.Equal(int64(1), rt[0].GetShape().GetDim()[0].GetDimValue())
	r.Equal("Database", rt[0].GetName())
	r.Equal("db", rt[0].GetStringData()[0])

	rt, err = runSQL(ctx, `SHOW GRANTS ON db;`)
	r.NoError(err)
	r.Equal(int64(1), rt[0].GetShape().GetDim()[0].GetDimValue())

	rt, err = runSQL(ctx, `SHOW GRANTS ON da;`)
	r.NoError(err)
	r.Equal(int64(0), rt[0].GetShape().GetDim()[0].GetDimValue())
}

func TestDescribe(t *testing.T) {
	is := infoschema.MockInfoSchema(
		map[string][]*model.TableInfo{
			"da": {
				{
					Name: model.NewCIStr("t1"),
					Columns: []*model.ColumnInfo{
						{
							Name:      model.NewCIStr("c1"),
							FieldType: *types.NewFieldType(mysql.TypeString),
							Comment:   "string value",
						},
						{
							Name:      model.NewCIStr("c2"),
							FieldType: *types.NewFieldType(mysql.TypeLong),
							Comment:   "long value",
						},
					},
				},
			},
			"db": {
				{
					Name: model.NewCIStr("t1"),
					Columns: []*model.ColumnInfo{
						{
							Name:      model.NewCIStr("c1"),
							FieldType: *types.NewFieldType(mysql.TypeString),
							Comment:   "string value",
						},
						{
							Name:      model.NewCIStr("c2"),
							FieldType: *types.NewFieldType(mysql.TypeLong),
							Comment:   "long value",
						},
					},
				},
			},
		})
	r := require.New(t)
	ctx := setupTestEnv(t)

	_, err := runSQL(ctx, `CREATE DATABASE da`)
	r.NoError(err)
	_, err = runSQL(ctx, `CREATE DATABASE db`)
	r.NoError(err)

	_, err = runSQL(ctx, `CREATE USER alice PARTY_CODE "party_A" IDENTIFIED BY "some_pwd"`)
	r.NoError(err)
	_, err = runSQL(ctx, `CREATE USER bob PARTY_CODE "party_B" IDENTIFIED BY "some_pwd"`)
	r.NoError(err)

	_, err = runSQL(ctx, `GRANT CREATE ON da.* to alice`)
	r.NoError(err)
	_, err = runSQL(ctx, `GRANT SHOW ON db.* to alice`)
	r.NoError(err)
	_, err = runSQL(ctx, `GRANT DESCRIBE ON db.* to bob`)
	r.NoError(err)

	// switch to user alice
	r.NoError(switchUser(ctx, userAlice))

	_, err = runSQL(ctx, `CREATE TABLE da.t1 (c1 int, c2 int) REF_TABLE=d1.t1 DB_TYPE='mysql'`)
	r.NoError(err)

	// alice>
	rt, err := Run(ctx, `DESCRIBE da.t1`, is)
	r.NoError(err)
	r.Equal(int64(2), rt[0].GetShape().GetDim()[0].GetDimValue())

	_, err = Run(ctx, `DESCRIBE db.t1`, is)
	r.Error(err)

	// alice> (switch to user bob)
	r.NoError(switchUser(ctx, userBob))
	// bob>
	rt, err = Run(ctx, `DESCRIBE db.t1`, is)
	r.NoError(err)
	r.Equal(int64(2), rt[0].GetShape().GetDim()[0].GetDimValue())

	_, err = Run(ctx, `DESCRIBE da.t1`, is)
	r.Error(err)
}

func TestShowTables(t *testing.T) {
	r := require.New(t)
	ctx := setupTestEnv(t)

	_, err := runSQL(ctx, `CREATE DATABASE da`)
	r.NoError(err)
	// root> create user alice
	_, err = runSQL(ctx, `CREATE USER alice PARTY_CODE "party_A" IDENTIFIED BY "some_pwd"`)
	r.NoError(err)

	// root> grant create priv on da.* to alice
	_, err = runSQL(ctx, `GRANT CREATE ON da.* TO alice`)
	r.NoError(err)

	// switch to user alice
	r.NoError(switchUser(ctx, userAlice))

	// NO DB selected
	_, err = runSQL(ctx, `SHOW TABLES`)
	r.Error(err)

	rt, err := runSQL(ctx, `SHOW TABLES FROM da`)
	r.NoError(err)
	r.Equal(int64(0), rt[0].GetShape().GetDim()[0].GetDimValue())

	_, err = runSQL(ctx, `CREATE TABLE da.t1 (c1 int, c2 int) REF_TABLE=d1.t1 DB_TYPE='mysql'`)
	r.NoError(err)

	rt, err = runSQL(ctx, `SHOW TABLES FROM da`)
	r.NoError(err)
	r.Equal(int64(1), rt[0].GetShape().GetDim()[1].GetDimValue())
	r.Equal(int64(1), rt[0].GetShape().GetDim()[0].GetDimValue())
	r.Equal("Tables_in_da", rt[0].GetName())
	r.Equal("t1", rt[0].GetStringData()[0])

	rt, err = runSQL(ctx, `SHOW FULL TABLES FROM da`)
	r.NoError(err)
	r.Equal(int64(1), rt[0].GetShape().GetDim()[1].GetDimValue())
	r.Equal(int64(1), rt[0].GetShape().GetDim()[0].GetDimValue())
	r.Equal("Tables_in_da", rt[0].GetName())
	r.Equal("t1", rt[0].GetStringData()[0])
	r.Equal("Table_type", rt[1].GetName())
	r.Equal(TableTypeBase, rt[1].GetStringData()[0])

	// with current db set
	ctx.GetSessionVars().CurrentDB = `da`
	rt, err = runSQL(ctx, `SHOW TABLES`)
	r.NoError(err)
	r.Equal(int64(1), rt[0].GetShape().GetDim()[1].GetDimValue())
	r.Equal(int64(1), rt[0].GetShape().GetDim()[0].GetDimValue())
	r.Equal("Tables_in_da", rt[0].GetName())
	r.Equal("t1", rt[0].GetStringData()[0])

	r.NoError(switchUser(ctx, userRoot))
	// root> (create bob alice before granting privilege)
	_, err = runSQL(ctx, `CREATE USER bob PARTY_CODE "party_B" IDENTIFIED BY "some_pwd"`)
	r.NoError(err)
	// root> (switch to user bob)
	r.NoError(switchUser(ctx, userBob))

	// bob>
	rt, err = runSQL(ctx, `SHOW TABLES FROM da`)
	r.Error(err)
}

func TestShowGrants(t *testing.T) {
	r := require.New(t)
	ctx := setupTestEnv(t)

	// Report error when database not existing
	_, err := runSQL(ctx, `SHOW GRANTS ON da FOR root`)
	r.Error(err)

	_, err = runSQL(ctx, `CREATE DATABASE da`)
	r.NoError(err)
	_, err = runSQL(ctx, `CREATE DATABASE db`)
	r.NoError(err)
	_, err = runSQL(ctx, `CREATE DATABASE dc`)
	r.NoError(err)

	// Report grants on da for root
	rt, err := runSQL(ctx, `SHOW GRANTS ON da FOR root`)
	r.NoError(err)
	r.Equal(int64(1), rt[0].GetShape().GetDim()[1].GetDimValue())
	r.Equal(int64(1), rt[0].GetShape().GetDim()[0].GetDimValue())
	r.Equal("Grants on da for root@%", rt[0].GetName())
	r.Equal("GRANT CREATE, CREATE USER, DROP, GRANT OPTION, DESCRIBE, SHOW ON *.* TO root", rt[0].GetStringData()[0])

	_, err = runSQL(ctx, `CREATE USER alice PARTY_CODE "party_A" IDENTIFIED BY "some_pwd"`)
	r.NoError(err)
	_, err = runSQL(ctx, `CREATE USER bob PARTY_CODE "party_B" IDENTIFIED BY "some_pwd"`)
	r.NoError(err)

	_, err = runSQL(ctx, `GRANT CREATE, DROP, GRANT OPTION ON da.* TO alice`)
	r.NoError(err)
	_, err = runSQL(ctx, `GRANT CREATE, DROP, GRANT OPTION ON db.* TO alice`)
	r.NoError(err)
	rt, err = runSQL(ctx, `SHOW GRANTS ON da FOR alice`)
	r.NoError(err)
	r.Equal(int64(1), rt[0].GetShape().GetDim()[1].GetDimValue())
	r.Equal(int64(1), rt[0].GetShape().GetDim()[0].GetDimValue())
	r.Equal("Grants on da for alice@%", rt[0].GetName())
	r.Equal("GRANT CREATE, DROP, GRANT OPTION ON da.* TO alice", rt[0].GetStringData()[0])

	// Report error when database not existing
	_, err = runSQL(ctx, `SHOW GRANTS ON dd FOR root`)
	r.Error(err)

	// Return empty: invoker has no privilege on database db
	rt, err = runSQL(ctx, `SHOW GRANTS ON dc FOR alice`)
	r.NoError(err)
	r.Equal(int64(1), rt[0].GetShape().GetDim()[1].GetDimValue())
	r.Equal(int64(0), rt[0].GetShape().GetDim()[0].GetDimValue())
	r.Equal("Grants on dc for alice@%", rt[0].GetName())

	// Return empty: invoker has privilege on database da, but destination user bob not
	rt, err = runSQL(ctx, `SHOW GRANTS ON da FOR bob`)
	r.NoError(err)
	r.Equal(int64(1), rt[0].GetShape().GetDim()[1].GetDimValue())
	r.Equal(int64(0), rt[0].GetShape().GetDim()[0].GetDimValue())
	r.Equal("Grants on da for bob@%", rt[0].GetName())

	// Return empty for or no privilege
	rt, err = runSQL(ctx, `SHOW GRANTS ON da FOR alice`)
	r.NoError(err)
	r.Equal(int64(1), rt[0].GetShape().GetDim()[1].GetDimValue())
	r.Equal(int64(1), rt[0].GetShape().GetDim()[0].GetDimValue())
	r.Equal("Grants on da for alice@%", rt[0].GetName())
	r.Equal("GRANT CREATE, DROP, GRANT OPTION ON da.* TO alice", rt[0].GetStringData()[0])

	// switch to user alice
	r.NoError(switchUser(ctx, userAlice))

	userAlice.password = "some_pwd"
	rt, err = runSQL(ctx, `SHOW GRANTS ON da FOR alice`)
	r.NoError(err)
	r.Equal(int64(1), rt[0].GetShape().GetDim()[1].GetDimValue())
	r.Equal(int64(1), rt[0].GetShape().GetDim()[0].GetDimValue())
	r.Equal("Grants on da for alice@%", rt[0].GetName())
	r.Equal("GRANT CREATE, DROP, GRANT OPTION ON da.* TO alice", rt[0].GetStringData()[0])

	rt, err = runSQL(ctx, `SHOW GRANTS ON db FOR alice`)
	r.NoError(err)
	r.Equal(int64(1), rt[0].GetShape().GetDim()[1].GetDimValue())
	r.Equal(int64(1), rt[0].GetShape().GetDim()[0].GetDimValue())
	r.Equal("Grants on db for alice@%", rt[0].GetName())
	r.Equal("GRANT CREATE, DROP, GRANT OPTION ON db.* TO alice", rt[0].GetStringData()[0])

	rt, err = runSQL(ctx, `SHOW GRANTS ON db`)
	r.NoError(err)
	r.Equal(int64(1), rt[0].GetShape().GetDim()[1].GetDimValue())
	r.Equal(int64(1), rt[0].GetShape().GetDim()[0].GetDimValue())
	r.Equal("Grants on db for User", rt[0].GetName())
	r.Equal("GRANT CREATE, DROP, GRANT OPTION ON db.* TO alice", rt[0].GetStringData()[0])

	// Return empty: invoker has privilege on database da, but destination user bob not
	rt, err = runSQL(ctx, `SHOW GRANTS ON da FOR bob`)
	r.NoError(err)
	r.Equal(int64(1), rt[0].GetShape().GetDim()[1].GetDimValue())
	r.Equal(int64(0), rt[0].GetShape().GetDim()[0].GetDimValue())
	r.Equal("Grants on da for bob@%", rt[0].GetName())

	rt, err = runSQL(ctx, `SHOW GRANTS ON da FOR bob`)
	r.NoError(err)
	r.Equal(int64(1), rt[0].GetShape().GetDim()[1].GetDimValue())
	r.Equal(int64(0), rt[0].GetShape().GetDim()[0].GetDimValue())
	r.Equal("Grants on da for bob@%", rt[0].GetName())

	// root has no explicit privilege on da
	rt, err = runSQL(ctx, `SHOW GRANTS ON da FOR root`)
	r.NoError(err)
	r.Equal(int64(1), rt[0].GetShape().GetDim()[1].GetDimValue())
	r.Equal(int64(0), rt[0].GetShape().GetDim()[0].GetDimValue())
	r.Equal("Grants on da for root@%", rt[0].GetName())
}

func TestDescribeTable(t *testing.T) {
	r := require.New(t)
	ctx := setupTestEnv(t)

	is := infoschema.MockInfoSchema(
		map[string][]*model.TableInfo{
			"da": {
				{
					Name: model.NewCIStr("t1"),
					Columns: []*model.ColumnInfo{
						{
							Name:      model.NewCIStr("c1"),
							FieldType: *types.NewFieldType(mysql.TypeString),
							Comment:   "string value",
						},
						{
							Name:      model.NewCIStr("c2"),
							FieldType: *types.NewFieldType(mysql.TypeLong),
							Comment:   "long value",
						},
					},
				},
			},
		})

	// fail due to scdb not support explain stmt
	_, err := Run(ctx, "EXPLAIN SELECT * from da.t1", is)
	r.Error(err)

	result, err := Run(ctx, "DESCRIBE da.t1", is)
	r.NoError(err)
	r.Equal(2, len(result))

	r.Equal("Field", result[0].GetName())
	r.ElementsMatch([]string{"c1", "c2"}, result[0].GetStringData())

	r.Equal("Type", result[1].GetName())
	r.Equal(2, len(result[1].GetStringData()))
	r.ElementsMatch([]string{"string", "int"}, result[1].GetStringData())
}

func TestGrantColumnScope(t *testing.T) {
	t.Run("GrantVisibilityPrivilege", func(t *testing.T) {
		r := require.New(t)
		ctx := setupTestEnv(t)

		// root> create user alice
		_, err := runSQL(ctx, `CREATE USER alice PARTY_CODE "party_A" IDENTIFIED BY "some_pwd"`)
		r.NoError(err)

		// root> create user bob
		_, err = runSQL(ctx, `CREATE USER bob PARTY_CODE "party_B" IDENTIFIED BY "some_pwd"`)
		r.NoError(err)

		// root> create database da
		_, err = runSQL(ctx, `CREATE DATABASE da`)
		r.NoError(err)

		// root> grant create grant, priv on da.* to alice
		_, err = runSQL(ctx, `GRANT CREATE, GRANT OPTION ON da.* TO alice`)
		r.NoError(err)

		// switch to user alice
		r.NoError(switchUser(ctx, userAlice))

		// alice> create table da.t1
		_, err = runSQL(ctx, `CREATE TABLE da.t1 (c1 int, c2 int) REF_TABLE=d1.t1 DB_TYPE='mysql'`)
		r.NoError(err)

		// alice> GRANT SELECT PLAINTEXT(c1), ENCRYPTED_ONLY(c2) on da.t1 to bob
		_, err = runSQL(ctx, `GRANT SELECT PLAINTEXT(C1), SELECT ENCRYPTED_ONLY(C2) ON da.t1 TO bob`)
		r.NoError(err)

		// check granted privileges
		exists, err := columnPrivilegesExists(ctx.GetSessionVars().Storage, "bob", "da", "t1", "c1", mysql.PlaintextPriv)
		r.NoError(err)
		r.True(exists)

		exists, err = columnPrivilegesExists(ctx.GetSessionVars().Storage, "bob", "da", "t1", "c2", mysql.EncryptedOnlyPriv)
		r.NoError(err)
		r.True(exists)

		// alice> GRANT SELECT PLAINTEXT(c3) on da.t1 to alice -- failed due to no column named `bob`
		_, err = runSQL(ctx, `GRANT SELECT PLAINTEXT(c3) ON da.t1 TO bob`)
		r.Error(err)

		// check granted privileges -- should be false
		exists, err = columnPrivilegesExists(ctx.GetSessionVars().Storage, "bob", "da", "t1", "c3", mysql.PlaintextPriv)
		r.NoError(err)
		r.False(exists)

		// alice> GRANT SELECT PLAINTEXT_AFTER_JOIN(c1), SELECT PLAINTEXT_AFTER_GROUP_BY(c2) ON da.t1 TO alice
		_, err = runSQL(ctx, `GRANT SELECT PLAINTEXT_AFTER_JOIN(c1), SELECT PLAINTEXT_AFTER_GROUP_BY(c2) ON da.t1 TO bob`)
		r.NoError(err)
		// check granted privileges
		exists, err = columnPrivilegesExists(ctx.GetSessionVars().Storage, "bob", "da", "t1", "c1", mysql.PlaintextAfterJoinPriv)
		r.NoError(err)
		r.True(exists)
		exists, err = columnPrivilegesExists(ctx.GetSessionVars().Storage, "bob", "da", "t1", "c2", mysql.PlaintextAfterGroupByPriv)
		r.NoError(err)
		r.True(exists)
	})
	t.Run("VisibilityPrivilegeOverride", func(t *testing.T) {
		r := require.New(t)
		ctx := setupTestEnv(t)

		// root> create user alice
		_, err := runSQL(ctx, `CREATE USER alice PARTY_CODE "party_A" IDENTIFIED BY "some_pwd"`)
		r.NoError(err)

		// root> create user bob
		_, err = runSQL(ctx, `CREATE USER bob PARTY_CODE "party_B" IDENTIFIED BY "some_pwd"`)
		r.NoError(err)

		// root> create database da
		_, err = runSQL(ctx, `CREATE DATABASE da`)
		r.NoError(err)

		// root> grant create, grant priv on da.* to alice
		_, err = runSQL(ctx, `GRANT CREATE, GRANT OPTION ON da.* TO alice`)
		r.NoError(err)

		// switch to user alice
		r.NoError(switchUser(ctx, userAlice))

		// root> create table da.t1
		_, err = runSQL(ctx, `CREATE TABLE da.t1 (c1 int, c2 int) REF_TABLE=d1.t1 DB_TYPE='mysql'`)
		r.NoError(err)

		// check privileges before grant
		exists, err := columnPrivilegesExists(ctx.GetSessionVars().Storage, "bob", "da", "t1", "c1", mysql.EncryptedOnlyPriv)
		r.NoError(err)
		r.False(exists)

		// alice> GRANT SELECT PLAINTEXT(c1,c2), ENCRYPTED_ONLY(c1) on da.t1 to bob
		_, err = runSQL(ctx, `GRANT SELECT PLAINTEXT(c1,c2), SELECT ENCRYPTED_ONLY(c1) ON da.t1 TO bob`)
		r.NoError(err)

		// check granted privileges
		// c1 will be override by ENCRYPTED_ONLY
		exists, err = columnPrivilegesExists(ctx.GetSessionVars().Storage, "bob", "da", "t1", "c1", mysql.EncryptedOnlyPriv)
		r.NoError(err)
		r.True(exists)

		exists, err = columnPrivilegesExists(ctx.GetSessionVars().Storage, "bob", "da", "t1", "c2", mysql.PlaintextPriv)
		r.NoError(err)
		r.True(exists)
	})
}

func TestPlatformSecurityConfig(t *testing.T) {
	r := require.New(t)
	ctx := setupTestEnv(t)

	// root> create user alice
	_, err := runSQL(ctx, `CREATE USER alice PARTY_CODE "party_A" IDENTIFIED BY "some_pwd"`)
	r.NoError(err)

	// root> create user bob
	_, err = runSQL(ctx, `CREATE USER bob PARTY_CODE "party_B" IDENTIFIED BY "some_pwd"`)
	r.NoError(err)

	// root> create database da
	_, err = runSQL(ctx, `CREATE DATABASE da`)
	r.NoError(err)

	// root> grant create grant priv on da.* to alice
	_, err = runSQL(ctx, `GRANT CREATE, GRANT OPTION ON da.* TO alice`)
	r.NoError(err)

	// switch to user alice
	r.NoError(switchUser(ctx, userAlice))

	// alice> create table da.t1
	_, err = runSQL(ctx, `CREATE TABLE da.t1 (c1 int, c2 int) REF_TABLE=d1.t1 DB_TYPE='mysql'`)
	r.NoError(err)

	states := []mysql.PrivilegeType{
		mysql.PlaintextPriv,
		mysql.PlaintextAfterComparePriv,
		mysql.PlaintextAfterGroupByPriv,
	}
	grantOrRevoke := func(s mysql.PrivilegeType, isGrant bool) {
		privs := []string{fmt.Sprintf("%s(c1)", mysql.Priv2Str[s])}
		if isGrant {
			stmt := fmt.Sprintf(`GRANT %s ON da.t1 TO bob`, strings.Join(privs, ","))
			_, err = runSQL(ctx, stmt)
			r.NoError(err, stmt)
		} else {
			stmt := fmt.Sprintf(`REVOKE %s ON da.t1 FROM bob`, strings.Join(privs, ","))
			_, err = runSQL(ctx, stmt)
			r.NoError(err, stmt)
		}
	}
	checkState := func(s mysql.PrivilegeType) {
		c := &storage.ColumnPriv{}
		ctx.GetSessionVars().Storage.Where(storage.ColumnPriv{
			Host:       "%",
			Db:         "da",
			User:       "bob",
			TableName:  "t1",
			ColumnName: "c1",
		}).Find(&c)
		r.Equal(s, c.VisibilityPriv)
	}
	for _, from := range states {
		for _, to := range states {
			if from == to {
				continue
			}
			grantOrRevoke(from, true)
			grantOrRevoke(from, false)
			grantOrRevoke(to, true)
			checkState(to)
			grantOrRevoke(to, false)
		}
	}
}

func TestCaseSensitive(t *testing.T) {
	r := require.New(t)
	ctx := setupTestEnv(t)

	// root> create user alice
	_, err := runSQL(ctx, `CREATE USER alice PARTY_CODE "party_A" IDENTIFIED BY "some_pwd"`)
	r.NoError(err)

	_, err = runSQL(ctx, `CREATE USER bob PARTY_CODE "party_B" IDENTIFIED BY "some_pwd"`)
	r.NoError(err)

	// switch to user root
	r.NoError(switchUser(ctx, userRoot))

	// root> GRANT ALL on *.* to alice
	_, err = runSQL(ctx, `GRANT ALL ON *.* to alice`)
	r.NoError(err)

	// alice> create database da -- success
	_, err = runSQL(ctx, `CREATE DATABASE da`)
	r.NoError(err)

	// alice> create table da.t1 -- success
	_, err = runSQL(ctx, `CREATE TABLE da.t1 (id string, data int) REF_TABLE=d1.t1 DB_TYPE='mysql'`)
	r.NoError(err)

	// alice> create table da.t1 -- error
	// due to duplicate column name
	_, err = runSQL(ctx, `CREATE TABLE da.t2 (id string, data int, Data int) REF_TABLE=d1.t1 DB_TYPE='mysql'`)
	r.Error(err)

	// alice> create table da.t2 -- success
	_, err = runSQL(ctx, `CREATE TABLE da.t2 (id string, Data int) REF_TABLE=d1.t1 DB_TYPE='mysql'`)
	r.NoError(err)

	// alice> create table da.T2 -- success
	_, err = runSQL(ctx, `CREATE TABLE da.T2 (id string, Data int) REF_TABLE=d1.t1 DB_TYPE='mysql'`)
	r.NoError(err)

	// grant data -- success
	_, err = runSQL(ctx, `GRANT SELECT PLAINTEXT(id, data) ON da.t1 TO alice`)
	r.NoError(err)
	// grant Data -- success
	_, err = runSQL(ctx, `GRANT SELECT PLAINTEXT(id, Data) ON da.t1 TO alice`)
	r.NoError(err)

	// alice> revoke data
	_, err = runSQL(ctx, `REVOKE SELECT PLAINTEXT(data) ON da.t1 FROM alice`)
	r.NoError(err)

	// alice> revoke ID
	_, err = runSQL(ctx, `REVOKE SELECT PLAINTEXT(ID) ON da.t1 FROM alice`)
	r.NoError(err)

	// alice> delete table da.t1 -- success
	_, err = runSQL(ctx, `DROP TABLE da.t1`)
	r.NoError(err)

	// alice> delete table da.T2 -- success
	_, err = runSQL(ctx, `DROP TABLE da.T2`)
	r.NoError(err)

	// alice> delete table da.t2 -- success
	_, err = runSQL(ctx, `DROP TABLE da.t2`)
	r.NoError(err)

	// alice> delete table da.T2 -- error
	// da.T2 not exist
	_, err = runSQL(ctx, `DROP TABLE da.T2`)
	r.Error(err)
}
