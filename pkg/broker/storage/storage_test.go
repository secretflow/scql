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

package storage

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	gormlog "gorm.io/gorm/logger"

	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/util/message"
)

func TestDistributedLock(t *testing.T) {
	r := require.New(t)
	id, err := uuid.NewUUID()
	r.NoError(err)
	connStr := fmt.Sprintf("file:%s?mode=memory&cache=shared", id)
	db, err := gorm.Open(sqlite.Open(connStr),
		&gorm.Config{
			SkipDefaultTransaction: true,
			Logger: gormlog.New(
				logrus.StandardLogger(),
				gormlog.Config{
					SlowThreshold: 200 * time.Millisecond,
					Colorful:      false,
					LogLevel:      gormlog.Info,
				}),
		})

	r.NoError(err)
	manager := NewMetaManager(db)
	err = manager.Bootstrap()
	r.NoError(err)

	err = NewDistributeLockGuard(manager).InitDistributedLockIfNecessary(DistributedLockID)
	r.NoError(err)

	ok, err := NewDistributeLockGuard(manager).PreemptDistributedLock(DistributedLockID, "alice", 5*time.Second)
	r.NoError(err)
	r.True(ok)
	ok, err = NewDistributeLockGuard(manager).PreemptDistributedLock(DistributedLockID, "alice", 5*time.Second)
	r.NoError(err)
	r.False(ok)
}

func TestBootstrap(t *testing.T) {
	r := require.New(t)
	id, err := uuid.NewUUID()
	r.NoError(err)
	connStr := fmt.Sprintf("file:%s?mode=memory&cache=shared", id)
	db, err := gorm.Open(sqlite.Open(connStr),
		&gorm.Config{
			SkipDefaultTransaction: true,
			Logger: gormlog.New(
				logrus.StandardLogger(),
				gormlog.Config{
					SlowThreshold: 200 * time.Millisecond,
					Colorful:      false,
					LogLevel:      gormlog.Info,
				}),
		})

	r.NoError(err)
	manager := NewMetaManager(db)
	err = manager.Bootstrap()
	r.NoError(err)
	transaction := manager.CreateMetaTransaction()
	projectID1 := "p1"
	projectName1 := "n1"
	projectConf, err := message.ProtoMarshal(&pb.ProjectConfig{})

	alice := "alice"
	// create project
	err = transaction.CreateProject(Project{ID: projectID1, Name: projectName1, ProjectConf: string(projectConf), Creator: alice})
	r.NoError(err)
	// create duplicated project
	err = transaction.CreateProject(Project{ID: projectID1, Name: projectName1, ProjectConf: string(projectConf), Creator: alice})
	r.Error(err)
	// test case sensitive
	proj, err := transaction.GetProject("P1")
	r.Error(err)
	proj, err = transaction.GetProject("p1")
	r.NoError(err)
	// disturbance terms
	unusedProjectName := "seems_wrong"
	err = transaction.CreateProject(Project{ID: unusedProjectName, Name: "wrong_n1", ProjectConf: string(projectConf), Creator: alice})
	r.NoError(err)

	projs, err := transaction.ListProjects([]string{})
	r.NoError(err)
	r.Equal(len(projs), 2)
	r.Equal(projs[0].Proj.ID, projectID1)
	r.Equal(projs[1].Proj.ID, unusedProjectName)

	tableName := "t1"
	t1Identifier := TableIdentifier{ProjectID: projectID1, TableName: tableName}
	tableMeta := TableMeta{
		Table: Table{TableIdentifier: t1Identifier, RefTable: "real.t1", Owner: alice},
		Columns: []ColumnMeta{
			{ColumnName: "c1", DType: "float"},
			{ColumnName: "c2", DType: "int"},
		},
	}
	// create table
	err = transaction.AddTable(tableMeta)
	r.NoError(err)
	c1Identifier := ColumnIdentifier{ProjectID: t1Identifier.ProjectID, TableName: t1Identifier.TableName, ColumnName: "c1"}
	c2Identifier := ColumnIdentifier{ProjectID: t1Identifier.ProjectID, TableName: t1Identifier.TableName, ColumnName: "c2"}
	// disturbance terms
	unusedTableName := "wrong_table"
	unusedTables := TableMeta{
		Table: Table{TableIdentifier: TableIdentifier{ProjectID: unusedProjectName, TableName: unusedTableName}, RefTable: "real.t1", Owner: alice},
		Columns: []ColumnMeta{
			{ColumnName: "c1", DType: "float"},
			{ColumnName: "c2", DType: "int"},
		},
	}
	err = transaction.AddTable(unusedTables)
	r.NoError(err)
	// create duplicated table with different owner is not allowed
	tableMeta.Table.Owner = "different owner"
	err = transaction.AddTable(tableMeta)
	r.Error(err)
	// project id not exist
	stupidTable := TableMeta{
		Table: Table{TableIdentifier: TableIdentifier{ProjectID: "not_exist", TableName: tableName}, RefTable: "real.t1", Owner: alice},
	}
	err = transaction.AddTable(stupidTable)
	r.Error(err)

	res, _, err := transaction.GetTableMetasByTableNames(projectID1, []string{"t1"})
	r.NoError(err)
	r.Equal(1, len(res))

	// update project
	newProjectConf, err := message.ProtoMarshal(&pb.ProjectConfig{})
	err = transaction.UpdateProject(Project{ID: projectID1, ProjectConf: string(newProjectConf)})
	r.NoError(err)
	projWithMembers, err := transaction.GetProjectAndMembers(projectID1)
	r.NoError(err)
	proj = projWithMembers.Proj
	r.Equal(string(newProjectConf), proj.ProjectConf)
	// alter table
	bob := "bob"
	res, _, err = transaction.GetTableMetasByTableNames(projectID1, []string{})
	r.NoError(err)
	r.Equal(1, len(res))
	r.Equal(2, len(res[0].Columns))
	for _, c := range res[0].Columns {
		if c.ColumnName == "c1" {
			r.Equal("float", c.DType)
		}
	}
	// invitation
	inviteBob := Invitation{
		ProjectID:   projectID1,
		ProjectConf: string(projectConf),
		Member:      strings.Join(projWithMembers.Members, ";"),
		InviteTime:  time.Now(),
		Inviter:     alice,
		Invitee:     bob,
	}
	err = transaction.AddInvitations([]Invitation{inviteBob})
	r.NoError(err)
	// invite carol
	carol := "carol"
	inviteCarol := Invitation{
		ProjectID:   projectID1,
		ProjectConf: string(projectConf),
		Member:      strings.Join(projWithMembers.Members, ";"),
		InviteTime:  time.Now(),
		Inviter:     alice,
		Invitee:     carol,
	}
	err = transaction.AddInvitations([]Invitation{inviteCarol})
	r.NoError(err)
	// disturbance terms
	inviteAnotherCarol := Invitation{
		ProjectID:   unusedProjectName,
		ProjectConf: string(projectConf),
		Member:      strings.Join(projWithMembers.Members, ";"),
		InviteTime:  time.Now(),
		Inviter:     alice,
		Invitee:     carol,
	}
	inviteAnotherBob := Invitation{
		ProjectID:   unusedProjectName,
		ProjectConf: string(projectConf),
		Member:      strings.Join(projWithMembers.Members, ";"),
		InviteTime:  time.Now(),
		Inviter:     alice,
		Invitee:     bob,
		Status:      int8(pb.InvitationStatus_INVALID),
	}
	err = transaction.AddInvitations([]Invitation{inviteAnotherBob, inviteAnotherCarol})
	r.NoError(err)
	invites, err := transaction.ListInvitations()
	r.Equal(4, len(invites))
	invites, err = transaction.GetInvitationsBy(Invitation{Inviter: alice}, false)
	r.Equal(4, len(invites))
	invites, err = transaction.GetInvitationsBy(Invitation{ProjectID: unusedProjectName, Inviter: alice}, true)
	r.Equal(1, len(invites))
	// grant
	privs := []ColumnPriv{
		{ColumnPrivIdentifier: ColumnPrivIdentifier{ProjectID: c1Identifier.ProjectID, TableName: c1Identifier.TableName, ColumnName: c1Identifier.ColumnName, DestParty: alice}, Priv: "plain"},
		{ColumnPrivIdentifier: ColumnPrivIdentifier{ProjectID: c1Identifier.ProjectID, TableName: c1Identifier.TableName, ColumnName: c1Identifier.ColumnName, DestParty: bob}, Priv: "encrypt"},
		{ColumnPrivIdentifier: ColumnPrivIdentifier{ProjectID: c2Identifier.ProjectID, TableName: c2Identifier.TableName, ColumnName: c2Identifier.ColumnName, DestParty: alice}, Priv: "plain"},
		{ColumnPrivIdentifier: ColumnPrivIdentifier{ProjectID: c2Identifier.ProjectID, TableName: c2Identifier.TableName, ColumnName: c2Identifier.ColumnName, DestParty: bob}, Priv: "encrypt"},
	}
	err = transaction.AddProjectMembers([]Member{{ProjectID: c1Identifier.ProjectID, Member: bob}})
	r.NoError(err)
	// project member [alice], but grant to [alice, bob]
	err = transaction.GrantColumnConstraints(privs)
	r.NoError(err)
	// duplicated grant
	err = transaction.GrantColumnConstraints(privs)
	r.NoError(err)
	// show grant alice
	privs, err = transaction.ListColumnConstraints(projectID1, []string{tableName}, []string{alice})
	r.NoError(err)
	r.Equal(2, len(privs))
	// show grant bob
	privs, err = transaction.ListColumnConstraints(projectID1, []string{tableName}, []string{bob})
	r.NoError(err)
	r.Equal(2, len(privs))
	// remove grant
	err = transaction.RevokeColumnConstraints([]ColumnPrivIdentifier{{ProjectID: c1Identifier.ProjectID, TableName: c1Identifier.TableName, ColumnName: c1Identifier.ColumnName}})
	r.NoError(err)
	// show grant bob
	privs, err = transaction.ListColumnConstraints(projectID1, []string{tableName}, []string{bob})
	r.NoError(err)
	r.Equal(2, len(privs))
	// grant or update
	privs = []ColumnPriv{
		{ColumnPrivIdentifier: ColumnPrivIdentifier{ProjectID: c1Identifier.ProjectID, TableName: c1Identifier.TableName, ColumnName: c1Identifier.ColumnName, DestParty: alice}, Priv: "encrypt"},
		{ColumnPrivIdentifier: ColumnPrivIdentifier{ProjectID: c1Identifier.ProjectID, TableName: c1Identifier.TableName, ColumnName: c1Identifier.ColumnName, DestParty: bob}, Priv: "encrypt"},
		{ColumnPrivIdentifier: ColumnPrivIdentifier{ProjectID: c2Identifier.ProjectID, TableName: c2Identifier.TableName, ColumnName: c2Identifier.ColumnName, DestParty: alice}, Priv: "plain"},
		{ColumnPrivIdentifier: ColumnPrivIdentifier{ProjectID: c2Identifier.ProjectID, TableName: c2Identifier.TableName, ColumnName: c2Identifier.ColumnName, DestParty: bob}, Priv: "plain"},
		{ColumnPrivIdentifier: ColumnPrivIdentifier{ProjectID: c1Identifier.ProjectID, TableName: c1Identifier.TableName, ColumnName: c1Identifier.ColumnName, DestParty: carol}, Priv: "plain"},
		{ColumnPrivIdentifier: ColumnPrivIdentifier{ProjectID: c2Identifier.ProjectID, TableName: c2Identifier.TableName, ColumnName: c2Identifier.ColumnName, DestParty: carol}, Priv: "encrypt"},
	}
	err = transaction.AddProjectMembers([]Member{Member{ProjectID: c1Identifier.ProjectID, Member: carol}})
	r.NoError(err)
	err = transaction.GrantColumnConstraints(privs)
	r.NoError(err)
	// show grant all
	privs, err = transaction.ListColumnConstraints(projectID1, []string{tableName}, []string{})
	r.NoError(err)
	r.Equal(6, len(privs))
	for _, priv := range privs {
		if priv.ColumnPrivIdentifier.ProjectID == c1Identifier.ProjectID &&
			priv.ColumnPrivIdentifier.TableName == c1Identifier.TableName &&
			priv.ColumnPrivIdentifier.ColumnName == c1Identifier.ColumnName &&
			priv.DestParty == alice {
			r.Equal("encrypt", priv.Priv)
		}
		if priv.ColumnPrivIdentifier.ProjectID == c1Identifier.ProjectID &&
			priv.ColumnPrivIdentifier.TableName == c1Identifier.TableName &&
			priv.ColumnPrivIdentifier.ColumnName == c1Identifier.ColumnName &&
			priv.DestParty == carol {
			r.Equal("plain", priv.Priv)
		}
		if priv.ColumnPrivIdentifier.ProjectID == c2Identifier.ProjectID &&
			priv.ColumnPrivIdentifier.TableName == c2Identifier.TableName &&
			priv.ColumnPrivIdentifier.ColumnName == c2Identifier.ColumnName && priv.DestParty == bob {
			r.Equal("plain", priv.Priv)
		}
	}
	transaction.Finish(nil)
}

func TestDeleteProject(t *testing.T) {
	r := require.New(t)
	id, err := uuid.NewUUID()
	r.NoError(err)
	connStr := fmt.Sprintf("file:%s?mode=memory&cache=shared", id)
	db, err := gorm.Open(sqlite.Open(connStr),
		&gorm.Config{
			SkipDefaultTransaction: true,
			Logger: gormlog.New(
				logrus.StandardLogger(),
				gormlog.Config{
					SlowThreshold: 200 * time.Millisecond,
					Colorful:      false,
					LogLevel:      gormlog.Info,
				}),
		})

	r.NoError(err)
	manager := NewMetaManager(db)
	err = manager.Bootstrap()
	r.NoError(err)
	transaction := manager.CreateMetaTransaction()

	// Create a project
	projectID := "test-project"
	projectName := "Test Project"
	projectConf, err := message.ProtoMarshal(&pb.ProjectConfig{})
	r.NoError(err)

	alice := "alice"
	bob := "bob"

	// Create project
	err = transaction.CreateProject(Project{ID: projectID, Name: projectName, ProjectConf: string(projectConf), Creator: alice})
	r.NoError(err)

	// Add another member
	err = transaction.AddProjectMembers([]Member{{ProjectID: projectID, Member: bob}})
	r.NoError(err)

	// Create tables for the project
	tableName1 := "table1"
	tableName2 := "table2"

	table1Meta := TableMeta{
		Table: Table{TableIdentifier: TableIdentifier{ProjectID: projectID, TableName: tableName1}, RefTable: "real.table1", Owner: alice},
		Columns: []ColumnMeta{
			{ColumnName: "col1", DType: "string"},
			{ColumnName: "col2", DType: "int"},
		},
	}

	table2Meta := TableMeta{
		Table: Table{TableIdentifier: TableIdentifier{ProjectID: projectID, TableName: tableName2}, RefTable: "real.table2", Owner: bob},
		Columns: []ColumnMeta{
			{ColumnName: "col1", DType: "float"},
			{ColumnName: "col2", DType: "bool"},
		},
	}

	err = transaction.AddTable(table1Meta)
	r.NoError(err)
	err = transaction.AddTable(table2Meta)
	r.NoError(err)

	// Add column privileges
	privs := []ColumnPriv{
		{ColumnPrivIdentifier: ColumnPrivIdentifier{ProjectID: projectID, TableName: tableName1, ColumnName: "col1", DestParty: alice}, Priv: "plain"},
		{ColumnPrivIdentifier: ColumnPrivIdentifier{ProjectID: projectID, TableName: tableName1, ColumnName: "col1", DestParty: bob}, Priv: "encrypt"},
		{ColumnPrivIdentifier: ColumnPrivIdentifier{ProjectID: projectID, TableName: tableName2, ColumnName: "col2", DestParty: alice}, Priv: "plain"},
	}
	err = transaction.GrantColumnConstraints(privs)
	r.NoError(err)

	// Add invitations
	invite := Invitation{
		ProjectID:   projectID,
		ProjectConf: string(projectConf),
		Member:      fmt.Sprintf("%s;%s", alice, bob),
		InviteTime:  time.Now(),
		Inviter:     alice,
		Invitee:     "carol",
	}
	err = transaction.AddInvitations([]Invitation{invite})
	r.NoError(err)

	// Try to delete project without archiving first (should fail)
	err = transaction.DeleteProject(projectID)
	r.Error(err)
	r.Contains(err.Error(), "must be archived before deletion")

	// Archive the project
	err = transaction.ArchiveProject(projectID)
	r.NoError(err)

	// Now delete the project (should succeed)
	err = transaction.DeleteProject(projectID)
	r.NoError(err)

	// Verify that the project and all related data have been deleted
	// Try to get the project (should fail)
	_, err = transaction.GetProject(projectID)
	r.Error(err)

	// Try to get project members (should return empty)
	members, err := transaction.GetProjectMembers(projectID)
	r.NoError(err)
	r.Equal(0, len(members))

	// Try to get tables (should return empty)
	tables, err := transaction.GetAllTables(projectID)
	r.NoError(err)
	r.Equal(0, len(tables))

	// Try to get column privileges (should return empty)
	ccls, err := transaction.ListColumnConstraints(projectID, []string{}, []string{})
	r.NoError(err)
	r.Equal(0, len(ccls))

	// Try to get invitations for the project (should return empty)
	invitations, err := transaction.GetInvitationsBy(Invitation{ProjectID: projectID}, false)
	r.NoError(err)
	r.Equal(0, len(invitations))

	transaction.Finish(nil)
}
