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
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"golang.org/x/exp/slices"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/sirupsen/logrus"

	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/util/sliceutil"
)

const (
	InsertBatchSize = 1000
)

var allTables = []interface{}{&Project{}, &Table{}, &Column{}, &ColumnPriv{}, &Invitation{}}

type MetaManager struct {
	// for now, no cache, all info store in db
	db *gorm.DB
}

func NewMetaManager(db *gorm.DB) *MetaManager {
	return &MetaManager{
		db: db,
	}
}

// NeedBootstrap checks if the store is empty
func (manager *MetaManager) NeedBootstrap() bool {
	for _, tn := range allTables {
		if manager.db.Migrator().HasTable(tn) {
			return false
		}
	}
	return true
}

// Bootstrap init db
func (manager *MetaManager) Bootstrap() error {
	// Migrate the schemas
	if err := manager.db.AutoMigrate(allTables...); err != nil {
		return err
	}
	return nil
}

// drop db for tests
func (manager *MetaManager) DropTables() error {
	if err := manager.db.Migrator().DropTable(allTables...); err != nil {
		return err
	}
	return nil
}

// CheckStorage verifies storage is valid
func CheckStorage(store *gorm.DB) error {
	for _, tn := range allTables {
		if !store.Migrator().HasTable(tn) {
			return fmt.Errorf("table %s is missing in storage", reflect.TypeOf(tn).String())
		}
	}
	return nil
}

func AddExclusiveLock(txn *MetaTransaction) *MetaTransaction {
	return &MetaTransaction{
		db: txn.db.Clauses(clause.Locking{Strength: "UPDATE"}),
	}
}

func AddShareLock(txn *MetaTransaction) *MetaTransaction {
	return &MetaTransaction{
		db: txn.db.Clauses(clause.Locking{Strength: "SHARE"}),
	}
}

// create a new MetaTransaction for every request
// and call Finish when you finish all your actions
func (manager *MetaManager) CreateMetaTransaction() *MetaTransaction {
	return &MetaTransaction{
		db: manager.db.Begin(&sql.TxOptions{Isolation: sql.LevelRepeatableRead}),
	}
}

type MetaTransaction struct {
	db *gorm.DB
}

func (t *MetaTransaction) Finish(err error) {
	if err == nil {
		t.db.Commit()
	} else {
		t.db.Rollback()
	}
}

// return err, if project exists
func (t *MetaTransaction) CreateProject(project Project) error {
	result := t.db.Create(&project)
	return result.Error
}

func (t *MetaTransaction) RemoveProject(projectID string) error {
	// drop invitations
	result := t.db.Delete(&Invitation{ProjectID: projectID})
	if result.Error != nil {
		return result.Error
	}
	tableIdentifier := TableIdentifier{ProjectID: projectID}
	columnIdentifier := ColumnIdentifier{ProjectID: projectID}
	columnPrivIndentifier := ColumnPrivIdentifier{ProjectID: projectID}
	// drop column privs
	result = t.db.Delete(&ColumnPriv{ColumnPrivIdentifier: columnPrivIndentifier})
	if result.Error != nil {
		return result.Error
	}
	// drop columns
	result = t.db.Delete(&Column{ColumnIdentifier: columnIdentifier})
	if result.Error != nil {
		return result.Error
	}
	// drop tables;
	result = t.db.Delete(&Table{TableIdentifier: tableIdentifier})
	if result.Error != nil {
		return result.Error
	}
	// drop project
	result = t.db.Delete(&Project{ID: projectID})
	return result.Error
}

// update project fail if project not exists or other reasons
func (t *MetaTransaction) UpdateProject(proj Project) error {
	result := t.db.Model(&Project{}).Where("id = ?", proj.ID).Updates(&proj)
	if result.RowsAffected != 1 {
		return fmt.Errorf("failed to update project %s, with affected rows num %d", proj.ID, result.RowsAffected)
	}
	return result.Error
}

func (t *MetaTransaction) AddProjectMember(projectID, newMember string) error {
	project, err := t.GetProject(projectID)
	if err != nil {
		return err
	}

	members := strings.Split(project.Member, ";")
	if slices.Contains(members, newMember) {
		logrus.Warnf("member %v already exists in project %v", newMember, projectID)
		return nil
	}
	members = append(members, newMember)

	result := t.db.Model(&Project{}).Where(&Project{ID: projectID}).Update("member", strings.Join(members, ";"))
	return result.Error
}

func (t *MetaTransaction) GetProject(projectID string) (Project, error) {
	project := Project{}
	result := t.db.Model(&Project{}).Where(&Project{ID: projectID}).First(&project)
	return project, result.Error
}

func (t *MetaTransaction) ListProjects(projectIDs []string) ([]Project, error) {
	var projects []Project
	result := t.db.Model(&Project{})
	if len(projectIDs) != 0 {
		result = result.Where("id in ?", projectIDs)
	}
	result.Scan(&projects)
	return projects, result.Error
}

// archive project fail if project not exists or other reasons
func (t *MetaTransaction) ArchiveProject(projectID string) error {
	result := t.db.Model(&Project{}).Where(&Project{ID: projectID}).Update("archived", true)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("failed to update project %s, with affected rows num %d", projectID, result.RowsAffected)
	}
	return nil
}

// duplicated invitations are permitted
func (t *MetaTransaction) AddInvitations(invitations []Invitation) error {
	result := t.db.CreateInBatches(&invitations, InsertBatchSize)
	return result.Error
}

func (t *MetaTransaction) ListInvitations() ([]Invitation, error) {
	var invitations []Invitation
	result := t.db.Find(&invitations)
	return invitations, result.Error
}

func (t *MetaTransaction) GetUnhandledInvitationWithID(invitationID uint64) (Invitation, error) {
	var invitation Invitation
	// NOTE: When querying with struct, GORM will only query with non-zero fields, so use map for 'accepted' instead of struct here.
	// ref: https://gorm.io/docs/query.html
	result := t.db.Model(&Invitation{}).Where(&Invitation{ID: invitationID}).Where(map[string]string{"accepted": "0"}).First(&invitation)
	return invitation, result.Error
}

func (t *MetaTransaction) GetUnhandledInvitation(projectID, inviter, invitee string) (Invitation, error) {
	var invitation Invitation
	result := t.db.Model(&Invitation{}).Where(&Invitation{ProjectID: projectID, Inviter: inviter, Invitee: invitee}).Where(map[string]string{"accepted": "0"}).First(&invitation)
	return invitation, result.Error
}

// update invitation status to accept or reject inviting
// NOTE: all invitations with same projectID + inviter + invitee will be updated
func (t *MetaTransaction) UpdateInvitation(invitation Invitation) error {
	result := t.db.Model(&Invitation{}).Where(&Invitation{ProjectID: invitation.ProjectID, Inviter: invitation.Inviter, Invitee: invitation.Invitee}).Update("accepted", invitation.Accepted)
	return result.Error
}

type ColumnMeta struct {
	ColumnName string
	DType      string
}
type TableMeta struct {
	Table   Table
	Columns []ColumnMeta
}

func (t *MetaTransaction) AddTable(table TableMeta) error {
	// if table already exists, check whether the owner is the same.
	{
		var tables []Table
		result := t.db.Model(&Table{}).Where(&Table{TableIdentifier: TableIdentifier{ProjectID: table.Table.ProjectID, TableName: table.Table.TableName}}).Find(&tables)
		if result.Error == nil && result.RowsAffected > 0 {
			if len(tables) != 1 {
				return fmt.Errorf("AddTable: existing multi tables with same name: %+v", tables)
			} else if tables[0].Owner != table.Table.Owner {
				return fmt.Errorf("AddTable: existing table owner{%s} not equal to {%s}", tables[0].Owner, table.Table.Owner)
			} else {
				logrus.Warnf("AddTable: already exist, droping table first: %+v", tables)
				err := t.DropTable(TableIdentifier{ProjectID: table.Table.ProjectID, TableName: table.Table.TableName})
				if err != nil {
					return fmt.Errorf("AddTable: drop existing table err: %v", err)
				}
			}
		}
	}
	result := t.db.Create(&table.Table)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("failed to create table %+v, with affected rows num %d", table.Table, result.RowsAffected)
	}
	var columns []Column
	for _, columnMeta := range table.Columns {
		columns = append(columns, Column{ColumnIdentifier: ColumnIdentifier{ProjectID: table.Table.ProjectID, TableName: table.Table.TableName, ColumnName: columnMeta.ColumnName}, DType: columnMeta.DType})
	}
	result = t.db.Create(&columns)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != int64(len(columns)) {
		return fmt.Errorf("failed to create columns %+v, with affected rows num %d", columns, result.RowsAffected)
	}
	return nil
}

// DropTable will drop columns and table
func (t *MetaTransaction) DropTable(table TableIdentifier) error {
	if err := t.DropTableColumns(table); err != nil {
		return err
	}
	// drop tables;
	result := t.db.Where("project_id = ?", table.ProjectID).Where("table_name = ?", table.TableName).Delete(&Table{})
	return result.Error
}

func (t *MetaTransaction) DropTableColumns(table TableIdentifier) error {
	// drop column privs
	result := t.db.Where("project_id = ?", table.ProjectID).Where("table_name = ?", table.TableName).Delete(&ColumnPriv{})
	if result.Error != nil {
		return result.Error
	}
	// drop columns
	result = t.db.Where("project_id = ?", table.ProjectID).Where("table_name = ?", table.TableName).Delete(&Column{})
	if result.Error != nil {
		return result.Error
	}

	return nil
}

type tableColumn struct {
	TableName  string
	RefTable   string
	DBType     string `gorm:"column:db_type"`
	Owner      string
	ColumnName string
	DType      string `gorm:"column:data_type"`
}

func (t *MetaTransaction) ListTables(projectID string) ([]TableMeta, error) {
	return t.GetTablesByTableNames(projectID, []string{})
}

func (t *MetaTransaction) ListDedupTableOwners(tableNames []string) ([]string, error) {
	result := t.db.Model(&Table{}).Select("tables.owner").Where("tables.table_name IN ?", tableNames)
	var owners []string
	result = result.Scan(&owners)
	if result.Error != nil {
		return nil, result.Error
	}
	return sliceutil.SliceDeDup(owners), nil
}

// if len(tableNames) == 0 return all tables
func (t *MetaTransaction) GetTablesByTableNames(projectID string, tableNames []string) ([]TableMeta, error) {
	var tableColumns []tableColumn
	// SELECT tables.table_name, tables.ref_table, tables.db_type, tables.owner, columns.column_name, columns.data_type FROM `tables` join columns on tables.project_id = columns.project_id
	result := t.db.Model(&Table{}).Select("tables.table_name, tables.ref_table, tables.db_type, tables.owner, columns.column_name, columns.data_type").Joins("join columns on tables.project_id = columns.project_id").Where("columns.project_id = ?", projectID).Where("columns.table_name = tables.table_name")
	if len(tableNames) != 0 {
		result = result.Where("tables.table_name in ?", tableNames)
	}
	result = result.Scan(&tableColumns)
	if result.Error != nil {
		return nil, result.Error
	}
	// fill columns into TableMeta
	tableMap := map[string]*TableMeta{}
	for _, tableCol := range tableColumns {
		tableMeta, exist := tableMap[tableCol.TableName]
		if !exist {
			tableMeta = &TableMeta{Table: Table{TableIdentifier: TableIdentifier{ProjectID: projectID, TableName: tableCol.TableName}, RefTable: tableCol.RefTable, DBType: tableCol.DBType, Owner: tableCol.Owner}}
			tableMap[tableCol.TableName] = tableMeta
		}
		columnMeta := ColumnMeta{ColumnName: tableCol.ColumnName, DType: tableCol.DType}
		tableMeta.Columns = append(tableMeta.Columns, columnMeta)
	}
	var tableMetas []TableMeta
	for _, tableName := range sliceutil.SortMapKeyForDeterminism(tableMap) {
		tableMetas = append(tableMetas, *tableMap[tableName])
	}
	return tableMetas, nil
}

func (t *MetaTransaction) GrantColumnConstraints(privs []ColumnPriv) error {
	// insert into storage
	// refer to: https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html
	result := t.db.Clauses(clause.OnConflict{
		UpdateAll: true,
	}).CreateInBatches(&privs, InsertBatchSize)
	if result.Error != nil {
		return result.Error
	}
	return nil
}

// map key project_id-table_name-dest_party
type constraintTripe struct {
	projectID string
	tableName string
	destParty string
}

// use when ccl changed
func (t *MetaTransaction) RevokeColumnConstraints(privIdentifiers []ColumnPrivIdentifier) error {
	constraintMap := make(map[constraintTripe][]string)
	for _, privIdentifier := range privIdentifiers {
		key := constraintTripe{projectID: privIdentifier.ProjectID, tableName: privIdentifier.TableName, destParty: privIdentifier.DestParty}
		constraintMap[key] = append(constraintMap[key], privIdentifier.ColumnName)
	}
	unknownCCL := pb.SecurityConfig_ColumnControl_Visibility_name[int32(pb.SecurityConfig_ColumnControl_UNKNOWN)]
	for key, columns := range constraintMap {
		result := t.db.Model(&ColumnPriv{}).Where(&ColumnPriv{ColumnPrivIdentifier: ColumnPrivIdentifier{ProjectID: key.projectID, TableName: key.tableName, DestParty: key.destParty}}).Where("column_name in ?", columns).Update("priv", unknownCCL)
		if result.Error != nil {
			return result.Error
		}
	}
	return nil
}

func (t *MetaTransaction) ListColumnConstraints(projectID string, tableNames []string, destParties []string) ([]ColumnPriv, error) {
	var privs []ColumnPriv
	result := t.db.Model(&ColumnPriv{}).Where(&ColumnPriv{ColumnPrivIdentifier: ColumnPrivIdentifier{ProjectID: projectID}})
	if len(tableNames) != 0 {
		result = result.Where("table_name in ?", tableNames)
	}
	if len(destParties) != 0 {
		result = result.Where("dest_party in ?", destParties)
	}
	result.Scan(&privs)
	return privs, result.Error
}
