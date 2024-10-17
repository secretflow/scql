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
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/sirupsen/logrus"
	"golang.org/x/exp/rand"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/util/sliceutil"
)

const (
	InsertBatchSize = 1000
)

var allTables = []interface{}{&Member{}, &Project{}, &Table{}, &Column{}, &ColumnPriv{}, &Invitation{}}
var sessionTables = []interface{}{&SessionInfo{}, &SessionResult{}, &Lock{}}

type MetaManager struct {
	// for now, no cache, all info store in db
	db             *gorm.DB
	persistSession bool
}

var (
	_ gocron.Elector = &DistributedElector{}
	_ gocron.Locker  = &DistributedLocker{}
	_ gocron.Lock    = &DistributedLock{}
)

type DistributedElector struct {
	leader   bool
	locker   *DistributedLocker
	stopChan chan bool
}

func NewDistributedElector(mm *MetaManager, id DistLockID, owner string, ttl time.Duration) (*DistributedElector, error) {
	locker, err := NewDistributedLocker(mm, id, owner, ttl)
	if err != nil {
		return nil, fmt.Errorf("fail to create distributed locker %d: %v", id, err)
	}
	return &DistributedElector{leader: false, locker: locker, stopChan: make(chan bool, 1)}, nil
}

func (elector *DistributedElector) IsLeader(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("done")
	default:
	}

	if !elector.leader {
		// Try to run for election
		err := elector.locker.metaMgr.HoldDistributedLock(elector.locker.id, elector.locker.owner, elector.locker.ttl)
		if err != nil {
			elector.leader = false
			logrus.Warnf("%s failed election: %v", elector.locker.owner, err)
			return fmt.Errorf("already elected leader")
		}

		elector.leader = true
		go elector.renewLeaderLock()
	} else {
		owner, err := elector.locker.metaMgr.GetDistributedLockOwner(elector.locker.id)
		if err != nil || owner != elector.locker.owner {
			elector.leader = false
			elector.StopRenewLeaderLock()

			logrus.Warnf("%s failed to extend mandate: %v", elector.locker.owner, err)
			return fmt.Errorf("failed to extent mandate")
		}
	}

	logrus.Infof("%v is the leader", elector.locker.owner)
	return nil
}

func (elector *DistributedElector) StopRenewLeaderLock() {
	elector.stopChan <- true
}

func (elector *DistributedElector) renewLeaderLock() {
	ticker := time.NewTicker(elector.locker.ttl / 2)
	defer ticker.Stop()
	for {
		select {
		case <-elector.stopChan:
			logrus.Infof("Stopping leader renewal for %s", elector.locker.owner)
			return
		case <-ticker.C:
			// Periodically update the lock to prevent expiration
			err := elector.locker.metaMgr.UpdateDistributedLock(elector.locker.id, elector.locker.owner, elector.locker.ttl)
			if err != nil {
				logrus.Warnf("failed to update distributed lock for owner %s: %v", elector.locker.owner, err)
				// Handle error or break the loop if necessary
			} else {
				logrus.Infof("successfully updated distributed lock for owner %s", elector.locker.owner)
			}
		}
	}
}

type DistributedLocker struct {
	metaMgr *MetaManager
	id      DistLockID
	owner   string
	ttl     time.Duration // lock holding ttl time
}

func NewDistributedLocker(mm *MetaManager, id DistLockID, owner string, ttl time.Duration) (*DistributedLocker, error) {
	if host := os.Getenv("HOSTNAME"); host != "" {
		owner = host
	} else {
		owner = owner + randString(8)
		logrus.Warnf("cannot find HOSTNAME env, using %s as owner", owner)
	}

	// Avoid situations where the lock is not released after the duration of the task has ended
	locker := &DistributedLocker{metaMgr: mm, id: id, owner: owner, ttl: ttl - 100*time.Millisecond}

	err := locker.metaMgr.InitDistributedLockIfNecessary(id)
	if err != nil {
		logrus.Errorf("failed to check distributed lock for owner %s:%v", locker.owner, err)
		return nil, fmt.Errorf("failed to check distributed lock:%v", err)
	}

	return locker, nil
}

// Lock acquires a lock
// Implementation of the gocron lock interface
func (l *DistributedLocker) Lock(ctx context.Context, key string) (gocron.Lock, error) {
	err := l.metaMgr.HoldDistributedLock(l.id, l.owner, l.ttl)
	if err != nil {
		logrus.Warnf("failed to get distributed lock %v for owner %s:%v", l.id, l.owner, err)
		return nil, fmt.Errorf("failed to get distributed lock")
	}

	logrus.Infof("Successfully acquired distributed lock %v for owner: %s", l.id, l.owner)
	return &DistributedLock{metaMgr: l.metaMgr, owner: l.owner}, nil
}

// GormLock represents a database lock
type DistributedLock struct {
	metaMgr *MetaManager
	owner   string
}

// Unlock releases the lock
// Implementation of the gocron unlock interface
func (l *DistributedLock) Unlock(ctx context.Context) error {
	// Not actively releasing locks to avoid lock being acquired immediately, which can put pressure on the database
	// After ttl(DistributedLocker.ttl) time, the lock will be released automatically
	return nil
}

func NewMetaManager(db *gorm.DB, ps bool) *MetaManager {
	return &MetaManager{
		db:             db,
		persistSession: ps,
	}
}

func (manager *MetaManager) Persistent() bool {
	return manager.persistSession
}

func (manager *MetaManager) tables() []interface{} {
	tables := allTables
	if manager.persistSession {
		tables = append(tables, sessionTables...)
	}
	return tables
}

// NeedBootstrap checks if the store is empty
func (manager *MetaManager) NeedBootstrap() bool {
	for _, tn := range manager.tables() {
		if manager.db.Migrator().HasTable(tn) {
			return false
		}
	}

	return true
}

// Bootstrap init db
func (manager *MetaManager) Bootstrap() error {
	logrus.Infof("migrate tables: %+v", allTables...)
	// Migrate the schemas
	if err := manager.db.AutoMigrate(manager.tables()...); err != nil {
		return err
	}

	return nil
}

// drop db for tests
func (manager *MetaManager) DropTables() error {
	if err := manager.db.Migrator().DropTable(manager.tables()...); err != nil {
		return err
	}
	return nil
}

func (manager *MetaManager) GetProject(projectId string) (*Project, error) {
	txn := manager.CreateMetaTransaction()
	project, err := txn.GetProject(projectId)
	txn.Finish(err)
	return &project, err
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
		db: manager.db.Begin(&sql.TxOptions{Isolation: sql.LevelReadCommitted}),
	}
}

func (manager *MetaManager) ExecInMetaTransaction(fn func(*MetaTransaction) error) error {
	txn := manager.CreateMetaTransaction()
	err := fn(txn)
	return txn.Finish(err)
}

type MetaTransaction struct {
	db *gorm.DB
}

// Automatic rollback will occur if a deadlock or timeout occurs.
func (t *MetaTransaction) Finish(err error) error {
	if err == nil {
		result := t.db.Commit()
		if result.Error != nil {
			logrus.Errorf("commit failed: %s", result.Error)
		}
		return result.Error
	} else {
		result := t.db.Rollback()
		if result.Error != nil {
			logrus.Errorf("%v, rollback failed: %s", err, result.Error)
		}
		return err
	}
}

// return err, if project exists
func (t *MetaTransaction) CreateProject(project Project) error {
	// add project
	result := t.db.Create(&project)
	if result.Error != nil {
		return result.Error
	}
	// add first member
	err := t.AddProjectMembers([]Member{Member{ProjectID: project.ID, Member: project.Creator}})
	return err
}

// update project fail if project not exists or other reasons
func (t *MetaTransaction) UpdateProject(proj Project) error {
	result := t.db.Model(&Project{}).Where("id = ?", proj.ID).Updates(&proj)
	if result.RowsAffected != 1 {
		return fmt.Errorf("failed to update project %s, with affected rows num %d", proj.ID, result.RowsAffected)
	}
	return result.Error
}

func (t *MetaTransaction) AddProjectMembers(members []Member) error {
	result := t.db.CreateInBatches(&members, InsertBatchSize)
	return result.Error
}

func (t *MetaTransaction) GetProject(projectID string) (Project, error) {
	project := Project{}
	result := t.db.Model(&Project{}).Where(&Project{ID: projectID}).First(&project)
	return project, result.Error
}

func (t *MetaTransaction) GetProjectMembers(projectID string) ([]string, error) {
	var members []string
	result := t.db.Model(&Member{}).Where(&Member{ProjectID: projectID}).Select("member").Scan(&members)
	return members, result.Error
}

type ProjectWithMember struct {
	Proj    Project
	Members []string
}

func (t *MetaTransaction) GetProjectAndMembers(projectID string) (projectAndMembers ProjectWithMember, err error) {
	project := Project{}
	result := t.db.Model(&Project{}).Where(&Project{ID: projectID}).First(&project)
	if result.Error != nil {
		return ProjectWithMember{}, result.Error
	}
	projectAndMembers.Proj = project
	projectAndMembers.Members, err = t.GetProjectMembers(projectID)
	return projectAndMembers, err
}

func (t *MetaTransaction) ListProjects(projectIDs []string) ([]ProjectWithMember, error) {
	var projects []Project
	result := t.db.Model(&Project{})
	if len(projectIDs) != 0 {
		result = result.Where("id in ?", projectIDs)
	}
	result.Scan(&projects)
	if result.Error != nil {
		return nil, result.Error
	}
	if len(projectIDs) != 0 {
		var existProjectIDs []string
		for _, proj := range projects {
			existProjectIDs = append(existProjectIDs, proj.ID)
		}
		if !sliceutil.ContainsAll(existProjectIDs, projectIDs) {
			return nil, fmt.Errorf("projects %+v not found", sliceutil.Subtraction(projectIDs, existProjectIDs))
		}
	}
	var members []Member
	result = t.db.Model(&Member{})
	if len(projectIDs) != 0 {
		result = result.Where("project_id in ?", projectIDs)
	}
	result.Scan(&members)
	if result.Error != nil {
		return nil, result.Error
	}
	memberMap := make(map[string][]string, len(projects))
	for _, member := range members {
		memberMap[member.ProjectID] = append(memberMap[member.ProjectID], member.Member)
	}
	var projectsWithMembers []ProjectWithMember
	for _, proj := range projects {
		projectsWithMembers = append(projectsWithMembers, ProjectWithMember{Proj: proj, Members: memberMap[proj.ID]})
	}
	return projectsWithMembers, result.Error
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

// Note: because undecided status is zero, gorm will ignore it, so if select undecided invitations, set selectUnDecidedStatus true
func (t *MetaTransaction) GetInvitationsBy(invitation Invitation, selectUnDecidedStatus bool) (invitations []Invitation, err error) {
	if selectUnDecidedStatus {
		invitation.Status = int8(pb.InvitationStatus_UNDECIDED)
		result := t.db.Model(&Invitation{}).Where(&invitation).Where(map[string]interface{}{"status": pb.InvitationStatus_UNDECIDED}).Scan(&invitations)
		err = result.Error
	} else {
		result := t.db.Model(&Invitation{}).Where(&invitation).Scan(&invitations)
		err = result.Error
	}
	return
}

func (t *MetaTransaction) GetUnhandledInvitationWithID(invitationID uint64) (Invitation, error) {
	var invitation Invitation
	// NOTE: When querying with struct, GORM will only query with non-zero fields, so use map for 'accepted' instead of struct here.
	// ref: https://gorm.io/docs/query.html
	result := t.db.Model(&Invitation{}).Where(&Invitation{ID: invitationID}).Where(map[string]interface{}{"status": pb.InvitationStatus_UNDECIDED}).First(&invitation)
	return invitation, result.Error
}

func (t *MetaTransaction) GetUnhandledInvitation(projectID, inviter, invitee string) (Invitation, error) {
	var invitation Invitation
	result := t.db.Model(&Invitation{}).Where(&Invitation{ProjectID: projectID, Inviter: inviter, Invitee: invitee}).Where(map[string]interface{}{"status": pb.InvitationStatus_UNDECIDED}).First(&invitation)
	return invitation, result.Error
}

// NOTE: invitation id may be zero don't use Where(&Invitation{ID: id})
func (t *MetaTransaction) ModifyInvitationStatus(invitationID uint64, status pb.InvitationStatus) error {
	result := t.db.Model(&Invitation{}).Where(map[string]interface{}{"id": invitationID}).Update("status", int8(status))
	return result.Error
}

func (t *MetaTransaction) SetInvitationInvalidByID(invitationID uint64) error {
	result := t.db.Model(&Invitation{}).Where(map[string]interface{}{"id": invitationID}).Update("status", pb.InvitationStatus_INVALID)
	return result.Error
}

func (t *MetaTransaction) SetUnhandledInvitationsInvalid(projectID, inviter, invitee string) error {
	result := t.db.Model(&Invitation{}).Where(&Invitation{ProjectID: projectID, Inviter: inviter, Invitee: invitee}).Where(map[string]interface{}{"status": pb.InvitationStatus_UNDECIDED}).Update("status", pb.InvitationStatus_INVALID)
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

func (t *MetaTransaction) ListDedupTableOwners(projectID string, tableNames []string) ([]string, error) {
	var owners []string
	result := t.db.Model(&Table{}).Select("owner").Where("project_id = ?", projectID).Where("table_name IN ?", tableNames).Scan(&owners)
	if result.Error != nil {
		return nil, result.Error
	}
	return sliceutil.SliceDeDup(owners), nil
}

// if len(tableNames) == 0 return all tables
// return err if ANY table DOESN'T exist
func (t *MetaTransaction) GetTableMetasByTableNames(projectID string, tableNames []string) (tableMetas []TableMeta, notFoundTables []string, err error) {
	var tableColumns []tableColumn
	// SELECT tables.table_name, tables.ref_table, tables.db_type, tables.owner, columns.column_name, columns.data_type FROM `tables` join columns on tables.project_id = columns.project_id and tables.table_name = columns.table_name where columns.project_id = ?
	result := t.db.Model(&Table{}).Select("tables.table_name, tables.ref_table, tables.db_type, tables.owner, columns.column_name, columns.data_type").Joins("join columns on tables.project_id = columns.project_id and tables.table_name = columns.table_name").Where("columns.project_id = ?", projectID)
	if len(tableNames) != 0 {
		result = result.Where("tables.table_name in ?", tableNames)
	}
	result = result.Scan(&tableColumns)
	if result.Error != nil {
		return nil, nil, result.Error
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
	for _, tableName := range sliceutil.SortMapKeyForDeterminism(tableMap) {
		tableMetas = append(tableMetas, *tableMap[tableName])
	}
	// check table exist
	for _, tableName := range tableNames {
		_, exist := tableMap[tableName]
		if !exist {
			logrus.Warningf("table %s not found", tableName)
			notFoundTables = append(notFoundTables, tableName)
		}
	}
	return
}

func (t *MetaTransaction) GetTables(projectID string, tableNames []string) (tables []Table, allTableExist bool, err error) {
	result := t.db.Model(&Table{}).Where("tables.project_id = ?", projectID).Where("tables.table_name in ?", tableNames).Scan(&tables)
	if result.Error != nil {
		return nil, false, result.Error
	}
	allTableExist = true
	if len(tables) != len(tableNames) {
		allTableExist = false
	}
	return tables, allTableExist, nil
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

type ProjectMeta struct {
	Proj   ProjectWithMember
	Tables []TableMeta
	CCLs   []ColumnPriv
}

// return all tables and ccls for the given project if tableNames and cclDestParties are nil
func (t *MetaTransaction) GetProjectMeta(projectID string, tableNames []string, cclDestParties []string, owner string) (*ProjectMeta, error) {
	var meta ProjectMeta
	proj, err := t.GetProjectAndMembers(projectID)
	if err != nil {
		return nil, err
	}
	meta.Proj = proj

	tables, _, err := t.GetTableMetasByTableNames(projectID, tableNames)
	if err != nil {
		return nil, err
	}
	var ownedTableNames []string
	for _, table := range tables {
		if table.Table.Owner == owner {
			ownedTableNames = append(ownedTableNames, table.Table.TableName)
			meta.Tables = append(meta.Tables, table)
		}
	}

	// if no table in tableNames is owned by current party, just return empty meta
	if len(tableNames) > 0 && len(ownedTableNames) == 0 {
		return &meta, nil
	}
	ccls, err := t.ListColumnConstraints(projectID, ownedTableNames, cclDestParties)
	if err != nil {
		return nil, err
	}
	meta.CCLs = ccls
	return &meta, nil
}

func randString(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	rand.Seed(uint64(time.Now().UnixNano()))
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
