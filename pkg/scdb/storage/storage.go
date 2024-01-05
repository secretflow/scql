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
	"os"
	"reflect"
	"sort"
	"strings"

	"github.com/sethvargo/go-password/password"
	"gorm.io/gorm"

	"github.com/secretflow/scql/pkg/infoschema"
	"github.com/secretflow/scql/pkg/parser/auth"
	"github.com/secretflow/scql/pkg/parser/model"
)

const (
	DefaultHostName = `%`
	DefaultRootName = `root`
	passwordLength  = 16
	numSymbols      = 4
	numDigits       = 8
	noUpper         = false
	allowRepeat     = false
)

var charMap = map[string]string{
	"special": `"#%'_()+/:;<=>?[\]^{|}~`,
	"lower":   `abcdefghijklmnopqrstuvwxyz`,
	"upper":   `ABCDEFGHIJKLMNOPQRSTUVWXYZ`,
	"digits":  `0123456789`,
}

var EnablePasswordCheck = false

// allTables contains all scdb meta tables
// keep creating order
var allTables = []interface{}{&User{}, &Database{}, &Table{}, &Column{}, &DatabasePriv{}, &TablePriv{}, &ColumnPriv{}}

// NeedBootstrap checks if the store is empty
func NeedBootstrap(store *gorm.DB) bool {
	for _, tn := range allTables {
		if store.Migrator().HasTable(tn) {
			return false
		}
	}
	return true
}
func InitPasswordValidation(enablePasswordCheck bool) {
	EnablePasswordCheck = enablePasswordCheck
}

func CheckValidPassword(password string) error {
	if !EnablePasswordCheck {
		return nil
	}
	if len(password) < passwordLength {
		return fmt.Errorf("password should be at least %v long", passwordLength)
	}
	for name, chars := range charMap {
		exist := false
		for _, chr := range password {
			if strings.Contains(chars, string(chr)) {
				exist = true
				break
			}
		}
		if !exist {
			return fmt.Errorf("password should have at least one %v character", name)
		}
	}
	return nil
}

// Bootstrap init store
func Bootstrap(store *gorm.DB) error {
	// Migrate the schemas
	if err := store.AutoMigrate(allTables...); err != nil {
		return err
	}
	rootPassword, err := password.Generate(passwordLength, numDigits, numSymbols, noUpper, allowRepeat)
	if err != nil {
		return err
	}
	// get default password for root user
	if passwd := os.Getenv("SCQL_ROOT_PASSWORD"); passwd != "" {
		rootPassword = passwd
	} else {
		fmt.Printf("generated root password: %v ,  please keep it safe\n", rootPassword)
	}
	if err := CheckValidPassword(rootPassword); err != nil {
		fmt.Printf("[warning] root password does not satisfy the security requirements: %v", err.Error())
	}
	// create default root user
	result := store.Create(&User{
		Host:           DefaultHostName,
		User:           DefaultRootName,
		Password:       auth.EncodePassword(rootPassword),
		CreatePriv:     true,
		CreateUserPriv: true,
		DropPriv:       true,
		GrantPriv:      true,
		DescribePriv:   true,
		ShowPriv:       true,
		CreateViewPriv: true,
	})
	if result.Error != nil {
		return result.Error
	}

	return nil
}

// CheckStorage verifies scdb storage is valid
func CheckStorage(store *gorm.DB) error {
	for _, tn := range allTables {
		if !store.Migrator().HasTable(tn) {
			return fmt.Errorf("table %s is missing in storage", reflect.TypeOf(tn).String())
		}
	}
	// check if root user exist
	if exist, err := CheckUserExist(store, DefaultRootName, DefaultHostName); err != nil || !exist {
		if err != nil {
			return err
		}
		return fmt.Errorf("root is missing in storage")
	}
	// TODO(shunde.csd): check table schema
	return nil
}

func QueryUserPartyCode(store *gorm.DB, userName, host string) (string, error) {
	var user User

	result := store.Where(User{User: userName, Host: host}).Find(&user)
	if result.Error != nil {
		return "", result.Error
	}
	if result.RowsAffected == 0 {
		return "", fmt.Errorf("user %s@%s not found", userName, host)
	}
	if user.PartyCode == "" {
		return "", fmt.Errorf("there is no party code for user %s@%s", userName, host)
	}
	return user.PartyCode, nil
}

func FindUserByParty(store *gorm.DB, partyCode string) (*User, error) {
	var user User
	result := store.Where(User{PartyCode: partyCode}).Find(&user)
	if result.Error != nil {
		return nil, result.Error
	}
	if result.RowsAffected != 1 {
		return nil, fmt.Errorf("expect only one user for party: %s, but got %d", partyCode, result.RowsAffected)
	}
	return &user, nil
}

func QueryInfoSchema(store *gorm.DB) (result infoschema.InfoSchema, err error) {
	callFc := func(tx *gorm.DB) error {
		result, err = queryInfoSchema(tx)
		return err
	}
	if err := store.Transaction(callFc, &sql.TxOptions{ReadOnly: true}); err != nil {
		return nil, fmt.Errorf("queryInfoSchema: %v", err)
	}
	return result, nil
}

func queryInfoSchema(store *gorm.DB) (infoschema.InfoSchema, error) {
	// FIXME(shunde.csd): use `infoschema.FromTableSchema` to construct infoschema.InfoSchema will lead
	// empty database missing in infoschema.InfoSchema

	// query database
	var dbNames []string
	result := store.Model(&Database{}).Pluck("db", &dbNames)
	if result.Error != nil {
		return nil, result.Error
	}

	dbMap := make(map[string][]string, len(dbNames))
	{
		// query tables in database
		var tables []Table
		result = store.Model(&Table{}).Where("db in ?", dbNames).Find(&tables)
		if result.Error != nil {
			return nil, result.Error
		}

		for _, table := range tables {
			dbMap[table.Db] = append(dbMap[table.Db], table.Table)
		}
	}

	var cols []Column
	for db, tables := range dbMap {
		var columns []Column
		result := store.Model(&Column{}).Where("db = ? AND table_name in ?", db, tables).Find(&columns)
		if result.Error != nil {
			return nil, result.Error
		}
		cols = append(cols, columns...)
	}

	tableSchemasMap := map[string]*infoschema.TableSchema{}
	for _, col := range cols {
		fullTableName := col.Db + "." + col.TableName
		if _, ok := tableSchemasMap[fullTableName]; !ok {
			tableSchemasMap[fullTableName] = &infoschema.TableSchema{
				DbName:    col.Db,
				TableName: col.TableName,
				Columns:   nil,
			}
		}
		m := tableSchemasMap[fullTableName]
		m.Columns = append(m.Columns, infoschema.ColumnDesc{
			Name:        col.ColumnName,
			Type:        col.Type,
			Description: col.Description,
		})
	}
	var tableSchemas []*infoschema.TableSchema
	for _, v := range tableSchemasMap {
		tableSchemas = append(tableSchemas, v)
	}
	return infoschema.FromTableSchema(tableSchemas)
}

func QueryDBInfoSchema(store *gorm.DB, dbName string) (result infoschema.InfoSchema, err error) {
	callFc := func(tx *gorm.DB) error {
		result, err = queryDBInfoSchema(tx, dbName)
		return err
	}
	if err := store.Transaction(callFc, &sql.TxOptions{ReadOnly: true}); err != nil {
		return nil, fmt.Errorf("queryDBInfoSchema: %v", err)
	}
	return result, nil
}

func queryDBInfoSchema(store *gorm.DB, dbName string) (infoschema.InfoSchema, error) {
	var dbNames []string
	if dbName != "" {
		dbNames = append(dbNames, dbName)
	} else {
		result := store.Model(&Database{}).Pluck("db", &dbNames)
		if result.Error != nil {
			return nil, result.Error
		}
	}

	dbMap := make(map[string][]Table, len(dbNames))
	{
		// query tables in database
		var tables []Table
		result := store.Model(&Table{}).Where("db in ?", dbNames).Find(&tables)
		if result.Error != nil {
			return nil, result.Error
		}

		for _, table := range tables {
			dbMap[table.Db] = append(dbMap[table.Db], table)
		}
	}

	info := make(map[string][]*model.TableInfo)
	for db, tables := range dbMap {
		tblNames := make([]string, len(tables))
		for _, tbl := range tables {
			tblNames = append(tblNames, tbl.Table)
		}

		var columns []Column
		result := store.Model(&Column{}).Where("db = ? AND table_name in ?", db, tblNames).Find(&columns)
		if result.Error != nil {
			return nil, result.Error
		}

		colMap := make(map[string][]Column)
		for _, col := range columns {
			colMap[col.TableName] = append(colMap[col.TableName], col)
		}

		var tableInfos []*model.TableInfo
		for i, tbl := range tables {
			tblInfo := &model.TableInfo{
				ID:          int64(i),
				TableId:     fmt.Sprint(i),
				Name:        model.NewCIStr(tbl.Table),
				Columns:     []*model.ColumnInfo{},
				Indices:     []*model.IndexInfo{},
				ForeignKeys: []*model.FKInfo{},
				State:       model.StatePublic,
				PKIsHandle:  false,
			}

			cols, exists := colMap[tbl.Table]
			if !exists || len(cols) == 0 {
				return nil, fmt.Errorf("table %s.%s not exists", db, tbl.Table)
			}

			if tbl.IsView {
				tblInfo.View = &model.ViewInfo{
					Algorithm:  model.AlgorithmMerge,
					SelectStmt: tbl.SelectString,
				}
				// FIXME(shunde.csd): Maybe we should sort columns for table too, use order by in SQL instead of sorting manually
				sort.Slice(cols, func(i, j int) bool { return cols[i].OrdinalPosition < cols[j].OrdinalPosition })
			}

			for i, col := range cols {
				colTyp := strings.ToLower(col.Type)
				defaultVal, err := infoschema.TypeDefaultValue(colTyp)
				if err != nil {
					return nil, err
				}
				fieldTp, err := infoschema.TypeConversion(colTyp)
				if err != nil {
					return nil, err
				}
				colInfo := &model.ColumnInfo{
					ID:                 int64(i),
					Name:               model.NewCIStr(col.ColumnName),
					Offset:             i,
					OriginDefaultValue: defaultVal,
					DefaultValue:       defaultVal,
					DefaultValueBit:    []byte{},
					Dependences:        map[string]struct{}{},
					FieldType:          fieldTp,
					State:              model.StatePublic,
					Comment:            col.Description,
				}
				tblInfo.Columns = append(tblInfo.Columns, colInfo)
			}
			tableInfos = append(tableInfos, tblInfo)
		}

		info[db] = tableInfos
	}
	return infoschema.MockInfoSchema(info), nil
}

func QueryTableSchemas(store *gorm.DB, dbName string, tableNames []string) (result []*infoschema.TableSchema, err error) {
	callFc := func(tx *gorm.DB) error {
		result, err = queryTableSchemas(tx, dbName, tableNames)
		return err
	}
	if err := store.Transaction(callFc, &sql.TxOptions{ReadOnly: true}); err != nil {
		return nil, fmt.Errorf("queryTableSchemas: %v", err)
	}
	return result, nil
}

func queryTableSchemas(store *gorm.DB, dbName string, tableNames []string) ([]*infoschema.TableSchema, error) {
	var tableSchemas []*infoschema.TableSchema
	for _, tn := range tableNames {
		var cols []Column
		result := store.Where(&Column{Db: dbName, TableName: tn}).Find(&cols)
		if result.Error != nil {
			return nil, result.Error
		}
		if len(cols) == 0 {
			return nil, fmt.Errorf("table %s.%s not exists", dbName, tn)
		}

		var colRecords []infoschema.ColumnDesc
		for _, col := range cols {
			colRecords = append(colRecords, infoschema.ColumnDesc{
				Name:        col.ColumnName,
				Type:        col.Type,
				Description: col.Description,
			})
		}
		tableSchemas = append(tableSchemas, &infoschema.TableSchema{
			DbName:    dbName,
			TableName: tn,
			Columns:   colRecords,
		})
	}
	return tableSchemas, nil
}

func QueryTablesOwner(store *gorm.DB, dbName string, tns []string) ([]string, error) {
	var owners []string
	result := store.Model(&Table{}).Where("db = ? AND `table_name` in ?", dbName, tns).Distinct().Pluck("owner", &owners)
	if result.Error != nil {
		return nil, result.Error
	}
	return owners, nil
}

// Get All view names in database `dbName`
func QueryAllViewsInDb(store *gorm.DB, dbName string) ([]string, error) {
	var tableNames []string
	result := store.Model(&Table{}).Where("db = ? AND is_view = true", dbName).Pluck("table_name", &tableNames)
	if result.Error != nil {
		return nil, result.Error
	}
	return tableNames, nil
}

func CheckDatabaseExist(db *gorm.DB, dbName string) (bool, error) {
	database := Database{}
	result := db.Where(&Database{Db: dbName}).Find(&database)
	if result.Error != nil {
		return false, fmt.Errorf("checkDatabaseExist failed: %v", result.Error)
	}
	if result.RowsAffected > 0 {
		return true, nil
	}
	return false, nil
}

func GetColumnOriginNameByLowerName(tx *gorm.DB, dbName, tblName string, colNames []string) ([]string, error) {
	var orginNames []string
	if err := tx.Model(&Column{}).Select("column_name").Where(&Column{
		Db:        dbName,
		TableName: tblName,
	}).Find(&orginNames, "lower(column_name) IN ?", colNames).Error; err != nil {
		return nil, err
	}
	return orginNames, nil
}

func CheckColumnsExist(tx *gorm.DB, dbName, tblName string, colNames []string) error {
	var columnPrivExist []string
	if err := tx.Model(&Column{}).Select("lower(column_name)").Where(&Column{
		Db:        dbName,
		TableName: tblName,
	}).Find(&columnPrivExist, "column_name IN ?", colNames).Error; err != nil {
		return err
	}

	columnPrivExistMap := make(map[string]bool, len(columnPrivExist))
	for _, col := range columnPrivExist {
		columnPrivExistMap[col] = true
	}

	for _, colName := range colNames {
		if _, exist := columnPrivExistMap[colName]; !exist {
			return fmt.Errorf("couldn't find column: %s", colName)
		}
	}
	return nil
}

func CheckTableExist(db *gorm.DB, dbName, tblName string) (bool, error) {
	table := Table{}
	result := db.Where(&Table{Db: dbName, Table: tblName}).Find(&table)
	if result.Error != nil {
		return false, fmt.Errorf("checkTableExist failed: %v", result.Error)
	}
	if result.RowsAffected > 0 {
		return true, nil
	}
	return false, nil
}

func CheckTableOwner(db *gorm.DB, dbName, tblName, userName, hostName string) error {
	table := &Table{}
	result := db.Where(&Table{Db: dbName, Table: tblName}).Find(table)
	if result.Error != nil {
		return fmt.Errorf("CheckTable failed: %v", result.Error)
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("table %v.%v doesn't exists", dbName, tblName)
	}
	if table.Owner != userName || table.Host != hostName {
		return fmt.Errorf("user %v.%v is not the owner of table %v.%v", userName, hostName, dbName, tblName)
	}
	return nil
}

func FindUser(db *gorm.DB, userName string, hostName string) (*User, error) {
	var user User
	result := db.Where(&User{User: userName, Host: hostName}).Find(&user)
	if result.Error != nil {
		return nil, fmt.Errorf("findUser failed: %v", result.Error)
	}
	if result.RowsAffected == 0 {
		return nil, fmt.Errorf("user %v doesn't exists", userName)
	}
	return &user, nil
}

func CheckUserExist(store *gorm.DB, name string, host string) (bool, error) {
	var user User
	result := store.Where(&User{User: name, Host: host}).Find(&user)
	if result.Error != nil {
		return false, fmt.Errorf("checkUserExist: %v", result.Error)
	}
	return result.RowsAffected > 0, nil
}
