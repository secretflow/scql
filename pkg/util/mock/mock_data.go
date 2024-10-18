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

package mock

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"gorm.io/gorm"

	"github.com/secretflow/scql/pkg/interpreter/ccl"
	"github.com/secretflow/scql/pkg/parser/auth"
	"github.com/secretflow/scql/pkg/parser/model"
	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/parser/types"
	"github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/scdb/storage"
	"github.com/secretflow/scql/pkg/sessionctx"
	"github.com/secretflow/scql/pkg/util/sliceutil"
)

var MockDBPath = "db.json"

var dTypeString2FieldType = map[string]types.FieldType{
	"int":       *(types.NewFieldType(mysql.TypeLong)),
	"string":    *(types.NewFieldType(mysql.TypeString)),
	"float":     *(types.NewFieldType(mysql.TypeFloat)),
	"datetime":  *(types.NewFieldType(mysql.TypeDatetime)),
	"timestamp": *(types.NewFieldType(mysql.TypeTimestamp)),
}

var cclString2CCLLevel = map[string]ccl.CCLLevel{
	"plain":       ccl.Plain,
	"join":        ccl.Join,
	"joinpayload": ccl.AsJoinPayload,
	"groupby":     ccl.GroupBy,
	"aggregate":   ccl.Aggregate,
	"compare":     ccl.Compare,
	"encrypt":     ccl.Encrypt,
}

// TODO: rename PhysicalTableMeta
type PhysicalTableMeta struct {
	DBName    string
	TableName string
	DBType    string
	Columns   []columnMeta
}

type columnMeta struct {
	Name  string
	DType string
}

func (pt *PhysicalTableMeta) ToCreateTableStmt(newTblName string, ifNotExists bool) string {
	var b strings.Builder
	b.WriteString("CREATE TABLE")
	if ifNotExists {
		b.WriteString(" IF NOT EXISTS")
	}
	b.WriteString(" ")
	b.WriteString(newTblName)
	b.WriteString(" (")
	for i, col := range pt.Columns {
		if i != 0 {
			b.WriteString(",")
		}
		b.WriteString(fmt.Sprintf("%s %s", col.Name, col.DType))
	}
	b.WriteString(") ")
	b.WriteString(fmt.Sprintf("REF_TABLE=%s.%s DB_TYPE='%s'", pt.DBName, pt.TableName, pt.DBType))
	return b.String()
}

func (pt *PhysicalTableMeta) RefTable() string {
	return fmt.Sprintf("%s.%s", pt.DBName, pt.TableName)
}

func (pt *PhysicalTableMeta) GetColumnDesc() []*scql.CreateTableRequest_ColumnDesc {
	var result []*scql.CreateTableRequest_ColumnDesc
	for _, col := range pt.Columns {
		result = append(result, &scql.CreateTableRequest_ColumnDesc{Name: col.Name, Dtype: col.DType})
	}
	return result
}

type allDBData struct {
	DbName    string
	PartyCode string
	Tables    map[string]tableInfo
	DBType    string
}

type db struct {
	TableFiles []string          `json:"table_files"`
	DBInfo     map[string]dbInfo `json:"db_info"`
}

type dbInfo struct {
	PartyCode string `json:"party_code"`
	DBType    string `json:"db_type"`
}

type tableInfo struct {
	DbName  string       `json:"db_name"`
	Columns []columnInfo `json:"columns"`
}
type columnInfo struct {
	ColumnName string   `json:"column_name"`
	Dtype      string   `json:"dtype"`
	CCL        []string `json:"ccl"`
}

type MockCCLConfig struct {
	dbName     string
	tableName  string
	columnName string
	party      string
	level      ccl.CCLLevel
}

func getByteArrayFromJson(filePath string) (res []byte, err error) {
	jsonFile, err := os.Open(filePath)
	if err != nil {
		return res, err
	}
	defer func() {
		if err1 := jsonFile.Close(); err == nil && err1 != nil {
			err = err1
		}
	}()
	byteValue, err := io.ReadAll(jsonFile)
	if err != nil {
		return res, err
	}
	// Remove comments, since they are not allowed in json.
	re := regexp.MustCompile("(?s)//.*?\n")
	return re.ReplaceAll(byteValue, nil), nil
}

func getDataFromJson[retType any](filePath string) (res retType, err error) {
	byteValue, err := getByteArrayFromJson(filePath)
	if err != nil {
		return
	}
	err = json.Unmarshal(byteValue, &res)
	return
}

func getMockData() ([]allDBData, []string, error) {
	var mock_db db
	var err error
	mockDBPath := MockDBPath
	if filepath.IsAbs(MockDBPath) {
		mock_db, err = getDataFromJson[db](MockDBPath)
		if err != nil {
			return nil, nil, err
		}
	} else {
		pre := "util/mock/testdata"
		workDir, _ := os.Getwd()
		re := regexp.MustCompile(".*/pkg")
		dir := re.FindString(workDir)

		if dir == "" {
			re = regexp.MustCompile(".*/cmd")
			dir = re.FindString(workDir)
			if dir == "" {
				return nil, nil, fmt.Errorf("cannot find pkg dir")
			}
			dir, _ = filepath.Split(dir)
			dir = dir + "pkg"
		}
		mockDBPath = filepath.Join(filepath.Join(dir, pre), MockDBPath)
		mock_db, err = getDataFromJson[db](mockDBPath)
		if err != nil {
			return nil, nil, err
		}
	}

	all_dbs := make(map[string]allDBData)
	for name, info := range mock_db.DBInfo {
		all_dbs[name] = allDBData{
			DbName:    name,
			PartyCode: info.PartyCode,
			DBType:    info.DBType,
			Tables:    make(map[string]tableInfo),
		}
	}
	for _, file := range mock_db.TableFiles {
		mock_table, err := getDataFromJson[map[string]tableInfo](filepath.Join(filepath.Dir(mockDBPath), file))
		if err != nil {
			return nil, nil, err
		}
		for table_name, table := range mock_table {
			if data, ok := all_dbs[table.DbName]; !ok {
				return nil, nil, fmt.Errorf("db %s not found", table.DbName)
			} else {
				data.DbName = table.DbName
				data.Tables[table_name] = table
			}
		}
	}
	var res []allDBData
	var allPartyCodes []string
	for _, key := range sliceutil.SortMapKeyForDeterminism(all_dbs) {
		res = append(res, all_dbs[key])
		allPartyCodes = append(allPartyCodes, all_dbs[key].PartyCode)
	}
	return res, allPartyCodes, nil
}

func AssignTableId(dbTables map[string][]*model.TableInfo) {
	var dbNames []string
	for dbName := range dbTables {
		dbNames = append(dbNames, dbName)
	}
	sort.Strings(dbNames)

	count := int64(0)
	for _, dbName := range dbNames {
		for _, table := range dbTables[dbName] {
			table.ID = count
			count += 1
		}
	}
}

func createTableSchema(tableName string, columns map[string]types.FieldType) *model.TableInfo {
	var columnNames []string
	for name := range columns {
		columnNames = append(columnNames, name)
	}
	sort.Strings(columnNames)

	columnInfos := []*model.ColumnInfo{}
	for i, name := range columnNames {
		columnInfos = append(columnInfos,
			&model.ColumnInfo{
				State:     model.StatePublic,
				Offset:    i,
				Name:      model.NewCIStr(name),
				FieldType: columns[name],
				ID:        int64(i + 1),
			})
	}
	table := &model.TableInfo{
		Columns: columnInfos,
		Name:    model.NewCIStr(tableName),
	}
	return table
}

func MockAllTables() (map[string][]*model.TableInfo, error) {
	data, _, err := getMockData()
	if err != nil {
		return nil, err
	}

	dbTables := make(map[string][]*model.TableInfo)

	for _, db := range data {
		if dbTables[db.DbName] == nil {
			dbTables[db.DbName] = []*model.TableInfo{}
		}
		for tableName, table := range db.Tables {
			columnInfos := map[string]types.FieldType{}
			for _, column := range table.Columns {
				columnInfos[column.ColumnName] = dTypeString2FieldType[column.Dtype]
			}
			tableSchema := createTableSchema(tableName, columnInfos)
			tableSchema.PartyCode = db.PartyCode
			dbTables[db.DbName] = append(dbTables[db.DbName], tableSchema)
		}
	}

	AssignTableId(dbTables)
	return dbTables, nil
}

// MockContext is only used for plan related tests.
func MockContext() sessionctx.Context {
	ctx := sessionctx.NewContext()
	ctx.GetSessionVars().CurrentDB = "test"
	return ctx
}

func GetCCLForParty(party, selfParty string, allPartyCodes []string, ccls []string) (ccl.CCLLevel, error) {
	if len(ccls) != len(allPartyCodes) && len(ccls) != 1 {
		return ccl.Unknown, fmt.Errorf("err ccl conf: ccls(%+v) for parties(%+v)", ccls, allPartyCodes)
	}
	if len(ccls) == 1 {
		if party == selfParty {
			return ccl.Plain, nil
		}
		return cclString2CCLLevel[ccls[0]], nil
	}
	for i, p := range allPartyCodes {
		if p == party {
			return cclString2CCLLevel[ccls[i]], nil
		}
	}
	return ccl.Unknown, fmt.Errorf("bad party(%s) not in all party codes(%+v)", party, allPartyCodes)
}

func GetAllCCL() ([]*MockCCLConfig, error) {
	data, allPartyCodes, err := getMockData()
	if err != nil {
		return nil, err
	}
	ccls := []*MockCCLConfig{}
	for _, db := range data {
		for tableName, table := range db.Tables {
			for _, column := range table.Columns {
				if len(column.CCL) == 0 {
					continue
				}
				for _, p := range allPartyCodes {
					level, err := GetCCLForParty(p, db.PartyCode, allPartyCodes, column.CCL)
					if err != nil {
						return nil, err
					}
					ccls = append(ccls, &MockCCLConfig{
						dbName:     db.DbName,
						tableName:  tableName,
						columnName: column.ColumnName,
						party:      p,
						level:      level,
					})
				}
			}
		}
	}
	return ccls, nil
}

func ConstructCCLWithColumnName(ccls []*MockCCLConfig) ([]*scql.SecurityConfig_ColumnControl, error) {
	result := []*scql.SecurityConfig_ColumnControl{}
	for _, ccl := range ccls {
		result = append(result, &scql.SecurityConfig_ColumnControl{
			DatabaseName: ccl.dbName,
			TableName:    ccl.tableName,
			ColumnName:   ccl.columnName,
			PartyCode:    ccl.party,
			Visibility:   scql.SecurityConfig_ColumnControl_Visibility(ccl.level),
		})
	}
	return result, nil
}

func MockAllCCL() ([]*scql.SecurityConfig_ColumnControl, error) {
	ccls, err := GetAllCCL()
	if err != nil {
		return nil, err
	}
	return ConstructCCLWithColumnName(ccls)
}

func MockAllCCLPlaintext() ([]*scql.SecurityConfig_ColumnControl, error) {
	ccls, err := GetAllCCL()
	if err != nil {
		return nil, err
	}
	plaintextCCL := []*MockCCLConfig{}
	for _, cc := range ccls {
		cc.level = ccl.Plain
		plaintextCCL = append(plaintextCCL, cc)
	}
	return ConstructCCLWithColumnName(plaintextCCL)
}

type MockEnginesInfo struct {
	PartyToUrls        map[string]string
	PartyToCredentials map[string]string
	PartyToTables      map[string][]string
	TableToRefs        map[string]string
}

func MockEngines() (*MockEnginesInfo, error) {
	data, allPartyCodes, err := getMockData()
	if err != nil {
		return nil, err
	}
	result := &MockEnginesInfo{
		PartyToUrls:        make(map[string]string),
		PartyToCredentials: make(map[string]string),
		PartyToTables:      make(map[string][]string),
		TableToRefs:        make(map[string]string),
	}
	for _, p := range allPartyCodes {
		result.PartyToUrls[p] = fmt.Sprintf("%s.com", p)
		result.PartyToCredentials[p] = fmt.Sprintf("%s_credential", p)
	}
	for _, db := range data {
		for tableName := range db.Tables {
			qualifiedName := strings.Join([]string{db.DbName, tableName}, ".")
			result.PartyToTables[db.PartyCode] = append(result.PartyToTables[db.PartyCode], qualifiedName)
			result.TableToRefs[qualifiedName] = qualifiedName
		}
	}
	return result, nil
}

func MockPhysicalTableMetas() (map[string]*PhysicalTableMeta, error) {
	data, _, err := getMockData()
	if err != nil {
		return nil, err
	}
	result := make(map[string]*PhysicalTableMeta)
	for _, db := range data {
		for tableName, tableConf := range db.Tables {
			pt := &PhysicalTableMeta{
				DBName:    db.DbName,
				TableName: tableName,
				DBType:    db.DBType,
			}
			for _, col := range tableConf.Columns {
				pt.Columns = append(pt.Columns, columnMeta{
					Name:  col.ColumnName,
					DType: col.Dtype,
				})
			}
			result[fmt.Sprintf(`%s_%s`, pt.DBName, pt.TableName)] = pt
		}
	}
	return result, nil
}

func MockStorage(db *gorm.DB) error {
	// initialize users
	users := []storage.User{
		{User: "alice", Host: "%", PartyCode: "alice", Password: auth.EncodePassword("alice123"), EngineEndpoints: "engine.alice.com", EngineToken: "alice_credential"},
		{User: "bob", Host: "%", PartyCode: "bob", Password: auth.EncodePassword("bob123"), EngineEndpoints: "engine.bob.com", EngineToken: "bob_credential"},
	}
	result := db.Create(&users)
	if result.Error != nil || result.RowsAffected != 2 {
		return fmt.Errorf("failed to initialize user table")
	}
	userRoot := storage.User{
		Host:           "%",
		User:           `root`,
		Password:       auth.EncodePassword("root"),
		CreatePriv:     true,
		CreateUserPriv: true,
		DropPriv:       true,
		GrantPriv:      true,
		DescribePriv:   true,
		ShowPriv:       true,
		CreateViewPriv: true,
	}
	result = db.FirstOrCreate(&storage.User{}, userRoot)
	if result.Error != nil {
		return fmt.Errorf("failed to initialize user root")
	}

	// initialize database
	database := storage.Database{
		Db: "test",
	}
	result = db.Create(&database)
	if result.Error != nil || result.RowsAffected != 1 {
		return fmt.Errorf("failed to initialize database table")
	}

	// initialize tables
	tables := []storage.Table{
		{Db: "test", Table: "table_1", Owner: "alice", Host: "%", RefTable: "table_1", RefDb: "test"},
		{Db: "test", Table: "table_2", Owner: "bob", Host: "%", RefTable: "table_2", RefDb: "test"},
		{Db: "test", Table: "table_3", Owner: "alice", Host: "%", RefTable: "table_3", RefDb: "test"},
	}
	result = db.Create(&tables)
	if result.Error != nil || result.RowsAffected != 3 {
		return fmt.Errorf("failed to initialize `table` table")
	}

	// initialize columns
	columns := []storage.Column{
		{Db: "test", TableName: "table_1", ColumnName: "column1_1", Type: "int"},
		{Db: "test", TableName: "table_1", ColumnName: "column1_2", Type: "int"},
		{Db: "test", TableName: "table_1", ColumnName: "column1_3", Type: "int"},
		{Db: "test", TableName: "table_2", ColumnName: "column2_1", Type: "int"},
		{Db: "test", TableName: "table_2", ColumnName: "column2_2", Type: "int"},
		{Db: "test", TableName: "table_3", ColumnName: "column3_1", Type: "int"},
	}

	result = db.Create(&columns)
	if result.Error != nil || result.RowsAffected != 6 {
		return fmt.Errorf("failed to initialize column table")
	}

	// initialize table level privileges
	tblPrivs := []storage.TablePriv{
		{Db: "test", TableName: "table_1", User: "alice", Host: "%", VisibilityPriv: mysql.EncryptedOnlyPriv},
		{Db: "test", TableName: "table_1", User: "bob", Host: "%", VisibilityPriv: mysql.PlaintextPriv},
		{Db: "test", TableName: "table_2", User: "alice", Host: "%", VisibilityPriv: mysql.PlaintextPriv},
		{Db: "test", TableName: "table_2", User: "bob", Host: "%", VisibilityPriv: mysql.EncryptedOnlyPriv},
	}
	result = db.Create(&tblPrivs)
	if result.Error != nil || result.RowsAffected != 4 {
		return fmt.Errorf("failed to initialize table privileges table")
	}

	// initialize column level privileges
	colPrivs := []storage.ColumnPriv{
		{Db: "test", TableName: "table_1", ColumnName: "column1_1", User: "alice", Host: "%", VisibilityPriv: mysql.PlaintextPriv},
		{Db: "test", TableName: "table_1", ColumnName: "column1_2", User: "alice", Host: "%", VisibilityPriv: mysql.PlaintextAfterComparePriv},
		{Db: "test", TableName: "table_1", ColumnName: "column1_3", User: "bob", Host: "%", VisibilityPriv: mysql.PlaintextAfterComparePriv},
	}
	result = db.Create(&colPrivs)
	if result.Error != nil || result.RowsAffected != 3 {
		return fmt.Errorf("failed to initialize column privileges table")
	}
	return nil
}
