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

package regtest

import (
	"database/sql"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	"github.com/secretflow/scql/pkg/parser/model"
	"github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/scdb/config"
)

// NumericalPrecision is based on experiment
const (
	NumericalPrecision float64 = 1e-1
)

var (
	TableToPartyCode map[string]string
)

func FillTableToPartyCodeMap(dbTables map[string][]*model.TableInfo) {
	TableToPartyCode = make(map[string]string)
	for db, tables := range dbTables {
		for _, t := range tables {
			TableToPartyCode[fmt.Sprintf("%s_%s", db, t.Name)] = t.PartyCode
		}
	}
}

type QueryCase struct {
	Name       string       `json:"name"`
	View       []string     `json:"view"`
	Query      string       `json:"query"`
	MySQLQuery string       `json:"mysql_query"`
	Result     *ResultTable `json:"result"`
}

type QueryTestSuit struct {
	Queries []QueryCase
}

type mysqlServerConfig struct {
	Host            string `json:"host"`
	Username        string `json:"username"`
	Passwd          string `json:"passwd"`
	Dbname          string `json:"dbname"`
	MaxOpenConns    int    `json:"max_open_conn"`
	MaxIdleConns    int    `json:"max_idle_conn"`
	ConnMaxLifetime int    `json:"conn_max_life_time"`
	Port            int    `json:"port"`
}

type ResultTable struct {
	Column []*ResultColumn `json:"column"`
}

type ResultColumn struct {
	Name    string    `json:"name"`
	Ss      []string  `json:"string"`
	Int64s  []int64   `json:"int"`
	Bools   []bool    `json:"bool"`
	Doubles []float64 `json:"float"`
}

func (c *ResultColumn) Getlen() int {
	return len(c.Ss) + len(c.Int64s) + len(c.Bools) + len(c.Doubles)
}

func roundTo3Decimals(val float64) float64 {
	return math.Round(val*1000) / 1000
}

func getColumnPrecedence(ta, tb *ResultTable) []int {
	var roundColumnIndexes, preciseColumnIndexes []int
	for i := range ta.Column {
		// Lower the precedence of columns that are likely to introduce errors in comparisons.
		if ta.Column[i].Doubles != nil || tb.Column[i].Doubles != nil {
			roundColumnIndexes = append(roundColumnIndexes, i)
		} else {
			preciseColumnIndexes = append(preciseColumnIndexes, i)
		}
	}
	return append(roundColumnIndexes, preciseColumnIndexes...)
}

func (t *ResultTable) getOrderedIndexes(columnPrecedence []int) []int {
	len := t.Column[0].Getlen()
	indexes := make([]int, len)
	for i := 0; i < len; i++ {
		indexes[i] = i
	}

	for _, i := range columnPrecedence {
		col := t.Column[i]
		if col.Ss != nil {
			sort.SliceStable(indexes, func(i, j int) bool {
				return col.Ss[indexes[i]] < col.Ss[indexes[j]]
			})
		}
		if col.Int64s != nil {
			sort.SliceStable(indexes, func(i, j int) bool {
				return col.Int64s[indexes[i]] < col.Int64s[indexes[j]]
			})
		}
		if col.Bools != nil {
			sort.SliceStable(indexes, func(i, j int) bool {
				return !col.Bools[indexes[i]] && col.Bools[indexes[j]]
			})
		}
		if col.Doubles != nil {
			sort.SliceStable(indexes, func(i, j int) bool {
				return roundTo3Decimals(col.Doubles[indexes[i]]) < roundTo3Decimals(col.Doubles[indexes[j]])
			})
		}
	}

	return indexes
}

func (t *ResultTable) convertToRows() []string {
	rowStrings := t.Column[0].toStringSlice()
	for _, c := range t.Column[1:] {
		for i, s := range c.toStringSlice() {
			rowStrings[i] = rowStrings[i] + s
		}
	}
	for i := range t.Column[0].toStringSlice() {
		rowStrings[i] = fmt.Sprintf("[%d]", i) + rowStrings[i] + "\n"
	}
	return rowStrings
}

func isColumnNil(col *ResultColumn) bool {
	if len(col.Bools) == 0 && len(col.Doubles) == 0 && len(col.Int64s) == 0 && len(col.Ss) == 0 {
		return true
	}
	return false
}

func (t *ResultTable) equalTo(o *ResultTable) bool {
	if len(t.Column) != len(o.Column) {
		return false
	}
	for i, col := range t.Column {
		// if both of datas are nil return true
		if isColumnNil(col) && isColumnNil(o.Column[i]) {
			return true
		}
		if !col.equalTo(o.Column[i]) {
			return false
		}
	}
	return true
}

func (c *ResultColumn) changeOrders(orders []int) {
	if c.Ss != nil {
		newSs := make([]string, len(c.Ss))
		for i, order := range orders {
			newSs[i] = c.Ss[order]
		}
		c.Ss = newSs
	}
	if c.Int64s != nil {
		newInts := make([]int64, len(c.Int64s))
		for i, order := range orders {
			newInts[i] = c.Int64s[order]
		}
		c.Int64s = newInts
	}
	if c.Bools != nil {
		newBools := make([]bool, len(c.Bools))
		for i, order := range orders {
			newBools[i] = c.Bools[order]
		}
		c.Bools = newBools
	}
	if c.Doubles != nil {
		newDoubles := make([]float64, len(c.Doubles))
		for i, order := range orders {
			newDoubles[i] = c.Doubles[order]
		}
		c.Doubles = newDoubles
	}
}

func (c *ResultColumn) toStringSlice() []string {
	var res []string
	if c.Ss != nil {
		for _, s := range c.Ss {
			res = append(res, fmt.Sprintf(`"%v"s`, s))
		}
		return res
	} else if c.Bools != nil {
		for _, b := range c.Bools {
			res = append(res, fmt.Sprintf(`"%v"b`, b))
		}
		return res
	} else if c.Doubles != nil {
		for _, f := range c.Doubles {
			// NOTE(@yang.y): SCQL handles float differently from MySQL
			res = append(res, fmt.Sprintf(`"%-.6f"f`, f))
		}
		return res
	} else {
		for _, i := range c.Int64s {
			res = append(res, fmt.Sprintf(`"%d"i`, i))
		}
		return res
	}
}

func (c *ResultColumn) equalTo(o *ResultColumn) bool {
	cLen := slices.Max([]int{len(c.Ss), len(c.Int64s), len(c.Bools), len(c.Doubles)})
	oLen := slices.Max([]int{len(o.Ss), len(o.Int64s), len(o.Bools), len(o.Doubles)})
	if cLen != oLen {
		logrus.Infof("the number of rows is not equal: %d != %d", cLen, oLen)
		return false
	}
	if c.Ss != nil {
		return reflect.DeepEqual(c.Ss, o.Ss)
	} else if c.Bools != nil || o.Bools != nil {
		if c.Bools != nil && o.Bools != nil {
			for i, d := range c.Bools {
				if d != o.Bools[i] {
					logrus.Infof("line number %d bools error %v != %v", i, d, o.Bools[i])
					return false
				}
			}
			return true
		}
		if o.Int64s != nil {
			for i, data := range c.Bools {
				if data && o.Int64s[i] != 1 {
					logrus.Infof("line number %d bools error bool(%v) != int(%d)", i, data, o.Int64s[i])
					return false
				}
				if !data && o.Int64s[i] != 0 {
					logrus.Infof("line number %d bools error bool(%v) != int(%d)", i, data, o.Int64s[i])
					return false
				}
			}
			return true
		}
		if c.Int64s != nil {
			for i, data := range o.Bools {
				if data && c.Int64s[i] != 1 {
					logrus.Infof("line number %d bools error int(%d) != bool(%v)", i, c.Int64s[i], data)
					return false
				}
				if !data && c.Int64s[i] != 0 {
					logrus.Infof("line number %d bools error int(%d) != bool(%v)", i, c.Int64s[i], data)
					return false
				}
			}
			return true
		}
		logrus.Info("unexpected data type for bool")
		return false
	} else if c.Doubles != nil || o.Doubles != nil {
		if c.Doubles != nil && o.Doubles != nil {
			for i, d := range c.Doubles {
				if !almostEqual(d, o.Doubles[i]) {
					logrus.Infof("line number %d floats error float64(%f) != float64(%f)", i, d, o.Doubles[i])
					return false
				}
			}
			return true
		}

		if o.Int64s != nil {
			for i, d := range c.Doubles {
				if !almostEqual(d, float64(o.Int64s[i])) {
					logrus.Infof("line number %d floats error float64(%f) != int(%d)", i, d, o.Int64s[i])
					return false
				}
			}
			return true
		}

		if c.Int64s != nil {
			for i, d := range o.Doubles {
				if !almostEqual(d, float64(c.Int64s[i])) {
					logrus.Infof("line number %d floats error int(%d) != float32(%f)", i, c.Int64s[i], d)
					return false
				}
			}
			return true
		}
		logrus.Info("unexpected data type for float")
		return false
	} else if c.Int64s != nil && o.Int64s != nil {
		for i, d := range c.Int64s {
			if d != o.Int64s[i] {
				logrus.Infof("line number %d floats error int(%d) != int(%d)", i, d, o.Int64s[i])
				return false
			}
		}
		return true
	} else {
		logrus.Info("unsupported data type not in (bool, string, int float)")
		return false
	}
}

func almostEqual(a, b float64) bool {
	return math.Abs(a-b) < NumericalPrecision
}

type TestDataSource struct {
	MysqlDb *gorm.DB
}

// connDB creates a connection to the MySQL instance
func (ds *TestDataSource) ConnDB(conf *config.StorageConf, maxRetries int, retryDelay time.Duration) (err error) {
	for i := 0; i < maxRetries; i++ {
		ds.MysqlDb, err = gorm.Open(mysql.Open(conf.ConnStr), &gorm.Config{})
		if err == nil {
			sqlDB, _ := ds.MysqlDb.DB()
			sqlDB.SetMaxOpenConns(conf.MaxOpenConns)
			sqlDB.SetMaxIdleConns(conf.MaxIdleConns)
			sqlDB.SetConnMaxLifetime(conf.ConnMaxLifetime)
			sqlDB.SetConnMaxIdleTime(conf.ConnMaxIdleTime)
			if err = sqlDB.Ping(); err == nil {
				return nil
			}
		}
		fmt.Printf("Failed to connect to the database: %v (attempt %d/%d)\n", err, i+1, maxRetries)
		time.Sleep(retryDelay)
	}
	return nil
}

// Truncate table
func (ds *TestDataSource) TruncateTable(tableName string) error {
	db, _ := ds.MysqlDb.DB()
	_, err := db.Exec(fmt.Sprintf("delete from %s where 1;", tableName))
	return err
}

func convertDateTimeToSqlFormat(datetime string) (string, error) {
	reg, err := regexp.Compile(`[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}`)
	if err != nil {
		return "", err
	}

	if reg.MatchString(datetime) {
		sql_datetime := reg.FindString(datetime)
		return strings.Replace(sql_datetime, "T", " ", 1), nil
	}

	return datetime, nil
}

func (ds *TestDataSource) getQueryResultFromMySQL(query string, needConvertDateTime bool) (curCaseResult *ResultTable, err error) {
	db, err := ds.MysqlDb.DB()
	if err != nil {
		return
	}
	rows, err := db.Query(query)
	if err != nil {
		return
	}
	columnNames, err := rows.Columns()
	if err != nil {
		return
	}
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return
	}
	if len(columnTypes) != len(columnNames) {
		err = fmt.Errorf("expected equal for len(columnTypes)(%d) == len(columnNames)(%d)", len(columnTypes), len(columnNames))
		return
	}
	curCaseResult = &ResultTable{Column: make([]*ResultColumn, len(columnNames))}
	for i := range curCaseResult.Column {
		curCaseResult.Column[i] = &ResultColumn{Name: columnNames[i]}
	}

	curRowColumns := make([]interface{}, len(columnNames))
	for i, col := range columnTypes {
		switch col.DatabaseTypeName() {
		case "INT", "BIGINT", "MEDIUMINT":
			var a sql.NullInt64
			curRowColumns[i] = &a
		case "VARCHAR", "TEXT", "NVARCHAR", "LONGTEXT", "DATETIME", "DURATION", "TIMESTAMP", "DATE":
			var a sql.NullString
			curRowColumns[i] = &a
		case "BOOL":
			var a sql.NullBool
			curRowColumns[i] = &a
		case "DECIMAL", "DOUBLE", "FLOAT":
			var a sql.NullFloat64
			curRowColumns[i] = &a
		default:
			return nil, fmt.Errorf("not supported database type:%v", col.DatabaseTypeName())
		}
	}
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(curRowColumns...)
		if err != nil {
			return
		}
		for colIndex, curCol := range curRowColumns {
			switch x := curCol.(type) {
			case *sql.NullString:
				var data = ""
				if x.Valid {
					data = x.String
				}
				if needConvertDateTime {
					// format datatime str from mysql has different pattern between go gorm and c++ Poco
					// go format: yyyy-MM-DDThh:mm::ss+TZ, c++: yyyy-MM-DD hh:mm::ss
					// for comparison, here need to convert
					data, err = convertDateTimeToSqlFormat(data)
					if err != nil {
						return
					}
				}
				curCaseResult.Column[colIndex].Ss = append(curCaseResult.Column[colIndex].Ss, data)
			case *sql.NullInt64:
				var data int64 = 0
				if x.Valid {
					data = x.Int64
				}
				curCaseResult.Column[colIndex].Int64s = append(curCaseResult.Column[colIndex].Int64s, data)
			case *sql.NullBool:
				var data = false
				if x.Valid {
					data = x.Bool
				}
				curCaseResult.Column[colIndex].Bools = append(curCaseResult.Column[colIndex].Bools, data)
			case *sql.NullFloat64:
				var data = 0.0
				if x.Valid {
					data = x.Float64
				}
				curCaseResult.Column[colIndex].Doubles = append(curCaseResult.Column[colIndex].Doubles, data)
			default:
				return nil, fmt.Errorf("unknown type:%T", x)
			}
		}
	}
	return curCaseResult, err
}

func CheckResult(testDataSource TestDataSource, expected *ResultTable, answer []*scql.Tensor, mysqlQueryString, errInfo string) (err error) {
	if expected == nil {
		expected, err = testDataSource.getQueryResultFromMySQL(mysqlQueryString, true)
		if err != nil {
			return fmt.Errorf("%s Error Info (%s)", errInfo, err)
		}
	}

	if expected == nil {
		return fmt.Errorf("%s Error Info (expected is nil)", errInfo)
	}
	if answer == nil {
		return fmt.Errorf("%s Error Info (answer is nil)", errInfo)
	}
	actualTable, err := convertTensorToResultTable(answer)
	if err != nil {
		return fmt.Errorf("%s Error Info (%s)", errInfo, err)
	}
	err = compareResultTableWithoutRowOrder(expected, actualTable)
	if err != nil {
		return fmt.Errorf("%s Error Info (%s)", errInfo, err)
	}
	return
}

func convertTensorToResultTable(ts []*scql.Tensor) (*ResultTable, error) {
	rt := &ResultTable{Column: make([]*ResultColumn, len(ts))}
	for i, t := range ts {
		rc := &ResultColumn{Name: t.GetName()}
		switch t.ElemType {
		case scql.PrimitiveDataType_BOOL:
			rc.Bools = t.GetBoolData()
		case scql.PrimitiveDataType_INT64:
			rc.Int64s = t.GetInt64Data()
		case scql.PrimitiveDataType_TIMESTAMP:
			// scql timestamp type is int64, for comparasion with mysql, convert to string
			format_timestamp := make([]string, len(t.GetInt64Data()))
			for i, v := range t.GetInt64Data() {
				ts := time.Unix(int64(v), 0).UTC()
				ts_str := ts.Format("2006-01-02 15:04:05")
				format_timestamp[i] = ts_str
			}
			rc.Ss = format_timestamp
		case scql.PrimitiveDataType_STRING, scql.PrimitiveDataType_DATETIME:
			rc.Ss = t.GetStringData()
		case scql.PrimitiveDataType_FLOAT32:
			dst := make([]float64, len(t.GetFloatData()))
			for i, v := range t.GetFloatData() {
				dst[i] = float64(v)
			}
			rc.Doubles = dst
		case scql.PrimitiveDataType_FLOAT64:
			rc.Doubles = t.GetDoubleData()
		default:
			return nil, fmt.Errorf("unsupported tensor type %v", t.ElemType)
		}
		rt.Column[i] = rc
	}
	return rt, nil
}

func compareResultTableWithoutRowOrder(expect, actual *ResultTable) error {
	if len(expect.Column) != len(actual.Column) {
		return fmt.Errorf("len(expect.Column) %v != len(actual.Column)) %v", len(expect.Column), len(actual.Column))
	}
	for i := range expect.Column {
		if expect.Column[i].Name != actual.Column[i].Name {
			return fmt.Errorf("i:%v th column name '%s' != '%s'", i, expect.Column[i].Name, actual.Column[i].Name)
		}
	}

	columnPrecedence := getColumnPrecedence(expect, actual)
	expectedIndexes := expect.getOrderedIndexes(columnPrecedence)
	actualIndexes := actual.getOrderedIndexes(columnPrecedence)

	for _, col := range expect.Column {
		col.changeOrders(expectedIndexes)
	}
	for _, col := range actual.Column {
		col.changeOrders(actualIndexes)
	}

	if !expect.equalTo(actual) {
		return fmt.Errorf("expected:\n%v\nactual:\n%v", expect.convertToRows(), actual.convertToRows())
	}
	return nil
}
