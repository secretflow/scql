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

	"github.com/sirupsen/logrus"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	"github.com/secretflow/scql/pkg/scdb/server"
)

// NumericalPrecision is based on experiment
const NumericalPrecision float64 = 1e-1

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
	Name   string    `json:"name"`
	Ss     []string  `json:"string"`
	Int64s []int64   `json:"int"`
	Bools  []bool    `json:"bool"`
	Floats []float32 `json:"float"`
}

type StringWithId struct {
	str string
	id  int
}

type StringWithIds []StringWithId

func (x StringWithIds) Len() int           { return len(x) }
func (x StringWithIds) Less(i, j int) bool { return x[i].str < x[j].str }
func (x StringWithIds) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }
func (x StringWithIds) getOrders() []int {
	var res []int
	for i := 0; i < x.Len(); i++ {
		res = append(res, x[i].id)
	}
	return res
}

func (t *ResultTable) ConvertToRows() StringWithIds {
	rowStrings := t.Column[0].toStringSlice()
	for _, c := range t.Column[1:] {
		for i, s := range c.toStringSlice() {
			rowStrings[i] = rowStrings[i] + s
		}
	}
	var res []StringWithId
	for i, s := range rowStrings {
		res = append(res, StringWithId{
			str: s,
			id:  i,
		})
	}
	return res
}

func IsColumnNil(col *ResultColumn) bool {
	if len(col.Bools) == 0 && len(col.Floats) == 0 && len(col.Int64s) == 0 && len(col.Ss) == 0 {
		return true
	}
	return false
}

func (t *ResultTable) EqualTo(o *ResultTable) bool {
	if len(t.Column) != len(o.Column) {
		return false
	}
	for i, col := range t.Column {
		// if both of datas are nil return true
		if IsColumnNil(col) && IsColumnNil(o.Column[i]) {
			return true
		}
		if !col.EqualTo(o.Column[i]) {
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
	if c.Floats != nil {
		newFloats := make([]float32, len(c.Floats))
		for i, order := range orders {
			newFloats[i] = c.Floats[order]
		}
		c.Floats = newFloats
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
	} else if c.Floats != nil {
		for _, f := range c.Floats {
			// NOTE(@yang.y): SCQL handles float differently from MySQL
			res = append(res, fmt.Sprintf(`"%-.4f"f`, f))
		}
		return res
	} else {
		for _, i := range c.Int64s {
			res = append(res, fmt.Sprintf(`"%d"i`, i))
		}
		return res
	}
}

func (c *ResultColumn) EqualTo(o *ResultColumn) bool {
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
	} else if c.Floats != nil || o.Floats != nil {
		if c.Floats != nil && o.Floats != nil {
			for i, d := range c.Floats {
				if !AlmostEqual(d, o.Floats[i]) {
					logrus.Infof("line number %d floats error float32(%f) != float32(%f)", i, d, o.Floats[i])
					return false
				}
			}
			return true
		}

		if o.Int64s != nil {
			for i, d := range c.Floats {
				if !AlmostEqual(d, float32(o.Int64s[i])) {
					logrus.Infof("line number %d floats error float32(%f) != int(%d)", i, d, o.Int64s[i])
					return false
				}
			}
			return true
		}

		if c.Int64s != nil {
			for i, d := range o.Floats {
				if !AlmostEqual(d, float32(c.Int64s[i])) {
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

func AlmostEqual(a, b float32) bool {
	return math.Abs(float64(a-b)) < NumericalPrecision
}

type TestDataSource struct {
	MysqlDb *gorm.DB
}

// connDB creates a connection to the MySQL instance
func (ds *TestDataSource) ConnDB(conf *server.StorageConf) (err error) {
	ds.MysqlDb, err = gorm.Open(mysql.Open(conf.ConnStr), &gorm.Config{})
	if err != nil {
		return err
	}
	sqlDB, _ := ds.MysqlDb.DB()
	sqlDB.SetMaxOpenConns(conf.MaxOpenConns)
	sqlDB.SetMaxIdleConns(conf.MaxIdleConns)
	sqlDB.SetConnMaxLifetime(conf.ConnMaxLifetime)
	sqlDB.SetConnMaxIdleTime(conf.ConnMaxIdleTime)
	if err = sqlDB.Ping(); err != nil {
		return err
	}
	return nil
}

func (ds *TestDataSource) GetQueryResultFromMySQL(query string) (curCaseResult *ResultTable, err error) {
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
		case "VARCHAR", "TEXT", "NVARCHAR", "LONGTEXT", "DATETIME", "DURATION", "TIMESTAMP":
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
				curCaseResult.Column[colIndex].Floats = append(curCaseResult.Column[colIndex].Floats, float32(data))
			default:
				return nil, fmt.Errorf("unknown type:%T", x)
			}
		}
	}
	return curCaseResult, err
}
