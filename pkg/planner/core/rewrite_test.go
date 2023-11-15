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

package core

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/secretflow/scql/pkg/parser/format"
)

func TestRestoreSQL(t *testing.T) {
	r := require.New(t)
	sql := "select * from dt1.test"
	tables, err := GetSourceTables(sql)
	r.NoError(err)
	r.Equal(1, len(tables))

	sql = "select * from dt1.test join dt2.test"
	tables, err = GetSourceTables(sql)
	r.NoError(err)
	r.Equal(2, len(tables))
	r.Equal("dt1.test", tables[0].String())
	r.Equal("dt2.test", tables[1].String())

	sql = "select * from dt1.test where id in (select id from dt2.test)"
	tables, err = GetSourceTables(sql)
	r.NoError(err)
	r.Equal(2, len(tables))
	r.Equal("dt1.test", tables[0].String())
	r.Equal("dt2.test", tables[1].String())

	m := map[DbTable]DbTable{
		NewDbTable("dt1", "test"): NewDbTable("newdb1", "newtable1"),
		NewDbTable("dt2", "test"): NewDbTable("newdb2", "newtable2"),
	}

	newSql, err := ReplaceTableNameInSQL(sql, format.NewMySQLDialect(), format.DefaultRestoreFlags, m)
	r.NoError(err)
	r.Equal("SELECT * FROM `newdb1`.`newtable1` WHERE `id` IN (SELECT `id` FROM `newdb2`.`newtable2`)", newSql)
}
