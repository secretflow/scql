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

package infoschema_test

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/stretchr/testify/require"

	"github.com/secretflow/scql/pkg/infoschema"
	"github.com/secretflow/scql/pkg/parser/model"
	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/types"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
}

func (testSuite) TestMockInfoSchema(c *C) {
	tblID := int64(1234)
	tblName := model.NewCIStr("tbl_m")
	tableInfo := &model.TableInfo{
		ID:    tblID,
		Name:  tblName,
		State: model.StatePublic,
	}
	colInfo := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    0,
		Name:      model.NewCIStr("h"),
		FieldType: *types.NewFieldType(mysql.TypeLong),
		ID:        1,
	}
	tableInfo.Columns = []*model.ColumnInfo{colInfo}
	is := infoschema.MockInfoSchema(map[string][]*model.TableInfo{"test": {tableInfo}})
	tbl, err := is.TableByName(model.NewCIStr("test"), tblName)
	c.Assert(err, IsNil)
	c.Assert(tbl.Meta().Name, Equals, tblName)
	c.Assert(tbl.Cols()[0].ColumnInfo, Equals, colInfo)
}

func TestFromTableSchema(t *testing.T) {
	tables := []*infoschema.TableSchema{
		{
			DbName:    "d1",
			TableName: "t1",
			Columns: []infoschema.ColumnDesc{
				{Name: "c1", Type: "int", Description: "foo"},
				{Name: "c2", Type: "string", Description: "bar"},
				{Name: "c3", Type: "int", Description: "foobar"},
			},
		},
		{
			DbName:    "d1",
			TableName: "t2",
			Columns: []infoschema.ColumnDesc{
				{Name: "c1", Type: "int", Description: "foo"},
			},
		},
		{
			DbName:    "d2",
			TableName: "t1",
			Columns: []infoschema.ColumnDesc{
				{Name: "c1", Type: "int", Description: "foo"},
			},
		},
	}

	a := require.New(t)
	is, err := infoschema.FromTableSchema(tables)
	a.NoError(err)

	d1, exists := is.SchemaByName(model.NewCIStr("d1"))
	a.True(exists)
	a.Equal(2, len(d1.Tables))
	a.Equal(model.NewCIStr("d1"), d1.Name)
	{
		t1 := d1.Tables[0]
		a.Equal(model.NewCIStr("t1"), t1.Name)
		a.Equal(3, len(t1.Columns))
		a.Equal(model.NewCIStr("c1"), t1.Columns[0].Name)
		a.Equal(mysql.TypeLong, t1.Columns[0].Tp)
		a.Equal("foo", t1.Columns[0].Comment)
		a.Equal(model.NewCIStr("c2"), t1.Columns[1].Name)
		a.Equal(mysql.TypeString, t1.Columns[1].Tp)
		a.Equal("bar", t1.Columns[1].Comment)
		a.Equal(model.NewCIStr("c3"), t1.Columns[2].Name)
		a.Equal(mysql.TypeLong, t1.Columns[2].Tp)
		a.Equal("foobar", t1.Columns[2].Comment)
	}
	{
		t2 := d1.Tables[1]
		a.Equal(model.NewCIStr("t2"), t2.Name)
		a.Equal(1, len(t2.Columns))
		a.Equal(model.NewCIStr("c1"), t2.Columns[0].Name)
		a.Equal(mysql.TypeLong, t2.Columns[0].Tp)
		a.Equal("foo", t2.Columns[0].Comment)
	}

	d2, exists := is.SchemaByName(model.NewCIStr("d2"))
	a.True(exists)
	a.Equal(1, len(d2.Tables))
	a.Equal(model.NewCIStr("d2"), d2.Name)
	{
		t1 := d2.Tables[0]
		a.Equal(model.NewCIStr("t1"), t1.Name)
		a.Equal(1, len(t1.Columns))
		a.Equal(model.NewCIStr("c1"), t1.Columns[0].Name)
		a.Equal(mysql.TypeLong, t1.Columns[0].Tp)
		a.Equal("foo", t1.Columns[0].Comment)
	}
}
