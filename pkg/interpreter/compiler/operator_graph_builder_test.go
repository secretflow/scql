// Copyright 2025 Ant Group Co., Ltd.
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

package compiler

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/secretflow/scql/pkg/expression"
	"github.com/secretflow/scql/pkg/infoschema"
	"github.com/secretflow/scql/pkg/interpreter/graph"
	"github.com/secretflow/scql/pkg/parser"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/planner/core"
	"github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/types"
	"github.com/secretflow/scql/pkg/util/mock"
	"github.com/secretflow/scql/pkg/util/sliceutil"
)

func ConvertMockEnginesToEnginesInfo(info *mock.MockEnginesInfo) (*graph.EnginesInfo, error) {
	participants := make([]*graph.Participant, 0, len(info.PartyToUrls))
	for code, url := range sliceutil.SortedMap(info.PartyToUrls) {
		participants = append(participants, &graph.Participant{
			PartyCode: code,
			Endpoints: []string{url},
			Token:     info.PartyToCredentials[code],
		})
	}
	partyToTables := make(map[string][]core.DbTable)
	tableToRefs := make(map[core.DbTable]core.DbTable)
	for p, tables := range info.PartyToTables {
		var dbTables []core.DbTable
		for _, t := range tables {
			dt, err := core.NewDbTableFromString(t)
			if err != nil {
				return nil, err
			}
			dbTables = append(dbTables, dt)
		}
		partyToTables[p] = dbTables
	}

	engineInfo := graph.NewEnginesInfo(graph.NewPartyInfo(participants), partyToTables)
	for table, refTable := range info.TableToRefs {
		ref, err := core.NewDbTableFromString(refTable)
		if err != nil {
			return nil, err
		}
		tbl, err := core.NewDbTableFromString(table)
		if err != nil {
			return nil, err
		}
		tableToRefs[tbl] = ref
	}
	engineInfo.UpdateTableToRefs(tableToRefs)
	return engineInfo, nil
}

type QueryCase struct {
	Name  string `json:"name"`
	Query string `json:"query"`
}

func TestBuilder(t *testing.T) {
	r := require.New(t)

	file, err := os.Open("data/test_queries.json")
	r.NoError(err)
	defer file.Close()

	byteValue, err := io.ReadAll(file)
	r.NoError(err)
	var queryCases []QueryCase
	err = json.Unmarshal(byteValue, &queryCases)
	r.NoError(err)

	mockTables, err := mock.MockAllTables()
	r.NoError(err)
	is := infoschema.MockInfoSchema(mockTables)
	parser := parser.New()
	ctx := mock.MockContext()
	mockEngines, err := mock.MockEngines()
	r.NoError(err)
	info, err := ConvertMockEnginesToEnginesInfo(mockEngines)
	r.NoError(err)

	for _, queryCase := range queryCases {
		fmt.Println("query: ", queryCase.Name)
		stmt, err := parser.ParseOneStmt(queryCase.Query, "", "")
		r.NoError(err)
		err = core.Preprocess(ctx, stmt, is)
		r.NoError(err)
		lp, _, err := core.BuildLogicalPlanWithOptimization(context.Background(), ctx, stmt, is)
		r.NoError(err)

		tensorMetaManager := NewTensorMetaManager()
		builder := NewOperatorGraphBuilder(tensorMetaManager, info, "alice", time.Now())
		_, err = builder.Build(lp)
		r.NoError(err)
	}
}

func TestCheckFuncInputsType(t *testing.T) {
	r := require.New(t)

	newTensorMeta := func(dtype *graph.DataType) *TensorMeta {
		return &TensorMeta{DType: dtype}
	}

	testCases := []struct {
		name     string
		funcName string
		inputs   []*TensorMeta
		wantErr  bool
		errMsg   string
	}{
		// Boolean functions - valid cases
		{"UnaryNot valid", ast.UnaryNot, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_BOOL))}, false, ""},
		{"LogicOr valid", ast.LogicOr, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_BOOL)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_BOOL))}, false, ""},

		// Boolean functions - invalid cases
		{"UnaryNot invalid type", ast.UnaryNot, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64))}, true, "expect bool input"},
		{"LogicAnd invalid type", ast.LogicAnd, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_STRING))}, true, "expect bool input"},

		// String functions - valid cases
		{"Lower valid", ast.Lower, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_STRING))}, false, ""},
		{"Upper valid", ast.Upper, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_STRING))}, false, ""},

		// String functions - invalid cases
		{"Trim invalid type", ast.Trim, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64))}, true, "expect string input"},

		// Numeric functions - valid cases
		{"Sin valid int", ast.Sin, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64))}, false, ""},
		{"Cos valid float", ast.Cos, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_FLOAT64))}, false, ""},
		{"Abs valid", ast.Abs, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_FLOAT32))}, false, ""},

		// Numeric functions - invalid cases
		{"Sin invalid string", ast.Sin, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_STRING))}, true, "expect numeric input"},
		{"Tan invalid bool", ast.Tan, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_BOOL))}, true, "expect numeric input"},

		// Comparison/arithmetic functions - valid cases
		{"LT valid int", ast.LT, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64))}, false, ""},
		{"Plus valid float", ast.Plus, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_FLOAT64)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_FLOAT64))}, false, ""},
		{"GE valid bool", ast.GE, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_BOOL)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_BOOL))}, false, ""},

		// Comparison/arithmetic functions - invalid cases
		{"LT invalid string", ast.LT, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_STRING))}, true, "got string input"},
		{"Minus invalid string", ast.Minus, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_STRING))}, true, "got string input"},

		// IntDiv/Mod - valid cases
		{"IntDiv valid int", ast.IntDiv, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64))}, false, ""},
		{"Mod valid datetime", ast.Mod, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_DATETIME)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64))}, false, ""},
		{"IntDiv valid timestamp", ast.IntDiv, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_TIMESTAMP)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64))}, false, ""},

		// IntDiv/Mod - invalid cases
		{"IntDiv invalid left", ast.IntDiv, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_STRING)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64))}, true, "requires both left and right operands be int64-like"},
		{"Mod invalid right", ast.Mod, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_STRING))}, true, "requires both left and right operands be int64-like"},

		// Case/If - valid cases
		{"Case valid", ast.Case, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_BOOL)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_BOOL)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64))}, false, ""},
		{"If valid", ast.If, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_BOOL)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_FLOAT64)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_FLOAT64))}, false, ""},

		// Case/If - invalid cases
		{"Case invalid condition string", ast.Case, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_STRING)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64))}, true, "does not support string condition"},
		{"If inconsistent types", ast.If, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_BOOL)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_FLOAT64))}, true, "all values must have the same type"},

		// Ifnull - valid cases
		{"Ifnull valid same type", ast.Ifnull, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64))}, false, ""},
		{"Ifnull valid string", ast.Ifnull, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_STRING)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_STRING))}, false, ""},

		// Ifnull - invalid cases
		{"Ifnull different types", ast.Ifnull, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_FLOAT64))}, true, "both arguments must have the same type"},

		// Unknown function - should pass
		{"Unknown function", "unknown_func", []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_STRING))}, false, ""},
	}

	// Run all test cases
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkFuncInputsType(tc.funcName, tc.inputs)
			if tc.wantErr {
				r.Error(err)
				r.Contains(err.Error(), tc.errMsg)
			} else {
				r.NoError(err)
			}
		})
	}
}

func TestGetOutputType(t *testing.T) {
	r := require.New(t)

	newTensorMeta := func(dtype *graph.DataType) *TensorMeta {
		return &TensorMeta{DType: dtype}
	}

	testCases := []struct {
		name     string
		funcName string
		inputs   []*TensorMeta
		want     *graph.DataType
		wantErr  bool
		errMsg   string
	}{
		// Boolean output functions
		{"UnaryNot", ast.UnaryNot, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64))}, graph.NewPrimitiveDataType(scql.PrimitiveDataType_BOOL), false, ""},
		{"IsNull", ast.IsNull, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_STRING))}, graph.NewPrimitiveDataType(scql.PrimitiveDataType_BOOL), false, ""},
		{"LT", ast.LT, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64))}, graph.NewPrimitiveDataType(scql.PrimitiveDataType_BOOL), false, ""},
		{"LogicAnd", ast.LogicAnd, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_BOOL)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_BOOL))}, graph.NewPrimitiveDataType(scql.PrimitiveDataType_BOOL), false, ""},

		// String output functions
		{"Lower", ast.Lower, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_STRING))}, graph.NewPrimitiveDataType(scql.PrimitiveDataType_STRING), false, ""},
		{"Upper", ast.Upper, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_STRING))}, graph.NewPrimitiveDataType(scql.PrimitiveDataType_STRING), false, ""},
		{"Concat", ast.Concat, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_STRING)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_STRING))}, graph.NewPrimitiveDataType(scql.PrimitiveDataType_STRING), false, ""},

		// Greatest/Least - numeric
		{"Greatest int", ast.Greatest, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64))}, graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64), false, ""},
		{"Greatest float32", ast.Greatest, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_FLOAT32)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_FLOAT32))}, graph.NewPrimitiveDataType(scql.PrimitiveDataType_FLOAT32), false, ""},
		{"Greatest mixed numeric", ast.Greatest, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_FLOAT32))}, graph.NewPrimitiveDataType(scql.PrimitiveDataType_FLOAT64), false, ""},
		{"Greatest timestamp", ast.Greatest, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_TIMESTAMP)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_TIMESTAMP))}, graph.NewPrimitiveDataType(scql.PrimitiveDataType_TIMESTAMP), false, ""},

		// Greatest/Least - invalid cases
		{"Greatest mixed timestamp_numeric", ast.Greatest, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_TIMESTAMP)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64))}, graph.NewPrimitiveDataType(scql.PrimitiveDataType_PrimitiveDataType_UNDEFINED), true, "got wrong type"},
		{"Least mixed numeric_timestamp", ast.Least, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_FLOAT64)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_DATETIME))}, graph.NewPrimitiveDataType(scql.PrimitiveDataType_PrimitiveDataType_UNDEFINED), true, "got wrong type"},
		{"Least mixed timestamp_numeric", ast.Least, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_TIMESTAMP)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64))}, graph.NewPrimitiveDataType(scql.PrimitiveDataType_PrimitiveDataType_UNDEFINED), true, "got wrong type"},
		// Case/If functions
		{"Case", ast.Case, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_BOOL)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_BOOL)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64))}, graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64), false, ""},
		{"If", ast.If, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_BOOL)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_FLOAT64)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_FLOAT64))}, graph.NewPrimitiveDataType(scql.PrimitiveDataType_FLOAT64), false, ""},

		// Type preservation functions
		{"Ifnull int", ast.Ifnull, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64))}, graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64), false, ""},
		{"Ifnull string", ast.Ifnull, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_STRING)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_STRING))}, graph.NewPrimitiveDataType(scql.PrimitiveDataType_STRING), false, ""},
		{"Abs", ast.Abs, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_FLOAT64))}, graph.NewPrimitiveDataType(scql.PrimitiveDataType_FLOAT64), false, ""},
		{"Coalesce int", ast.Coalesce, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64))}, graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64), false, ""},
		{"Coalesce float32", ast.Coalesce, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_FLOAT32)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_FLOAT32))}, graph.NewPrimitiveDataType(scql.PrimitiveDataType_FLOAT64), false, ""},
		// Float64 output functions
		{"Sin", ast.Sin, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64))}, graph.NewPrimitiveDataType(scql.PrimitiveDataType_FLOAT64), false, ""},
		{"Cos", ast.Cos, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_FLOAT32))}, graph.NewPrimitiveDataType(scql.PrimitiveDataType_FLOAT64), false, ""},
		{"Div", ast.Div, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64))}, graph.NewPrimitiveDataType(scql.PrimitiveDataType_FLOAT64), false, ""},
		{"Atan2", ast.Atan2, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_FLOAT32)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_FLOAT32))}, graph.NewPrimitiveDataType(scql.PrimitiveDataType_FLOAT64), false, ""},

		// Int64 output functions
		{"Ceil", ast.Ceil, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_FLOAT64))}, graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64), false, ""},
		{"Floor", ast.Floor, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_FLOAT32))}, graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64), false, ""},
		{"IntDiv", ast.IntDiv, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64))}, graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64), false, ""},
		{"Mod", ast.Mod, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64))}, graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64), false, ""},
		{"DateDiff", ast.DateDiff, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_DATETIME)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_DATETIME))}, graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64), false, ""},

		// Plus/Minus with datetime
		{"Plus int", ast.Plus, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64))}, graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64), false, ""},
		{"Minus int", ast.Minus, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64))}, graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64), false, ""},
		{"Minus datetime", ast.Minus, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_DATETIME)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_DATETIME))}, graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64), false, ""},
		{"Minus timestamp", ast.Minus, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_TIMESTAMP)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_TIMESTAMP))}, graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64), false, ""},

		// Mul/Pow
		{"Mul int", ast.Mul, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64))}, graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64), false, ""},
		{"Pow float32", ast.Pow, []*TensorMeta{newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_FLOAT32)), newTensorMeta(graph.NewPrimitiveDataType(scql.PrimitiveDataType_FLOAT32))}, graph.NewPrimitiveDataType(scql.PrimitiveDataType_FLOAT64), false, ""},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := getOutputType(tc.funcName, tc.inputs)
			if tc.wantErr {
				r.Error(err)
				r.Contains(err.Error(), tc.errMsg)
			} else {
				r.NoError(err)
				r.True(tc.want.Equal(got))
			}
		})
	}
}

func TestCheckArgsNum(t *testing.T) {
	r := require.New(t)

	newTensorMeta := func() *TensorMeta {
		return &TensorMeta{DType: graph.NewPrimitiveDataType(scql.PrimitiveDataType_INT64)}
	}

	newConst := func(value interface{}) *expression.Constant {
		return &expression.Constant{Value: types.NewDatum(value)}
	}

	testCases := []struct {
		name        string
		funcName    string
		inputs      []*TensorMeta
		constParams []*expression.Constant
		wantErr     bool
		errMsg      string
	}{
		// Invalid inputs number
		{"no inputs", "sin", []*TensorMeta{}, []*expression.Constant{}, true, "expects at least 1 input tensor"},
		// Unsupported function
		{"unsupported function", "unknown_func", []*TensorMeta{newTensorMeta()}, []*expression.Constant{}, true, "unsupported function"},

		// Fixed argument count functions
		{"sin valid", "sin", []*TensorMeta{newTensorMeta()}, []*expression.Constant{}, false, ""},
		{"sin invalid count", "sin", []*TensorMeta{newTensorMeta(), newTensorMeta()}, []*expression.Constant{}, true, "expects 1 arguments"},
		{"cos valid", "cos", []*TensorMeta{newTensorMeta()}, []*expression.Constant{}, false, ""},
		{"abs valid", "abs", []*TensorMeta{newTensorMeta()}, []*expression.Constant{}, false, ""},

		// Two argument functions
		{"plus valid", "plus", []*TensorMeta{newTensorMeta(), newTensorMeta()}, []*expression.Constant{}, false, ""},
		{"plus invalid", "plus", []*TensorMeta{newTensorMeta()}, []*expression.Constant{}, true, "expects 2 arguments"},
		{"minus valid", "minus", []*TensorMeta{newTensorMeta(), newTensorMeta()}, []*expression.Constant{}, false, ""},
		{"lt valid", "lt", []*TensorMeta{newTensorMeta(), newTensorMeta()}, []*expression.Constant{}, false, ""},

		// Three argument functions
		{"case valid", "case", []*TensorMeta{newTensorMeta(), newTensorMeta(), newTensorMeta()}, []*expression.Constant{}, false, ""},
		{"case invalid even", "case", []*TensorMeta{newTensorMeta(), newTensorMeta()}, []*expression.Constant{}, true, "expects odd number of arguments"},
		{"case valid 5 args", "case", []*TensorMeta{newTensorMeta(), newTensorMeta(), newTensorMeta(), newTensorMeta(), newTensorMeta()}, []*expression.Constant{}, false, ""},

		// Substr/Substring special cases
		{"substr valid 2 args", "substr", []*TensorMeta{newTensorMeta()}, []*expression.Constant{newConst(1)}, false, ""},
		{"substr valid 3 args", "substr", []*TensorMeta{newTensorMeta()}, []*expression.Constant{newConst(1), newConst(5)}, false, ""},
		{"substr invalid tensor count", "substr", []*TensorMeta{newTensorMeta(), newTensorMeta()}, []*expression.Constant{newConst(1)}, true, "substr expects 1 arguments, but got 2"},
		{"substr invalid const count", "substr", []*TensorMeta{newTensorMeta()}, []*expression.Constant{newConst(1), newConst(5), newConst(10)}, true, "expects 1 or 2 constant parameters"},
		{"substring valid", "substring", []*TensorMeta{newTensorMeta()}, []*expression.Constant{newConst(1), newConst(5)}, false, ""},

		// GeoDist special cases
		{"geodist valid 4 args", "geodist", []*TensorMeta{newTensorMeta(), newTensorMeta(), newTensorMeta(), newTensorMeta()}, []*expression.Constant{}, false, ""},
		{"geodist valid 5 args", "geodist", []*TensorMeta{newTensorMeta(), newTensorMeta(), newTensorMeta(), newTensorMeta(), newTensorMeta()}, []*expression.Constant{}, false, ""},
		{"geodist invalid 3 args", "geodist", []*TensorMeta{newTensorMeta(), newTensorMeta(), newTensorMeta()}, []*expression.Constant{}, true, "expects 4 or 5 arguments"},
		{"geodist invalid 6 args", "geodist", []*TensorMeta{newTensorMeta(), newTensorMeta(), newTensorMeta(), newTensorMeta(), newTensorMeta(), newTensorMeta()}, []*expression.Constant{}, true, "expects 4 or 5 arguments"},

		// Variadic functions
		{"greatest valid 2 args", "greatest", []*TensorMeta{newTensorMeta(), newTensorMeta()}, []*expression.Constant{}, false, ""},
		{"greatest valid 3 args", "greatest", []*TensorMeta{newTensorMeta(), newTensorMeta(), newTensorMeta()}, []*expression.Constant{}, false, ""},
		{"least valid 2 args", "least", []*TensorMeta{newTensorMeta(), newTensorMeta()}, []*expression.Constant{}, false, ""},
		{"coalesce valid 2 args", "coalesce", []*TensorMeta{newTensorMeta(), newTensorMeta()}, []*expression.Constant{}, false, ""},
		{"concat valid 2 args", "concat", []*TensorMeta{newTensorMeta(), newTensorMeta()}, []*expression.Constant{}, false, ""},
		{"concat valid 3 args", "concat", []*TensorMeta{newTensorMeta(), newTensorMeta(), newTensorMeta()}, []*expression.Constant{}, false, ""},

		// Single argument functions
		{"ceil valid", "ceil", []*TensorMeta{newTensorMeta()}, []*expression.Constant{}, false, ""},
		{"floor valid", "floor", []*TensorMeta{newTensorMeta()}, []*expression.Constant{}, false, ""},
		{"exp valid", "exp", []*TensorMeta{newTensorMeta()}, []*expression.Constant{}, false, ""},
		{"ln valid", "ln", []*TensorMeta{newTensorMeta()}, []*expression.Constant{}, false, ""},

		// Two argument functions with different counts
		{"atan2 valid", "atan2", []*TensorMeta{newTensorMeta(), newTensorMeta()}, []*expression.Constant{}, false, ""},
		{"pow valid", "pow", []*TensorMeta{newTensorMeta(), newTensorMeta()}, []*expression.Constant{}, false, ""},
		{"mod valid", "mod", []*TensorMeta{newTensorMeta(), newTensorMeta()}, []*expression.Constant{}, false, ""},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkArgsNum(tc.funcName, tc.inputs, tc.constParams)
			if tc.wantErr {
				r.Error(err)
				r.Contains(err.Error(), tc.errMsg)
			} else {
				r.NoError(err)
			}
		})
	}
}
