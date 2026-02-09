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
	"fmt"

	"github.com/secretflow/scql/pkg/expression"
	"github.com/secretflow/scql/pkg/expression/aggregation"
	"github.com/secretflow/scql/pkg/interpreter/graph"
	"github.com/secretflow/scql/pkg/parser/ast"
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
)

// simpleCount means Complete Count, except count(distinct), e.g: count(colA), count(*), count(1)...
func isSimpleCount(aggFunc *aggregation.AggFuncDesc) bool {
	return aggFunc.Name == ast.AggFuncCount && aggFunc.Mode == aggregation.CompleteMode && !aggFunc.HasDistinct
}

func extractEQColumns(exp *expression.ScalarFunction) ([]*expression.Column, error) {
	if exp.FuncName.L != ast.EQ || len(exp.GetArgs()) != 2 {
		return nil, fmt.Errorf("extractEQColumns: unsupported eq condition %v", exp)
	}
	left, right := exp.GetArgs()[0], exp.GetArgs()[1]
	leftCol, ok1 := left.(*expression.Column)
	rightCol, ok2 := right.(*expression.Column)
	if !ok1 || !ok2 {
		return nil, fmt.Errorf("extractEQColumns: unsupported eq condition %v", exp)
	}
	return []*expression.Column{leftCol, rightCol}, nil
}

func inferAggOutputType(aggFunc *aggregation.AggFuncDesc, inputType *graph.DataType) *graph.DataType {
	outputType := inputType
	switch aggFunc.Name {
	case ast.AggFuncAvg:
		outputType = graph.NewPrimitiveDataType(proto.PrimitiveDataType_FLOAT64)
	case ast.AggFuncSum:
		if inputType.IsBoolType() {
			outputType = graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64)
		} else if isFloatOrDoubleType(inputType.DType) {
			outputType = graph.NewPrimitiveDataType(proto.PrimitiveDataType_FLOAT64)
		}
	case ast.AggFuncCount:
		outputType = graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64)
	}
	return outputType
}

func isFloatOrDoubleType(tp proto.PrimitiveDataType) bool {
	return tp == proto.PrimitiveDataType_FLOAT32 || tp == proto.PrimitiveDataType_FLOAT64
}
