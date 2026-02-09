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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/secretflow/scql/pkg/interpreter/graph"
	"github.com/secretflow/scql/pkg/parser/mysql"
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/types"
)

func TestConvertDataType(t *testing.T) {
	// Test TypeLonglong with boolean flag
	boolField := &types.FieldType{
		Tp:   mysql.TypeLonglong,
		Flag: mysql.IsBooleanFlag,
	}
	result, err := graph.ConvertDataType(boolField)
	assert.NoError(t, err)
	assert.Equal(t, proto.PrimitiveDataType_BOOL, result.DType)

	// Test TypeLonglong without boolean flag
	int64Field := &types.FieldType{
		Tp: mysql.TypeLonglong,
	}
	result, err = graph.ConvertDataType(int64Field)
	assert.NoError(t, err)
	assert.Equal(t, proto.PrimitiveDataType_INT64, result.DType)

	// Test TypeLong
	longField := &types.FieldType{
		Tp: mysql.TypeLong,
	}
	result, err = graph.ConvertDataType(longField)
	assert.NoError(t, err)
	assert.Equal(t, proto.PrimitiveDataType_INT64, result.DType)

	// Test TypeDuration
	durationField := &types.FieldType{
		Tp: mysql.TypeDuration,
	}
	result, err = graph.ConvertDataType(durationField)
	assert.NoError(t, err)
	assert.Equal(t, proto.PrimitiveDataType_INT64, result.DType)

	// Test TypeString
	stringField := &types.FieldType{
		Tp: mysql.TypeString,
	}
	result, err = graph.ConvertDataType(stringField)
	assert.NoError(t, err)
	assert.Equal(t, proto.PrimitiveDataType_STRING, result.DType)

	// Test TypeVarchar
	varcharField := &types.FieldType{
		Tp: mysql.TypeVarchar,
	}
	result, err = graph.ConvertDataType(varcharField)
	assert.NoError(t, err)
	assert.Equal(t, proto.PrimitiveDataType_STRING, result.DType)

	// Test TypeVarString
	varStringField := &types.FieldType{
		Tp: mysql.TypeVarString,
	}
	result, err = graph.ConvertDataType(varStringField)
	assert.NoError(t, err)
	assert.Equal(t, proto.PrimitiveDataType_STRING, result.DType)

	// Test TypeTiny
	tinyField := &types.FieldType{
		Tp: mysql.TypeTiny,
	}
	result, err = graph.ConvertDataType(tinyField)
	assert.NoError(t, err)
	assert.Equal(t, proto.PrimitiveDataType_BOOL, result.DType)

	// Test TypeFloat
	floatField := &types.FieldType{
		Tp: mysql.TypeFloat,
	}
	result, err = graph.ConvertDataType(floatField)
	assert.NoError(t, err)
	assert.Equal(t, proto.PrimitiveDataType_FLOAT32, result.DType)

	// Test TypeDouble
	doubleField := &types.FieldType{
		Tp: mysql.TypeDouble,
	}
	result, err = graph.ConvertDataType(doubleField)
	assert.NoError(t, err)
	assert.Equal(t, proto.PrimitiveDataType_FLOAT64, result.DType)

	// Test TypeNewDecimal
	decimalField := &types.FieldType{
		Tp: mysql.TypeNewDecimal,
	}
	result, err = graph.ConvertDataType(decimalField)
	assert.NoError(t, err)
	assert.Equal(t, proto.PrimitiveDataType_FLOAT64, result.DType)

	// Test TypeDatetime
	datetimeField := &types.FieldType{
		Tp: mysql.TypeDatetime,
	}
	result, err = graph.ConvertDataType(datetimeField)
	assert.NoError(t, err)
	assert.Equal(t, proto.PrimitiveDataType_DATETIME, result.DType)

	// Test TypeDate
	dateField := &types.FieldType{
		Tp: mysql.TypeDate,
	}
	result, err = graph.ConvertDataType(dateField)
	assert.NoError(t, err)
	assert.Equal(t, proto.PrimitiveDataType_DATETIME, result.DType)

	// Test TypeYear
	yearField := &types.FieldType{
		Tp: mysql.TypeYear,
	}
	result, err = graph.ConvertDataType(yearField)
	assert.NoError(t, err)
	assert.Equal(t, proto.PrimitiveDataType_DATETIME, result.DType)

	// Test TypeTimestamp
	timestampField := &types.FieldType{
		Tp: mysql.TypeTimestamp,
	}
	result, err = graph.ConvertDataType(timestampField)
	assert.NoError(t, err)
	assert.Equal(t, proto.PrimitiveDataType_TIMESTAMP, result.DType)

	// Test unsupported type
	jsonField := &types.FieldType{
		Tp: mysql.TypeJSON,
	}
	result, err = graph.ConvertDataType(jsonField)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ConvertDataType doesn't support type")
	assert.Equal(t, proto.PrimitiveDataType_PrimitiveDataType_UNDEFINED, result.DType)
}
