// Copyright 2026 Ant Group Co., Ltd.
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

package graph

import (
	"fmt"

	"github.com/secretflow/scql/pkg/parser/mysql"
	"github.com/secretflow/scql/pkg/parser/types"
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
)

type DataType struct {
	DType proto.PrimitiveDataType
	Scale int32
	Width int32
}

func NewPrimitiveDataType(dtype proto.PrimitiveDataType) *DataType {
	return &DataType{
		DType: dtype,
		Scale: 0,
		Width: 0,
	}
}
func NewDataType(dtype proto.PrimitiveDataType, width, scale int32) *DataType {
	return &DataType{
		DType: dtype,
		Scale: scale,
		Width: width,
	}
}

func (dType *DataType) Equal(otherType *DataType) bool {
	return dType.DType == otherType.DType && dType.Scale == otherType.Scale && dType.Width == otherType.Width
}

func (dType *DataType) String() string {
	if dType.DType == proto.PrimitiveDataType_DECIMAL {
		return fmt.Sprintf("%s(%d,%d)", proto.PrimitiveDataType_name[int32(dType.DType)], dType.Width, dType.Scale)
	}
	return proto.PrimitiveDataType_name[int32(dType.DType)]
}

func (dType *DataType) Clone() *DataType {
	return &DataType{
		DType: dType.DType,
		Scale: dType.Scale,
		Width: dType.Width,
	}
}

// for now, support data type: bool/int/float/double/string
func ConvertDataType(typ *types.FieldType) (*DataType, error) {
	switch typ.Tp {
	case mysql.TypeLonglong:
		if mysql.HasIsBooleanFlag(typ.Flag) {
			return NewPrimitiveDataType(proto.PrimitiveDataType_BOOL), nil
		}
		return NewPrimitiveDataType(proto.PrimitiveDataType_INT64), nil
	case mysql.TypeLong, mysql.TypeDuration:
		return NewPrimitiveDataType(proto.PrimitiveDataType_INT64), nil
	case mysql.TypeString, mysql.TypeVarchar, mysql.TypeVarString:
		return NewPrimitiveDataType(proto.PrimitiveDataType_STRING), nil
	case mysql.TypeTiny:
		return NewPrimitiveDataType(proto.PrimitiveDataType_BOOL), nil
	case mysql.TypeFloat:
		return NewPrimitiveDataType(proto.PrimitiveDataType_FLOAT32), nil
	case mysql.TypeDouble, mysql.TypeNewDecimal:
		return NewPrimitiveDataType(proto.PrimitiveDataType_FLOAT64), nil
	case mysql.TypeDatetime, mysql.TypeDate, mysql.TypeYear:
		return NewPrimitiveDataType(proto.PrimitiveDataType_DATETIME), nil
	case mysql.TypeTimestamp:
		return NewPrimitiveDataType(proto.PrimitiveDataType_TIMESTAMP), nil
	}
	return NewPrimitiveDataType(proto.PrimitiveDataType_PrimitiveDataType_UNDEFINED), fmt.Errorf("ConvertDataType doesn't support type %v", typ.Tp)
}

var numericTypes = map[proto.PrimitiveDataType]bool{
	proto.PrimitiveDataType_INT8:    true,
	proto.PrimitiveDataType_INT16:   true,
	proto.PrimitiveDataType_INT32:   true,
	proto.PrimitiveDataType_INT64:   true,
	proto.PrimitiveDataType_FLOAT32: true,
	proto.PrimitiveDataType_FLOAT64: true,
	proto.PrimitiveDataType_DECIMAL: true,
}
var integerTypes = map[proto.PrimitiveDataType]bool{
	proto.PrimitiveDataType_INT8:  true,
	proto.PrimitiveDataType_INT16: true,
	proto.PrimitiveDataType_INT32: true,
	proto.PrimitiveDataType_INT64: true,
}
var dataTypeIndexMap = map[proto.PrimitiveDataType]int{
	proto.PrimitiveDataType_INT8:    0,
	proto.PrimitiveDataType_INT16:   1,
	proto.PrimitiveDataType_INT32:   2,
	proto.PrimitiveDataType_INT64:   3,
	proto.PrimitiveDataType_FLOAT32: 4,
	proto.PrimitiveDataType_FLOAT64: 5,
}

func (dType *DataType) IsStringType() bool {
	return dType.DType == proto.PrimitiveDataType_STRING
}

func (dType *DataType) IsNumericType() bool {
	_, ok := numericTypes[dType.DType]
	return ok
}

func (dType *DataType) IsBoolType() bool {
	return dType.DType == proto.PrimitiveDataType_BOOL
}

func (dType *DataType) IsFloatOrDoubleType() bool {
	return dType.DType == proto.PrimitiveDataType_FLOAT32 || dType.DType == proto.PrimitiveDataType_FLOAT64
}

func (dType *DataType) IsIntegerType() bool {
	_, ok := integerTypes[dType.DType]
	return ok
}

func (dType *DataType) IsTimeType() bool {
	return dType.DType == proto.PrimitiveDataType_TIMESTAMP || dType.DType == proto.PrimitiveDataType_DATETIME
}

// GetWiderType returns the wider type between two numeric types
// TODO(xiaoyuan): currently only consider INT and FLOAT types, need to reconsider DECIMAL type later
func GetWiderType(dtype1, dtype2 *DataType) (*DataType, error) {
	if !dtype1.IsNumericType() {
		return nil, fmt.Errorf("%v is not numeric type", dtype1.String())
	}
	if !dtype2.IsNumericType() {
		return nil, fmt.Errorf("%v is not numeric type", dtype2.String())
	}

	// Handle DECIMAL type
	if dtype1.DType == proto.PrimitiveDataType_DECIMAL && dtype2.DType == proto.PrimitiveDataType_DECIMAL {
		// Both are DECIMAL, return the wider one
		width := dtype1.Width
		if dtype2.Width > width {
			width = dtype2.Width
		}
		scale := dtype1.Scale
		if dtype2.Scale > scale {
			scale = dtype2.Scale
		}
		return NewDataType(proto.PrimitiveDataType_DECIMAL, width, scale), nil
	}
	// TODO: fix scale and width handling for mixed DECIMAL and other numeric types
	if dtype1.DType == proto.PrimitiveDataType_DECIMAL {
		// One is DECIMAL, the other is another numeric type
		if dtype2.IsIntegerType() {
			// DECIMAL and integer type, return DECIMAL
			return dtype1.Clone(), nil
		} else if dtype2.IsFloatOrDoubleType() {
			// DECIMAL and float type, return float type (wider)
			return dtype2.Clone(), nil
		}
	}

	if dtype2.DType == proto.PrimitiveDataType_DECIMAL {
		// Same as above, symmetric handling
		if dtype1.IsIntegerType() {
			return dtype2.Clone(), nil
		} else if dtype1.IsFloatOrDoubleType() {
			return dtype1.Clone(), nil
		}
	}

	// Handle integer types
	if dtype1.IsIntegerType() && dtype2.IsIntegerType() {
		if dataTypeIndexMap[dtype1.DType] > dataTypeIndexMap[dtype2.DType] {
			return dtype1.Clone(), nil
		} else {
			return dtype2.Clone(), nil
		}
	}

	// Handle float types
	if dtype1.IsFloatOrDoubleType() && dtype2.IsFloatOrDoubleType() {
		if dataTypeIndexMap[dtype1.DType] > dataTypeIndexMap[dtype2.DType] {
			return dtype1.Clone(), nil
		} else {
			return dtype2.Clone(), nil
		}
	}

	// Handle mixed integer and float types
	if (dtype1.IsIntegerType() && dtype2.IsFloatOrDoubleType()) || (dtype1.IsFloatOrDoubleType() && dtype2.IsIntegerType()) {
		if dtype1.DType == proto.PrimitiveDataType_FLOAT64 || dtype2.DType == proto.PrimitiveDataType_FLOAT64 {
			return NewPrimitiveDataType(proto.PrimitiveDataType_FLOAT64), nil
		}
		if dtype1.DType == proto.PrimitiveDataType_INT64 || dtype2.DType == proto.PrimitiveDataType_INT64 {
			return NewPrimitiveDataType(proto.PrimitiveDataType_FLOAT64), nil
		}
		return NewPrimitiveDataType(proto.PrimitiveDataType_FLOAT32), nil
	}

	return nil, fmt.Errorf("unsupported type combination: %v and %v", dtype1.String(), dtype2.String())
}
