// Copyright 2024 Ant Group Co., Ltd.
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

package typeutil

import (
	"fmt"

	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
)

var numericTypes = map[pb.PrimitiveDataType]bool{
	pb.PrimitiveDataType_INT8:    true,
	pb.PrimitiveDataType_INT16:   true,
	pb.PrimitiveDataType_INT32:   true,
	pb.PrimitiveDataType_INT64:   true,
	pb.PrimitiveDataType_FLOAT32: true,
	pb.PrimitiveDataType_FLOAT64: true,
}

var integerTypes = map[pb.PrimitiveDataType]bool{
	pb.PrimitiveDataType_INT8:  true,
	pb.PrimitiveDataType_INT16: true,
	pb.PrimitiveDataType_INT32: true,
	pb.PrimitiveDataType_INT64: true,
}

var floatTypes = map[pb.PrimitiveDataType]bool{
	pb.PrimitiveDataType_FLOAT32: true,
	pb.PrimitiveDataType_FLOAT64: true,
}

var dataTypeIndexMap = map[pb.PrimitiveDataType]int{
	pb.PrimitiveDataType_INT8:  0,
	pb.PrimitiveDataType_INT16: 1,
	pb.PrimitiveDataType_INT32: 2,
	pb.PrimitiveDataType_INT64: 3,

	pb.PrimitiveDataType_FLOAT32: 4,
	pb.PrimitiveDataType_FLOAT64: 5,
}

func IsNumericDtype(dtype pb.PrimitiveDataType) bool {
	_, ok := numericTypes[dtype]
	return ok
}

func IsFloatType(dtype pb.PrimitiveDataType) bool {
	_, ok := floatTypes[dtype]
	return ok
}

func IsIntegerType(dtype pb.PrimitiveDataType) bool {
	_, ok := integerTypes[dtype]
	return ok
}

func IsTimeType(dtype pb.PrimitiveDataType) bool {
	return dtype == pb.PrimitiveDataType_TIMESTAMP || dtype == pb.PrimitiveDataType_DATETIME
}

func GetWiderType(dtype1, dtype2 pb.PrimitiveDataType) (pb.PrimitiveDataType, error) {
	if !IsNumericDtype(dtype1) {
		return 0, fmt.Errorf("%v is not numeric type", dtype1.String())
	}
	if !IsNumericDtype(dtype2) {
		return 0, fmt.Errorf("%v is not numeric type", dtype2.String())
	}

	if IsIntegerType(dtype1) && IsIntegerType(dtype2) {
		if dataTypeIndexMap[dtype1] > dataTypeIndexMap[dtype2] {
			return dtype1, nil
		} else {
			return dtype2, nil
		}
	}

	if IsFloatType(dtype1) && IsFloatType(dtype2) {
		if dataTypeIndexMap[dtype1] > dataTypeIndexMap[dtype2] {
			return dtype1, nil
		} else {
			return dtype2, nil
		}
	}

	// one is integer, another is float
	if dtype1 == pb.PrimitiveDataType_FLOAT64 || dtype2 == pb.PrimitiveDataType_FLOAT64 {
		return pb.PrimitiveDataType_FLOAT64, nil
	}

	if dtype1 == pb.PrimitiveDataType_INT64 || dtype2 == pb.PrimitiveDataType_INT64 {
		return pb.PrimitiveDataType_FLOAT64, nil
	} else {
		return pb.PrimitiveDataType_FLOAT32, nil
	}
}
