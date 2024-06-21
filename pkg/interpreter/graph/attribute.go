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

package graph

import (
	"fmt"
	"strings"

	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
)

// Attribute is the in-memory data structure of proto.Attribute
type Attribute struct {
	TensorValue *Tensor
}

// ToString dumps a debug string of the attribute
func (attr *Attribute) ToString() string {
	var builder strings.Builder
	fmt.Fprint(&builder, attr.GetAttrValue())
	return builder.String()
}

func (attr *Attribute) SetStrings(v []string) {
	attr.TensorValue = &Tensor{
		StringS: v,
		DType:   proto.PrimitiveDataType_STRING,
		Shape:   []int{len(v)},
	}
}

func (attr *Attribute) GetStrings() ([]string, error) {
	if attr.TensorValue == nil || len(attr.TensorValue.StringS) == 0 {
		return nil, fmt.Errorf("getString: invalid attribute %v", attr)
	}
	return attr.TensorValue.StringS, nil
}

func (attr *Attribute) SetString(v string) {
	attr.TensorValue = &Tensor{
		StringS: []string{v},
		DType:   proto.PrimitiveDataType_STRING,
	}
}

func (attr *Attribute) SetInt(v int) {
	attr.TensorValue = &Tensor{
		Int32S: []int32{int32(v)},
		DType:  proto.PrimitiveDataType_INT64,
	}
}

func (attr *Attribute) SetInt64(v int64) {
	attr.TensorValue = &Tensor{
		Int64S: []int64{v},
		DType:  proto.PrimitiveDataType_INT64,
	}
}

func (attr *Attribute) SetFloat(v float32) {
	attr.TensorValue = &Tensor{
		FloatS: []float32{v},
		DType:  proto.PrimitiveDataType_FLOAT32,
	}
}

func (attr *Attribute) SetDouble(v float64) {
	attr.TensorValue = &Tensor{
		DoubleS: []float64{v},
		DType:   proto.PrimitiveDataType_FLOAT64,
	}
}

func (attr *Attribute) SetBool(v bool) {
	attr.TensorValue = &Tensor{
		BooleanS: []bool{v},
		DType:    proto.PrimitiveDataType_BOOL,
	}
}

// GetAttrValue returns attr value
func (attr *Attribute) GetAttrValue() interface{} {
	if attr.TensorValue == nil {
		return nil
	}
	if attr.TensorValue.StringS != nil {
		if len(attr.TensorValue.Shape) == 0 && len(attr.TensorValue.StringS) == 1 {
			return attr.TensorValue.StringS[0]
		}
		return attr.TensorValue.StringS
	}
	if attr.TensorValue.BooleanS != nil {
		if len(attr.TensorValue.Shape) == 0 && len(attr.TensorValue.BooleanS) == 1 {
			return attr.TensorValue.BooleanS[0]
		}
		return attr.TensorValue.BooleanS
	}
	if attr.TensorValue.Int32S != nil {
		if len(attr.TensorValue.Shape) == 0 && len(attr.TensorValue.Int32S) == 1 {
			return attr.TensorValue.Int32S[0]
		}
		return attr.TensorValue.Int32S
	}
	if attr.TensorValue.Int64S != nil {
		if len(attr.TensorValue.Shape) == 0 && len(attr.TensorValue.Int64S) == 1 {
			return attr.TensorValue.Int64S[0]
		}
		return attr.TensorValue.Int64S
	}
	if attr.TensorValue.FloatS != nil {
		if len(attr.TensorValue.Shape) == 0 && len(attr.TensorValue.FloatS) == 1 {
			return attr.TensorValue.FloatS[0]
		}
		return attr.TensorValue.FloatS
	}
	if attr.TensorValue.DoubleS != nil {
		if len(attr.TensorValue.Shape) == 0 && len(attr.TensorValue.DoubleS) == 1 {
			return attr.TensorValue.DoubleS[0]
		}
		return attr.TensorValue.DoubleS
	}
	return nil
}

func (attr *Attribute) ToProto() *proto.AttributeValue {
	return &proto.AttributeValue{
		Value: &proto.AttributeValue_T{
			T: attr.TensorValue.ToProto()},
	}
}
