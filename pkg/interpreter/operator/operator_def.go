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

package operator

import (
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
)

type StreamingOpType int

// This is by design
const (
	SinkOp StreamingOpType = iota
	StreamingOp
)

// OperatorDef defines the signature of an operator
type OperatorDef struct {
	proto.OperatorDef
	err             error
	opStreamingType StreamingOpType
}

// SetName sets name of an operator def
func (op *OperatorDef) SetName(name string) {
	op.Name = name
}

func (op *OperatorDef) GetName() string {
	return op.Name
}

func (op *OperatorDef) SetStreamingType(typ StreamingOpType) {
	op.opStreamingType = typ
}

func (op *OperatorDef) GetStreamingType() StreamingOpType {
	return op.opStreamingType
}

// SetDefinition adds detailed definition of the operator
func (op *OperatorDef) SetDefinition(comment string) {
	op.Definition = comment
}

// AddInput adds an input parameter
func (op *OperatorDef) AddInput(name string, doc string, option proto.FormalParameterOptions, statusConstraintName string) {
	if op.err != nil {
		return
	}
	inputs := op.InputParams
	if inputs == nil {
		op.InputParams = make([]*proto.FormalParameter, 0)
	}
	param := &proto.FormalParameter{}
	param.ParamName = name
	param.Option = option
	param.Definition = doc
	param.ParameterStatusConstraintName = statusConstraintName
	op.InputParams = append(op.InputParams, param)
}

// AddOutput adds an output parameter
func (op *OperatorDef) AddOutput(name string, doc string, option proto.FormalParameterOptions, statusConstraintName string) {
	if op.err != nil {
		return
	}
	outputs := op.OutputParams
	if outputs == nil {
		op.OutputParams = make([]*proto.FormalParameter, 0)
	}
	param := &proto.FormalParameter{}
	param.ParamName = name
	param.Option = option
	param.Definition = doc
	param.ParameterStatusConstraintName = statusConstraintName
	op.OutputParams = append(op.OutputParams, param)
}

// AddAttribute adds an attribute name
func (op *OperatorDef) AddAttribute(name string, definition string) {
	if op.err != nil {
		return
	}
	op.AttributeParams = append(op.AttributeParams, &proto.FormalAttribute{Name: name, Definition: definition})
}

// AddDefaultAttributeValue adds default value for attr
func (op *OperatorDef) AddDefaultAttributeValue(name string, attr *proto.AttributeValue) {
	if op.err != nil {
		return
	}
	if op.DefaultAttributeValues == nil {
		op.DefaultAttributeValues = make(map[string]*proto.AttributeValue)
	}
	op.DefaultAttributeValues[name] = attr
}

func CreateIntAttribute(data int) *proto.AttributeValue {
	return &proto.AttributeValue{
		Value: &proto.AttributeValue_T{
			T: &proto.Tensor{
				ElemType:  proto.PrimitiveDataType_INT64,
				Int64Data: []int64{int64(data)},
			}},
	}
}

func CreateIntsAttribute(datas []int64) *proto.AttributeValue {
	return &proto.AttributeValue{
		Value: &proto.AttributeValue_T{
			T: &proto.Tensor{
				ElemType:  proto.PrimitiveDataType_INT64,
				Int64Data: datas,
				Shape: &proto.TensorShape{
					Dim: []*proto.TensorShape_Dimension{&proto.TensorShape_Dimension{
						Value: &proto.TensorShape_Dimension_DimValue{DimValue: int64(len(datas))},
					}},
				},
			}},
	}
}

func CreateFloatAttribute(data float32) *proto.AttributeValue {
	return &proto.AttributeValue{
		Value: &proto.AttributeValue_T{
			T: &proto.Tensor{
				ElemType:  proto.PrimitiveDataType_FLOAT32,
				FloatData: []float32{data},
			}},
	}
}

func CreateBoolAttribute(data bool) *proto.AttributeValue {
	return &proto.AttributeValue{
		Value: &proto.AttributeValue_T{
			T: &proto.Tensor{
				ElemType: proto.PrimitiveDataType_BOOL,
				BoolData: []bool{data},
			}},
	}
}

func CreateStringAttribute(str string) *proto.AttributeValue {
	return &proto.AttributeValue{
		Value: &proto.AttributeValue_T{
			T: &proto.Tensor{
				ElemType:   proto.PrimitiveDataType_STRING,
				StringData: []string{str},
			}},
	}
}

// Set param type constraint
func (op *OperatorDef) SetParamTypeConstraint(constraintName string, status []proto.TensorStatus) {
	if op.err != nil {
		return
	}
	if op.ParamStatusConstraints == nil {
		op.ParamStatusConstraints = make(map[string]*proto.TensorStatusList)
	}
	op.ParamStatusConstraints[constraintName] = &proto.TensorStatusList{
		Status: status,
	}
}

func (op *OperatorDef) GetDefaultAttribute() map[string]*proto.AttributeValue {
	return op.DefaultAttributeValues
}

func (op *OperatorDef) GetOperatorDefProto() *proto.OperatorDef {
	return &op.OperatorDef
}
