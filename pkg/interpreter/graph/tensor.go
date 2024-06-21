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

	"github.com/secretflow/scql/pkg/interpreter/ccl"
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
)

// Tensor struct
type Tensor struct {
	ID     int
	Name   string
	Option proto.TensorOptions
	DType  proto.PrimitiveDataType
	status proto.TensorStatus
	Shape  []int
	// TODO add more data types
	StringS  []string
	FloatS   []float32
	DoubleS  []float64
	Int32S   []int32
	Int64S   []int64
	BooleanS []bool
	// control tensor related calculation
	CC *ccl.CCL
	// NOTE(yang.y): if SkipDTypeCheck = true, the tensor data type check with
	// logical plan is skipped during the translation.
	SkipDTypeCheck bool
	// `OwnerPartyCode` make sense only when tensor is in private status
	OwnerPartyCode string
	// used to record parties who convert string into secret, they may need to participate revealing secret string later.
	SecretStringOwners []string

	// `IsConstScalar` is true when the tensor's data is constant scalar,
	// or the result of expression which only contains constant scalar.
	IsConstScalar bool

	RefNum int32
}

// ToString dumps a debug string of the tensor
func (t *Tensor) ToString() string {
	return t.ToBriefString()
}

func (t Tensor) Status() proto.TensorStatus {
	return t.status
}

func (t *Tensor) SetStatus(s proto.TensorStatus) {
	t.status = s
}

// ToBriefString dumps a brief string of the tensor
func (t *Tensor) ToBriefString() string {
	var builder strings.Builder
	fmt.Fprintf(&builder, "t_%d:{", t.ID)
	fmt.Fprintf(&builder, "%s:%s:%s}",
		shortName(t.Name),
		ShortStatus(proto.TensorStatus_name[int32(t.Status())]),
		proto.PrimitiveDataType_name[int32(t.DType)],
	)
	return builder.String()
}

func shortName(name string) string {
	s := strings.Split(name, ".")
	return s[len(s)-1]
}

func ShortStatus(status string) string {
	s := strings.Split(status, "_")
	return s[len(s)-1]
}

// NewTensor creates a tensor instance
func NewTensor(id int, name string) *Tensor {
	return &Tensor{
		ID:   id,
		Name: name,
		CC:   ccl.NewCCL(),
	}
}

func (t *Tensor) UniqueName() string {
	return fmt.Sprintf("%s.%d", t.Name, t.ID)
}

func TensorNameFromUniqueName(uniqName string) string {
	ss := strings.Split(uniqName, ".")
	return strings.Join(ss[:len(ss)-1], ".")
}

func newTensorFromProto(pb *proto.Tensor) *Tensor {
	t := &Tensor{
		Name:   pb.Name,
		Option: pb.Option,
		DType:  pb.ElemType,
		RefNum: pb.RefNum,
	}

	if pb.Annotation != nil {
		t.SetStatus(pb.Annotation.Status)
	}

	switch pb.ElemType {
	case proto.PrimitiveDataType_BOOL:
		dst := make([]bool, len(pb.BoolData))
		copy(dst, pb.BoolData)
		t.BooleanS = dst
	case proto.PrimitiveDataType_STRING:
		dst := make([]string, len(pb.StringData))
		copy(dst, pb.StringData)
		t.StringS = dst
	case proto.PrimitiveDataType_INT8, proto.PrimitiveDataType_INT16, proto.PrimitiveDataType_INT32:
		dst := make([]int32, len(pb.Int32Data))
		copy(dst, pb.Int32Data)
		t.Int32S = dst
	case proto.PrimitiveDataType_INT64:
		dst := make([]int64, len(pb.Int64Data))
		copy(dst, pb.Int64Data)
		t.Int64S = dst
	case proto.PrimitiveDataType_FLOAT32:
		dst := make([]float32, len(pb.FloatData))
		copy(dst, pb.FloatData)
		t.FloatS = dst
	case proto.PrimitiveDataType_FLOAT64:
		dst := make([]float64, len(pb.DoubleData))
		copy(dst, pb.DoubleData)
		t.DoubleS = dst
	}

	if x := pb.Shape; x != nil {
		var shape []int
		for _, s := range x.Dim {
			shape = append(shape, int(s.GetDimValue()))
		}
		t.Shape = shape
	}

	return t
}

// ToProto serializes in-memory tensor to proto
func (t *Tensor) ToProto() *proto.Tensor {
	pb := &proto.Tensor{
		Name:     t.UniqueName(),
		Option:   t.Option,
		ElemType: t.DType,
		RefNum:   t.RefNum,
	}
	if t.Status() != proto.TensorStatus_TENSORSTATUS_UNKNOWN {
		pb.Annotation = &proto.TensorAnnotation{
			Status: t.Status()}
	}

	if t.Shape != nil && len(t.Shape) > 0 {
		pb.Shape = &proto.TensorShape{
			Dim: []*proto.TensorShape_Dimension{},
		}
		for _, s := range t.Shape {
			dim := &proto.TensorShape_Dimension{
				Value: &proto.TensorShape_Dimension_DimValue{DimValue: int64(s)},
			}
			pb.Shape.Dim = append(pb.Shape.Dim, dim)
		}
	}

	if t.StringS != nil {
		pb.StringData = t.StringS
	}

	if t.FloatS != nil {
		pb.FloatData = t.FloatS
	}

	if t.DoubleS != nil {
		pb.DoubleData = t.DoubleS
	}

	if t.Int32S != nil {
		pb.Int32Data = t.Int32S
	}

	if t.Int64S != nil {
		pb.Int64Data = t.Int64S
	}

	if t.BooleanS != nil {
		pb.BoolData = t.BooleanS
	}
	return pb
}
