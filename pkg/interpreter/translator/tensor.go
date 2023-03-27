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

package translator

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
	Status proto.TensorStatus
	Shape  []int
	// TODO add more data types
	StringS  []string
	FloatS   []float32
	Int32S   []int32
	Int64S   []int64
	BooleanS []bool
	// control tensor related calculation
	cc *ccl.CCL
	// NOTE(yang.y): if skipDTypeCheck = true, the tensor data type check with
	// logical plan is skipped during the translation.
	skipDTypeCheck bool
	// `OwnerPartyCode` make sense only when tensor is in private status
	OwnerPartyCode string

	// `isConstScalar` is true when the tensor's data is constant scalar,
	// or the result of expression which only contains constant scalar.
	isConstScalar bool
}

// ToString dumps a debug string of the tensor
func (t *Tensor) ToString() string {
	return t.ToBriefString()
}

func (t Tensor) status() proto.TensorStatus {
	return t.Status
}

// ToBriefString dumps a brief string of the tensor
func (t *Tensor) ToBriefString() string {
	var builder strings.Builder
	fmt.Fprintf(&builder, "t_%d:{", t.ID)
	fmt.Fprintf(&builder, "%s:%s:%s}",
		shortName(t.Name),
		shortStatus(proto.TensorStatus_name[int32(t.Status)]),
		proto.PrimitiveDataType_name[int32(t.DType)],
	)
	return builder.String()
}

func shortName(name string) string {
	s := strings.Split(name, ".")
	return s[len(s)-1]
}

func shortStatus(status string) string {
	s := strings.Split(status, "_")
	return s[len(s)-1]
}

// NewTensor creates a tensor instance
func NewTensor(id int, name string) *Tensor {
	return &Tensor{
		ID:   id,
		Name: name,
		cc:   ccl.NewCCL(),
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
	}

	if pb.Annotation != nil {
		t.Status = pb.Annotation.Status
	}

	if x := pb.GetI64S(); x != nil {
		dst := make([]int64, len(x.I64S))
		copy(dst, x.I64S)
		t.Int64S = dst
		return t
	}

	if x := pb.GetSs(); x != nil {
		dst := make([]string, len(x.Ss))
		copy(dst, x.Ss)
		t.StringS = dst
		return t
	}

	if x := pb.GetBs(); x != nil {
		dst := make([]bool, len(x.Bs))
		copy(dst, x.Bs)
		t.BooleanS = dst
		return t
	}

	if x := pb.GetIs(); x != nil {
		dst := make([]int32, len(x.Is))
		copy(dst, x.Is)
		t.Int32S = dst
		return t
	}

	if x := pb.GetFs(); x != nil {
		dst := make([]float32, len(x.Fs))
		copy(dst, x.Fs)
		t.FloatS = x.Fs
		return t
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
	}
	if t.Status != proto.TensorStatus_TENSORSTATUS_UNKNOWN {
		pb.Annotation = &proto.TensorAnnotation{
			Status: t.Status}
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
		pb.Value = &proto.Tensor_Ss{
			Ss: &proto.Strings{Ss: t.StringS},
		}
	}

	if t.FloatS != nil {
		pb.Value = &proto.Tensor_Fs{
			Fs: &proto.Floats{Fs: t.FloatS},
		}
	}

	if t.Int32S != nil {
		pb.Value = &proto.Tensor_Is{
			Is: &proto.Int32S{Is: t.Int32S},
		}
	}

	if t.Int64S != nil {
		pb.Value = &proto.Tensor_I64S{
			I64S: &proto.Int64S{I64S: t.Int64S},
		}
	}

	if t.BooleanS != nil {
		pb.Value = &proto.Tensor_Bs{
			Bs: &proto.Booleans{Bs: t.BooleanS},
		}
	}

	return pb
}

// for graph test
func (t *Tensor) SetCCL(cc *ccl.CCL) {
	t.cc = cc
}
