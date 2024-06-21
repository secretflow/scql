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
	"testing"

	"github.com/stretchr/testify/assert"

	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
)

func TestAttribute(t *testing.T) {
	a := assert.New(t)
	{
		attr := &Attribute{}
		attr.SetString("alice")
		r1 := attr.GetAttrValue()
		a.Equal("alice", r1)
	}

	{
		attr := &Attribute{}
		attr.SetStrings([]string{"alice", "bob"})
		r1 := attr.GetAttrValue()
		a.Equal([]string{"alice", "bob"}, r1)
	}

	{
		attr := &Attribute{}
		attr.SetBool(true)
		r1 := attr.GetAttrValue()
		a.Equal(true, r1)
	}

	{
		attr := &Attribute{}
		attr.SetInt(1)
		pb2 := &proto.AttributeValue_T{
			T: attr.TensorValue.ToProto(),
		}
		a.NotNil(pb2)
		a.Equal(int32(1), attr.GetAttrValue())
	}

	{
		attr := &Attribute{}
		attr.SetFloat(1.0)
		pb2 := &proto.AttributeValue_T{
			T: attr.TensorValue.ToProto(),
		}
		a.NotNil(pb2)
		a.Equal(float32(1.0), attr.GetAttrValue())
	}
}
