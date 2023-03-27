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
	"testing"

	"github.com/stretchr/testify/assert"

	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
)

func TestOperatorDef(t *testing.T) {
	var opDef OperatorDef
	opDef.SetName("RunSQL")
	opDef.AddInput("X", "", proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, "T")
	opDef.AddInput("X1", "", proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, "T")
	opDef.SetParamTypeConstraint("T", []proto.TensorStatus{proto.TensorStatus_TENSORSTATUS_PRIVATE})
	assert.Nil(t, opDef.err)
	assert.Equal(t, 2, len(opDef.InputParams))
	assert.Equal(t, 0, len(opDef.OutputParams))
	assert.Equal(t, 1, len(opDef.ParamStatusConstraints))

	opDef1 := &OperatorDef{}
	opDef1.SetName("RunSQL")
	opDef1.AddInput("X", "", proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, "T")
	opDef1.AddInput("X1", "", proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, "T")
	opDef1.AddOutput("X2", "", proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, "T")
	assert.Nil(t, opDef1.err)
	assert.Equal(t, 1, len(opDef1.OutputParams))
	assert.Equal(t, 0, len(opDef1.ParamStatusConstraints))

	opDef2 := &OperatorDef{}
	opDef2.SetName("Less")
	opDef2.AddInput("Left", "", proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, "T")
	opDef2.AddInput("Right", "", proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_SINGLE, "T")
	opDef1.SetParamTypeConstraint("T", []proto.TensorStatus{proto.TensorStatus_TENSORSTATUS_PUBLIC})
	assert.Nil(t, opDef2.err)

	opDef3 := &OperatorDef{}
	opDef3.SetName("Great")
	opDef3.AddOutput("Out", "", proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, "T")
	opDef1.SetParamTypeConstraint("T", []proto.TensorStatus{proto.TensorStatus_TENSORSTATUS_SECRET})
	assert.Nil(t, opDef3.err)
	opDef3.AddOutput("O", "", proto.FormalParameterOptions_FORMALPARAMETEROPTIONS_VARIADIC, "T")
	assert.Nil(t, opDef3.err)
}
