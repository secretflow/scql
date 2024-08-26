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

	"github.com/secretflow/scql/pkg/interpreter/ccl"
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
)

func TestGraphSimple(t *testing.T) {
	participants := []*Participant{
		{
			PartyCode: "party1",
			Endpoints: []string{"party1.net"},
			Token:     "party1_credential",
		},
		{
			PartyCode: "party2",
			Endpoints: []string{"party2.net"},
			Token:     "party2_credential",
		},
	}
	partyInfo := NewPartyInfo(participants)
	e1 := NewGraphBuilder(partyInfo, false)

	t1 := e1.AddTensor("alice.t1")
	t1.SetStatus(proto.TensorStatus_TENSORSTATUS_PRIVATE)
	e1.AddRunSQLNode("RunSQLOp1", []*Tensor{t1}, "select f1 from alice.t1", []string{"alice.t1"}, "party1")
	t1.CC = ccl.CreateAllPlainCCL([]string{"party1", "party2"})
	t2 := e1.AddTensor("alice.t2")
	t2.SetStatus(proto.TensorStatus_TENSORSTATUS_PRIVATE)
	e1.AddRunSQLNode("RunSQLOp2", []*Tensor{t2}, "select * from alice.t2", []string{"alice.t2"}, "party2")
	t2.CC = ccl.CreateAllPlainCCL([]string{"party1", "party2"})
	t3, err := e1.AddCopyNode("copy", t1, "party1", "party2")
	assert.NotNil(t, t3)
	assert.Nil(t, err)
	assert.Equal(t, proto.TensorStatus_TENSORSTATUS_PRIVATE, t3.Status())

	t4, err := e1.AddCopyNode("copy", t2, "party2", "party1")
	assert.NotNil(t, t4)
	assert.Nil(t, err)
	assert.Equal(t, proto.TensorStatus_TENSORSTATUS_PRIVATE, t4.Status())

	graph := e1.Build()
	pipelineNodes, err := graph.TopologicalSort()
	assert.Nil(t, err)
	assert.NotNil(t, pipelineNodes)
	// 2 runsql + 2 copy
	assert.Equal(t, 4, len(pipelineNodes[0]))
}

func TestGraphSimpleStreaming(t *testing.T) {
	participants := []*Participant{
		{
			PartyCode: "party1",
			Endpoints: []string{"party1.net"},
			Token:     "party1_credential",
		},
		{
			PartyCode: "party2",
			Endpoints: []string{"party2.net"},
			Token:     "party2_credential",
		},
	}
	partyInfo := NewPartyInfo(participants)
	e1 := NewGraphBuilder(partyInfo, true)

	t1 := e1.AddTensor("alice.t1")
	t1.SetStatus(proto.TensorStatus_TENSORSTATUS_PRIVATE)
	e1.AddRunSQLNode("RunSQLOp1", []*Tensor{t1}, "select f1 from alice.t1", []string{"alice.t1"}, "party1")
	t1.CC = ccl.CreateAllPlainCCL([]string{"party1", "party2"})
	t2 := e1.AddTensor("alice.t2")
	t2.SetStatus(proto.TensorStatus_TENSORSTATUS_PRIVATE)
	e1.AddRunSQLNode("RunSQLOp2", []*Tensor{t2}, "select * from alice.t2", []string{"alice.t2"}, "party2")
	t2.CC = ccl.CreateAllPlainCCL([]string{"party1", "party2"})
	t3, err := e1.AddCopyNode("copy", t1, "party1", "party2")
	assert.NotNil(t, t3)
	assert.Nil(t, err)
	assert.Equal(t, proto.TensorStatus_TENSORSTATUS_PRIVATE, t3.Status())

	t4, err := e1.AddCopyNode("copy", t2, "party2", "party1")
	assert.NotNil(t, t4)
	assert.Nil(t, err)
	assert.Equal(t, proto.TensorStatus_TENSORSTATUS_PRIVATE, t4.Status())

	graph := e1.Build()
	pipelineNodes, err := graph.TopologicalSort()
	assert.Nil(t, err)
	assert.NotNil(t, pipelineNodes)
	// 2 runsql + 2 copy
	assert.Equal(t, 2, len(pipelineNodes[0]))
	assert.Equal(t, 2, len(pipelineNodes[1]))
}
