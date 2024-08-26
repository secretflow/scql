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

package optimizer

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/secretflow/scql/pkg/interpreter/graph"
	"github.com/secretflow/scql/pkg/interpreter/operator"

	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
)

func TestMapSimple(t *testing.T) {

	partyInfo := graph.NewPartyInfo([]*graph.Participant{
		{
			PartyCode: "p1",
			Endpoints: []string{"p1.net"},
			Token:     "p1_credential",
		},
		{
			PartyCode: "p2",
			Endpoints: []string{"p2.net"},
			Token:     "p2_credential",
		},
		{
			PartyCode: "p3",
			Endpoints: []string{"p3.net"},
			Token:     "p3_credential",
		},
		{
			PartyCode: "alice",
			Endpoints: []string{"alice.net"},
			Token:     "alice_credential",
		},
	})

	e1 := graph.NewGraphBuilder(partyInfo, false)

	t0 := e1.AddTensor("t0")
	t0.OwnerPartyCode = "p1"
	t0.SetStatus(proto.TensorStatus_TENSORSTATUS_PRIVATE)
	t1 := e1.AddTensorAs(t0)
	t1.OwnerPartyCode = "p1"
	t2 := e1.AddTensorAs(t0)
	t2.OwnerPartyCode = "p1"
	e1.AddRunSQLNode("RunSQLOp1", []*graph.Tensor{t0, t1, t2},
		"select sum(test.h1.in_hospital_days), max(test.h1.in_hospital_days), min(test.h1.in_hospital_days) from test.h1", []string{"test.h1"}, "p1")

	t3 := e1.AddTensorAs(t0)
	t3.OwnerPartyCode = "p2"
	t4 := e1.AddTensorAs(t0)
	t4.OwnerPartyCode = "p2"
	t5 := e1.AddTensorAs(t0)
	t5.OwnerPartyCode = "p2"
	e1.AddRunSQLNode("RunSQLOp2", []*graph.Tensor{t3, t4, t5},
		"select sum(test.h2.in_hospital_days), max(test.h2.in_hospital_days), min(test.h2.in_hospital_days) from test.h2", []string{"test.h2"}, "p2")

	t6 := e1.AddTensorAs(t0)
	t6.OwnerPartyCode = "p3"
	t7 := e1.AddTensorAs(t0)
	t7.OwnerPartyCode = "p3"
	t8 := e1.AddTensorAs(t0)
	t8.OwnerPartyCode = "p3"
	e1.AddRunSQLNode("RunSQLOp3", []*graph.Tensor{t6, t7, t8},
		"select sum(test.h3.in_hospital_days), max(test.h3.in_hospital_days), min(test.h3.in_hospital_days) from test.h3", []string{"test.h3"}, "p3")
	allParties := []string{"p1", "p2", "p3", "alice"}
	nt0, err := e1.AddMakeShareNode("make_share", t0, allParties)
	assert.NoError(t, err)
	nt1, err := e1.AddMakeShareNode("make_share", t1, allParties)
	assert.NoError(t, err)
	nt2, err := e1.AddMakeShareNode("make_share", t2, allParties)
	assert.NoError(t, err)
	nt3, err := e1.AddMakeShareNode("make_share", t3, allParties)
	assert.NoError(t, err)
	nt4, err := e1.AddMakeShareNode("make_share", t4, allParties)
	assert.NoError(t, err)
	nt5, err := e1.AddMakeShareNode("make_share", t5, allParties)
	assert.NoError(t, err)
	nt6, err := e1.AddMakeShareNode("make_share", t6, allParties)
	assert.NoError(t, err)
	nt7, err := e1.AddMakeShareNode("make_share", t7, allParties)
	assert.NoError(t, err)
	nt8, err := e1.AddMakeShareNode("make_share", t8, allParties)
	assert.NoError(t, err)
	out1 := e1.AddTensorAs(nt0)
	_, err = e1.AddExecutionNode("concat", operator.OpNameConcat,
		map[string][]*graph.Tensor{"In": {nt0, nt1, nt2}}, map[string][]*graph.Tensor{graph.Out: {out1}},
		map[string]*graph.Attribute{}, allParties)
	assert.NoError(t, err)

	out2 := e1.AddTensorAs(nt3)
	_, err = e1.AddExecutionNode("concat", operator.OpNameConcat,
		map[string][]*graph.Tensor{"In": {nt3, nt4, nt5}}, map[string][]*graph.Tensor{graph.Out: {out2}},
		map[string]*graph.Attribute{}, allParties)
	assert.NoError(t, err)

	out3 := e1.AddTensorAs(nt6)
	_, err = e1.AddExecutionNode("concat", operator.OpNameConcat,
		map[string][]*graph.Tensor{"In": {nt6, nt7, nt8}}, map[string][]*graph.Tensor{graph.Out: {out3}},
		map[string]*graph.Attribute{}, allParties)
	assert.NoError(t, err)

	graph := e1.Build()
	p := NewGraphPartitioner(graph)
	p.NaivePartition()

	m := NewGraphMapper(p.Graph, p.Pipelines)
	m.Map()

	// NaivePartition only includes one worker number
	assert.Equal(t, m.Codes["p1"].Policy.WorkerNumber, 1)
	assert.Equal(t, m.Codes["p2"].Policy.WorkerNumber, 1)
	assert.Equal(t, m.Codes["p3"].Policy.WorkerNumber, 1)
	assert.Equal(t, m.Codes["alice"].Policy.WorkerNumber, 1)

	// check one make share node
	id1 := m.Codes["p1"].Policy.PipelineJobs[0].Jobs[3].Jobs[0][0]
	op1 := m.Codes["p1"].Nodes[id1]
	assert.Equal(t, op1.OpType, operator.OpNameMakeShare)

	assert.Equal(t, m.Codes["p2"].Policy.PipelineJobs[0].Jobs[3].Jobs[0][0], id1)
	assert.Equal(t, m.Codes["p3"].Policy.PipelineJobs[0].Jobs[3].Jobs[0][0], id1)
	assert.Equal(t, m.Codes["alice"].Policy.PipelineJobs[0].Jobs[3].Jobs[0][0], id1)
}

func TestMapSimpleStreaming(t *testing.T) {
	partyInfo := graph.NewPartyInfo([]*graph.Participant{
		{
			PartyCode: "p1",
			Endpoints: []string{"p1.net"},
			Token:     "p1_credential",
		},
		{
			PartyCode: "p2",
			Endpoints: []string{"p2.net"},
			Token:     "p2_credential",
		},
		{
			PartyCode: "p3",
			Endpoints: []string{"p3.net"},
			Token:     "p3_credential",
		},
		{
			PartyCode: "alice",
			Endpoints: []string{"alice.net"},
			Token:     "alice_credential",
		},
	})

	e1 := graph.NewGraphBuilder(partyInfo, true)

	t0 := e1.AddTensor("t0")
	t0.OwnerPartyCode = "p1"
	t0.SetStatus(proto.TensorStatus_TENSORSTATUS_PRIVATE)
	t1 := e1.AddTensorAs(t0)
	t1.OwnerPartyCode = "p1"
	t2 := e1.AddTensorAs(t0)
	t2.OwnerPartyCode = "p1"
	e1.AddRunSQLNode("RunSQLOp1", []*graph.Tensor{t0, t1, t2},
		"select sum(test.h1.in_hospital_days), max(test.h1.in_hospital_days), min(test.h1.in_hospital_days) from test.h1", []string{"test.h1"}, "p1")

	t3 := e1.AddTensorAs(t0)
	t3.OwnerPartyCode = "p2"
	t4 := e1.AddTensorAs(t0)
	t4.OwnerPartyCode = "p2"
	t5 := e1.AddTensorAs(t0)
	t5.OwnerPartyCode = "p2"
	e1.AddRunSQLNode("RunSQLOp2", []*graph.Tensor{t3, t4, t5},
		"select sum(test.h2.in_hospital_days), max(test.h2.in_hospital_days), min(test.h2.in_hospital_days) from test.h2", []string{"test.h2"}, "p2")

	t6 := e1.AddTensorAs(t0)
	t6.OwnerPartyCode = "p3"
	t7 := e1.AddTensorAs(t0)
	t7.OwnerPartyCode = "p3"
	t8 := e1.AddTensorAs(t0)
	t8.OwnerPartyCode = "p3"
	e1.AddRunSQLNode("RunSQLOp3", []*graph.Tensor{t6, t7, t8},
		"select sum(test.h3.in_hospital_days), max(test.h3.in_hospital_days), min(test.h3.in_hospital_days) from test.h3", []string{"test.h3"}, "p3")
	allParties := []string{"p1", "p2", "p3", "alice"}
	nt0, err := e1.AddMakeShareNode("make_share", t0, allParties)
	assert.NoError(t, err)
	nt1, err := e1.AddMakeShareNode("make_share", t1, allParties)
	assert.NoError(t, err)
	nt2, err := e1.AddMakeShareNode("make_share", t2, allParties)
	assert.NoError(t, err)
	nt3, err := e1.AddMakeShareNode("make_share", t3, allParties)
	assert.NoError(t, err)
	nt4, err := e1.AddMakeShareNode("make_share", t4, allParties)
	assert.NoError(t, err)
	nt5, err := e1.AddMakeShareNode("make_share", t5, allParties)
	assert.NoError(t, err)
	nt6, err := e1.AddMakeShareNode("make_share", t6, allParties)
	assert.NoError(t, err)
	nt7, err := e1.AddMakeShareNode("make_share", t7, allParties)
	assert.NoError(t, err)
	nt8, err := e1.AddMakeShareNode("make_share", t8, allParties)
	assert.NoError(t, err)
	out1 := e1.AddTensorAs(nt0)
	_, err = e1.AddExecutionNode("concat", operator.OpNameConcat,
		map[string][]*graph.Tensor{"In": {nt0, nt1, nt2}}, map[string][]*graph.Tensor{graph.Out: {out1}},
		map[string]*graph.Attribute{}, allParties)
	assert.NoError(t, err)

	out2 := e1.AddTensorAs(nt3)
	_, err = e1.AddExecutionNode("concat", operator.OpNameConcat,
		map[string][]*graph.Tensor{"In": {nt3, nt4, nt5}}, map[string][]*graph.Tensor{graph.Out: {out2}},
		map[string]*graph.Attribute{}, allParties)
	assert.NoError(t, err)

	out3 := e1.AddTensorAs(nt6)
	_, err = e1.AddExecutionNode("concat", operator.OpNameConcat,
		map[string][]*graph.Tensor{"In": {nt6, nt7, nt8}}, map[string][]*graph.Tensor{graph.Out: {out3}},
		map[string]*graph.Attribute{}, allParties)
	assert.NoError(t, err)

	graph := e1.Build()
	p := NewGraphPartitioner(graph)
	p.NaivePartition()

	m := NewGraphMapper(p.Graph, p.Pipelines)
	m.Map()

	// NaivePartition only includes one worker number
	assert.Equal(t, m.Codes["p1"].Policy.WorkerNumber, 1)
	assert.Equal(t, m.Codes["p2"].Policy.WorkerNumber, 1)
	assert.Equal(t, m.Codes["p3"].Policy.WorkerNumber, 1)
	assert.Equal(t, m.Codes["alice"].Policy.WorkerNumber, 1)

	// check one make share node
	id1 := m.Codes["p1"].Policy.PipelineJobs[1].Jobs[0].Jobs[0][0]
	op1 := m.Codes["p1"].Nodes[id1]
	assert.Equal(t, false, m.Codes["p1"].Policy.PipelineJobs[1].Batched)
	assert.Equal(t, op1.OpType, operator.OpNameMakeShare)

	assert.Equal(t, m.Codes["p2"].Policy.PipelineJobs[1].Jobs[0].Jobs[0][0], id1)
	assert.Equal(t, m.Codes["p3"].Policy.PipelineJobs[1].Jobs[0].Jobs[0][0], id1)
	assert.Equal(t, m.Codes["alice"].Policy.PipelineJobs[1].Jobs[0].Jobs[0][0], id1)
}
