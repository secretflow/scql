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

	"github.com/secretflow/scql/pkg/interpreter/operator"
	"github.com/secretflow/scql/pkg/interpreter/translator"

	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
)

func TestMapSimple(t *testing.T) {

	partyInfo := translator.NewPartyInfo([]*translator.Participant{
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

	e1 := translator.NewGraphBuilder(partyInfo)

	t0 := e1.AddTensor("t0")
	t0.OwnerPartyCode = "p1"
	t0.Status = proto.TensorStatus_TENSORSTATUS_PRIVATE
	t1 := e1.AddTensorAs(t0)
	t1.OwnerPartyCode = "p1"
	t2 := e1.AddTensorAs(t0)
	t2.OwnerPartyCode = "p1"
	e1.AddRunSQLNode("RunSQLOp1", []*translator.Tensor{t0, t1, t2},
		"select sum(test.h1.in_hospital_days), max(test.h1.in_hospital_days), min(test.h1.in_hospital_days) from test.h1", []string{"test.h1"}, "p1")

	t3 := e1.AddTensorAs(t0)
	t3.OwnerPartyCode = "p2"
	t4 := e1.AddTensorAs(t0)
	t4.OwnerPartyCode = "p2"
	t5 := e1.AddTensorAs(t0)
	t5.OwnerPartyCode = "p2"
	e1.AddRunSQLNode("RunSQLOp2", []*translator.Tensor{t3, t4, t5},
		"select sum(test.h2.in_hospital_days), max(test.h2.in_hospital_days), min(test.h2.in_hospital_days) from test.h2", []string{"test.h2"}, "p2")

	t6 := e1.AddTensorAs(t0)
	t6.OwnerPartyCode = "p3"
	t7 := e1.AddTensorAs(t0)
	t7.OwnerPartyCode = "p3"
	t8 := e1.AddTensorAs(t0)
	t8.OwnerPartyCode = "p3"
	e1.AddRunSQLNode("RunSQLOp3", []*translator.Tensor{t6, t7, t8},
		"select sum(test.h3.in_hospital_days), max(test.h3.in_hospital_days), min(test.h3.in_hospital_days) from test.h3", []string{"test.h3"}, "p3")

	e1.AddConcatNode("concat", []*translator.Tensor{t0, t3, t6})
	e1.AddConcatNode("concat", []*translator.Tensor{t1, t4, t7})
	e1.AddConcatNode("concat", []*translator.Tensor{t2, t5, t8})

	graph := e1.Build()
	p := NewGraphPartitioner(graph)
	p.NaivePartition()

	m := NewGraphMapper(p.Graph, p.SubDAGs)
	m.Map()

	// NaivePartition only includes one worker number
	assert.Equal(t, m.Codes["p1"].Policy.WorkerNumber, 1)
	assert.Equal(t, m.Codes["p2"].Policy.WorkerNumber, 1)
	assert.Equal(t, m.Codes["p3"].Policy.WorkerNumber, 1)
	assert.Equal(t, m.Codes["alice"].Policy.WorkerNumber, 1)

	// check one make share node
	id1 := m.Codes["p1"].Policy.Jobs[3].Jobs[0][0]
	op1 := m.Codes["p1"].Nodes[id1]
	assert.Equal(t, op1.OpType, operator.OpNameMakeShare)

	assert.Equal(t, m.Codes["p2"].Policy.Jobs[3].Jobs[0][0], id1)
	assert.Equal(t, m.Codes["p3"].Policy.Jobs[3].Jobs[0][0], id1)
	assert.Equal(t, m.Codes["alice"].Policy.Jobs[3].Jobs[0][0], id1)
}
