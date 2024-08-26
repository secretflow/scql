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
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/secretflow/scql/pkg/infoschema"
	"github.com/secretflow/scql/pkg/interpreter/ccl"
	"github.com/secretflow/scql/pkg/interpreter/graph"
	"github.com/secretflow/scql/pkg/interpreter/translator"
	"github.com/secretflow/scql/pkg/parser"
	"github.com/secretflow/scql/pkg/planner/core"
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/util/mock"
)

func TestSplitSimple(t *testing.T) {
	r := require.New(t)
	partyInfo := graph.NewPartyInfo([]*graph.Participant{
		{
			PartyCode: "hospital1",
			Endpoints: []string{"hospital1.net"},
			Token:     "h1_credential",
		},
		{
			PartyCode: "hospital2",
			Endpoints: []string{"hospital2.net"},
			Token:     "h2_credential",
		},
		{
			PartyCode: "hospital3",
			Endpoints: []string{"hospital3.net"},
			Token:     "h3_credential",
		},
	})
	e1 := graph.NewGraphBuilder(partyInfo, false)

	t0 := e1.AddTensor("t0")
	t0.SetStatus(proto.TensorStatus_TENSORSTATUS_PRIVATE)
	t0.OwnerPartyCode = "hospital1"
	t0.CC = ccl.CreateAllPlainCCL([]string{"hospital1", "hospital2", "hospital3"})
	e1.AddRunSQLNode("RunSQLOp1", []*graph.Tensor{t0},
		"select test.h1.in_hospital_days from test.h1", []string{"test.h1"}, "hospital1")

	t1 := e1.AddTensorAs(t0)
	t1.OwnerPartyCode = "hospital2"
	e1.AddRunSQLNode("RunSQLOp2", []*graph.Tensor{t1},
		"select test.h2.in_hospital_days from test.h2", []string{"test.h2"}, "hospital2")

	t2 := e1.AddTensorAs(t0)
	t2.OwnerPartyCode = "hospital3"
	e1.AddRunSQLNode("RunSQLOp3", []*graph.Tensor{t2},
		"select test.h3.in_hospital_day from test.h3", []string{"test.h3"}, "hospital3")

	leftIndex, rightIndex, err := e1.AddJoinNode("join", []*graph.Tensor{t0}, []*graph.Tensor{t1}, []string{"hospital1", "hospital2"}, graph.InnerJoin, 0)
	r.NoError(err)
	t0AfterFilters, err := e1.AddFilterByIndexNode("filter", leftIndex, []*graph.Tensor{t0}, "hospital1")
	r.NoError(err)
	t1AfterFilters, err := e1.AddFilterByIndexNode("filter", rightIndex, []*graph.Tensor{t1}, "hospital2")
	r.NoError(err)
	leftIndex, rightIndex, err = e1.AddJoinNode("join", []*graph.Tensor{t0AfterFilters[0]}, []*graph.Tensor{t2}, []string{"hospital1", "hospital3"}, graph.InnerJoin, 0)
	r.NoError(err)
	_, err = e1.AddFilterByIndexNode("filter", leftIndex, []*graph.Tensor{t0AfterFilters[0]}, "hospital1")
	r.NoError(err)
	copyIndex, err := e1.AddCopyNode("copy", leftIndex, "hospital1", "hospital2")
	r.NoError(err)
	_, err = e1.AddFilterByIndexNode("filter", copyIndex, []*graph.Tensor{t1AfterFilters[0]}, "hospital2")
	r.NoError(err)
	_, err = e1.AddFilterByIndexNode("filter", rightIndex, []*graph.Tensor{t2}, "hospital3")
	r.NoError(err)

	graph := e1.Build()
	pipelineNodes, err := graph.TopologicalSort()
	r.NoError(err)
	r.Equal(11, len(pipelineNodes[0]))

	p := NewGraphPartitioner(graph)
	p.NaivePartition()
	partySubDAGs := make(map[string][]*PartySubDAG)
	for _, subDAG := range p.Pipelines[0].SubDAGs {
		dags, err := Split(subDAG)
		r.NoError(err)
		for code, partySubDAG := range dags {
			_, ok := partySubDAGs[code]
			if !ok {
				partySubDAGs[code] = make([]*PartySubDAG, 0)
			}
			partySubDAGs[code] = append(partySubDAGs[code], partySubDAG)
		}
	}

	// hospital1, 6 PartySubDAGs, 1 runsql + 2 join + 2 filter + 1 copy
	r.Equal(6, len(partySubDAGs["hospital1"]))
	r.Equal(1, len(partySubDAGs["hospital1"][0].Nodes))
	r.Equal(1, len(partySubDAGs["hospital1"][1].Nodes))
	r.Equal(1, len(partySubDAGs["hospital1"][2].Nodes))

	// hospital2, 5 PartySubDAGs, 1 runsql + 1 join + 2 filter + 1 copy
	r.Equal(5, len(partySubDAGs["hospital2"]))
	r.Equal(1, len(partySubDAGs["hospital2"][0].Nodes))
	r.Equal(1, len(partySubDAGs["hospital2"][1].Nodes))
	r.Equal(1, len(partySubDAGs["hospital2"][2].Nodes))

	// hospital3, 3 PartySubDAGs, 1 runsql + 1 join + 1 filter
	r.Equal(3, len(partySubDAGs["hospital3"]))
	r.Equal(1, len(partySubDAGs["hospital3"][0].Nodes))
	r.Equal(1, len(partySubDAGs["hospital3"][1].Nodes))
	r.Equal(1, len(partySubDAGs["hospital3"][2].Nodes))
}

func TestSplitComplex(t *testing.T) {
	r := require.New(t)
	sql := `select ta.plain_int_0 from alice.tbl_0 as ta join bob.tbl_0 as tb on ta.plain_int_0 = tb.plain_int_0 where ta.plain_int_1 > tb.plain_int_1 and ta.compare_int_0 < tb.compare_int_0 and ta.compare_int_1 <> tb.compare_int_1 and ta.compare_int_2 >= tb.compare_int_2 and ta.compare_float_0 <= tb.compare_float_0`
	mockTables, err := mock.MockAllTables()
	r.NoError(err)
	is := infoschema.MockInfoSchema(mockTables)
	parser := parser.New()
	ctx := mock.MockContext()
	mockEngines, err := mock.MockEngines()
	r.NoError(err)
	info, err := translator.ConvertMockEnginesToEnginesInfo(mockEngines)
	r.NoError(err)
	ccl, err := mock.MockAllCCL()
	r.NoError(err)

	stmt, err := parser.ParseOneStmt(sql, "", "")
	r.NoError(err)

	err = core.Preprocess(ctx, stmt, is)
	r.NoError(err)

	lp, _, err := core.BuildLogicalPlanWithOptimization(context.Background(), ctx, stmt, is)
	r.NoError(err)
	compileOpts := proto.CompileOptions{
		SecurityCompromise: &proto.SecurityCompromiseConfig{RevealGroupMark: false, GroupByThreshold: 4},
	}
	trans, err := translator.NewTranslator(info, &proto.SecurityConfig{ColumnControlList: ccl}, "alice", &compileOpts)
	r.NoError(err)
	ep, err := trans.Translate(lp)
	r.Nil(err)

	p := NewGraphPartitioner(ep)
	p.NaivePartition()
	partySubDAGs := make(map[string][]*PartySubDAG)
	for _, subDAG := range p.Pipelines[0].SubDAGs {
		dags, err := Split(subDAG)
		r.NoError(err)

		for code, partySubDAG := range dags {
			_, ok := partySubDAGs[code]
			if !ok {
				partySubDAGs[code] = make([]*PartySubDAG, 0)
			}
			partySubDAGs[code] = append(partySubDAGs[code], partySubDAG)
		}
	}

	r.Equal(28, len(partySubDAGs["alice"]))
	r.Equal(1, len(partySubDAGs["alice"][0].Nodes))

	r.Equal(22, len(partySubDAGs["bob"]))
	r.Equal(1, len(partySubDAGs["bob"][0].Nodes))
}
