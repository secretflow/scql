// Copyright 2025 Ant Group Co., Ltd.
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

package compiler

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/secretflow/scql/pkg/expression"
	"github.com/secretflow/scql/pkg/expression/aggregation"
	"github.com/secretflow/scql/pkg/interpreter/graph"
	"github.com/secretflow/scql/pkg/interpreter/operator"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/planner/core"
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
	v1 "github.com/secretflow/scql/pkg/proto-gen/scql/v1alpha1"
	"github.com/secretflow/scql/pkg/types"
)

type kernelTestCase struct {
	name               string
	node               Operator
	kernel             Kernel
	wantEnsureErr      bool
	wantEnsureErrMsg   string
	wantToEngineErr    bool
	wantToEngineErrMsg string
	execNodeChecker    func(engineOps []*graph.ExecutionNode)
}

func createAggFunc(name string, hasDistinct bool, mode aggregation.AggFunctionMode) *aggregation.AggFuncDesc {
	aggFunc := &aggregation.AggFuncDesc{}
	aggFunc.Name = name
	aggFunc.HasDistinct = hasDistinct
	aggFunc.Mode = mode
	return aggFunc
}

func createTestSuit() (*ExecutionGraphBuilder, func(string, tensorPlacement, []string) *TensorMeta, func(string) *TensorMeta) {
	tensorMetaManager := NewTensorMetaManager()
	srm := NewSecurityRelaxationManager(&GlobalSecurityRelaxation{}, nil)
	vt := NewVisibilityTable([]string{"alice", "bob", "carol"})
	info := &graph.EnginesInfo{}

	builder := NewExecutionGraphBuilder(tensorMetaManager, srm, vt, info, &v1.CompileOptions{Batched: false})

	createInputTensor := func(name string, place tensorPlacement, vis []string) *TensorMeta {
		meta := tensorMetaManager.CreateTensorMeta(name, graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64))
		vt.UpdateVisibility(meta, NewVisibleParties(vis))
		_, _ = builder.tm.CreateAndSetFirstTensor(meta, place)
		return meta
	}

	createOutputTensor := func(name string) *TensorMeta {
		return tensorMetaManager.CreateTensorMeta(name, graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64))
	}

	return builder, createInputTensor, createOutputTensor
}

func runKernelTests(t *testing.T, builder *ExecutionGraphBuilder, tests []kernelTestCase) {
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset builder state for each test
			builder.pipelineEngineNodes = nil

			// Set proper next tensor ID, since tensor meta may be created after TensorManager is created
			builder.tm.nextTensorID = builder.tensorMetaManager.tensorNum + 1

			// Test ensureTensorPlace
			err := tt.kernel.ensureTensorPlace(builder, tt.node)
			if tt.wantEnsureErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantEnsureErrMsg)
				return
			}
			assert.NoError(t, err)

			// Test toEngineNodes
			err = tt.kernel.toEngineNodes(builder, tt.node)
			if tt.wantToEngineErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantToEngineErrMsg)
				return
			}
			assert.NoError(t, err)

			// Verify the operation was added
			assert.Len(t, builder.pipelineEngineNodes, 1)

			if tt.execNodeChecker != nil {
				tt.execNodeChecker(builder.pipelineEngineNodes[0].ExecutionNodes)
			}
		})
	}
}

func TestKernelRunSQL(t *testing.T) {
	builder, _, createOutputTensor := createTestSuit()

	tests := []kernelTestCase{
		{
			name: "basic test",
			node: &OperatorRunSQL{
				sourceParty: "alice",
				sql:         "SELECT * FROM table1",
				outputs: []*TensorMeta{
					createOutputTensor("col1"),
					createOutputTensor("col2"),
				},
				tableRefs: []string{"table1"},
			},
			kernel: &KernelRunSQL{},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Equal(t, 1, len(engineOps))
				assert.Equal(t, operator.OpNameRunSQL, engineOps[0].OpType)
				assert.Equal(t, 2, len(engineOps[0].Outputs["Out"]))
			},
		},
		{
			name: "empty outputs",
			node: &OperatorRunSQL{
				sourceParty: "bob",
				sql:         "SELECT 1",
				outputs:     []*TensorMeta{},
				tableRefs:   []string{},
			},
			kernel:             &KernelRunSQL{},
			wantToEngineErr:    true,
			wantToEngineErrMsg: "must contains at least one argument",
		},
	}

	runKernelTests(t, builder, tests)
}

func TestKernelPublishResult(t *testing.T) {
	builder, createInputTensor, _ := createTestSuit()

	tests := []kernelTestCase{
		{
			name: "basic publish result",
			node: &OperatorResult{
				resultTensors: []*TensorMeta{
					createInputTensor("col1", &privatePlacement{partyCode: "alice"}, []string{"alice"}),
					createInputTensor("col2", &privatePlacement{partyCode: "alice"}, []string{"alice"}),
				},
				outputNames:     []string{"result1", "result2"},
				issuerPartyCode: "alice",
			},
			kernel: &KernelPublishResult{},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Equal(t, 1, len(engineOps))
				assert.Equal(t, operator.OpNamePublish, engineOps[0].OpType)
				assert.Equal(t, 2, len(engineOps[0].Outputs["Out"]))
			},
		},
		{
			name: "length mismatch",
			node: &OperatorResult{
				resultTensors: []*TensorMeta{
					createInputTensor("col1", &privatePlacement{partyCode: "alice"}, []string{"alice"}),
				},
				outputNames:     []string{"result1", "result2"}, // mismatch
				issuerPartyCode: "alice",
			},
			kernel:             &KernelPublishResult{},
			wantToEngineErr:    true,
			wantToEngineErrMsg: "length not match",
		},
		{
			name: "invisible to issuer",
			node: &OperatorResult{
				resultTensors: []*TensorMeta{
					createInputTensor("col1", &privatePlacement{partyCode: "bob"}, []string{"bob"}),
				},
				outputNames:     []string{"result1"},
				issuerPartyCode: "alice",
			},
			kernel:           &KernelPublishResult{},
			wantEnsureErr:    true,
			wantEnsureErrMsg: "not visible",
		},
	}

	runKernelTests(t, builder, tests)
}

func TestKernelInsertTable(t *testing.T) {
	builder, createInputTensor, _ := createTestSuit()

	tests := []kernelTestCase{
		{
			name: "basic insert table",
			node: &OperatorResult{
				resultTensors: []*TensorMeta{
					createInputTensor("col1", &privatePlacement{partyCode: "alice"}, []string{"alice"}),
					createInputTensor("col2", &privatePlacement{partyCode: "alice"}, []string{"alice"}),
				},
				outputNames:     []string{"result1", "result2"},
				issuerPartyCode: "alice",
			},
			kernel: &KernelInsertTable{insertTableOpt: &core.InsertTableOption{}},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Equal(t, 1, len(engineOps))
				assert.Equal(t, operator.OpNameInsertTable, engineOps[0].OpType)
				assert.Equal(t, 2, len(engineOps[0].Outputs["Out"]))
			},
		},
		{
			name: "length mismatch",
			node: &OperatorResult{
				resultTensors: []*TensorMeta{
					createInputTensor("col1", &privatePlacement{partyCode: "alice"}, []string{"alice"}),
				},
				outputNames:     []string{"result1", "result2"}, // mismatch
				issuerPartyCode: "alice",
			},
			kernel:             &KernelInsertTable{insertTableOpt: &core.InsertTableOption{}},
			wantToEngineErr:    true,
			wantToEngineErrMsg: "length not match",
		},
	}

	runKernelTests(t, builder, tests)
}

func TestKernelDumpFile(t *testing.T) {
	builder, createInputTensor, _ := createTestSuit()

	intoOpt := &core.IntoOpt{
		Opt: &ast.SelectIntoOption{
			PartyFiles: []*ast.PartyFile{
				{PartyCode: "alice", FileName: "/tmp/test.csv"},
			},
			LinesInfo:  &ast.LinesClause{},
			FieldsInfo: &ast.FieldsClause{},
		},
	}

	tests := []kernelTestCase{
		{
			name: "basic dump file",
			node: &OperatorResult{
				resultTensors: []*TensorMeta{
					createInputTensor("col1", &privatePlacement{partyCode: "alice"}, []string{"alice"}),
					createInputTensor("col2", &privatePlacement{partyCode: "alice"}, []string{"alice"}),
				},
				outputNames:     []string{"result1", "result2"},
				issuerPartyCode: "alice",
			},
			kernel: &KernelDumpFile{intoOpt: intoOpt},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Equal(t, 1, len(engineOps))
				assert.Equal(t, operator.OpNameDumpFile, engineOps[0].OpType)
			},
		},
		{
			name: "length mismatch",
			node: &OperatorResult{
				resultTensors: []*TensorMeta{
					createInputTensor("col1", &privatePlacement{partyCode: "alice"}, []string{"alice"}),
				},
				outputNames:     []string{"result1", "result2"}, // mismatch
				issuerPartyCode: "alice",
			},
			kernel:             &KernelDumpFile{intoOpt: intoOpt},
			wantToEngineErr:    true,
			wantToEngineErrMsg: "length",
		},
	}

	runKernelTests(t, builder, tests)
}

func TestKernelPsiEQJoin(t *testing.T) {
	builder, createInputTensor, createOutputTensor := createTestSuit()

	tests := []kernelTestCase{
		{
			name: "cross-party index tensor copying",
			node: &OperatorEQJoin{
				leftKeys:      []*TensorMeta{createInputTensor("left_key1", &privatePlacement{partyCode: "alice"}, []string{"alice"})},
				rightKeys:     []*TensorMeta{createInputTensor("right_key1", &privatePlacement{partyCode: "bob"}, []string{"bob"})},
				leftPayloads:  []*TensorMeta{createInputTensor("left_payload1", &privatePlacement{partyCode: "bob"}, []string{"bob"})},
				rightPayloads: []*TensorMeta{createInputTensor("right_payload1", &privatePlacement{partyCode: "alice"}, []string{"alice"})},
				leftOutputs:   []*TensorMeta{createOutputTensor("left_out1")},
				rightOutputs:  []*TensorMeta{createOutputTensor("right_out1")},
				joinType:      core.InnerJoin,
			},
			kernel: &KernelPsiEQJoin{
				leftParty:              "alice",
				rightParty:             "bob",
				leftPayloadsPlacement:  []tensorPlacement{&privatePlacement{partyCode: "bob"}},
				rightPayloadsPlacement: []tensorPlacement{&privatePlacement{partyCode: "alice"}},
				leftTouchResult:        true,
				rightTouchResult:       true,
			},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Equal(t, len(engineOps), 5) // PSI join + left filter + right filter + 2 copies
				assert.Equal(t, operator.OpNameJoin, engineOps[0].OpType)
			},
		},
		{
			name: "basic psi join with keys and payloads",
			node: &OperatorEQJoin{
				leftKeys:      []*TensorMeta{createInputTensor("left_key1", &privatePlacement{partyCode: "alice"}, []string{"alice"})},
				rightKeys:     []*TensorMeta{createInputTensor("right_key1", &privatePlacement{partyCode: "bob"}, []string{"bob"})},
				leftPayloads:  []*TensorMeta{createInputTensor("left_payload1", &privatePlacement{partyCode: "alice"}, []string{"alice"})},
				rightPayloads: []*TensorMeta{createInputTensor("right_payload1", &privatePlacement{partyCode: "bob"}, []string{"bob"})},
				leftOutputs:   []*TensorMeta{createOutputTensor("left_out1")},
				rightOutputs:  []*TensorMeta{createOutputTensor("right_out1")},
				joinType:      core.InnerJoin,
			},
			kernel: &KernelPsiEQJoin{
				leftParty:              "alice",
				rightParty:             "bob",
				leftPayloadsPlacement:  []tensorPlacement{&privatePlacement{partyCode: "alice"}},
				rightPayloadsPlacement: []tensorPlacement{&privatePlacement{partyCode: "bob"}},
				leftTouchResult:        true,
				rightTouchResult:       true,
			},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Equal(t, len(engineOps), 3) // PSI join + left filter + right filter
				assert.Equal(t, operator.OpNameJoin, engineOps[0].OpType)
			},
		},
		{
			name: "payload and output mismatch",
			node: &OperatorEQJoin{
				leftKeys:      []*TensorMeta{createInputTensor("left_key1", &privatePlacement{partyCode: "alice"}, []string{"alice"})},
				rightKeys:     []*TensorMeta{createInputTensor("right_key1", &privatePlacement{partyCode: "bob"}, []string{"bob"})},
				leftPayloads:  []*TensorMeta{},
				rightPayloads: []*TensorMeta{},
				leftOutputs:   []*TensorMeta{createOutputTensor("left_out1")},
				rightOutputs:  []*TensorMeta{createOutputTensor("right_out1")},
				joinType:      core.InnerJoin,
			},
			kernel: &KernelPsiEQJoin{
				leftParty:              "alice",
				rightParty:             "bob",
				leftPayloadsPlacement:  []tensorPlacement{},
				rightPayloadsPlacement: []tensorPlacement{},
				leftTouchResult:        true,
				rightTouchResult:       true,
			},
			wantToEngineErr:    true,
			wantToEngineErrMsg: "length not match",
		},
		{
			name: "invalid operator type",
			node: &OperatorRunSQL{
				sourceParty: "alice",
				sql:         "SELECT 1",
				outputs:     []*TensorMeta{createOutputTensor("col1")},
				tableRefs:   []string{},
			},
			kernel:           &KernelPsiEQJoin{},
			wantEnsureErr:    true,
			wantEnsureErrMsg: "expect eq join node",
		},
		{
			name: "empty payloads handling",
			node: &OperatorEQJoin{
				leftKeys:      []*TensorMeta{createInputTensor("left_key1", &privatePlacement{partyCode: "alice"}, []string{"alice"})},
				rightKeys:     []*TensorMeta{createInputTensor("right_key1", &privatePlacement{partyCode: "bob"}, []string{"bob"})},
				leftPayloads:  []*TensorMeta{},
				rightPayloads: []*TensorMeta{},
				leftOutputs:   []*TensorMeta{},
				rightOutputs:  []*TensorMeta{},
				joinType:      core.InnerJoin,
			},
			kernel: &KernelPsiEQJoin{
				leftParty:              "alice",
				rightParty:             "bob",
				leftPayloadsPlacement:  []tensorPlacement{},
				rightPayloadsPlacement: []tensorPlacement{},
				leftTouchResult:        false,
				rightTouchResult:       false,
			},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Equal(t, len(engineOps), 1) // PSI join
				assert.Equal(t, operator.OpNameJoin, engineOps[0].OpType)
			},
		},
	}

	runKernelTests(t, builder, tests)
}

func TestKernelSecretEQJoin(t *testing.T) {
	builder, createInputTensor, createOutputTensor := createTestSuit()

	tests := []kernelTestCase{
		{
			name: "basic secret join with keys and payloads",
			node: &OperatorEQJoin{
				leftKeys:      []*TensorMeta{createInputTensor("left_key1", &secretPlacement{}, []string{})},
				rightKeys:     []*TensorMeta{createInputTensor("right_key1", &secretPlacement{}, []string{})},
				leftPayloads:  []*TensorMeta{createInputTensor("left_payload1", &secretPlacement{}, []string{})},
				rightPayloads: []*TensorMeta{createInputTensor("right_payload1", &secretPlacement{}, []string{})},
				leftOutputs:   []*TensorMeta{createOutputTensor("left_out1")},
				rightOutputs:  []*TensorMeta{createOutputTensor("right_out1")},
				joinType:      core.InnerJoin,
			},
			kernel: &KernelSecretEQJoin{},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Equal(t, len(engineOps), 1)
				assert.Equal(t, operator.OpNameSecretJoin, engineOps[0].OpType)
			},
		},
		{
			name: "secret join with keys only",
			node: &OperatorEQJoin{
				leftKeys:      []*TensorMeta{createInputTensor("left_key1", &secretPlacement{}, []string{})},
				rightKeys:     []*TensorMeta{createInputTensor("right_key1", &secretPlacement{}, []string{})},
				leftPayloads:  []*TensorMeta{},
				rightPayloads: []*TensorMeta{},
				leftOutputs:   []*TensorMeta{createOutputTensor("left_out1")},
				rightOutputs:  []*TensorMeta{createOutputTensor("right_out1")},
				joinType:      core.InnerJoin,
			},
			kernel: &KernelSecretEQJoin{},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Equal(t, len(engineOps), 1)
				assert.Equal(t, operator.OpNameSecretJoin, engineOps[0].OpType)
			},
		},
	}

	runKernelTests(t, builder, tests)
}

func TestKernelSimpleReduce(t *testing.T) {

	builder, createInputTensor, createOutputTensor := createTestSuit()

	tests := []kernelTestCase{
		{
			name: "basic secret reduce with SUM",
			node: &OperatorReduce{
				input:   createInputTensor("input", &secretPlacement{}, []string{}),
				output:  createOutputTensor("output"),
				aggFunc: createAggFunc(ast.AggFuncSum, false, aggregation.CompleteMode),
			},
			kernel: &KernelSimpleReduce{placement: &secretPlacement{}, aggFunc: createAggFunc(ast.AggFuncSum, false, aggregation.CompleteMode)},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Equal(t, len(engineOps), 1)
				assert.Equal(t, operator.OpNameReduceSum, engineOps[0].OpType)
			},
		},
		{
			name: "basic private reduce with MAX",
			node: &OperatorReduce{
				input:   createInputTensor("input", &privatePlacement{partyCode: "alice"}, []string{"alice"}),
				output:  createOutputTensor("output"),
				aggFunc: createAggFunc(ast.AggFuncMax, false, aggregation.CompleteMode),
			},
			kernel: &KernelSimpleReduce{placement: &privatePlacement{partyCode: "alice"}, aggFunc: createAggFunc(ast.AggFuncMax, false, aggregation.CompleteMode)},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Equal(t, len(engineOps), 1)
				assert.Equal(t, operator.OpNameReduceMax, engineOps[0].OpType)
			},
		},
	}

	runKernelTests(t, builder, tests)
}

func TestKernelPrivateDistinctCount(t *testing.T) {
	builder, createInputTensor, createOutputTensor := createTestSuit()

	tests := []kernelTestCase{
		{
			name: "basic private distinct count",
			node: &OperatorReduce{
				input:   createInputTensor("input", &privatePlacement{partyCode: "alice"}, []string{"alice"}),
				output:  createOutputTensor("output"),
				aggFunc: createAggFunc(ast.AggFuncCount, true, aggregation.CompleteMode),
			},
			kernel: &KernelPrivateDistinctCount{partyCode: "alice"},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Equal(t, 2, len(engineOps))
				assert.Equal(t, operator.OpNameUnique, engineOps[0].OpType)
				assert.Equal(t, operator.OpNameReduceCount, engineOps[1].OpType)
			},
		},
	}

	runKernelTests(t, builder, tests)
}

func TestKernelSecretDistinctCount(t *testing.T) {
	builder, createInputTensor, createOutputTensor := createTestSuit()

	tests := []kernelTestCase{
		{
			name: "basic secret distinct count",
			node: &OperatorReduce{
				input:   createInputTensor("input", &secretPlacement{}, []string{}),
				output:  createOutputTensor("output"),
				aggFunc: createAggFunc(ast.AggFuncCount, true, aggregation.CompleteMode),
			},
			kernel: &KernelSecretDistinctCount{},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Equal(t, 3, len(engineOps))
				assert.Equal(t, operator.OpNameSort, engineOps[0].OpType)
				assert.Equal(t, operator.OpNameObliviousGroupMark, engineOps[1].OpType)
				assert.Equal(t, operator.OpNameReduceSum, engineOps[2].OpType)
			},
		},
	}

	runKernelTests(t, builder, tests)
}

func TestKernelShapeCount(t *testing.T) {
	builder, createInputTensor, createOutputTensor := createTestSuit()

	tests := []kernelTestCase{
		{
			name: "basic shape count",
			node: &OperatorReduce{
				input:   createInputTensor("input", &privatePlacement{partyCode: "alice"}, []string{"alice"}),
				output:  createOutputTensor("output"),
				aggFunc: createAggFunc(ast.AggFuncCount, false, aggregation.CompleteMode),
			},
			kernel: &KernelShapeCount{inputPlacement: &privatePlacement{partyCode: "alice"}, partyCode: "alice"},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Equal(t, 1, len(engineOps))
				assert.Equal(t, operator.OpNameShape, engineOps[0].OpType)
			},
		},
	}

	runKernelTests(t, builder, tests)
}

func TestKernelBroadcastTo(t *testing.T) {
	builder, createInputTensor, createOutputTensor := createTestSuit()

	tests := []kernelTestCase{
		{
			name: "basic broadcast to private",
			node: &OperatorBroadcastTo{
				shapeRef: createInputTensor("shape_ref", &privatePlacement{partyCode: "alice"}, []string{"alice"}),
				scalars:  []*TensorMeta{createInputTensor("scalar", &publicPlacement{}, []string{"alice", "bob", "carol"})},
				outputs:  []*TensorMeta{createOutputTensor("output")},
			},
			kernel: &KernelBroadcastTo{refPlacement: &privatePlacement{partyCode: "alice"}},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Equal(t, 1, len(engineOps))
				assert.Equal(t, operator.OpNameBroadcastTo, engineOps[0].OpType)
				assert.Equal(t, proto.TensorStatus_TENSORSTATUS_PRIVATE, engineOps[0].Outputs["Out"][0].Status())
			},
		},
		{
			name: "broadcast to public",
			node: &OperatorBroadcastTo{
				shapeRef: createInputTensor("shape_ref", &publicPlacement{}, []string{"alice", "bob", "carol"}),
				scalars:  []*TensorMeta{createInputTensor("scalar", &publicPlacement{}, []string{"alice", "bob", "carol"})},
				outputs:  []*TensorMeta{createOutputTensor("output")},
			},
			kernel: &KernelBroadcastTo{refPlacement: &publicPlacement{}},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Equal(t, 1, len(engineOps))
				assert.Equal(t, operator.OpNameBroadcastTo, engineOps[0].OpType)
				assert.Equal(t, proto.TensorStatus_TENSORSTATUS_PUBLIC, engineOps[0].Outputs["Out"][0].Status())
			},
		},
	}

	runKernelTests(t, builder, tests)
}

func TestKernelPrivateGroupAgg(t *testing.T) {
	builder, createInputTensor, createOutputTensor := createTestSuit()

	tests := []kernelTestCase{
		{
			name: "basic private group by with SUM",
			node: &OperatorGroupAgg{
				groupKeys:          []*TensorMeta{createInputTensor("group_key", &privatePlacement{partyCode: "alice"}, []string{"alice"})},
				aggArgs:            []*TensorMeta{createInputTensor("payload", &privatePlacement{partyCode: "alice"}, []string{"alice"})},
				argFuncOutputs:     []*TensorMeta{createOutputTensor("output")},
				simpleCountOutputs: []*TensorMeta{},
				aggFuncsWithArg:    []*aggregation.AggFuncDesc{createAggFunc(ast.AggFuncSum, false, aggregation.CompleteMode)},
			},
			kernel: &KernelPrivateGroupAgg{partyCode: "alice"},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Equal(t, 2, len(engineOps))
				assert.Equal(t, operator.OpNameGroup, engineOps[0].OpType)
				assert.Equal(t, operator.OpNameGroupSum, engineOps[1].OpType)
			},
		},
		{
			name: "private group by with COUNT DISTINCT",
			node: &OperatorGroupAgg{
				groupKeys:          []*TensorMeta{createInputTensor("group_key", &privatePlacement{partyCode: "alice"}, []string{"alice"})},
				aggArgs:            []*TensorMeta{createInputTensor("payload", &privatePlacement{partyCode: "alice"}, []string{"alice"})},
				argFuncOutputs:     []*TensorMeta{createOutputTensor("output")},
				simpleCountOutputs: []*TensorMeta{},
				aggFuncsWithArg:    []*aggregation.AggFuncDesc{createAggFunc(ast.AggFuncCount, true, aggregation.CompleteMode)},
			},
			kernel: &KernelPrivateGroupAgg{partyCode: "alice"},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Equal(t, 2, len(engineOps))
				assert.Equal(t, operator.OpNameGroup, engineOps[0].OpType)
				assert.Equal(t, operator.OpNameGroupCountDistinct, engineOps[1].OpType)
			},
		},
		{
			name: "private group by with secret SUM",
			node: &OperatorGroupAgg{
				groupKeys:          []*TensorMeta{createInputTensor("group_key", &privatePlacement{partyCode: "alice"}, []string{"alice"})},
				aggArgs:            []*TensorMeta{createInputTensor("payload", &privatePlacement{partyCode: "bob"}, []string{"bob"})},
				argFuncOutputs:     []*TensorMeta{createOutputTensor("output")},
				simpleCountOutputs: []*TensorMeta{},
				aggFuncsWithArg:    []*aggregation.AggFuncDesc{createAggFunc(ast.AggFuncSum, false, aggregation.CompleteMode)},
			},
			kernel: &KernelPrivateGroupAgg{partyCode: "alice", PGSParties: map[int]string{8: "bob"}},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Equal(t, 6, len(engineOps))
				assert.Equal(t, operator.OpNameGroup, engineOps[0].OpType)
				assert.Equal(t, operator.OpNameGroupSecretSum, engineOps[4].OpType)
			},
		},
	}

	runKernelTests(t, builder, tests)
}

func TestKernelObliviousGroupAgg(t *testing.T) {
	builder, createInputTensor, createOutputTensor := createTestSuit()

	tests := []kernelTestCase{
		{
			name: "basic oblivious group by with multiple agg funcs",
			node: &OperatorGroupAgg{
				groupKeys:          []*TensorMeta{createInputTensor("group_key", &secretPlacement{}, []string{})},
				aggArgs:            []*TensorMeta{createInputTensor("payload1", &secretPlacement{}, []string{}), createInputTensor("payload2", &secretPlacement{}, []string{}), createInputTensor("payload3", &secretPlacement{}, []string{})},
				argFuncOutputs:     []*TensorMeta{createOutputTensor("sum_out"), createOutputTensor("avg_out"), createOutputTensor("count_distinct_out")},
				simpleCountOutputs: []*TensorMeta{createOutputTensor("count_out")},
				aggFuncsWithArg: []*aggregation.AggFuncDesc{
					createAggFunc(ast.AggFuncSum, false, aggregation.CompleteMode),
					createAggFunc(ast.AggFuncAvg, false, aggregation.CompleteMode),
					createAggFunc(ast.AggFuncCount, true, aggregation.CompleteMode),
				},
			},
			kernel: &KernelObliviousGroupAgg{},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Len(t, engineOps, 12)
				assert.Equal(t, operator.OpNameSort, engineOps[0].OpType)
				assert.Equal(t, operator.OpNameObliviousGroupMark, engineOps[1].OpType)
				assert.Equal(t, operator.OpNameObliviousGroupCount, engineOps[2].OpType)
				assert.Equal(t, operator.OpNameObliviousGroupSum, engineOps[3].OpType)
				assert.Equal(t, operator.OpNameObliviousGroupAvg, engineOps[4].OpType)
				assert.Equal(t, operator.OpNameSort, engineOps[5].OpType)
				assert.Equal(t, operator.OpNameObliviousGroupMark, engineOps[6].OpType)
				assert.Equal(t, operator.OpNameLogicalOr, engineOps[7].OpType)
				assert.Equal(t, operator.OpNameObliviousGroupSum, engineOps[8].OpType)
				assert.Equal(t, operator.OpNameShuffle, engineOps[9].OpType)
				assert.Equal(t, operator.OpNameMakePublic, engineOps[10].OpType)
				assert.Equal(t, operator.OpNameFilter, engineOps[11].OpType)
			},
		},
		{
			name: "oblivious group by with alice private sort",
			node: &OperatorGroupAgg{
				groupKeys:          []*TensorMeta{createInputTensor("group_key", &privatePlacement{partyCode: "alice"}, []string{"alice"})},
				aggArgs:            []*TensorMeta{createInputTensor("payload1", &secretPlacement{}, []string{}), createInputTensor("payload2", &secretPlacement{}, []string{}), createInputTensor("payload3", &secretPlacement{}, []string{})},
				argFuncOutputs:     []*TensorMeta{createOutputTensor("sum_out"), createOutputTensor("avg_out"), createOutputTensor("count_distinct_out")},
				simpleCountOutputs: []*TensorMeta{createOutputTensor("count_out")},
				aggFuncsWithArg: []*aggregation.AggFuncDesc{
					createAggFunc(ast.AggFuncSum, false, aggregation.CompleteMode),
					createAggFunc(ast.AggFuncAvg, false, aggregation.CompleteMode),
					createAggFunc(ast.AggFuncCount, true, aggregation.CompleteMode),
				},
			},
			kernel: &KernelObliviousGroupAgg{privateSortParty: "alice"},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Len(t, engineOps, 16)
				assert.Equal(t, operator.OpNameMakeShare, engineOps[0].OpType)
				assert.Equal(t, operator.OpNameShuffle, engineOps[1].OpType)
				assert.Equal(t, operator.OpNameMakePrivate, engineOps[2].OpType)
				assert.Equal(t, operator.OpNameMakeShare, engineOps[3].OpType)
				assert.Equal(t, operator.OpNameSort, engineOps[4].OpType)
				assert.Equal(t, operator.OpNameObliviousGroupMark, engineOps[5].OpType)
				assert.Equal(t, operator.OpNameObliviousGroupCount, engineOps[6].OpType)
				assert.Equal(t, operator.OpNameObliviousGroupSum, engineOps[7].OpType)
				assert.Equal(t, operator.OpNameObliviousGroupAvg, engineOps[8].OpType)
				assert.Equal(t, operator.OpNameSort, engineOps[9].OpType)
				assert.Equal(t, operator.OpNameObliviousGroupMark, engineOps[10].OpType)
				assert.Equal(t, operator.OpNameLogicalOr, engineOps[11].OpType)
				assert.Equal(t, operator.OpNameObliviousGroupSum, engineOps[12].OpType)
				assert.Equal(t, operator.OpNameShuffle, engineOps[13].OpType)
				assert.Equal(t, operator.OpNameMakePublic, engineOps[14].OpType)
				assert.Equal(t, operator.OpNameFilter, engineOps[15].OpType)
			},
		},
		{
			name: "oblivious group by with reveal group mark",
			node: &OperatorGroupAgg{
				groupKeys:          []*TensorMeta{createInputTensor("group_key", &secretPlacement{}, []string{})},
				aggArgs:            []*TensorMeta{createInputTensor("payload1", &secretPlacement{}, []string{}), createInputTensor("payload2", &secretPlacement{}, []string{}), createInputTensor("payload3", &secretPlacement{}, []string{})},
				argFuncOutputs:     []*TensorMeta{createOutputTensor("sum_out"), createOutputTensor("avg_out"), createOutputTensor("count_distinct_out")},
				simpleCountOutputs: []*TensorMeta{createOutputTensor("count_out")},
				aggFuncsWithArg: []*aggregation.AggFuncDesc{
					createAggFunc(ast.AggFuncSum, false, aggregation.CompleteMode),
					createAggFunc(ast.AggFuncAvg, false, aggregation.CompleteMode),
					createAggFunc(ast.AggFuncCount, true, aggregation.CompleteMode),
				},
			},
			kernel: &KernelObliviousGroupAgg{revealGroupMark: true},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Len(t, engineOps, 11)
				assert.Equal(t, operator.OpNameSort, engineOps[0].OpType)
				assert.Equal(t, operator.OpNameObliviousGroupMark, engineOps[1].OpType)
				assert.Equal(t, operator.OpNameObliviousGroupCount, engineOps[2].OpType)
				assert.Equal(t, operator.OpNameObliviousGroupSum, engineOps[3].OpType)
				assert.Equal(t, operator.OpNameObliviousGroupAvg, engineOps[4].OpType)
				assert.Equal(t, operator.OpNameSort, engineOps[5].OpType)
				assert.Equal(t, operator.OpNameObliviousGroupMark, engineOps[6].OpType)
				assert.Equal(t, operator.OpNameLogicalOr, engineOps[7].OpType)
				assert.Equal(t, operator.OpNameObliviousGroupSum, engineOps[8].OpType)
				assert.Equal(t, operator.OpNameMakePublic, engineOps[9].OpType)
				assert.Equal(t, operator.OpNameFilter, engineOps[10].OpType)
			},
		},
	}

	runKernelTests(t, builder, tests)
}

func TestKernelCrossJoin(t *testing.T) {
	builder, createInputTensor, createOutputTensor := createTestSuit()

	tests := []kernelTestCase{
		{
			name: "basic cross join between alice and bob",
			node: &OperatorCrossJoin{
				leftInputs:   []*TensorMeta{createInputTensor("left_col1", &privatePlacement{partyCode: "alice"}, []string{"alice"})},
				rightInputs:  []*TensorMeta{createInputTensor("right_col1", &privatePlacement{partyCode: "bob"}, []string{"bob"})},
				leftOutputs:  []*TensorMeta{createOutputTensor("left_out1")},
				rightOutputs: []*TensorMeta{createOutputTensor("right_out1")},
			},
			kernel: &KernelCrossJoin{
				leftParty:    "alice",
				rightParty:   "bob",
				leftInputs:   []*graph.Tensor{},
				rightInputs:  []*graph.Tensor{},
				leftOutputs:  []*graph.Tensor{},
				rightOutputs: []*graph.Tensor{},
			},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Equal(t, 1, len(engineOps))
				assert.Equal(t, operator.OpNameReplicate, engineOps[0].OpType)
			},
		},
	}

	runKernelTests(t, builder, tests)
}

func TestKernelLimit(t *testing.T) {
	builder, createInputTensor, createOutputTensor := createTestSuit()

	tests := []kernelTestCase{
		{
			name: "basic limit with private placement",
			node: &OperatorLimit{
				inputs:  []*TensorMeta{createInputTensor("input", &privatePlacement{partyCode: "alice"}, []string{"alice"})},
				outputs: []*TensorMeta{createOutputTensor("output")},
				offset:  10,
				count:   100,
			},
			kernel: &KernelLimit{},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Equal(t, 1, len(engineOps))
				assert.Equal(t, operator.OpNameLimit, engineOps[0].OpType)
				assert.Equal(t, int64(10), engineOps[0].Attributes[operator.LimitOffsetAttr].GetAttrValue())
				assert.Equal(t, int64(100), engineOps[0].Attributes[operator.LimitCountAttr].GetAttrValue())
			},
		},
	}

	runKernelTests(t, builder, tests)
}

func TestKernelFilter(t *testing.T) {
	builder, createInputTensor, createOutputTensor := createTestSuit()

	tests := []kernelTestCase{
		{
			name: "filter with private placement and private mask",
			node: &OperatorFilter{
				mask:    createInputTensor("mask", &privatePlacement{partyCode: "alice"}, []string{"alice"}),
				inputs:  []*TensorMeta{createInputTensor("input", &privatePlacement{partyCode: "alice"}, []string{"alice"})},
				outputs: []*TensorMeta{createOutputTensor("output")},
			},
			kernel: &KernelFilter{
				inputPlacements: []tensorPlacement{&privatePlacement{partyCode: "alice"}},
			},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Equal(t, 1, len(engineOps))
				assert.Equal(t, operator.OpNameFilter, engineOps[0].OpType)
			},
		},
		{
			name: "filter with secret placement and public mask",
			node: &OperatorFilter{
				mask:    createInputTensor("mask", &publicPlacement{}, []string{"alice", "bob", "carol"}),
				inputs:  []*TensorMeta{createInputTensor("input", &secretPlacement{}, []string{})},
				outputs: []*TensorMeta{createOutputTensor("output")},
			},
			kernel: &KernelFilter{
				inputPlacements: []tensorPlacement{&secretPlacement{}},
			},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Equal(t, 1, len(engineOps))
				assert.Equal(t, operator.OpNameFilter, engineOps[0].OpType)
			},
		},
	}

	runKernelTests(t, builder, tests)
}

func TestKernelSecretConcat(t *testing.T) {
	builder, createInputTensor, createOutputTensor := createTestSuit()

	tests := []kernelTestCase{
		{
			name: "basic secret concat with multiple inputs",
			node: &OperatorConcat{
				inputs: []*TensorMeta{
					createInputTensor("input1", &secretPlacement{}, []string{}),
					createInputTensor("input2", &secretPlacement{}, []string{}),
					createInputTensor("input3", &secretPlacement{}, []string{}),
				},
				output: createOutputTensor("output"),
			},
			kernel: &KernelSecretConcat{},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Equal(t, 1, len(engineOps))
				assert.Equal(t, operator.OpNameConcat, engineOps[0].OpType)
			},
		},
	}

	runKernelTests(t, builder, tests)
}

func TestKernelSecretSort(t *testing.T) {
	builder, createInputTensor, createOutputTensor := createTestSuit()

	tests := []kernelTestCase{
		{
			name: "basic secret sort with keys and payloads",
			node: &OperatorSort{
				sortKeys: []*TensorMeta{
					createInputTensor("sort_key", &secretPlacement{}, []string{}),
				},
				payloads: []*TensorMeta{
					createInputTensor("payload1", &secretPlacement{}, []string{}),
					createInputTensor("payload2", &secretPlacement{}, []string{}),
				},
				outputs: []*TensorMeta{
					createOutputTensor("sorted_key"),
					createOutputTensor("sorted_payload1"),
					createOutputTensor("sorted_payload2"),
				},
				descending: []bool{false},
			},
			kernel: &KernelSecretSort{},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Equal(t, 1, len(engineOps))
				assert.Equal(t, operator.OpNameSort, engineOps[0].OpType)
			},
		},
	}

	runKernelTests(t, builder, tests)
}

func TestKernelPrivateSort(t *testing.T) {
	builder, createInputTensor, createOutputTensor := createTestSuit()

	tests := []kernelTestCase{
		{
			name: "basic private sort with alice party",
			node: &OperatorSort{
				sortKeys: []*TensorMeta{
					createInputTensor("sort_key", &privatePlacement{partyCode: "alice"}, []string{"alice"}),
				},
				payloads: []*TensorMeta{
					createInputTensor("payload1", &privatePlacement{partyCode: "alice"}, []string{"alice"}),
				},
				outputs: []*TensorMeta{
					createOutputTensor("sorted_key"),
					createOutputTensor("sorted_payload1"),
				},
				descending: []bool{false},
			},
			kernel: &KernelPrivateSort{partyCode: "alice"},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Equal(t, 1, len(engineOps))
				assert.Equal(t, operator.OpNameSort, engineOps[0].OpType)
			},
		},
	}

	runKernelTests(t, builder, tests)
}

func TestKernelConstant(t *testing.T) {
	builder, _, createOutputTensor := createTestSuit()

	tests := []kernelTestCase{
		{
			name: "basic integer constant",
			node: &OperatorConstant{
				output: createOutputTensor("const_output"),
				value: func() *types.Datum {
					d := types.NewDatum(42)
					return &d
				}(),
			},
			kernel: &KernelConstant{},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Equal(t, 1, len(engineOps))
				assert.Equal(t, operator.OpNameConstant, engineOps[0].OpType)
			},
		},
	}

	runKernelTests(t, builder, tests)
}

func TestKernelLocalIn(t *testing.T) {
	builder, createInputTensor, createOutputTensor := createTestSuit()

	tests := []kernelTestCase{
		{
			name: "basic local in with alice party",
			node: &OperatorIn{
				left:   createInputTensor("left_col", &privatePlacement{partyCode: "alice"}, []string{"alice"}),
				right:  createInputTensor("right_col", &privatePlacement{partyCode: "alice"}, []string{"alice"}),
				output: createOutputTensor("in_result"),
			},
			kernel: &KernelLocalIn{partyCode: "alice"},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Equal(t, 1, len(engineOps))
				assert.Equal(t, operator.OpNameIn, engineOps[0].OpType)
			},
		},
	}

	runKernelTests(t, builder, tests)
}

func TestKernelPsiIn(t *testing.T) {
	builder, createInputTensor, createOutputTensor := createTestSuit()

	tests := []kernelTestCase{
		{
			name: "basic psi in between alice and bob",
			node: &OperatorIn{
				left:   createInputTensor("left_col", &privatePlacement{partyCode: "alice"}, []string{"alice"}),
				right:  createInputTensor("right_col", &privatePlacement{partyCode: "bob"}, []string{"bob"}),
				output: createOutputTensor("in_result"),
			},
			kernel: &KernelPsiIn{
				leftParty:  "alice",
				rightParty: "bob",
			},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Equal(t, 1, len(engineOps))
				assert.Equal(t, operator.OpNameIn, engineOps[0].OpType)
			},
		},
	}

	runKernelTests(t, builder, tests)
}

func TestKernelPrivateWindow(t *testing.T) {
	builder, createInputTensor, createOutputTensor := createTestSuit()

	tests := []kernelTestCase{
		{
			name: "basic private window row_number with alice party",
			node: &OperatorWindow{
				partitionKeys: []*TensorMeta{
					createInputTensor("partition_col", &privatePlacement{partyCode: "alice"}, []string{"alice"}),
				},
				orderKeys: []*TensorMeta{
					createInputTensor("order_col", &privatePlacement{partyCode: "alice"}, []string{"alice"}),
				},
				payloads: []*TensorMeta{
					createInputTensor("payload1", &privatePlacement{partyCode: "alice"}, []string{"alice"}),
				},
				payloadOutputs: []*TensorMeta{
					createOutputTensor("payload_out1"),
				},
				funcOutput: createOutputTensor("row_number_result"),
				funcName:   ast.WindowFuncRowNumber,
				descs:      []string{"false"},
			},
			kernel: &KernelPrivateWindow{partyCode: "alice"},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Equal(t, 2, len(engineOps))
				assert.Equal(t, operator.OpNameGroup, engineOps[0].OpType)
				assert.Equal(t, operator.OpNameRowNumber, engineOps[1].OpType)
			},
		},
	}

	runKernelTests(t, builder, tests)
}

func TestKernelObliviousWindow(t *testing.T) {
	builder, createInputTensor, createOutputTensor := createTestSuit()

	tests := []kernelTestCase{
		{
			name: "basic oblivious window row_number with secret placement",
			node: &OperatorWindow{
				partitionKeys: []*TensorMeta{
					createInputTensor("partition_col", &secretPlacement{}, []string{}),
				},
				orderKeys: []*TensorMeta{
					createInputTensor("order_col", &secretPlacement{}, []string{}),
				},
				payloads: []*TensorMeta{
					createInputTensor("payload1", &secretPlacement{}, []string{}),
				},
				payloadOutputs: []*TensorMeta{
					createOutputTensor("payload_out1"),
				},
				funcOutput: createOutputTensor("row_number_result"),
				funcName:   ast.WindowFuncRowNumber,
				descs:      []string{"false"},
			},
			kernel: &KernelObliviousWindow{},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Equal(t, 3, len(engineOps))
				assert.Equal(t, operator.OpNameSort, engineOps[0].OpType)
				assert.Equal(t, operator.OpNameObliviousGroupMark, engineOps[1].OpType)
				assert.Equal(t, operator.OpNameObliviousGroupCount, engineOps[2].OpType)
			},
		},
	}

	runKernelTests(t, builder, tests)
}

func TestKernelArrowFunction(t *testing.T) {
	builder, createInputTensor, createOutputTensor := createTestSuit()

	tests := []kernelTestCase{
		{
			name: "basic arrow function substr with alice party",
			node: &OperatorFunction{
				inputs: []*TensorMeta{
					createInputTensor("input", &privatePlacement{partyCode: "alice"}, []string{"alice"}),
				},
				output: createOutputTensor("output"),
				constParams: []*expression.Constant{
					{Value: types.NewDatum(int64(2))}, {Value: types.NewDatum(int64(5))},
				},
				funcName: ast.Substr,
			},
			kernel: &KernelArrowFunction{partyCode: "alice"},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Equal(t, 1, len(engineOps))
				assert.Equal(t, operator.OpNameArrowFunc, engineOps[0].OpType)
			},
		},
	}

	runKernelTests(t, builder, tests)
}

func TestKernelUnary(t *testing.T) {
	builder, createInputTensor, createOutputTensor := createTestSuit()

	tests := []kernelTestCase{
		{
			name: "basic unary abs with secret placement",
			node: &OperatorFunction{
				inputs: []*TensorMeta{
					createInputTensor("input", &secretPlacement{}, []string{}),
				},
				output:   createOutputTensor("output"),
				funcName: ast.Abs,
			},
			kernel: &KernelUnary{place: &secretPlacement{}},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Equal(t, 1, len(engineOps))
				assert.Equal(t, operator.OpNameAbs, engineOps[0].OpType)
			},
		},
	}

	runKernelTests(t, builder, tests)
}

func TestKernelBinary(t *testing.T) {
	builder, createInputTensor, createOutputTensor := createTestSuit()

	tests := []kernelTestCase{
		{
			name: "basic binary add with secret placement",
			node: &OperatorFunction{
				inputs: []*TensorMeta{
					createInputTensor("lhs", &secretPlacement{}, []string{}),
					createInputTensor("rhs", &secretPlacement{}, []string{}),
				},
				output:   createOutputTensor("output"),
				funcName: ast.Plus,
			},
			kernel: &KernelBinary{
				lhsPlace: &secretPlacement{},
				rhsPlace: &secretPlacement{},
				outPlace: &secretPlacement{},
			},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Equal(t, 1, len(engineOps))
				assert.Equal(t, operator.OpNameAdd, engineOps[0].OpType)
			},
		},
	}

	runKernelTests(t, builder, tests)
}

func TestKernelVariadicCompare(t *testing.T) {
	builder, createInputTensor, createOutputTensor := createTestSuit()

	tests := []kernelTestCase{
		{
			name: "basic variadic compare greatest with secret placement",
			node: &OperatorFunction{
				inputs: []*TensorMeta{
					createInputTensor("input1", &secretPlacement{}, []string{}),
					createInputTensor("input2", &secretPlacement{}, []string{}),
					createInputTensor("input3", &secretPlacement{}, []string{}),
				},
				output:   createOutputTensor("output"),
				funcName: ast.Greatest,
			},
			kernel: &KernelVariadicCompare{placement: &secretPlacement{}},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Equal(t, 1, len(engineOps))
				assert.Equal(t, operator.OpNameGreatest, engineOps[0].OpType)
			},
		},
	}

	runKernelTests(t, builder, tests)
}

func TestKernelPrivateFunc(t *testing.T) {
	builder, createInputTensor, createOutputTensor := createTestSuit()

	tests := []kernelTestCase{
		{
			name: "basic private func ifnull with alice party",
			node: &OperatorFunction{
				inputs: []*TensorMeta{
					createInputTensor("input1", &privatePlacement{partyCode: "alice"}, []string{"alice"}),
					createInputTensor("input2", &privatePlacement{partyCode: "alice"}, []string{"alice"}),
				},
				output:   createOutputTensor("output"),
				funcName: ast.Ifnull,
			},
			kernel: &KernelPrivateFunc{partyCode: "alice"},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Equal(t, 1, len(engineOps))
				assert.Equal(t, operator.OpNameIfNull, engineOps[0].OpType)
			},
		},
	}

	runKernelTests(t, builder, tests)
}

func TestKernelCast(t *testing.T) {
	builder, createInputTensor, createOutputTensor := createTestSuit()

	// Helper function to create output tensor with specific dtype
	createOutputTensorWithType := func(name string, dtype *graph.DataType) *TensorMeta {
		tensor := createOutputTensor(name)
		tensor.DType = dtype
		return tensor
	}

	tests := []kernelTestCase{
		{
			name: "basic cast with private placement",
			node: &OperatorFunction{
				inputs: []*TensorMeta{
					createInputTensor("input", &privatePlacement{partyCode: "alice"}, []string{"alice"}),
				},
				output:   createOutputTensorWithType("output", graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT32)),
				funcName: ast.Cast,
			},
			kernel: &KernelCast{placement: &privatePlacement{partyCode: "alice"}},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Equal(t, 1, len(engineOps))
				assert.Equal(t, operator.OpNameCast, engineOps[0].OpType)
			},
		},
	}

	runKernelTests(t, builder, tests)
}

func TestKernelIf(t *testing.T) {
	builder, createInputTensor, createOutputTensor := createTestSuit()

	tests := []kernelTestCase{
		{
			name: "basic if with alice party",
			node: &OperatorFunction{
				inputs: []*TensorMeta{
					createInputTensor("cond", &privatePlacement{partyCode: "alice"}, []string{"alice"}),
					createInputTensor("true_value", &privatePlacement{partyCode: "alice"}, []string{"alice"}),
					createInputTensor("false_value", &privatePlacement{partyCode: "alice"}, []string{"alice"}),
				},
				output:   createOutputTensor("output"),
				funcName: ast.If,
			},
			kernel: &KernelIf{
				condPlace:       &privatePlacement{partyCode: "alice"},
				trueValuePlace:  &privatePlacement{partyCode: "alice"},
				falseValuePlace: &privatePlacement{partyCode: "alice"},
				outputPlace:     &privatePlacement{partyCode: "alice"},
			},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Equal(t, 1, len(engineOps))
				assert.Equal(t, operator.OpNameIf, engineOps[0].OpType)
			},
		},
		{
			name: "if with secret placement",
			node: &OperatorFunction{
				inputs: []*TensorMeta{
					createInputTensor("cond", &secretPlacement{}, []string{}),
					createInputTensor("true_value", &secretPlacement{}, []string{}),
					createInputTensor("false_value", &secretPlacement{}, []string{}),
				},
				output:   createOutputTensor("output"),
				funcName: ast.If,
			},
			kernel: &KernelIf{
				condPlace:       &secretPlacement{},
				trueValuePlace:  &secretPlacement{},
				falseValuePlace: &secretPlacement{},
				outputPlace:     &secretPlacement{},
			},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Equal(t, 1, len(engineOps))
				assert.Equal(t, operator.OpNameIf, engineOps[0].OpType)
			},
		},
	}

	runKernelTests(t, builder, tests)
}

func TestKernelCase(t *testing.T) {
	builder, createInputTensor, createOutputTensor := createTestSuit()

	tests := []kernelTestCase{
		{
			name: "basic case with alice party",
			node: &OperatorFunction{
				inputs: []*TensorMeta{
					createInputTensor("cond1", &privatePlacement{partyCode: "alice"}, []string{"alice"}),
					createInputTensor("value1", &privatePlacement{partyCode: "alice"}, []string{"alice"}),
					createInputTensor("cond2", &privatePlacement{partyCode: "alice"}, []string{"alice"}),
					createInputTensor("value2", &privatePlacement{partyCode: "alice"}, []string{"alice"}),
					createInputTensor("else_value", &privatePlacement{partyCode: "alice"}, []string{"alice"}),
				},
				output:   createOutputTensor("output"),
				funcName: ast.Case,
			},
			kernel: &KernelCase{
				inputPlacements: []tensorPlacement{
					&privatePlacement{partyCode: "alice"},
					&privatePlacement{partyCode: "alice"},
					&privatePlacement{partyCode: "alice"},
					&privatePlacement{partyCode: "alice"},
					&privatePlacement{partyCode: "alice"},
				},
				outputPlacement: &privatePlacement{partyCode: "alice"},
			},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Equal(t, 1, len(engineOps))
				assert.Equal(t, operator.OpNameCaseWhen, engineOps[0].OpType)
			},
		},
	}

	runKernelTests(t, builder, tests)
}

func TestKernelConcatString(t *testing.T) {
	builder, createInputTensor, createOutputTensor := createTestSuit()

	tests := []kernelTestCase{
		{
			name: "basic concat string with alice party",
			node: &OperatorFunction{
				inputs: []*TensorMeta{
					createInputTensor("str1", &privatePlacement{partyCode: "alice"}, []string{"alice"}),
					createInputTensor("str2", &privatePlacement{partyCode: "alice"}, []string{"alice"}),
					createInputTensor("str3", &privatePlacement{partyCode: "alice"}, []string{"alice"}),
				},
				output:   createOutputTensor("output"),
				funcName: ast.Concat,
			},
			kernel: &KernelConcatString{partyCode: "alice"},
			execNodeChecker: func(engineOps []*graph.ExecutionNode) {
				assert.Equal(t, 3, len(engineOps)) // constant + broadcast + arrow func
				assert.Equal(t, operator.OpNameConstant, engineOps[0].OpType)
				assert.Equal(t, operator.OpNameBroadcastTo, engineOps[1].OpType)
				assert.Equal(t, operator.OpNameArrowFunc, engineOps[2].OpType)
			},
		},
	}

	runKernelTests(t, builder, tests)
}
