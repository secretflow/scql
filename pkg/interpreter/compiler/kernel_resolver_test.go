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
	"github.com/stretchr/testify/require"

	"github.com/secretflow/scql/pkg/expression/aggregation"
	"github.com/secretflow/scql/pkg/interpreter/graph"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/planner/core"
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
)

func TestChoosePlacement(t *testing.T) {
	parties := []string{"alice", "bob", "carol"}
	vt := NewVisibilityTable(parties)
	tm := NewTensorManager(1)
	kr := NewKernelResolver(nil, vt, tm, nil)

	meta := &TensorMeta{ID: 1, Name: "test_tensor", DType: graph.NewPrimitiveDataType(proto.PrimitiveDataType_STRING)}

	tests := []struct {
		name       string
		setup      func()
		wantStatus proto.TensorStatus
	}{
		{
			name: "empty visibility returns secret placement",
			setup: func() {
			},
			wantStatus: proto.TensorStatus_TENSORSTATUS_SECRET,
		},
		{
			name: "public visibility returns public placement",
			setup: func() {
				vt.UpdateVisibility(meta, vt.PublicVisibility())
			},
			wantStatus: proto.TensorStatus_TENSORSTATUS_PUBLIC,
		},
		{
			name: "single party visibility returns private placement",
			setup: func() {
				vt.UpdateVisibility(meta, NewVisibleParties([]string{"alice"}))
			},
			wantStatus: proto.TensorStatus_TENSORSTATUS_PRIVATE,
		},
		{
			name: "existing private placement is preferred over non-existing public placement",
			setup: func() {
				vt.UpdateVisibility(meta, vt.PublicVisibility())
				tm.CreateAndSetFirstTensor(meta, &privatePlacement{partyCode: "alice"})
			},
			wantStatus: proto.TensorStatus_TENSORSTATUS_PRIVATE,
		},
		{
			name: "existing private placement is preferred",
			setup: func() {
				vt.UpdateVisibility(meta, NewVisibleParties([]string{"alice"}))
				privateTensor := &graph.Tensor{ID: 101, Name: "private_tensor", OwnerPartyCode: "alice"}
				privateTensor.SetStatus(proto.TensorStatus_TENSORSTATUS_PRIVATE)
				tm.CreateAndSetFirstTensor(meta, &privatePlacement{partyCode: "alice"})
				tm.setPlacedTensor(meta, privateTensor, &privatePlacement{partyCode: "alice"})
			},
			wantStatus: proto.TensorStatus_TENSORSTATUS_PRIVATE,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset for each test
			vt = NewVisibilityTable(parties)
			tm = NewTensorManager(1)
			kr = NewKernelResolver(nil, vt, tm, nil)

			if tt.setup != nil {
				(tt.setup)()
			}

			result := kr.choosePlacement(meta)

			assert.Equal(t, tt.wantStatus, result.Status())
		})
	}
}

func TestChooseSecretOrPrivatePlacement(t *testing.T) {
	parties := []string{"alice", "bob", "carol"}
	vt := NewVisibilityTable(parties)
	tm := NewTensorManager(1)
	kr := NewKernelResolver(nil, vt, tm, nil)

	meta := &TensorMeta{ID: 1, Name: "test_tensor"}

	tests := []struct {
		name       string
		setup      func()
		wantStatus proto.TensorStatus
	}{
		{
			name: "empty visibility returns secret placement",
			setup: func() {
			},
			wantStatus: proto.TensorStatus_TENSORSTATUS_SECRET,
		},
		{
			name: "public visibility returns private placement (public not allowed)",
			setup: func() {
				vt.UpdateVisibility(meta, vt.PublicVisibility())
			},
			wantStatus: proto.TensorStatus_TENSORSTATUS_PRIVATE,
		},
		{
			name: "single party visibility returns private placement",
			setup: func() {
				vt.UpdateVisibility(meta, NewVisibleParties([]string{"alice"}))
			},
			wantStatus: proto.TensorStatus_TENSORSTATUS_PRIVATE,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset for each test
			vt = NewVisibilityTable(parties)
			tm = NewTensorManager(1)
			kr = NewKernelResolver(nil, vt, tm, nil)

			if tt.setup != nil {
				(tt.setup)()
			}

			result := kr.chooseSecretOrPrivatePlacement(meta)

			assert.Equal(t, tt.wantStatus, result.Status())
		})
	}
}

func TestChoosePlacementAllFine(t *testing.T) {
	parties := []string{"alice", "bob", "carol"}
	vt := NewVisibilityTable(parties)
	tm := NewTensorManager(1)
	kr := NewKernelResolver(nil, vt, tm, nil)

	meta := &TensorMeta{ID: 1, Name: "test_tensor"}

	tests := []struct {
		name       string
		setup      func()
		wantStatus proto.TensorStatus
	}{
		{
			name: "empty visibility returns secret placement",
			setup: func() {
			},
			wantStatus: proto.TensorStatus_TENSORSTATUS_SECRET,
		},
		{
			name: "public visibility returns public placement",
			setup: func() {
				vt.UpdateVisibility(meta, vt.PublicVisibility())
			},
			wantStatus: proto.TensorStatus_TENSORSTATUS_PUBLIC,
		},
		{
			name: "single party visibility returns private placement",
			setup: func() {
				vt.UpdateVisibility(meta, NewVisibleParties([]string{"alice"}))
			},
			wantStatus: proto.TensorStatus_TENSORSTATUS_PRIVATE,
		},
		{
			name: "existing placement is preferred even when it is secret",
			setup: func() {
				vt.UpdateVisibility(meta, NewVisibleParties([]string{"alice"}))
				tm.CreateAndSetFirstTensor(meta, &secretPlacement{})
			},
			wantStatus: proto.TensorStatus_TENSORSTATUS_SECRET,
		},
		{
			name: "existing public placement is preferred over existing private placement",
			setup: func() {
				vt.UpdateVisibility(meta, vt.PublicVisibility())
				tm.CreateAndSetFirstTensor(meta, &publicPlacement{})
				privateTensor := &graph.Tensor{ID: 101, Name: "private_tensor", OwnerPartyCode: "alice"}
				privateTensor.SetStatus(proto.TensorStatus_TENSORSTATUS_PRIVATE)
				tm.setPlacedTensor(meta, privateTensor, &privatePlacement{partyCode: "alice"})
			},
			wantStatus: proto.TensorStatus_TENSORSTATUS_PUBLIC,
		},
		{
			name: "existing private placement is preferred over existing secret placement",
			setup: func() {
				vt.UpdateVisibility(meta, NewVisibleParties([]string{"alice"}))
				tm.CreateAndSetFirstTensor(meta, &secretPlacement{})
				privateTensor := &graph.Tensor{ID: 101, Name: "private_tensor", OwnerPartyCode: "alice"}
				privateTensor.SetStatus(proto.TensorStatus_TENSORSTATUS_PRIVATE)
				tm.setPlacedTensor(meta, privateTensor, &privatePlacement{partyCode: "alice"})
			},
			wantStatus: proto.TensorStatus_TENSORSTATUS_PRIVATE,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset for each test
			vt = NewVisibilityTable(parties)
			tm = NewTensorManager(1)
			kr = NewKernelResolver(nil, vt, tm, nil)

			if tt.setup != nil {
				(tt.setup)()
			}

			result := kr.choosePlacementAllFine(meta)

			assert.Equal(t, tt.wantStatus, result.Status())
		})
	}
}

func TestResolveEQJoin(t *testing.T) {
	tensorMetaManager := NewTensorMetaManager()
	srm := NewSecurityRelaxationManager(&GlobalSecurityRelaxation{RevealKeyAfterJoin: true}, nil)
	vt := NewVisibilityTable([]string{"alice", "bob", "carol"})
	tm := NewTensorManager(1)
	kr := NewKernelResolver(srm, vt, tm, nil)

	createPlacedTensor := func(name string, place tensorPlacement, vis []string) (*TensorMeta, *graph.Tensor) {
		meta := tensorMetaManager.CreateTensorMeta(name, graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64))
		vt.UpdateVisibility(meta, NewVisibleParties(vis))
		tensor, _ := tm.CreateAndSetFirstTensor(meta, place)
		return meta, tensor
	}

	tests := []struct {
		name               string
		revealKeyAfterJoin bool
		setup              func() *OperatorEQJoin
		wantKernelType     any
		wantLeftParty      string
		wantRightParty     string
		wantErr            bool
		wantErrMsg         string
	}{
		{
			name:               "psi join with valid parties and need tensor status conversion",
			revealKeyAfterJoin: true,
			setup: func() *OperatorEQJoin {
				leftKey, _ := createPlacedTensor("left_key", &secretPlacement{}, []string{"alice"})
				rightKey, _ := createPlacedTensor("right_key", &secretPlacement{}, []string{"bob"})
				leftPayload, _ := createPlacedTensor("left_payload", &privatePlacement{partyCode: "alice"}, []string{"alice"})
				rightPayload, _ := createPlacedTensor("right_payload", &privatePlacement{partyCode: "bob"}, []string{"bob"})

				return &OperatorEQJoin{
					baseOperator:  baseOperator{id: 1},
					leftKeys:      []*TensorMeta{leftKey},
					rightKeys:     []*TensorMeta{rightKey},
					leftPayloads:  []*TensorMeta{leftPayload},
					rightPayloads: []*TensorMeta{rightPayload},
					leftOutputs:   []*TensorMeta{leftPayload},
					rightOutputs:  []*TensorMeta{rightPayload},
					joinType:      core.InnerJoin,
				}
			},
			wantKernelType: &KernelPsiEQJoin{},
			wantLeftParty:  "alice",
			wantRightParty: "bob",
		},
		{
			name:               "psi join using existing private placement",
			revealKeyAfterJoin: true,
			setup: func() *OperatorEQJoin {
				leftKey, _ := createPlacedTensor("left_key", &privatePlacement{partyCode: "alice"}, []string{"alice", "bob"})
				rightKey, _ := createPlacedTensor("right_key", &privatePlacement{partyCode: "bob"}, []string{"alice", "bob"})
				leftPayload, _ := createPlacedTensor("left_payload", &privatePlacement{partyCode: "bob"}, []string{"alice", "bob"})
				rightPayload, _ := createPlacedTensor("right_payload", &privatePlacement{partyCode: "alice"}, []string{"alice", "bob"})

				return &OperatorEQJoin{
					baseOperator:  baseOperator{id: 1},
					leftKeys:      []*TensorMeta{leftKey},
					rightKeys:     []*TensorMeta{rightKey},
					leftPayloads:  []*TensorMeta{leftPayload},
					rightPayloads: []*TensorMeta{rightPayload},
					leftOutputs:   []*TensorMeta{leftPayload},
					rightOutputs:  []*TensorMeta{rightPayload},
					joinType:      core.InnerJoin,
				}
			},
			wantKernelType: &KernelPsiEQJoin{},
			wantLeftParty:  "alice",
			wantRightParty: "bob",
		},
		{
			name:               "secret join when no valid parties",
			revealKeyAfterJoin: false,
			setup: func() *OperatorEQJoin {
				leftKey, _ := createPlacedTensor("left_key", &secretPlacement{}, []string{})
				rightKey, _ := createPlacedTensor("right_key", &secretPlacement{}, []string{"alice"})
				leftPayload, _ := createPlacedTensor("left_payload", &privatePlacement{partyCode: "alice"}, []string{"alice"})
				rightPayload, _ := createPlacedTensor("right_payload", &privatePlacement{partyCode: "alice"}, []string{"alice"})

				return &OperatorEQJoin{
					baseOperator:  baseOperator{id: 1},
					leftKeys:      []*TensorMeta{leftKey},
					rightKeys:     []*TensorMeta{rightKey},
					leftPayloads:  []*TensorMeta{leftPayload},
					rightPayloads: []*TensorMeta{rightPayload},
					leftOutputs:   []*TensorMeta{leftPayload},
					rightOutputs:  []*TensorMeta{rightPayload},
					joinType:      core.InnerJoin,
				}
			},
			wantKernelType: &KernelSecretEQJoin{},
		},
		{
			name:               "valid parties to perform psi but RevealKeyAfterJoin=false",
			revealKeyAfterJoin: false,
			setup: func() *OperatorEQJoin {
				leftKey, _ := createPlacedTensor("left_key", &secretPlacement{}, []string{"alice"})
				rightKey, _ := createPlacedTensor("right_key", &secretPlacement{}, []string{"bob"})
				leftPayload, _ := createPlacedTensor("left_payload", &privatePlacement{partyCode: "alice"}, []string{"alice"})
				rightPayload, _ := createPlacedTensor("right_payload", &privatePlacement{partyCode: "bob"}, []string{"bob"})

				return &OperatorEQJoin{
					baseOperator:  baseOperator{id: 1},
					leftKeys:      []*TensorMeta{leftKey},
					rightKeys:     []*TensorMeta{rightKey},
					leftPayloads:  []*TensorMeta{leftPayload},
					rightPayloads: []*TensorMeta{rightPayload},
					leftOutputs:   []*TensorMeta{leftPayload},
					rightOutputs:  []*TensorMeta{rightPayload},
					joinType:      core.InnerJoin,
				}
			},
			wantKernelType: &KernelSecretEQJoin{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset for each test
			tensorMetaManager = NewTensorMetaManager()
			tm = NewTensorManager(1)
			vt = NewVisibilityTable([]string{"alice", "bob", "carol"})
			srm = NewSecurityRelaxationManager(&GlobalSecurityRelaxation{RevealKeyAfterJoin: tt.revealKeyAfterJoin}, nil)
			kr = NewKernelResolver(srm, vt, tm, nil)

			n := tt.setup()
			kernel, err := kr.resolveEQJoin(n)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrMsg)
				return
			}

			require.NoError(t, err)
			assert.IsType(t, tt.wantKernelType, kernel)

			// Check specific fields for PsiEQJoin
			if psiKernel, ok := kernel.(*KernelPsiEQJoin); ok {
				assert.Equal(t, tt.wantLeftParty, psiKernel.leftParty)
				assert.Equal(t, tt.wantRightParty, psiKernel.rightParty)
				// Since compileOpts is nil, it should use default value
				assert.Equal(t, proto.PsiAlgorithmType_AUTO, psiKernel.psiAlgorithmType)
			}
		})
	}
}

func TestResolveRunSQL(t *testing.T) {
	kr := NewKernelResolver(nil, nil, nil, nil)

	n := &OperatorRunSQL{
		baseOperator: baseOperator{id: 1},
		sql:          "SELECT * FROM test",
		sourceParty:  "alice",
	}

	kernel, err := kr.resolveRunSQL(n)

	assert.NoError(t, err)
	assert.IsType(t, &KernelRunSQL{}, kernel)
}

func TestResolveResult(t *testing.T) {
	kr := NewKernelResolver(nil, nil, nil, nil)

	tests := []struct {
		name           string
		setup          func() *OperatorResult
		wantKernelType any
	}{
		{
			name: "result with intoOpt",
			setup: func() *OperatorResult {
				return &OperatorResult{
					baseOperator: baseOperator{id: 1},
					intoOpt:      &core.IntoOpt{},
				}
			},
			wantKernelType: &KernelDumpFile{},
		},
		{
			name: "result with insertTableOpt",
			setup: func() *OperatorResult {
				return &OperatorResult{
					baseOperator:   baseOperator{id: 2},
					insertTableOpt: &core.InsertTableOption{TableName: "test_table"},
				}
			},
			wantKernelType: &KernelInsertTable{},
		},
		{
			name: "result with neither option",
			setup: func() *OperatorResult {
				return &OperatorResult{
					baseOperator: baseOperator{id: 3},
				}
			},
			wantKernelType: &KernelPublishResult{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := tt.setup()
			kernel, err := kr.resolveResult(n)

			assert.NoError(t, err)
			assert.IsType(t, tt.wantKernelType, kernel)
		})
	}
}

func TestResolveLimit(t *testing.T) {
	kr := NewKernelResolver(nil, nil, nil, nil)

	n := &OperatorLimit{
		baseOperator: baseOperator{id: 1},
	}

	kernel, err := kr.resolveLimit(n)

	assert.NoError(t, err)
	assert.IsType(t, &KernelLimit{}, kernel)
}

func TestResolveConcat(t *testing.T) {
	kr := NewKernelResolver(nil, nil, nil, nil)

	n := &OperatorConcat{
		baseOperator: baseOperator{id: 1},
	}

	kernel, err := kr.resolveConcat(n)

	assert.NoError(t, err)
	assert.IsType(t, &KernelSecretConcat{}, kernel)
}

func TestResolveConstant(t *testing.T) {
	kr := NewKernelResolver(nil, nil, nil, nil)

	n := &OperatorConstant{
		baseOperator: baseOperator{id: 1},
	}

	kernel, err := kr.resolveConstant(n)

	assert.NoError(t, err)
	assert.IsType(t, &KernelConstant{}, kernel)
}

func TestResolveGroupAgg(t *testing.T) {
	tensorMetaManager := NewTensorMetaManager()
	srm := NewSecurityRelaxationManager(&GlobalSecurityRelaxation{RevealGroupCount: false, RevealGroupMark: false}, nil)
	vt := NewVisibilityTable([]string{"alice", "bob", "carol"})
	tm := NewTensorManager(1)
	kr := NewKernelResolver(srm, vt, tm, nil)

	createPlacedTensor := func(name string, place tensorPlacement, vis []string) (*TensorMeta, *graph.Tensor) {
		meta := tensorMetaManager.CreateTensorMeta(name, graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64))
		vt.UpdateVisibility(meta, NewVisibleParties(vis))
		tensor, _ := tm.CreateAndSetFirstTensor(meta, place)
		return meta, tensor
	}

	createAggFunc := func(name string) *aggregation.AggFuncDesc {
		agg := &aggregation.AggFuncDesc{}
		agg.Name = name
		return agg
	}

	tests := []struct {
		name                string
		revealGroupCount    bool
		revealGroupMark     bool
		setup               func() *OperatorGroupAgg
		wantKernelType      any
		wantPrivateParty    string
		wantPrivateSort     string
		wantRevealGroupMark bool
		wantPGSFuncNum      int
	}{
		{
			name: "private group agg with all tensors visible to same party",
			setup: func() *OperatorGroupAgg {
				groupKey, _ := createPlacedTensor("group_key", &privatePlacement{partyCode: "alice"}, []string{"alice"})
				payload, _ := createPlacedTensor("payload", &privatePlacement{partyCode: "alice"}, []string{"alice"})

				return &OperatorGroupAgg{
					baseOperator:    baseOperator{id: 1},
					groupKeys:       []*TensorMeta{groupKey},
					aggArgs:         []*TensorMeta{payload},
					aggFuncsWithArg: []*aggregation.AggFuncDesc{createAggFunc(ast.AggFuncSum)},
				}
			},
			wantKernelType:   &KernelPrivateGroupAgg{},
			wantPrivateParty: "alice",
			wantPGSFuncNum:   0,
		},
		{
			name: "private group agg with secret sum optimization",
			setup: func() *OperatorGroupAgg {
				groupKey, _ := createPlacedTensor("group_key", &privatePlacement{partyCode: "alice"}, []string{"alice"})
				payload, _ := createPlacedTensor("payload", &privatePlacement{partyCode: "bob"}, []string{"bob"})

				return &OperatorGroupAgg{
					baseOperator:    baseOperator{id: 2},
					groupKeys:       []*TensorMeta{groupKey},
					aggArgs:         []*TensorMeta{payload},
					aggFuncsWithArg: []*aggregation.AggFuncDesc{createAggFunc(ast.AggFuncSum)},
				}
			},
			wantKernelType:   &KernelPrivateGroupAgg{},
			wantPrivateParty: "alice",
			wantPGSFuncNum:   1,
		},
		{
			name:             "oblivious group agg when no common private party",
			revealGroupCount: false,
			setup: func() *OperatorGroupAgg {
				groupKey, _ := createPlacedTensor("group_key", &privatePlacement{partyCode: "alice"}, []string{"alice"})
				payload, _ := createPlacedTensor("payload", &privatePlacement{partyCode: "bob"}, []string{"bob"})

				return &OperatorGroupAgg{
					baseOperator:    baseOperator{id: 3},
					groupKeys:       []*TensorMeta{groupKey},
					aggArgs:         []*TensorMeta{payload},
					aggFuncsWithArg: []*aggregation.AggFuncDesc{createAggFunc(ast.AggFuncMax)},
				}
			},
			wantKernelType:      &KernelObliviousGroupAgg{},
			wantPrivateSort:     "",
			wantRevealGroupMark: false,
		},
		{
			name:             "oblivious group agg with reveal group count enabled",
			revealGroupCount: true,
			setup: func() *OperatorGroupAgg {
				groupKey1, _ := createPlacedTensor("group_key1", &privatePlacement{partyCode: "alice"}, []string{"alice"})
				groupKey2, _ := createPlacedTensor("group_key2", &privatePlacement{partyCode: "alice"}, []string{"alice"})
				payload, _ := createPlacedTensor("payload", &privatePlacement{partyCode: "bob"}, []string{"bob"})

				return &OperatorGroupAgg{
					baseOperator:    baseOperator{id: 4},
					groupKeys:       []*TensorMeta{groupKey1, groupKey2},
					aggArgs:         []*TensorMeta{payload},
					aggFuncsWithArg: []*aggregation.AggFuncDesc{createAggFunc(ast.AggFuncMin)},
				}
			},
			wantKernelType:      &KernelObliviousGroupAgg{},
			wantPrivateSort:     "alice",
			wantRevealGroupMark: false,
		},
		{
			name:            "oblivious group agg with reveal group mark enabled",
			revealGroupMark: true,
			setup: func() *OperatorGroupAgg {
				groupKey, _ := createPlacedTensor("group_key", &privatePlacement{partyCode: "alice"}, []string{"alice"})
				payload, _ := createPlacedTensor("payload", &privatePlacement{partyCode: "bob"}, []string{"bob"})

				return &OperatorGroupAgg{
					baseOperator:    baseOperator{id: 5},
					groupKeys:       []*TensorMeta{groupKey},
					aggArgs:         []*TensorMeta{payload},
					aggFuncsWithArg: []*aggregation.AggFuncDesc{createAggFunc(ast.AggFuncMax)},
				}
			},
			wantKernelType:      &KernelObliviousGroupAgg{},
			wantPrivateSort:     "",
			wantRevealGroupMark: true,
		},
		{
			name: "private group agg with multiple group keys",
			setup: func() *OperatorGroupAgg {
				groupKey1, _ := createPlacedTensor("group_key1", &privatePlacement{partyCode: "alice"}, []string{"alice"})
				groupKey2, _ := createPlacedTensor("group_key2", &privatePlacement{partyCode: "alice"}, []string{"alice"})
				payload, _ := createPlacedTensor("payload", &privatePlacement{partyCode: "alice"}, []string{"alice"})

				return &OperatorGroupAgg{
					baseOperator:    baseOperator{id: 6},
					groupKeys:       []*TensorMeta{groupKey1, groupKey2},
					aggArgs:         []*TensorMeta{payload},
					aggFuncsWithArg: []*aggregation.AggFuncDesc{createAggFunc(ast.AggFuncCount)},
				}
			},
			wantKernelType:   &KernelPrivateGroupAgg{},
			wantPrivateParty: "alice",
			wantPGSFuncNum:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset for each test
			tensorMetaManager = NewTensorMetaManager()
			tm = NewTensorManager(1)
			vt = NewVisibilityTable([]string{"alice", "bob", "carol"})
			srm = NewSecurityRelaxationManager(&GlobalSecurityRelaxation{RevealGroupCount: tt.revealGroupCount, RevealGroupMark: tt.revealGroupMark}, nil)
			kr = NewKernelResolver(srm, vt, tm, nil)

			n := tt.setup()
			kernel, err := kr.resolveGroupAgg(n)

			assert.NoError(t, err)
			assert.IsType(t, tt.wantKernelType, kernel)

			if privateKernel, ok := kernel.(*KernelPrivateGroupAgg); ok {
				assert.Equal(t, tt.wantPrivateParty, privateKernel.partyCode)
				assert.Equal(t, tt.wantPGSFuncNum, len(privateKernel.PGSParties))
			}

			if obliviousKernel, ok := kernel.(*KernelObliviousGroupAgg); ok {
				assert.Equal(t, tt.wantPrivateSort, obliviousKernel.privateSortParty)
				assert.Equal(t, tt.wantRevealGroupMark, obliviousKernel.revealGroupMark)
			}
		})
	}
}

func TestResolveFilter(t *testing.T) {
	tensorMetaManager := NewTensorMetaManager()
	srm := NewSecurityRelaxationManager(&GlobalSecurityRelaxation{}, nil)
	vt := NewVisibilityTable([]string{"alice", "bob", "carol"})
	tm := NewTensorManager(1)
	kr := NewKernelResolver(srm, vt, tm, nil)

	createPlacedTensor := func(name string, place tensorPlacement, vis []string) (*TensorMeta, *graph.Tensor) {
		meta := tensorMetaManager.CreateTensorMeta(name, graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64))
		vt.UpdateVisibility(meta, NewVisibleParties(vis))
		tensor, _ := tm.CreateAndSetFirstTensor(meta, place)
		return meta, tensor
	}

	tests := []struct {
		name            string
		setup           func() *OperatorFilter
		wantKernelType  any
		wantErr         bool
		wantErrMsg      string
		checkPlacements func([]tensorPlacement)
	}{
		{
			name: "all tensors visible to same party",
			setup: func() *OperatorFilter {
				mask, _ := createPlacedTensor("mask", &privatePlacement{partyCode: "alice"}, []string{"alice"})
				input, _ := createPlacedTensor("input", &privatePlacement{partyCode: "alice"}, []string{"alice"})

				return &OperatorFilter{
					baseOperator: baseOperator{id: 1},
					mask:         mask,
					inputs:       []*TensorMeta{input},
				}
			},
			wantKernelType: &KernelFilter{},
			checkPlacements: func(placements []tensorPlacement) {
				require.Len(t, placements, 1)
				privatePlace, ok := placements[0].(*privatePlacement)
				assert.True(t, ok)
				assert.Equal(t, "alice", privatePlace.partyCode)
			},
		},
		{
			name: "secret input with public filter",
			setup: func() *OperatorFilter {
				mask, _ := createPlacedTensor("mask", &publicPlacement{}, []string{"alice", "bob", "carol"})
				input, _ := createPlacedTensor("input", &secretPlacement{}, []string{})

				return &OperatorFilter{
					baseOperator: baseOperator{id: 2},
					mask:         mask,
					inputs:       []*TensorMeta{input},
				}
			},
			wantKernelType: &KernelFilter{},
			checkPlacements: func(placements []tensorPlacement) {
				require.Len(t, placements, 1)
				_, ok := placements[0].(*secretPlacement)
				assert.True(t, ok)
			},
		},
		{
			name: "no common visibility between filter and input",
			setup: func() *OperatorFilter {
				mask, _ := createPlacedTensor("mask", &privatePlacement{partyCode: "alice"}, []string{"alice"})
				input, _ := createPlacedTensor("input", &privatePlacement{partyCode: "bob"}, []string{"bob"})

				return &OperatorFilter{
					baseOperator: baseOperator{id: 3},
					mask:         mask,
					inputs:       []*TensorMeta{input},
				}
			},
			wantErr:    true,
			wantErrMsg: "resolveFilter: can not find proper party",
		},
		{
			name: "multiple inputs with intersection visibility",
			setup: func() *OperatorFilter {
				mask, _ := createPlacedTensor("mask", &privatePlacement{partyCode: "alice"}, []string{"alice", "bob"})
				input1, _ := createPlacedTensor("input1", &privatePlacement{partyCode: "alice"}, []string{"alice", "carol"})
				input2, _ := createPlacedTensor("input2", &privatePlacement{partyCode: "bob"}, []string{"bob", "carol"})

				return &OperatorFilter{
					baseOperator: baseOperator{id: 4},
					mask:         mask,
					inputs:       []*TensorMeta{input1, input2},
				}
			},
			wantKernelType: &KernelFilter{},
			checkPlacements: func(placements []tensorPlacement) {
				require.Len(t, placements, 2)

				// Check that each placement is a private placement with the expected party
				alicePlace, ok1 := placements[0].(*privatePlacement)
				assert.True(t, ok1)
				assert.Equal(t, "alice", alicePlace.partyCode)

				bobPlace, ok2 := placements[1].(*privatePlacement)
				assert.True(t, ok2)
				assert.Equal(t, "bob", bobPlace.partyCode)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset for each test
			tensorMetaManager = NewTensorMetaManager()
			tm = NewTensorManager(1)
			vt = NewVisibilityTable([]string{"alice", "bob", "carol"})
			srm = NewSecurityRelaxationManager(&GlobalSecurityRelaxation{}, nil)
			kr = NewKernelResolver(srm, vt, tm, nil)

			n := tt.setup()
			kernel, err := kr.resolveFilter(n)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrMsg)
				return
			}

			require.NoError(t, err)
			assert.IsType(t, tt.wantKernelType, kernel)

			if tt.checkPlacements != nil {
				filterKernel, ok := kernel.(*KernelFilter)
				require.True(t, ok)
				tt.checkPlacements(filterKernel.inputPlacements)
			}
		})
	}
}

func TestResolveReduce(t *testing.T) {
	tensorMetaManager := NewTensorMetaManager()
	scm := NewSecurityRelaxationManager(&GlobalSecurityRelaxation{}, nil)
	vt := NewVisibilityTable([]string{"alice", "bob", "carol"})
	tm := NewTensorManager(1)
	kr := NewKernelResolver(scm, vt, tm, nil)

	createPlacedTensor := func(name string, place tensorPlacement, vis []string) (*TensorMeta, *graph.Tensor) {
		meta := tensorMetaManager.CreateTensorMeta(name, graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64))
		vt.UpdateVisibility(meta, NewVisibleParties(vis))
		tensor, _ := tm.CreateAndSetFirstTensor(meta, place)
		return meta, tensor
	}

	createAggFunc := func(name string, mode aggregation.AggFunctionMode, hasDistinct bool) *aggregation.AggFuncDesc {
		agg := &aggregation.AggFuncDesc{}
		agg.Name = name
		agg.Mode = mode
		agg.HasDistinct = hasDistinct
		return agg
	}

	tests := []struct {
		name           string
		setup          func() *OperatorReduce
		wantKernelType any
		wantErr        bool
		wantErrMsg     string
		checkKernel    func(Kernel)
	}{
		{
			name: "simple reduce SUM with private input",
			setup: func() *OperatorReduce {
				input, _ := createPlacedTensor("input", &privatePlacement{partyCode: "alice"}, []string{"alice"})
				aggFunc := createAggFunc(ast.AggFuncSum, aggregation.CompleteMode, false)

				return &OperatorReduce{
					baseOperator: baseOperator{id: 1},
					input:        input,
					aggFunc:      aggFunc,
				}
			},
			wantKernelType: &KernelSimpleReduce{},
			checkKernel: func(k Kernel) {
				reduceKernel := k.(*KernelSimpleReduce)
				assert.Equal(t, ast.AggFuncSum, reduceKernel.aggFunc.Name)
				privatePlace, ok := reduceKernel.placement.(*privatePlacement)
				assert.True(t, ok)
				assert.Equal(t, "alice", privatePlace.partyCode)
			},
		},
		{
			name: "simple reduce SUM with secret input",
			setup: func() *OperatorReduce {
				input, _ := createPlacedTensor("input", &secretPlacement{}, []string{})
				aggFunc := createAggFunc(ast.AggFuncSum, aggregation.CompleteMode, false)

				return &OperatorReduce{
					baseOperator: baseOperator{id: 2},
					input:        input,
					aggFunc:      aggFunc,
				}
			},
			wantKernelType: &KernelSimpleReduce{},
			checkKernel: func(k Kernel) {
				reduceKernel := k.(*KernelSimpleReduce)
				assert.Equal(t, ast.AggFuncSum, reduceKernel.aggFunc.Name)
				_, ok := reduceKernel.placement.(*secretPlacement)
				assert.True(t, ok)
			},
		},
		{
			name: "COUNT DISTINCT with private input",
			setup: func() *OperatorReduce {
				input, _ := createPlacedTensor("input", &privatePlacement{partyCode: "alice"}, []string{"alice"})
				aggFunc := createAggFunc(ast.AggFuncCount, aggregation.CompleteMode, true)

				return &OperatorReduce{
					baseOperator: baseOperator{id: 3},
					input:        input,
					aggFunc:      aggFunc,
				}
			},
			wantKernelType: &KernelPrivateDistinctCount{},
			checkKernel: func(k Kernel) {
				distinctCount := k.(*KernelPrivateDistinctCount)
				assert.Equal(t, "alice", distinctCount.partyCode)
			},
		},
		{
			name: "COUNT DISTINCT with secret input",
			setup: func() *OperatorReduce {
				input, _ := createPlacedTensor("input", &secretPlacement{}, []string{})
				aggFunc := createAggFunc(ast.AggFuncCount, aggregation.CompleteMode, true)

				return &OperatorReduce{
					baseOperator: baseOperator{id: 4},
					input:        input,
					aggFunc:      aggFunc,
				}
			},
			wantKernelType: &KernelSecretDistinctCount{},
		},
		{
			name: "unsupported aggregation function",
			setup: func() *OperatorReduce {
				input, _ := createPlacedTensor("input", &privatePlacement{partyCode: "alice"}, []string{"alice"})
				aggFunc := createAggFunc("unsupported", aggregation.CompleteMode, false)

				return &OperatorReduce{
					baseOperator: baseOperator{id: 5},
					input:        input,
					aggFunc:      aggFunc,
				}
			},
			wantErr:    true,
			wantErrMsg: "resolveReduce: unsupported aggregation function",
		},
		{
			name: "unsupported aggregation mode",
			setup: func() *OperatorReduce {
				input, _ := createPlacedTensor("input", &privatePlacement{partyCode: "alice"}, []string{"alice"})
				aggFunc := createAggFunc(ast.AggFuncCount, aggregation.Partial1Mode, false)

				return &OperatorReduce{
					baseOperator: baseOperator{id: 6},
					input:        input,
					aggFunc:      aggFunc,
				}
			},
			wantErr:    true,
			wantErrMsg: "resolveReduce: unsupported aggregation mode",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset for each test
			tensorMetaManager = NewTensorMetaManager()
			tm = NewTensorManager(1)
			vt = NewVisibilityTable([]string{"alice", "bob", "carol"})
			scm = NewSecurityRelaxationManager(&GlobalSecurityRelaxation{}, nil)
			kr = NewKernelResolver(scm, vt, tm, nil)

			n := tt.setup()
			kernel, err := kr.resolveReduce(n)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrMsg)
				return
			}

			require.NoError(t, err)
			assert.IsType(t, tt.wantKernelType, kernel)

			if tt.checkKernel != nil {
				tt.checkKernel(kernel)
			}
		})
	}
}

func TestResolveBroadcastTo(t *testing.T) {
	tensorMetaManager := NewTensorMetaManager()
	scm := NewSecurityRelaxationManager(&GlobalSecurityRelaxation{}, nil)
	vt := NewVisibilityTable([]string{"alice", "bob", "carol"})
	tm := NewTensorManager(1)
	kr := NewKernelResolver(scm, vt, tm, nil)

	createPlacedTensor := func(name string, place tensorPlacement, vis []string) (*TensorMeta, *graph.Tensor) {
		meta := tensorMetaManager.CreateTensorMeta(name, graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64))
		vt.UpdateVisibility(meta, NewVisibleParties(vis))
		tensor, _ := tm.CreateAndSetFirstTensor(meta, place)
		return meta, tensor
	}

	tests := []struct {
		name           string
		setup          func() *OperatorBroadcastTo
		wantKernelType any
		wantErr        bool
		wantErrMsg     string
		checkKernel    func(Kernel)
	}{
		{
			name: "broadcast with private shapeRef",
			setup: func() *OperatorBroadcastTo {
				shapeRef, _ := createPlacedTensor("shapeRef", &privatePlacement{partyCode: "alice"}, []string{"alice"})
				scalar1, _ := createPlacedTensor("scalar1", &publicPlacement{}, []string{"alice", "bob", "carol"})
				scalar2, _ := createPlacedTensor("scalar2", &publicPlacement{}, []string{"alice", "bob", "carol"})

				return &OperatorBroadcastTo{
					baseOperator: baseOperator{id: 1},
					shapeRef:     shapeRef,
					scalars:      []*TensorMeta{scalar1, scalar2},
				}
			},
			wantKernelType: &KernelBroadcastTo{},
			checkKernel: func(k Kernel) {
				broadcastKernel := k.(*KernelBroadcastTo)
				privatePlace, ok := broadcastKernel.refPlacement.(*privatePlacement)
				assert.True(t, ok)
				assert.Equal(t, "alice", privatePlace.partyCode)
			},
		},
		{
			name: "broadcast with secret shapeRef",
			setup: func() *OperatorBroadcastTo {
				shapeRef, _ := createPlacedTensor("shapeRef", &secretPlacement{}, []string{"alice", "bob"})
				scalar, _ := createPlacedTensor("scalar", &publicPlacement{}, []string{"alice", "bob", "carol"})

				return &OperatorBroadcastTo{
					baseOperator: baseOperator{id: 2},
					shapeRef:     shapeRef,
					scalars:      []*TensorMeta{scalar},
				}
			},
			wantKernelType: &KernelBroadcastTo{},
			checkKernel: func(k Kernel) {
				broadcastKernel := k.(*KernelBroadcastTo)
				_, ok := broadcastKernel.refPlacement.(*secretPlacement)
				assert.True(t, ok)
			},
		},
		{
			name: "non-public scalar error",
			setup: func() *OperatorBroadcastTo {
				shapeRef, _ := createPlacedTensor("shapeRef", &privatePlacement{partyCode: "alice"}, []string{"alice"})
				scalar, _ := createPlacedTensor("scalar", &privatePlacement{partyCode: "bob"}, []string{"bob"})

				return &OperatorBroadcastTo{
					baseOperator: baseOperator{id: 3},
					shapeRef:     shapeRef,
					scalars:      []*TensorMeta{scalar},
				}
			},
			wantErr:    true,
			wantErrMsg: "resolveBroadcastTo: scalar",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset for each test
			tensorMetaManager = NewTensorMetaManager()
			tm = NewTensorManager(1)
			vt = NewVisibilityTable([]string{"alice", "bob", "carol"})
			scm = NewSecurityRelaxationManager(&GlobalSecurityRelaxation{}, nil)
			kr = NewKernelResolver(scm, vt, tm, nil)

			n := tt.setup()
			kernel, err := kr.resolveBroadcastTo(n)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrMsg)
				return
			}

			require.NoError(t, err)
			assert.IsType(t, tt.wantKernelType, kernel)

			if tt.checkKernel != nil {
				tt.checkKernel(kernel)
			}
		})
	}
}

func TestResolveCrossJoin(t *testing.T) {
	tensorMetaManager := NewTensorMetaManager()
	srm := NewSecurityRelaxationManager(&GlobalSecurityRelaxation{}, nil)
	vt := NewVisibilityTable([]string{"alice", "bob", "carol"})
	tm := NewTensorManager(1)
	kr := NewKernelResolver(srm, vt, tm, nil)

	createPlacedTensor := func(name string, place tensorPlacement, vis []string) (*TensorMeta, *graph.Tensor) {
		meta := tensorMetaManager.CreateTensorMeta(name, graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64))
		vt.UpdateVisibility(meta, NewVisibleParties(vis))
		tensor, _ := tm.CreateAndSetFirstTensor(meta, place)
		return meta, tensor
	}

	tests := []struct {
		name           string
		setup          func() *OperatorCrossJoin
		wantKernelType any
		wantLeftParty  string
		wantRightParty string
		wantErr        bool
		wantErrMsg     string
	}{
		{
			name: "cross join with valid party pair",
			setup: func() *OperatorCrossJoin {
				leftInput, _ := createPlacedTensor("left_input", &privatePlacement{partyCode: "alice"}, []string{"alice"})
				rightInput, _ := createPlacedTensor("right_input", &privatePlacement{partyCode: "bob"}, []string{"bob"})

				return &OperatorCrossJoin{
					baseOperator: baseOperator{id: 1},
					leftInputs:   []*TensorMeta{leftInput},
					rightInputs:  []*TensorMeta{rightInput},
					leftOutputs:  []*TensorMeta{{ID: 101}},
					rightOutputs: []*TensorMeta{{ID: 102}},
				}
			},
			wantKernelType: &KernelCrossJoin{},
			wantLeftParty:  "alice",
			wantRightParty: "bob",
		},
		{
			name: "cross join with existing placed tensors",
			setup: func() *OperatorCrossJoin {
				leftInput, _ := createPlacedTensor("left_input", &privatePlacement{partyCode: "alice"}, []string{"alice", "bob"})
				rightInput, _ := createPlacedTensor("right_input", &privatePlacement{partyCode: "bob"}, []string{"alice", "bob"})

				// Create additional existing tensors
				leftInput2, _ := createPlacedTensor("left_input2", &privatePlacement{partyCode: "bob"}, []string{"alice", "bob"})
				rightInput2, _ := createPlacedTensor("right_input2", &privatePlacement{partyCode: "alice"}, []string{"alice", "bob"})
				leftInput3, _ := createPlacedTensor("left_input3", &privatePlacement{partyCode: "bob"}, []string{"alice", "bob"})
				rightInput3, _ := createPlacedTensor("right_input3", &privatePlacement{partyCode: "alice"}, []string{"alice", "bob"})

				return &OperatorCrossJoin{
					baseOperator: baseOperator{id: 2},
					leftInputs:   []*TensorMeta{leftInput, leftInput2, leftInput3},
					rightInputs:  []*TensorMeta{rightInput, rightInput2, rightInput3},
					leftOutputs:  []*TensorMeta{{ID: 201}},
					rightOutputs: []*TensorMeta{{ID: 202}},
				}
			},
			wantKernelType: &KernelCrossJoin{},
			wantLeftParty:  "bob",
			wantRightParty: "alice",
		},
		{
			name: "cross join single party",
			setup: func() *OperatorCrossJoin {
				leftInput, _ := createPlacedTensor("left_input", &privatePlacement{partyCode: "alice"}, []string{"alice"})
				rightInput, _ := createPlacedTensor("right_input", &privatePlacement{partyCode: "alice"}, []string{"alice"})

				return &OperatorCrossJoin{
					baseOperator: baseOperator{id: 3},
					leftInputs:   []*TensorMeta{leftInput},
					rightInputs:  []*TensorMeta{rightInput},
					leftOutputs:  []*TensorMeta{{ID: 301}},
					rightOutputs: []*TensorMeta{{ID: 302}},
				}
			},
			wantKernelType: &KernelCrossJoin{},
			wantLeftParty:  "alice",
			wantRightParty: "alice",
		},
		{
			name: "cross join with single party",
			setup: func() *OperatorCrossJoin {
				leftInput, _ := createPlacedTensor("left_input", &privatePlacement{partyCode: "alice"}, []string{"alice"})
				rightInput, _ := createPlacedTensor("right_input", &privatePlacement{partyCode: "alice"}, []string{"alice"})

				return &OperatorCrossJoin{
					baseOperator: baseOperator{id: 4},
					leftInputs:   []*TensorMeta{leftInput},
					rightInputs:  []*TensorMeta{rightInput},
					leftOutputs:  []*TensorMeta{{ID: 401}},
					rightOutputs: []*TensorMeta{{ID: 402}},
				}
			},
			wantKernelType: &KernelCrossJoin{},
			wantLeftParty:  "alice",
			wantRightParty: "alice",
		},
		{
			name: "cross join with multiple parties choosing best pair",
			setup: func() *OperatorCrossJoin {
				leftInput, _ := createPlacedTensor("left_input", &privatePlacement{partyCode: "alice"}, []string{"alice", "carol"})
				rightInput, _ := createPlacedTensor("right_input", &privatePlacement{partyCode: "bob"}, []string{"bob", "carol"})

				// Create existing tensors to influence heuristic
				leftInput2, _ := createPlacedTensor("left_input2", &privatePlacement{partyCode: "alice"}, []string{"alice", "carol"})
				rightInput2, _ := createPlacedTensor("right_input2", &privatePlacement{partyCode: "bob"}, []string{"bob", "carol"})

				return &OperatorCrossJoin{
					baseOperator: baseOperator{id: 5},
					leftInputs:   []*TensorMeta{leftInput, leftInput2},
					rightInputs:  []*TensorMeta{rightInput, rightInput2},
					leftOutputs:  []*TensorMeta{{ID: 501}},
					rightOutputs: []*TensorMeta{{ID: 502}},
				}
			},
			wantKernelType: &KernelCrossJoin{},
			wantLeftParty:  "alice",
			wantRightParty: "bob",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset for each test
			tensorMetaManager = NewTensorMetaManager()
			tm = NewTensorManager(1)
			vt = NewVisibilityTable([]string{"alice", "bob", "carol"})
			srm = NewSecurityRelaxationManager(&GlobalSecurityRelaxation{}, nil)
			kr = NewKernelResolver(srm, vt, tm, nil)

			n := tt.setup()
			kernel, err := kr.resolveCrossJoin(n)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrMsg)
				return
			}

			require.NoError(t, err)
			assert.IsType(t, tt.wantKernelType, kernel)

			if crossJoinKernel, ok := kernel.(*KernelCrossJoin); ok {
				assert.Equal(t, tt.wantLeftParty, crossJoinKernel.leftParty)
				assert.Equal(t, tt.wantRightParty, crossJoinKernel.rightParty)
			}
		})
	}
}

func TestResolveSort(t *testing.T) {
	tensorMetaManager := NewTensorMetaManager()
	srm := NewSecurityRelaxationManager(&GlobalSecurityRelaxation{}, nil)
	vt := NewVisibilityTable([]string{"alice", "bob", "carol"})
	tm := NewTensorManager(1)
	kr := NewKernelResolver(srm, vt, tm, nil)

	createPlacedTensor := func(name string, place tensorPlacement, vis []string) (*TensorMeta, *graph.Tensor) {
		meta := tensorMetaManager.CreateTensorMeta(name, graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64))
		vt.UpdateVisibility(meta, NewVisibleParties(vis))
		tensor, _ := tm.CreateAndSetFirstTensor(meta, place)
		return meta, tensor
	}

	tests := []struct {
		name           string
		setup          func() *OperatorSort
		wantKernelType any
		wantParty      string
	}{
		{
			name: "sort with all tensors visible to same party",
			setup: func() *OperatorSort {
				sortKey, _ := createPlacedTensor("sort_key", &privatePlacement{partyCode: "alice"}, []string{"alice"})
				payload, _ := createPlacedTensor("payload", &privatePlacement{partyCode: "alice"}, []string{"alice"})

				return &OperatorSort{
					baseOperator: baseOperator{id: 1},
					sortKeys:     []*TensorMeta{sortKey},
					payloads:     []*TensorMeta{payload},
					outputs:      []*TensorMeta{{ID: 101}},
					descending:   []bool{false},
				}
			},
			wantKernelType: &KernelPrivateSort{},
			wantParty:      "alice",
		},
		{
			name: "sort with secret inputs",
			setup: func() *OperatorSort {
				sortKey, _ := createPlacedTensor("sort_key", &secretPlacement{}, []string{})
				payload, _ := createPlacedTensor("payload", &secretPlacement{}, []string{})

				return &OperatorSort{
					baseOperator: baseOperator{id: 3},
					sortKeys:     []*TensorMeta{sortKey},
					payloads:     []*TensorMeta{payload},
					outputs:      []*TensorMeta{{ID: 301}},
					descending:   []bool{false},
				}
			},
			wantKernelType: &KernelSecretSort{},
		},
		{
			name: "sort with public inputs - secret placement",
			setup: func() *OperatorSort {
				sortKey, _ := createPlacedTensor("sort_key", &publicPlacement{}, []string{"alice", "bob", "carol"})
				payload, _ := createPlacedTensor("payload", &publicPlacement{}, []string{"alice", "bob", "carol"})

				return &OperatorSort{
					baseOperator: baseOperator{id: 4},
					sortKeys:     []*TensorMeta{sortKey},
					payloads:     []*TensorMeta{payload},
					outputs:      []*TensorMeta{{ID: 401}},
					descending:   []bool{false},
				}
			},
			wantKernelType: &KernelPrivateSort{},
			wantParty:      "alice",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset for each test
			tensorMetaManager = NewTensorMetaManager()
			tm = NewTensorManager(1)
			vt = NewVisibilityTable([]string{"alice", "bob", "carol"})
			srm = NewSecurityRelaxationManager(&GlobalSecurityRelaxation{}, nil)
			kr = NewKernelResolver(srm, vt, tm, nil)

			n := tt.setup()
			kernel, err := kr.resolveSort(n)

			require.NoError(t, err)
			assert.IsType(t, tt.wantKernelType, kernel)

			if privateSortKernel, ok := kernel.(*KernelPrivateSort); ok {
				assert.Equal(t, tt.wantParty, privateSortKernel.partyCode)
			}
		})
	}
}

func TestResolveIn(t *testing.T) {
	tensorMetaManager := NewTensorMetaManager()
	srm := NewSecurityRelaxationManager(&GlobalSecurityRelaxation{}, nil)
	vt := NewVisibilityTable([]string{"alice", "bob", "carol"})
	tm := NewTensorManager(1)
	kr := NewKernelResolver(srm, vt, tm, nil)

	createPlacedTensor := func(name string, place tensorPlacement, vis []string) (*TensorMeta, *graph.Tensor) {
		meta := tensorMetaManager.CreateTensorMeta(name, graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64))
		vt.UpdateVisibility(meta, NewVisibleParties(vis))
		tensor, _ := tm.CreateAndSetFirstTensor(meta, place)
		return meta, tensor
	}

	tests := []struct {
		name           string
		setup          func() *OperatorIn
		wantKernelType any
		wantLeftParty  string
		wantRightParty string
		wantParty      string
		wantErr        bool
		wantErrMsg     string
	}{
		{
			name: "local in with common party",
			setup: func() *OperatorIn {
				left, _ := createPlacedTensor("left", &privatePlacement{partyCode: "alice"}, []string{"alice"})
				right, _ := createPlacedTensor("right", &privatePlacement{partyCode: "alice"}, []string{"alice"})

				return &OperatorIn{
					baseOperator: baseOperator{id: 1},
					left:         left,
					right:        right,
				}
			},
			wantKernelType: &KernelLocalIn{},
			wantParty:      "alice",
		},
		{
			name: "psi in with different parties",
			setup: func() *OperatorIn {
				left, _ := createPlacedTensor("left", &privatePlacement{partyCode: "alice"}, []string{"alice"})
				right, _ := createPlacedTensor("right", &privatePlacement{partyCode: "bob"}, []string{"bob"})

				return &OperatorIn{
					baseOperator: baseOperator{id: 2},
					left:         left,
					right:        right,
				}
			},
			wantKernelType: &KernelPsiIn{},
			wantLeftParty:  "alice",
			wantRightParty: "bob",
		},
		{
			name: "in with secret inputs",
			setup: func() *OperatorIn {
				left, _ := createPlacedTensor("left", &secretPlacement{}, []string{})
				right, _ := createPlacedTensor("right", &secretPlacement{}, []string{})

				return &OperatorIn{
					baseOperator: baseOperator{id: 4},
					left:         left,
					right:        right,
				}
			},
			wantErr:    true,
			wantErrMsg: "resolveIn: no valid kernel",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset for each test
			tensorMetaManager = NewTensorMetaManager()
			tm = NewTensorManager(1)
			vt = NewVisibilityTable([]string{"alice", "bob", "carol"})
			srm = NewSecurityRelaxationManager(&GlobalSecurityRelaxation{}, nil)
			kr = NewKernelResolver(srm, vt, tm, nil)

			n := tt.setup()
			kernel, err := kr.resolveIn(n)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrMsg)
				return
			}

			require.NoError(t, err)
			assert.IsType(t, tt.wantKernelType, kernel)

			switch k := kernel.(type) {
			case *KernelLocalIn:
				assert.Equal(t, tt.wantParty, k.partyCode)
			case *KernelPsiIn:
				assert.Equal(t, tt.wantLeftParty, k.leftParty)
				assert.Equal(t, tt.wantRightParty, k.rightParty)
			}
		})
	}
}

func TestResolveWindow(t *testing.T) {
	tensorMetaManager := NewTensorMetaManager()
	srm := NewSecurityRelaxationManager(&GlobalSecurityRelaxation{}, nil)
	vt := NewVisibilityTable([]string{"alice", "bob", "carol"})
	tm := NewTensorManager(1)
	kr := NewKernelResolver(srm, vt, tm, nil)

	createPlacedTensor := func(name string, place tensorPlacement, vis []string) (*TensorMeta, *graph.Tensor) {
		meta := tensorMetaManager.CreateTensorMeta(name, graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64))
		vt.UpdateVisibility(meta, NewVisibleParties(vis))
		tensor, _ := tm.CreateAndSetFirstTensor(meta, place)
		return meta, tensor
	}

	tests := []struct {
		name           string
		setup          func() *OperatorWindow
		wantKernelType any
		wantParty      string
	}{
		{
			name: "private window with common party",
			setup: func() *OperatorWindow {
				partitionKey, _ := createPlacedTensor("partition_key", &privatePlacement{partyCode: "alice"}, []string{"alice"})
				orderKey, _ := createPlacedTensor("order_key", &privatePlacement{partyCode: "alice"}, []string{"alice"})

				return &OperatorWindow{
					baseOperator:  baseOperator{id: 1},
					partitionKeys: []*TensorMeta{partitionKey},
					orderKeys:     []*TensorMeta{orderKey},
				}
			},
			wantKernelType: &KernelPrivateWindow{},
			wantParty:      "alice",
		},
		{
			name: "oblivious window with no common party",
			setup: func() *OperatorWindow {
				partitionKey, _ := createPlacedTensor("partition_key", &secretPlacement{}, []string{"alice"})
				orderKey, _ := createPlacedTensor("order_key", &secretPlacement{}, []string{})

				return &OperatorWindow{
					baseOperator:  baseOperator{id: 2},
					partitionKeys: []*TensorMeta{partitionKey},
					orderKeys:     []*TensorMeta{orderKey},
				}
			},
			wantKernelType: &KernelObliviousWindow{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset for each test
			tensorMetaManager = NewTensorMetaManager()
			tm = NewTensorManager(1)
			vt = NewVisibilityTable([]string{"alice", "bob", "carol"})
			srm = NewSecurityRelaxationManager(&GlobalSecurityRelaxation{}, nil)
			kr = NewKernelResolver(srm, vt, tm, nil)

			n := tt.setup()
			kernel, err := kr.resolveWindow(n)

			require.NoError(t, err)
			assert.IsType(t, tt.wantKernelType, kernel)

			if privateWindowKernel, ok := kernel.(*KernelPrivateWindow); ok {
				assert.Equal(t, tt.wantParty, privateWindowKernel.partyCode)
			}
		})
	}
}

func TestResolveFunction(t *testing.T) {
	tensorMetaManager := NewTensorMetaManager()
	srm := NewSecurityRelaxationManager(&GlobalSecurityRelaxation{}, nil)
	vt := NewVisibilityTable([]string{"alice", "bob", "carol"})
	tm := NewTensorManager(1)
	kr := NewKernelResolver(srm, vt, tm, nil)

	createPlacedTensor := func(name string, place tensorPlacement, vis []string) (*TensorMeta, *graph.Tensor) {
		meta := tensorMetaManager.CreateTensorMeta(name, graph.NewPrimitiveDataType(proto.PrimitiveDataType_INT64))
		vt.UpdateVisibility(meta, NewVisibleParties(vis))
		tensor, _ := tm.CreateAndSetFirstTensor(meta, place)
		return meta, tensor
	}

	tests := []struct {
		name           string
		funcName       string
		setup          func() []*TensorMeta
		wantKernelType any
		wantParty      string
		wantErr        bool
		wantErrMsg     string
	}{
		// ArrowFunction cases
		{
			name:     "arrow function with private party",
			funcName: ast.Lower,
			setup: func() []*TensorMeta {
				input, _ := createPlacedTensor("input", &privatePlacement{partyCode: "alice"}, []string{"alice"})
				return []*TensorMeta{input}
			},
			wantKernelType: &KernelArrowFunction{},
			wantParty:      "alice",
		},
		{
			name:     "arrow function with no private party",
			funcName: ast.Upper,
			setup: func() []*TensorMeta {
				input, _ := createPlacedTensor("input", &secretPlacement{}, []string{})
				return []*TensorMeta{input}
			},
			wantErr:    true,
			wantErrMsg: "resolveFunction: could not find proper candidate",
		},

		// Unary cases
		{
			name:     "unary with private input",
			funcName: ast.Abs,
			setup: func() []*TensorMeta {
				input, _ := createPlacedTensor("input", &privatePlacement{partyCode: "bob"}, []string{"bob"})
				return []*TensorMeta{input}
			},
			wantKernelType: &KernelUnary{},
		},
		{
			name:     "unary with secret input",
			funcName: ast.Sqrt,
			setup: func() []*TensorMeta {
				input, _ := createPlacedTensor("input", &secretPlacement{}, []string{})
				return []*TensorMeta{input}
			},
			wantKernelType: &KernelUnary{},
		},

		// Binary cases
		{
			name:     "binary with common private party",
			funcName: ast.Plus,
			setup: func() []*TensorMeta {
				left, _ := createPlacedTensor("left", &privatePlacement{partyCode: "alice"}, []string{"alice"})
				right, _ := createPlacedTensor("right", &privatePlacement{partyCode: "alice"}, []string{"alice"})
				return []*TensorMeta{left, right}
			},
			wantKernelType: &KernelBinary{},
			wantParty:      "alice",
		},
		{
			name:     "binary with secret inputs",
			funcName: ast.Mul,
			setup: func() []*TensorMeta {
				left, _ := createPlacedTensor("left", &secretPlacement{}, []string{})
				right, _ := createPlacedTensor("right", &secretPlacement{}, []string{})
				return []*TensorMeta{left, right}
			},
			wantKernelType: &KernelBinary{},
		},

		// VariadicCompare cases
		{
			name:     "variadic compare with private party",
			funcName: ast.Greatest,
			setup: func() []*TensorMeta {
				input1, _ := createPlacedTensor("input1", &privatePlacement{partyCode: "alice"}, []string{"alice"})
				input2, _ := createPlacedTensor("input2", &privatePlacement{partyCode: "alice"}, []string{"alice"})
				return []*TensorMeta{input1, input2}
			},
			wantKernelType: &KernelVariadicCompare{},
			wantParty:      "alice",
		},
		{
			name:     "variadic compare with secret inputs",
			funcName: ast.Least,
			setup: func() []*TensorMeta {
				input1, _ := createPlacedTensor("input1", &secretPlacement{}, []string{})
				input2, _ := createPlacedTensor("input2", &secretPlacement{}, []string{})
				return []*TensorMeta{input1, input2}
			},
			wantKernelType: &KernelVariadicCompare{},
		},

		// PrivateFunc cases
		{
			name:     "private function with common party",
			funcName: ast.Ifnull,
			setup: func() []*TensorMeta {
				input1, _ := createPlacedTensor("input1", &privatePlacement{partyCode: "bob"}, []string{"bob"})
				input2, _ := createPlacedTensor("input2", &privatePlacement{partyCode: "bob"}, []string{"bob"})
				return []*TensorMeta{input1, input2}
			},
			wantKernelType: &KernelPrivateFunc{},
			wantParty:      "bob",
		},
		{
			name:     "private function with no common party",
			funcName: ast.IsNull,
			setup: func() []*TensorMeta {
				input1, _ := createPlacedTensor("input1", &secretPlacement{}, []string{})
				input2, _ := createPlacedTensor("input2", &secretPlacement{}, []string{})
				return []*TensorMeta{input1, input2}
			},
			wantErr:    true,
			wantErrMsg: "resolveFunction: could not find proper candidate",
		},

		// Cast cases
		{
			name:     "cast with private input",
			funcName: ast.Cast,
			setup: func() []*TensorMeta {
				input, _ := createPlacedTensor("input", &privatePlacement{partyCode: "alice"}, []string{"alice"})
				return []*TensorMeta{input}
			},
			wantKernelType: &KernelCast{},
		},
		{
			name:     "cast with secret input",
			funcName: ast.Cast,
			setup: func() []*TensorMeta {
				input, _ := createPlacedTensor("input", &secretPlacement{}, []string{})
				return []*TensorMeta{input}
			},
			wantErr:    true,
			wantErrMsg: "not support cast for string in spu",
		},

		// If cases
		{
			name:     "if with common private party",
			funcName: ast.If,
			setup: func() []*TensorMeta {
				cond, _ := createPlacedTensor("cond", &privatePlacement{partyCode: "alice"}, []string{"alice"})
				trueVal, _ := createPlacedTensor("true", &privatePlacement{partyCode: "alice"}, []string{"alice"})
				falseVal, _ := createPlacedTensor("false", &privatePlacement{partyCode: "alice"}, []string{"alice"})
				return []*TensorMeta{cond, trueVal, falseVal}
			},
			wantKernelType: &KernelIf{},
			wantParty:      "alice",
		},
		{
			name:     "if with secret inputs",
			funcName: ast.If,
			setup: func() []*TensorMeta {
				cond, _ := createPlacedTensor("cond", &secretPlacement{}, []string{})
				trueVal, _ := createPlacedTensor("true", &secretPlacement{}, []string{})
				falseVal, _ := createPlacedTensor("false", &secretPlacement{}, []string{})
				return []*TensorMeta{cond, trueVal, falseVal}
			},
			wantKernelType: &KernelIf{},
		},

		// Case cases
		{
			name:     "case with common private party",
			funcName: ast.Case,
			setup: func() []*TensorMeta {
				input1, _ := createPlacedTensor("input1", &privatePlacement{partyCode: "bob"}, []string{"bob"})
				input2, _ := createPlacedTensor("input2", &privatePlacement{partyCode: "bob"}, []string{"bob"})
				input3, _ := createPlacedTensor("input3", &privatePlacement{partyCode: "bob"}, []string{"bob"})
				return []*TensorMeta{input1, input2, input3}
			},
			wantKernelType: &KernelCase{},
			wantParty:      "bob",
		},
		{
			name:     "case with secret inputs",
			funcName: ast.Case,
			setup: func() []*TensorMeta {
				input1, _ := createPlacedTensor("input1", &secretPlacement{}, []string{})
				input2, _ := createPlacedTensor("input2", &secretPlacement{}, []string{})
				input3, _ := createPlacedTensor("input3", &secretPlacement{}, []string{})
				return []*TensorMeta{input1, input2, input3}
			},
			wantKernelType: &KernelCase{},
		},

		// Concat cases
		{
			name:     "concat with private party",
			funcName: ast.Concat,
			setup: func() []*TensorMeta {
				input1, _ := createPlacedTensor("input1", &privatePlacement{partyCode: "alice"}, []string{"alice"})
				input2, _ := createPlacedTensor("input2", &privatePlacement{partyCode: "alice"}, []string{"alice"})
				return []*TensorMeta{input1, input2}
			},
			wantKernelType: &KernelConcatString{},
			wantParty:      "alice",
		},
		{
			name:     "concat with no common party",
			funcName: ast.Concat,
			setup: func() []*TensorMeta {
				input1, _ := createPlacedTensor("input1", &secretPlacement{}, []string{})
				input2, _ := createPlacedTensor("input2", &secretPlacement{}, []string{})
				return []*TensorMeta{input1, input2}
			},
			wantErr:    true,
			wantErrMsg: "could not find proper candidate for string concat",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset for each test
			tensorMetaManager = NewTensorMetaManager()
			tm = NewTensorManager(1)
			vt = NewVisibilityTable([]string{"alice", "bob", "carol"})
			srm = NewSecurityRelaxationManager(&GlobalSecurityRelaxation{}, nil)
			kr = NewKernelResolver(srm, vt, tm, nil)

			inputs := tt.setup()
			n := &OperatorFunction{
				baseOperator: baseOperator{id: 1},
				funcName:     tt.funcName,
				inputs:       inputs,
			}

			if tt.funcName == ast.Cast {
				output := &TensorMeta{
					DType: graph.NewPrimitiveDataType(proto.PrimitiveDataType_STRING),
				}
				// output type need to be checked when resolving cast
				n.output = output
			}

			kernel, err := kr.resolveFunction(n)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrMsg)
				return
			}

			require.NoError(t, err)
			assert.IsType(t, tt.wantKernelType, kernel)

			switch k := kernel.(type) {
			case *KernelArrowFunction:
				assert.Equal(t, tt.wantParty, k.partyCode)
			case *KernelBinary:
				if tt.wantParty != "" {
					assert.Equal(t, &privatePlacement{partyCode: tt.wantParty}, k.lhsPlace)
					assert.Equal(t, &privatePlacement{partyCode: tt.wantParty}, k.rhsPlace)
				}
			case *KernelVariadicCompare:
				if tt.wantParty != "" {
					assert.Equal(t, &privatePlacement{partyCode: tt.wantParty}, k.placement)
				}
			case *KernelPrivateFunc:
				assert.Equal(t, tt.wantParty, k.partyCode)
			case *KernelIf:
				if tt.wantParty != "" {
					assert.Equal(t, &privatePlacement{partyCode: tt.wantParty}, k.condPlace)
				}
			case *KernelCase:
				if tt.wantParty != "" {
					assert.Equal(t, &privatePlacement{partyCode: tt.wantParty}, k.outputPlacement)
				}
			case *KernelConcatString:
				assert.Equal(t, tt.wantParty, k.partyCode)
			}
		})
	}
}
