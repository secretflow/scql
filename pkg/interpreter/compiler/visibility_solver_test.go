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
	"fmt"
	"testing"

	"github.com/secretflow/scql/pkg/expression"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/parser/model"
	"github.com/secretflow/scql/pkg/planner/core"
)

type mockOperator struct {
	baseOperator
	inputs         []*TensorMeta
	outputs        []*TensorMeta
	inferErr       error
	reverseErr     error
	updated        []*TensorMeta
	reverseUpdated []*TensorMeta
}

func (m *mockOperator) InferVis(vr VisibilityRegistry) error {
	return nil
}
func (m *mockOperator) InferCSR(csr ColumnSecurityRelaxation, tvc TensorVisibilityChecker) error {
	return nil
}
func (m *mockOperator) Infer(vs *VisibilitySolver, applySecurityRelaxation bool) ([]*TensorMeta, error) {
	for _, tensor := range m.updated {
		vs.vt.UpdateVisibility(tensor, vs.vt.PublicVisibility())
	}
	return m.updated, m.inferErr
}
func (m *mockOperator) ReverseInfer(vs *VisibilitySolver) ([]*TensorMeta, error) {
	for _, tensor := range m.reverseUpdated {
		vs.vt.UpdateVisibility(tensor, vs.vt.PublicVisibility())
	}
	return m.reverseUpdated, m.reverseErr
}
func (m *mockOperator) Inputs() map[string][]*TensorMeta {
	return map[string][]*TensorMeta{"input": m.inputs}
}
func (m *mockOperator) Outputs() map[string][]*TensorMeta {
	return map[string][]*TensorMeta{"output": m.outputs}
}
func (m *mockOperator) Attrs() map[string]any { return nil }
func (m *mockOperator) String() string        { return "" }

func TestVisibilitySolverSolve(t *testing.T) {

	tests := []struct {
		name    string
		setup   func() (*OperatorGraph, *VisibilityTable)
		reverse bool
		wantErr bool
	}{
		{
			name: "empty graph",
			setup: func() (*OperatorGraph, *VisibilityTable) {
				vt := NewVisibilityTable([]string{"alice", "bob"})
				graph := &OperatorGraph{}
				return graph, vt
			},
			reverse: false,
			wantErr: true,
		},
		{
			name: "forward only",
			setup: func() (*OperatorGraph, *VisibilityTable) {
				vt := NewVisibilityTable([]string{"alice", "bob"})

				producerTracker := &TensorProducerTracker{}
				consumerTracker := &TensorConsumerTracker{}

				tensor1 := &TensorMeta{ID: 1}
				vt.UpdateVisibility(tensor1, NewVisibleParties([]string{"bob"}))

				op1 := &mockOperator{outputs: []*TensorMeta{tensor1}}
				producerTracker.SetProducer(tensor1, op1)
				op1.updated = []*TensorMeta{tensor1}

				op2 := &OperatorResult{issuerPartyCode: "alice", resultTensors: []*TensorMeta{tensor1}}
				consumerTracker.AddConsumer(tensor1, op2)

				graph := &OperatorGraph{
					operators:       []Operator{op1, op2},
					producerTracker: producerTracker,
					consumerTracker: consumerTracker,
				}
				return graph, vt
			},
			reverse: false,
			wantErr: false,
		},
		{
			name: "reverse enabled",
			setup: func() (*OperatorGraph, *VisibilityTable) {
				vt := NewVisibilityTable([]string{"alice", "bob"})

				producerTracker := &TensorProducerTracker{}
				consumerTracker := &TensorConsumerTracker{}

				tensor1 := &TensorMeta{ID: 1}
				tensor2 := &TensorMeta{ID: 2}
				tensor3 := &TensorMeta{ID: 3}

				// Configure visibility for all tensors
				vt.UpdateVisibility(tensor1, NewVisibleParties([]string{"alice"}))
				vt.UpdateVisibility(tensor2, NewVisibleParties([]string{"bob"}))

				op1 := &mockOperator{outputs: []*TensorMeta{tensor1, tensor2}}
				producerTracker.SetProducer(tensor1, op1)
				producerTracker.SetProducer(tensor2, op1)

				op2 := &mockOperator{inputs: []*TensorMeta{tensor1, tensor2}, outputs: []*TensorMeta{tensor3}}
				producerTracker.SetProducer(tensor3, op2)
				consumerTracker.AddConsumer(tensor1, op2)
				consumerTracker.AddConsumer(tensor2, op2)
				op2.reverseUpdated = []*TensorMeta{tensor1}

				op3 := &OperatorResult{issuerPartyCode: "alice", resultTensors: []*TensorMeta{tensor3}}
				consumerTracker.AddConsumer(tensor3, op3)

				graph := &OperatorGraph{
					operators:       []Operator{op1, op2, op3},
					producerTracker: producerTracker,
					consumerTracker: consumerTracker,
				}
				return graph, vt
			},
			reverse: true,
			wantErr: false,
		},
		{
			name: "infer error",
			setup: func() (*OperatorGraph, *VisibilityTable) {
				vt := NewVisibilityTable([]string{"alice", "bob"})

				producerTracker := &TensorProducerTracker{}
				consumerTracker := &TensorConsumerTracker{}

				tensor1 := &TensorMeta{ID: 1}
				tensor2 := &TensorMeta{ID: 2}

				vt.UpdateVisibility(tensor1, NewVisibleParties([]string{"alice"}))

				op1 := &mockOperator{outputs: []*TensorMeta{tensor1}}
				producerTracker.SetProducer(tensor1, op1)

				op2 := &mockOperator{
					inputs:   []*TensorMeta{tensor1},
					outputs:  []*TensorMeta{tensor2},
					inferErr: fmt.Errorf("infer error"),
				}
				producerTracker.SetProducer(tensor2, op2)
				consumerTracker.AddConsumer(tensor1, op2)

				op3 := &OperatorResult{issuerPartyCode: "alice", resultTensors: []*TensorMeta{tensor2}}
				consumerTracker.AddConsumer(tensor2, op3)

				graph := &OperatorGraph{
					operators:       []Operator{op1, op2, op3},
					producerTracker: producerTracker,
					consumerTracker: consumerTracker,
				}
				return graph, vt
			},
			reverse: false,
			wantErr: true,
		},
		{
			name: "reverse error",
			setup: func() (*OperatorGraph, *VisibilityTable) {
				vt := NewVisibilityTable([]string{"alice", "bob"})

				producerTracker := &TensorProducerTracker{}
				consumerTracker := &TensorConsumerTracker{}

				tensor1 := &TensorMeta{ID: 1}
				tensor2 := &TensorMeta{ID: 2}
				tensor3 := &TensorMeta{ID: 3}

				vt.UpdateVisibility(tensor1, NewVisibleParties([]string{"alice"}))
				vt.UpdateVisibility(tensor2, NewVisibleParties([]string{"bob"}))

				op1 := &mockOperator{outputs: []*TensorMeta{tensor1, tensor2}}
				producerTracker.SetProducer(tensor1, op1)
				producerTracker.SetProducer(tensor2, op1)

				op2 := &mockOperator{
					inputs:     []*TensorMeta{tensor1, tensor2},
					outputs:    []*TensorMeta{tensor3},
					reverseErr: fmt.Errorf("reverse error"),
				}
				producerTracker.SetProducer(tensor3, op2)
				consumerTracker.AddConsumer(tensor1, op2)
				consumerTracker.AddConsumer(tensor2, op2)

				op3 := &OperatorResult{issuerPartyCode: "alice", resultTensors: []*TensorMeta{tensor3}}
				consumerTracker.AddConsumer(tensor3, op3)

				graph := &OperatorGraph{
					operators:       []Operator{op1, op2, op3},
					producerTracker: producerTracker,
					consumerTracker: consumerTracker,
				}
				return graph, vt
			},
			reverse: true,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			graph, vt := tt.setup()

			srm := NewSecurityRelaxationManager(&GlobalSecurityRelaxation{}, map[string][]string{})
			vs := NewVisibilitySolver(vt, srm, nil, make(map[int]*VisibleParties), tt.reverse)

			err := vs.Solve(graph)
			if (err != nil) != tt.wantErr {
				t.Errorf("Solve() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRevealResult(t *testing.T) {
	tests := []struct {
		name    string
		setup   func() (*OperatorGraph, *VisibilitySolver)
		wantErr bool
	}{
		{
			name: "empty graph",
			setup: func() (*OperatorGraph, *VisibilitySolver) {
				graph := &OperatorGraph{operators: []Operator{}}
				vs := NewVisibilitySolver(NewVisibilityTable([]string{"alice"}), nil, nil, nil, false)
				return graph, vs
			},
			wantErr: true,
		},
		{
			name: "invalid last node",
			setup: func() (*OperatorGraph, *VisibilitySolver) {
				graph := &OperatorGraph{operators: []Operator{&mockOperator{}}}
				vs := NewVisibilitySolver(NewVisibilityTable([]string{"alice"}), nil, nil, nil, false)
				return graph, vs
			},
			wantErr: true,
		},
		{
			name: "result node with nil intoOpt",
			setup: func() (*OperatorGraph, *VisibilitySolver) {
				vt := NewVisibilityTable([]string{"alice", "bob"})
				resultNode := &OperatorResult{
					issuerPartyCode: "alice",
					resultTensors:   []*TensorMeta{{ID: 1}},
				}
				graph := &OperatorGraph{operators: []Operator{resultNode}}
				vs := NewVisibilitySolver(vt, nil, nil, make(map[int]*VisibleParties), false)
				return graph, vs
			},
			wantErr: false,
		},
		{
			name: "result node with intoOpt and empty field list",
			setup: func() (*OperatorGraph, *VisibilitySolver) {
				vt := NewVisibilityTable([]string{"alice", "bob"})
				resultNode := &OperatorResult{
					issuerPartyCode: "alice",
					resultTensors:   []*TensorMeta{{ID: 1}},
					intoOpt: &core.IntoOpt{
						Opt: &ast.SelectIntoOption{
							Tp: ast.SelectIntoDumpfile,
							PartyFiles: []*ast.PartyFile{
								{PartyCode: "alice", FieldList: []*ast.SelectField{}},
							},
						},
						PartyColumns: map[string][]*expression.Column{
							"alice": {},
						},
					},
				}
				graph := &OperatorGraph{operators: []Operator{resultNode}}
				vs := NewVisibilitySolver(vt, nil, nil, make(map[int]*VisibleParties), false)
				return graph, vs
			},
			wantErr: false,
		},
		{
			name: "result node with intoOpt and field list",
			setup: func() (*OperatorGraph, *VisibilitySolver) {
				vt := NewVisibilityTable([]string{"alice", "bob"})
				tensor := &TensorMeta{ID: 1}
				col := &expression.Column{UniqueID: 1}
				resultNode := &OperatorResult{
					issuerPartyCode: "alice",
					resultTensors:   []*TensorMeta{tensor},
					resultTable:     map[int64]*TensorMeta{1: tensor},
					intoOpt: &core.IntoOpt{
						Opt: &ast.SelectIntoOption{
							Tp: ast.SelectIntoDumpfile,
							PartyFiles: []*ast.PartyFile{
								{PartyCode: "alice", FieldList: []*ast.SelectField{
									{Expr: &ast.ColumnNameExpr{Name: &ast.ColumnName{Name: model.NewCIStr("col1")}}},
								}},
							},
						},
						PartyColumns: map[string][]*expression.Column{
							"alice": {col},
						},
					},
				}
				graph := &OperatorGraph{operators: []Operator{resultNode}}
				vs := NewVisibilitySolver(vt, nil, nil, make(map[int]*VisibleParties), false)
				return graph, vs
			},
			wantErr: false,
		},
		{
			name: "result node with missing column",
			setup: func() (*OperatorGraph, *VisibilitySolver) {
				vt := NewVisibilityTable([]string{"alice", "bob"})
				col := &expression.Column{UniqueID: 2} // Different from tensor ID
				resultNode := &OperatorResult{
					issuerPartyCode: "alice",
					resultTensors:   []*TensorMeta{{ID: 1}},
					resultTable:     map[int64]*TensorMeta{}, // Empty table
					intoOpt: &core.IntoOpt{
						Opt: &ast.SelectIntoOption{
							Tp: ast.SelectIntoDumpfile,
							PartyFiles: []*ast.PartyFile{
								{PartyCode: "alice", FieldList: []*ast.SelectField{{Expr: &ast.ColumnNameExpr{Name: &ast.ColumnName{Name: model.NewCIStr("col1")}}}}},
							},
						},
						PartyColumns: map[string][]*expression.Column{
							"alice": {col}, // Column not in resultTable
						},
					},
				}
				graph := &OperatorGraph{operators: []Operator{resultNode}}
				vs := NewVisibilitySolver(vt, nil, nil, make(map[int]*VisibleParties), false)
				return graph, vs
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			graph, vs := tt.setup()
			err := vs.revealResult(graph)
			if (err != nil) != tt.wantErr {
				t.Errorf("revealResult() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
