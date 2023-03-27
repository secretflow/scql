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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/secretflow/scql/pkg/interpreter/ccl"
	"github.com/secretflow/scql/pkg/interpreter/operator"
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
)

func TestGraphSimple(t *testing.T) {
	partyInfo, err := NewPartyInfo([]string{"party1", "party2"}, []string{"party1.net", "party2.net"}, []string{"party1_credential", "party2_credential"})
	assert.Nil(t, err)
	e1 := NewGraphBuilder(partyInfo)

	t1 := e1.AddTensor("alice.t1")
	t1.Status = proto.TensorStatus_TENSORSTATUS_PRIVATE
	e1.AddRunSQLNode("RunSQLOp1", []*Tensor{t1}, "select f1 from alice.t1", []string{"alice.t1"}, "party1")

	t2 := e1.AddTensor("alice.t2")
	t2.Status = proto.TensorStatus_TENSORSTATUS_PRIVATE
	e1.AddRunSQLNode("RunSQLOp2", []*Tensor{t2}, "select * from alice.t2", []string{"alice.t2"}, "party2")

	t3, err := e1.AddBinaryNode("LessOp1", operator.OpNameLess, t1, t2, ccl.CreateAllPlainCCL([]string{"party1.net", "party2.net"}), []string{"party1", "party2"})
	assert.NotNil(t, t3)
	assert.Nil(t, err)
	assert.Equal(t, proto.TensorStatus_TENSORSTATUS_SECRET, t3.Status)

	graph := e1.Build()
	nodes, err := graph.TopologicalSort()
	assert.Nil(t, err)
	assert.NotNil(t, nodes)
	// 2 runsql + 1 less + 2 make_share
	assert.Equal(t, 5, len(nodes))
}
