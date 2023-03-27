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

	"github.com/stretchr/testify/require"

	"github.com/secretflow/scql/pkg/interpreter/ccl"
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
)

func TestGetStatusConversionCost(t *testing.T) {
	type testCase struct {
		old  placement
		new  placement
		pass bool
	}
	testCases := []testCase{
		{&privatePlacement{"a"}, &privatePlacement{"b"}, true},
		{&privatePlacement{"a"}, &privatePlacement{"a"}, true},
		{&privatePlacement{"a"}, &sharePlacement{[]string{"a", "b"}}, true},
		{&sharePlacement{[]string{"a", "b"}}, &privatePlacement{"a"}, true},
		{&sharePlacement{[]string{"a", "b"}}, &sharePlacement{[]string{"a", "b"}}, true},
	}

	a := require.New(t)
	for _, tc := range testCases {
		_, err := getStatusConversionCost(tc.old, tc.new)
		a.Nil(err)
	}
}

func TestAddTensorStatusConversion(t *testing.T) {
	a := require.New(t)
	pi, err := NewPartyInfo([]string{"party1", "party2"}, []string{"party1.net", "party2.net"}, []string{"party1_credential", "party2_credential"})
	a.NoError(err)
	newSimplePlan := func() *GraphBuilder {
		e1 := NewGraphBuilder(pi)
		t1 := e1.AddTensor("alice.t1")
		t1.cc.SetLevelForParty("party1", ccl.Plain)
		t1.cc.SetLevelForParty("party2", ccl.Plain)
		t1.OwnerPartyCode = "party1"
		t1.Status = proto.TensorStatus_TENSORSTATUS_PRIVATE
		e1.AddRunSQLNode("RunSQLOp1", []*Tensor{t1}, "select f1 from alice.t1", []string{"alice.t1"}, "party1")
		return e1
	}

	{
		e := newSimplePlan()
		tensor := e.Tensors[0]
		_, err := e.addTensorStatusConversion(tensor, &privatePlacement{"party2"})
		a.NoError(err)
	}

	{
		e := newSimplePlan()
		tensor := e.Tensors[0]
		t2, err := e.addTensorStatusConversion(tensor, &privatePlacement{"party1"})
		a.NoError(err)
		a.Equal(tensor, t2)
	}
}
