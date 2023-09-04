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
	"github.com/secretflow/scql/pkg/proto-gen/scql"
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
)

func createTenosorFromPlace(place placement) *Tensor {
	t := NewTensor(0, "")
	switch x := place.(type) {
	case *privatePlacement:
		t.Status = proto.TensorStatus_TENSORSTATUS_PRIVATE
		t.OwnerPartyCode = x.partyCode
	case *sharePlacement:
		t.Status = proto.TensorStatus_TENSORSTATUS_SECRET
	case *publicPlacement:
		t.Status = proto.TensorStatus_TENSORSTATUS_PUBLIC
	}
	return t
}

func TestGetStatusConversionCost(t *testing.T) {
	type testCase struct {
		old  *Tensor
		new  placement
		pass bool
	}

	testCases := []testCase{
		{createTenosorFromPlace(&privatePlacement{"a"}), &privatePlacement{"b"}, true},
		{createTenosorFromPlace(&privatePlacement{"a"}), &privatePlacement{"a"}, true},
		{createTenosorFromPlace(&privatePlacement{"a"}), &sharePlacement{[]string{"a", "b"}}, true},
		{createTenosorFromPlace(&sharePlacement{[]string{"a", "b"}}), &privatePlacement{"a"}, true},
		{createTenosorFromPlace(&sharePlacement{[]string{"a", "b"}}), &sharePlacement{[]string{"a", "b"}}, true},
	}

	a := require.New(t)
	for _, tc := range testCases {
		_, err := getStatusConversionCost(tc.old, tc.new)
		a.Nil(err)
	}
}

func TestAddTensorStatusConversion(t *testing.T) {
	a := require.New(t)
	pi := NewPartyInfo([]*Participant{
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
	})
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
		_, err := e.converter.convertTo(tensor, &privatePlacement{"party2"})
		a.NoError(err)
	}

	{
		e := newSimplePlan()
		tensor := e.Tensors[0]
		t2, err := e.converter.convertTo(tensor, &privatePlacement{"party1"})
		a.NoError(err)
		a.Equal(tensor, t2)
	}
}

func TestConvertTo(t *testing.T) {
	r := require.New(t)
	partyInfo := NewPartyInfo([]*Participant{
		{
			PartyCode: "Alice",
			Endpoints: []string{"alice.net"},
			Token:     "alice_credential",
		},
		{
			PartyCode: "Bob",
			Endpoints: []string{"bob.net"},
			Token:     "bob_credential",
		},
	})
	e1 := NewGraphBuilder(partyInfo)
	mockT1 := e1.AddTensor("t1.1")
	mockT1.Status = scql.TensorStatus_TENSORSTATUS_PRIVATE
	mockT1.OwnerPartyCode = "Alice"
	mockT1.cc = ccl.CreateAllPlainCCL(partyInfo.GetParties())

	convertToBob1, err := e1.converter.convertTo(mockT1, &privatePlacement{partyCode: "Bob"})
	r.Nil(err)
	r.NotEqual(convertToBob1.ID, mockT1.ID)
	convertToBob2, err := e1.converter.convertTo(mockT1, &privatePlacement{partyCode: "Bob"})
	r.Nil(err)
	r.Equal(convertToBob1.ID, convertToBob2.ID)
	convertBobToShare, err := e1.converter.convertTo(convertToBob1, &sharePlacement{partyCodes: []string{"Alice", "Bob"}})
	r.Nil(err)
	r.NotEqual(convertBobToShare.ID, convertToBob2.ID)
}
