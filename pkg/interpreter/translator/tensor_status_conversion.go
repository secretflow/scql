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
	"fmt"

	"github.com/secretflow/scql/pkg/interpreter/graph"
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/util/sliceutil"
)

type tensorToStatusPair struct {
	tensorID   int
	destStatus string
}

type statusConverter struct {
	convertMap map[tensorToStatusPair]*graph.Tensor
	plan       *graph.GraphBuilder
}

func newStatusConverter(plan *graph.GraphBuilder) *statusConverter {
	return &statusConverter{convertMap: make(map[tensorToStatusPair]*graph.Tensor), plan: plan}
}

func (b *statusConverter) getExpectTensorFromMap(inputT *graph.Tensor, expectPlace placement) *graph.Tensor {
	if t, ok := b.convertMap[tensorToStatusPair{tensorID: inputT.ID, destStatus: expectPlace.toString()}]; ok {
		return t
	}
	return nil
}

func (b *statusConverter) convertTo(inputT *graph.Tensor, expectPlace placement) (*graph.Tensor, error) {
	if t := b.getExpectTensorFromMap(inputT, expectPlace); t != nil {
		return t, nil
	}
	t, err := b.addTensorStatusConversion(inputT, expectPlace)
	if err != nil {
		return nil, err
	}
	b.convertMap[tensorToStatusPair{tensorID: inputT.ID, destStatus: expectPlace.toString()}] = t
	return t, nil
}

func (b *statusConverter) convertStatusForMap(inputT map[string][]*graph.Tensor, expect map[string][]placement) (map[string][]*graph.Tensor, error) {
	result := make(map[string][]*graph.Tensor)
	for _, key := range sliceutil.SortMapKeyForDeterminism(inputT) {
		ts := inputT[key]
		for i, t := range ts {
			convertedT, err := b.convertTo(t, expect[key][i])
			if err != nil {
				return nil, err
			}
			result[key] = append(result[key], convertedT)
		}
	}
	return result, nil
}

func (b *statusConverter) addTensorStatusConversion(tensor *graph.Tensor, placement placement) (*graph.Tensor, error) {
	switch tensor.Status() {
	case proto.TensorStatus_TENSORSTATUS_PRIVATE:
		switch placement.Status() {
		case proto.TensorStatus_TENSORSTATUS_PUBLIC:
			return b.plan.AddMakePublicNode("make_public", tensor, placement.partyList())
		case proto.TensorStatus_TENSORSTATUS_PRIVATE:
			oldPartyCode := tensor.OwnerPartyCode
			partyCode := placement.partyList()[0]
			if oldPartyCode != partyCode {
				tensor, err := b.plan.AddCopyNode("copy", tensor, oldPartyCode, partyCode)
				if err != nil {
					return nil, fmt.Errorf("addTensorStatusConversion: %v", err)
				}
				return tensor, nil
			}
			return tensor, nil
		case proto.TensorStatus_TENSORSTATUS_SECRET:
			return b.plan.AddMakeShareNode("make_share", tensor, placement.partyList())
		}
	case proto.TensorStatus_TENSORSTATUS_SECRET:
		switch placement.Status() {
		case proto.TensorStatus_TENSORSTATUS_PUBLIC:
			return b.plan.AddMakePublicNode("make_public", tensor, placement.partyList())
		case proto.TensorStatus_TENSORSTATUS_SECRET:
			return tensor, nil
		case proto.TensorStatus_TENSORSTATUS_PRIVATE:
			revealParty := placement.partyList()[0]
			if tensor.DType == proto.PrimitiveDataType_STRING {
				return b.revealString(tensor, revealParty)
			}
			return b.plan.AddMakePrivateNode("make_private", tensor, revealParty, b.plan.GetPartyInfo().GetParties())
		}
	case proto.TensorStatus_TENSORSTATUS_PUBLIC:
		switch placement.Status() {
		case proto.TensorStatus_TENSORSTATUS_PRIVATE:
			revealParty := placement.partyList()[0]
			if tensor.DType == proto.PrimitiveDataType_STRING {
				return b.revealString(tensor, revealParty)
			}
			return b.plan.AddMakePrivateNode("make_private", tensor, revealParty, b.plan.GetPartyInfo().GetParties())
		case proto.TensorStatus_TENSORSTATUS_SECRET:
			return b.plan.AddMakeShareNode("make_share", tensor, placement.partyList())
		case proto.TensorStatus_TENSORSTATUS_PUBLIC:
			return tensor, nil
		}
	}
	return nil, fmt.Errorf("addTensorStatusConversion doesn't support converting from %v to %v", tensor.Status(), placement.Status())
}

func (b *statusConverter) getStatusConversionCost(inputT *graph.Tensor, expectPlace placement) (algCost, error) {
	if t := b.getExpectTensorFromMap(inputT, expectPlace); t != nil {
		return newAlgCost(0, 0), nil
	}
	return getStatusConversionCost(inputT, expectPlace)
}

// secret string is represented in hash, when reveal out, it need parties who convert string into secret to participate.
// typically only one party converts the private string, in union there may exist multi parities
// 1) for one party: make string private in this party, and copy if needed.
// 2) for multi parties: make string hash public(additional public ccl for all parties needed), then make private.
func (b *statusConverter) revealString(input *graph.Tensor, revealParty string) (*graph.Tensor, error) {
	if len(input.SecretStringOwners) == 1 {
		privateT, err := b.plan.AddMakePrivateNode("make_private", input, input.SecretStringOwners[0], b.plan.GetPartyInfo().GetParties())
		if err != nil {
			return nil, fmt.Errorf("revealString: %v", err)
		}
		if input.SecretStringOwners[0] != revealParty {
			return b.plan.AddCopyNode("copy", privateT, input.SecretStringOwners[0], revealParty)
		} else {
			return privateT, nil
		}
	}

	if input.Status() != proto.TensorStatus_TENSORSTATUS_PUBLIC {
		var err error
		input, err = b.plan.AddMakePublicNode("make_public", input, b.plan.GetPartyInfo().GetParties())
		if err != nil {
			return nil, fmt.Errorf("revealString: %v", err)
		}
	}
	return b.plan.AddMakePrivateNode("make_private", input, revealParty, b.plan.GetPartyInfo().GetParties())
}

// if status conversion is possible return cost else return error
func getStatusConversionCost(inputT *graph.Tensor, newPlacement placement) (algCost, error) {
	// TODO(xiaoyuan) Conversion cost here may be re-assign by secret protocol
	switch inputT.Status() {
	case proto.TensorStatus_TENSORSTATUS_PRIVATE:
		switch newPlacement.Status() {
		case proto.TensorStatus_TENSORSTATUS_PRIVATE:
			if inputT.OwnerPartyCode == newPlacement.partyList()[0] {
				return newAlgCost(0, 0), nil
			}
			return newAlgCost(1, 0), nil
		case proto.TensorStatus_TENSORSTATUS_SECRET:
			return newAlgCost(len(newPlacement.partyList())-1, 0), nil
		case proto.TensorStatus_TENSORSTATUS_PUBLIC:
			return newAlgCost(len(newPlacement.partyList())-1, 0), nil
		}
	case proto.TensorStatus_TENSORSTATUS_SECRET:
		switch newPlacement.Status() {
		case proto.TensorStatus_TENSORSTATUS_PRIVATE:
			return newAlgCost(len(newPlacement.partyList())-1, 0), nil
		case proto.TensorStatus_TENSORSTATUS_SECRET:
			return newAlgCost(0, 0), nil
		case proto.TensorStatus_TENSORSTATUS_PUBLIC:
			return newAlgCost(len(newPlacement.partyList()), 0), nil
		}
	case proto.TensorStatus_TENSORSTATUS_PUBLIC:
		switch newPlacement.Status() {
		case proto.TensorStatus_TENSORSTATUS_PRIVATE:
			return newAlgCost(0, 1), nil
		case proto.TensorStatus_TENSORSTATUS_SECRET:
			return newAlgCost(len(newPlacement.partyList())-1, 0), nil
		case proto.TensorStatus_TENSORSTATUS_PUBLIC:
			return newAlgCost(0, 0), nil
		}
	}
	// unexpected status conversion
	return algCost{}, fmt.Errorf("failed to find status conversion cost")
}

func areStatusesAllPrivate(ss ...proto.TensorStatus) bool {
	for _, s := range ss {
		if s != proto.TensorStatus_TENSORSTATUS_PRIVATE {
			return false
		}
	}
	return true
}

func areStatusesAllShare(ss ...proto.TensorStatus) bool {
	for _, s := range ss {
		if s != proto.TensorStatus_TENSORSTATUS_SECRET {
			return false
		}
	}
	return true
}

func areStatusesAllPublic(ss ...proto.TensorStatus) bool {
	for _, s := range ss {
		if s != proto.TensorStatus_TENSORSTATUS_PUBLIC {
			return false
		}
	}
	return true
}

func oneOfStatusesPrivate(ss ...proto.TensorStatus) bool {
	for _, s := range ss {
		if s == proto.TensorStatus_TENSORSTATUS_PRIVATE {
			return true
		}
	}
	return false
}

func oneOfStatusesShare(ss ...proto.TensorStatus) bool {
	for _, s := range ss {
		if s == proto.TensorStatus_TENSORSTATUS_SECRET {
			return true
		}
	}
	return false
}

func oneOfStatusesPublic(ss ...proto.TensorStatus) bool {
	for _, s := range ss {
		if s == proto.TensorStatus_TENSORSTATUS_PUBLIC {
			return true
		}
	}
	return false
}
