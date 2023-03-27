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

	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
)

// if status conversion is possible return cost else return error
func getStatusConversionCost(placement, newPlacement placement) (algCost, error) {
	// TODO(xiaoyuan) Conversion cost here may be re-assign by secret protocol
	switch placement.status() {
	case proto.TensorStatus_TENSORSTATUS_PRIVATE:
		switch newPlacement.status() {
		case proto.TensorStatus_TENSORSTATUS_PRIVATE:
			if placement.partyList()[0] == newPlacement.partyList()[0] {
				return newAlgCost(0, 0), nil
			}
			return newAlgCost(1, 0), nil
		case proto.TensorStatus_TENSORSTATUS_SECRET:
			return newAlgCost(len(placement.partyList())-1, 0), nil
		case proto.TensorStatus_TENSORSTATUS_PUBLIC:
			return newAlgCost(len(placement.partyList())-1, 0), nil
		}
	case proto.TensorStatus_TENSORSTATUS_SECRET:
		switch newPlacement.status() {
		case proto.TensorStatus_TENSORSTATUS_PRIVATE:
			return newAlgCost(len(placement.partyList())-1, 0), nil
		case proto.TensorStatus_TENSORSTATUS_SECRET:
			return newAlgCost(0, 0), nil
		case proto.TensorStatus_TENSORSTATUS_PUBLIC:
			return newAlgCost((len(placement.partyList())-1)*len(placement.partyList()), 0), nil
		}
	case proto.TensorStatus_TENSORSTATUS_PUBLIC:
		switch newPlacement.status() {
		case proto.TensorStatus_TENSORSTATUS_PUBLIC:
			return newAlgCost(0, 0), nil
		}
	}
	// unexpected status conversion
	return algCost{}, fmt.Errorf("failed to find status conversion cost")
}

func (plan *GraphBuilder) addTensorStatusConversion(tensor *Tensor, placement placement) (*Tensor, error) {
	switch tensor.Status {
	case proto.TensorStatus_TENSORSTATUS_PRIVATE:
		switch placement.status() {
		case proto.TensorStatus_TENSORSTATUS_PUBLIC:
			return plan.AddMakePublicNode("make_public", tensor, placement.partyList())
		case proto.TensorStatus_TENSORSTATUS_PRIVATE:
			oldPartyCode := tensor.OwnerPartyCode
			partyCode := placement.partyList()[0]
			if oldPartyCode != partyCode {
				tensor, err := plan.AddCopyNode("copy", tensor, oldPartyCode, partyCode)
				if err != nil {
					return nil, fmt.Errorf("addTensorStatusConversion: %v", err)
				}
				return tensor, nil
			}
			return tensor, nil
		case proto.TensorStatus_TENSORSTATUS_SECRET:
			return plan.AddMakeShareNode("make_share", tensor, placement.partyList())
		}
	case proto.TensorStatus_TENSORSTATUS_SECRET:
		switch placement.status() {
		case proto.TensorStatus_TENSORSTATUS_PUBLIC:
			return plan.AddMakePublicNode("make_public", tensor, placement.partyList())
		case proto.TensorStatus_TENSORSTATUS_SECRET:
			return tensor, nil
		case proto.TensorStatus_TENSORSTATUS_PRIVATE:
			revealParty := placement.partyList()[0]
			return plan.AddMakePrivateNode("make_private", tensor, revealParty, plan.partyInfo.GetParties())
		}
	case proto.TensorStatus_TENSORSTATUS_PUBLIC:
		switch placement.status() {
		// if tensor is public placement must be public
		case proto.TensorStatus_TENSORSTATUS_PUBLIC:
			return tensor, nil
		}
	}
	return nil, fmt.Errorf("addTensorStatusConversion doesn't support converting from %v to %v", tensor.Status, placement.status())
}

func (plan *GraphBuilder) findTensorPlacement(t *Tensor) (placement, error) {
	switch t.Status {
	case proto.TensorStatus_TENSORSTATUS_PRIVATE:
		return &privatePlacement{partyCode: t.OwnerPartyCode}, nil
	case proto.TensorStatus_TENSORSTATUS_SECRET:
		return &sharePlacement{partyCodes: plan.partyInfo.GetParties()}, nil
	case proto.TensorStatus_TENSORSTATUS_PUBLIC:
		return &publicPlacement{partyCodes: plan.partyInfo.GetParties()}, nil
	}
	return nil, fmt.Errorf("findTensorPlacement: unsupported tensor type %s", t.Status)
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
