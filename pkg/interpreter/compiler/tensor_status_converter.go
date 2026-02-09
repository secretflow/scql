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

	"github.com/secretflow/scql/pkg/interpreter/graph"
	"github.com/secretflow/scql/pkg/interpreter/operator"
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
)

// convertStatus converts a tensor from its current status to the expected placement.
// It handles transitions between private, secret, and public tensor states.
// The fromTensor must be bonded.
func (builder *ExecutionGraphBuilder) convertStatus(fromTensor *graph.Tensor, expectPlace tensorPlacement, checkVis bool) (*graph.Tensor, error) {
	if checkVis {
		meta, err := builder.tm.CorrespondingMeta(fromTensor)
		if err != nil {
			return nil, fmt.Errorf("convertStatus: %v", err)
		}
		if err := builder.checkVis(meta, expectPlace); err != nil {
			return nil, fmt.Errorf("convertStatus: %v", err)
		}
	}

	switch fromTensor.Status() {
	case proto.TensorStatus_TENSORSTATUS_PRIVATE:
		switch place := expectPlace.(type) {
		case *privatePlacement:
			fromParty := fromTensor.OwnerPartyCode
			expectParty := place.partyCode
			if fromParty == expectParty {
				return nil, fmt.Errorf("convertStatus: %s has already met expect place %s", fromTensor, expectPlace)
			}
			return builder.addCopyNode(fromTensor, fromParty, expectParty)
		case *publicPlacement:
			return builder.addMakePublicNode(fromTensor)
		case *secretPlacement:
			return builder.addMakeShareNode(fromTensor)
		}
	case proto.TensorStatus_TENSORSTATUS_SECRET:
		switch place := expectPlace.(type) {
		case *privatePlacement:
			if fromTensor.DType.IsStringType() {
				return builder.revealString(fromTensor, place.partyCode)
			}
			return builder.addMakePrivateNode(fromTensor, place.partyCode)
		case *publicPlacement:
			return builder.addMakePublicNode(fromTensor)
		case *secretPlacement:
			return nil, fmt.Errorf("convertStatus: %s has already met expect place %s", fromTensor, expectPlace)
		}
	case proto.TensorStatus_TENSORSTATUS_PUBLIC:
		switch place := expectPlace.(type) {
		case *privatePlacement:
			if fromTensor.DType.IsStringType() {
				return builder.revealString(fromTensor, place.partyCode)
			}
			return builder.addMakePrivateNode(fromTensor, place.partyCode)
		case *publicPlacement:
			return nil, fmt.Errorf("convertStatus: %s has already met expect place %s", fromTensor, expectPlace)
		case *secretPlacement:
			return builder.addMakeShareNode(fromTensor)
		}
	}
	return nil, fmt.Errorf("convertStatus doesn't support converting from %v to %v", fromTensor.Status(), expectPlace.Status())
}

// revealString converts a string tensor to private status for the specified party.
// String tensors require special handling because the underlying SPU does not fully support secret strings.
// String are stored in hashed form when secret-shared, and only the secret string owner can convert it back to original string.
func (builder *ExecutionGraphBuilder) revealString(tensor *graph.Tensor, revealParty string) (*graph.Tensor, error) {
	if tensor.Status() != proto.TensorStatus_TENSORSTATUS_SECRET && tensor.Status() != proto.TensorStatus_TENSORSTATUS_PUBLIC {
		return nil, fmt.Errorf("revealString: input tensor %s must be secret or public", tensor)
	}

	if len(tensor.SecretStringOwners) == 1 {
		ownerPrivateTensor, err := builder.addMakePrivateNode(tensor, tensor.SecretStringOwners[0])
		if err != nil {
			return nil, fmt.Errorf("revealString: %v", err)
		}
		if tensor.SecretStringOwners[0] == revealParty {
			return ownerPrivateTensor, nil
		}
		return builder.addCopyNode(ownerPrivateTensor, tensor.SecretStringOwners[0], revealParty)
	}

	if tensor.Status() != proto.TensorStatus_TENSORSTATUS_PUBLIC {
		publicTensor, err := builder.addMakePublicNode(tensor)
		if err != nil {
			return nil, fmt.Errorf("revealString: %v", err)
		}
		return builder.addMakePrivateNode(publicTensor, revealParty)
	} else {
		return builder.addMakePrivateNode(tensor, revealParty)
	}
}

// addMakeShareNode creates a new node that converts a tensor to secret-shared status.
// The output tensor will have TENSORSTATUS_SECRET status.
func (builder *ExecutionGraphBuilder) addMakeShareNode(input *graph.Tensor) (*graph.Tensor, error) {
	if input.Status() != proto.TensorStatus_TENSORSTATUS_PRIVATE {
		return nil, fmt.Errorf("addMakeShareNode: input tensor %s must be private", input)
	}

	output, err := builder.tm.CreateConvertedTensor(input, &secretPlacement{})
	if err != nil {
		return nil, fmt.Errorf("addMakeShareNode: %v", err)
	}

	output.SecretStringOwners = []string{input.OwnerPartyCode}

	// TODO handle secret string owner
	if err := builder.addEngineNode("make_share", operator.OpNameMakeShare, map[string][]*graph.Tensor{"In": {input}},
		map[string][]*graph.Tensor{"Out": {output}}, map[string]*graph.Attribute{}, builder.GetAllParties()); err != nil {
		return nil, fmt.Errorf("addMakeShareNode: %v", err)
	}
	return output, nil
}

// addMakePrivateNode creates a new node that converts a tensor to private status for a specific party.
// The output tensor will have TENSORSTATUS_PRIVATE status and be owned by placementParty.
func (builder *ExecutionGraphBuilder) addMakePrivateNode(input *graph.Tensor, placementParty string) (*graph.Tensor, error) {
	output, err := builder.tm.CreateConvertedTensor(input, &privatePlacement{placementParty})
	if err != nil {
		return nil, fmt.Errorf("addMakePrivateNode: %v", err)
	}

	attr := &graph.Attribute{}
	attr.SetString(placementParty)
	if err := builder.addEngineNode("make_private", operator.OpNameMakePrivate, map[string][]*graph.Tensor{"In": {input}},
		map[string][]*graph.Tensor{"Out": {output}}, map[string]*graph.Attribute{operator.RevealToAttr: attr}, builder.GetAllParties()); err != nil {
		return nil, fmt.Errorf("addMakePrivateNode: %v", err)
	}
	return output, nil
}

// addMakePublicNode creates a new node that converts a tensor to public status.
// The output tensor will have TENSORSTATUS_PUBLIC status.
func (builder *ExecutionGraphBuilder) addMakePublicNode(input *graph.Tensor) (*graph.Tensor, error) {
	output, err := builder.tm.CreateConvertedTensor(input, &publicPlacement{})
	if err != nil {
		return nil, fmt.Errorf("addMakePublicNode: %v", err)
	}

	if err := builder.addEngineNode("make_public", operator.OpNameMakePublic, map[string][]*graph.Tensor{"In": {input}},
		map[string][]*graph.Tensor{"Out": {output}}, map[string]*graph.Attribute{}, builder.GetAllParties()); err != nil {
		return nil, fmt.Errorf("addMakePublicNode: %v", err)
	}
	return output, nil
}

// addCopyNode creates a new node that copies a tensor from one party to another.
// The output tensor will have TENSORSTATUS_PRIVATE status and be owned by the target party.
func (builder *ExecutionGraphBuilder) addCopyNode(input *graph.Tensor, fromParty, toParty string) (*graph.Tensor, error) {
	output, err := builder.tm.CreateConvertedTensor(input, &privatePlacement{toParty})
	if err != nil {
		return nil, fmt.Errorf("addCopyNode: %v", err)
	}

	inPartyAttr := &graph.Attribute{}
	inPartyAttr.SetString(fromParty)
	outPartyAttr := &graph.Attribute{}
	outPartyAttr.SetString(toParty)

	if err := builder.addEngineNode("copy", operator.OpNameCopy, map[string][]*graph.Tensor{"In": {input}}, map[string][]*graph.Tensor{"Out": {output}},
		map[string]*graph.Attribute{operator.InputPartyCodesAttr: inPartyAttr, operator.OutputPartyCodesAttr: outPartyAttr}, []string{fromParty, toParty}); err != nil {
		return nil, fmt.Errorf("addCopyNode: %v", err)
	}
	return output, nil
}

// getOrCreatePlacedTensor retrieves or creates a tensor with the expected placement.
// It first checks if a tensor with the expected placement already exists, otherwise it performs
// the necessary conversion from the original tensor.
func (builder *ExecutionGraphBuilder) getOrCreatePlacedTensor(meta *TensorMeta, expectPlace tensorPlacement) (*graph.Tensor, error) {
	// already exists
	tensor, err := builder.tm.getPlacedTensor(meta, expectPlace)
	if err == nil {
		return tensor, nil
	}

	// need conversion
	fromTensor, err := builder.tm.findTensorForConversion(meta, expectPlace)
	if err != nil {
		return nil, fmt.Errorf("getOrCreatePlacedTensor: %v", err)
	}

	tensor, err = builder.convertStatus(fromTensor, expectPlace, true)
	if err != nil {
		return nil, fmt.Errorf("getOrCreatePlacedTensor: %v", err)
	}
	return tensor, nil
}

func (builder *ExecutionGraphBuilder) checkVis(tensorMeta *TensorMeta, place tensorPlacement) error {
	switch place := place.(type) {
	case *privatePlacement:
		visibilityFine := builder.vt.TensorVisibleTo(tensorMeta, place.partyCode)
		if !visibilityFine {
			return fmt.Errorf("checkVis: %s is not visible for party %s", tensorMeta, place.partyCode)
		}
	case *publicPlacement:
		if !builder.vt.IsTensorPublic(tensorMeta) {
			return fmt.Errorf("checkVis: %s is not visible for all parties", tensorMeta)
		}
	case *secretPlacement:
		// no need to check visibility for secret placement
		return nil
	default:
		return fmt.Errorf("checkVis: unsupported placement type %T", place)
	}
	return nil
}
