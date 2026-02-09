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
	"slices"

	"github.com/sirupsen/logrus"

	"github.com/secretflow/scql/pkg/interpreter/graph"
	proto "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/util/sliceutil"
)

// TensorPlaceMap is a map from tensor placement string to placed tensor
type TensorPlaceMap map[string]*graph.Tensor

// TensorManager manages the lifecycle and relationships between tensor metas
// and their corresponding placed tensors across different parties and placements.
//
// It maintains two key mappings:
// 1. MetaId2Pt: Maps TensorMeta IDs to their coresponding placed tensor by placement string
// 2. tensorId2Meta: Maps placed tensor IDs back to their corresponding TensorMeta IDs
//
// This enables tracking of how a single logical tensor exists in different states
// (public, private, secret) across multiple parties while maintaining referential integrity.
type TensorManager struct {
	tensors      []*graph.Tensor // note that tensors in this slice are not in the order of their IDs
	nextTensorID int
	// TODO: use slice instead of map
	metaId2Tpm    map[int]TensorPlaceMap // use TensorMeta id as key
	tensorId2Meta map[int]*TensorMeta    // use placed tensor id as key
}

func NewTensorManager(nextTensorID int) *TensorManager {
	return &TensorManager{
		nextTensorID:  nextTensorID,
		metaId2Tpm:    make(map[int]TensorPlaceMap),
		tensorId2Meta: make(map[int]*TensorMeta),
	}
}

func (tm *TensorManager) Tensors() []*graph.Tensor {
	return tm.tensors
}

func (tm *TensorManager) CreateTensorAs(source *graph.Tensor) *graph.Tensor {
	tensor := tm.newTensor(source.Name)
	tensor.DType = source.DType
	tensor.Option = source.Option
	tensor.IsConstScalar = source.IsConstScalar
	tensor.OwnerPartyCode = source.OwnerPartyCode
	tensor.SecretStringOwners = source.SecretStringOwners
	tensor.SetStatus(source.Status())
	return tensor
}

func (tm *TensorManager) CreateResultTensor(source *graph.Tensor, outputName string) *graph.Tensor {
	tensor := tm.CreateTensorAs(source)
	tensor.Option = proto.TensorOptions_VALUE
	tensor.Name = outputName
	tensor.DType = graph.NewPrimitiveDataType(proto.PrimitiveDataType_STRING)
	tensor.StringS = []string{outputName}
	return tensor
}

func (tm *TensorManager) CreateConvertedTensor(source *graph.Tensor, placement tensorPlacement) (*graph.Tensor, error) {
	tensor := tm.CreateTensorAs(source)
	switch place := placement.(type) {
	case *privatePlacement:
		tensor.SetStatus(proto.TensorStatus_TENSORSTATUS_PRIVATE)
		tensor.OwnerPartyCode = place.partyCode
	case *publicPlacement:
		tensor.SetStatus(proto.TensorStatus_TENSORSTATUS_PUBLIC)
	case *secretPlacement:
		tensor.SetStatus(proto.TensorStatus_TENSORSTATUS_SECRET)
	default:
		return nil, fmt.Errorf("CreateConvertedTensor: unsupported placement type %T", place)
	}

	meta, metaExist := tm.tensorId2Meta[source.ID]
	if metaExist {
		if err := tm.setPlacedTensor(meta, tensor, placement); err != nil {
			return nil, fmt.Errorf("CreateConvertedTensor: setPlacedTensor failed: %v", err)
		}
	}

	return tensor, nil
}

// CreateUnbondedTensor creates a new unbonded tensor with the specified name, data type, and placement
// If a tensor does not have corresponding schema column in LogicalPlan(in another word, does not have corresponding TensorMeta in OperatorGraph), it is unbonded tensor
func (tm *TensorManager) CreateUnbondedTensor(name string, dtype *graph.DataType, placement tensorPlacement) *graph.Tensor {
	tensor := tm.newTensor(name)
	tensor.DType = dtype

	switch place := placement.(type) {
	case *privatePlacement:
		tensor.SetStatus(proto.TensorStatus_TENSORSTATUS_PRIVATE)
		tensor.OwnerPartyCode = place.partyCode
	case *publicPlacement:
		tensor.SetStatus(proto.TensorStatus_TENSORSTATUS_PUBLIC)
	case *secretPlacement:
		tensor.SetStatus(proto.TensorStatus_TENSORSTATUS_SECRET)
	}

	return tensor
}

// CreateAndSetFirstTensor creates the initial placed tensor for a tensor meta
// at a specific placement.
// Note that we need to distinguish the first placed tensor from the other placed tensors
// because the first placed tensor has the same tensor ID with corresponding tensor meta.
func (tm *TensorManager) CreateAndSetFirstTensor(meta *TensorMeta, place tensorPlacement) (*graph.Tensor, error) {
	_, ok := tm.metaId2Tpm[meta.ID]
	if ok {
		return nil, fmt.Errorf("CreateAndSetFirstTensor: tensorPlaceMap already exists")
	}

	tensorPlaceMap := make(TensorPlaceMap)
	tensor := tm.tensorFromMeta(meta, place)

	tensorPlaceMap[place.String()] = tensor
	tm.metaId2Tpm[meta.ID] = tensorPlaceMap
	tm.tensorId2Meta[tensor.ID] = meta
	return tensor, nil
}

// CorrespondingMeta returns the tensor meta corresponding to a placed tensor.
func (tm *TensorManager) CorrespondingMeta(pt *graph.Tensor) (*TensorMeta, error) {
	meta, ok := tm.tensorId2Meta[pt.ID]
	if !ok {
		return nil, fmt.Errorf("CorrespondingMeta: can not find corresponding meta for tensor %s", pt)
	} else {
		return meta, nil
	}
}

func (tm *TensorManager) newTensorID() int {
	id := tm.nextTensorID
	tm.nextTensorID++
	return id
}

func (tm *TensorManager) newTensor(name string) *graph.Tensor {
	tensor := graph.NewTensor(tm.newTensorID(), name)
	tm.tensors = append(tm.tensors, tensor)
	return tensor
}

func (tm *TensorManager) tensorFromMeta(meta *TensorMeta, place tensorPlacement) *graph.Tensor {
	tensor := graph.NewTensor(meta.ID, meta.Name)
	tensor.DType = meta.DType
	tensor.IsConstScalar = meta.IsConstScalar
	tensor.Option = proto.TensorOptions_REFERENCE

	switch x := place.(type) {
	case *privatePlacement:
		tensor.SetStatus(proto.TensorStatus_TENSORSTATUS_PRIVATE)
		tensor.OwnerPartyCode = x.partyCode
	case *publicPlacement:
		tensor.SetStatus(proto.TensorStatus_TENSORSTATUS_PUBLIC)
	case *secretPlacement:
		tensor.SetStatus(proto.TensorStatus_TENSORSTATUS_SECRET)
	default:
		logrus.Warnf("tensorFromMeta: unknown placement type %T", x)
	}

	return tensor
}

func (tm *TensorManager) getPlacedTensor(meta *TensorMeta, expectPlace tensorPlacement) (*graph.Tensor, error) {
	tensorPlaceMap, ok := tm.metaId2Tpm[meta.ID]
	if !ok {
		return nil, fmt.Errorf("getPlacedTensor: the input tensor does not have any corresponding placed tensor")
	}

	pt, ok := tensorPlaceMap[expectPlace.String()]
	if !ok {
		return nil, fmt.Errorf("getPlacedTensor: the input tensor does not have any corresponding placed tensor with place %s", expectPlace.String())
	}
	return pt, nil
}

func (tm *TensorManager) hasPlacedTensor(meta *TensorMeta, place tensorPlacement) bool {
	tensorPlaceMap, ok := tm.metaId2Tpm[meta.ID]
	if !ok {
		return false
	}

	_, ok = tensorPlaceMap[place.String()]
	return ok
}

// existingPrivateParties returns the list of parties that have private placed tensors
// for the given tensor meta.
func (tm *TensorManager) existingPrivateParties(meta *TensorMeta) []string {
	tensorPlaceMap, ok := tm.metaId2Tpm[meta.ID]
	if !ok {
		return nil
	}

	privateParties := []string{}
	for _, pt := range tensorPlaceMap {
		if pt.Status() == proto.TensorStatus_TENSORSTATUS_PRIVATE {
			privateParties = append(privateParties, pt.OwnerPartyCode)
		}
	}
	return privateParties
}

// findTensorForConversion determines which placed tensor of the given tensor meta
// should be used as the source for converting an placed tensor to a new placement.
func (tm *TensorManager) findTensorForConversion(meta *TensorMeta, expectPlace tensorPlacement) (*graph.Tensor, error) {
	tensorPlaceMap, ok := tm.metaId2Tpm[meta.ID]
	if !ok {
		return nil, fmt.Errorf("findTensorForConversion: the input tensor does not have any corresponding placed tensor")
	}
	orderedTensors := slices.Collect(sliceutil.ValueSortedByMapKey(tensorPlaceMap))
	if len(orderedTensors) == 0 {
		return nil, fmt.Errorf("findTensorForConversion: the input tensor does not have any corresponding placed tensor")
	}

	// TODO choose best tensor
	if IsSecret(expectPlace) {
		for _, pt := range orderedTensors {
			if pt.Status() == proto.TensorStatus_TENSORSTATUS_PRIVATE {
				return pt, nil
			}
		}
		return nil, fmt.Errorf("findTensorForConversion: no private tensor found to convert to secret")
	}

	return orderedTensors[0], nil
}

// setPlacedTensor sets a placed tensor for the given tensor meta.
func (tm *TensorManager) setPlacedTensor(meta *TensorMeta, pt *graph.Tensor, place tensorPlacement) error {
	tensorPlaceMap, ok := tm.metaId2Tpm[meta.ID]
	if !ok {
		return fmt.Errorf("setPlacedTensor: the input tensor does not have any corresponding placed tensor")
	}

	// Validate state consistency: tensor status must match placement status
	expectedStatus := place.Status()
	if pt.Status() != expectedStatus {
		return fmt.Errorf("setPlacedTensor: tensor status %v does not match placement status %v",
			pt.Status(), expectedStatus)
	}

	tensorPlaceMap[place.String()] = pt

	if _, ok := tm.tensorId2Meta[pt.ID]; ok {
		return fmt.Errorf("setPlacedTensor: placed tensor %s already been set", pt)
	}
	tm.tensorId2Meta[pt.ID] = meta

	return nil
}

// getOnePlacedTensor returns a single placed tensor for the given tensor meta
// with specific priority: public tensors first, then private tensors (sorted by party code),
// then any remaining tensor as fallback.
func (tm *TensorManager) getOnePlacedTensor(meta *TensorMeta) (*graph.Tensor, error) {
	tensorPlaceMap, ok := tm.metaId2Tpm[meta.ID]
	if !ok || len(tensorPlaceMap) == 0 {
		return nil, fmt.Errorf("getOnePlacedTensor: the input tensor meta does not have any corresponding placed tensor")
	}

	// TODO more elegantly
	for pt := range sliceutil.ValueSortedByMapKey(tensorPlaceMap) {
		if pt.Status() == proto.TensorStatus_TENSORSTATUS_PUBLIC {
			return pt, nil
		}
	}
	for pt := range sliceutil.ValueSortedByMapKey(tensorPlaceMap) {
		if pt.Status() == proto.TensorStatus_TENSORSTATUS_PRIVATE {
			return pt, nil
		}
	}
	for pt := range sliceutil.ValueSortedByMapKey(tensorPlaceMap) {
		return pt, nil
	}
	return nil, fmt.Errorf("getOnePlacedTensor: should not reach here")
}

// choosePrivatePlacement selects the best private placement party for tensors based on
// existing tensor locations. It counts existing tensors per candidate party and selects
// the party with the most existing tensors to minimize data movement.
// This is useful when generating private kernels.
func (tm *TensorManager) choosePrivatePlacement(candidates *VisibleParties, tensors []*TensorMeta) *privatePlacement {
	existingTensorCount := make(map[string]int)
	for _, candidate := range candidates.GetParties() {
		for _, meta := range tensors {
			if tm.hasPlacedTensor(meta, &privatePlacement{partyCode: candidate}) {
				existingTensorCount[candidate]++
			}
		}
	}
	chosenParty := ""
	partyTensorCount := -1
	for party, count := range sliceutil.SortedMap(existingTensorCount) {
		if count > partyTensorCount {
			chosenParty = party
			partyTensorCount = count
		}
	}

	if chosenParty == "" {
		return &privatePlacement{partyCode: candidates.GetOneParty()}
	}
	return &privatePlacement{partyCode: chosenParty}
}
