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
	"strings"
)

// VisibilityRegistry defines the contract for managing tensor visibility across parties
// in the SCQL compiler. It tracks which parties can see which tensors during query execution.
type VisibilityRegistry interface {
	// TensorVisibleParties returns the visible parties for a given tensor
	TensorVisibleParties(tensor *TensorMeta) *VisibleParties

	// CommonVisibleParties returns the intersection of visible parties across multiple tensors
	// Returns empty VisibleParties if input slice is empty
	CommonVisibleParties(tensors []*TensorMeta) *VisibleParties

	// UpdateVisibility updates the visible parties for a tensor by merging with increment
	// Silently ignores nil tensor or nil increment
	UpdateVisibility(tensor *TensorMeta, increment *VisibleParties)

	// PublicVisibility returns a VisibleParties containing all parties (has public visibility)
	PublicVisibility() *VisibleParties

	// IsTensorPublic returns true if the tensor is visible to all parties
	IsTensorPublic(tensor *TensorMeta) bool
}

type VisibilityTable struct {
	tensorsVisibility map[int]*VisibleParties
	allParties        []string
}

// NewVisibilityTable creates a new VisibilityTable with the given parties as public visibility
func NewVisibilityTable(parties []string) *VisibilityTable {
	return &VisibilityTable{
		tensorsVisibility: make(map[int]*VisibleParties),
		allParties:        parties,
	}
}

func (vt *VisibilityTable) TensorVisibleParties(tensor *TensorMeta) *VisibleParties {
	if tensor == nil {
		return nil
	}
	if v, ok := vt.tensorsVisibility[tensor.ID]; ok {
		return v
	}
	return NewVisibleParties(nil)
}

func (vt *VisibilityTable) UpdateVisibility(tensor *TensorMeta, increment *VisibleParties) {
	if tensor == nil || increment == nil {
		return
	}
	vp := vt.TensorVisibleParties(tensor)
	vp.UpdateWith(increment)
	vt.tensorsVisibility[tensor.ID] = vp
}

// CommonVisibleParties returns the intersection of visible parties across multiple tensors
// Returns public VisibleParties if input slice is empty
func (vt *VisibilityTable) CommonVisibleParties(tensors []*TensorMeta) *VisibleParties {
	if len(tensors) == 0 {
		return vt.PublicVisibility()
	}
	vis := vt.TensorVisibleParties(tensors[0])
	for _, tensor := range tensors[1:] {
		vis = VPIntersection(vis, vt.TensorVisibleParties(tensor))
	}
	return vis
}

// PublicVisibility returns a VisibleParties containing all parties (public visibility)
func (vt *VisibilityTable) PublicVisibility() *VisibleParties {
	return NewVisibleParties(vt.allParties)
}

// VisibilityPublic returns true if the given visibility covers all parties
func (vt *VisibilityTable) VisibilityPublic(vis *VisibleParties) bool {
	for _, party := range vt.allParties {
		if !vis.Contains(party) {
			return false
		}
	}
	return true
}

// TryUpdateVisibility attempts to update visibility only if increment adds new parties
// Returns true if the visibility was actually modified, false if no changes were needed
// because the tensor already covers all parties in the increment
func (vt *VisibilityTable) TryUpdateVisibility(tensor *TensorMeta, increment *VisibleParties) bool {
	vp := vt.TensorVisibleParties(tensor)
	if vp.Covers(increment) {
		return false
	}
	vt.UpdateVisibility(tensor, increment)
	return true
}

// TensorRegistered returns true if the tensor has been registered with visibility information
func (vs *VisibilityTable) TensorRegistered(tensor *TensorMeta) bool {
	if tensor == nil {
		return false
	}
	_, ok := vs.tensorsVisibility[tensor.ID]
	return ok
}

func (vt *VisibilityTable) DumpVisibility() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "Tensor Visibility:\n")
	for tensorID, VP := range vt.tensorsVisibility {
		fmt.Fprintf(&sb, "t_%d: \tVisibleParties[%s]\n", tensorID, VP.parties)
	}
	return sb.String()
}

// choosePlacement determines the optimal placement for a tensor meta based on visibility and constraints
//
// Parameters:
// - tensor: the meta of the tensor to place
// - tm: tensor manager for checking existing placements
// - allowPublic: whether public placement is allowed
// - allowPrivate: whether private placement is allowed
// - secretFine: whether secret placement is considered as good as other placements
//
// Decision priority:
// 1. Empty visibility → secret placement
// 2. Existing placement (to avoid unnecessary conversion)
// 3. Public placement if tensor is visible to all parties
// 4. Private placement for single party visibility
// 5. Secret placement as fallback
func (vt *VisibilityTable) choosePlacement(tensor *TensorMeta, tm *TensorManager, allowPublic, allowPrivate, secretFine bool) tensorPlacement {
	vp := vt.TensorVisibleParties(tensor)
	if vp.IsEmpty() {
		return &secretPlacement{}
	}
	// check existing placment to avoid conversion
	if allowPublic && tm.hasPlacedTensor(tensor, &publicPlacement{}) {
		return &publicPlacement{}
	}
	if allowPrivate {
		for _, party := range vp.GetParties() {
			placement := &privatePlacement{partyCode: party}
			if tm.hasPlacedTensor(tensor, placement) {
				return placement
			}
		}
	}
	if secretFine && tm.hasPlacedTensor(tensor, &secretPlacement{}) {
		return &secretPlacement{}
	}
	if vt.VisibilityPublic(vp) && allowPublic {
		return &publicPlacement{}
	}
	if allowPrivate && !vp.IsEmpty() {
		return &privatePlacement{partyCode: vp.GetOneParty()}
	}
	return &secretPlacement{}
}

// IsTensorPublic returns true if the tensor is visible to all parties
func (vt *VisibilityTable) IsTensorPublic(tensor *TensorMeta) bool {
	vp := vt.TensorVisibleParties(tensor)
	return vt.VisibilityPublic(vp)
}

// TensorVisibleTo returns true if the tensor is visible to the specified party
func (vt *VisibilityTable) TensorVisibleTo(tensor *TensorMeta, party string) bool {
	vp := vt.TensorVisibleParties(tensor)
	return vp.Contains(party)
}

// UpdateTensorsVisibilityLike increase given tensors‘ visibility according to another registry.
// This is useful for synchronizing the VisibilityTable with a temporary OverlayVisibilityTable.
func (vt *VisibilityTable) UpdateTensorsVisibilityLike(vr VisibilityRegistry, tensors []*TensorMeta) {
	for _, tensor := range tensors {
		vt.UpdateVisibility(tensor, vr.TensorVisibleParties(tensor))
	}
}

// OverlayVisibilityTable implements a decorator pattern for VisibilityRegistry
// It provides a layered visibility system where overlay changes take precedence over base table
// This allows temporary visibility modifications without affecting the underlying base table
// This is useful for complex visibility inference scenarios
//
// Usage pattern:
// - Base table contains permanent visibility assignments
// - Overlay contains temporary/modified visibility for specific tensors
// - Lookup order: overlay → base table (with cloning to prevent mutation)
type OverlayVisibilityTable struct {
	overlayVisibility map[int]*VisibleParties
	baseTable         *VisibilityTable
}

func (ovt *OverlayVisibilityTable) TensorVisibleParties(tensor *TensorMeta) *VisibleParties {
	if tensor == nil {
		return NewVisibleParties(nil)
	}
	if v, ok := ovt.overlayVisibility[tensor.ID]; ok {
		return v
	}
	return ovt.baseTable.TensorVisibleParties(tensor).Clone()
}

func (ovt *OverlayVisibilityTable) CommonVisibleParties(tensors []*TensorMeta) *VisibleParties {
	if len(tensors) == 0 {
		return ovt.PublicVisibility()
	}
	vis := ovt.TensorVisibleParties(tensors[0])
	for _, tensor := range tensors[1:] {
		vis = VPIntersection(vis, ovt.TensorVisibleParties(tensor))
	}
	return vis
}

func (ovt *OverlayVisibilityTable) UpdateVisibility(tensor *TensorMeta, increment *VisibleParties) {
	if tensor == nil || increment == nil {
		return
	}
	vp := ovt.TensorVisibleParties(tensor)
	vp.UpdateWith(increment)
	ovt.overlayVisibility[tensor.ID] = vp
}

func (ovt *OverlayVisibilityTable) PublicVisibility() *VisibleParties {
	return ovt.baseTable.PublicVisibility()
}

func (ovt *OverlayVisibilityTable) IsTensorPublic(tensor *TensorMeta) bool {
	vis := ovt.TensorVisibleParties(tensor)
	return ovt.baseTable.VisibilityPublic(vis)
}
