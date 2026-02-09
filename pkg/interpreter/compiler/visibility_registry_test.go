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
)

func TestNewVisibilityTable(t *testing.T) {
	parties := []string{"alice", "bob", "carol"}
	vt := NewVisibilityTable(parties)

	assert.NotNil(t, vt)
	assert.Equal(t, parties, vt.allParties)
	assert.NotNil(t, vt.tensorsVisibility)
	assert.Empty(t, vt.tensorsVisibility)
}

func TestVisibilityTableTensorVisibleParties(t *testing.T) {
	parties := []string{"alice", "bob"}
	vt := NewVisibilityTable(parties)

	tensor := &TensorMeta{ID: 1}

	// Test non-existent tensor returns empty VisibleParties
	vis := vt.TensorVisibleParties(tensor)
	assert.NotNil(t, vis)
	assert.True(t, vis.IsEmpty())

	// Test after adding visibility
	vt.UpdateVisibility(tensor, NewVisibleParties([]string{"alice"}))
	vis = vt.TensorVisibleParties(tensor)
	assert.Equal(t, []string{"alice"}, vis.GetParties())
}

func TestVisibilityTableTensorsVisibleParties(t *testing.T) {
	parties := []string{"alice", "bob", "carol"}
	vt := NewVisibilityTable(parties)

	tensor1 := &TensorMeta{ID: 1}
	tensor2 := &TensorMeta{ID: 2}
	tensor3 := &TensorMeta{ID: 3}

	// Test empty slice
	vis := vt.CommonVisibleParties([]*TensorMeta{})
	assert.True(t, vt.VisibilityPublic(vis))

	// Test single tensor
	vt.UpdateVisibility(tensor1, NewVisibleParties([]string{"alice", "bob"}))
	vis = vt.CommonVisibleParties([]*TensorMeta{tensor1})
	assert.Equal(t, []string{"alice", "bob"}, vis.GetParties())

	// Test multiple tensors with intersection
	vt.UpdateVisibility(tensor2, NewVisibleParties([]string{"bob", "carol"}))
	vt.UpdateVisibility(tensor3, NewVisibleParties([]string{"bob"}))

	vis = vt.CommonVisibleParties([]*TensorMeta{tensor1, tensor2, tensor3})
	assert.Equal(t, []string{"bob"}, vis.GetParties())
}

func TestVisibilityTableUpdateVisibility(t *testing.T) {
	parties := []string{"alice", "bob"}
	vt := NewVisibilityTable(parties)

	tensor := &TensorMeta{ID: 1}

	// Test basic update
	vt.UpdateVisibility(tensor, NewVisibleParties([]string{"alice"}))
	vis := vt.TensorVisibleParties(tensor)
	assert.Equal(t, []string{"alice"}, vis.GetParties())

	// Test update with increment (merge)
	vt.UpdateVisibility(tensor, NewVisibleParties([]string{"bob"}))
	vis = vt.TensorVisibleParties(tensor)
	assert.ElementsMatch(t, []string{"alice", "bob"}, vis.GetParties())

	// Test nil tensor (should not panic)
	vt.UpdateVisibility(nil, NewVisibleParties([]string{"alice"}))

	// Test nil increment (should not panic)
	vt.UpdateVisibility(tensor, nil)

	// Verify no changes after nil inputs
	vis = vt.TensorVisibleParties(tensor)
	assert.ElementsMatch(t, []string{"alice", "bob"}, vis.GetParties())
}

func TestVisibilityTablePublicVisibility(t *testing.T) {
	parties := []string{"alice", "bob", "carol"}
	vt := NewVisibilityTable(parties)

	public := vt.PublicVisibility()
	assert.ElementsMatch(t, parties, public.GetParties())
}

func TestVisibilityTableIsTensorPublic(t *testing.T) {
	parties := []string{"alice", "bob", "carol"}
	vt := NewVisibilityTable(parties)

	tensor := &TensorMeta{ID: 1}

	// Test non-public tensor
	vt.UpdateVisibility(tensor, NewVisibleParties([]string{"alice"}))
	assert.False(t, vt.IsTensorPublic(tensor))

	// Test public tensor
	vt.UpdateVisibility(tensor, NewVisibleParties(parties))
	assert.True(t, vt.IsTensorPublic(tensor))

	// Test empty visibility
	emptyTensor := &TensorMeta{ID: 2}
	assert.False(t, vt.IsTensorPublic(emptyTensor))
}

func TestVisibilityTableTryUpdateVisibility(t *testing.T) {
	parties := []string{"alice", "bob"}
	vt := NewVisibilityTable(parties)

	tensor := &TensorMeta{ID: 1}

	// Test first update (should return true)
	updated := vt.TryUpdateVisibility(tensor, NewVisibleParties([]string{"alice"}))
	assert.True(t, updated)
	assert.Equal(t, []string{"alice"}, vt.TensorVisibleParties(tensor).GetParties())

	// Test redundant update (should return false)
	updated = vt.TryUpdateVisibility(tensor, NewVisibleParties([]string{"alice"}))
	assert.False(t, updated)
	assert.Equal(t, []string{"alice"}, vt.TensorVisibleParties(tensor).GetParties())

	// Test update with new party (should return true)
	updated = vt.TryUpdateVisibility(tensor, NewVisibleParties([]string{"bob"}))
	assert.True(t, updated)
	assert.ElementsMatch(t, []string{"alice", "bob"}, vt.TensorVisibleParties(tensor).GetParties())
}

func TestVisibilityTableVisibilityPublic(t *testing.T) {
	parties := []string{"alice", "bob", "carol"}
	vt := NewVisibilityTable(parties)

	// Test public visibility
	public := NewVisibleParties(parties)
	assert.True(t, vt.VisibilityPublic(public))

	// Test partial visibility
	partial := NewVisibleParties([]string{"alice", "bob"})
	assert.False(t, vt.VisibilityPublic(partial))

	// Test empty visibility
	empty := NewVisibleParties([]string{})
	assert.False(t, vt.VisibilityPublic(empty))

	// Test nil visibility
	assert.False(t, vt.VisibilityPublic(nil))
}

func TestVisibilityTableTensorRegistered(t *testing.T) {
	parties := []string{"alice", "bob"}
	vt := NewVisibilityTable(parties)

	tensor := &TensorMeta{ID: 1}

	// Test unregistered tensor
	assert.False(t, vt.TensorRegistered(tensor))

	// Test registered tensor
	vt.UpdateVisibility(tensor, NewVisibleParties([]string{"alice"}))
	assert.True(t, vt.TensorRegistered(tensor))
}

func TestVisibilityTableTensorVisibleTo(t *testing.T) {
	parties := []string{"alice", "bob", "carol"}
	vt := NewVisibilityTable(parties)

	tensor := &TensorMeta{ID: 1}
	vt.UpdateVisibility(tensor, NewVisibleParties([]string{"alice", "bob"}))
	assert.True(t, vt.TensorVisibleTo(tensor, "alice"))
	assert.True(t, vt.TensorVisibleTo(tensor, "bob"))
	assert.False(t, vt.TensorVisibleTo(tensor, "carol"))
}

func TestOverlayVisibilityTableBasic(t *testing.T) {
	parties := []string{"alice", "bob"}
	base := NewVisibilityTable(parties)

	// Create overlay
	overlay := &OverlayVisibilityTable{
		overlayVisibility: make(map[int]*VisibleParties),
		baseTable:         base,
	}

	tensor := &TensorMeta{ID: 1}

	// Test falls back to base table
	base.UpdateVisibility(tensor, NewVisibleParties([]string{"alice"}))
	vis := overlay.TensorVisibleParties(tensor)
	assert.Equal(t, []string{"alice"}, vis.GetParties())

	// Test update with OverlayVibilityTable
	overlay.UpdateVisibility(tensor, NewVisibleParties([]string{"bob"}))
	vis = overlay.TensorVisibleParties(tensor)
	assert.Equal(t, []string{"alice", "bob"}, vis.GetParties())

	// Base table unchanged
	vis = base.TensorVisibleParties(tensor)
	assert.Equal(t, []string{"alice"}, vis.GetParties())
}

func TestOverlayVisibilityTablePublicVisibility(t *testing.T) {
	parties := []string{"alice", "bob"}
	base := NewVisibilityTable(parties)

	overlay := &OverlayVisibilityTable{
		overlayVisibility: make(map[int]*VisibleParties),
		baseTable:         base,
	}

	// Public visibility comes from base table
	public := overlay.PublicVisibility()
	assert.ElementsMatch(t, parties, public.GetParties())
}

func TestOverlayVisibilityTableIsTensorPublic(t *testing.T) {
	parties := []string{"alice", "bob", "carol"}
	base := NewVisibilityTable(parties)

	overlay := &OverlayVisibilityTable{
		overlayVisibility: make(map[int]*VisibleParties),
		baseTable:         base,
	}

	tensor := &TensorMeta{ID: 1}

	// Test with overlay visibility
	overlay.UpdateVisibility(tensor, NewVisibleParties(parties))
	assert.True(t, overlay.IsTensorPublic(tensor))

	// Test with base table visibility
	overlay.overlayVisibility = make(map[int]*VisibleParties) // clear overlay
	base.UpdateVisibility(tensor, NewVisibleParties(parties))
	assert.True(t, overlay.IsTensorPublic(tensor))
}

func TestUpdateTensorsVisibilityLike(t *testing.T) {
	parties := []string{"alice", "bob"}
	source := NewVisibilityTable(parties)
	target := NewVisibilityTable(parties)

	tensors := []*TensorMeta{
		{ID: 1},
		{ID: 2},
		{ID: 3},
	}

	// Set up source visibility
	source.UpdateVisibility(tensors[0], NewVisibleParties([]string{"alice"}))
	source.UpdateVisibility(tensors[1], NewVisibleParties([]string{"bob"}))
	source.UpdateVisibility(tensors[2], NewVisibleParties([]string{"alice", "bob"}))

	// Set up target visibility
	target.UpdateVisibility(tensors[1], NewVisibleParties([]string{"alice"}))

	// Increase visibility
	target.UpdateTensorsVisibilityLike(source, tensors)

	// Check updated visibility
	assert.Equal(t, []string{"alice"}, target.TensorVisibleParties(tensors[0]).GetParties())
	assert.Equal(t, []string{"alice", "bob"}, target.TensorVisibleParties(tensors[1]).GetParties())
	assert.Equal(t, []string{"alice", "bob"}, target.TensorVisibleParties(tensors[2]).GetParties())

}

func TestEdgeCases(t *testing.T) {
	parties := []string{"alice", "bob"}
	vt := NewVisibilityTable(parties)

	// Test nil tensor inputs
	vt.UpdateVisibility(nil, NewVisibleParties([]string{"alice"})) // Should not panic
	assert.False(t, vt.IsTensorPublic(nil))                        // Should not panic

	// Test empty parties
	emptyVT := NewVisibilityTable([]string{})
	emptyPublic := emptyVT.PublicVisibility()
	assert.True(t, emptyPublic.IsEmpty())

	// Test with nil VisibleParties
	vt.UpdateVisibility(&TensorMeta{ID: 1}, nil) // Should not panic
	vis := vt.TensorVisibleParties(&TensorMeta{ID: 1})
	assert.True(t, vis.IsEmpty())
}
