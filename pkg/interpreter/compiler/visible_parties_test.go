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

func TestNewVisibleParties(t *testing.T) {
	// Test with duplicates
	parties := NewVisibleParties([]string{"alice", "bob", "alice", "carol"})
	assert.Equal(t, []string{"alice", "bob", "carol"}, parties.GetParties())
	assert.Len(t, parties.GetParties(), 3)

	// Test with empty slice
	empty := NewVisibleParties([]string{})
	assert.Empty(t, empty.GetParties())

	// Test with nil slice
	nilSlice := NewVisibleParties(nil)
	assert.Empty(t, nilSlice.GetParties())
}

func TestVisiblePartiesContains(t *testing.T) {
	parties := NewVisibleParties([]string{"alice", "bob"})
	assert.True(t, parties.Contains("alice"))
	assert.True(t, parties.Contains("bob"))
	assert.False(t, parties.Contains("carol"))
}

func TestVisiblePartiesCovers(t *testing.T) {
	aliceBob := NewVisibleParties([]string{"alice", "bob"})
	alice := NewVisibleParties([]string{"alice"})
	bobCarol := NewVisibleParties([]string{"bob", "carol"})

	assert.True(t, aliceBob.Covers(alice))
	assert.False(t, alice.Covers(aliceBob))
	assert.False(t, aliceBob.Covers(bobCarol))
}

func TestVisiblePartiesIsEmpty(t *testing.T) {
	empty := NewVisibleParties([]string{})
	nonEmpty := NewVisibleParties([]string{"alice"})

	assert.True(t, empty.IsEmpty())
	assert.False(t, nonEmpty.IsEmpty())
}

func TestVisiblePartiesGetOneParty(t *testing.T) {
	parties := NewVisibleParties([]string{"alice", "bob"})
	empty := NewVisibleParties([]string{})

	assert.Equal(t, "alice", parties.GetOneParty())
	assert.Equal(t, "", empty.GetOneParty())
}

func TestVisiblePartiesAddParty(t *testing.T) {
	parties := NewVisibleParties([]string{"alice"})
	parties.AddParty("bob")
	parties.AddParty("alice") // duplicate

	assert.Equal(t, []string{"alice", "bob"}, parties.GetParties())
}

func TestVisiblePartiesUpdateWith(t *testing.T) {
	base := NewVisibleParties([]string{"alice"})
	increment := NewVisibleParties([]string{"bob", "alice", "carol"})

	base.UpdateWith(increment)
	assert.Equal(t, []string{"alice", "bob", "carol"}, base.GetParties())
}

func TestVisiblePartiesCopyFrom(t *testing.T) {
	source := NewVisibleParties([]string{"alice", "bob"})
	dest := NewVisibleParties([]string{"carol"})

	dest.CopyFrom(source)
	assert.Equal(t, []string{"alice", "bob"}, dest.GetParties())
	assert.NotSame(t, source.GetParties(), dest.GetParties()) // ensure deep copy
}

func TestVisiblePartiesClone(t *testing.T) {
	original := NewVisibleParties([]string{"alice", "bob"})
	cloned := original.Clone()

	assert.Equal(t, original.GetParties(), cloned.GetParties())
	assert.NotSame(t, original.GetParties(), cloned.GetParties()) // ensure deep copy

	// Modify original to ensure independence
	original.AddParty("carol")
	assert.Len(t, cloned.GetParties(), 2)
}

func TestVPIntersection(t *testing.T) {
	a := NewVisibleParties([]string{"alice", "bob", "carol"})
	b := NewVisibleParties([]string{"bob", "carol", "dave"})

	result := VPIntersection(a, b)
	assert.Equal(t, []string{"bob", "carol"}, result.GetParties())
}

// Test nil handling
func TestVisiblePartiesNilHandling(t *testing.T) {
	var nilVP *VisibleParties

	// Test nil receiver
	assert.True(t, nilVP.IsEmpty())
	assert.Nil(t, nilVP.GetParties())
	assert.Equal(t, "", nilVP.GetOneParty())
	assert.False(t, nilVP.Contains("alice"))
	assert.False(t, nilVP.Covers(NewVisibleParties([]string{"alice"})))

	// Test nil parameters
	vp := NewVisibleParties([]string{"alice"})
	assert.False(t, vp.Covers(nil))
	vp.UpdateWith(nil)
	assert.Equal(t, []string{"alice"}, vp.GetParties())

	// Test VPIntersection with nil
	assert.NotNil(t, VPIntersection(nil, nil))
	assert.True(t, VPIntersection(nil, nil).IsEmpty())
	assert.True(t, VPIntersection(vp, nil).IsEmpty())
	assert.True(t, VPIntersection(nil, vp).IsEmpty())

	// Test CopyFrom with nil
	vp.CopyFrom(nil)
	assert.Empty(t, vp.GetParties())

	// Test Clone with nil
	assert.Nil(t, nilVP.Clone())
}
