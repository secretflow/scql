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
	"slices"
	"sort"
	"strings"

	"github.com/secretflow/scql/pkg/util/sliceutil"
)

type VisibleParties struct {
	parties []string
}

func NewVisibleParties(parties []string) *VisibleParties {
	return &VisibleParties{
		parties: sliceutil.SliceDeDup(parties),
	}
}

func (vp *VisibleParties) String() string {
	if vp == nil {
		return ""
	}
	return strings.Join(vp.parties, ",")
}

func (vp *VisibleParties) Clone() *VisibleParties {
	if vp == nil {
		return nil
	}
	return &VisibleParties{
		parties: append([]string{}, vp.parties...),
	}
}

func (vp *VisibleParties) Contains(party string) bool {
	if vp == nil {
		return false
	}
	return slices.Contains(vp.parties, party)
}

// Covers returns true if this VisibleParties contains all parties present in other.
// Returns false if other contains any party not present in this instance.
// Returns false if either VisibleParties is nil.
func (vp *VisibleParties) Covers(other *VisibleParties) bool {
	if vp == nil || other == nil {
		return false
	}
	for _, party := range other.parties {
		if !vp.Contains(party) {
			return false
		}
	}
	return true
}

func (vp *VisibleParties) IsEmpty() bool {
	if vp == nil {
		return true
	}
	return len(vp.parties) == 0
}

func (vp *VisibleParties) GetParties() []string {
	if vp == nil {
		return nil
	}
	return append([]string{}, vp.parties...)
}

// GetOneParty returns the first party from the list, or empty string if no parties exist.
// The order is not guaranteed unless the list has been sorted.
func (vp *VisibleParties) GetOneParty() string {
	if vp == nil || len(vp.parties) == 0 {
		return ""
	}
	return vp.parties[0]
}

// AddParty adds a party to the list if it doesn't already exist.
func (vp *VisibleParties) AddParty(party string) {
	if vp == nil {
		return
	}
	if !slices.Contains(vp.parties, party) {
		vp.parties = append(vp.parties, party)
	}
}

// UpdateWith merges all parties from increment into thisVisibleParties.
// Duplicate parties are filtered out and the final list is sorted alphabetically.
func (vp *VisibleParties) UpdateWith(increment *VisibleParties) {
	if vp == nil || increment == nil {
		return
	}
	for _, party := range increment.parties {
		vp.AddParty(party)
	}
	sort.Strings(vp.parties)
}

func (vp *VisibleParties) CopyFrom(ref *VisibleParties) {
	if vp == nil {
		return
	}
	if ref == nil {
		vp.parties = nil
		return
	}
	vp.parties = append([]string{}, ref.parties...)
}

func VPIntersection(a, b *VisibleParties) *VisibleParties {
	if a == nil || b == nil {
		return NewVisibleParties(nil)
	}
	return NewVisibleParties(sliceutil.Intersection(a.parties, b.parties))
}
