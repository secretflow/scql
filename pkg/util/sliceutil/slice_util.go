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

package sliceutil

import (
	"sort"

	"golang.org/x/exp/constraints"
	"golang.org/x/exp/slices"
)

func SliceDeDup[S ~[]E, E constraints.Ordered](s S) S {
	newS := slices.Clone(s)
	sortSlice(newS)
	return slices.Compact(newS)
}

func sortSlice[T constraints.Ordered](s []T) {
	sort.Slice(s, func(i, j int) bool {
		return s[i] < s[j]
	})
}

func SortMapKeyForDeterminism[k constraints.Ordered, v any](m map[k]v) []k {
	var keys []k
	for k := range m {
		keys = append(keys, k)
	}
	sortSlice(keys)
	return keys
}

// Contains reports whether all elements in sub are present in super.
func ContainsAll[S ~[]E, E comparable](super S, sub S) bool {
	for _, element := range sub {
		if !slices.Contains(super, element) {
			return false
		}
	}
	return true
}

func Subtraction[S ~[]E, E comparable](a S, b S) S {
	var result []E
	for _, element := range a {
		if !slices.Contains(b, element) {
			result = append(result, element)
		}
	}
	return result
}

func Equal[S ~[]E, E constraints.Ordered](left S, right S) bool {
	sortSlice(left)
	sortSlice(right)
	return slices.Equal(left, right)
}
