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
	"cmp"
	"slices"
)

func SliceDeDup[S ~[]E, E cmp.Ordered](s S) S {
	newS := slices.Clone(s)
	slices.Sort(newS)
	return slices.Compact(newS)
}

func SortMapKeyForDeterminism[k cmp.Ordered, v any](m map[k]v) []k {
	var keys []k
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
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

func Exclude[S ~[]E, E comparable](set S, excludeElement E) S {
	var result []E
	for _, element := range set {
		if element != excludeElement {
			result = append(result, element)
		}
	}
	return result
}

func Equal[S ~[]E, E cmp.Ordered](left S, right S) bool {
	slices.Sort(left)
	slices.Sort(right)
	return slices.Equal(left, right)
}

// please ensure len(slice) <= len(predicate) before calling this function.
func Take[T any](slice []T, predicate []bool) []T {
	var result []T
	for i, item := range slice {
		if predicate[i] {
			result = append(result, item)
		}
	}
	return result
}

// please ensure len(slice) <= len(predicate) before calling this function.
func InplaceTake[T any](slice []T, predicate []bool) []T {
	newIndex := 0
	for oldIndex := 0; oldIndex < len(slice); oldIndex++ {
		if predicate[oldIndex] {
			slice[newIndex] = slice[oldIndex]
			newIndex++
		}
	}
	clear(slice[newIndex:])
	return slice[:newIndex]
}
