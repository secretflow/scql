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
	"iter"
	"maps"
	"slices"
)

// SliceDeDup removes duplicate elements from a slice.
func SliceDeDup[S ~[]E, E cmp.Ordered](s S) S {
	newS := slices.Clone(s)
	slices.Sort(newS)
	return slices.Compact(newS)
}

// AllOf returns true if all elements in slice satisfy the predicate function.
func AllOf[S ~[]E, E any](slice S, predicate func(E) bool) bool {
	for _, element := range slice {
		if !predicate(element) {
			return false
		}
	}
	return true
}

// Filter returns a new slice containing only elements that satisfy the predicate function.
// Returns nil if the input slice is empty.
func Filter[T any](slice []T, predicate func(T) bool) []T {
	if len(slice) == 0 {
		return nil
	}
	var result []T
	for _, item := range slice {
		if predicate(item) {
			result = append(result, item)
		}
	}
	return result
}

// Contains reports whether all elements in sub are present in super.
func ContainsAll[S ~[]E, E comparable](super S, sub S) bool {
	return AllOf(sub, func(element E) bool {
		return slices.Contains(super, element)
	})
}

// Subtraction returns a new slice containing elements in a that are not present in b.
func Subtraction[S ~[]E, E comparable](a S, b S) S {
	return Filter(a, func(element E) bool {
		return !slices.Contains(b, element)
	})
}

// Exclude returns a new slice containing elements in set that are not equal to excludeElement.
func Exclude[S ~[]E, E comparable](set S, excludeElement E) S {
	return Filter(set, func(element E) bool {
		return element != excludeElement
	})
}

// Take returns a new slice containing elements from slice where the corresponding element in predicate is true.
// Please ensure len(slice) <= len(predicate) before calling this function.
func Take[T any](slice []T, predicate []bool) []T {
	var result []T
	for i, item := range slice {
		if predicate[i] {
			result = append(result, item)
		}
	}
	return result
}

// InplaceTake modifies the slice in place to contain only elements where the corresponding element in predicate is true.
// Please ensure len(slice) <= len(predicate) before calling this function.
func InplaceTake[T any](slice []T, predicate []bool) []T {
	newIndex := 0
	for oldIndex := range slice {
		if predicate[oldIndex] {
			slice[newIndex] = slice[oldIndex]
			newIndex++
		}
	}
	clear(slice[newIndex:])
	return slice[:newIndex]
}

// SortMapKeyForDeterminism returns a slice of keys from a map in sorted order.
func SortMapKeyForDeterminism[k cmp.Ordered, v any](m map[k]v) []k {
	keys := maps.Keys(m)
	return slices.Sorted(keys)
}

// SortedMap returns an iterator that yields key-value pairs from a map in sorted key order
func SortedMap[k cmp.Ordered, v any](m map[k]v) iter.Seq2[k, v] {
	return func(yield func(k, v) bool) {
		keys := SortMapKeyForDeterminism(m)
		for _, key := range keys {
			if !yield(key, m[key]) {
				return
			}
		}
	}
}

// ValueSortedByMapKey returns an iterator that yields values from a map in sorted key order.
func ValueSortedByMapKey[k cmp.Ordered, v any](m map[k]v) iter.Seq[v] {
	return func(yield func(v) bool) {
		keys := SortMapKeyForDeterminism(m)
		for _, key := range keys {
			if !yield(m[key]) {
				return
			}
		}
	}
}
