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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSliceDeDup(t *testing.T) {
	r := require.New(t)
	r.Equal([]string{"a", "b"}, SliceDeDup([]string{"a", "b", "a", "b", "b"}))
	r.Equal([]int{1, 2, 3}, SliceDeDup([]int{1, 2, 1, 2, 2, 1, 3}))
	r.Equal([]float64{0.1, 0.2}, SliceDeDup([]float64{0.1, 0.2, 0.1}))
}

func TestAllOf(t *testing.T) {
	r := require.New(t)

	// Test with integers
	intSlice := []int{2, 4, 6, 8, 10}
	r.True(AllOf(intSlice, func(n int) bool { return n%2 == 0 }))
	r.False(AllOf(intSlice, func(n int) bool { return n > 5 }))

	// Test with strings
	strSlice := []string{"hello", "world", "hello"}
	r.True(AllOf(strSlice, func(s string) bool { return len(s) > 0 }))
	r.False(AllOf(strSlice, func(s string) bool { return s == "hello" }))

	// Test with empty slice
	emptySlice := []int{}
	r.True(AllOf(emptySlice, func(n int) bool { return n > 0 }))
}

func TestFilter(t *testing.T) {
	r := require.New(t)

	// Test with integers
	intSlice := []int{1, 2, 3, 4, 5, 6}
	evenNums := Filter(intSlice, func(n int) bool { return n%2 == 0 })
	r.Equal([]int{2, 4, 6}, evenNums)

	// Test with strings
	strSlice := []string{"hello", "world", "golang"}
	longStrs := Filter(strSlice, func(s string) bool { return len(s) > 5 })
	r.Equal([]string{"golang"}, longStrs)

	// Test with empty slice
	emptySlice := []int{}
	r.Nil(Filter(emptySlice, func(n int) bool { return n > 0 }))
}

func TestContainsAll(t *testing.T) {
	r := require.New(t)
	// Test string slices
	r.True(ContainsAll([]string{"a", "b", "c"}, []string{"a", "b", "c"}))       // Equal sets
	r.True(ContainsAll([]string{"a", "b", "c"}, []string{"a", "c"}))            // Proper subset
	r.True(ContainsAll([]string{"a", "b", "c"}, []string{"a", "a", "c"}))       // Subset with duplicates
	r.False(ContainsAll([]string{"a", "b", "c"}, []string{"a", "a", "c", "d"})) // Not a subset
	r.False(ContainsAll([]string{}, []string{"a", "a", "c", "d"}))              // Empty super set
	r.True(ContainsAll([]string{"a", "b", "c"}, []string{}))                    // Empty subset

	// Test int slices
	r.True(ContainsAll([]int{1, 2, 3}, []int{1, 2, 3}))    // Equal sets
	r.True(ContainsAll([]int{1, 2, 3}, []int{1, 2, 3, 3})) // Subset with duplicates
	r.False(ContainsAll([]int{1, 2}, []int{1, 2, 3, 3}))   // Not a subset
	r.False(ContainsAll([]int{}, []int{1, 2, 3, 3}))       // Empty super set
	r.True(ContainsAll([]int{1, 2, 3}, []int{}))           // Empty subset
	r.True(ContainsAll([]int{}, []int{}))                  // Both empty
}

func TestSubtraction(t *testing.T) {
	r := require.New(t)
	// Test subtraction of int slices
	a := []int{1, 2, 3, 4, 5}
	b := []int{2, 4}
	r.Equal([]int{1, 3, 5}, Subtraction(a, b))

	// Test subtraction of string slices
	c := []string{"a", "b", "c"}
	d := []string{"a", "d"}
	r.Equal([]string{"b", "c"}, Subtraction(c, d))
}

func TestExclude(t *testing.T) {
	r := require.New(t)

	// Test with integers
	intSlice := []int{1, 2, 3, 4, 5}
	r.Equal([]int{1, 2, 4, 5}, Exclude(intSlice, 3))

	// Test with strings
	strSlice := []string{"a", "b", "c", "d"}
	r.Equal([]string{"a", "b", "d"}, Exclude(strSlice, "c"))

	// Test with empty slice
	emptySlice := []int{}
	r.Empty(Exclude(emptySlice, 1))
}

func TestTake(t *testing.T) {
	r := require.New(t)

	slice := []int{1, 2, 3, 4, 5}
	predicate := []bool{true, false, true, false, true}

	result := Take(slice, predicate)
	r.Equal([]int{1, 3, 5}, result)

	// Test with empty slice
	emptySlice := []int{}
	emptyPredicate := []bool{}
	r.Empty(Take(emptySlice, emptyPredicate))
}

func TestInplaceTake(t *testing.T) {
	r := require.New(t)

	slice := []int{1, 2, 3, 4, 5}
	predicate := []bool{true, false, true, false, true}

	result := InplaceTake(slice, predicate)
	r.Equal([]int{1, 3, 5}, result)

	// Test with empty slice
	emptySlice := []int{}
	emptyPredicate := []bool{}
	r.Empty(InplaceTake(emptySlice, emptyPredicate))
}

func TestSortMapKeyForDeterminism(t *testing.T) {
	r := require.New(t)

	m := map[string]int{
		"c": 3,
		"a": 1,
		"b": 2,
	}

	keys := SortMapKeyForDeterminism(m)
	r.Equal([]string{"a", "b", "c"}, keys)

	// Test with empty map
	emptyMap := map[string]int{}
	r.Empty(SortMapKeyForDeterminism(emptyMap))
}

func TestSortedMap(t *testing.T) {
	r := require.New(t)

	m := map[string]int{
		"c": 3,
		"a": 1,
		"b": 2,
	}

	var pairs []struct {
		key   string
		value int
	}

	for k, v := range SortedMap(m) {
		pairs = append(pairs, struct {
			key   string
			value int
		}{k, v})
	}

	r.Equal([]struct {
		key   string
		value int
	}{
		{"a", 1},
		{"b", 2},
		{"c", 3},
	}, pairs)
}

func TestValueSortedByMapKey(t *testing.T) {
	r := require.New(t)

	m := map[string]int{
		"c": 3,
		"a": 1,
		"b": 2,
	}

	var values []int
	for v := range ValueSortedByMapKey(m) {
		values = append(values, v)
	}

	r.Equal([]int{1, 2, 3}, values)
}
