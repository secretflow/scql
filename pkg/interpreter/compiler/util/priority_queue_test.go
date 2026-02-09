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

package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPriorityQueue(t *testing.T) {
	// Test with integer values
	pq := NewPriorityQueue(func(x int) int {
		return x // Use the value itself as priority
	})

	// Test empty queue
	val, ok := pq.Dequeue()
	assert.False(t, ok)
	assert.Equal(t, 0, val)

	// Test enqueue and dequeue
	pq.Enqueue(3)
	pq.Enqueue(1)
	pq.Enqueue(4)
	pq.Enqueue(2)

	// Test dequeue order (should be in descending order)
	val, ok = pq.Dequeue()
	assert.True(t, ok)
	assert.Equal(t, 4, val)

	val, ok = pq.Dequeue()
	assert.True(t, ok)
	assert.Equal(t, 3, val)

	val, ok = pq.Dequeue()
	assert.True(t, ok)
	assert.Equal(t, 2, val)

	val, ok = pq.Dequeue()
	assert.True(t, ok)
	assert.Equal(t, 1, val)

	// Test empty queue after dequeue
	val, ok = pq.Dequeue()
	assert.False(t, ok)
	assert.Equal(t, 0, val)
}
