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

import "container/heap"

// PriorityQueueItem represents an item in the priority queue
type PriorityQueueItem[T any] struct {
	value    T
	priority int
}

// PriorityQueue implements a priority queue using a max-heap
// Duplicates are not allowed and we use a map to track existing values
type PriorityQueue[T comparable] struct {
	items []*PriorityQueueItem[T]
	// existing is used to track existing values for O(1) duplicate checking
	existing map[T]struct{}
	// priorityFunc is used to calculate priority for items
	priorityFunc func(T) int
}

// NewPriorityQueue creates a new priority queue with a priority function
func NewPriorityQueue[T comparable](priorityFunc func(T) int) *PriorityQueue[T] {
	pq := &PriorityQueue[T]{
		existing:     make(map[T]struct{}),
		priorityFunc: priorityFunc,
	}
	heap.Init(pq)
	return pq
}

// Len returns the number of items in the priority queue
// for sort.Interface
func (pq *PriorityQueue[T]) Len() int {
	return len(pq.items)
}

// Less compares the priority of two items
// for sort.Interface
func (pq *PriorityQueue[T]) Less(i, j int) bool {
	return pq.items[i].priority > pq.items[j].priority
}

// Swap swaps two items in the priority queue
// for sort.Interface
func (pq *PriorityQueue[T]) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
}

// Push adds an item to the priority queue
// for heap.Interface
func (pq *PriorityQueue[T]) Push(x any) {
	item := x.(*PriorityQueueItem[T])
	pq.items = append(pq.items, item)
}

// Pop removes and returns the item with the highest priority
// for heap.Interface
func (pq *PriorityQueue[T]) Pop() any {
	old := pq.items
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	pq.items = old[0 : n-1]
	return item
}

// Enqueue adds a new item to the priority queue with the given value
func (pq *PriorityQueue[T]) Enqueue(value T) {
	// Check for duplicates using map
	if _, exists := pq.existing[value]; exists {
		return
	}

	item := &PriorityQueueItem[T]{
		value:    value,
		priority: pq.priorityFunc(value),
	}
	heap.Push(pq, item)
	pq.existing[value] = struct{}{}
}

// Dequeue removes and returns the item with the highest priority
func (pq *PriorityQueue[T]) Dequeue() (T, bool) {
	if pq.Len() == 0 {
		var zero T
		return zero, false
	}
	item := heap.Pop(pq).(*PriorityQueueItem[T])
	delete(pq.existing, item.value)
	return item.value, true
}
