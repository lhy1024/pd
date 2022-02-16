// Copyright 2022 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package heap

import "container/heap"

// TopNItem represents a single object in TopN.
type TopNItem interface {
	// ID is used to check identity.
	ID() uint64
	// Less tests whether the current item is less than the given argument in the `k`th dimension.
	Less(k int, than TopNItem) bool
}

// IndexedHeap is a heap with index.
type IndexedHeap struct {
	k     int
	rev   bool
	items []TopNItem
	index map[uint64]int
}

// NewMinHeap returns a min heap with the given hint.
func NewMinHeap(k, hint int) *IndexedHeap {
	return &IndexedHeap{
		k:     k,
		rev:   false,
		items: make([]TopNItem, 0, hint),
		index: map[uint64]int{},
	}
}

// NewMaxHeap returns a max heap with the given hint.
func NewMaxHeap(k, hint int) *IndexedHeap {
	return &IndexedHeap{
		k:     k,
		rev:   true,
		items: make([]TopNItem, 0, hint),
		index: map[uint64]int{},
	}
}

// Implementing heap.Interface.
func (hp *IndexedHeap) Len() int {
	return len(hp.items)
}

// Implementing heap.Interface.
func (hp *IndexedHeap) Less(i, j int) bool {
	if !hp.rev {
		return hp.items[i].Less(hp.k, hp.items[j])
	}
	return hp.items[j].Less(hp.k, hp.items[i])
}

// Implementing heap.Interface.
func (hp *IndexedHeap) Swap(i, j int) {
	lid := hp.items[i].ID()
	rid := hp.items[j].ID()
	hp.items[i], hp.items[j] = hp.items[j], hp.items[i]
	hp.index[lid] = j
	hp.index[rid] = i
}

// Implementing heap.Interface.
func (hp *IndexedHeap) Push(x interface{}) {
	item := x.(TopNItem)
	hp.index[item.ID()] = hp.Len()
	hp.items = append(hp.items, item)
}

// Implementing heap.Interface.
func (hp *IndexedHeap) Pop() interface{} {
	l := hp.Len()
	item := hp.items[l-1]
	hp.items = hp.items[:l-1]
	delete(hp.index, item.ID())
	return item
}

// Top returns the top item.
func (hp *IndexedHeap) Top() TopNItem {
	if hp.Len() <= 0 {
		return nil
	}
	return hp.items[0]
}

// Get returns item with the given ID.
func (hp *IndexedHeap) Get(id uint64) TopNItem {
	idx, ok := hp.index[id]
	if !ok {
		return nil
	}
	item := hp.items[idx]
	return item
}

// GetAll returns all the items.
func (hp *IndexedHeap) GetAll() []TopNItem {
	all := make([]TopNItem, len(hp.items))
	copy(all, hp.items)
	return all
}

// Put inserts item or updates the old item if it exists.
func (hp *IndexedHeap) Put(item TopNItem) (isUpdate bool) {
	if idx, ok := hp.index[item.ID()]; ok {
		hp.items[idx] = item
		heap.Fix(hp, idx)
		return true
	}
	heap.Push(hp, item)
	return false
}

// Remove deletes item by ID and returns it.
func (hp *IndexedHeap) Remove(id uint64) TopNItem {
	if idx, ok := hp.index[id]; ok {
		item := heap.Remove(hp, idx)
		return item.(TopNItem)
	}
	return nil
}
