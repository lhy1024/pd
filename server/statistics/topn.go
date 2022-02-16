// Copyright 2019 TiKV Project Authors.
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

package statistics

import (
	"container/heap"
	"container/list"
	"fmt"
	"sync"
	"time"

	. "github.com/tikv/pd/pkg/heap"
)

// TopN maintains the N largest items of multiple dimensions.
type TopN struct {
	rw     sync.RWMutex
	topns  []*singleTopN
	ttlLst *ttlList
}

// NewTopN returns a k-dimensional TopN with given TTL.
// NOTE: panic if k <= 0 or n <= 0.
func NewTopN(k, n int, ttl time.Duration) *TopN {
	if k <= 0 || n <= 0 {
		panic(fmt.Sprintf("invalid arguments for NewTopN: k = %d, n = %d", k, n))
	}
	ret := &TopN{
		topns:  make([]*singleTopN, k),
		ttlLst: newTTLList(ttl),
	}
	for i := 0; i < k; i++ {
		ret.topns[i] = newSingleTopN(i, n)
	}
	return ret
}

// Len returns number of all items.
func (tn *TopN) Len() int {
	tn.rw.RLock()
	defer tn.rw.RUnlock()
	return tn.ttlLst.Len()
}

// GetTopNMin returns the min item in top N of the `k`th dimension.
func (tn *TopN) GetTopNMin(k int) TopNItem {
	tn.rw.RLock()
	defer tn.rw.RUnlock()
	return tn.topns[k].GetTopNMin()
}

// GetAllTopN returns the top N items of the `k`th dimension.
func (tn *TopN) GetAllTopN(k int) []TopNItem {
	tn.rw.RLock()
	defer tn.rw.RUnlock()
	return tn.topns[k].GetAllTopN()
}

// GetAll returns all items.
func (tn *TopN) GetAll() []TopNItem {
	tn.rw.RLock()
	defer tn.rw.RUnlock()
	return tn.topns[0].GetAll()
}

// Get returns the item with given id, nil if there is no such item.
func (tn *TopN) Get(id uint64) TopNItem {
	tn.rw.RLock()
	defer tn.rw.RUnlock()
	return tn.topns[0].Get(id)
}

// Put inserts item or updates the old item if it exists.
func (tn *TopN) Put(item TopNItem) (isUpdate bool) {
	tn.rw.Lock()
	defer tn.rw.Unlock()
	for _, stn := range tn.topns {
		isUpdate = stn.Put(item)
	}
	tn.ttlLst.Put(item.ID())
	tn.maintain()
	return
}

// RemoveExpired deletes all expired items.
func (tn *TopN) RemoveExpired() {
	tn.rw.Lock()
	defer tn.rw.Unlock()
	tn.maintain()
}

// Remove deletes the item by given ID and returns it.
func (tn *TopN) Remove(id uint64) (item TopNItem) {
	tn.rw.Lock()
	defer tn.rw.Unlock()
	for _, stn := range tn.topns {
		item = stn.Remove(id)
	}
	_ = tn.ttlLst.Remove(id)
	tn.maintain()
	return
}

func (tn *TopN) maintain() {
	for _, id := range tn.ttlLst.TakeExpired() {
		for _, stn := range tn.topns {
			stn.Remove(id)
		}
	}
}

type singleTopN struct {
	k    int
	n    int
	topn *IndexedHeap
	rest *IndexedHeap
}

func newSingleTopN(k, n int) *singleTopN {
	return &singleTopN{
		k:    k,
		n:    n,
		topn: NewMinHeap(k, n),
		rest: NewMaxHeap(k, n),
	}
}

func (stn *singleTopN) Len() int {
	return stn.topn.Len() + stn.rest.Len()
}

func (stn *singleTopN) GetTopNMin() TopNItem {
	return stn.topn.Top()
}

func (stn *singleTopN) GetAllTopN() []TopNItem {
	return stn.topn.GetAll()
}

func (stn *singleTopN) GetAll() []TopNItem {
	topn := stn.topn.GetAll()
	return append(topn, stn.rest.GetAll()...)
}

func (stn *singleTopN) Get(id uint64) TopNItem {
	if item := stn.topn.Get(id); item != nil {
		return item
	}
	return stn.rest.Get(id)
}

func (stn *singleTopN) Put(item TopNItem) (isUpdate bool) {
	if stn.topn.Get(item.ID()) != nil {
		isUpdate = true
		stn.topn.Put(item)
	} else {
		isUpdate = stn.rest.Put(item)
	}
	stn.maintain()
	return
}

func (stn *singleTopN) Remove(id uint64) TopNItem {
	item := stn.topn.Remove(id)
	if item == nil {
		item = stn.rest.Remove(id)
	}
	stn.maintain()
	return item
}

func (stn *singleTopN) promote() {
	heap.Push(stn.topn, heap.Pop(stn.rest))
}

func (stn *singleTopN) demote() {
	heap.Push(stn.rest, heap.Pop(stn.topn))
}

func (stn *singleTopN) maintain() {
	for stn.topn.Len() < stn.n && stn.rest.Len() > 0 {
		stn.promote()
	}
	rest1 := stn.rest.Top()
	if rest1 == nil {
		return
	}
	for topn1 := stn.topn.Top(); topn1.Less(stn.k, rest1); {
		stn.demote()
		stn.promote()
		rest1 = stn.rest.Top()
		topn1 = stn.topn.Top()
	}
}

type ttlItem struct {
	id     uint64
	expire time.Time
}

type ttlList struct {
	ttl   time.Duration
	lst   *list.List
	index map[uint64]*list.Element
}

func newTTLList(ttl time.Duration) *ttlList {
	return &ttlList{
		ttl:   ttl,
		lst:   list.New(),
		index: map[uint64]*list.Element{},
	}
}

func (tl *ttlList) Len() int {
	return tl.lst.Len()
}

func (tl *ttlList) TakeExpired() []uint64 {
	expired := []uint64{}
	now := time.Now()
	for ele := tl.lst.Front(); ele != nil; ele = tl.lst.Front() {
		item := ele.Value.(ttlItem)
		if item.expire.After(now) {
			break
		}
		expired = append(expired, item.id)
		_ = tl.lst.Remove(ele)
		delete(tl.index, item.id)
	}
	return expired
}

func (tl *ttlList) Put(id uint64) (isUpdate bool) {
	item := ttlItem{id: id}
	if ele, ok := tl.index[id]; ok {
		isUpdate = true
		_ = tl.lst.Remove(ele)
	}
	item.expire = time.Now().Add(tl.ttl)
	tl.index[id] = tl.lst.PushBack(item)
	return
}

func (tl *ttlList) Remove(id uint64) (removed bool) {
	if ele, ok := tl.index[id]; ok {
		_ = tl.lst.Remove(ele)
		delete(tl.index, id)
		removed = true
	}
	return
}
