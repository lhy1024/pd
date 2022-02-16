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

import (
	"testing"

	. "github.com/pingcap/check"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testIndexedHeapSuite{})

type testIndexedHeapSuite struct {
}

type testItem struct {
	id    uint64
	value float64
}

func (it *testItem) ID() uint64 {
	return it.id
}

func (it *testItem) Less(k int, than TopNItem) bool {
	return it.value < than.(*testItem).value
}

func NewTestItem(id uint64, value float64) *testItem {
	return &testItem{
		id:    id,
		value: value,
	}
}

func (s *testIndexedHeapSuite) Test(c *C) {
	h := NewMinHeap(0, 10)
	v := h.Top()
	c.Assert(v, Equals, nil)

	h.Put(NewTestItem(1, 1.0))
	h.Put(NewTestItem(2, 2.0))
	v = h.Top()
	c.Assert(v.(*testItem).value, Equals, 1.0)

	h.Put(NewTestItem(2, 0.0))
	v = h.Top()
	c.Assert(v.(*testItem).value, Equals, 0.0)

	h.Put(NewTestItem(1, -1.0))
	v = h.Top()
	c.Assert(v.(*testItem).value, Equals, -1.0)

	h = NewMaxHeap(0, 10)
	h.Put(NewTestItem(1, 1.0))
	h.Put(NewTestItem(2, 2.0))
	v = h.Top()
	c.Assert(v.(*testItem).value, Equals, 2.0)

	h.Put(NewTestItem(2, 3.0))
	v = h.Top()
	c.Assert(v.(*testItem).value, Equals, 3.0)

	h.Put(NewTestItem(1, 4.0))
	v = h.Top()
	c.Assert(v.(*testItem).value, Equals, 4.0)
}
