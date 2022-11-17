// Copyright 2020 TiKV Project Authors.
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

package movingaverage

import (
	"sort"
	"sync"
)

// MedianFilter works as a median filter with specified window size.
// There are at most `size` data points for calculating.
// References: https://en.wikipedia.org/wiki/Median_filter.
type MedianFilter struct {
	sync.RWMutex
	records       []float64
	sortCopy      []float64
	size          uint64
	count         uint64
	instantaneous float64
	isUpdated     bool
	result        float64
}

// NewMedianFilter returns a MedianFilter.
func NewMedianFilter(size int) *MedianFilter {
	return &MedianFilter{
		records:   make([]float64, size),
		sortCopy:  make([]float64, size),
		size:      uint64(size),
		isUpdated: false,
		result:    0,
	}
}

// Add adds a data point.
func (r *MedianFilter) Add(n float64) {
	r.Lock()
	defer r.Unlock()
	r.instantaneous = n
	r.records[r.count%r.size] = n
	r.count++
	r.isUpdated = true
}

// Get returns the median of the data set.
func (r *MedianFilter) Get() float64 {
	r.Lock()
	defer r.Unlock()
	if r.count == 0 {
		return 0
	}
	if !r.isUpdated {
		return r.result
	}
	copy(r.sortCopy, r.records)
	index := r.size / 2
	sortCopy := r.sortCopy
	if r.count < r.size {
		index = r.count / 2
		sortCopy = r.sortCopy[:r.count]
	}
	sort.Float64s(sortCopy)
	r.result = sortCopy[index]
	r.isUpdated = false
	return r.result
}

// Reset cleans the data set.
func (r *MedianFilter) Reset() {
	r.Lock()
	defer r.Unlock()
	r.instantaneous = 0
	r.count = 0
	r.isUpdated = true
}

// Set = Reset + Add.
func (r *MedianFilter) Set(n float64) {
	r.Lock()
	defer r.Unlock()
	r.instantaneous = n
	r.records[0] = n
	r.count = 1
	r.isUpdated = true
}

// GetInstantaneous returns the value just added.
func (r *MedianFilter) GetInstantaneous() float64 {
	r.RLock()
	defer r.RUnlock()
	return r.instantaneous
}

// Clone returns a copy of MedianFilter
func (r *MedianFilter) Clone() *MedianFilter {
	r.RLock()
	defer r.RUnlock()
	records := make([]float64, len(r.records))
	copy(records, r.records)
	return &MedianFilter{
		records:       records,
		sortCopy:      make([]float64, len(r.sortCopy)),
		size:          r.size,
		count:         r.count,
		instantaneous: r.instantaneous,
		isUpdated:     r.isUpdated,
		result:        r.result,
	}
}
