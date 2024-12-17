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

package config

import (
	"encoding/json"
	"reflect"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"go.uber.org/zap"
)

var (
	// default region max size is 144MB
	defaultRegionMaxSize = uint64(144)
	// default region split size is 96MB
	defaultRegionSplitSize = uint64(96)
	// default bucket size is 96MB
	defaultBucketSize = uint64(96)
	// default region max key is 144000
	defaultRegionMaxKey = uint64(1440000)
	// default region split key is 960000
	defaultRegionSplitKey = uint64(960000)
	// RaftstoreV2 is the v2 raftstore engine mark.
	RaftstoreV2 = "raft-kv2"
)

// StoreConfig is the config of store like TiKV.
// generated by https://mholt.github.io/json-to-go/.
type StoreConfig struct {
	Coprocessor `json:"coprocessor"`
	Storage     `json:"storage"`

	RegionMaxSizeMB    uint64 `json:"-"`
	RegionSplitSizeMB  uint64 `json:"-"`
	RegionBucketSizeMB uint64 `json:"-"`
}

// Storage is the config for the tikv storage.
type Storage struct {
	Engine string `json:"engine"`
}

// Coprocessor is the config of coprocessor.
type Coprocessor struct {
	// RegionMaxSize is the max size of a region, if the region size is larger than this value, region will be
	// split by RegionSplitSize.
	RegionMaxSize string `json:"region-max-size"`
	// RegionSplitSize is the split size of a region, region will according to this value to split.
	RegionSplitSize    string `json:"region-split-size"`
	RegionMaxKeys      int    `json:"region-max-keys"`
	RegionSplitKeys    int    `json:"region-split-keys"`
	EnableRegionBucket bool   `json:"enable-region-bucket"`
	RegionBucketSize   string `json:"region-bucket-size"`
}

// Adjust adjusts the config to calculate some fields.
func (c *StoreConfig) Adjust() {
	if c == nil {
		return
	}

	c.RegionMaxSizeMB = typeutil.ParseMBFromText(c.RegionMaxSize, defaultRegionMaxSize)
	c.RegionSplitSizeMB = typeutil.ParseMBFromText(c.RegionSplitSize, defaultRegionSplitSize)
	c.RegionBucketSizeMB = typeutil.ParseMBFromText(c.RegionBucketSize, defaultBucketSize)
}

// Equal returns true if the two configs are equal.
func (c *StoreConfig) Equal(other *StoreConfig) bool {
	return reflect.DeepEqual(c.Coprocessor, other.Coprocessor) && reflect.DeepEqual(c.Storage, other.Storage)
}

// String implements fmt.Stringer interface.
func (c *StoreConfig) String() string {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return "<nil>"
	}
	return string(data)
}

// GetRegionMaxSize returns the max region size in MB
func (c *StoreConfig) GetRegionMaxSize() uint64 {
	if c == nil || len(c.RegionMaxSize) == 0 {
		return defaultRegionMaxSize
	}
	return c.RegionMaxSizeMB
}

// GetRegionSplitSize returns the region split size in MB
func (c *StoreConfig) GetRegionSplitSize() uint64 {
	if c == nil || len(c.RegionSplitSize) == 0 {
		return defaultRegionSplitSize
	}
	return c.RegionSplitSizeMB
}

// GetRegionSplitKeys returns the region split keys
func (c *StoreConfig) GetRegionSplitKeys() uint64 {
	if c == nil || c.RegionSplitKeys == 0 {
		return defaultRegionSplitKey
	}
	return uint64(c.Coprocessor.RegionSplitKeys)
}

// GetRegionMaxKeys returns the region split keys
func (c *StoreConfig) GetRegionMaxKeys() uint64 {
	if c == nil || c.RegionMaxKeys == 0 {
		return defaultRegionMaxKey
	}
	return uint64(c.RegionMaxKeys)
}

// IsEnableRegionBucket return true if the region bucket is enabled.
func (c *StoreConfig) IsEnableRegionBucket() bool {
	if c == nil {
		return false
	}
	return c.Coprocessor.EnableRegionBucket
}

// IsRaftKV2 returns true if the raft kv is v2.
func (c *StoreConfig) IsRaftKV2() bool {
	if c == nil {
		return false
	}
	return c.Storage.Engine == RaftstoreV2
}

// SetRegionBucketEnabled sets if the region bucket is enabled.
func (c *StoreConfig) SetRegionBucketEnabled(enabled bool) {
	if c == nil {
		return
	}
	c.Coprocessor.EnableRegionBucket = enabled
}

// GetRegionBucketSize returns region bucket size if enable region buckets.
func (c *StoreConfig) GetRegionBucketSize() uint64 {
	if c == nil || !c.Coprocessor.EnableRegionBucket {
		return 0
	}
	if len(c.RegionBucketSize) == 0 {
		return defaultBucketSize
	}
	return c.RegionBucketSizeMB
}

// CheckRegionSize return error if the smallest region's size is less than mergeSize
func (c *StoreConfig) CheckRegionSize(size, mergeSize uint64) error {
	// the merged region will not be split if it's size less than region max size.
	if size < c.GetRegionMaxSize() {
		return nil
	}
	// This could happen when the region split size is set to a value less than 1MiB,
	// which is a very extreme case, we just pass the check here to prevent panic.
	regionSplitSize := c.GetRegionSplitSize()
	if regionSplitSize == 0 {
		return nil
	}
	// the smallest of the split regions can not be merge again, so it's size should be less than merge size.
	if smallSize := size % regionSplitSize; smallSize <= mergeSize && smallSize != 0 {
		log.Debug("region size is too small", zap.Uint64("size", size), zap.Uint64("merge-size", mergeSize), zap.Uint64("small-size", smallSize))
		return errs.ErrCheckerMergeAgain.FastGenByArgs("the smallest region of the split regions is less than max-merge-region-size, " +
			"it will be merged again")
	}
	return nil
}

// CheckRegionKeys return error if the smallest region's keys is less than mergeKeys
func (c *StoreConfig) CheckRegionKeys(keys, mergeKeys uint64) error {
	if keys < c.GetRegionMaxKeys() {
		return nil
	}

	if smallKeys := keys % c.GetRegionSplitKeys(); smallKeys <= mergeKeys && smallKeys > 0 {
		log.Debug("region keys is too small", zap.Uint64("keys", keys), zap.Uint64("merge-keys", mergeKeys), zap.Uint64("smallSize", smallKeys))
		return errs.ErrCheckerMergeAgain.FastGenByArgs("the smallest region of the split regions is less than max-merge-region-keys")
	}
	return nil
}

// Clone makes a deep copy of the config.
func (c *StoreConfig) Clone() *StoreConfig {
	cfg := *c
	return &cfg
}
