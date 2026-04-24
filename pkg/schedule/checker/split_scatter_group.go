// Copyright 2026 TiKV Project Authors.
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

package checker

import (
	"bytes"

	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/core"
)

var (
	splitScatterTablePrefix = []byte{'t'}
	splitScatterIndexPrefix = []byte("_i")
)

func resolveSplitScatterRangeHint(region *core.RegionInfo) splitScatterRangeHint {
	rawPrefix, ok := resolveSplitScatterPrefix(region.GetStartKey(), region.GetEndKey())
	if !ok {
		return splitScatterRangeHint{}
	}
	return splitScatterPrefixRange(rawPrefix)
}

func resolveSplitScatterPrefix(startKey, endKey []byte) ([]byte, bool) {
	tablePrefix, rawPrefix, isIndex, ok := parseSplitScatterPrefix(startKey)
	if !ok {
		return nil, false
	}
	if isIndex {
		// We intentionally over-approximate ambiguous table-key ranges. If PD can
		// no longer prove the region stays within a single index prefix, it falls
		// back to the table-scoped group instead of dropping back to the family
		// group, so table-boundary splits and merged ranges still participate in
		// the broader scatter continuity/baseline.
		entityRange := splitScatterPrefixRange(rawPrefix)
		if len(endKey) == 0 || len(entityRange.startKey) == 0 || len(entityRange.endKey) == 0 || bytes.Compare(endKey, entityRange.endKey) > 0 {
			rawPrefix = tablePrefix
		}
	}
	return rawPrefix, true
}

func parseSplitScatterPrefix(key []byte) (tablePrefix, rawPrefix []byte, isIndex bool, ok bool) {
	_, decoded, err := codec.DecodeBytes(key)
	if err != nil || !bytes.HasPrefix(decoded, splitScatterTablePrefix) {
		// Keyspace-prefixed txn/raw keys (x... / r...) are not classified here yet
		// and will fall back to the family-scoped split-scatter group.
		return nil, nil, false, false
	}
	rest := decoded[len(splitScatterTablePrefix):]
	rest, tableID, err := codec.DecodeInt(rest)
	if err != nil {
		return nil, nil, false, false
	}

	tablePrefix = append([]byte(nil), codec.GenerateTableKey(tableID)...)
	rawPrefix = append([]byte(nil), tablePrefix...)
	if bytes.HasPrefix(rest, splitScatterIndexPrefix) {
		indexRest := rest[len(splitScatterIndexPrefix):]
		_, indexID, err := codec.DecodeInt(indexRest)
		if err != nil {
			return nil, nil, false, false
		}
		rawPrefix = append(rawPrefix, splitScatterIndexPrefix...)
		rawPrefix = codec.EncodeInt(rawPrefix, indexID)
		return tablePrefix, rawPrefix, true, true
	}
	return tablePrefix, rawPrefix, false, true
}

func splitScatterPrefixRange(rawPrefix []byte) splitScatterRangeHint {
	startKey := append([]byte(nil), codec.EncodeBytes(rawPrefix)...)
	endRawPrefix := splitScatterNextPrefix(rawPrefix)
	if len(endRawPrefix) == 0 {
		return splitScatterRangeHint{startKey: startKey}
	}
	return splitScatterRangeHint{
		startKey: startKey,
		endKey:   append([]byte(nil), codec.EncodeBytes(endRawPrefix)...),
	}
}

func splitScatterNextPrefix(key []byte) []byte {
	next := append([]byte(nil), key...)
	for i := len(next) - 1; i >= 0; i-- {
		if next[i] == 0xFF {
			continue
		}
		next[i]++
		return next[:i+1]
	}
	return nil
}
