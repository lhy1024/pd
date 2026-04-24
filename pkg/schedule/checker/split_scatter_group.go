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

type splitScatterEntityKind uint8

const (
	splitScatterEntityTable splitScatterEntityKind = iota + 1
	splitScatterEntityIndex
)

type splitScatterEntity struct {
	kind      splitScatterEntityKind
	tableID   int64
	rawPrefix []byte
}

func resolveSplitScatterRangeHint(region *core.RegionInfo) splitScatterRangeHint {
	entity, ok := resolveSplitScatterEntity(region.GetStartKey(), region.GetEndKey())
	if !ok {
		return splitScatterRangeHint{}
	}
	return splitScatterPrefixRange(entity.rawPrefix)
}

func resolveSplitScatterEntity(startKey, endKey []byte) (splitScatterEntity, bool) {
	entity, ok := parseSplitScatterEntity(startKey)
	if !ok {
		return splitScatterEntity{}, false
	}
	if entity.kind == splitScatterEntityIndex {
		// We intentionally over-approximate ambiguous table-key ranges. If PD can
		// no longer prove the region stays within a single index prefix, it falls
		// back to the table-scoped group instead of dropping back to the family
		// group, so table-boundary splits and merged ranges still participate in
		// the broader scatter continuity/baseline.
		entityRange := splitScatterPrefixRange(entity.rawPrefix)
		if len(endKey) == 0 || !entityRange.valid() || len(entityRange.endKey) == 0 || bytes.Compare(endKey, entityRange.endKey) > 0 {
			entity = splitScatterTableEntity(entity.tableID)
		}
	}
	return entity, true
}

func parseSplitScatterEntity(key []byte) (splitScatterEntity, bool) {
	_, decoded, err := codec.DecodeBytes(key)
	if err != nil || !bytes.HasPrefix(decoded, splitScatterTablePrefix) {
		// Keyspace-prefixed txn/raw keys (x... / r...) are not classified here yet
		// and will fall back to the family-scoped split-scatter group.
		return splitScatterEntity{}, false
	}
	rest := decoded[len(splitScatterTablePrefix):]
	rest, tableID, err := codec.DecodeInt(rest)
	if err != nil {
		return splitScatterEntity{}, false
	}

	rawPrefix := splitScatterTablePrefixKey(tableID)
	switch {
	case bytes.HasPrefix(rest, splitScatterIndexPrefix):
		indexRest := rest[len(splitScatterIndexPrefix):]
		_, indexID, err := codec.DecodeInt(indexRest)
		if err != nil {
			return splitScatterEntity{}, false
		}
		rawPrefix = append(rawPrefix, splitScatterIndexPrefix...)
		rawPrefix = codec.EncodeInt(rawPrefix, indexID)
		return splitScatterEntity{
			kind:      splitScatterEntityIndex,
			tableID:   tableID,
			rawPrefix: rawPrefix,
		}, true
	default:
		return splitScatterTableEntity(tableID), true
	}
}

func splitScatterTableEntity(tableID int64) splitScatterEntity {
	rawPrefix := splitScatterTablePrefixKey(tableID)
	return splitScatterEntity{
		kind:      splitScatterEntityTable,
		tableID:   tableID,
		rawPrefix: rawPrefix,
	}
}

func splitScatterTablePrefixKey(tableID int64) []byte {
	return append([]byte(nil), codec.GenerateTableKey(tableID)...)
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
