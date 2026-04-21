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
	"fmt"

	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/core"
)

var (
	splitScatterTablePrefix  = []byte{'t'}
	splitScatterRecordPrefix = []byte("_r")
	splitScatterIndexPrefix  = []byte("_i")
)

type splitScatterEntityKind uint8

const (
	splitScatterEntityUnknown splitScatterEntityKind = iota
	splitScatterEntityRecord
	splitScatterEntityIndex
)

type splitScatterGroupHint struct {
	group     string
	rangeHint splitScatterRangeHint
}

type splitScatterEntity struct {
	kind      splitScatterEntityKind
	tableID   int64
	indexID   int64
	rawPrefix []byte
}

func resolveSplitScatterGroup(region *core.RegionInfo, fallbackGroup string) splitScatterGroupHint {
	entity, ok := resolveStrictSplitScatterEntity(region.GetStartKey(), region.GetEndKey())
	if !ok {
		return splitScatterGroupHint{group: fallbackGroup}
	}

	hint := splitScatterGroupHint{
		rangeHint: splitScatterPrefixRange(entity.rawPrefix),
	}
	switch entity.kind {
	case splitScatterEntityIndex:
		hint.group = fmt.Sprintf("split-scatter-index-%d-%d", entity.tableID, entity.indexID)
	case splitScatterEntityRecord:
		hint.group = fmt.Sprintf("split-scatter-record-%d", entity.tableID)
	default:
		hint.group = fallbackGroup
		hint.rangeHint = splitScatterRangeHint{}
	}
	if hint.group == "" {
		hint.group = fallbackGroup
	}
	return hint
}

func resolveStrictSplitScatterEntity(startKey, endKey []byte) (splitScatterEntity, bool) {
	entity, ok := parseSplitScatterEntity(startKey)
	if !ok {
		return splitScatterEntity{}, false
	}
	// We intentionally give up ambiguous ranges here. Without schema metadata or
	// split-key hints, PD cannot safely decide whether a bare table-boundary key
	// or a cross-entity/cross-table merged region belongs to a single index or
	// record space, so those regions fall back to the family-scoped group.
	if len(endKey) == 0 {
		return splitScatterEntity{}, false
	}
	entityRange := splitScatterPrefixRange(entity.rawPrefix)
	if !entityRange.valid() || len(entityRange.endKey) == 0 {
		return splitScatterEntity{}, false
	}
	if bytes.Compare(endKey, entityRange.endKey) > 0 {
		return splitScatterEntity{}, false
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

	rawPrefix := append([]byte(nil), splitScatterTablePrefix...)
	rawPrefix = codec.EncodeInt(rawPrefix, tableID)
	switch {
	case bytes.HasPrefix(rest, splitScatterRecordPrefix):
		rawPrefix = append(rawPrefix, splitScatterRecordPrefix...)
		return splitScatterEntity{
			kind:      splitScatterEntityRecord,
			tableID:   tableID,
			rawPrefix: rawPrefix,
		}, true
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
			indexID:   indexID,
			rawPrefix: rawPrefix,
		}, true
	default:
		return splitScatterEntity{}, false
	}
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
