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

package command

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTiDBGroupKeyRange(t *testing.T) {
	re := require.New(t)
	// The expected keys are TiDB table prefixes t{id} encoded like Region keys,
	// as TiDB builds them for affinity groups. A keyspace adds the transaction
	// keyspace prefix x{keyspace ID in 3 bytes} before the encoding.
	for _, tc := range []struct {
		groupID    string
		start, end string
	}{
		{"_tidb_t_100", "7480000000000000ff6400000000000000f8", "7480000000000000ff6500000000000000f8"},
		{"_tidb_pt_100_p101", "7480000000000000ff6500000000000000f8", "7480000000000000ff6600000000000000f8"},
		{"_tidb_ks1_t_100", "7800000174800000ff0000000064000000fc", "7800000174800000ff0000000065000000fc"},
		{"_tidb_ks0_pt_100_p101", "7800000074800000ff0000000065000000fc", "7800000074800000ff0000000066000000fc"},
	} {
		keyRange, err := tidbGroupKeyRange(tc.groupID)
		re.NoError(err, tc.groupID)
		re.Equal(tc.start, hex.EncodeToString(keyRange.StartKey), tc.groupID)
		re.Equal(tc.end, hex.EncodeToString(keyRange.EndKey), tc.groupID)
	}

	for _, groupID := range []string{"_tidb_pg1_0", "_tidb_ks1_pg2_3", "_tidb_x_1", "_tidb_t_0", "_tidb_ks16777216_t_1", "_tidb_t_9223372036854775807"} {
		_, err := tidbGroupKeyRange(groupID)
		re.Error(err, groupID)
	}
}

func TestParseAffinityKeyRanges(t *testing.T) {
	re := require.New(t)
	ranges, err := parseAffinityKeyRanges([]string{"0a,0b", " 7480 , 7481 ", ","})
	re.NoError(err)
	re.Len(ranges, 3)
	re.Equal([]byte{0x0a}, ranges[0].StartKey)
	re.Equal([]byte{0x0b}, ranges[0].EndKey)
	re.Equal([]byte{0x74, 0x80}, ranges[1].StartKey)
	re.Empty(ranges[2].StartKey)
	re.Empty(ranges[2].EndKey)

	for _, raw := range []string{"0a", "0a,0b,0c", "zz,0b", "0a,zz", "0,1"} {
		_, err := parseAffinityKeyRanges([]string{raw})
		re.Error(err, raw)
	}
}
