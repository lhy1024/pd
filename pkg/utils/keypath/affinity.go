// Copyright 2025 TiKV Project Authors.
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

package keypath

import (
	"fmt"
)

// AffinityLabelPrefix identifies label rules owned by the affinity Manager.
const AffinityLabelPrefix = "affinity_group/"

// AffinityRangeVersionPath identifies the global CAS version for affinity
// range/label writes. All such writes compare and increment this key in their
// transaction so stale cross-group overlap checks cannot commit concurrently.
func AffinityRangeVersionPath() string {
	return fmt.Sprintf("/pd/%d/affinity_range_version", ClusterID())
}

// AffinityGroupPath returns the path to save a specific affinity group object.
// Its format is: "/pd/{cluster_id}/affinity_groups/{group_id}"
func AffinityGroupPath(groupID string) string {
	return fmt.Sprintf(affinityGroupPathFormat, ClusterID(), groupID)
}

// AffinityGroupsPrefix returns the path prefix for all affinity groups.
// Its format is: "/pd/{cluster_id}/affinity_groups/"
// This is used for loading all groups via a range read.
func AffinityGroupsPrefix() string {
	// By passing an empty string, we get the prefix for all groups,
	// similar to RuleGroupPathPrefix()
	return AffinityGroupPath("")
}
