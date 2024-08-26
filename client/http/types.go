// Copyright 2023 TiKV Project Authors.
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

package http

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	pd "github.com/tikv/pd/client"
)

// ServiceSafePoint is the safepoint for a specific service
// NOTE: This type is in sync with pd/pkg/storage/endpoint/gc_safe_point.go
type ServiceSafePoint struct {
	ServiceID string `json:"service_id"`
	ExpiredAt int64  `json:"expired_at"`
	SafePoint uint64 `json:"safe_point"`
}

// ListServiceGCSafepoint is the response for list service GC safepoint.
// NOTE: This type is in sync with pd/server/api/service_gc_safepoint.go
type ListServiceGCSafepoint struct {
	ServiceGCSafepoints   []*ServiceSafePoint `json:"service_gc_safe_points"`
	MinServiceGcSafepoint uint64              `json:"min_service_gc_safe_point,omitempty"`
	GCSafePoint           uint64              `json:"gc_safe_point"`
}

// ClusterState saves some cluster state information.
// NOTE: This type sync with https://github.com/tikv/pd/blob/5eae459c01a797cbd0c416054c6f0cad16b8740a/server/cluster/cluster.go#L173
type ClusterState struct {
	RaftBootstrapTime time.Time `json:"raft_bootstrap_time,omitempty"`
	ReplicationStatus string    `json:"replication_status"`
	IsInitialized     bool      `json:"is_initialized"`
}

// State is the status of PD server.
// NOTE: This type sync with https://github.com/tikv/pd/blob/1d77b25656bc18e1f5aa82337d4ab62a34b10087/pkg/versioninfo/versioninfo.go#L29
type State struct {
	BuildTS        string `json:"build_ts"`
	Version        string `json:"version"`
	GitHash        string `json:"git_hash"`
	StartTimestamp int64  `json:"start_timestamp"`
}

// KeyRange alias pd.KeyRange to avoid break client compatibility.
type KeyRange = pd.KeyRange

// NewKeyRange alias pd.NewKeyRange to avoid break client compatibility.
var NewKeyRange = pd.NewKeyRange

// NOTICE: the structures below are copied from the PD API definitions.
// Please make sure the consistency if any change happens to the PD API.

// RegionInfo stores the information of one region.
type RegionInfo struct {
	ReplicationStatus *ReplicationStatus `json:"replication_status,omitempty"`
	StartKey          string             `json:"start_key"`
	EndKey            string             `json:"end_key"`
	PendingPeers      []RegionPeer       `json:"pending_peers"`
	Peers             []RegionPeer       `json:"peers"`
	DownPeers         []RegionPeerStat   `json:"down_peers"`
	Leader            RegionPeer         `json:"leader"`
	Epoch             RegionEpoch        `json:"epoch"`
	WrittenBytes      uint64             `json:"written_bytes"`
	ReadBytes         uint64             `json:"read_bytes"`
	ApproximateSize   int64              `json:"approximate_size"`
	ApproximateKeys   int64              `json:"approximate_keys"`
	ID                int64              `json:"id"`
}

// GetStartKey gets the start key of the region.
func (r *RegionInfo) GetStartKey() string { return r.StartKey }

// GetEndKey gets the end key of the region.
func (r *RegionInfo) GetEndKey() string { return r.EndKey }

// RegionEpoch stores the information about its epoch.
type RegionEpoch struct {
	ConfVer int64 `json:"conf_ver"`
	Version int64 `json:"version"`
}

// RegionPeer stores information of one peer.
type RegionPeer struct {
	ID        int64 `json:"id"`
	StoreID   int64 `json:"store_id"`
	IsLearner bool  `json:"is_learner"`
}

// RegionPeerStat stores one field `DownSec` which indicates how long it's down than `RegionPeer`.
type RegionPeerStat struct {
	Peer    RegionPeer `json:"peer"`
	DownSec int64      `json:"down_seconds"`
}

// ReplicationStatus represents the replication mode status of the region.
type ReplicationStatus struct {
	State   string `json:"state"`
	StateID int64  `json:"state_id"`
}

// RegionsInfo stores the information of regions.
type RegionsInfo struct {
	Regions []RegionInfo `json:"regions"`
	Count   int64        `json:"count"`
}

func newRegionsInfo(count int64) *RegionsInfo {
	return &RegionsInfo{
		Count:   count,
		Regions: make([]RegionInfo, 0, count),
	}
}

// Merge merges two RegionsInfo together and returns a new one.
func (ri *RegionsInfo) Merge(other *RegionsInfo) *RegionsInfo {
	if ri == nil {
		ri = newRegionsInfo(0)
	}
	if other == nil {
		other = newRegionsInfo(0)
	}
	newRegionsInfo := newRegionsInfo(ri.Count + other.Count)
	m := make(map[int64]RegionInfo, ri.Count+other.Count)
	for _, region := range ri.Regions {
		m[region.ID] = region
	}
	for _, region := range other.Regions {
		m[region.ID] = region
	}
	for _, region := range m {
		newRegionsInfo.Regions = append(newRegionsInfo.Regions, region)
	}
	newRegionsInfo.Count = int64(len(newRegionsInfo.Regions))
	return newRegionsInfo
}

// StoreHotPeersInfos is used to get human-readable description for hot regions.
type StoreHotPeersInfos struct {
	AsPeer   StoreHotPeersStat `json:"as_peer"`
	AsLeader StoreHotPeersStat `json:"as_leader"`
}

// StoreHotPeersStat is used to record the hot region statistics group by store.
type StoreHotPeersStat map[uint64]*HotPeersStat

// HotPeersStat records all hot regions statistics
type HotPeersStat struct {
	Stats          []HotPeerStatShow `json:"statistics"`
	StoreByteRate  float64           `json:"store_bytes"`
	StoreKeyRate   float64           `json:"store_keys"`
	StoreQueryRate float64           `json:"store_query"`
	TotalBytesRate float64           `json:"total_flow_bytes"`
	TotalKeysRate  float64           `json:"total_flow_keys"`
	TotalQueryRate float64           `json:"total_flow_query"`
	Count          int               `json:"regions_count"`
}

// HotPeerStatShow records the hot region statistics for output
type HotPeerStatShow struct {
	LastUpdateTime time.Time `json:"last_update_time,omitempty"`
	Stores         []uint64  `json:"stores"`
	StoreID        uint64    `json:"store_id"`
	RegionID       uint64    `json:"region_id"`
	HotDegree      int       `json:"hot_degree"`
	ByteRate       float64   `json:"flow_bytes"`
	KeyRate        float64   `json:"flow_keys"`
	QueryRate      float64   `json:"flow_query"`
	AntiCount      int       `json:"anti_count"`
	IsLeader       bool      `json:"is_leader"`
	IsLearner      bool      `json:"is_learner"`
}

// HistoryHotRegionsRequest wrap the request conditions.
type HistoryHotRegionsRequest struct {
	RegionIDs      []uint64 `json:"region_ids,omitempty"`
	StoreIDs       []uint64 `json:"store_ids,omitempty"`
	PeerIDs        []uint64 `json:"peer_ids,omitempty"`
	IsLearners     []bool   `json:"is_learners,omitempty"`
	IsLeaders      []bool   `json:"is_leaders,omitempty"`
	HotRegionTypes []string `json:"hot_region_type,omitempty"`
	StartTime      int64    `json:"start_time,omitempty"`
	EndTime        int64    `json:"end_time,omitempty"`
}

// HistoryHotRegions wraps historyHotRegion
type HistoryHotRegions struct {
	HistoryHotRegion []*HistoryHotRegion `json:"history_hot_region"`
}

// HistoryHotRegion wraps hot region info
// it is storage format of hot_region_storage
type HistoryHotRegion struct {
	EncryptionMeta *encryptionpb.EncryptionMeta `json:"encryption_meta,omitempty"`
	HotRegionType  string                       `json:"hot_region_type"`
	EndKey         string                       `json:"end_key"`
	StartKey       string                       `json:"start_key"`
	StoreID        uint64                       `json:"store_id"`
	HotDegree      int64                        `json:"hot_degree"`
	FlowBytes      float64                      `json:"flow_bytes"`
	KeyRate        float64                      `json:"key_rate"`
	QueryRate      float64                      `json:"query_rate"`
	UpdateTime     int64                        `json:"update_time"`
	PeerID         uint64                       `json:"peer_id"`
	RegionID       uint64                       `json:"region_id"`
	IsLearner      bool                         `json:"is_learner"`
	IsLeader       bool                         `json:"is_leader"`
}

// StoresInfo represents the information of all TiKV/TiFlash stores.
type StoresInfo struct {
	Stores []StoreInfo `json:"stores"`
	Count  int         `json:"count"`
}

// StoreInfo represents the information of one TiKV/TiFlash store.
type StoreInfo struct {
	Store  MetaStore   `json:"store"`
	Status StoreStatus `json:"status"`
}

// MetaStore represents the meta information of one store.
type MetaStore struct {
	Address        string       `json:"address"`
	StateName      string       `json:"state_name"`
	Version        string       `json:"version"`
	StatusAddress  string       `json:"status_address"`
	GitHash        string       `json:"git_hash"`
	Labels         []StoreLabel `json:"labels"`
	ID             int64        `json:"id"`
	State          int64        `json:"state"`
	StartTimestamp int64        `json:"start_timestamp"`
}

// StoreLabel stores the information of one store label.
type StoreLabel struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// StoreStatus stores the detail information of one store.
type StoreStatus struct {
	LastHeartbeatTS time.Time `json:"last_heartbeat_ts"`
	StartTS         time.Time `json:"start_ts"`
	Capacity        string    `json:"capacity"`
	Available       string    `json:"available"`
	Uptime          string    `json:"uptime"`
	RegionCount     int64     `json:"region_count"`
	LeaderSize      int64     `json:"leader_size"`
	RegionWeight    float64   `json:"region_weight"`
	RegionScore     float64   `json:"region_score"`
	RegionSize      int64     `json:"region_size"`
	LeaderScore     float64   `json:"leader_score"`
	LeaderWeight    float64   `json:"leader_weight"`
	LeaderCount     int64     `json:"leader_count"`
}

// RegionStats stores the statistics of regions.
type RegionStats struct {
	StoreLeaderCount map[uint64]int `json:"store_leader_count"`
	StorePeerCount   map[uint64]int `json:"store_peer_count"`
	Count            int            `json:"count"`
	EmptyCount       int            `json:"empty_count"`
	StorageSize      int64          `json:"storage_size"`
	StorageKeys      int64          `json:"storage_keys"`
}

// PeerRoleType is the expected peer type of the placement rule.
type PeerRoleType string

const (
	// Voter can either match a leader peer or follower peer
	Voter PeerRoleType = "voter"
	// Leader matches a leader.
	Leader PeerRoleType = "leader"
	// Follower matches a follower.
	Follower PeerRoleType = "follower"
	// Learner matches a learner.
	Learner PeerRoleType = "learner"
)

// LabelConstraint is used to filter store when trying to place peer of a region.
type LabelConstraint struct {
	Key    string            `json:"key,omitempty"`
	Op     LabelConstraintOp `json:"op,omitempty"`
	Values []string          `json:"values,omitempty"`
}

// LabelConstraintOp defines how a LabelConstraint matches a store. It can be one of
// 'in', 'notIn', 'exists', or 'notExists'.
type LabelConstraintOp string

const (
	// In restricts the store label value should in the value list.
	// If label does not exist, `in` is always false.
	In LabelConstraintOp = "in"
	// NotIn restricts the store label value should not in the value list.
	// If label does not exist, `notIn` is always true.
	NotIn LabelConstraintOp = "notIn"
	// Exists restricts the store should have the label.
	Exists LabelConstraintOp = "exists"
	// NotExists restricts the store should not have the label.
	NotExists LabelConstraintOp = "notExists"
)

// Rule is the placement rule that can be checked against a region. When
// applying rules (apply means schedule regions to match selected rules), the
// apply order is defined by the tuple [GroupIndex, GroupID, Index, ID].
type Rule struct {
	EndKeyHex        string            `json:"end_key"`
	ID               string            `json:"id"`
	IsolationLevel   string            `json:"isolation_level,omitempty"`
	Role             PeerRoleType      `json:"role"`
	GroupID          string            `json:"group_id"`
	StartKeyHex      string            `json:"start_key"`
	StartKey         []byte            `json:"-"`
	EndKey           []byte            `json:"-"`
	LabelConstraints []LabelConstraint `json:"label_constraints,omitempty"`
	LocationLabels   []string          `json:"location_labels,omitempty"`
	Count            int               `json:"count"`
	Index            int               `json:"index,omitempty"`
	Version          uint64            `json:"version,omitempty"`
	CreateTimestamp  uint64            `json:"create_timestamp,omitempty"`
	Override         bool              `json:"override,omitempty"`
	IsWitness        bool              `json:"is_witness"`
}

// String returns the string representation of this rule.
func (r *Rule) String() string {
	b, _ := json.Marshal(r)
	return string(b)
}

// Clone returns a copy of Rule.
func (r *Rule) Clone() *Rule {
	var clone Rule
	_ = json.Unmarshal([]byte(r.String()), &clone)
	clone.StartKey = append(r.StartKey[:0:0], r.StartKey...)
	clone.EndKey = append(r.EndKey[:0:0], r.EndKey...)
	return &clone
}

var (
	_ json.Marshaler   = (*Rule)(nil)
	_ json.Unmarshaler = (*Rule)(nil)
)

// This is a helper struct used to customizing the JSON marshal/unmarshal methods of `Rule`.
type rule struct {
	GroupID          string            `json:"group_id"`
	ID               string            `json:"id"`
	StartKeyHex      string            `json:"start_key"`
	EndKeyHex        string            `json:"end_key"`
	Role             PeerRoleType      `json:"role"`
	IsolationLevel   string            `json:"isolation_level,omitempty"`
	LabelConstraints []LabelConstraint `json:"label_constraints,omitempty"`
	LocationLabels   []string          `json:"location_labels,omitempty"`
	Index            int               `json:"index,omitempty"`
	Count            int               `json:"count"`
	Override         bool              `json:"override,omitempty"`
	IsWitness        bool              `json:"is_witness"`
}

// MarshalJSON implements `json.Marshaler` interface to make sure we could set the correct start/end key.
func (r *Rule) MarshalJSON() ([]byte, error) {
	tempRule := &rule{
		GroupID:          r.GroupID,
		ID:               r.ID,
		Index:            r.Index,
		Override:         r.Override,
		StartKeyHex:      r.StartKeyHex,
		EndKeyHex:        r.EndKeyHex,
		Role:             r.Role,
		IsWitness:        r.IsWitness,
		Count:            r.Count,
		LabelConstraints: r.LabelConstraints,
		LocationLabels:   r.LocationLabels,
		IsolationLevel:   r.IsolationLevel,
	}
	// Converts the start/end key to hex format if the corresponding hex field is empty.
	if len(r.StartKey) > 0 && len(r.StartKeyHex) == 0 {
		tempRule.StartKeyHex = rawKeyToKeyHexStr(r.StartKey)
	}
	if len(r.EndKey) > 0 && len(r.EndKeyHex) == 0 {
		tempRule.EndKeyHex = rawKeyToKeyHexStr(r.EndKey)
	}
	return json.Marshal(tempRule)
}

// UnmarshalJSON implements `json.Unmarshaler` interface to make sure we could get the correct start/end key.
func (r *Rule) UnmarshalJSON(bytes []byte) error {
	var tempRule rule
	err := json.Unmarshal(bytes, &tempRule)
	if err != nil {
		return err
	}
	newRule := Rule{
		GroupID:          tempRule.GroupID,
		ID:               tempRule.ID,
		Index:            tempRule.Index,
		Override:         tempRule.Override,
		StartKeyHex:      tempRule.StartKeyHex,
		EndKeyHex:        tempRule.EndKeyHex,
		Role:             tempRule.Role,
		IsWitness:        tempRule.IsWitness,
		Count:            tempRule.Count,
		LabelConstraints: tempRule.LabelConstraints,
		LocationLabels:   tempRule.LocationLabels,
		IsolationLevel:   tempRule.IsolationLevel,
	}
	newRule.StartKey, err = keyHexStrToRawKey(newRule.StartKeyHex)
	if err != nil {
		return err
	}
	newRule.EndKey, err = keyHexStrToRawKey(newRule.EndKeyHex)
	if err != nil {
		return err
	}
	*r = newRule
	return nil
}

// RuleOpType indicates the operation type
type RuleOpType string

const (
	// RuleOpAdd a placement rule, only need to specify the field *Rule
	RuleOpAdd RuleOpType = "add"
	// RuleOpDel a placement rule, only need to specify the field `GroupID`, `ID`, `MatchID`
	RuleOpDel RuleOpType = "del"
)

// RuleOp is for batching placement rule actions.
// The action type is distinguished by the field `Action`.
type RuleOp struct {
	*Rule                       // information of the placement rule to add/delete the operation type
	Action           RuleOpType `json:"action"`
	DeleteByIDPrefix bool       `json:"delete_by_id_prefix"` // if action == delete, delete by the prefix of id
}

func (r RuleOp) String() string {
	b, _ := json.Marshal(r)
	return string(b)
}

var (
	_ json.Marshaler   = (*RuleOp)(nil)
	_ json.Unmarshaler = (*RuleOp)(nil)
)

// This is a helper struct used to customizing the JSON marshal/unmarshal methods of `RuleOp`.
type ruleOp struct {
	GroupID          string            `json:"group_id"`
	ID               string            `json:"id"`
	Action           RuleOpType        `json:"action"`
	IsolationLevel   string            `json:"isolation_level,omitempty"`
	StartKeyHex      string            `json:"start_key"`
	EndKeyHex        string            `json:"end_key"`
	Role             PeerRoleType      `json:"role"`
	LabelConstraints []LabelConstraint `json:"label_constraints,omitempty"`
	LocationLabels   []string          `json:"location_labels,omitempty"`
	Count            int               `json:"count"`
	Index            int               `json:"index,omitempty"`
	IsWitness        bool              `json:"is_witness"`
	Override         bool              `json:"override,omitempty"`
	DeleteByIDPrefix bool              `json:"delete_by_id_prefix"`
}

// MarshalJSON implements `json.Marshaler` interface to make sure we could set the correct start/end key.
func (r *RuleOp) MarshalJSON() ([]byte, error) {
	tempRuleOp := &ruleOp{
		GroupID:          r.GroupID,
		ID:               r.ID,
		Index:            r.Index,
		Override:         r.Override,
		StartKeyHex:      r.StartKeyHex,
		EndKeyHex:        r.EndKeyHex,
		Role:             r.Role,
		IsWitness:        r.IsWitness,
		Count:            r.Count,
		LabelConstraints: r.LabelConstraints,
		LocationLabels:   r.LocationLabels,
		IsolationLevel:   r.IsolationLevel,
		Action:           r.Action,
		DeleteByIDPrefix: r.DeleteByIDPrefix,
	}
	// Converts the start/end key to hex format if the corresponding hex field is empty.
	if len(r.StartKey) > 0 && len(r.StartKeyHex) == 0 {
		tempRuleOp.StartKeyHex = rawKeyToKeyHexStr(r.StartKey)
	}
	if len(r.EndKey) > 0 && len(r.EndKeyHex) == 0 {
		tempRuleOp.EndKeyHex = rawKeyToKeyHexStr(r.EndKey)
	}
	return json.Marshal(tempRuleOp)
}

// UnmarshalJSON implements `json.Unmarshaler` interface to make sure we could get the correct start/end key.
func (r *RuleOp) UnmarshalJSON(bytes []byte) error {
	var tempRuleOp ruleOp
	err := json.Unmarshal(bytes, &tempRuleOp)
	if err != nil {
		return err
	}
	newRuleOp := RuleOp{
		Rule: &Rule{
			GroupID:          tempRuleOp.GroupID,
			ID:               tempRuleOp.ID,
			Index:            tempRuleOp.Index,
			Override:         tempRuleOp.Override,
			StartKeyHex:      tempRuleOp.StartKeyHex,
			EndKeyHex:        tempRuleOp.EndKeyHex,
			Role:             tempRuleOp.Role,
			IsWitness:        tempRuleOp.IsWitness,
			Count:            tempRuleOp.Count,
			LabelConstraints: tempRuleOp.LabelConstraints,
			LocationLabels:   tempRuleOp.LocationLabels,
			IsolationLevel:   tempRuleOp.IsolationLevel,
		},
		Action:           tempRuleOp.Action,
		DeleteByIDPrefix: tempRuleOp.DeleteByIDPrefix,
	}
	newRuleOp.StartKey, err = keyHexStrToRawKey(newRuleOp.StartKeyHex)
	if err != nil {
		return err
	}
	newRuleOp.EndKey, err = keyHexStrToRawKey(newRuleOp.EndKeyHex)
	if err != nil {
		return err
	}
	*r = newRuleOp
	return nil
}

// RuleGroup defines properties of a rule group.
type RuleGroup struct {
	ID       string `json:"id,omitempty"`
	Index    int    `json:"index,omitempty"`
	Override bool   `json:"override,omitempty"`
}

func (g *RuleGroup) String() string {
	b, _ := json.Marshal(g)
	return string(b)
}

// GroupBundle represents a rule group and all rules belong to the group.
type GroupBundle struct {
	ID       string  `json:"group_id"`
	Rules    []*Rule `json:"rules"`
	Index    int     `json:"group_index"`
	Override bool    `json:"group_override"`
}

// RegionLabel is the label of a region.
type RegionLabel struct {
	Key     string `json:"key"`
	Value   string `json:"value"`
	TTL     string `json:"ttl,omitempty"`
	StartAt string `json:"start_at,omitempty"`
}

// LabelRule is the rule to assign labels to a region.
type LabelRule struct {
	Data     any           `json:"data"`
	ID       string        `json:"id"`
	RuleType string        `json:"rule_type"`
	Labels   []RegionLabel `json:"labels"`
	Index    int           `json:"index"`
}

// LabelRulePatch is the patch to update the label rules.
type LabelRulePatch struct {
	SetRules    []*LabelRule `json:"sets"`
	DeleteRules []string     `json:"deletes"`
}

// MembersInfo is PD members info returned from PD RESTful interface
// type Members map[string][]*pdpb.Member
type MembersInfo struct {
	Header     *pdpb.ResponseHeader `json:"header,omitempty"`
	Leader     *pdpb.Member         `json:"leader,omitempty"`
	EtcdLeader *pdpb.Member         `json:"etcd_leader,omitempty"`
	Members    []*pdpb.Member       `json:"members,omitempty"`
}

// MicroServiceMember is the member info of a micro service.
type MicroServiceMember struct {
	ServiceAddr    string `json:"service-addr"`
	Version        string `json:"version"`
	GitHash        string `json:"git-hash"`
	DeployPath     string `json:"deploy-path"`
	StartTimestamp int64  `json:"start-timestamp"`
}

// KeyspaceGCManagementType represents parameters needed to modify the gc management type.
// If `gc_management_type` is `global_gc`, it means the current keyspace requires a tidb without 'keyspace-name'
// configured to run a global gc worker to calculate a global gc safe point.
// If `gc_management_type` is `keyspace_level_gc` it means the current keyspace can calculate gc safe point by its own.
type KeyspaceGCManagementType struct {
	GCManagementType string `json:"gc_management_type,omitempty"`
}

// KeyspaceGCManagementTypeConfig represents parameters needed to modify target keyspace's configs.
type KeyspaceGCManagementTypeConfig struct {
	Config KeyspaceGCManagementType `json:"config"`
}

// tempKeyspaceMeta is the keyspace meta struct that returned from the http interface.
type tempKeyspaceMeta struct {
	Config         map[string]string `json:"config"`
	Name           string            `json:"name"`
	State          string            `json:"state"`
	CreatedAt      int64             `json:"created_at"`
	StateChangedAt int64             `json:"state_changed_at"`
	ID             uint32            `json:"id"`
}

func stringToKeyspaceState(str string) (keyspacepb.KeyspaceState, error) {
	switch str {
	case "ENABLED":
		return keyspacepb.KeyspaceState_ENABLED, nil
	case "DISABLED":
		return keyspacepb.KeyspaceState_DISABLED, nil
	case "ARCHIVED":
		return keyspacepb.KeyspaceState_ARCHIVED, nil
	case "TOMBSTONE":
		return keyspacepb.KeyspaceState_TOMBSTONE, nil
	default:
		return keyspacepb.KeyspaceState(0), fmt.Errorf("invalid KeyspaceState string: %s", str)
	}
}

// Health reflects the cluster's health.
// NOTE: This type is moved from `server/api/health.go`, maybe move them to the same place later.
type Health struct {
	Name       string   `json:"name"`
	ClientUrls []string `json:"client_urls"`
	MemberID   uint64   `json:"member_id"`
	Health     bool     `json:"health"`
}
