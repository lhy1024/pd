// Copyright 2024 TiKV Project Authors.
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

package types

type CheckerSchedulerType string

func (n CheckerSchedulerType) String() string {
	return string(n)
}

const (
	// JointStateChecker is the name for joint state checker.
	JointStateChecker CheckerSchedulerType = "joint-state-checker"
	// LearnerChecker is the name for learner checker.
	LearnerChecker CheckerSchedulerType = "learner-checker"
	// MergeChecker is the name for split checker.
	MergeChecker CheckerSchedulerType = "merge-checker"
	// ReplicaChecker is the name for replica checker.
	ReplicaChecker CheckerSchedulerType = "replica-checker"
	// RuleChecker is the name for rule checker.
	RuleChecker CheckerSchedulerType = "rule-checker"
	// SplitChecker is the name for split checker.
	SplitChecker CheckerSchedulerType = "split-checker"

	// BalanceLeaderScheduler is balance leader scheduler name.
	BalanceLeaderScheduler CheckerSchedulerType = "balance-leader-scheduler"
	// BalanceRegionScheduler is balance region scheduler name.
	BalanceRegionScheduler CheckerSchedulerType = "balance-region-scheduler"
	// BalanceWitnessScheduler is balance witness scheduler name.
	BalanceWitnessScheduler CheckerSchedulerType = "balance-witness-scheduler"
	// EvictLeaderScheduler is evict leader scheduler name.
	EvictLeaderScheduler CheckerSchedulerType = "evict-leader-scheduler"
	// EvictSlowStoreScheduler is evict leader scheduler name.
	EvictSlowStoreScheduler CheckerSchedulerType = "evict-slow-store-scheduler"
	// EvictSlowTrendScheduler is evict leader by slow trend scheduler name.
	EvictSlowTrendScheduler CheckerSchedulerType = "evict-slow-trend-scheduler"
	// GrantLeaderScheduler is grant leader scheduler name.
	GrantLeaderScheduler CheckerSchedulerType = "grant-leader-scheduler"
	// GrantHotRegionScheduler is grant hot region scheduler name.
	GrantHotRegionScheduler CheckerSchedulerType = "grant-hot-region-scheduler"
	// HotRegionScheduler is balance hot region scheduler name.
	HotRegionScheduler CheckerSchedulerType = "balance-hot-region-scheduler"
	// RandomMergeScheduler is random merge scheduler name.
	RandomMergeScheduler CheckerSchedulerType = "random-merge-scheduler"
	// ScatterRangeScheduler is scatter range scheduler name.
	// TODO: update to `scatter-range-scheduler`
	ScatterRangeScheduler CheckerSchedulerType = "scatter-range"
	// ShuffleHotRegionScheduler is shuffle hot region scheduler name.
	ShuffleHotRegionScheduler CheckerSchedulerType = "shuffle-hot-region-scheduler"
	// ShuffleLeaderScheduler is shuffle leader scheduler name.
	ShuffleLeaderScheduler CheckerSchedulerType = "shuffle-leader-scheduler"
	// ShuffleRegionScheduler is shuffle region scheduler name.
	ShuffleRegionScheduler CheckerSchedulerType = "shuffle-region-scheduler"
	// SplitBucketScheduler is the split bucket name.
	SplitBucketScheduler CheckerSchedulerType = "split-bucket-scheduler"
	// TransferWitnessLeaderScheduler is transfer witness leader scheduler name.
	TransferWitnessLeaderScheduler CheckerSchedulerType = "transfer-witness-leader-scheduler"
	// LabelScheduler is label scheduler name.
	LabelScheduler CheckerSchedulerType = "label-scheduler"
)
