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
// See the License for the specific language governing permissions and
// limitations under the License.

package autoscaling

import (
	"fmt"
	"math"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockoption"
	"github.com/tikv/pd/server/core"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&calculationTestSuite{})

type calculationTestSuite struct{}

func (s *calculationTestSuite) TestGetScaledTiKVGroups(c *C) {
	// case1 indicates the tikv cluster with not any group existed
	case1 := mockcluster.NewCluster(mockoption.NewScheduleOptions())
	case1.AddLabelsStore(1, 1, map[string]string{})
	case1.AddLabelsStore(2, 1, map[string]string{
		"foo": "bar",
	})
	case1.AddLabelsStore(3, 1, map[string]string{
		"id": "3",
	})

	// case2 indicates the tikv cluster with 1 auto-scaling group existed
	case2 := mockcluster.NewCluster(mockoption.NewScheduleOptions())
	case2.AddLabelsStore(1, 1, map[string]string{})
	case2.AddLabelsStore(2, 1, map[string]string{
		groupLabelKey:        fmt.Sprintf("%s-0", autoScalingGroupLabelKeyPrefix),
		resourceTypeLabelKey: "a",
	})
	case2.AddLabelsStore(3, 1, map[string]string{
		groupLabelKey:        fmt.Sprintf("%s-0", autoScalingGroupLabelKeyPrefix),
		resourceTypeLabelKey: "a",
	})

	// case3 indicates the tikv cluster with other group existed
	case3 := mockcluster.NewCluster(mockoption.NewScheduleOptions())
	case3.AddLabelsStore(1, 1, map[string]string{})
	case3.AddLabelsStore(2, 1, map[string]string{
		groupLabelKey: "foo",
	})
	case3.AddLabelsStore(3, 1, map[string]string{
		groupLabelKey: "foo",
	})

	testcases := []struct {
		name             string
		informer         core.StoreSetInformer
		healthyInstances []instance
		expectedPlan     []*Plan
		errChecker       Checker
	}{
		{
			name:     "no scaled tikv group",
			informer: case1,
			healthyInstances: []instance{
				{
					id:      1,
					address: "1",
				},
				{
					id:      2,
					address: "2",
				},
				{
					id:      3,
					address: "3",
				},
			},
			expectedPlan: nil,
			errChecker:   IsNil,
		},
		{
			name:     "exist 1 scaled tikv group",
			informer: case2,
			healthyInstances: []instance{
				{
					id:      1,
					address: "1",
				},
				{
					id:      2,
					address: "2",
				},
				{
					id:      3,
					address: "3",
				},
			},
			expectedPlan: []*Plan{
				{
					Component:    TiKV.String(),
					Count:        2,
					ResourceType: "a",
					Labels: []*metapb.StoreLabel{
						{
							Key:   groupLabelKey,
							Value: fmt.Sprintf("%s-0", autoScalingGroupLabelKeyPrefix),
						},
						{
							Key:   resourceTypeLabelKey,
							Value: "a",
						},
					},
				},
			},
			errChecker: IsNil,
		},
		{
			name:     "exist 1 tikv scaled group with inconsistency healthy instances",
			informer: case2,
			healthyInstances: []instance{
				{
					id:      1,
					address: "1",
				},
				{
					id:      2,
					address: "2",
				},
				{
					id:      4,
					address: "4",
				},
			},
			expectedPlan: nil,
			errChecker:   NotNil,
		},
		{
			name:     "exist 1 tikv scaled group with less healthy instances",
			informer: case2,
			healthyInstances: []instance{
				{
					id:      1,
					address: "1",
				},
				{
					id:      2,
					address: "2",
				},
			},
			expectedPlan: []*Plan{
				{
					Component:    TiKV.String(),
					Count:        1,
					ResourceType: "a",
					Labels: []*metapb.StoreLabel{
						{
							Key:   groupLabelKey,
							Value: fmt.Sprintf("%s-0", autoScalingGroupLabelKeyPrefix),
						},
						{
							Key:   resourceTypeLabelKey,
							Value: "a",
						},
					},
				},
			},
			errChecker: IsNil,
		},
		{
			name:     "existed other tikv group",
			informer: case3,
			healthyInstances: []instance{
				{
					id:      1,
					address: "1",
				},
				{
					id:      2,
					address: "2",
				},
				{
					id:      3,
					address: "3",
				},
			},
			expectedPlan: nil,
			errChecker:   IsNil,
		},
	}

	for _, testcase := range testcases {
		c.Log(testcase.name)
		plans, err := getScaledTiKVGroups(testcase.informer, testcase.healthyInstances)
		if testcase.expectedPlan == nil {
			c.Assert(plans, IsNil)
			c.Assert(err, testcase.errChecker)
		} else {
			c.Assert(plans, DeepEquals, testcase.expectedPlan)
		}
	}
}

type mockQuerier struct{}

func (q *mockQuerier) Query(options *QueryOptions) (QueryResult, error) {
	result := make(QueryResult)
	for _, addr := range options.addresses {
		result[addr] = mockResultValue
	}

	return result, nil
}

func (s *calculationTestSuite) TestGetTotalCPUUseTime(c *C) {
	querier := &mockQuerier{}
	instances := []instance{
		{
			address: "1",
			id:      1,
		},
		{
			address: "2",
			id:      2,
		},
		{
			address: "3",
			id:      3,
		},
	}
	totalCPUUseTime, _ := getTotalCPUUseTime(querier, TiDB, instances, time.Now(), 0)
	expected := mockResultValue * float64(len(instances))
	c.Assert(math.Abs(expected-totalCPUUseTime) < 1e-6, IsTrue)
}

func (s *calculationTestSuite) TestGetTotalCPUQuota(c *C) {
	querier := &mockQuerier{}
	instances := []instance{
		{
			address: "1",
			id:      1,
		},
		{
			address: "2",
			id:      2,
		},
		{
			address: "3",
			id:      3,
		},
	}
	totalCPUQuota, _ := getTotalCPUQuota(querier, TiDB, instances, time.Now())
	expected := uint64(mockResultValue * float64(len(instances)*millicores))
	c.Assert(totalCPUQuota, Equals, expected)
}
