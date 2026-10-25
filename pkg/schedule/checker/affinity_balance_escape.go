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
	"slices"

	"github.com/tikv/pd/pkg/schedule/affinity"
)

// Escape is deliberately bounded to one neutral move followed by a strict
// improvement on another group. Both targets must pass the normal checks; only
// the first is committed, and subsequent searches start from current inputs.
// These limits bound additional enumeration and expensive Region validation.
const (
	balanceEscapeCandidateLimit  = 4096
	balanceEscapeValidationLimit = 32
)

func (b *affinityBalancer) balanceEscapeAllowed(id affinity.TableID, score affinity.BalanceScore) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	previous, used := b.escapeScores[id]
	return !used || score.Compare(previous) < 0
}

func findBalanceEscape(snapshot *affinity.BalanceSnapshot, scorer *affinity.BalanceScorer,
	filters map[string]balanceTargetPlacementFilter, check func(balanceStoreAction) balanceStatus,
	checkInput func() error, validate func(*affinity.GroupState, *affinity.Group) (bool, error),
) (first, successor *affinity.Group, err error) {
	remaining, validations := balanceEscapeCandidateLimit, balanceEscapeValidationLimit
	for _, replace := range []bool{false, true} {
		candidates := newBalanceCandidates(snapshot, scorer, replace, check)
		for remaining > 0 && validations > 0 {
			if err := checkInput(); err != nil {
				return nil, nil, err
			}
			move, found := candidates.next()
			if !found {
				break
			}
			remaining--
			score, _ := scorer.Evaluate(move)
			if score != scorer.Score() {
				continue
			}
			group := snapshot.Groups[candidates.group]
			target := balanceTargetForMove(group, move)
			if !filters[group.ID].maySatisfy(&target) || balanceTargetExecutionStatus(&group.Group, &target, check) != balanceAllowed {
				continue
			}
			// Candidate enumeration excludes malformed/no-op moves. Scoring a
			// copied layout lets the successor use the counts after the neutral
			// move without modifying Manager state or retaining a plan.
			virtual := *snapshot
			virtual.Groups = slices.Clone(snapshot.Groups)
			changed := *group
			changed.Group = target
			virtual.Groups[candidates.group] = &changed
			groups := make([]*affinity.Group, len(virtual.Groups))
			for i, state := range virtual.Groups {
				groups[i] = &state.Group
			}
			nextScorer, err := affinity.NewBalanceScorer(groups)
			if err != nil {
				return nil, nil, err
			}
			firstChecked, firstAllowed := false, true
			for _, replaceNext := range []bool{false, true} {
				nextCandidates := newBalanceCandidates(&virtual, nextScorer, replaceNext, check)
				blockedLeader := false
				for remaining > 0 && validations > 0 {
					if err := checkInput(); err != nil {
						return nil, nil, err
					}
					nextMove, found := nextCandidates.next()
					if !found {
						break
					}
					remaining--
					if nextMove.GroupID == group.ID {
						continue
					}
					if _, improves := nextScorer.Evaluate(nextMove); !improves {
						continue
					}
					nextGroup := snapshot.Groups[nextCandidates.group]
					next := balanceTargetForMove(nextGroup, nextMove)
					if !filters[nextGroup.ID].maySatisfy(&next) {
						continue
					}
					status := balanceTargetExecutionStatus(&nextGroup.Group, &next, check)
					if status != balanceAllowed {
						blockedLeader = blockedLeader || !replaceNext && status == balanceTemporarilyBlocked
						continue
					}
					if validate != nil {
						if !firstChecked {
							validations--
							firstAllowed, err = validate(group, &target)
							if err != nil {
								return nil, nil, err
							}
							firstChecked = true
						}
						if !firstAllowed || validations == 0 {
							break
						}
						validations--
						allowed, err := validate(nextGroup, &next)
						if err != nil {
							return nil, nil, err
						}
						if !allowed {
							continue
						}
					}
					return &target, &next, nil
				}
				if blockedLeader || !firstAllowed {
					break
				}
			}
		}
	}
	return nil, nil, nil
}
