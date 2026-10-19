package api

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/labeler"
)

func TestValidateRegionLabelPatchRejectsAffinityIDsBeforeWrites(t *testing.T) {
	require.NoError(t, rejectAffinityLabelID("ordinary", nil))
	require.ErrorIs(t, rejectAffinityLabelID("affinity_group/g", nil), errs.ErrAffinityLabelProtected)
	require.ErrorIs(t, rejectAffinityLabelRule(&labeler.LabelRule{ID: "custom", Labels: []labeler.RegionLabel{{Key: affinity.AffinityLabelKey}}}), errs.ErrAffinityLabelProtected)
	require.ErrorIs(t, validateRegionLabelPatch(labeler.LabelRulePatch{
		SetRules: []*labeler.LabelRule{{ID: "ordinary"}, {ID: "affinity_group/g"}},
	}, nil), errs.ErrAffinityLabelProtected)
	require.NoError(t, validateRegionLabelPatch(labeler.LabelRulePatch{
		SetRules: []*labeler.LabelRule{{ID: "ordinary"}}, DeleteRules: []string{"other"},
	}, nil))
}

func TestRegionLabelPatchProtectsExistingLegacyAffinityRule(t *testing.T) {
	legacy := &labeler.LabelRule{ID: "legacy", Labels: []labeler.RegionLabel{{Key: affinity.AffinityLabelKey, Value: "g"}}}
	lookup := func(id string) *labeler.LabelRule {
		if id == legacy.ID {
			return legacy
		}
		return nil
	}
	require.ErrorIs(t, rejectAffinityLabelID(legacy.ID, lookup), errs.ErrAffinityLabelProtected)
	for _, patch := range []labeler.LabelRulePatch{
		{SetRules: []*labeler.LabelRule{{ID: "ordinary"}, {ID: legacy.ID}}},
		{SetRules: []*labeler.LabelRule{{ID: "ordinary"}}, DeleteRules: []string{legacy.ID}},
	} {
		require.ErrorIs(t, validateRegionLabelPatch(patch, lookup), errs.ErrAffinityLabelProtected)
	}
}
