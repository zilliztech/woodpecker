package topology

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCurrentTopologyDefaults(t *testing.T) {
	t.Setenv(ClusterNameEnvKey, "")
	t.Setenv(RegionEnvKey, "")
	t.Setenv(AvailabilityZoneEnvKey, "")

	assert.Equal(t, "default", GetCurrentClusterName())
	assert.Equal(t, "", GetCurrentRegion())
	assert.Equal(t, "", GetCurrentAvailabilityZone())
}

func TestCurrentTopologyFromEnv(t *testing.T) {
	t.Setenv(ClusterNameEnvKey, "cluster-a")
	t.Setenv(RegionEnvKey, "region-a")
	t.Setenv(AvailabilityZoneEnvKey, "az-a")

	assert.Equal(t, "cluster-a", GetCurrentClusterName())
	assert.Equal(t, "region-a", GetCurrentRegion())
	assert.Equal(t, "az-a", GetCurrentAvailabilityZone())
}

func TestScope(t *testing.T) {
	cases := []struct {
		name                 string
		localRegion, localAZ string
		peerRegion, peerAZ   string
		want                 string
	}{
		{"same region and az", "r1", "a1", "r1", "a1", ScopeLocal},
		{"same region other az", "r1", "a1", "r1", "a2", ScopeCrossAZ},
		{"other region", "r1", "a1", "r2", "a1", ScopeCrossRegion},
		{"other region and az", "r1", "a1", "r2", "a2", ScopeCrossRegion},
		{"local placement unset", "", "", "r1", "a1", ScopeUnknown},
		{"local az unset", "r1", "", "r1", "a1", ScopeUnknown},
		{"peer placement unset", "r1", "a1", "", "", ScopeUnknown},
		{"peer az unset", "r1", "a1", "r1", "", ScopeUnknown},
		{"local placement already rendered", ScopeUnknown, ScopeUnknown, "r1", "a1", ScopeUnknown},
		{"peer placement already rendered", "r1", "a1", ScopeUnknown, ScopeUnknown, ScopeUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, Scope(tc.localRegion, tc.localAZ, tc.peerRegion, tc.peerAZ))
		})
	}
}

// Issue #292: an unset REGION/AVAILABILITY_ZONE became an empty metric label,
// which is indistinguishable from a recorded placement on a dashboard.
func TestLabelOrUnknown(t *testing.T) {
	assert.Equal(t, ScopeUnknown, LabelOrUnknown(""), "an unset placement renders as unknown")
	assert.Equal(t, "us-west-2a", LabelOrUnknown("us-west-2a"), "a real placement is unchanged")
	// Deliberately the word Scope already reports for the same condition, so a
	// dashboard grouping by az and one reading scope agree with each other
	// rather than inventing a second spelling of "we do not know".
	assert.Equal(t, "unknown", ScopeUnknown)
}

// Placement stays optional — this reports, it does not refuse.
func TestMissingPlacementEnv(t *testing.T) {
	t.Run("both unset", func(t *testing.T) {
		t.Setenv(RegionEnvKey, "")
		t.Setenv(AvailabilityZoneEnvKey, "")
		assert.Equal(t, []string{RegionEnvKey, AvailabilityZoneEnvKey}, MissingPlacementEnv())
	})

	t.Run("region unset only", func(t *testing.T) {
		t.Setenv(RegionEnvKey, "")
		t.Setenv(AvailabilityZoneEnvKey, "us-west-2a")
		assert.Equal(t, []string{RegionEnvKey}, MissingPlacementEnv())
	})

	t.Run("az unset only", func(t *testing.T) {
		t.Setenv(RegionEnvKey, "us-west-2")
		t.Setenv(AvailabilityZoneEnvKey, "")
		assert.Equal(t, []string{AvailabilityZoneEnvKey}, MissingPlacementEnv())
	})

	t.Run("both set is silent", func(t *testing.T) {
		t.Setenv(RegionEnvKey, "us-west-2")
		t.Setenv(AvailabilityZoneEnvKey, "us-west-2a")
		assert.Empty(t, MissingPlacementEnv(), "a configured process warns about nothing")
	})

	// CLUSTER_NAME has a real default and is deliberately not part of this:
	// reporting it would make the warning fire on every correctly configured
	// single-cluster deployment, which is how warnings get ignored.
	t.Run("cluster name is not placement", func(t *testing.T) {
		t.Setenv(ClusterNameEnvKey, "")
		t.Setenv(RegionEnvKey, "us-west-2")
		t.Setenv(AvailabilityZoneEnvKey, "us-west-2a")
		assert.Empty(t, MissingPlacementEnv())
		assert.Equal(t, DefaultClusterNameValue, GetCurrentClusterName())
	})
}

// IsUnknown decides "never configured", and it has to hold for both forms: the
// empty string a placement is carried as, and the ScopeUnknown a metric label
// spells it out as.
func TestIsUnknown(t *testing.T) {
	assert.True(t, IsUnknown(""), "the carried form of an unset placement")
	assert.True(t, IsUnknown(ScopeUnknown), "the rendered form must not be mistaken for a real region")
	assert.False(t, IsUnknown("us-west-2"))
	assert.False(t, IsUnknown("us-west-2c"))
	// Deliberately not tolerated: placement values are compared as given, so a
	// value differing only in case or padding is a real misconfiguration between
	// two deployment templates and must not be folded away into "unset".
	assert.False(t, IsUnknown("Unknown"))
	assert.False(t, IsUnknown(" "))
}

// Two unplaced processes must never be reported as co-located. Compared as
// ordinary strings they match on both region and AZ, so every byte between them
// would be counted as node-local traffic - a silent lie, and worse than
// reporting the pair as unknown. Scope excludes unset placements before it
// compares anything, through IsUnknown, so this holds for the carried form and
// for a value that was rendered by LabelOrUnknown before reaching it.
func TestScope_UnknownNeverBecomesLocal(t *testing.T) {
	assert.Equal(t, ScopeUnknown, Scope("", "", "", ""),
		"two unplaced processes are unknown to each other")
	assert.Equal(t, ScopeUnknown, Scope(LabelOrUnknown(""), LabelOrUnknown(""), LabelOrUnknown(""), LabelOrUnknown("")),
		"a placement rendered before classifying must not compare equal into ScopeLocal")
	assert.Equal(t, ScopeUnknown, Scope(ScopeUnknown, ScopeUnknown, "r1", "a1"),
		"a rendered local placement is still an unset one")
	assert.Equal(t, ScopeUnknown, Scope("r1", "a1", ScopeUnknown, ScopeUnknown),
		"a rendered peer placement is still an unset one")
}
