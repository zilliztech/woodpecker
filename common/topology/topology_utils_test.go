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
