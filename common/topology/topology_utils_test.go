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

func TestLabelOrUnknown(t *testing.T) {
	assert.Equal(t, Unknown, LabelOrUnknown(""))
	assert.Equal(t, "us-west-2c", LabelOrUnknown("us-west-2c"))
	assert.Equal(t, Unknown, LabelOrUnknown(Unknown))
}

func TestCurrentPlacement(t *testing.T) {
	t.Run("fully configured", func(t *testing.T) {
		t.Setenv(RegionEnvKey, "region-a")
		t.Setenv(AvailabilityZoneEnvKey, "az-a")

		region, az, configured := CurrentPlacement()
		assert.Equal(t, "region-a", region)
		assert.Equal(t, "az-a", az)
		assert.True(t, configured)
	})

	t.Run("half configured is not configured", func(t *testing.T) {
		// A region without an AZ cannot classify anything: Scope needs both.
		t.Setenv(RegionEnvKey, "region-a")
		t.Setenv(AvailabilityZoneEnvKey, "")

		_, _, configured := CurrentPlacement()
		assert.False(t, configured)
	})

	t.Run("unset", func(t *testing.T) {
		t.Setenv(RegionEnvKey, "")
		t.Setenv(AvailabilityZoneEnvKey, "")

		region, az, configured := CurrentPlacement()
		assert.Equal(t, "", region)
		assert.Equal(t, "", az)
		assert.False(t, configured)
	})
}

// The empty string is Scope's "never configured" sentinel and must stay that way.
// Rewriting placement to Unknown before classifying is the obvious simplification
// once Unknown exists, and it is a trap: two unplaced processes would then compare
// equal on both region and AZ and be classified ScopeLocal, reporting every byte
// between them as node-local traffic. That is strictly worse than reporting it as
// unknown, and it would be invisible. LabelOrUnknown is documented as label-only
// for this reason; this test fails loudly if the boundary is ever crossed.
func TestScope_EmptyIsTheUnsetSentinel(t *testing.T) {
	assert.Equal(t, ScopeUnknown, Scope("", "", "", ""),
		"two unplaced processes are unknown to each other")
	assert.Equal(t, ScopeLocal, Scope(Unknown, Unknown, Unknown, Unknown),
		"pre-rewriting placement to Unknown makes unplaced processes look co-located — do not do it")
}
