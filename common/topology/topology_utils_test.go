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
