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
