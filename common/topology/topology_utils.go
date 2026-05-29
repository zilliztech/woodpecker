package topology

import "os"

const (
	ClusterNameEnvKey       = "CLUSTER_NAME"
	RegionEnvKey            = "REGION"
	AvailabilityZoneEnvKey  = "AVAILABILITY_ZONE"
	DefaultClusterNameValue = "default"
)

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func GetCurrentClusterName() string {
	return envOrDefault(ClusterNameEnvKey, DefaultClusterNameValue)
}

func GetCurrentRegion() string {
	return envOrDefault(RegionEnvKey, "")
}

func GetCurrentAvailabilityZone() string {
	return envOrDefault(AvailabilityZoneEnvKey, "")
}
