package topology

import "os"

const (
	ClusterNameEnvKey       = "CLUSTER_NAME"
	RegionEnvKey            = "REGION"
	AvailabilityZoneEnvKey  = "AVAILABILITY_ZONE"
	DefaultClusterNameValue = "default"
)

// Placement scopes describe how far a peer sits from this process. They are
// used as a metric label, so the value set is deliberately tiny — the peer's
// identity is intentionally not part of it.
const (
	ScopeLocal       = "local"
	ScopeCrossAZ     = "cross_az"
	ScopeCrossRegion = "cross_region"
	ScopeUnknown     = "unknown"
)

// Scope classifies a peer's placement relative to a local placement. Either
// side may be unset — REGION/AVAILABILITY_ZONE are optional, and a process
// without them cannot tell local from remote at all — which yields
// ScopeUnknown rather than a guess.
func Scope(localRegion, localAZ, peerRegion, peerAZ string) string {
	if localRegion == "" || localAZ == "" || peerRegion == "" || peerAZ == "" {
		return ScopeUnknown
	}
	if localRegion != peerRegion {
		return ScopeCrossRegion
	}
	if localAZ != peerAZ {
		return ScopeCrossAZ
	}
	return ScopeLocal
}

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
