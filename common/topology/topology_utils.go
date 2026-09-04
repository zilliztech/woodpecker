package topology

import "os"

const (
	ClusterNameEnvKey       = "CLUSTER_NAME"
	RegionEnvKey            = "REGION"
	AvailabilityZoneEnvKey  = "AVAILABILITY_ZONE"
	DefaultClusterNameValue = "default"
)

// Unknown is what an unconfigured placement reads as once it reaches a metric
// label or a scope classification. The empty string is not usable there: PromQL
// renders a blank label as an ordinary aggregation with no breakdown rather than
// as missing data, which is how an unset REGION/AVAILABILITY_ZONE survived
// unnoticed on a cluster for 49 days.
const Unknown = "unknown"

// Placement scopes describe how far a peer sits from this process. They are
// used as a metric label, so the value set is deliberately tiny — the peer's
// identity is intentionally not part of it.
const (
	ScopeLocal       = "local"
	ScopeCrossAZ     = "cross_az"
	ScopeCrossRegion = "cross_region"
	ScopeUnknown     = Unknown
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

// LabelOrUnknown maps an unset placement value to Unknown for use as a metric
// label.
//
// Do not apply it before Scope. The empty string is the sentinel Scope relies on
// to tell "never configured" from a real placement: if both sides arrived here
// already rewritten to Unknown, their regions and AZs would compare equal and
// two unplaced processes would be classified ScopeLocal — reporting every byte
// as node-local traffic, which is worse than reporting it as unknown.
func LabelOrUnknown(v string) string {
	if v == "" {
		return Unknown
	}
	return v
}

// CurrentPlacement returns this process's own placement together with whether it
// is fully configured. A process missing either half cannot classify any peer,
// so every scope it reports degrades to ScopeUnknown regardless of how well the
// peers are configured.
func CurrentPlacement() (region string, az string, configured bool) {
	region = GetCurrentRegion()
	az = GetCurrentAvailabilityZone()
	return region, az, region != "" && az != ""
}
