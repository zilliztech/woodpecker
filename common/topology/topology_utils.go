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

// IsUnknown reports whether a placement value carries no information.
//
// It accepts the rendered form as well as the empty string, and that is what
// makes it total. Rewriting a placement to ScopeUnknown before classifying it is
// the obvious simplification once LabelOrUnknown exists, and it is a trap: two
// unplaced processes would compare equal on both region and AZ and be
// classified ScopeLocal, so every byte between them would be reported as
// node-local traffic. Recognising the rendered form here means that mistake
// yields ScopeUnknown instead of a silent lie, and the boundary is held by this
// function rather than by a comment on LabelOrUnknown telling callers where not
// to use it.
func IsUnknown(v string) bool {
	return v == "" || v == ScopeUnknown
}

// Scope classifies a peer's placement relative to a local placement. Either
// side may be unset — REGION/AVAILABILITY_ZONE are optional, and a process
// without them cannot tell local from remote at all — which yields
// ScopeUnknown rather than a guess.
func Scope(localRegion, localAZ, peerRegion, peerAZ string) string {
	if IsUnknown(localRegion) || IsUnknown(localAZ) || IsUnknown(peerRegion) || IsUnknown(peerAZ) {
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

// LabelOrUnknown renders a placement value for use as a metric label.
//
// The empty string is never emitted. A series carrying `az=""` is structurally
// indistinguishable from one whose placement is genuinely recorded, so a
// misconfigured deployment produced dashboards that looked fine and said
// nothing — for 49 days, in the case that prompted this. `unknown` is the same
// word Scope already uses for the same condition, so the two agree.
//
// This is a rendering decision, not a validation one: placement stays optional,
// and MissingPlacementEnv is what says so out loud at startup.
func LabelOrUnknown(value string) string {
	if value == "" {
		return ScopeUnknown
	}
	return value
}

// MissingPlacementEnv returns the placement environment variables that are
// unset, in a stable order, so a caller can name them in a startup warning.
//
// Deliberately not an error. Placement is genuinely optional for a single-AZ
// deployment, and refusing to start would break those — but a process that
// cannot tell local from remote should say so once, loudly, rather than
// reporting `unknown` forever and leaving somebody to notice from a dashboard.
func MissingPlacementEnv() []string {
	var missing []string
	if os.Getenv(RegionEnvKey) == "" {
		missing = append(missing, RegionEnvKey)
	}
	if os.Getenv(AvailabilityZoneEnvKey) == "" {
		missing = append(missing, AvailabilityZoneEnvKey)
	}
	return missing
}
