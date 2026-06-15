package workload

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"github.com/zilliztech/woodpecker/common/werr"
)

func payloadHash(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

// isWoodpeckerPermanent reports true ONLY for a Woodpecker-typed, non-retryable error.
// Raw context deadline / cancellation / transport errors are transient (owned by the
// client's own retry budget) and must NOT be charged as I5 violations (Correction #4).
func isWoodpeckerPermanent(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return false
	}
	if werr.IsTransportError(err) {
		return false
	}
	// A retryable Woodpecker error is acceptable degradation, not a permanent failure.
	if werr.IsRetryableErr(err) {
		return false
	}
	// Permanent iff it is a Woodpecker-typed error that is NOT retryable.
	var wpErr interface{ IsRetryable() bool }
	if errors.As(err, &wpErr) {
		return !wpErr.IsRetryable()
	}
	return false // unknown / non-WP error: not our I5 target
}
