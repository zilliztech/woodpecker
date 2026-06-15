package workload

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPayloadHashDeterministic(t *testing.T) {
	require.Equal(t, payloadHash([]byte("abc")), payloadHash([]byte("abc")))
	require.NotEqual(t, payloadHash([]byte("abc")), payloadHash([]byte("abd")))
}

func TestIsWoodpeckerPermanent_TransientAndNil(t *testing.T) {
	require.False(t, isWoodpeckerPermanent(nil))
	require.False(t, isWoodpeckerPermanent(context.DeadlineExceeded))
	require.False(t, isWoodpeckerPermanent(context.Canceled))
	require.False(t, isWoodpeckerPermanent(errors.New("some random non-wp error")))
}
