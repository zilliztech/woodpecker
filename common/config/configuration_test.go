package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfiguration(t *testing.T) {
	configDefault, err := NewConfiguration("")
	assert.NoError(t, err)
	assert.NotNil(t, configDefault)
	assert.Equal(t, 100_000_000, configDefault.LogStore.LogFileSyncPolicy.MaxBytes)

	configFromFile, err := NewConfiguration("../../config/woodpecker.yaml")
	assert.NoError(t, err)
	assert.NotNil(t, configFromFile)
	assert.Equal(t, 64_000_000, configFromFile.LogStore.LogFileSyncPolicy.MaxBytes)
}
