package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJsonDiff_Identical(t *testing.T) {
	a := []byte(`{"foo":"bar","num":42}`)
	diffs := jsonDiff(a, a)
	assert.Empty(t, diffs)
}

func TestJsonDiff_LeafDiff(t *testing.T) {
	a := []byte(`{"level":"info","port":9091}`)
	b := []byte(`{"level":"debug","port":9091}`)
	diffs := jsonDiff(a, b)
	require.Len(t, diffs, 1)
	assert.Equal(t, "level", diffs[0].Path)
	assert.Equal(t, "info", diffs[0].RefValue)
	assert.Equal(t, "debug", diffs[0].CmpValue)
}

func TestJsonDiff_NestedDiff(t *testing.T) {
	a := []byte(`{"server":{"grpc":{"maxSend":512},"port":9091}}`)
	b := []byte(`{"server":{"grpc":{"maxSend":1024},"port":9091}}`)
	diffs := jsonDiff(a, b)
	require.Len(t, diffs, 1)
	assert.Equal(t, "server.grpc.maxSend", diffs[0].Path)
	assert.Equal(t, "512", diffs[0].RefValue)
	assert.Equal(t, "1024", diffs[0].CmpValue)
}

func TestJsonDiff_MissingKey(t *testing.T) {
	a := []byte(`{"a":"1","b":"2"}`)
	b := []byte(`{"a":"1"}`)
	diffs := jsonDiff(a, b)
	require.Len(t, diffs, 1)
	assert.Equal(t, "b", diffs[0].Path)
	assert.Equal(t, "2", diffs[0].RefValue)
	assert.Equal(t, "(missing)", diffs[0].CmpValue)
}

func TestJsonDiff_ExtraKey(t *testing.T) {
	a := []byte(`{"a":"1"}`)
	b := []byte(`{"a":"1","c":"3"}`)
	diffs := jsonDiff(a, b)
	require.Len(t, diffs, 1)
	assert.Equal(t, "c", diffs[0].Path)
	assert.Equal(t, "(missing)", diffs[0].RefValue)
	assert.Equal(t, "3", diffs[0].CmpValue)
}

func TestRenderDiffEntries(t *testing.T) {
	diffs := []diffEntry{
		{Path: "server.port", RefValue: "9091", CmpValue: "9092"},
		{Path: "log.level", RefValue: "info", CmpValue: "debug"},
	}
	out := renderDiffEntries(diffs, "node-1", "node-2")
	assert.Contains(t, out, "server.port")
	assert.Contains(t, out, "9091")
	assert.Contains(t, out, "9092")
	assert.Contains(t, out, "log.level")
	assert.Contains(t, out, "node-1")
	assert.Contains(t, out, "node-2")
}
