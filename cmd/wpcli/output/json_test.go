package output

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

type sample struct {
	Name string `json:"name" yaml:"name"`
	Age  int    `json:"age" yaml:"age"`
}

func TestRenderJSON(t *testing.T) {
	buf := new(bytes.Buffer)
	err := RenderJSON(buf, sample{Name: "alice", Age: 30})
	require.NoError(t, err)
	require.Contains(t, buf.String(), `"name": "alice"`)
	require.Contains(t, buf.String(), `"age": 30`)
}

func TestRenderYAML(t *testing.T) {
	buf := new(bytes.Buffer)
	err := RenderYAML(buf, sample{Name: "alice", Age: 30})
	require.NoError(t, err)
	require.Contains(t, buf.String(), "name: alice")
	require.Contains(t, buf.String(), "age: 30")
}
