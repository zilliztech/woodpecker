package output

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRenderTree_Simple(t *testing.T) {
	root := &TreeNode{
		Label: "Cluster",
		Children: []*TreeNode{
			{
				Label: "us-east-1a",
				Children: []*TreeNode{
					{Label: "node-1"},
					{Label: "node-2"},
				},
			},
			{
				Label: "us-east-1b",
				Children: []*TreeNode{
					{Label: "node-3"},
				},
			},
		},
	}
	buf := new(bytes.Buffer)
	require.NoError(t, RenderTree(buf, root))
	out := buf.String()

	require.Contains(t, out, "Cluster")
	require.Contains(t, out, "us-east-1a")
	require.Contains(t, out, "node-1")
	require.Contains(t, out, "node-3")
	// Uses box-drawing characters.
	require.True(t,
		strings.Contains(out, "├──") || strings.Contains(out, "└──"),
		"expected box-drawing chars, got:\n%s", out)
}
