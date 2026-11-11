package output

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRowTable_Basic(t *testing.T) {
	buf := new(bytes.Buffer)
	rows := [][]string{
		{"node-1", "10.0.1.1", "active"},
		{"node-2", "10.0.1.2", "decommissioning"},
	}
	err := RenderRowTable(buf, []string{"NAME", "ADDR", "STATE"}, rows)
	require.NoError(t, err)
	lines := strings.Split(buf.String(), "\n")
	// header, node-1, node-2, trailing empty
	require.GreaterOrEqual(t, len(lines), 3)
	require.Contains(t, lines[0], "NAME")
	require.Contains(t, lines[0], "STATE")
	require.Contains(t, lines[1], "node-1")
	require.Contains(t, lines[2], "node-2")
}

func TestRowTable_EmptyRows(t *testing.T) {
	buf := new(bytes.Buffer)
	err := RenderRowTable(buf, []string{"NAME", "ADDR"}, nil)
	require.NoError(t, err)
	// Header still printed.
	require.Contains(t, buf.String(), "NAME")
}

func TestSectionedTable_Basic(t *testing.T) {
	buf := new(bytes.Buffer)
	sections := []Section{
		{
			Title: "Identity",
			Pairs: [][2]string{
				{"node_id", "node-2"},
				{"gossip_addr", "10.0.1.2:17946"},
			},
		},
		{
			Title: "Lifecycle",
			Pairs: [][2]string{
				{"state", "decommissioning"},
				{"since", "18m42s ago"},
			},
		},
	}
	err := RenderSectionedTable(buf, sections)
	require.NoError(t, err)
	out := buf.String()
	require.Contains(t, out, "Identity")
	require.Contains(t, out, "node_id")
	require.Contains(t, out, "node-2")
	require.Contains(t, out, "Lifecycle")
	require.Contains(t, out, "decommissioning")
}
