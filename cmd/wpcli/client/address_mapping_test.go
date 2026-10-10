package client

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodeAdminMappingResolution(t *testing.T) {
	m := Member{ID: "wp-0", ServiceAddr: "wp-0.internal:8080", GossipAddr: "10.0.0.1:7946", Tags: map[string]string{"admin_port": "9191"}}
	for _, tc := range []struct {
		name     string
		mappings map[string]string
		want     string
	}{
		{"unchanged", nil, "http://wp-0.internal:9191"},
		{"exact service overrides identity", map[string]string{"wp-0.internal:8080": "http://localhost:19091", "wp-0": "http://localhost:29091"}, "http://localhost:19091"},
		{"identity", map[string]string{"wp-0": "https://external:9091"}, "https://external:9091"},
		{"gossip", map[string]string{"10.0.0.1:7946": "http://localhost:19091"}, "http://localhost:19091"},
		{"host", map[string]string{"wp-0.internal": "http://192.0.2.1:9091"}, "http://192.0.2.1:9091"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := New("http://seed:9091", ClientOpts{NodeAdminURLs: tc.mappings})
			require.Equal(t, tc.want, c.PeerAdminURL(m))
			require.Equal(t, "wp-0.internal:8080", m.ServiceAddr)
		})
	}
}

func TestMappedHistoricalQuorumMember(t *testing.T) {
	c := New("http://seed:9091", ClientOpts{NodeAdminURLs: map[string]string{"old.internal:8080": "http://localhost:19092"}})
	m, ok := c.QuorumMember(&Memberlist{}, "old.internal:8080")
	require.True(t, ok)
	require.Empty(t, m.ID)
	require.Equal(t, "old.internal:8080", m.ServiceAddr)
	require.Equal(t, "http://localhost:19092", c.PeerAdminURL(m))
	_, ok = c.QuorumMember(&Memberlist{}, "other.internal:8080")
	require.False(t, ok)
	_, ok = c.QuorumMember(&Memberlist{}, "localhost:19092")
	require.False(t, ok, "an external destination does not establish quorum identity")
	c = New("http://seed:9091", ClientOpts{})
	_, ok = c.QuorumMember(&Memberlist{}, "old.internal:8080")
	require.False(t, ok)
}

func TestMappedSingleNodeIdentity(t *testing.T) {
	c := New("http://seed:9091", ClientOpts{NodeAdminURLs: map[string]string{"wp-0": "http://localhost:19091"}})
	m, ok := c.ResolveMember(&Memberlist{}, "wp-0")
	require.True(t, ok)
	require.Equal(t, "wp-0", m.ID)
	require.Equal(t, "http://localhost:19091", c.PeerAdminURL(m))
}

func TestValidateNodeAdminURLs(t *testing.T) {
	for _, value := range []string{"localhost:9091", "ftp://host:9091", "http://", "http://user:password@host", "http://host/admin", "http://host?x=1", "http://host#x", "http://host:99999", "http://host:bad"} {
		t.Run(value, func(t *testing.T) {
			_, err := ValidateNodeAdminURLs(map[string]string{"node": value})
			require.Error(t, err)
		})
	}
	_, err := ValidateNodeAdminURLs(map[string]string{"": "http://host"})
	require.Error(t, err)
	normalized, err := ValidateNodeAdminURLs(map[string]string{"node": "http://[::1]:19091/"})
	require.NoError(t, err)
	require.Equal(t, "http://[::1]:19091", normalized["node"])
}
