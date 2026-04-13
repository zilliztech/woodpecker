package client

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// Member is one entry in the admin memberlist JSON response.
type Member struct {
	ID          string            `json:"id"`
	GossipAddr  string            `json:"gossip_addr"`
	ServiceAddr string            `json:"service_addr"`
	AZ          string            `json:"az"`
	RG          string            `json:"rg"`
	State       int               `json:"state"`
	Incarnation uint32            `json:"incarnation"`
	LastSeenMS  int64             `json:"last_seen_ms"`
	Tags        map[string]string `json:"tags,omitempty"`
}

// Memberlist is the top-level response of GET /admin/memberlist (JSON mode).
type Memberlist struct {
	Members []Member `json:"members"`
}

// Resolve finds a member by node_id, by `host:port`, or by IP prefix (gossip host).
// Returns the matching member and true on a unique match.
func (m *Memberlist) Resolve(identifier string) (Member, bool) {
	// 1. Exact node_id
	for _, mem := range m.Members {
		if mem.ID == identifier {
			return mem, true
		}
	}
	// 2. Exact host:port match on gossip or service addr
	for _, mem := range m.Members {
		if mem.GossipAddr == identifier || mem.ServiceAddr == identifier {
			return mem, true
		}
	}
	// 3. IP prefix (host portion of gossip_addr)
	var matches []Member
	for _, mem := range m.Members {
		host := mem.GossipAddr
		if i := strings.LastIndex(host, ":"); i >= 0 {
			host = host[:i]
		}
		if strings.HasPrefix(host, identifier) {
			matches = append(matches, mem)
		}
	}
	if len(matches) == 1 {
		return matches[0], true
	}
	return Member{}, false
}

// GetMemberlist fetches and parses /admin/memberlist from the client's seed URL.
func (c *Client) GetMemberlist() (*Memberlist, error) {
	req, err := http.NewRequest(http.MethodGet, c.baseURL+"/admin/memberlist", nil)
	if err != nil {
		return nil, fmt.Errorf("build memberlist request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("get memberlist: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("memberlist returned status %d", resp.StatusCode)
	}
	var ml Memberlist
	if err := json.NewDecoder(resp.Body).Decode(&ml); err != nil {
		return nil, fmt.Errorf("decode memberlist: %w", err)
	}
	return &ml, nil
}

// PeerAdminURL returns the admin HTTP URL for a given member, using the configured admin port.
//
// Address resolution order:
//  1. If the member has an "admin_port" tag, use it with the service_addr host.
//  2. Otherwise use service_addr host + the configured AdminPort.
//  3. Fall back to gossip_addr host + AdminPort if service_addr is empty.
//
// Using service_addr host (instead of gossip_addr) is important for Docker/NAT
// environments where gossip_addr is an internal container IP not reachable from
// the host machine.
func (c *Client) PeerAdminURL(m Member) string {
	// Pick the host from service_addr (preferred) or gossip_addr (fallback).
	host := extractHost(m.ServiceAddr)
	if host == "" {
		host = extractHost(m.GossipAddr)
	}

	port := c.opts.AdminPort
	if port == 0 {
		port = 9091
	}

	// Check for per-node admin_port in tags.
	if tagPort, ok := m.Tags["admin_port"]; ok {
		if p := parsePort(tagPort); p > 0 {
			port = p
		}
	}

	return fmt.Sprintf("http://%s:%d", host, port)
}

func extractHost(addr string) string {
	if addr == "" {
		return ""
	}
	if i := strings.LastIndex(addr, ":"); i >= 0 {
		return addr[:i]
	}
	return addr
}

func parsePort(s string) int {
	var p int
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0
		}
		p = p*10 + int(c-'0')
	}
	return p
}
