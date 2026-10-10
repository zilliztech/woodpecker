package client

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

// ValidateNodeAdminURLs validates and normalizes explicit admin endpoints before
// discovery or any operation can contact a node. Paths, credentials and query
// strings are rejected: each value names a node's admin origin.
func ValidateNodeAdminURLs(mappings map[string]string) (map[string]string, error) {
	normalized := make(map[string]string, len(mappings))
	for key, value := range mappings {
		if key == "" || strings.TrimSpace(key) != key {
			return nil, fmt.Errorf("node admin mapping requires a non-empty identity without surrounding whitespace")
		}
		u, err := url.Parse(value)
		if err != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" {
			return nil, fmt.Errorf("node admin mapping %q requires an http(s) origin without credentials, path, query or fragment", key)
		}
		if port := u.Port(); port != "" {
			n, err := strconv.Atoi(port)
			if err != nil || n <= 0 || n > 65535 {
				return nil, fmt.Errorf("node admin mapping %q has an invalid port", key)
			}
		}
		normalized[key] = strings.TrimSuffix(value, "/")
	}
	return normalized, nil
}

// mappedAdminURL prefers the advertised service address, then node ID, gossip
// address, and finally their hosts. Exact addresses support distinct forwarded
// ports even when several replicas advertise the same host.
func (c *Client) mappedAdminURL(m Member) (string, bool) {
	keys := []string{m.ServiceAddr, m.ID, m.GossipAddr, mappingHost(m.ServiceAddr), mappingHost(m.GossipAddr)}
	for _, key := range keys {
		if key != "" {
			if mapped, ok := c.opts.NodeAdminURLs[key]; ok {
				return mapped, true
			}
		}
	}
	return "", false
}

func mappingHost(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err == nil {
		return host
	}
	return addr
}

// QuorumMember resolves only the original quorum identity. A historical node
// missing from discovery can be contacted only when an explicit mapping exists;
// its original address remains its label. External destinations are never used
// to infer or validate quorum membership.
func (c *Client) QuorumMember(members *Memberlist, addr string) (Member, bool) {
	if members != nil {
		for _, m := range members.Members {
			if m.ID == addr || m.ServiceAddr == addr || m.GossipAddr == addr {
				return m, true
			}
		}
	}
	historical := Member{ServiceAddr: addr}
	if _, ok := c.mappedAdminURL(historical); ok {
		return historical, true
	}
	return Member{}, false
}

// ResolveMember shares destination mapping with single-node commands while
// retaining memberlist identity when available and the existing explicit-target
// fallback. An explicitly mapped node ID can also name an undiscovered node.
func (c *Client) ResolveMember(members *Memberlist, identifier string) (Member, bool) {
	if members != nil {
		if m, ok := members.Resolve(identifier); ok {
			return m, true
		}
	}
	m := Member{ID: identifier, ServiceAddr: identifier}
	if _, ok := c.mappedAdminURL(m); ok {
		return m, true
	}
	return Member{}, false
}
