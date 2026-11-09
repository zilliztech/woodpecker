package webui

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"
)

// Legacy response shapes remain empty; no metadata connection tuples are stored.
type metadataRecent struct {
	ID         string    `json:"id"`
	Etcd       string    `json:"etcd"`
	Prefix     string    `json:"prefix"`
	LogName    string    `json:"log_name,omitempty"`
	ClientMeta string    `json:"client_meta,omitempty"`
	Bucket     string    `json:"bucket,omitempty"`
	RootPath   string    `json:"root_path,omitempty"`
	LastUsed   time.Time `json:"last_used"`
}
type metadataChoice struct {
	metadataSession
	ID        string     `json:"id"`
	Origin    string     `json:"origin"`
	HistoryID string     `json:"history_id,omitempty"`
	LastUsed  *time.Time `json:"last_used,omitempty"`
}

func normalizeMetadata(m metadataSession) (metadataSession, error) {
	m.Prefix = strings.Trim(strings.TrimSpace(m.Prefix), "/")
	m.LogName = strings.TrimSpace(m.LogName)
	if m.Prefix == "" || len(m.Etcd+m.Prefix+m.LogName+m.Bucket+m.RootPath) > 2048 || strings.ContainsAny(m.Prefix+m.LogName+m.Bucket+m.RootPath, "\x00\r\n") || strings.HasPrefix(m.LogName, "-") || strings.Contains(m.LogName, "/") {
		return m, fmt.Errorf("Provide etcd endpoints, a full metadata prefix and an optional valid log name")
	}
	endpoints := []string{}
	for _, endpoint := range strings.Split(m.Etcd, ",") {
		u, err := url.Parse(strings.TrimSpace(endpoint))
		if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || (u.Scheme != "http" && u.Scheme != "https") {
			return m, fmt.Errorf("Use HTTP(S) etcd endpoints without embedded credentials or query parameters")
		}
		u.Host = strings.ToLower(u.Host)
		if u.Path == "/" {
			u.Path = ""
		}
		endpoints = append(endpoints, u.String())
	}
	sort.Strings(endpoints)
	unique := endpoints[:0]
	for _, endpoint := range endpoints {
		if len(unique) == 0 || unique[len(unique)-1] != endpoint {
			unique = append(unique, endpoint)
		}
	}
	m.Etcd = strings.Join(unique, ",")
	return m, nil
}
func metadataKey(m metadataSession) string { return m.Etcd + "\x00" + m.Prefix }
func metadataHistoryID(m metadataSession) string {
	sum := sha256.Sum256([]byte(metadataKey(m)))
	return "recent-" + hex.EncodeToString(sum[:12])
}
