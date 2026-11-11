package loki

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
)

type Event struct {
	StreamID       string            `json:"stream_id"`
	ID             string            `json:"event_id"`
	TimestampNS    string            `json:"timestamp_ns"`
	Labels         map[string]string `json:"labels"`
	Pattern        string            `json:"pattern"`
	PatternVersion int               `json:"pattern_version"`
	Excerpt        string            `json:"matched_excerpt"`
	RequestID      string            `json:"request_id,omitempty"`
	TraceID        string            `json:"trace_id,omitempty"`
	RawWithheld    bool              `json:"raw_line_withheld"`
}

var (
	identity   = regexp.MustCompile(`^[a-zA-Z0-9_:.\-]{1,128}$`)
	trace      = regexp.MustCompile(`^[a-fA-F0-9]{32}$`)
	textPrefix = regexp.MustCompile(`^\[[^\]]+\] \[(WARN|ERROR|INFO)\] \[[^\]]+\] \[`)
)

func fingerprint(v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func parseEvent(line, ts string, labels map[string]string, scope, flow string) *Event {
	var fields map[string]json.RawMessage
	msg := ""
	requestID, traceID := "", ""
	if json.Unmarshal([]byte(line), &fields) == nil && fields != nil {
		for _, key := range []string{"msg", "message"} {
			if json.Unmarshal(fields[key], &msg) == nil && msg != "" {
				break
			}
		}
		for _, key := range []string{"requestID", "request_id"} {
			var value string
			if json.Unmarshal(fields[key], &value) == nil && identity.MatchString(value) {
				requestID = value
				break
			}
		}
		for _, key := range []string{"traceID", "trace_id"} {
			var value string
			if json.Unmarshal(fields[key], &value) == nil && trace.MatchString(value) && strings.Trim(value, "0") != "" {
				traceID = strings.ToLower(value)
				break
			}
		}
	} else {
		// Supported text format has a timestamp, severity, caller and quoted or bare
		// message in brackets. A substring in an arbitrary error body is insufficient.
		if !textPrefix.MatchString(line) {
			return nil
		}
		parts := strings.SplitN(line, "] [", 4)
		if len(parts) != 4 {
			return nil
		}
		tail := parts[3]
		end := strings.Index(tail, "]")
		if end < 0 {
			return nil
		}
		msg = tail[:end]
		if strings.HasPrefix(msg, "\"") {
			if e := json.Unmarshal([]byte(msg), &msg); e != nil {
				return nil
			}
		}
		requestID, traceID = textIdentities(tail[end+1:])
	}
	for _, p := range Catalog() {
		if applicable(p, scope, flow) && p.Component == labels["component"] && p.Message == msg {
			safe := map[string]string{}
			for _, k := range []string{"cluster", "namespace", "pod", "container", "component"} {
				if v := labels[k]; v != "" && len(v) <= 256 && !strings.ContainsAny(v, "\x00\r\n") {
					safe[k] = v
				}
			}
			return &Event{StreamID: fingerprint(labels), ID: "loki:event:" + fingerprint([]any{labels, ts, line}), TimestampNS: ts, Labels: safe, Pattern: p.ID, PatternVersion: p.Version, Excerpt: p.Message, RequestID: requestID, TraceID: traceID, RawWithheld: true}
		}
	}
	return nil
}

func validNS(ts string) (int64, bool) {
	n, e := strconv.ParseInt(ts, 10, 64)
	return n, e == nil && n > 0 && strconv.FormatInt(n, 10) == ts
}

// Read only top-level fields following the message, honoring quoted strings and
// nested brackets. IDs embedded in error strings, objects or arrays are not
// event identities. Context fields precede call-site fields; keep the first
// valid top-level ID rather than allowing a later duplicate to overwrite it.
func textIdentities(fields string) (requestID, traceID string) {
	for {
		field, rest, ok := nextTextField(fields)
		if !ok {
			return
		}
		fields = rest
		key, value, ok := strings.Cut(field, "=")
		if !ok {
			continue
		}
		if strings.HasPrefix(value, "\"") {
			var decoded string
			if json.Unmarshal([]byte(value), &decoded) != nil {
				continue
			}
			value = decoded
		}
		switch key {
		case "requestID", "request_id":
			if requestID == "" && identity.MatchString(value) {
				requestID = value
			}
		case "traceID", "trace_id":
			if traceID == "" && trace.MatchString(value) && strings.Trim(value, "0") != "" {
				traceID = strings.ToLower(value)
			}
		}
	}
}

func nextTextField(text string) (field, rest string, ok bool) {
	text = strings.TrimLeft(text, " \t")
	if text == "" || text[0] != '[' {
		return "", "", false
	}
	depth := 0
	quoted, escaped := false, false
	for i := 0; i < len(text); i++ {
		ch := text[i]
		if ch == '\n' || ch == '\r' {
			return "", "", false
		}
		if quoted {
			if escaped {
				escaped = false
				continue
			}
			if ch == '\\' {
				escaped = true
			} else if ch == '"' {
				quoted = false
			}
			continue
		}
		switch ch {
		case '"':
			quoted = true
		case '[':
			depth++
		case ']':
			depth--
			if depth == 0 {
				return text[1:i], text[i+1:], true
			}
		}
	}
	return "", "", false
}
