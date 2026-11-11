package diagnostics

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ConnectionCheck describes query-API availability, never workload health.
type ConnectionCheck struct {
	Source     string    `json:"source"`
	Component  string    `json:"component,omitempty"`
	Status     string    `json:"status"`
	Code       string    `json:"code"`
	Message    string    `json:"message"`
	Next       string    `json:"next,omitempty"`
	HTTPStatus int       `json:"http_status,omitempty"`
	ElapsedMS  int64     `json:"elapsed_ms"`
	CheckedAt  time.Time `json:"checked_at"`
}

func FailedConnection(code, message, next string) ConnectionCheck {
	return ConnectionCheck{Status: "failed", Code: code, Message: message, Next: next, CheckedAt: time.Now().UTC()}
}

// CheckQueryAPI sends a single bounded GET using the same private TLS/auth
// settings as collection. Redirects and remote error bodies are not exposed.
func CheckQueryAPI(ctx context.Context, conn Connection, endpoint, path string, query url.Values, tls TLSConfig, token, user, password string, validate func([]byte) ConnectionCheck) (out ConnectionCheck) {
	started := time.Now()
	defer func() { out.CheckedAt = time.Now().UTC(); out.ElapsedMS = time.Since(started).Milliseconds() }()
	transport, err := HTTPTransport(tls, conn.BaseDir)
	if err != nil {
		return FailedConnection("tls_configuration_unavailable", "Local TLS configuration could not be loaded.", "Check the configured CA, certificate and key paths.")
	}
	defer transport.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" || u.User != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return FailedConnection("invalid_connection_settings", "Invalid query API address.", "Enter the HTTP(S) base URL without credentials or query parameters.")
	}
	u.Path = strings.TrimRight(u.Path, "/") + path
	u.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return FailedConnection("invalid_request", "Cannot prepare the connection check.", "Review the source address.")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if user != "" {
		req.SetBasicAuth(user, password)
	}
	client := http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return FailedConnection("connection_timeout", "The query API did not respond within the connection-check budget.", "Check port forwarding, network access and service load; retry when reachable.")
		}
		var trust x509.UnknownAuthorityError
		var host x509.HostnameError
		if errors.As(err, &trust) || errors.As(err, &host) {
			return FailedConnection("tls_verification_failed", "The query API TLS certificate could not be verified.", "Check the CA and endpoint hostname.")
		}
		return FailedConnection("transport_failed", "The query API could not be reached.", "Check the address, running service, network and port forwarding from the machine running wp webui.")
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		code, message, next := "http_error", "The query API returned an HTTP error.", "Check the source service or reverse proxy."
		switch response.StatusCode {
		case 401, 403:
			code = "authentication_failed"
			message = "The query API denied this request."
			next = "Review the saved source authentication and query permissions. Credentials stay on the server."
		case 404:
			code = "api_not_found"
			message = "The query API path was not found."
			next = "Use the Prometheus or Jaeger query base URL; check any reverse-proxy path prefix."
		case 301, 302, 303, 307, 308:
			code = "redirect_rejected"
			message = "The query API redirected this request."
			next = "Use its direct query API URL; login-page redirects are not followed."
		}
		out = FailedConnection(code, message, next)
		out.HTTPStatus = response.StatusCode
		return out
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, (256<<10)+1))
	if err != nil || len(body) > 256<<10 {
		return FailedConnection("invalid_response", "The query API response was unreadable or too large.", "Verify the API address and proxy response.")
	}
	if !json.Valid(body) {
		return FailedConnection("invalid_response", "The address returned a non-JSON response.", "Use the query API base URL, rather than a UI or login page.")
	}
	out = validate(body)
	out.HTTPStatus = response.StatusCode
	return out
}
