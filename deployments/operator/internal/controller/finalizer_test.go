/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// setupTestHTTPClient replaces the package-level httpClient with one that
// routes all requests to the given test server, regardless of the original host.
func setupTestHTTPClient(t *testing.T, srv *httptest.Server) {
	t.Helper()
	orig := httpClient
	httpClient = srv.Client()
	httpClient.Transport = &rewriteTransport{target: srv.URL, inner: srv.Client().Transport}
	t.Cleanup(func() { httpClient = orig })
}

type rewriteTransport struct {
	target string
	inner  http.RoundTripper
}

func (t *rewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Preserve path; replace scheme+host.
	targetURL := t.target + req.URL.Path
	newReq, err := http.NewRequestWithContext(req.Context(), req.Method, targetURL, req.Body)
	if err != nil {
		return nil, err
	}
	newReq.Header = req.Header
	inner := t.inner
	if inner == nil {
		inner = http.DefaultTransport
	}
	return inner.RoundTrip(newReq)
}

func newRunningPod(name, ip string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: ip,
		},
	}
}

func TestDecommissionPod_Safe(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/admin/node/decommission/progress"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"state":"decommissioned","safe_to_terminate":true}`))
		case strings.HasSuffix(r.URL.Path, "/admin/node/decommission"):
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()
	setupTestHTTPClient(t, srv)

	r := &WoodpeckerClusterReconciler{}
	safe, err := r.decommissionPod(context.Background(), newRunningPod("wp-0", "1.2.3.4"), 9091)
	require.NoError(t, err)
	assert.True(t, safe)
}

func TestDecommissionPod_NotSafe(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/progress") {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"state":"decommissioning","safe_to_terminate":false}`))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	setupTestHTTPClient(t, srv)

	r := &WoodpeckerClusterReconciler{}
	safe, err := r.decommissionPod(context.Background(), newRunningPod("wp-0", "1.2.3.4"), 9091)
	require.NoError(t, err)
	assert.False(t, safe)
}

func TestDecommissionPod_ProgressNon200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/progress") {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"unavailable"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	setupTestHTTPClient(t, srv)

	r := &WoodpeckerClusterReconciler{}
	safe, err := r.decommissionPod(context.Background(), newRunningPod("wp-0", "1.2.3.4"), 9091)
	require.NoError(t, err)
	assert.False(t, safe)
}

func TestDecommissionPod_EmptyPodIP(t *testing.T) {
	// No PodIP yet — should return (false, nil) without making HTTP calls.
	r := &WoodpeckerClusterReconciler{}
	safe, err := r.decommissionPod(context.Background(), newRunningPod("wp-0", ""), 9091)
	require.NoError(t, err)
	assert.False(t, safe)
}

func TestDecommissionPod_DecommissionEndpointError(t *testing.T) {
	// Server closed — Post will fail.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.Close() // immediate close
	setupTestHTTPClient(t, srv)

	r := &WoodpeckerClusterReconciler{}
	_, err := r.decommissionPod(context.Background(), newRunningPod("wp-0", "1.2.3.4"), 9091)
	require.Error(t, err)
}
