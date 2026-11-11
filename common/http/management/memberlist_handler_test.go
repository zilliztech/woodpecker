// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package management

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

const memberlistPath = "/admin/memberlist"

func memberlistTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc(memberlistPath, NewMemberlistHandler(
		func() []byte { return []byte(`{"members":[{"id":"node-1"}]}`) },
		func() string { return "Total Members: 1\n" },
	))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestMemberlistHandler_DefaultsToText(t *testing.T) {
	srv := memberlistTestServer(t)

	resp, err := http.Get(srv.URL + memberlistPath)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	require.Contains(t, string(body), "Total Members")
	require.Equal(t, "text/plain; charset=utf-8", resp.Header.Get("Content-Type"))
}

func TestMemberlistHandler_ServesJSONWhenAccepted(t *testing.T) {
	srv := memberlistTestServer(t)

	req, _ := http.NewRequest(http.MethodGet, srv.URL+memberlistPath, nil)
	req.Header.Set("Accept", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	require.Contains(t, string(body), `"members"`)
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))
}

// TestMemberlistHandler_NilCallbacksAreNotServed pins the nil guards: a node with no
// memberlist wired must return an empty body, not panic.
func TestMemberlistHandler_NilCallbacksAreNotServed(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc(memberlistPath, NewMemberlistHandler(nil, nil))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + memberlistPath)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Empty(t, body)
}
