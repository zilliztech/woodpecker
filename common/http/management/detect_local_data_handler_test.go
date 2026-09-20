// Copyright (C) 2025 Zilliz. All rights reserved.
//
// This file is part of the Woodpecker project.
//
// Woodpecker is dual-licensed under the GNU Affero General Public License v3.0
// (AGPLv3) and the Server Side Public License v1 (SSPLv1). You may use this
// file under either license, at your option.
//
// AGPLv3 License: https://www.gnu.org/licenses/agpl-3.0.html
// SSPLv1 License: https://www.mongodb.com/licensing/server-side-public-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under these licenses is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the license texts for specific language governing permissions and
// limitations under the licenses.

package management

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

// captureInstanceDataFilter builds a handler that records the filter it was handed.
func captureInstanceDataFilter(bucket, root *string) http.HandlerFunc {
	return NewInstanceDataHandler(func(b, r string) any {
		*bucket, *root = b, r
		return map[string]any{"instances": []any{}}
	})
}

func TestInstanceDataHandler_NoFilterReturnsAll(t *testing.T) {
	var gotBucket, gotRoot string
	h := captureInstanceDataFilter(&gotBucket, &gotRoot)
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/admin/instance/data", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Empty(t, gotBucket)
	require.Empty(t, gotRoot)
}

func TestInstanceDataHandler_FilterPassedThrough(t *testing.T) {
	var gotBucket, gotRoot string
	h := captureInstanceDataFilter(&gotBucket, &gotRoot)
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/admin/instance/data?bucket_name=b&root_path=r", nil))
	require.Equal(t, "b", gotBucket)
	require.Equal(t, "r", gotRoot)
}

// TestInstanceDataHandler_PartialFilterIgnored matches /admin/log-health, the only other
// endpoint taking these two params. The response echoes the filter that took effect, so
// the widening is visible to the caller rather than silent.
func TestInstanceDataHandler_PartialFilterIgnored(t *testing.T) {
	for _, query := range []string{"?bucket_name=b", "?root_path=r"} {
		var gotBucket, gotRoot string
		h := captureInstanceDataFilter(&gotBucket, &gotRoot)
		rec := httptest.NewRecorder()
		h(rec, httptest.NewRequest(http.MethodGet, "/admin/instance/data"+query, nil))
		require.Empty(t, gotBucket, query)
		require.Empty(t, gotRoot, query)
	}
}

// TestInstanceDataHandler_AcceptsAlternateParamSpellings mirrors the log-health
// tolerance, so a caller does not get a silently unfiltered answer over casing alone.
func TestInstanceDataHandler_AcceptsAlternateParamSpellings(t *testing.T) {
	for _, query := range []string{
		"?bucketName=b&rootPath=r",
		"?bucketname=b&rootpath=r",
	} {
		var gotBucket, gotRoot string
		h := captureInstanceDataFilter(&gotBucket, &gotRoot)
		rec := httptest.NewRecorder()
		h(rec, httptest.NewRequest(http.MethodGet, "/admin/instance/data"+query, nil))
		require.Equal(t, "b", gotBucket, query)
		require.Equal(t, "r", gotRoot, query)
	}
}

func TestInstanceDataHandler_RejectsNonGet(t *testing.T) {
	h := NewInstanceDataHandler(func(string, string) any { return nil })
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodPost, "/admin/instance/data", nil))
	require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestInstanceDataHandler_EncodesJSON(t *testing.T) {
	h := NewInstanceDataHandler(func(string, string) any {
		return map[string]any{"node_id": "woodpecker-0", "instance_count": 2}
	})
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/admin/instance/data", nil))
	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, "woodpecker-0", body["node_id"])
}
