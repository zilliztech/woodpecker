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
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- NewLogDeleteHandler tests ----

func TestLogDeleteHandler_Success(t *testing.T) {
	var gotBucket, gotRoot string
	var gotLogId int64
	handler := NewLogDeleteHandler(func(bucketName, rootPath string, logId int64) error {
		gotBucket = bucketName
		gotRoot = rootPath
		gotLogId = logId
		return nil
	})

	body := strings.NewReader(`{"bucketName":"mybucket","rootPath":"my/root","logId":42}`)
	req := httptest.NewRequest(http.MethodPost, "/admin/log/delete", body)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Header().Get("Content-Type"), "application/json")
	assert.Contains(t, w.Body.String(), `"ok"`)
	assert.Equal(t, "mybucket", gotBucket)
	assert.Equal(t, "my/root", gotRoot)
	assert.Equal(t, int64(42), gotLogId)
}

func TestLogDeleteHandler_BadJSON(t *testing.T) {
	handler := NewLogDeleteHandler(func(bucketName, rootPath string, logId int64) error {
		return nil
	})

	body := strings.NewReader(`not-json`)
	req := httptest.NewRequest(http.MethodPost, "/admin/log/delete", body)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "error")
}

func TestLogDeleteHandler_CallbackError(t *testing.T) {
	handler := NewLogDeleteHandler(func(bucketName, rootPath string, logId int64) error {
		return errors.New("evict failed")
	})

	body := strings.NewReader(`{"bucketName":"b","rootPath":"r","logId":1}`)
	req := httptest.NewRequest(http.MethodPost, "/admin/log/delete", body)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	require.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Contains(t, w.Body.String(), "evict failed")
}

func TestLogDeleteHandler_MethodNotAllowed(t *testing.T) {
	handler := NewLogDeleteHandler(func(bucketName, rootPath string, logId int64) error {
		return nil
	})

	req := httptest.NewRequest(http.MethodGet, "/admin/log/delete", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	require.Equal(t, http.StatusMethodNotAllowed, w.Code)
}

// ---- NewInstanceDeleteHandler tests ----

func TestInstanceDeleteHandler_Success(t *testing.T) {
	var gotBucket, gotRoot string
	handler := NewInstanceDeleteHandler(func(bucketName, rootPath string) error {
		gotBucket = bucketName
		gotRoot = rootPath
		return nil
	})

	body := strings.NewReader(`{"bucketName":"mybucket","rootPath":"my/root"}`)
	req := httptest.NewRequest(http.MethodPost, "/admin/instance/delete", body)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Header().Get("Content-Type"), "application/json")
	assert.Contains(t, w.Body.String(), `"ok"`)
	assert.Equal(t, "mybucket", gotBucket)
	assert.Equal(t, "my/root", gotRoot)
}

func TestInstanceDeleteHandler_BadJSON(t *testing.T) {
	handler := NewInstanceDeleteHandler(func(bucketName, rootPath string) error {
		return nil
	})

	body := strings.NewReader(`{bad json}`)
	req := httptest.NewRequest(http.MethodPost, "/admin/instance/delete", body)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "error")
}

func TestInstanceDeleteHandler_CallbackError(t *testing.T) {
	handler := NewInstanceDeleteHandler(func(bucketName, rootPath string) error {
		return errors.New("instance evict failed")
	})

	body := strings.NewReader(`{"bucketName":"b","rootPath":"r"}`)
	req := httptest.NewRequest(http.MethodPost, "/admin/instance/delete", body)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	require.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Contains(t, w.Body.String(), "instance evict failed")
}

func TestInstanceDeleteHandler_MethodNotAllowed(t *testing.T) {
	handler := NewInstanceDeleteHandler(func(bucketName, rootPath string) error {
		return nil
	})

	req := httptest.NewRequest(http.MethodGet, "/admin/instance/delete", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	require.Equal(t, http.StatusMethodNotAllowed, w.Code)
}
