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

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/logger"
)

// NewLogDeleteHandler returns an http.HandlerFunc for POST /admin/log/delete.
// markDeleted is a callback that marks a single log as deleted on the node.
func NewLogDeleteHandler(markDeleted func(bucketName, rootPath string, logId int64) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			BucketName string `json:"bucketName"`
			RootPath   string `json:"rootPath"`
			LogId      int64  `json:"logId"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid request body: " + err.Error()})
			return
		}
		logger.Ctx(r.Context()).Info("log delete requested",
			zap.String("remoteAddr", r.RemoteAddr),
			zap.String("bucketName", body.BucketName),
			zap.String("rootPath", body.RootPath),
			zap.Int64("logId", body.LogId))
		if err := markDeleted(body.BucketName, body.RootPath, body.LogId); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	}
}

// NewInstanceDeleteHandler returns an http.HandlerFunc for POST /admin/instance/delete.
// markDeleted is a callback that marks a whole instance as deleted on the node.
func NewInstanceDeleteHandler(markDeleted func(bucketName, rootPath string) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			BucketName string `json:"bucketName"`
			RootPath   string `json:"rootPath"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid request body: " + err.Error()})
			return
		}
		logger.Ctx(r.Context()).Info("instance delete requested",
			zap.String("remoteAddr", r.RemoteAddr),
			zap.String("bucketName", body.BucketName),
			zap.String("rootPath", body.RootPath))
		if err := markDeleted(body.BucketName, body.RootPath); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	}
}
