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
)

// InstanceDataCallback returns the node's local-data inventory, optionally filtered to
// one (bucketName, rootPath) instance. Empty strings mean "all instances".
type InstanceDataCallback func(bucketName, rootPath string) any

// NewInstanceDataHandler serves GET /admin/instance/data, the read half of the
// /admin/instance family whose write half is POST /admin/instance/delete.
//
// Optional query params: ?bucket_name=<bucket>&root_path=<root>. Filter semantics follow
// /admin/log-health, the only other endpoint taking these two params: both must be
// supplied or the filter is ignored, and the common spelling variants are accepted. The
// payload echoes the filter that actually took effect, so a caller that misspelled a
// param can see it received an unfiltered answer instead of mistaking it for one
// instance's data.
func NewInstanceDataHandler(get InstanceDataCallback) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}
		bucketName := firstNonEmpty(
			r.URL.Query().Get("bucket_name"),
			r.URL.Query().Get("bucketName"),
			r.URL.Query().Get("bucketname"),
		)
		rootPath := firstNonEmpty(
			r.URL.Query().Get("root_path"),
			r.URL.Query().Get("rootPath"),
			r.URL.Query().Get("rootpath"),
		)
		if bucketName == "" || rootPath == "" {
			bucketName, rootPath = "", "" // partial filter -> no filter
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(get(bucketName, rootPath))
	}
}
