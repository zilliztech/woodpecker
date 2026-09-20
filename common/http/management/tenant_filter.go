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

import "net/http"

// tenantFilter parses the (bucket_name, root_path) instance filter that
// /admin/log-health and /admin/instance/data both accept.
//
// Both params must be present or the filter is dropped entirely — a partial filter
// would otherwise silently widen the query to every tenant, which is the last thing
// a caller feeding a deletion decision should get. The common spelling variants are
// accepted so no caller ends up with an unfiltered answer over casing alone. Callers
// echo the returned pair back in their payload, which is how one that misspelled a
// param can see that no filter took effect.
//
// The two endpoints share this so their filter semantics cannot drift apart.
func tenantFilter(r *http.Request) (bucketName, rootPath string) {
	bucketName = firstNonEmpty(
		r.URL.Query().Get("bucket_name"),
		r.URL.Query().Get("bucketName"),
		r.URL.Query().Get("bucketname"),
	)
	rootPath = firstNonEmpty(
		r.URL.Query().Get("root_path"),
		r.URL.Query().Get("rootPath"),
		r.URL.Query().Get("rootpath"),
	)
	if bucketName == "" || rootPath == "" {
		return "", "" // partial filter -> no filter
	}
	return bucketName, rootPath
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
