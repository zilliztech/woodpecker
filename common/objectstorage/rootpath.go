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

package objectstorage

import "strings"

// NormalizeRootPathForKey strips leading/trailing/doubled slashes from rootPath so an
// object-storage key built from it is canonical. It is a no-op for a clean rootPath (the
// expected case: rootPath is operator-set configuration, not request input) and exists so
// every key-building site — the staged writer/reader/delete-GC, the compacted-file-cleanup
// footer HEAD, and the client's direct reader — derives the exact same key from the same
// configuration value.
func NormalizeRootPathForKey(rootPath string) string {
	segs := make([]string, 0, strings.Count(rootPath, "/")+1)
	for _, p := range strings.Split(rootPath, "/") {
		if p != "" {
			segs = append(segs, p)
		}
	}
	return strings.Join(segs, "/")
}
