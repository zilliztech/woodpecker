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

package codec

// SearchBlock searches for a block in a list of index records
// Returns the index record if found, or nil if not found
func SearchBlock(list []*IndexRecord, entryId int64) (*IndexRecord, error) {
	low, high := 0, len(list)-1
	var candidate *IndexRecord

	for low <= high {
		mid := (low + high) / 2
		block := list[mid]

		firstEntryID := block.FirstEntryID
		if firstEntryID > entryId {
			high = mid - 1
		} else {
			lastEntryID := block.LastEntryID
			if lastEntryID >= entryId {
				candidate = block
				return candidate, nil
			} else {
				low = mid + 1
			}
		}
	}
	return candidate, nil
}
