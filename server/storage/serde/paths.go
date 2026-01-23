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

package serde

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
)

// =============================================================================
// File Path Constants
// =============================================================================

const (
	// BlockFileExtension is the extension for block files
	BlockFileExtension = ".blk"
	// InflightSuffix is appended to files during write (stage 1)
	InflightSuffix = ".inflight"
	// CompletedSuffix is appended to files after write but before final rename (stage 2)
	CompletedSuffix = ".completed"
	// FooterFileName is the name of the footer file
	FooterFileName = "footer.blk"
	// WriteLockFileName is the name of the write lock file
	WriteLockFileName = "write.lock"
	// MergedBlockPrefix is the prefix for merged (compacted) block files
	MergedBlockPrefix = "m_"
)

// =============================================================================
// Local File System Path Helpers
// =============================================================================

// GetSegmentDir returns the directory path for a segment.
// Format: {baseDir}/{logId}/{segmentId}
func GetSegmentDir(baseDir string, logId int64, segmentId int64) string {
	return filepath.Join(baseDir, fmt.Sprintf("%d/%d", logId, segmentId))
}

// Note: GetSegmentFilePath (legacy data.log path) has been moved to
// server/storage/disk/paths_legacy.go and server/storage/stagedstorage/paths_legacy.go

// GetBlockFilePath returns the path to a block file.
// Format: {baseDir}/{logId}/{segmentId}/{blockId}.blk
func GetBlockFilePath(baseDir string, logId int64, segmentId int64, blockId int64) string {
	return filepath.Join(GetSegmentDir(baseDir, logId, segmentId), fmt.Sprintf("%d%s", blockId, BlockFileExtension))
}

// GetInflightBlockPath returns the path to an inflight block file (stage 1 of atomic write).
// Format: {baseDir}/{logId}/{segmentId}/{blockId}.blk.inflight
func GetInflightBlockPath(baseDir string, logId int64, segmentId int64, blockId int64) string {
	return filepath.Join(GetSegmentDir(baseDir, logId, segmentId), fmt.Sprintf("%d%s%s", blockId, BlockFileExtension, InflightSuffix))
}

// GetCompletedBlockPath returns the path to a completed block file (stage 2 of atomic write).
// Format: {baseDir}/{logId}/{segmentId}/{blockId}.blk.completed
func GetCompletedBlockPath(baseDir string, logId int64, segmentId int64, blockId int64) string {
	return filepath.Join(GetSegmentDir(baseDir, logId, segmentId), fmt.Sprintf("%d%s%s", blockId, BlockFileExtension, CompletedSuffix))
}

// GetFooterBlockPath returns the path to the footer block file.
// Format: {baseDir}/{logId}/{segmentId}/footer.blk
func GetFooterBlockPath(baseDir string, logId int64, segmentId int64) string {
	return filepath.Join(GetSegmentDir(baseDir, logId, segmentId), FooterFileName)
}

// GetFooterInflightPath returns the path to the inflight footer file.
// Format: {baseDir}/{logId}/{segmentId}/footer.blk.inflight
func GetFooterInflightPath(baseDir string, logId int64, segmentId int64) string {
	return filepath.Join(GetSegmentDir(baseDir, logId, segmentId), FooterFileName+InflightSuffix)
}

// Note: GetFenceFlagPath and GetFenceBlockInflightPath have been moved to
// server/storage/disk/paths_legacy.go as they are disk-specific legacy functions.

// GetFenceBlockDirPath returns the path to a fence directory.
// The fence is implemented as a directory with name {blockId}.blk
// When a stale writer tries to rename a file to this path, it will fail because
// renaming a file to a directory path fails on all operating systems.
// Format: {baseDir}/{logId}/{segmentId}/{blockId}.blk (as directory)
func GetFenceBlockDirPath(baseDir string, logId int64, segmentId int64, blockId int64) string {
	return filepath.Join(GetSegmentDir(baseDir, logId, segmentId), fmt.Sprintf("%d%s", blockId, BlockFileExtension))
}

// GetMergedBlockFilePath returns the path to a merged (compacted) block file.
// Format: {baseDir}/{logId}/{segmentId}/m_{blockId}.blk
func GetMergedBlockFilePath(baseDir string, logId int64, segmentId int64, blockId int64) string {
	return filepath.Join(GetSegmentDir(baseDir, logId, segmentId), fmt.Sprintf("%s%d%s", MergedBlockPrefix, blockId, BlockFileExtension))
}

// GetMergedBlockInflightPath returns the path to an inflight merged block file (stage 1 of atomic write).
// Format: {baseDir}/{logId}/{segmentId}/m_{blockId}.blk.inflight
func GetMergedBlockInflightPath(baseDir string, logId int64, segmentId int64, blockId int64) string {
	return filepath.Join(GetSegmentDir(baseDir, logId, segmentId), fmt.Sprintf("%s%d%s%s", MergedBlockPrefix, blockId, BlockFileExtension, InflightSuffix))
}

// GetMergedBlockCompletedPath returns the path to a completed merged block file (stage 2 of atomic write).
// Format: {baseDir}/{logId}/{segmentId}/m_{blockId}.blk.completed
func GetMergedBlockCompletedPath(baseDir string, logId int64, segmentId int64, blockId int64) string {
	return filepath.Join(GetSegmentDir(baseDir, logId, segmentId), fmt.Sprintf("%s%d%s%s", MergedBlockPrefix, blockId, BlockFileExtension, CompletedSuffix))
}

// =============================================================================
// Object Storage Key Helpers
// =============================================================================

// GetSegmentKey returns the key prefix for a segment in object storage.
// Format: {baseDir}/{logId}/{segmentId}
func GetSegmentKey(baseDir string, logId int64, segmentId int64) string {
	return fmt.Sprintf("%s/%d/%d", baseDir, logId, segmentId)
}

// GetBlockKey returns the key for a block in object storage.
// Format: {segmentKey}/{blockId}.blk
func GetBlockKey(segmentKey string, blockId int64) string {
	return fmt.Sprintf("%s/%d%s", segmentKey, blockId, BlockFileExtension)
}

// GetMergedBlockKey returns the key for a merged (compacted) block in object storage.
// Format: {segmentKey}/m_{blockId}.blk
func GetMergedBlockKey(segmentKey string, blockId int64) string {
	return fmt.Sprintf("%s/%s%d%s", segmentKey, MergedBlockPrefix, blockId, BlockFileExtension)
}

// GetFooterKey returns the key for the footer in object storage.
// Format: {segmentKey}/footer.blk
func GetFooterKey(segmentKey string) string {
	return fmt.Sprintf("%s/%s", segmentKey, FooterFileName)
}

// GetWriteLockKey returns the key for the write lock in object storage.
// Format: {segmentKey}/write.lock
func GetWriteLockKey(segmentKey string) string {
	return fmt.Sprintf("%s/%s", segmentKey, WriteLockFileName)
}

// =============================================================================
// Path Parsing Helpers
// =============================================================================

// ParseBlockIdFromPath parses the block ID from a block file path or key.
// Returns the block ID, whether it's a merged block, and any error.
func ParseBlockIdFromPath(path string) (blockId int64, isMerged bool, err error) {
	filename := filepath.Base(path)

	// Remove extension
	name := strings.TrimSuffix(filename, BlockFileExtension)

	// Check if it's a merged block
	if strings.HasPrefix(name, MergedBlockPrefix) {
		isMerged = true
		idStr := strings.TrimPrefix(name, MergedBlockPrefix)
		blockId, err = strconv.ParseInt(idStr, 10, 64)
		return blockId, isMerged, err
	}

	// Regular block
	isMerged = false
	blockId, err = strconv.ParseInt(name, 10, 64)
	return blockId, isMerged, err
}

// IsBlockFile checks if the given filename is a valid block file (not inflight or completed).
func IsBlockFile(filename string) bool {
	if !strings.HasSuffix(filename, BlockFileExtension) {
		return false
	}
	if strings.HasSuffix(filename, InflightSuffix) {
		return false
	}
	if strings.HasSuffix(filename, CompletedSuffix) {
		return false
	}
	// Exclude footer.blk
	if filename == FooterFileName {
		return false
	}
	return true
}

// IsInflightFile checks if the given filename is an inflight file.
func IsInflightFile(filename string) bool {
	return strings.HasSuffix(filename, InflightSuffix)
}

// IsCompletedFile checks if the given filename is a completed file (stage 2).
func IsCompletedFile(filename string) bool {
	return strings.HasSuffix(filename, CompletedSuffix)
}

// IsMergedBlockFile checks if the given filename is a merged (compacted) block file.
func IsMergedBlockFile(filename string) bool {
	name := filepath.Base(filename)
	return strings.HasPrefix(name, MergedBlockPrefix) && strings.HasSuffix(name, BlockFileExtension)
}

// IsMergedBlockInflightFile checks if the given filename is an inflight merged block file.
func IsMergedBlockInflightFile(filename string) bool {
	name := filepath.Base(filename)
	return strings.HasPrefix(name, MergedBlockPrefix) && strings.HasSuffix(name, BlockFileExtension+InflightSuffix)
}

// IsMergedBlockCompletedFile checks if the given filename is a completed merged block file.
func IsMergedBlockCompletedFile(filename string) bool {
	name := filepath.Base(filename)
	return strings.HasPrefix(name, MergedBlockPrefix) && strings.HasSuffix(name, BlockFileExtension+CompletedSuffix)
}

// IsOriginalBlockFile checks if the given filename is an original (non-merged) block file.
// This excludes merged blocks, inflight files, completed files, footer, and legacy data files.
func IsOriginalBlockFile(filename string) bool {
	name := filepath.Base(filename)
	// Must end with .blk
	if !strings.HasSuffix(name, BlockFileExtension) {
		return false
	}
	// Exclude inflight and completed files
	if strings.HasSuffix(name, InflightSuffix) || strings.HasSuffix(name, CompletedSuffix) {
		return false
	}
	// Exclude merged blocks
	if strings.HasPrefix(name, MergedBlockPrefix) {
		return false
	}
	// Exclude footer.blk
	if name == FooterFileName {
		return false
	}
	return true
}

// IsFooterFile checks if the given filename is the footer file.
func IsFooterFile(filename string) bool {
	return filepath.Base(filename) == FooterFileName
}

// Note: IsLegacyDataFile has been moved to server/storage/disk/paths_legacy.go

// IsFenceDirectory checks if the given path is a fence directory.
// A fence directory is a directory named {blockId}.blk that prevents stale writers
// from writing to that block position. Returns the block ID if it's a fence directory.
func IsFenceDirectory(name string, isDir bool) (bool, int64) {
	if !isDir {
		return false, -1
	}
	// Check if name matches pattern {blockId}.blk
	if !strings.HasSuffix(name, BlockFileExtension) {
		return false, -1
	}
	// Exclude footer.blk
	if name == FooterFileName {
		return false, -1
	}
	// Exclude merged blocks
	if strings.HasPrefix(name, MergedBlockPrefix) {
		return false, -1
	}
	// Try to parse block ID
	idStr := strings.TrimSuffix(name, BlockFileExtension)
	blockId, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return false, -1
	}
	return true, blockId
}
