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

package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/server/storage/codec"
)

// InspectMaxBlocksDefault bounds a survey that did not ask for a bound, and InspectMaxBlocksLimit
// bounds one that asked for too much. A full survey reads every byte of the segment, so this is an
// explicitly heavy operation and the node keeps it within its own means.
const (
	InspectMaxBlocksDefault = int64(64)
	InspectMaxBlocksLimit   = int64(4096)
)

// surveyStopNoLocalBlocks is this layer's own: there is no local file to walk. A compacted segment
// keeps its blocks as objects that every replica shares, so surveying it per replica would be one
// copy answered repeatedly.
const surveyStopNoLocalBlocks = "no_local_blocks"

// SegmentInspectRequest is one node's part of a survey.
type SegmentInspectRequest struct {
	Bucket    string
	RootPath  string
	LogID     int64
	SegmentID int64
	FromBlock int64
	MaxBlocks int64
}

// SegmentInspectResult is what this node holds and what a walk of its blocks found.
type SegmentInspectResult struct {
	NodeID    string              `json:"node_id"`
	Bucket    string              `json:"bucket_name"`
	RootPath  string              `json:"root_path"`
	LogID     int64               `json:"log_id"`
	SegmentID int64               `json:"segment_id"`
	Local     SegmentLocalFacts   `json:"local"`
	Source    string              `json:"source"`
	Survey    codec.SegmentSurvey `json:"survey"`
	ElapsedMs int64               `json:"elapsed_ms"`
}

// InspectSegment walks this node's copy of a segment block by block and reports what it found at
// each one, continuing past a block it could not read. A read stops at the first failure, so
// nothing else can say whether one block is damaged or everything after it is.
//
// It answers for this node alone and asks no peer. It opens the segment file read-only and creates
// nothing: the local facts come from stat calls, and a node with no local file opens nothing at all.
func (l *logStore) InspectSegment(ctx context.Context, req SegmentInspectRequest) (*SegmentInspectResult, error) {
	if l.stopped.Load() {
		return nil, werr.ErrLogStoreShutdown
	}
	req.MaxBlocks = inspectBound(req.MaxBlocks)
	if req.FromBlock < 0 {
		req.FromBlock = 0
	}
	bucket, rootPath, err := l.resolveProbeInstance(SegmentProbeRequest{
		Bucket: req.Bucket, RootPath: req.RootPath, LogID: req.LogID, SegmentID: req.SegmentID,
	})
	if err != nil {
		return nil, err
	}

	facts := l.localSegmentFacts(bucket, rootPath, req.LogID, req.SegmentID)
	source, _ := l.probeSource(facts)
	result := &SegmentInspectResult{
		NodeID: l.GetAddress(), Bucket: bucket, RootPath: rootPath,
		LogID: req.LogID, SegmentID: req.SegmentID,
		Local: facts, Source: source,
		Survey: codec.SegmentSurvey{
			Blocks: []codec.BlockReport{}, TotalBlocksKnown: -1, LAC: -1,
			StopReason: surveyStopNoLocalBlocks,
		},
	}
	if !facts.DataLog {
		// Nothing local to walk, and nothing is opened to find that out.
		return result, nil
	}

	start := time.Now()
	path := filepath.Join(l.localSegmentDir(bucket, rootPath, req.LogID, req.SegmentID), "data.log")
	file, err := os.Open(path) //nolint:gosec // a path this node built from its own storage root
	if err != nil {
		return nil, werr.ErrSegmentNotFound.WithCauseErrMsg(fmt.Sprintf("open %s: %v", path, err))
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, werr.ErrSegmentNotFound.WithCauseErrMsg(fmt.Sprintf("stat %s: %v", path, err))
	}

	result.Survey = codec.InspectBlocks(ctx, file, info.Size(), req.FromBlock, req.MaxBlocks)
	result.ElapsedMs = time.Since(start).Milliseconds()
	return result, nil
}

// inspectBound keeps a survey within bounds the node sets, whatever the caller asked for.
func inspectBound(asked int64) int64 {
	switch {
	case asked <= 0:
		return InspectMaxBlocksDefault
	case asked > InspectMaxBlocksLimit:
		return InspectMaxBlocksLimit
	default:
		return asked
	}
}
