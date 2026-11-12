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

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/mocks/mocks_objectstorage"
	"github.com/zilliztech/woodpecker/server/storage/codec"
)

func recoveryLogContext(parent context.Context) (context.Context, *observer.ObservedLogs) {
	core, logs := observer.New(zapcore.DebugLevel)
	return context.WithValue(parent, logger.CtxLogKey, zap.New(core)), logs
}

func TestRecoveryScanNormalLogsStayAtDebug(t *testing.T) {
	for _, blocks := range []int{0, 3} {
		for _, fenced := range []bool{false, true} {
			t.Run(fmt.Sprintf("blocks=%d/fenced=%t", blocks, fenced), func(t *testing.T) {
				ctx, logs := recoveryLogContext(context.Background())
				w := newTestMinioFileWriter()
				t.Cleanup(w.pool.Release)
				m := mocks_objectstorage.NewObjectStorage(t)
				w.client = m
				m.EXPECT().IsObjectNotExistsError(mock.Anything).Return(true).Maybe()
				for block := int64(0); block < int64(blocks); block++ {
					expectCheckpointTailBlock(t, w, m, block)
				}
				expectCheckpointEnd(w, m, int64(blocks), fenced)
				result, err := w.scanContiguousBlocks(ctx, 0, -1)
				require.NoError(t, err)
				require.Len(t, result.indexes, blocks)
				// At most one detail per recovered block plus a normal termination event.
				// Neither the scanner nor its header parser should emit Info/Warn per block.
				require.Equal(t, blocks+1, logs.Len())
				for _, entry := range logs.All() {
					require.Equal(t, zapcore.DebugLevel, entry.Level)
				}
			})
		}
	}
}

type failingRecoveryReader struct {
	*mockFileReader
	err error
}

func (r *failingRecoveryReader) Read([]byte) (int, error) { return 0, r.err }

func TestRecoveryScanLogsFailureOnceWithContext(t *testing.T) {
	for _, failurePoint := range []string{"stat", "get", "read", "truncated_header", "empty", "missing_header", "validate", "canceled"} {
		t.Run(failurePoint, func(t *testing.T) {
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx, logs := recoveryLogContext(parent)
			w := newTestMinioFileWriter()
			t.Cleanup(w.pool.Release)
			m := mocks_objectstorage.NewObjectStorage(t)
			w.client = m
			key := getBlockKey(w.segmentFileKey, 1)
			failure := errors.New("temporary storage failure")
			expectedOperation := "read_header"
			expectedLevel := zapcore.WarnLevel
			expectedError := ""
			switch failurePoint {
			case "stat", "canceled":
				expectedOperation = "stat"
				if failurePoint == "canceled" {
					cancel()
					failure = context.Canceled
					expectedLevel = zapcore.DebugLevel
				}
				m.EXPECT().StatObject(mock.Anything, w.bucket, key, mock.Anything, mock.Anything).Return(int64(0), false, failure).Once()
				m.EXPECT().IsObjectNotExistsError(failure).Return(false).Once()
			default:
				m.EXPECT().StatObject(mock.Anything, w.bucket, key, mock.Anything, mock.Anything).Return(int64(128), false, nil).Once()
				readSize := int64(codec.RecordHeaderSize + codec.BlockHeaderRecordSize)
				switch failurePoint {
				case "get":
					m.EXPECT().GetObject(mock.Anything, w.bucket, key, int64(0), readSize, mock.Anything, mock.Anything).Return(nil, failure).Once()
				case "read":
					m.EXPECT().GetObject(mock.Anything, w.bucket, key, int64(0), readSize, mock.Anything, mock.Anything).Return(&failingRecoveryReader{mockFileReader: &mockFileReader{}, err: failure}, nil).Once()
				default:
					var data []byte
					switch failurePoint {
					case "truncated_header":
						data = []byte{0xff}
						expectedError = "BlockHeaderRecord not found"
					case "empty":
						expectedError = "empty data"
					case "missing_header":
						data = codec.EncodeRecord(&codec.HeaderRecord{Version: codec.FormatVersion})
						expectedError = "BlockHeaderRecord not found"
					case "validate":
						data = codec.EncodeRecord(&codec.BlockHeaderRecord{BlockNumber: 1, FirstEntryID: 2, LastEntryID: 2})
						expectedOperation = "validate_header"
						expectedError = "non-contiguous block header"
					}
					m.EXPECT().GetObject(mock.Anything, w.bucket, key, int64(0), readSize, mock.Anything, mock.Anything).Return(&mockFileReader{data: data}, nil).Once()
				}
			}
			result, err := w.scanContiguousBlocks(ctx, 1, 0)
			require.Error(t, err)
			require.Nil(t, result)
			if expectedError != "" {
				require.ErrorContains(t, err, expectedError)
			} else {
				require.ErrorIs(t, err, failure)
			}
			// A parser/transport failure must not be logged at several recovery layers.
			require.Equal(t, 1, logs.Len())
			entry := logs.All()[0]
			require.Equal(t, expectedLevel, entry.Level)
			fields := entry.ContextMap()
			require.Equal(t, w.segmentFileKey, fields["segmentFileKey"])
			require.Equal(t, key, fields["blockKey"])
			require.Equal(t, int64(1), fields["startBlockID"])
			require.Equal(t, int64(1), fields["blockID"])
			require.Equal(t, int64(0), fields["scannedBlocks"])
			require.Equal(t, expectedOperation, fields["operation"])
			require.NotEmpty(t, fields["error"])
			require.Contains(t, fields, "elapsed")
		})
	}
}
