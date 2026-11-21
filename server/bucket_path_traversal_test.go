// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zilliztech/woodpecker/common/config"
)

// A caller-supplied bucketName is joined into local storage paths. A ".."
// segment must be rejected before any marker or data directory is touched.
func TestBucketNameCannotEscapeStorageRoot(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "data")
	victimDir := filepath.Join(base, "victim", "7")
	victimFile := filepath.Join(victimDir, "keep.txt")
	require.NoError(t, os.MkdirAll(victimDir, 0o755))
	require.NoError(t, os.WriteFile(victimFile, []byte("do not delete"), 0o644))

	cfg, err := config.NewConfiguration()
	require.NoError(t, err)
	cfg.Woodpecker.Storage.Type = "service"
	cfg.Woodpecker.Storage.RootPath = root
	store := createTestLogStore()
	store.cfg = cfg

	// Same call the gRPC MarkLogDeleted / HTTP admin EvictLog endpoint makes with
	// request.BucketName verbatim.
	_, err = store.EvictLog(context.Background(), "../victim", "", 7, true)
	require.Error(t, err, "traversal bucketName must be rejected")
	_, statErr := os.Stat(victimFile)
	require.NoError(t, statErr, "bucketName='../victim' deleted data outside the storage root")
}

func TestEvictLogValidBucketStillRemovesLocalData(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "data")
	dataDir := filepath.Join(root, "good-bucket", "scope", "7")
	require.NoError(t, os.MkdirAll(dataDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dataDir, "data.log"), []byte("x"), 0o644))

	cfg, err := config.NewConfiguration()
	require.NoError(t, err)
	cfg.Woodpecker.Storage.Type = "service"
	cfg.Woodpecker.Storage.RootPath = root
	store := createTestLogStore()
	store.cfg = cfg

	existed, err := store.EvictLog(context.Background(), "good-bucket", "scope", 7, true)
	require.NoError(t, err)
	require.True(t, existed)
	_, statErr := os.Stat(dataDir)
	require.True(t, os.IsNotExist(statErr), "valid bucket's local data was not reclaimed")
}
