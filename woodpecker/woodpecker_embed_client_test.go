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

package woodpecker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_objectstorage"
	"github.com/zilliztech/woodpecker/mocks/mocks_server"
	mocks_logstore_client "github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_logstore_client"
	"github.com/zilliztech/woodpecker/proto"
)

// TestDetectAndStoreConditionWriteCapability_Disabled tests that detection is skipped when explicitly disabled
func TestDetectAndStoreConditionWriteCapability_Disabled(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				FencePolicy: config.FencePolicyConfig{
					ConditionWrite: "disable",
				},
			},
		},
	}

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockStorage := mocks_objectstorage.NewObjectStorage(t)

	// Should not call any methods
	result, err := detectAndStoreConditionWriteCapability(ctx, cfg, mockMetadata, mockStorage)

	assert.NoError(t, err)
	assert.False(t, result)
}

// TestDetectAndStoreConditionWriteCapability_ExistingResultInEtcd tests reading existing result from etcd
func TestDetectAndStoreConditionWriteCapability_ExistingResultInEtcd(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				FencePolicy: config.FencePolicyConfig{
					ConditionWrite: "auto",
				},
			},
		},
		Minio: config.MinioConfig{
			BucketName: "test-bucket",
			RootPath:   "test-path",
		},
	}

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockStorage := mocks_objectstorage.NewObjectStorage(t)

	// Mock: etcd already has result
	mockMetadata.EXPECT().GetConditionWriteResult(mock.Anything).Return(true, nil).Once()

	result, err := detectAndStoreConditionWriteCapability(ctx, cfg, mockMetadata, mockStorage)

	assert.NoError(t, err)
	assert.True(t, result)
}

// TestDetectAndStoreConditionWriteCapability_AutoModeSuccess tests auto mode with successful detection
func TestDetectAndStoreConditionWriteCapability_AutoModeSuccess(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				FencePolicy: config.FencePolicyConfig{
					ConditionWrite: "auto",
				},
			},
		},
		Minio: config.MinioConfig{
			BucketName: "test-bucket",
			RootPath:   "test-path",
		},
	}

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockStorage := mocks_objectstorage.NewObjectStorage(t)

	// Mock: no existing result in etcd
	mockMetadata.EXPECT().GetConditionWriteResult(mock.Anything).Return(false, werr.ErrMetadataKeyNotExists).Once()

	// Mock: detection succeeds and stores successfully
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(werr.ErrObjectAlreadyExists).Once()
	mockStorage.EXPECT().PutFencedObject(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(werr.ErrObjectAlreadyExists).Once()
	mockStorage.EXPECT().PutFencedObject(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(nil).Times(2)
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(werr.ErrSegmentFenced).Once()
	mockStorage.EXPECT().RemoveObject(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(nil).Times(2)

	mockMetadata.EXPECT().StoreOrGetConditionWriteResult(mock.Anything, true).Return(true, nil).Once()

	result, err := detectAndStoreConditionWriteCapability(ctx, cfg, mockMetadata, mockStorage)

	assert.NoError(t, err)
	assert.True(t, result)
}

// TestDetectAndStoreConditionWriteCapability_AutoModeFailureWithFallback tests auto mode falling back to disable
func TestDetectAndStoreConditionWriteCapability_AutoModeFailureWithFallback(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				FencePolicy: config.FencePolicyConfig{
					ConditionWrite: "auto",
				},
			},
		},
		Minio: config.MinioConfig{
			BucketName: "test-bucket",
			RootPath:   "test-path",
		},
	}

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockStorage := mocks_objectstorage.NewObjectStorage(t)

	// Mock: no existing result in etcd
	mockMetadata.EXPECT().GetConditionWriteResult(mock.Anything).Return(false, werr.ErrMetadataKeyNotExists).Maybe()

	// Mock: detection always fails
	detectErr := fmt.Errorf("storage not supported")
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(detectErr).Maybe()
	mockStorage.EXPECT().RemoveObject(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(nil).Maybe()

	// Mock: store false result after fallback
	mockMetadata.EXPECT().StoreOrGetConditionWriteResult(mock.Anything, false).Return(false, nil).Once()

	result, err := detectAndStoreConditionWriteCapability(ctx, cfg, mockMetadata, mockStorage)

	assert.NoError(t, err)
	assert.False(t, result)
}

// TestDetectAndStoreConditionWriteCapability_EnableModePanicOnFailure tests enable mode panics on failure
func TestDetectAndStoreConditionWriteCapability_EnableModePanicOnFailure(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				FencePolicy: config.FencePolicyConfig{
					ConditionWrite: "enable",
				},
			},
		},
		Minio: config.MinioConfig{
			BucketName: "test-bucket",
			RootPath:   "test-path",
		},
	}

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockStorage := mocks_objectstorage.NewObjectStorage(t)

	// Mock: no existing result in etcd
	mockMetadata.EXPECT().GetConditionWriteResult(mock.Anything).Return(false, werr.ErrMetadataKeyNotExists).Maybe()

	// Mock: detection always fails
	detectErr := fmt.Errorf("storage not supported")
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(detectErr).Maybe()
	mockStorage.EXPECT().RemoveObject(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(nil).Maybe()

	// Should panic after 10 retries
	assert.Panics(t, func() {
		_, _ = detectAndStoreConditionWriteCapability(ctx, cfg, mockMetadata, mockStorage)
	})
}

// TestDetectAndStoreConditionWriteCapability_AnotherNodeSetsResult tests that detection stops when another node sets result
func TestDetectAndStoreConditionWriteCapability_AnotherNodeSetsResult(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				FencePolicy: config.FencePolicyConfig{
					ConditionWrite: "auto",
				},
			},
		},
		Minio: config.MinioConfig{
			BucketName: "test-bucket",
			RootPath:   "test-path",
		},
	}

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockStorage := mocks_objectstorage.NewObjectStorage(t)

	// First call: no result
	mockMetadata.EXPECT().GetConditionWriteResult(mock.Anything).Return(false, werr.ErrMetadataKeyNotExists).Once()

	// Detection fails on first attempt
	detectErr := fmt.Errorf("temporary error")
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(detectErr).Once()
	mockStorage.EXPECT().RemoveObject(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(nil).Maybe()

	// Second retry: another node has set the result
	mockMetadata.EXPECT().GetConditionWriteResult(mock.Anything).Return(true, nil).Once()

	// When we get result from etcd, we don't need to store it again, just return it
	result, err := detectAndStoreConditionWriteCapability(ctx, cfg, mockMetadata, mockStorage)

	assert.NoError(t, err)
	assert.True(t, result)
}

// TestDetectAndStoreConditionWriteCapability_StoreFailureThenRetrySuccess tests retry after etcd store failure
func TestDetectAndStoreConditionWriteCapability_StoreFailureThenRetrySuccess(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				FencePolicy: config.FencePolicyConfig{
					ConditionWrite: "auto",
				},
			},
		},
		Minio: config.MinioConfig{
			BucketName: "test-bucket",
			RootPath:   "test-path",
		},
	}

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockStorage := mocks_objectstorage.NewObjectStorage(t)

	// Mock: no existing result in etcd
	mockMetadata.EXPECT().GetConditionWriteResult(mock.Anything).Return(false, werr.ErrMetadataKeyNotExists).Once()

	// Detection succeeds once
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(werr.ErrObjectAlreadyExists).Once()
	mockStorage.EXPECT().PutFencedObject(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(werr.ErrObjectAlreadyExists).Once()
	mockStorage.EXPECT().PutFencedObject(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(nil).Times(2)
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(werr.ErrSegmentFenced).Once()
	mockStorage.EXPECT().RemoveObject(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(nil).Times(2)

	// Storing to etcd fails on first attempt
	etcdErr := fmt.Errorf("etcd connection failed")
	mockMetadata.EXPECT().StoreOrGetConditionWriteResult(mock.Anything, true).Return(false, etcdErr).Once()

	// Second store attempt succeeds (etcd retry, not detection retry)
	mockMetadata.EXPECT().StoreOrGetConditionWriteResult(mock.Anything, true).Return(true, nil).Once()

	result, err := detectAndStoreConditionWriteCapability(ctx, cfg, mockMetadata, mockStorage)

	assert.NoError(t, err)
	assert.True(t, result)
}

// TestDetectAndStoreConditionWriteCapability_DetectionFalseStored tests storing false detection result
func TestDetectAndStoreConditionWriteCapability_DetectionFalseStored(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				FencePolicy: config.FencePolicyConfig{
					ConditionWrite: "auto",
				},
			},
		},
		Minio: config.MinioConfig{
			BucketName: "test-bucket",
			RootPath:   "test-path",
		},
	}

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockStorage := mocks_objectstorage.NewObjectStorage(t)

	// Mock: no existing result in etcd for all retry checks
	mockMetadata.EXPECT().GetConditionWriteResult(mock.Anything).Return(false, werr.ErrMetadataKeyNotExists).Maybe()

	// Mock: detection always fails (not supported)
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(fmt.Errorf("not supported")).Maybe()
	mockStorage.EXPECT().RemoveObject(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(nil).Maybe()

	// After all retries fail, store false
	mockMetadata.EXPECT().StoreOrGetConditionWriteResult(mock.Anything, false).Return(false, nil).Once()

	result, err := detectAndStoreConditionWriteCapability(ctx, cfg, mockMetadata, mockStorage)

	require.NoError(t, err)
	assert.False(t, result)
}

// TestDetectAndStoreConditionWriteCapability_AgreedResultDifferent tests using cluster agreed result when different
func TestDetectAndStoreConditionWriteCapability_AgreedResultDifferent(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				FencePolicy: config.FencePolicyConfig{
					ConditionWrite: "auto",
				},
			},
		},
		Minio: config.MinioConfig{
			BucketName: "test-bucket",
			RootPath:   "test-path",
		},
	}

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockStorage := mocks_objectstorage.NewObjectStorage(t)

	// Mock: no existing result in etcd
	mockMetadata.EXPECT().GetConditionWriteResult(mock.Anything).Return(false, werr.ErrMetadataKeyNotExists).Once()

	// Mock: detection succeeds with true
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(werr.ErrObjectAlreadyExists).Once()
	mockStorage.EXPECT().PutFencedObject(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(werr.ErrObjectAlreadyExists).Once()
	mockStorage.EXPECT().PutFencedObject(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(nil).Times(2)
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(werr.ErrSegmentFenced).Once()
	mockStorage.EXPECT().RemoveObject(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(nil).Times(2)

	// But another node stored false first (CAS returns different value)
	mockMetadata.EXPECT().StoreOrGetConditionWriteResult(mock.Anything, true).Return(false, nil).Once()

	// We directly return the agreed result, no need to store false again
	result, err := detectAndStoreConditionWriteCapability(ctx, cfg, mockMetadata, mockStorage)

	assert.NoError(t, err)
	assert.False(t, result, "Should use the agreed cluster result (false) instead of own detection (true)")
}

// TestDetectAndStoreConditionWriteCapability_DetectionTrueButEtcdAlwaysFails tests detection succeeds but etcd always fails
func TestDetectAndStoreConditionWriteCapability_DetectionTrueButEtcdAlwaysFails(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				FencePolicy: config.FencePolicyConfig{
					ConditionWrite: "auto",
				},
			},
		},
		Minio: config.MinioConfig{
			BucketName: "test-bucket",
			RootPath:   "test-path",
		},
	}

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockStorage := mocks_objectstorage.NewObjectStorage(t)

	// Mock: no existing result in etcd
	mockMetadata.EXPECT().GetConditionWriteResult(mock.Anything).Return(false, werr.ErrMetadataKeyNotExists).Once()

	// Mock: detection succeeds with true (supports condition write)
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(werr.ErrObjectAlreadyExists).Once()
	mockStorage.EXPECT().PutFencedObject(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(werr.ErrObjectAlreadyExists).Once()
	mockStorage.EXPECT().PutFencedObject(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(nil).Times(2)
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(werr.ErrSegmentFenced).Once()
	mockStorage.EXPECT().RemoveObject(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(nil).Times(2)

	// But etcd operations always fail (all 10 retry attempts)
	etcdErr := fmt.Errorf("etcd connection timeout")
	mockMetadata.EXPECT().StoreOrGetConditionWriteResult(mock.Anything, true).Return(false, etcdErr).Maybe()

	result, err := detectAndStoreConditionWriteCapability(ctx, cfg, mockMetadata, mockStorage)

	assert.Error(t, err, "Should return error when etcd operations always fail")
	assert.Contains(t, err.Error(), "failed to store condition write result to etcd")
	assert.False(t, result)
}

// TestDetectAndStoreConditionWriteCapability_DetectionFalseButEtcdAlwaysFails tests detection fails and etcd always fails
func TestDetectAndStoreConditionWriteCapability_DetectionFalseButEtcdAlwaysFails(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				FencePolicy: config.FencePolicyConfig{
					ConditionWrite: "auto",
				},
			},
		},
		Minio: config.MinioConfig{
			BucketName: "test-bucket",
			RootPath:   "test-path",
		},
	}

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockStorage := mocks_objectstorage.NewObjectStorage(t)

	// Mock: no existing result in etcd
	mockMetadata.EXPECT().GetConditionWriteResult(mock.Anything).Return(false, werr.ErrMetadataKeyNotExists).Maybe()

	// Mock: detection always fails (does not support condition write)
	detectErr := fmt.Errorf("storage not supported")
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(detectErr).Maybe()
	mockStorage.EXPECT().RemoveObject(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(nil).Maybe()

	// After all detection retries fail, try to store false but etcd operations always fail
	etcdErr := fmt.Errorf("etcd connection timeout")
	mockMetadata.EXPECT().StoreOrGetConditionWriteResult(mock.Anything, false).Return(false, etcdErr).Maybe()

	result, err := detectAndStoreConditionWriteCapability(ctx, cfg, mockMetadata, mockStorage)

	assert.Error(t, err, "Should return error when etcd operations always fail")
	assert.Contains(t, err.Error(), "failed to store false result to etcd")
	assert.False(t, result)
}

// TestDetectAndStoreConditionWriteCapability_EtcdReturnsAgreedResult tests etcd returns agreed result successfully
func TestDetectAndStoreConditionWriteCapability_EtcdReturnsAgreedResult(t *testing.T) {
	testCases := []struct {
		name                string
		localDetection      bool
		agreedResult        bool
		detectionShouldFail bool
	}{
		{
			name:                "Detection succeeds with true, agreed result is true",
			localDetection:      true,
			agreedResult:        true,
			detectionShouldFail: false,
		},
		{
			name:                "Detection succeeds with true, agreed result is false",
			localDetection:      true,
			agreedResult:        false,
			detectionShouldFail: false,
		},
		{
			name:                "Detection fails (false), agreed result is false",
			localDetection:      false,
			agreedResult:        false,
			detectionShouldFail: true,
		},
		{
			name:                "Detection fails (false), agreed result is true",
			localDetection:      false,
			agreedResult:        true,
			detectionShouldFail: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cfg := &config.Configuration{
				Woodpecker: config.WoodpeckerConfig{
					Logstore: config.LogstoreConfig{
						FencePolicy: config.FencePolicyConfig{
							ConditionWrite: "auto",
						},
					},
				},
				Minio: config.MinioConfig{
					BucketName: "test-bucket",
					RootPath:   "test-path",
				},
			}

			mockMetadata := mocks_meta.NewMetadataProvider(t)
			mockStorage := mocks_objectstorage.NewObjectStorage(t)

			// Mock: no existing result in etcd
			mockMetadata.EXPECT().GetConditionWriteResult(mock.Anything).Return(false, werr.ErrMetadataKeyNotExists).Maybe()

			if tc.detectionShouldFail {
				// Detection always fails
				detectErr := fmt.Errorf("storage not supported")
				mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(detectErr).Maybe()
				mockStorage.EXPECT().RemoveObject(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(nil).Maybe()

				// Store false result and get agreed result
				mockMetadata.EXPECT().StoreOrGetConditionWriteResult(mock.Anything, false).Return(tc.agreedResult, nil).Once()
			} else {
				// Detection succeeds
				mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
				mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(werr.ErrObjectAlreadyExists).Once()
				mockStorage.EXPECT().PutFencedObject(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(werr.ErrObjectAlreadyExists).Once()
				mockStorage.EXPECT().PutFencedObject(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(nil).Times(2)
				mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(werr.ErrSegmentFenced).Once()
				mockStorage.EXPECT().RemoveObject(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(nil).Times(2)

				// Store detection result and get agreed result
				mockMetadata.EXPECT().StoreOrGetConditionWriteResult(mock.Anything, tc.localDetection).Return(tc.agreedResult, nil).Once()
			}

			result, err := detectAndStoreConditionWriteCapability(ctx, cfg, mockMetadata, mockStorage)

			assert.NoError(t, err, "Should not return error when etcd operations succeed")
			assert.Equal(t, tc.agreedResult, result, "Should return the agreed result from etcd")
		})
	}
}

// TestDetectAndStoreConditionWriteCapability_UnexpectedEtcdError tests when GetConditionWriteResult
// returns an unexpected error (not key-not-exists) - the function should continue with detection
func TestDetectAndStoreConditionWriteCapability_UnexpectedEtcdError(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				FencePolicy: config.FencePolicyConfig{
					ConditionWrite: "auto",
				},
			},
		},
		Minio: config.MinioConfig{
			BucketName: "test-bucket",
			RootPath:   "test-path",
		},
	}

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockStorage := mocks_objectstorage.NewObjectStorage(t)

	// First call returns an unexpected error (not key-not-exists) - covers line 137-140
	unexpectedErr := fmt.Errorf("etcd connection refused")
	mockMetadata.EXPECT().GetConditionWriteResult(mock.Anything).Return(false, unexpectedErr).Once()

	// Detection succeeds
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(werr.ErrObjectAlreadyExists).Once()
	mockStorage.EXPECT().PutFencedObject(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(werr.ErrObjectAlreadyExists).Once()
	mockStorage.EXPECT().PutFencedObject(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(nil).Times(2)
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(werr.ErrSegmentFenced).Once()
	mockStorage.EXPECT().RemoveObject(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(nil).Times(2)

	mockMetadata.EXPECT().StoreOrGetConditionWriteResult(mock.Anything, true).Return(true, nil).Once()

	result, err := detectAndStoreConditionWriteCapability(ctx, cfg, mockMetadata, mockStorage)
	assert.NoError(t, err)
	assert.True(t, result)
}

// TestDetectAndStoreConditionWriteCapability_UnexpectedEtcdErrorInRetry tests when GetConditionWriteResult
// returns an unexpected error during retry loop (attempt > 1) - covers lines 175-179
func TestDetectAndStoreConditionWriteCapability_UnexpectedEtcdErrorInRetry(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				FencePolicy: config.FencePolicyConfig{
					ConditionWrite: "auto",
				},
			},
		},
		Minio: config.MinioConfig{
			BucketName: "test-bucket",
			RootPath:   "test-path",
		},
	}

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockStorage := mocks_objectstorage.NewObjectStorage(t)

	// First call: no existing result (key not exists)
	mockMetadata.EXPECT().GetConditionWriteResult(mock.Anything).Return(false, werr.ErrMetadataKeyNotExists).Once()
	// Detection fails on first attempt
	detectErr := fmt.Errorf("temporary error")
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(detectErr).Once()
	mockStorage.EXPECT().RemoveObject(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(nil).Maybe()

	// Second retry: GetConditionWriteResult returns unexpected error (not key-not-exists) - covers line 175-179
	unexpectedErr := fmt.Errorf("etcd timeout")
	mockMetadata.EXPECT().GetConditionWriteResult(mock.Anything).Return(false, unexpectedErr).Once()

	// Detection succeeds on second attempt
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(werr.ErrObjectAlreadyExists).Once()
	mockStorage.EXPECT().PutFencedObject(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(werr.ErrObjectAlreadyExists).Once()
	mockStorage.EXPECT().PutFencedObject(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(nil).Times(2)
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(werr.ErrSegmentFenced).Once()
	mockStorage.EXPECT().RemoveObject(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(nil).Maybe()

	mockMetadata.EXPECT().StoreOrGetConditionWriteResult(mock.Anything, true).Return(true, nil).Once()

	result, err := detectAndStoreConditionWriteCapability(ctx, cfg, mockMetadata, mockStorage)
	assert.NoError(t, err)
	assert.True(t, result)
}

// ============================================================
// Helper for creating test woodpeckerEmbedClient
// ============================================================

func newTestEmbedClient(t *testing.T) (*woodpeckerEmbedClient, *mocks_meta.MetadataProvider, *mocks_logstore_client.LogStoreClientPool) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentRollingPolicy: config.SegmentRollingPolicyConfig{
					MaxSize:     config.ByteSize(100000000),
					MaxInterval: config.NewDurationSecondsFromInt(800),
					MaxBlocks:   1000,
				},
			},
		},
		Minio: config.MinioConfig{
			BucketName: "test-bucket",
			RootPath:   "test-root",
		},
	}
	c := &woodpeckerEmbedClient{
		cfg:        cfg,
		Metadata:   mockMeta,
		clientPool: mockPool,
	}
	c.closeState.Store(false)
	return c, mockMeta, mockPool
}

// ============================================================
// Tests for woodpeckerEmbedClient.GetMetadataProvider
// ============================================================

func TestEmbedClient_GetMetadataProvider(t *testing.T) {
	c, mockMeta, _ := newTestEmbedClient(t)
	assert.Equal(t, mockMeta, c.GetMetadataProvider())
}

// ============================================================
// Tests for woodpeckerEmbedClient.CreateLog
// ============================================================

func TestEmbedClient_CreateLog_Success(t *testing.T) {
	c, mockMeta, _ := newTestEmbedClient(t)
	ctx := context.Background()

	mockMeta.EXPECT().CreateLog(mock.Anything, "test-log").Return(nil).Once()

	err := c.CreateLog(ctx, "test-log")
	assert.NoError(t, err)
}

func TestEmbedClient_CreateLog_Closed(t *testing.T) {
	c, _, _ := newTestEmbedClient(t)
	c.closeState.Store(true)
	ctx := context.Background()

	err := c.CreateLog(ctx, "test-log")
	assert.Error(t, err)
	assert.True(t, werr.ErrWoodpeckerClientClosed.Is(err))
}

func TestEmbedClient_CreateLog_Error(t *testing.T) {
	c, mockMeta, _ := newTestEmbedClient(t)
	ctx := context.Background()

	createErr := fmt.Errorf("create failed")
	mockMeta.EXPECT().CreateLog(mock.Anything, "test-log").Return(createErr).Once()

	err := c.CreateLog(ctx, "test-log")
	assert.Error(t, err)
}

// ============================================================
// Tests for woodpeckerEmbedClient.OpenLog
// ============================================================

func TestEmbedClient_OpenLog_Success(t *testing.T) {
	c, mockMeta, _ := newTestEmbedClient(t)
	ctx := context.Background()

	logMeta := &meta.LogMeta{
		Metadata: &proto.LogMeta{LogId: 1},
		Revision: 1,
	}
	segmentsMeta := map[int64]*meta.SegmentMeta{}
	mockMeta.EXPECT().OpenLog(mock.Anything, "test-log").Return(logMeta, segmentsMeta, nil).Once()

	handle, err := c.OpenLog(ctx, "test-log")
	require.NoError(t, err)
	assert.NotNil(t, handle)
}

func TestEmbedClient_OpenLog_Error(t *testing.T) {
	c, mockMeta, _ := newTestEmbedClient(t)
	ctx := context.Background()

	openErr := fmt.Errorf("open failed")
	mockMeta.EXPECT().OpenLog(mock.Anything, "test-log").Return(nil, nil, openErr).Once()

	handle, err := c.OpenLog(ctx, "test-log")
	assert.Error(t, err)
	assert.Nil(t, handle)
}

func TestEmbedClient_OpenLog_Closed(t *testing.T) {
	c, _, _ := newTestEmbedClient(t)
	c.closeState.Store(true)
	ctx := context.Background()

	handle, err := c.OpenLog(ctx, "test-log")
	assert.Error(t, err)
	assert.Nil(t, handle)
	assert.True(t, werr.ErrWoodpeckerClientClosed.Is(err))
}

// ============================================================
// Tests for woodpeckerEmbedClient.SelectQuorumNodes
// ============================================================

func TestEmbedClient_SelectQuorumNodes(t *testing.T) {
	c, _, _ := newTestEmbedClient(t)
	ctx := context.Background()

	result, err := c.SelectQuorumNodes(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(-1), result.Id)
	assert.Equal(t, int32(1), result.Wq)
	assert.Equal(t, int32(1), result.Aq)
	assert.Equal(t, int32(1), result.Es)
	assert.Equal(t, []string{"127.0.0.1:59456"}, result.Nodes)
}

// ============================================================
// Tests for woodpeckerEmbedClient.DeleteLog
// ============================================================

func TestEmbedClient_DeleteLog(t *testing.T) {
	c, _, _ := newTestEmbedClient(t)
	ctx := context.Background()

	err := c.DeleteLog(ctx, "test-log")
	assert.Error(t, err)
	assert.True(t, werr.ErrOperationNotSupported.Is(err))
}

// ============================================================
// Tests for woodpeckerEmbedClient.LogExists
// ============================================================

func TestEmbedClient_LogExists_True(t *testing.T) {
	c, mockMeta, _ := newTestEmbedClient(t)
	ctx := context.Background()

	mockMeta.EXPECT().CheckExists(mock.Anything, "test-log").Return(true, nil).Once()

	exists, err := c.LogExists(ctx, "test-log")
	assert.NoError(t, err)
	assert.True(t, exists)
}

func TestEmbedClient_LogExists_False(t *testing.T) {
	c, mockMeta, _ := newTestEmbedClient(t)
	ctx := context.Background()

	mockMeta.EXPECT().CheckExists(mock.Anything, "test-log").Return(false, nil).Once()

	exists, err := c.LogExists(ctx, "test-log")
	assert.NoError(t, err)
	assert.False(t, exists)
}

func TestEmbedClient_LogExists_Closed(t *testing.T) {
	c, _, _ := newTestEmbedClient(t)
	c.closeState.Store(true)
	ctx := context.Background()

	exists, err := c.LogExists(ctx, "test-log")
	assert.Error(t, err)
	assert.False(t, exists)
	assert.True(t, werr.ErrWoodpeckerClientClosed.Is(err))
}

// ============================================================
// Tests for woodpeckerEmbedClient.GetAllLogs
// ============================================================

func TestEmbedClient_GetAllLogs_Success(t *testing.T) {
	c, mockMeta, _ := newTestEmbedClient(t)
	ctx := context.Background()

	expected := []string{"log1", "log2"}
	mockMeta.EXPECT().ListLogs(mock.Anything).Return(expected, nil).Once()

	logs, err := c.GetAllLogs(ctx)
	assert.NoError(t, err)
	assert.Equal(t, expected, logs)
}

func TestEmbedClient_GetAllLogs_Closed(t *testing.T) {
	c, _, _ := newTestEmbedClient(t)
	c.closeState.Store(true)
	ctx := context.Background()

	logs, err := c.GetAllLogs(ctx)
	assert.Error(t, err)
	assert.Nil(t, logs)
	assert.True(t, werr.ErrWoodpeckerClientClosed.Is(err))
}

// ============================================================
// Tests for woodpeckerEmbedClient.GetLogsWithPrefix
// ============================================================

func TestEmbedClient_GetLogsWithPrefix_Success(t *testing.T) {
	c, mockMeta, _ := newTestEmbedClient(t)
	ctx := context.Background()

	expected := []string{"prefix-a", "prefix-b"}
	mockMeta.EXPECT().ListLogsWithPrefix(mock.Anything, "prefix-").Return(expected, nil).Once()

	logs, err := c.GetLogsWithPrefix(ctx, "prefix-")
	assert.NoError(t, err)
	assert.Equal(t, expected, logs)
}

func TestEmbedClient_GetLogsWithPrefix_Closed(t *testing.T) {
	c, _, _ := newTestEmbedClient(t)
	c.closeState.Store(true)
	ctx := context.Background()

	logs, err := c.GetLogsWithPrefix(ctx, "prefix-")
	assert.Error(t, err)
	assert.Nil(t, logs)
	assert.True(t, werr.ErrWoodpeckerClientClosed.Is(err))
}

// ============================================================
// Tests for woodpeckerEmbedClient.Close
// ============================================================

func TestEmbedClient_Close_Success(t *testing.T) {
	c, mockMeta, mockPool := newTestEmbedClient(t)
	ctx := context.Background()

	mockMeta.EXPECT().Close().Return(nil).Once()
	mockPool.EXPECT().Close(mock.Anything).Return(nil).Once()

	err := c.Close(ctx)
	assert.NoError(t, err)
	assert.True(t, c.closeState.Load())
}

func TestEmbedClient_Close_AlreadyClosed(t *testing.T) {
	c, _, _ := newTestEmbedClient(t)
	c.closeState.Store(true)
	ctx := context.Background()

	err := c.Close(ctx)
	assert.Error(t, err)
	assert.True(t, werr.ErrWoodpeckerClientClosed.Is(err))
}

func TestEmbedClient_Close_WithErrors(t *testing.T) {
	c, mockMeta, mockPool := newTestEmbedClient(t)
	ctx := context.Background()

	closeMetaErr := fmt.Errorf("meta close error")
	closePoolErr := fmt.Errorf("pool close error")
	mockMeta.EXPECT().Close().Return(closeMetaErr).Once()
	mockPool.EXPECT().Close(mock.Anything).Return(closePoolErr).Once()

	err := c.Close(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "meta close error")
	assert.Contains(t, err.Error(), "pool close error")
}

func TestEmbedClient_Close_ManagedEtcd_CloseError(t *testing.T) {
	// This test verifies the managed etcd close path by checking
	// that when managedCli is false, etcdCli.Close() is not called
	c, mockMeta, mockPool := newTestEmbedClient(t)
	c.managedCli = false
	ctx := context.Background()

	mockMeta.EXPECT().Close().Return(nil).Once()
	mockPool.EXPECT().Close(mock.Anything).Return(nil).Once()

	err := c.Close(ctx)
	assert.NoError(t, err)
}

// ============================================================
// Tests for woodpeckerEmbedClient.initClient
// ============================================================

func TestEmbedClient_InitClient_Success(t *testing.T) {
	c, mockMeta, _ := newTestEmbedClient(t)
	ctx := context.Background()

	mockMeta.EXPECT().InitIfNecessary(mock.Anything).Return(nil).Once()

	err := c.initClient(ctx)
	assert.NoError(t, err)
}

func TestEmbedClient_InitClient_Error(t *testing.T) {
	c, mockMeta, _ := newTestEmbedClient(t)
	ctx := context.Background()

	initErr := fmt.Errorf("init failed")
	mockMeta.EXPECT().InitIfNecessary(mock.Anything).Return(initErr).Once()

	err := c.initClient(ctx)
	assert.Error(t, err)
	assert.True(t, werr.ErrWoodpeckerClientInitFailed.Is(err))
}

// ============================================================
// Tests for StopEmbedLogStore
// ============================================================

func TestStopEmbedLogStore_NotRunning(t *testing.T) {
	// Save original state
	embedLogStoreMu.Lock()
	origStore := embedLogStore
	origRunning := isLogStoreRunning
	embedLogStore = nil
	isLogStoreRunning = false
	embedLogStoreMu.Unlock()

	defer func() {
		embedLogStoreMu.Lock()
		embedLogStore = origStore
		isLogStoreRunning = origRunning
		embedLogStoreMu.Unlock()
	}()

	err := StopEmbedLogStore()
	assert.NoError(t, err)
}

// ============================================================
// Tests for woodpeckerEmbedClient.Close with managed etcd
// ============================================================

func TestEmbedClient_Close_ManagedEtcdSuccess(t *testing.T) {
	c, mockMeta, mockPool := newTestEmbedClient(t)
	ctx := context.Background()

	// Create a real etcd client (lazy connection, no actual etcd needed)
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:12345"},
		DialTimeout: 1 * time.Millisecond,
	})
	require.NoError(t, err)

	c.managedCli = true
	c.etcdCli = etcdCli

	mockMeta.EXPECT().Close().Return(nil).Once()
	mockPool.EXPECT().Close(mock.Anything).Return(nil).Once()

	closeErr := c.Close(ctx)
	assert.NoError(t, closeErr)
}

func TestEmbedClient_Close_ManagedEtcdWithAllErrors(t *testing.T) {
	c, mockMeta, mockPool := newTestEmbedClient(t)
	ctx := context.Background()

	// Create a real etcd client and close it first to make Close() return an error
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:12345"},
		DialTimeout: 1 * time.Millisecond,
	})
	require.NoError(t, err)
	etcdCli.Close()

	c.managedCli = true
	c.etcdCli = etcdCli

	metaErr := fmt.Errorf("meta err")
	poolErr := fmt.Errorf("pool err")
	mockMeta.EXPECT().Close().Return(metaErr).Once()
	mockPool.EXPECT().Close(mock.Anything).Return(poolErr).Once()

	closeErr := c.Close(ctx)
	assert.Error(t, closeErr)
}

// ============================================================
// Tests for NewEmbedClientFromConfig error paths
// ============================================================

// ============================================================
// Tests for StopEmbedLogStore (running case)
// ============================================================

func TestStopEmbedLogStore_Running_Success(t *testing.T) {
	// Save original state
	embedLogStoreMu.Lock()
	origStore := embedLogStore
	origRunning := isLogStoreRunning
	embedLogStoreMu.Unlock()

	defer func() {
		embedLogStoreMu.Lock()
		embedLogStore = origStore
		isLogStoreRunning = origRunning
		embedLogStoreMu.Unlock()
	}()

	mockLogStore := mocks_server.NewLogStore(t)
	mockLogStore.EXPECT().Stop().Return(nil).Once()

	embedLogStoreMu.Lock()
	embedLogStore = mockLogStore
	isLogStoreRunning = true
	embedLogStoreMu.Unlock()

	err := StopEmbedLogStore()
	assert.NoError(t, err)

	embedLogStoreMu.Lock()
	assert.False(t, isLogStoreRunning)
	assert.Nil(t, embedLogStore)
	embedLogStoreMu.Unlock()
}

func TestStopEmbedLogStore_Running_StopError(t *testing.T) {
	// Save original state
	embedLogStoreMu.Lock()
	origStore := embedLogStore
	origRunning := isLogStoreRunning
	embedLogStoreMu.Unlock()

	defer func() {
		embedLogStoreMu.Lock()
		embedLogStore = origStore
		isLogStoreRunning = origRunning
		embedLogStoreMu.Unlock()
	}()

	mockLogStore := mocks_server.NewLogStore(t)
	stopErr := fmt.Errorf("stop error")
	mockLogStore.EXPECT().Stop().Return(stopErr).Once()

	embedLogStoreMu.Lock()
	embedLogStore = mockLogStore
	isLogStoreRunning = true
	embedLogStoreMu.Unlock()

	err := StopEmbedLogStore()
	assert.Error(t, err)
	assert.Equal(t, stopErr, err)

	// When stop fails, isLogStoreRunning should still be true
	embedLogStoreMu.Lock()
	assert.True(t, isLogStoreRunning)
	assert.NotNil(t, embedLogStore)
	embedLogStoreMu.Unlock()
}

// ============================================================
// Tests for startEmbedLogStore (already running case)
// ============================================================

func TestStartEmbedLogStore_NewStart(t *testing.T) {
	// Save original state
	embedLogStoreMu.Lock()
	origStore := embedLogStore
	origRunning := isLogStoreRunning
	embedLogStore = nil
	isLogStoreRunning = false
	embedLogStoreMu.Unlock()

	defer func() {
		// Stop the logstore we started and restore original state
		embedLogStoreMu.Lock()
		if isLogStoreRunning && embedLogStore != nil {
			embedLogStore.Stop()
		}
		embedLogStore = origStore
		isLogStoreRunning = origRunning
		embedLogStoreMu.Unlock()
	}()

	cfg := &config.Configuration{
		Trace: config.TraceConfig{Exporter: "noop"},
		Woodpecker: config.WoodpeckerConfig{
			Logstore: config.LogstoreConfig{
				ProcessorCleanupPolicy: config.ProcessorCleanupPolicyConfig{
					CleanupInterval: config.NewDurationSecondsFromInt(60),
					MaxIdleTime:     config.NewDurationSecondsFromInt(300),
					ShutdownTimeout: config.NewDurationSecondsFromInt(15),
				},
			},
		},
	}

	managed, err := startEmbedLogStore(cfg, nil)
	assert.NoError(t, err)
	assert.True(t, managed)

	embedLogStoreMu.Lock()
	assert.True(t, isLogStoreRunning)
	assert.NotNil(t, embedLogStore)
	embedLogStoreMu.Unlock()
}

func TestStartEmbedLogStore_AlreadyRunning(t *testing.T) {
	// Save original state
	embedLogStoreMu.Lock()
	origStore := embedLogStore
	origRunning := isLogStoreRunning
	embedLogStoreMu.Unlock()

	defer func() {
		embedLogStoreMu.Lock()
		embedLogStore = origStore
		isLogStoreRunning = origRunning
		embedLogStoreMu.Unlock()
	}()

	mockLogStore := mocks_server.NewLogStore(t)

	embedLogStoreMu.Lock()
	embedLogStore = mockLogStore
	isLogStoreRunning = true
	embedLogStoreMu.Unlock()

	managed, err := startEmbedLogStore(nil, nil)
	assert.NoError(t, err)
	assert.False(t, managed)
}

func TestNewEmbedClientFromConfig_StorageServiceError(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Storage: config.StorageConfig{
				Type: "service",
			},
		},
		Trace: config.TraceConfig{
			Exporter: "noop",
		},
	}

	client, err := NewEmbedClientFromConfig(ctx, cfg)
	assert.Error(t, err)
	assert.Nil(t, client)
	assert.True(t, werr.ErrOperationNotSupported.Is(err))
}

func TestNewEmbedClientFromConfig_EtcdConnectionError(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Storage: config.StorageConfig{
				Type: "minio",
			},
		},
		Trace: config.TraceConfig{
			Exporter: "noop",
		},
		Etcd: config.EtcdConfig{
			Endpoints: []string{}, // empty endpoints fail immediately
		},
	}

	client, err := NewEmbedClientFromConfig(ctx, cfg)
	assert.Error(t, err)
	assert.Nil(t, client)
	assert.True(t, werr.ErrWoodpeckerClientConnectionFailed.Is(err))
}

// ============================================================
// Tests for NewEmbedClient
// ============================================================

func TestNewEmbedClient_InitClientError(t *testing.T) {
	// Use a short-lived context so InitIfNecessary fails quickly
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:12345"},
		DialTimeout: 1 * time.Millisecond,
	})
	require.NoError(t, err)
	defer etcdCli.Close()

	// Save and restore singleton state
	embedLogStoreMu.Lock()
	origStore := embedLogStore
	origRunning := isLogStoreRunning
	embedLogStoreMu.Unlock()
	defer func() {
		embedLogStoreMu.Lock()
		embedLogStore = origStore
		isLogStoreRunning = origRunning
		embedLogStoreMu.Unlock()
	}()

	// Pre-set embedLogStore as running so startEmbedLogStore returns immediately
	mockLogStore := mocks_server.NewLogStore(t)
	embedLogStoreMu.Lock()
	embedLogStore = mockLogStore
	isLogStoreRunning = true
	embedLogStoreMu.Unlock()

	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Storage: config.StorageConfig{
				Type: "local", // not minio, skip condition write detection
			},
		},
		Trace: config.TraceConfig{Exporter: "noop"},
	}

	// NewEmbedClient should fail at initClient because etcd is unreachable
	c, err := NewEmbedClient(ctx, cfg, etcdCli, nil, false)
	assert.Error(t, err)
	assert.Nil(t, c)
	assert.True(t, werr.ErrWoodpeckerClientInitFailed.Is(err))
}

func TestNewEmbedClient_WithMinioStorage_DetectionDisabled(t *testing.T) {
	// Use a short-lived context so InitIfNecessary fails quickly
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:12345"},
		DialTimeout: 1 * time.Millisecond,
	})
	require.NoError(t, err)
	defer etcdCli.Close()

	// Save and restore singleton state
	embedLogStoreMu.Lock()
	origStore := embedLogStore
	origRunning := isLogStoreRunning
	embedLogStoreMu.Unlock()
	defer func() {
		embedLogStoreMu.Lock()
		embedLogStore = origStore
		isLogStoreRunning = origRunning
		embedLogStoreMu.Unlock()
	}()

	// Pre-set embedLogStore as running so startEmbedLogStore returns immediately
	mockLogStore := mocks_server.NewLogStore(t)
	embedLogStoreMu.Lock()
	embedLogStore = mockLogStore
	isLogStoreRunning = true
	embedLogStoreMu.Unlock()

	mockStorage := mocks_objectstorage.NewObjectStorage(t)

	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Storage: config.StorageConfig{
				Type: "minio", // triggers condition write detection
			},
			Logstore: config.LogstoreConfig{
				FencePolicy: config.FencePolicyConfig{
					ConditionWrite: "disable", // skips actual detection
				},
			},
		},
		Trace: config.TraceConfig{Exporter: "noop"},
	}

	// detectAndStoreConditionWriteCapability returns false, nil (disabled mode)
	// startEmbedLogStore returns false, nil (already running)
	// initClient will fail due to unreachable etcd
	c, err := NewEmbedClient(ctx, cfg, etcdCli, mockStorage, true)
	assert.Error(t, err)
	assert.Nil(t, c)
	assert.True(t, werr.ErrWoodpeckerClientInitFailed.Is(err))
}

func TestNewEmbedClient_SkipsConditionWriteForNonMinio(t *testing.T) {
	// Use a short-lived context so InitIfNecessary fails quickly
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:12345"},
		DialTimeout: 1 * time.Millisecond,
	})
	require.NoError(t, err)
	defer etcdCli.Close()

	// Save and restore singleton state
	embedLogStoreMu.Lock()
	origStore := embedLogStore
	origRunning := isLogStoreRunning
	embedLogStoreMu.Unlock()
	defer func() {
		embedLogStoreMu.Lock()
		embedLogStore = origStore
		isLogStoreRunning = origRunning
		embedLogStoreMu.Unlock()
	}()

	mockLogStore := mocks_server.NewLogStore(t)
	embedLogStoreMu.Lock()
	embedLogStore = mockLogStore
	isLogStoreRunning = true
	embedLogStoreMu.Unlock()

	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Storage: config.StorageConfig{
				Type: "local",
			},
		},
		Trace: config.TraceConfig{Exporter: "noop"},
	}

	// Will fail at initClient due to unreachable etcd, but exercises NewEmbedClient's code paths
	c, err := NewEmbedClient(ctx, cfg, etcdCli, nil, true)
	assert.Error(t, err)
	assert.Nil(t, c)
}
