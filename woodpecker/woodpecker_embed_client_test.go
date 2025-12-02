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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/mocks/mocks_meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_objectstorage"
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
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(nil).Once()
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(werr.ErrObjectAlreadyExists).Once()
	mockStorage.EXPECT().PutFencedObject(mock.Anything, "test-bucket", mock.AnythingOfType("string")).Return(nil).Times(2)
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(werr.ErrSegmentFenced).Once()
	mockStorage.EXPECT().RemoveObject(mock.Anything, "test-bucket", mock.AnythingOfType("string")).Return(nil).Times(2)

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
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(detectErr).Maybe()
	mockStorage.EXPECT().RemoveObject(mock.Anything, "test-bucket", mock.AnythingOfType("string")).Return(nil).Maybe()

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
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(detectErr).Maybe()
	mockStorage.EXPECT().RemoveObject(mock.Anything, "test-bucket", mock.AnythingOfType("string")).Return(nil).Maybe()

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
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(detectErr).Once()
	mockStorage.EXPECT().RemoveObject(mock.Anything, "test-bucket", mock.AnythingOfType("string")).Return(nil).Maybe()

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
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(nil).Once()
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(werr.ErrObjectAlreadyExists).Once()
	mockStorage.EXPECT().PutFencedObject(mock.Anything, "test-bucket", mock.AnythingOfType("string")).Return(nil).Times(2)
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(werr.ErrSegmentFenced).Once()
	mockStorage.EXPECT().RemoveObject(mock.Anything, "test-bucket", mock.AnythingOfType("string")).Return(nil).Times(2)

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
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(fmt.Errorf("not supported")).Maybe()
	mockStorage.EXPECT().RemoveObject(mock.Anything, "test-bucket", mock.AnythingOfType("string")).Return(nil).Maybe()

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
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(nil).Once()
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(werr.ErrObjectAlreadyExists).Once()
	mockStorage.EXPECT().PutFencedObject(mock.Anything, "test-bucket", mock.AnythingOfType("string")).Return(nil).Times(2)
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(werr.ErrSegmentFenced).Once()
	mockStorage.EXPECT().RemoveObject(mock.Anything, "test-bucket", mock.AnythingOfType("string")).Return(nil).Times(2)

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
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(nil).Once()
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(werr.ErrObjectAlreadyExists).Once()
	mockStorage.EXPECT().PutFencedObject(mock.Anything, "test-bucket", mock.AnythingOfType("string")).Return(nil).Times(2)
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(werr.ErrSegmentFenced).Once()
	mockStorage.EXPECT().RemoveObject(mock.Anything, "test-bucket", mock.AnythingOfType("string")).Return(nil).Times(2)

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
	mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(detectErr).Maybe()
	mockStorage.EXPECT().RemoveObject(mock.Anything, "test-bucket", mock.AnythingOfType("string")).Return(nil).Maybe()

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
				mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(detectErr).Maybe()
				mockStorage.EXPECT().RemoveObject(mock.Anything, "test-bucket", mock.AnythingOfType("string")).Return(nil).Maybe()

				// Store false result and get agreed result
				mockMetadata.EXPECT().StoreOrGetConditionWriteResult(mock.Anything, false).Return(tc.agreedResult, nil).Once()
			} else {
				// Detection succeeds
				mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(nil).Once()
				mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(werr.ErrObjectAlreadyExists).Once()
				mockStorage.EXPECT().PutFencedObject(mock.Anything, "test-bucket", mock.AnythingOfType("string")).Return(nil).Times(2)
				mockStorage.EXPECT().PutObjectIfNoneMatch(mock.Anything, "test-bucket", mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(werr.ErrSegmentFenced).Once()
				mockStorage.EXPECT().RemoveObject(mock.Anything, "test-bucket", mock.AnythingOfType("string")).Return(nil).Times(2)

				// Store detection result and get agreed result
				mockMetadata.EXPECT().StoreOrGetConditionWriteResult(mock.Anything, tc.localDetection).Return(tc.agreedResult, nil).Once()
			}

			result, err := detectAndStoreConditionWriteCapability(ctx, cfg, mockMetadata, mockStorage)

			assert.NoError(t, err, "Should not return error when etcd operations succeed")
			assert.Equal(t, tc.agreedResult, result, "Should return the agreed result from etcd")
		})
	}
}
