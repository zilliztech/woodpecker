package objectstorage

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/zilliztech/woodpecker/mocks/mocks_objectstorage"
)

// TestMinioFileWriter_RecoverFromStorage_TransientStatError_ReturnsError verifies
// that recoverFromStorageUnsafe returns an error (instead of panicking) when
// StatObject fails with a transient error that is NOT "object not found".
func TestMinioFileWriter_RecoverFromStorage_TransientStatError_ReturnsError(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	transientErr := errors.New("context canceled")

	mockClient.EXPECT().StatObject(
		mock.Anything, "test-bucket", "test-base/1/0/footer.blk", mock.Anything, mock.Anything,
	).Return(int64(-1), false, transientErr).Once()

	mockClient.EXPECT().IsObjectNotExistsError(transientErr).Return(false).Maybe()

	// After the fix, this must NOT panic — it should return an error.
	assert.NotPanics(t, func() {
		err := w.recoverFromStorageUnsafe(ctx)
		assert.Error(t, err, "should return error, not panic")
		assert.Contains(t, err.Error(), "context canceled")
	}, "recoverFromStorageUnsafe should not panic on transient StatObject errors")
}

// TestMinioFileWriter_RecoverFromStorage_ContextCanceled_ReturnsError verifies
// the fix for context.Canceled errors from StatObject.
func TestMinioFileWriter_RecoverFromStorage_ContextCanceled_ReturnsError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	ctxCanceledErr := context.Canceled

	mockClient.EXPECT().StatObject(
		mock.Anything, "test-bucket", "test-base/1/0/footer.blk", mock.Anything, mock.Anything,
	).Return(int64(-1), false, ctxCanceledErr).Once()

	mockClient.EXPECT().IsObjectNotExistsError(ctxCanceledErr).Return(false).Maybe()

	assert.NotPanics(t, func() {
		err := w.recoverFromStorageUnsafe(ctx)
		assert.Error(t, err, "should return error for context.Canceled")
		assert.Contains(t, err.Error(), "context canceled")
	}, "recoverFromStorageUnsafe should not panic on context.Canceled")
}

// TestMinioFileWriter_RecoverFromStorage_NetworkTimeout_ReturnsError verifies
// the fix for i/o timeout errors from StatObject.
func TestMinioFileWriter_RecoverFromStorage_NetworkTimeout_ReturnsError(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	timeoutErr := errors.New("i/o timeout")

	mockClient.EXPECT().StatObject(
		mock.Anything, "test-bucket", "test-base/1/0/footer.blk", mock.Anything, mock.Anything,
	).Return(int64(-1), false, timeoutErr).Once()

	mockClient.EXPECT().IsObjectNotExistsError(timeoutErr).Return(false).Maybe()

	assert.NotPanics(t, func() {
		err := w.recoverFromStorageUnsafe(ctx)
		assert.Error(t, err, "should return error for i/o timeout")
		assert.Contains(t, err.Error(), "i/o timeout")
	}, "recoverFromStorageUnsafe should not panic on i/o timeout")
}
