package err

import "github.com/cockroachdb/errors"

var (
	ErrInvalidEntryId = newWoodpeckerError("Invalid EntryId", 1, false)
	ErrBufferIsEmpty  = newWoodpeckerError("Buffer is empty", 2, false)
	ErrEntryNotFound  = newWoodpeckerError("Entry is not found", 3, false)
)

// woodpeckerError is a custom error type that provides richer error information.
type woodpeckerError struct {
	msg       string // msg stores the detailed error message, describing the specific situation of the error.
	errCode   int32  // errCode is an integer error code used to identify specific types of errors.
	retryable bool   // retryable indicates whether the error can be retried. Certain operations can decide whether to retry based on this flag when encountering an error.
}

func newWoodpeckerError(msg string, code int32, retryable bool) woodpeckerError {
	err := woodpeckerError{
		msg:       msg,
		errCode:   code,
		retryable: retryable,
	}
	return err
}

func (e woodpeckerError) Code() int32 {
	return e.errCode
}

func (e woodpeckerError) Error() string {
	return e.msg
}

func (e woodpeckerError) IsRetryable() bool {
	return e.retryable
}

func (e woodpeckerError) Is(err error) bool {
	cause := errors.Cause(err)
	if cause, ok := cause.(woodpeckerError); ok {
		return e.errCode == cause.errCode
	}
	return false
}

func IsRetryableErr(err error) bool {
	if err, ok := err.(woodpeckerError); ok {
		return err.retryable
	}
	return false
}
