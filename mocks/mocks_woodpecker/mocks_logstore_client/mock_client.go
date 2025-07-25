// Code generated by mockery v2.53.4. DO NOT EDIT.

package mocks_logstore_client

import (
	channel "github.com/zilliztech/woodpecker/common/channel"

	context "context"

	mock "github.com/stretchr/testify/mock"

	processor "github.com/zilliztech/woodpecker/server/processor"

	proto "github.com/zilliztech/woodpecker/proto"
)

// LogStoreClient is an autogenerated mock type for the LogStoreClient type
type LogStoreClient struct {
	mock.Mock
}

type LogStoreClient_Expecter struct {
	mock *mock.Mock
}

func (_m *LogStoreClient) EXPECT() *LogStoreClient_Expecter {
	return &LogStoreClient_Expecter{mock: &_m.Mock}
}

// AppendEntry provides a mock function with given fields: ctx, logId, entry, syncedResultCh
func (_m *LogStoreClient) AppendEntry(ctx context.Context, logId int64, entry *processor.SegmentEntry, syncedResultCh channel.ResultChannel) (int64, error) {
	ret := _m.Called(ctx, logId, entry, syncedResultCh)

	if len(ret) == 0 {
		panic("no return value specified for AppendEntry")
	}

	var r0 int64
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, int64, *processor.SegmentEntry, channel.ResultChannel) (int64, error)); ok {
		return rf(ctx, logId, entry, syncedResultCh)
	}
	if rf, ok := ret.Get(0).(func(context.Context, int64, *processor.SegmentEntry, channel.ResultChannel) int64); ok {
		r0 = rf(ctx, logId, entry, syncedResultCh)
	} else {
		r0 = ret.Get(0).(int64)
	}

	if rf, ok := ret.Get(1).(func(context.Context, int64, *processor.SegmentEntry, channel.ResultChannel) error); ok {
		r1 = rf(ctx, logId, entry, syncedResultCh)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// LogStoreClient_AppendEntry_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'AppendEntry'
type LogStoreClient_AppendEntry_Call struct {
	*mock.Call
}

// AppendEntry is a helper method to define mock.On call
//   - ctx context.Context
//   - logId int64
//   - entry *processor.SegmentEntry
//   - syncedResultCh channel.ResultChannel
func (_e *LogStoreClient_Expecter) AppendEntry(ctx interface{}, logId interface{}, entry interface{}, syncedResultCh interface{}) *LogStoreClient_AppendEntry_Call {
	return &LogStoreClient_AppendEntry_Call{Call: _e.mock.On("AppendEntry", ctx, logId, entry, syncedResultCh)}
}

func (_c *LogStoreClient_AppendEntry_Call) Run(run func(ctx context.Context, logId int64, entry *processor.SegmentEntry, syncedResultCh channel.ResultChannel)) *LogStoreClient_AppendEntry_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(int64), args[2].(*processor.SegmentEntry), args[3].(channel.ResultChannel))
	})
	return _c
}

func (_c *LogStoreClient_AppendEntry_Call) Return(_a0 int64, _a1 error) *LogStoreClient_AppendEntry_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *LogStoreClient_AppendEntry_Call) RunAndReturn(run func(context.Context, int64, *processor.SegmentEntry, channel.ResultChannel) (int64, error)) *LogStoreClient_AppendEntry_Call {
	_c.Call.Return(run)
	return _c
}

// CompleteSegment provides a mock function with given fields: ctx, logId, segmentId
func (_m *LogStoreClient) CompleteSegment(ctx context.Context, logId int64, segmentId int64) (int64, error) {
	ret := _m.Called(ctx, logId, segmentId)

	if len(ret) == 0 {
		panic("no return value specified for CompleteSegment")
	}

	var r0 int64
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, int64, int64) (int64, error)); ok {
		return rf(ctx, logId, segmentId)
	}
	if rf, ok := ret.Get(0).(func(context.Context, int64, int64) int64); ok {
		r0 = rf(ctx, logId, segmentId)
	} else {
		r0 = ret.Get(0).(int64)
	}

	if rf, ok := ret.Get(1).(func(context.Context, int64, int64) error); ok {
		r1 = rf(ctx, logId, segmentId)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// LogStoreClient_CompleteSegment_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'CompleteSegment'
type LogStoreClient_CompleteSegment_Call struct {
	*mock.Call
}

// CompleteSegment is a helper method to define mock.On call
//   - ctx context.Context
//   - logId int64
//   - segmentId int64
func (_e *LogStoreClient_Expecter) CompleteSegment(ctx interface{}, logId interface{}, segmentId interface{}) *LogStoreClient_CompleteSegment_Call {
	return &LogStoreClient_CompleteSegment_Call{Call: _e.mock.On("CompleteSegment", ctx, logId, segmentId)}
}

func (_c *LogStoreClient_CompleteSegment_Call) Run(run func(ctx context.Context, logId int64, segmentId int64)) *LogStoreClient_CompleteSegment_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(int64), args[2].(int64))
	})
	return _c
}

func (_c *LogStoreClient_CompleteSegment_Call) Return(_a0 int64, _a1 error) *LogStoreClient_CompleteSegment_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *LogStoreClient_CompleteSegment_Call) RunAndReturn(run func(context.Context, int64, int64) (int64, error)) *LogStoreClient_CompleteSegment_Call {
	_c.Call.Return(run)
	return _c
}

// FenceSegment provides a mock function with given fields: ctx, logId, segmentId
func (_m *LogStoreClient) FenceSegment(ctx context.Context, logId int64, segmentId int64) (int64, error) {
	ret := _m.Called(ctx, logId, segmentId)

	if len(ret) == 0 {
		panic("no return value specified for FenceSegment")
	}

	var r0 int64
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, int64, int64) (int64, error)); ok {
		return rf(ctx, logId, segmentId)
	}
	if rf, ok := ret.Get(0).(func(context.Context, int64, int64) int64); ok {
		r0 = rf(ctx, logId, segmentId)
	} else {
		r0 = ret.Get(0).(int64)
	}

	if rf, ok := ret.Get(1).(func(context.Context, int64, int64) error); ok {
		r1 = rf(ctx, logId, segmentId)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// LogStoreClient_FenceSegment_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'FenceSegment'
type LogStoreClient_FenceSegment_Call struct {
	*mock.Call
}

// FenceSegment is a helper method to define mock.On call
//   - ctx context.Context
//   - logId int64
//   - segmentId int64
func (_e *LogStoreClient_Expecter) FenceSegment(ctx interface{}, logId interface{}, segmentId interface{}) *LogStoreClient_FenceSegment_Call {
	return &LogStoreClient_FenceSegment_Call{Call: _e.mock.On("FenceSegment", ctx, logId, segmentId)}
}

func (_c *LogStoreClient_FenceSegment_Call) Run(run func(ctx context.Context, logId int64, segmentId int64)) *LogStoreClient_FenceSegment_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(int64), args[2].(int64))
	})
	return _c
}

func (_c *LogStoreClient_FenceSegment_Call) Return(_a0 int64, _a1 error) *LogStoreClient_FenceSegment_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *LogStoreClient_FenceSegment_Call) RunAndReturn(run func(context.Context, int64, int64) (int64, error)) *LogStoreClient_FenceSegment_Call {
	_c.Call.Return(run)
	return _c
}

// GetLastAddConfirmed provides a mock function with given fields: ctx, logId, segmentId
func (_m *LogStoreClient) GetLastAddConfirmed(ctx context.Context, logId int64, segmentId int64) (int64, error) {
	ret := _m.Called(ctx, logId, segmentId)

	if len(ret) == 0 {
		panic("no return value specified for GetLastAddConfirmed")
	}

	var r0 int64
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, int64, int64) (int64, error)); ok {
		return rf(ctx, logId, segmentId)
	}
	if rf, ok := ret.Get(0).(func(context.Context, int64, int64) int64); ok {
		r0 = rf(ctx, logId, segmentId)
	} else {
		r0 = ret.Get(0).(int64)
	}

	if rf, ok := ret.Get(1).(func(context.Context, int64, int64) error); ok {
		r1 = rf(ctx, logId, segmentId)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// LogStoreClient_GetLastAddConfirmed_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetLastAddConfirmed'
type LogStoreClient_GetLastAddConfirmed_Call struct {
	*mock.Call
}

// GetLastAddConfirmed is a helper method to define mock.On call
//   - ctx context.Context
//   - logId int64
//   - segmentId int64
func (_e *LogStoreClient_Expecter) GetLastAddConfirmed(ctx interface{}, logId interface{}, segmentId interface{}) *LogStoreClient_GetLastAddConfirmed_Call {
	return &LogStoreClient_GetLastAddConfirmed_Call{Call: _e.mock.On("GetLastAddConfirmed", ctx, logId, segmentId)}
}

func (_c *LogStoreClient_GetLastAddConfirmed_Call) Run(run func(ctx context.Context, logId int64, segmentId int64)) *LogStoreClient_GetLastAddConfirmed_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(int64), args[2].(int64))
	})
	return _c
}

func (_c *LogStoreClient_GetLastAddConfirmed_Call) Return(_a0 int64, _a1 error) *LogStoreClient_GetLastAddConfirmed_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *LogStoreClient_GetLastAddConfirmed_Call) RunAndReturn(run func(context.Context, int64, int64) (int64, error)) *LogStoreClient_GetLastAddConfirmed_Call {
	_c.Call.Return(run)
	return _c
}

// ReadEntriesBatchAdv provides a mock function with given fields: ctx, logId, segmentId, fromEntryId, maxSize, lastReadState
func (_m *LogStoreClient) ReadEntriesBatchAdv(ctx context.Context, logId int64, segmentId int64, fromEntryId int64, maxSize int64, lastReadState *processor.LastReadState) (*processor.BatchData, error) {
	ret := _m.Called(ctx, logId, segmentId, fromEntryId, maxSize, lastReadState)

	if len(ret) == 0 {
		panic("no return value specified for ReadEntriesBatchAdv")
	}

	var r0 *processor.BatchData
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, int64, int64, int64, int64, *processor.LastReadState) (*processor.BatchData, error)); ok {
		return rf(ctx, logId, segmentId, fromEntryId, maxSize, lastReadState)
	}
	if rf, ok := ret.Get(0).(func(context.Context, int64, int64, int64, int64, *processor.LastReadState) *processor.BatchData); ok {
		r0 = rf(ctx, logId, segmentId, fromEntryId, maxSize, lastReadState)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*processor.BatchData)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, int64, int64, int64, int64, *processor.LastReadState) error); ok {
		r1 = rf(ctx, logId, segmentId, fromEntryId, maxSize, lastReadState)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// LogStoreClient_ReadEntriesBatchAdv_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ReadEntriesBatchAdv'
type LogStoreClient_ReadEntriesBatchAdv_Call struct {
	*mock.Call
}

// ReadEntriesBatchAdv is a helper method to define mock.On call
//   - ctx context.Context
//   - logId int64
//   - segmentId int64
//   - fromEntryId int64
//   - maxSize int64
//   - lastReadState *processor.LastReadState
func (_e *LogStoreClient_Expecter) ReadEntriesBatchAdv(ctx interface{}, logId interface{}, segmentId interface{}, fromEntryId interface{}, maxSize interface{}, lastReadState interface{}) *LogStoreClient_ReadEntriesBatchAdv_Call {
	return &LogStoreClient_ReadEntriesBatchAdv_Call{Call: _e.mock.On("ReadEntriesBatchAdv", ctx, logId, segmentId, fromEntryId, maxSize, lastReadState)}
}

func (_c *LogStoreClient_ReadEntriesBatchAdv_Call) Run(run func(ctx context.Context, logId int64, segmentId int64, fromEntryId int64, maxSize int64, lastReadState *processor.LastReadState)) *LogStoreClient_ReadEntriesBatchAdv_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(int64), args[2].(int64), args[3].(int64), args[4].(int64), args[5].(*processor.LastReadState))
	})
	return _c
}

func (_c *LogStoreClient_ReadEntriesBatchAdv_Call) Return(_a0 *processor.BatchData, _a1 error) *LogStoreClient_ReadEntriesBatchAdv_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *LogStoreClient_ReadEntriesBatchAdv_Call) RunAndReturn(run func(context.Context, int64, int64, int64, int64, *processor.LastReadState) (*processor.BatchData, error)) *LogStoreClient_ReadEntriesBatchAdv_Call {
	_c.Call.Return(run)
	return _c
}

// SegmentClean provides a mock function with given fields: ctx, logId, segmentId, flag
func (_m *LogStoreClient) SegmentClean(ctx context.Context, logId int64, segmentId int64, flag int) error {
	ret := _m.Called(ctx, logId, segmentId, flag)

	if len(ret) == 0 {
		panic("no return value specified for SegmentClean")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, int64, int64, int) error); ok {
		r0 = rf(ctx, logId, segmentId, flag)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// LogStoreClient_SegmentClean_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SegmentClean'
type LogStoreClient_SegmentClean_Call struct {
	*mock.Call
}

// SegmentClean is a helper method to define mock.On call
//   - ctx context.Context
//   - logId int64
//   - segmentId int64
//   - flag int
func (_e *LogStoreClient_Expecter) SegmentClean(ctx interface{}, logId interface{}, segmentId interface{}, flag interface{}) *LogStoreClient_SegmentClean_Call {
	return &LogStoreClient_SegmentClean_Call{Call: _e.mock.On("SegmentClean", ctx, logId, segmentId, flag)}
}

func (_c *LogStoreClient_SegmentClean_Call) Run(run func(ctx context.Context, logId int64, segmentId int64, flag int)) *LogStoreClient_SegmentClean_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(int64), args[2].(int64), args[3].(int))
	})
	return _c
}

func (_c *LogStoreClient_SegmentClean_Call) Return(_a0 error) *LogStoreClient_SegmentClean_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *LogStoreClient_SegmentClean_Call) RunAndReturn(run func(context.Context, int64, int64, int) error) *LogStoreClient_SegmentClean_Call {
	_c.Call.Return(run)
	return _c
}

// SegmentCompact provides a mock function with given fields: ctx, logId, segmentId
func (_m *LogStoreClient) SegmentCompact(ctx context.Context, logId int64, segmentId int64) (*proto.SegmentMetadata, error) {
	ret := _m.Called(ctx, logId, segmentId)

	if len(ret) == 0 {
		panic("no return value specified for SegmentCompact")
	}

	var r0 *proto.SegmentMetadata
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, int64, int64) (*proto.SegmentMetadata, error)); ok {
		return rf(ctx, logId, segmentId)
	}
	if rf, ok := ret.Get(0).(func(context.Context, int64, int64) *proto.SegmentMetadata); ok {
		r0 = rf(ctx, logId, segmentId)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*proto.SegmentMetadata)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, int64, int64) error); ok {
		r1 = rf(ctx, logId, segmentId)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// LogStoreClient_SegmentCompact_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SegmentCompact'
type LogStoreClient_SegmentCompact_Call struct {
	*mock.Call
}

// SegmentCompact is a helper method to define mock.On call
//   - ctx context.Context
//   - logId int64
//   - segmentId int64
func (_e *LogStoreClient_Expecter) SegmentCompact(ctx interface{}, logId interface{}, segmentId interface{}) *LogStoreClient_SegmentCompact_Call {
	return &LogStoreClient_SegmentCompact_Call{Call: _e.mock.On("SegmentCompact", ctx, logId, segmentId)}
}

func (_c *LogStoreClient_SegmentCompact_Call) Run(run func(ctx context.Context, logId int64, segmentId int64)) *LogStoreClient_SegmentCompact_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(int64), args[2].(int64))
	})
	return _c
}

func (_c *LogStoreClient_SegmentCompact_Call) Return(_a0 *proto.SegmentMetadata, _a1 error) *LogStoreClient_SegmentCompact_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *LogStoreClient_SegmentCompact_Call) RunAndReturn(run func(context.Context, int64, int64) (*proto.SegmentMetadata, error)) *LogStoreClient_SegmentCompact_Call {
	_c.Call.Return(run)
	return _c
}

// NewLogStoreClient creates a new instance of LogStoreClient. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewLogStoreClient(t interface {
	mock.TestingT
	Cleanup(func())
}) *LogStoreClient {
	mock := &LogStoreClient{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
