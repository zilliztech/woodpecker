// Code generated by mockery v2.53.4. DO NOT EDIT.

package mocks_server

import (
	channel "github.com/zilliztech/woodpecker/common/channel"
	clientv3 "go.etcd.io/etcd/client/v3"

	context "context"

	mock "github.com/stretchr/testify/mock"

	processor "github.com/zilliztech/woodpecker/server/processor"

	proto "github.com/zilliztech/woodpecker/proto"
)

// LogStore is an autogenerated mock type for the LogStore type
type LogStore struct {
	mock.Mock
}

type LogStore_Expecter struct {
	mock *mock.Mock
}

func (_m *LogStore) EXPECT() *LogStore_Expecter {
	return &LogStore_Expecter{mock: &_m.Mock}
}

// AddEntry provides a mock function with given fields: _a0, _a1, _a2, _a3
func (_m *LogStore) AddEntry(_a0 context.Context, _a1 int64, _a2 *processor.SegmentEntry, _a3 channel.ResultChannel) (int64, error) {
	ret := _m.Called(_a0, _a1, _a2, _a3)

	if len(ret) == 0 {
		panic("no return value specified for AddEntry")
	}

	var r0 int64
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, int64, *processor.SegmentEntry, channel.ResultChannel) (int64, error)); ok {
		return rf(_a0, _a1, _a2, _a3)
	}
	if rf, ok := ret.Get(0).(func(context.Context, int64, *processor.SegmentEntry, channel.ResultChannel) int64); ok {
		r0 = rf(_a0, _a1, _a2, _a3)
	} else {
		r0 = ret.Get(0).(int64)
	}

	if rf, ok := ret.Get(1).(func(context.Context, int64, *processor.SegmentEntry, channel.ResultChannel) error); ok {
		r1 = rf(_a0, _a1, _a2, _a3)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// LogStore_AddEntry_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'AddEntry'
type LogStore_AddEntry_Call struct {
	*mock.Call
}

// AddEntry is a helper method to define mock.On call
//   - _a0 context.Context
//   - _a1 int64
//   - _a2 *processor.SegmentEntry
//   - _a3 channel.ResultChannel
func (_e *LogStore_Expecter) AddEntry(_a0 interface{}, _a1 interface{}, _a2 interface{}, _a3 interface{}) *LogStore_AddEntry_Call {
	return &LogStore_AddEntry_Call{Call: _e.mock.On("AddEntry", _a0, _a1, _a2, _a3)}
}

func (_c *LogStore_AddEntry_Call) Run(run func(_a0 context.Context, _a1 int64, _a2 *processor.SegmentEntry, _a3 channel.ResultChannel)) *LogStore_AddEntry_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(int64), args[2].(*processor.SegmentEntry), args[3].(channel.ResultChannel))
	})
	return _c
}

func (_c *LogStore_AddEntry_Call) Return(_a0 int64, _a1 error) *LogStore_AddEntry_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *LogStore_AddEntry_Call) RunAndReturn(run func(context.Context, int64, *processor.SegmentEntry, channel.ResultChannel) (int64, error)) *LogStore_AddEntry_Call {
	_c.Call.Return(run)
	return _c
}

// CleanSegment provides a mock function with given fields: _a0, _a1, _a2, _a3
func (_m *LogStore) CleanSegment(_a0 context.Context, _a1 int64, _a2 int64, _a3 int) error {
	ret := _m.Called(_a0, _a1, _a2, _a3)

	if len(ret) == 0 {
		panic("no return value specified for CleanSegment")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, int64, int64, int) error); ok {
		r0 = rf(_a0, _a1, _a2, _a3)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// LogStore_CleanSegment_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'CleanSegment'
type LogStore_CleanSegment_Call struct {
	*mock.Call
}

// CleanSegment is a helper method to define mock.On call
//   - _a0 context.Context
//   - _a1 int64
//   - _a2 int64
//   - _a3 int
func (_e *LogStore_Expecter) CleanSegment(_a0 interface{}, _a1 interface{}, _a2 interface{}, _a3 interface{}) *LogStore_CleanSegment_Call {
	return &LogStore_CleanSegment_Call{Call: _e.mock.On("CleanSegment", _a0, _a1, _a2, _a3)}
}

func (_c *LogStore_CleanSegment_Call) Run(run func(_a0 context.Context, _a1 int64, _a2 int64, _a3 int)) *LogStore_CleanSegment_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(int64), args[2].(int64), args[3].(int))
	})
	return _c
}

func (_c *LogStore_CleanSegment_Call) Return(_a0 error) *LogStore_CleanSegment_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *LogStore_CleanSegment_Call) RunAndReturn(run func(context.Context, int64, int64, int) error) *LogStore_CleanSegment_Call {
	_c.Call.Return(run)
	return _c
}

// CompactSegment provides a mock function with given fields: _a0, _a1, _a2
func (_m *LogStore) CompactSegment(_a0 context.Context, _a1 int64, _a2 int64) (*proto.SegmentMetadata, error) {
	ret := _m.Called(_a0, _a1, _a2)

	if len(ret) == 0 {
		panic("no return value specified for CompactSegment")
	}

	var r0 *proto.SegmentMetadata
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, int64, int64) (*proto.SegmentMetadata, error)); ok {
		return rf(_a0, _a1, _a2)
	}
	if rf, ok := ret.Get(0).(func(context.Context, int64, int64) *proto.SegmentMetadata); ok {
		r0 = rf(_a0, _a1, _a2)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*proto.SegmentMetadata)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, int64, int64) error); ok {
		r1 = rf(_a0, _a1, _a2)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// LogStore_CompactSegment_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'CompactSegment'
type LogStore_CompactSegment_Call struct {
	*mock.Call
}

// CompactSegment is a helper method to define mock.On call
//   - _a0 context.Context
//   - _a1 int64
//   - _a2 int64
func (_e *LogStore_Expecter) CompactSegment(_a0 interface{}, _a1 interface{}, _a2 interface{}) *LogStore_CompactSegment_Call {
	return &LogStore_CompactSegment_Call{Call: _e.mock.On("CompactSegment", _a0, _a1, _a2)}
}

func (_c *LogStore_CompactSegment_Call) Run(run func(_a0 context.Context, _a1 int64, _a2 int64)) *LogStore_CompactSegment_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(int64), args[2].(int64))
	})
	return _c
}

func (_c *LogStore_CompactSegment_Call) Return(_a0 *proto.SegmentMetadata, _a1 error) *LogStore_CompactSegment_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *LogStore_CompactSegment_Call) RunAndReturn(run func(context.Context, int64, int64) (*proto.SegmentMetadata, error)) *LogStore_CompactSegment_Call {
	_c.Call.Return(run)
	return _c
}

// CompleteSegment provides a mock function with given fields: _a0, _a1, _a2
func (_m *LogStore) CompleteSegment(_a0 context.Context, _a1 int64, _a2 int64) (int64, error) {
	ret := _m.Called(_a0, _a1, _a2)

	if len(ret) == 0 {
		panic("no return value specified for CompleteSegment")
	}

	var r0 int64
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, int64, int64) (int64, error)); ok {
		return rf(_a0, _a1, _a2)
	}
	if rf, ok := ret.Get(0).(func(context.Context, int64, int64) int64); ok {
		r0 = rf(_a0, _a1, _a2)
	} else {
		r0 = ret.Get(0).(int64)
	}

	if rf, ok := ret.Get(1).(func(context.Context, int64, int64) error); ok {
		r1 = rf(_a0, _a1, _a2)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// LogStore_CompleteSegment_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'CompleteSegment'
type LogStore_CompleteSegment_Call struct {
	*mock.Call
}

// CompleteSegment is a helper method to define mock.On call
//   - _a0 context.Context
//   - _a1 int64
//   - _a2 int64
func (_e *LogStore_Expecter) CompleteSegment(_a0 interface{}, _a1 interface{}, _a2 interface{}) *LogStore_CompleteSegment_Call {
	return &LogStore_CompleteSegment_Call{Call: _e.mock.On("CompleteSegment", _a0, _a1, _a2)}
}

func (_c *LogStore_CompleteSegment_Call) Run(run func(_a0 context.Context, _a1 int64, _a2 int64)) *LogStore_CompleteSegment_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(int64), args[2].(int64))
	})
	return _c
}

func (_c *LogStore_CompleteSegment_Call) Return(_a0 int64, _a1 error) *LogStore_CompleteSegment_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *LogStore_CompleteSegment_Call) RunAndReturn(run func(context.Context, int64, int64) (int64, error)) *LogStore_CompleteSegment_Call {
	_c.Call.Return(run)
	return _c
}

// FenceSegment provides a mock function with given fields: _a0, _a1, _a2
func (_m *LogStore) FenceSegment(_a0 context.Context, _a1 int64, _a2 int64) (int64, error) {
	ret := _m.Called(_a0, _a1, _a2)

	if len(ret) == 0 {
		panic("no return value specified for FenceSegment")
	}

	var r0 int64
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, int64, int64) (int64, error)); ok {
		return rf(_a0, _a1, _a2)
	}
	if rf, ok := ret.Get(0).(func(context.Context, int64, int64) int64); ok {
		r0 = rf(_a0, _a1, _a2)
	} else {
		r0 = ret.Get(0).(int64)
	}

	if rf, ok := ret.Get(1).(func(context.Context, int64, int64) error); ok {
		r1 = rf(_a0, _a1, _a2)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// LogStore_FenceSegment_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'FenceSegment'
type LogStore_FenceSegment_Call struct {
	*mock.Call
}

// FenceSegment is a helper method to define mock.On call
//   - _a0 context.Context
//   - _a1 int64
//   - _a2 int64
func (_e *LogStore_Expecter) FenceSegment(_a0 interface{}, _a1 interface{}, _a2 interface{}) *LogStore_FenceSegment_Call {
	return &LogStore_FenceSegment_Call{Call: _e.mock.On("FenceSegment", _a0, _a1, _a2)}
}

func (_c *LogStore_FenceSegment_Call) Run(run func(_a0 context.Context, _a1 int64, _a2 int64)) *LogStore_FenceSegment_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(int64), args[2].(int64))
	})
	return _c
}

func (_c *LogStore_FenceSegment_Call) Return(_a0 int64, _a1 error) *LogStore_FenceSegment_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *LogStore_FenceSegment_Call) RunAndReturn(run func(context.Context, int64, int64) (int64, error)) *LogStore_FenceSegment_Call {
	_c.Call.Return(run)
	return _c
}

// GetAddress provides a mock function with no fields
func (_m *LogStore) GetAddress() string {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetAddress")
	}

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// LogStore_GetAddress_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetAddress'
type LogStore_GetAddress_Call struct {
	*mock.Call
}

// GetAddress is a helper method to define mock.On call
func (_e *LogStore_Expecter) GetAddress() *LogStore_GetAddress_Call {
	return &LogStore_GetAddress_Call{Call: _e.mock.On("GetAddress")}
}

func (_c *LogStore_GetAddress_Call) Run(run func()) *LogStore_GetAddress_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *LogStore_GetAddress_Call) Return(_a0 string) *LogStore_GetAddress_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *LogStore_GetAddress_Call) RunAndReturn(run func() string) *LogStore_GetAddress_Call {
	_c.Call.Return(run)
	return _c
}

// GetBatchEntriesAdv provides a mock function with given fields: _a0, _a1, _a2, _a3, _a4, _a5
func (_m *LogStore) GetBatchEntriesAdv(_a0 context.Context, _a1 int64, _a2 int64, _a3 int64, _a4 int64, _a5 *processor.LastReadState) (*processor.BatchData, error) {
	ret := _m.Called(_a0, _a1, _a2, _a3, _a4, _a5)

	if len(ret) == 0 {
		panic("no return value specified for GetBatchEntriesAdv")
	}

	var r0 *processor.BatchData
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, int64, int64, int64, int64, *processor.LastReadState) (*processor.BatchData, error)); ok {
		return rf(_a0, _a1, _a2, _a3, _a4, _a5)
	}
	if rf, ok := ret.Get(0).(func(context.Context, int64, int64, int64, int64, *processor.LastReadState) *processor.BatchData); ok {
		r0 = rf(_a0, _a1, _a2, _a3, _a4, _a5)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*processor.BatchData)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, int64, int64, int64, int64, *processor.LastReadState) error); ok {
		r1 = rf(_a0, _a1, _a2, _a3, _a4, _a5)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// LogStore_GetBatchEntriesAdv_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetBatchEntriesAdv'
type LogStore_GetBatchEntriesAdv_Call struct {
	*mock.Call
}

// GetBatchEntriesAdv is a helper method to define mock.On call
//   - _a0 context.Context
//   - _a1 int64
//   - _a2 int64
//   - _a3 int64
//   - _a4 int64
//   - _a5 *processor.LastReadState
func (_e *LogStore_Expecter) GetBatchEntriesAdv(_a0 interface{}, _a1 interface{}, _a2 interface{}, _a3 interface{}, _a4 interface{}, _a5 interface{}) *LogStore_GetBatchEntriesAdv_Call {
	return &LogStore_GetBatchEntriesAdv_Call{Call: _e.mock.On("GetBatchEntriesAdv", _a0, _a1, _a2, _a3, _a4, _a5)}
}

func (_c *LogStore_GetBatchEntriesAdv_Call) Run(run func(_a0 context.Context, _a1 int64, _a2 int64, _a3 int64, _a4 int64, _a5 *processor.LastReadState)) *LogStore_GetBatchEntriesAdv_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(int64), args[2].(int64), args[3].(int64), args[4].(int64), args[5].(*processor.LastReadState))
	})
	return _c
}

func (_c *LogStore_GetBatchEntriesAdv_Call) Return(_a0 *processor.BatchData, _a1 error) *LogStore_GetBatchEntriesAdv_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *LogStore_GetBatchEntriesAdv_Call) RunAndReturn(run func(context.Context, int64, int64, int64, int64, *processor.LastReadState) (*processor.BatchData, error)) *LogStore_GetBatchEntriesAdv_Call {
	_c.Call.Return(run)
	return _c
}

// GetSegmentLastAddConfirmed provides a mock function with given fields: _a0, _a1, _a2
func (_m *LogStore) GetSegmentLastAddConfirmed(_a0 context.Context, _a1 int64, _a2 int64) (int64, error) {
	ret := _m.Called(_a0, _a1, _a2)

	if len(ret) == 0 {
		panic("no return value specified for GetSegmentLastAddConfirmed")
	}

	var r0 int64
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, int64, int64) (int64, error)); ok {
		return rf(_a0, _a1, _a2)
	}
	if rf, ok := ret.Get(0).(func(context.Context, int64, int64) int64); ok {
		r0 = rf(_a0, _a1, _a2)
	} else {
		r0 = ret.Get(0).(int64)
	}

	if rf, ok := ret.Get(1).(func(context.Context, int64, int64) error); ok {
		r1 = rf(_a0, _a1, _a2)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// LogStore_GetSegmentLastAddConfirmed_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetSegmentLastAddConfirmed'
type LogStore_GetSegmentLastAddConfirmed_Call struct {
	*mock.Call
}

// GetSegmentLastAddConfirmed is a helper method to define mock.On call
//   - _a0 context.Context
//   - _a1 int64
//   - _a2 int64
func (_e *LogStore_Expecter) GetSegmentLastAddConfirmed(_a0 interface{}, _a1 interface{}, _a2 interface{}) *LogStore_GetSegmentLastAddConfirmed_Call {
	return &LogStore_GetSegmentLastAddConfirmed_Call{Call: _e.mock.On("GetSegmentLastAddConfirmed", _a0, _a1, _a2)}
}

func (_c *LogStore_GetSegmentLastAddConfirmed_Call) Run(run func(_a0 context.Context, _a1 int64, _a2 int64)) *LogStore_GetSegmentLastAddConfirmed_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(int64), args[2].(int64))
	})
	return _c
}

func (_c *LogStore_GetSegmentLastAddConfirmed_Call) Return(_a0 int64, _a1 error) *LogStore_GetSegmentLastAddConfirmed_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *LogStore_GetSegmentLastAddConfirmed_Call) RunAndReturn(run func(context.Context, int64, int64) (int64, error)) *LogStore_GetSegmentLastAddConfirmed_Call {
	_c.Call.Return(run)
	return _c
}

// Register provides a mock function with given fields: _a0
func (_m *LogStore) Register(_a0 context.Context) error {
	ret := _m.Called(_a0)

	if len(ret) == 0 {
		panic("no return value specified for Register")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// LogStore_Register_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Register'
type LogStore_Register_Call struct {
	*mock.Call
}

// Register is a helper method to define mock.On call
//   - _a0 context.Context
func (_e *LogStore_Expecter) Register(_a0 interface{}) *LogStore_Register_Call {
	return &LogStore_Register_Call{Call: _e.mock.On("Register", _a0)}
}

func (_c *LogStore_Register_Call) Run(run func(_a0 context.Context)) *LogStore_Register_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context))
	})
	return _c
}

func (_c *LogStore_Register_Call) Return(_a0 error) *LogStore_Register_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *LogStore_Register_Call) RunAndReturn(run func(context.Context) error) *LogStore_Register_Call {
	_c.Call.Return(run)
	return _c
}

// SetAddress provides a mock function with given fields: _a0
func (_m *LogStore) SetAddress(_a0 string) {
	_m.Called(_a0)
}

// LogStore_SetAddress_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SetAddress'
type LogStore_SetAddress_Call struct {
	*mock.Call
}

// SetAddress is a helper method to define mock.On call
//   - _a0 string
func (_e *LogStore_Expecter) SetAddress(_a0 interface{}) *LogStore_SetAddress_Call {
	return &LogStore_SetAddress_Call{Call: _e.mock.On("SetAddress", _a0)}
}

func (_c *LogStore_SetAddress_Call) Run(run func(_a0 string)) *LogStore_SetAddress_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *LogStore_SetAddress_Call) Return() *LogStore_SetAddress_Call {
	_c.Call.Return()
	return _c
}

func (_c *LogStore_SetAddress_Call) RunAndReturn(run func(string)) *LogStore_SetAddress_Call {
	_c.Run(run)
	return _c
}

// SetEtcdClient provides a mock function with given fields: _a0
func (_m *LogStore) SetEtcdClient(_a0 *clientv3.Client) {
	_m.Called(_a0)
}

// LogStore_SetEtcdClient_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SetEtcdClient'
type LogStore_SetEtcdClient_Call struct {
	*mock.Call
}

// SetEtcdClient is a helper method to define mock.On call
//   - _a0 *clientv3.Client
func (_e *LogStore_Expecter) SetEtcdClient(_a0 interface{}) *LogStore_SetEtcdClient_Call {
	return &LogStore_SetEtcdClient_Call{Call: _e.mock.On("SetEtcdClient", _a0)}
}

func (_c *LogStore_SetEtcdClient_Call) Run(run func(_a0 *clientv3.Client)) *LogStore_SetEtcdClient_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*clientv3.Client))
	})
	return _c
}

func (_c *LogStore_SetEtcdClient_Call) Return() *LogStore_SetEtcdClient_Call {
	_c.Call.Return()
	return _c
}

func (_c *LogStore_SetEtcdClient_Call) RunAndReturn(run func(*clientv3.Client)) *LogStore_SetEtcdClient_Call {
	_c.Run(run)
	return _c
}

// Start provides a mock function with no fields
func (_m *LogStore) Start() error {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Start")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// LogStore_Start_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Start'
type LogStore_Start_Call struct {
	*mock.Call
}

// Start is a helper method to define mock.On call
func (_e *LogStore_Expecter) Start() *LogStore_Start_Call {
	return &LogStore_Start_Call{Call: _e.mock.On("Start")}
}

func (_c *LogStore_Start_Call) Run(run func()) *LogStore_Start_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *LogStore_Start_Call) Return(_a0 error) *LogStore_Start_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *LogStore_Start_Call) RunAndReturn(run func() error) *LogStore_Start_Call {
	_c.Call.Return(run)
	return _c
}

// Stop provides a mock function with no fields
func (_m *LogStore) Stop() error {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Stop")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// LogStore_Stop_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Stop'
type LogStore_Stop_Call struct {
	*mock.Call
}

// Stop is a helper method to define mock.On call
func (_e *LogStore_Expecter) Stop() *LogStore_Stop_Call {
	return &LogStore_Stop_Call{Call: _e.mock.On("Stop")}
}

func (_c *LogStore_Stop_Call) Run(run func()) *LogStore_Stop_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *LogStore_Stop_Call) Return(_a0 error) *LogStore_Stop_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *LogStore_Stop_Call) RunAndReturn(run func() error) *LogStore_Stop_Call {
	_c.Call.Return(run)
	return _c
}

// NewLogStore creates a new instance of LogStore. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewLogStore(t interface {
	mock.TestingT
	Cleanup(func())
}) *LogStore {
	mock := &LogStore{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
