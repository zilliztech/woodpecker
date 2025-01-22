// Code generated by mockery v2.46.0. DO NOT EDIT.

package mocks_log_handle

import (
	context "context"

	mock "github.com/stretchr/testify/mock"
	log "github.com/zilliztech/woodpecker/woodpecker/log"
)

// LogReader is an autogenerated mock type for the LogReader type
type LogReader struct {
	mock.Mock
}

type LogReader_Expecter struct {
	mock *mock.Mock
}

func (_m *LogReader) EXPECT() *LogReader_Expecter {
	return &LogReader_Expecter{mock: &_m.Mock}
}

// Close provides a mock function with given fields: _a0
func (_m *LogReader) Close(_a0 context.Context) error {
	ret := _m.Called(_a0)

	if len(ret) == 0 {
		panic("no return value specified for Close")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// LogReader_Close_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Close'
type LogReader_Close_Call struct {
	*mock.Call
}

// Close is a helper method to define mock.On call
//   - _a0 context.Context
func (_e *LogReader_Expecter) Close(_a0 interface{}) *LogReader_Close_Call {
	return &LogReader_Close_Call{Call: _e.mock.On("Close", _a0)}
}

func (_c *LogReader_Close_Call) Run(run func(_a0 context.Context)) *LogReader_Close_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context))
	})
	return _c
}

func (_c *LogReader_Close_Call) Return(_a0 error) *LogReader_Close_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *LogReader_Close_Call) RunAndReturn(run func(context.Context) error) *LogReader_Close_Call {
	_c.Call.Return(run)
	return _c
}

// ReadNext provides a mock function with given fields: _a0
func (_m *LogReader) ReadNext(_a0 context.Context) (*log.LogMessage, error) {
	ret := _m.Called(_a0)

	if len(ret) == 0 {
		panic("no return value specified for ReadNext")
	}

	var r0 *log.LogMessage
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context) (*log.LogMessage, error)); ok {
		return rf(_a0)
	}
	if rf, ok := ret.Get(0).(func(context.Context) *log.LogMessage); ok {
		r0 = rf(_a0)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*log.LogMessage)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context) error); ok {
		r1 = rf(_a0)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// LogReader_ReadNext_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ReadNext'
type LogReader_ReadNext_Call struct {
	*mock.Call
}

// ReadNext is a helper method to define mock.On call
//   - _a0 context.Context
func (_e *LogReader_Expecter) ReadNext(_a0 interface{}) *LogReader_ReadNext_Call {
	return &LogReader_ReadNext_Call{Call: _e.mock.On("ReadNext", _a0)}
}

func (_c *LogReader_ReadNext_Call) Run(run func(_a0 context.Context)) *LogReader_ReadNext_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context))
	})
	return _c
}

func (_c *LogReader_ReadNext_Call) Return(_a0 *log.LogMessage, _a1 error) *LogReader_ReadNext_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *LogReader_ReadNext_Call) RunAndReturn(run func(context.Context) (*log.LogMessage, error)) *LogReader_ReadNext_Call {
	_c.Call.Return(run)
	return _c
}

// NewLogReader creates a new instance of LogReader. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewLogReader(t interface {
	mock.TestingT
	Cleanup(func())
}) *LogReader {
	mock := &LogReader{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
