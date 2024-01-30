// Code generated by MockGen. DO NOT EDIT.
// Source: ./blockindex/bloomfilterindexer.go

// Package mock_blockindex is a generated GoMock package.
package mock_blockindex

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	bloom "github.com/iotexproject/go-pkgs/bloom"
	logfilter "github.com/iotexproject/iotex-core/api/logfilter"
	block "github.com/iotexproject/iotex-core/blockchain/block"
)

// MockBloomFilterIndexer is a mock of BloomFilterIndexer interface.
type MockBloomFilterIndexer struct {
	ctrl     *gomock.Controller
	recorder *MockBloomFilterIndexerMockRecorder
}

// MockBloomFilterIndexerMockRecorder is the mock recorder for MockBloomFilterIndexer.
type MockBloomFilterIndexerMockRecorder struct {
	mock *MockBloomFilterIndexer
}

// NewMockBloomFilterIndexer creates a new mock instance.
func NewMockBloomFilterIndexer(ctrl *gomock.Controller) *MockBloomFilterIndexer {
	mock := &MockBloomFilterIndexer{ctrl: ctrl}
	mock.recorder = &MockBloomFilterIndexerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockBloomFilterIndexer) EXPECT() *MockBloomFilterIndexerMockRecorder {
	return m.recorder
}

// BlockFilterByHeight mocks base method.
func (m *MockBloomFilterIndexer) BlockFilterByHeight(arg0 uint64) (bloom.BloomFilter, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "BlockFilterByHeight", arg0)
	ret0, _ := ret[0].(bloom.BloomFilter)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// BlockFilterByHeight indicates an expected call of BlockFilterByHeight.
func (mr *MockBloomFilterIndexerMockRecorder) BlockFilterByHeight(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "BlockFilterByHeight", reflect.TypeOf((*MockBloomFilterIndexer)(nil).BlockFilterByHeight), arg0)
}

// DeleteTipBlock mocks base method.
func (m *MockBloomFilterIndexer) DeleteTipBlock(arg0 context.Context, arg1 *block.Block) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteTipBlock", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// DeleteTipBlock indicates an expected call of DeleteTipBlock.
func (mr *MockBloomFilterIndexerMockRecorder) DeleteTipBlock(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteTipBlock", reflect.TypeOf((*MockBloomFilterIndexer)(nil).DeleteTipBlock), arg0, arg1)
}

// FilterBlocksInRange mocks base method.
func (m *MockBloomFilterIndexer) FilterBlocksInRange(arg0 *logfilter.LogFilter, arg1, arg2, arg3 uint64) ([]uint64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "FilterBlocksInRange", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].([]uint64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// FilterBlocksInRange indicates an expected call of FilterBlocksInRange.
func (mr *MockBloomFilterIndexerMockRecorder) FilterBlocksInRange(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "FilterBlocksInRange", reflect.TypeOf((*MockBloomFilterIndexer)(nil).FilterBlocksInRange), arg0, arg1, arg2, arg3)
}

// Height mocks base method.
func (m *MockBloomFilterIndexer) Height() (uint64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Height")
	ret0, _ := ret[0].(uint64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Height indicates an expected call of Height.
func (mr *MockBloomFilterIndexerMockRecorder) Height() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Height", reflect.TypeOf((*MockBloomFilterIndexer)(nil).Height))
}

// Name mocks base method.
func (m *MockBloomFilterIndexer) Name() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Name")
	ret0, _ := ret[0].(string)
	return ret0
}

// Name indicates an expected call of Name.
func (mr *MockBloomFilterIndexerMockRecorder) Name() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Name", reflect.TypeOf((*MockBloomFilterIndexer)(nil).Name))
}

// PutBlock mocks base method.
func (m *MockBloomFilterIndexer) PutBlock(arg0 context.Context, arg1 *block.Block) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PutBlock", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// PutBlock indicates an expected call of PutBlock.
func (mr *MockBloomFilterIndexerMockRecorder) PutBlock(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PutBlock", reflect.TypeOf((*MockBloomFilterIndexer)(nil).PutBlock), arg0, arg1)
}

// RangeBloomFilterNumElements mocks base method.
func (m *MockBloomFilterIndexer) RangeBloomFilterNumElements() uint64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RangeBloomFilterNumElements")
	ret0, _ := ret[0].(uint64)
	return ret0
}

// RangeBloomFilterNumElements indicates an expected call of RangeBloomFilterNumElements.
func (mr *MockBloomFilterIndexerMockRecorder) RangeBloomFilterNumElements() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RangeBloomFilterNumElements", reflect.TypeOf((*MockBloomFilterIndexer)(nil).RangeBloomFilterNumElements))
}

// Start mocks base method.
func (m *MockBloomFilterIndexer) Start(ctx context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Start", ctx)
	ret0, _ := ret[0].(error)
	return ret0
}

// Start indicates an expected call of Start.
func (mr *MockBloomFilterIndexerMockRecorder) Start(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Start", reflect.TypeOf((*MockBloomFilterIndexer)(nil).Start), ctx)
}

// Stop mocks base method.
func (m *MockBloomFilterIndexer) Stop(ctx context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Stop", ctx)
	ret0, _ := ret[0].(error)
	return ret0
}

// Stop indicates an expected call of Stop.
func (mr *MockBloomFilterIndexerMockRecorder) Stop(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockBloomFilterIndexer)(nil).Stop), ctx)
}
