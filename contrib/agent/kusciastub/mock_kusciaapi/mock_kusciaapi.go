// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/secretflow/kuscia/proto/api/v1alpha1/kusciaapi (interfaces: ServingServiceClient,DomainDataServiceClient)
//
// Generated by this command:
//
//	mockgen github.com/secretflow/kuscia/proto/api/v1alpha1/kusciaapi ServingServiceClient,DomainDataServiceClient
//

// Package mock_kusciaapi is a generated GoMock package.
package mock_kusciaapi

import (
	context "context"
	reflect "reflect"

	kusciaapi "github.com/secretflow/kuscia/proto/api/v1alpha1/kusciaapi"
	gomock "go.uber.org/mock/gomock"
	grpc "google.golang.org/grpc"
)

// MockServingServiceClient is a mock of ServingServiceClient interface.
type MockServingServiceClient struct {
	ctrl     *gomock.Controller
	recorder *MockServingServiceClientMockRecorder
}

// MockServingServiceClientMockRecorder is the mock recorder for MockServingServiceClient.
type MockServingServiceClientMockRecorder struct {
	mock *MockServingServiceClient
}

// NewMockServingServiceClient creates a new mock instance.
func NewMockServingServiceClient(ctrl *gomock.Controller) *MockServingServiceClient {
	mock := &MockServingServiceClient{ctrl: ctrl}
	mock.recorder = &MockServingServiceClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockServingServiceClient) EXPECT() *MockServingServiceClientMockRecorder {
	return m.recorder
}

// BatchQueryServingStatus mocks base method.
func (m *MockServingServiceClient) BatchQueryServingStatus(arg0 context.Context, arg1 *kusciaapi.BatchQueryServingStatusRequest, arg2 ...grpc.CallOption) (*kusciaapi.BatchQueryServingStatusResponse, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "BatchQueryServingStatus", varargs...)
	ret0, _ := ret[0].(*kusciaapi.BatchQueryServingStatusResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// BatchQueryServingStatus indicates an expected call of BatchQueryServingStatus.
func (mr *MockServingServiceClientMockRecorder) BatchQueryServingStatus(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "BatchQueryServingStatus", reflect.TypeOf((*MockServingServiceClient)(nil).BatchQueryServingStatus), varargs...)
}

// CreateServing mocks base method.
func (m *MockServingServiceClient) CreateServing(arg0 context.Context, arg1 *kusciaapi.CreateServingRequest, arg2 ...grpc.CallOption) (*kusciaapi.CreateServingResponse, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "CreateServing", varargs...)
	ret0, _ := ret[0].(*kusciaapi.CreateServingResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CreateServing indicates an expected call of CreateServing.
func (mr *MockServingServiceClientMockRecorder) CreateServing(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateServing", reflect.TypeOf((*MockServingServiceClient)(nil).CreateServing), varargs...)
}

// DeleteServing mocks base method.
func (m *MockServingServiceClient) DeleteServing(arg0 context.Context, arg1 *kusciaapi.DeleteServingRequest, arg2 ...grpc.CallOption) (*kusciaapi.DeleteServingResponse, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "DeleteServing", varargs...)
	ret0, _ := ret[0].(*kusciaapi.DeleteServingResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DeleteServing indicates an expected call of DeleteServing.
func (mr *MockServingServiceClientMockRecorder) DeleteServing(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteServing", reflect.TypeOf((*MockServingServiceClient)(nil).DeleteServing), varargs...)
}

// QueryServing mocks base method.
func (m *MockServingServiceClient) QueryServing(arg0 context.Context, arg1 *kusciaapi.QueryServingRequest, arg2 ...grpc.CallOption) (*kusciaapi.QueryServingResponse, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "QueryServing", varargs...)
	ret0, _ := ret[0].(*kusciaapi.QueryServingResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryServing indicates an expected call of QueryServing.
func (mr *MockServingServiceClientMockRecorder) QueryServing(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryServing", reflect.TypeOf((*MockServingServiceClient)(nil).QueryServing), varargs...)
}

// UpdateServing mocks base method.
func (m *MockServingServiceClient) UpdateServing(arg0 context.Context, arg1 *kusciaapi.UpdateServingRequest, arg2 ...grpc.CallOption) (*kusciaapi.UpdateServingResponse, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "UpdateServing", varargs...)
	ret0, _ := ret[0].(*kusciaapi.UpdateServingResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UpdateServing indicates an expected call of UpdateServing.
func (mr *MockServingServiceClientMockRecorder) UpdateServing(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateServing", reflect.TypeOf((*MockServingServiceClient)(nil).UpdateServing), varargs...)
}

// MockDomainDataServiceClient is a mock of DomainDataServiceClient interface.
type MockDomainDataServiceClient struct {
	ctrl     *gomock.Controller
	recorder *MockDomainDataServiceClientMockRecorder
}

// MockDomainDataServiceClientMockRecorder is the mock recorder for MockDomainDataServiceClient.
type MockDomainDataServiceClientMockRecorder struct {
	mock *MockDomainDataServiceClient
}

// NewMockDomainDataServiceClient creates a new mock instance.
func NewMockDomainDataServiceClient(ctrl *gomock.Controller) *MockDomainDataServiceClient {
	mock := &MockDomainDataServiceClient{ctrl: ctrl}
	mock.recorder = &MockDomainDataServiceClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockDomainDataServiceClient) EXPECT() *MockDomainDataServiceClientMockRecorder {
	return m.recorder
}

// BatchQueryDomainData mocks base method.
func (m *MockDomainDataServiceClient) BatchQueryDomainData(arg0 context.Context, arg1 *kusciaapi.BatchQueryDomainDataRequest, arg2 ...grpc.CallOption) (*kusciaapi.BatchQueryDomainDataResponse, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "BatchQueryDomainData", varargs...)
	ret0, _ := ret[0].(*kusciaapi.BatchQueryDomainDataResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// BatchQueryDomainData indicates an expected call of BatchQueryDomainData.
func (mr *MockDomainDataServiceClientMockRecorder) BatchQueryDomainData(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "BatchQueryDomainData", reflect.TypeOf((*MockDomainDataServiceClient)(nil).BatchQueryDomainData), varargs...)
}

// CreateDomainData mocks base method.
func (m *MockDomainDataServiceClient) CreateDomainData(arg0 context.Context, arg1 *kusciaapi.CreateDomainDataRequest, arg2 ...grpc.CallOption) (*kusciaapi.CreateDomainDataResponse, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "CreateDomainData", varargs...)
	ret0, _ := ret[0].(*kusciaapi.CreateDomainDataResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CreateDomainData indicates an expected call of CreateDomainData.
func (mr *MockDomainDataServiceClientMockRecorder) CreateDomainData(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateDomainData", reflect.TypeOf((*MockDomainDataServiceClient)(nil).CreateDomainData), varargs...)
}

// DeleteDomainData mocks base method.
func (m *MockDomainDataServiceClient) DeleteDomainData(arg0 context.Context, arg1 *kusciaapi.DeleteDomainDataRequest, arg2 ...grpc.CallOption) (*kusciaapi.DeleteDomainDataResponse, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "DeleteDomainData", varargs...)
	ret0, _ := ret[0].(*kusciaapi.DeleteDomainDataResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DeleteDomainData indicates an expected call of DeleteDomainData.
func (mr *MockDomainDataServiceClientMockRecorder) DeleteDomainData(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteDomainData", reflect.TypeOf((*MockDomainDataServiceClient)(nil).DeleteDomainData), varargs...)
}

// ListDomainData mocks base method.
func (m *MockDomainDataServiceClient) ListDomainData(arg0 context.Context, arg1 *kusciaapi.ListDomainDataRequest, arg2 ...grpc.CallOption) (*kusciaapi.ListDomainDataResponse, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "ListDomainData", varargs...)
	ret0, _ := ret[0].(*kusciaapi.ListDomainDataResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListDomainData indicates an expected call of ListDomainData.
func (mr *MockDomainDataServiceClientMockRecorder) ListDomainData(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListDomainData", reflect.TypeOf((*MockDomainDataServiceClient)(nil).ListDomainData), varargs...)
}

// QueryDomainData mocks base method.
func (m *MockDomainDataServiceClient) QueryDomainData(arg0 context.Context, arg1 *kusciaapi.QueryDomainDataRequest, arg2 ...grpc.CallOption) (*kusciaapi.QueryDomainDataResponse, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "QueryDomainData", varargs...)
	ret0, _ := ret[0].(*kusciaapi.QueryDomainDataResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryDomainData indicates an expected call of QueryDomainData.
func (mr *MockDomainDataServiceClientMockRecorder) QueryDomainData(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryDomainData", reflect.TypeOf((*MockDomainDataServiceClient)(nil).QueryDomainData), varargs...)
}

// UpdateDomainData mocks base method.
func (m *MockDomainDataServiceClient) UpdateDomainData(arg0 context.Context, arg1 *kusciaapi.UpdateDomainDataRequest, arg2 ...grpc.CallOption) (*kusciaapi.UpdateDomainDataResponse, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "UpdateDomainData", varargs...)
	ret0, _ := ret[0].(*kusciaapi.UpdateDomainDataResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UpdateDomainData indicates an expected call of UpdateDomainData.
func (mr *MockDomainDataServiceClientMockRecorder) UpdateDomainData(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateDomainData", reflect.TypeOf((*MockDomainDataServiceClient)(nil).UpdateDomainData), varargs...)
}
