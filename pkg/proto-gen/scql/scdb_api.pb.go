// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.0
// 	protoc        v3.19.4
// source: api/scdb_api.proto

package scql

import (
	context "context"
	_ "google.golang.org/genproto/googleapis/api"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type User_AccountSystemType int32

const (
	User_UNKNOWN     User_AccountSystemType = 0
	User_NATIVE_USER User_AccountSystemType = 1
)

// Enum value maps for User_AccountSystemType.
var (
	User_AccountSystemType_name = map[int32]string{
		0: "UNKNOWN",
		1: "NATIVE_USER",
	}
	User_AccountSystemType_value = map[string]int32{
		"UNKNOWN":     0,
		"NATIVE_USER": 1,
	}
)

func (x User_AccountSystemType) Enum() *User_AccountSystemType {
	p := new(User_AccountSystemType)
	*p = x
	return p
}

func (x User_AccountSystemType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (User_AccountSystemType) Descriptor() protoreflect.EnumDescriptor {
	return file_api_scdb_api_proto_enumTypes[0].Descriptor()
}

func (User_AccountSystemType) Type() protoreflect.EnumType {
	return &file_api_scdb_api_proto_enumTypes[0]
}

func (x User_AccountSystemType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use User_AccountSystemType.Descriptor instead.
func (User_AccountSystemType) EnumDescriptor() ([]byte, []int) {
	return file_api_scdb_api_proto_rawDescGZIP(), []int{4, 0}
}

type SCDBQueryRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Header                 *RequestHeader  `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	User                   *SCDBCredential `protobuf:"bytes,2,opt,name=user,proto3" json:"user,omitempty"`
	Query                  string          `protobuf:"bytes,3,opt,name=query,proto3" json:"query,omitempty"`
	QueryResultCallbackUrl string          `protobuf:"bytes,4,opt,name=query_result_callback_url,json=queryResultCallbackUrl,proto3" json:"query_result_callback_url,omitempty"`
	BizRequestId           string          `protobuf:"bytes,5,opt,name=biz_request_id,json=bizRequestId,proto3" json:"biz_request_id,omitempty"`
	DbName                 string          `protobuf:"bytes,6,opt,name=db_name,json=dbName,proto3" json:"db_name,omitempty"`
}

func (x *SCDBQueryRequest) Reset() {
	*x = SCDBQueryRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_scdb_api_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SCDBQueryRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SCDBQueryRequest) ProtoMessage() {}

func (x *SCDBQueryRequest) ProtoReflect() protoreflect.Message {
	mi := &file_api_scdb_api_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SCDBQueryRequest.ProtoReflect.Descriptor instead.
func (*SCDBQueryRequest) Descriptor() ([]byte, []int) {
	return file_api_scdb_api_proto_rawDescGZIP(), []int{0}
}

func (x *SCDBQueryRequest) GetHeader() *RequestHeader {
	if x != nil {
		return x.Header
	}
	return nil
}

func (x *SCDBQueryRequest) GetUser() *SCDBCredential {
	if x != nil {
		return x.User
	}
	return nil
}

func (x *SCDBQueryRequest) GetQuery() string {
	if x != nil {
		return x.Query
	}
	return ""
}

func (x *SCDBQueryRequest) GetQueryResultCallbackUrl() string {
	if x != nil {
		return x.QueryResultCallbackUrl
	}
	return ""
}

func (x *SCDBQueryRequest) GetBizRequestId() string {
	if x != nil {
		return x.BizRequestId
	}
	return ""
}

func (x *SCDBQueryRequest) GetDbName() string {
	if x != nil {
		return x.DbName
	}
	return ""
}

type SCDBSubmitResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Status        *Status `protobuf:"bytes,1,opt,name=status,proto3" json:"status,omitempty"`
	ScdbSessionId string  `protobuf:"bytes,2,opt,name=scdb_session_id,json=scdbSessionId,proto3" json:"scdb_session_id,omitempty"`
}

func (x *SCDBSubmitResponse) Reset() {
	*x = SCDBSubmitResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_scdb_api_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SCDBSubmitResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SCDBSubmitResponse) ProtoMessage() {}

func (x *SCDBSubmitResponse) ProtoReflect() protoreflect.Message {
	mi := &file_api_scdb_api_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SCDBSubmitResponse.ProtoReflect.Descriptor instead.
func (*SCDBSubmitResponse) Descriptor() ([]byte, []int) {
	return file_api_scdb_api_proto_rawDescGZIP(), []int{1}
}

func (x *SCDBSubmitResponse) GetStatus() *Status {
	if x != nil {
		return x.Status
	}
	return nil
}

func (x *SCDBSubmitResponse) GetScdbSessionId() string {
	if x != nil {
		return x.ScdbSessionId
	}
	return ""
}

type SCDBFetchRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Header        *RequestHeader  `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	User          *SCDBCredential `protobuf:"bytes,2,opt,name=user,proto3" json:"user,omitempty"`
	ScdbSessionId string          `protobuf:"bytes,3,opt,name=scdb_session_id,json=scdbSessionId,proto3" json:"scdb_session_id,omitempty"`
}

func (x *SCDBFetchRequest) Reset() {
	*x = SCDBFetchRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_scdb_api_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SCDBFetchRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SCDBFetchRequest) ProtoMessage() {}

func (x *SCDBFetchRequest) ProtoReflect() protoreflect.Message {
	mi := &file_api_scdb_api_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SCDBFetchRequest.ProtoReflect.Descriptor instead.
func (*SCDBFetchRequest) Descriptor() ([]byte, []int) {
	return file_api_scdb_api_proto_rawDescGZIP(), []int{2}
}

func (x *SCDBFetchRequest) GetHeader() *RequestHeader {
	if x != nil {
		return x.Header
	}
	return nil
}

func (x *SCDBFetchRequest) GetUser() *SCDBCredential {
	if x != nil {
		return x.User
	}
	return nil
}

func (x *SCDBFetchRequest) GetScdbSessionId() string {
	if x != nil {
		return x.ScdbSessionId
	}
	return ""
}

type SCDBQueryResultResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Status        *Status       `protobuf:"bytes,1,opt,name=status,proto3" json:"status,omitempty"`
	OutColumns    []*Tensor     `protobuf:"bytes,2,rep,name=out_columns,json=outColumns,proto3" json:"out_columns,omitempty"`
	ScdbSessionId string        `protobuf:"bytes,3,opt,name=scdb_session_id,json=scdbSessionId,proto3" json:"scdb_session_id,omitempty"`
	AffectedRows  int64         `protobuf:"varint,4,opt,name=affected_rows,json=affectedRows,proto3" json:"affected_rows,omitempty"`
	Warnings      []*SQLWarning `protobuf:"bytes,5,rep,name=warnings,proto3" json:"warnings,omitempty"`
}

func (x *SCDBQueryResultResponse) Reset() {
	*x = SCDBQueryResultResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_scdb_api_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SCDBQueryResultResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SCDBQueryResultResponse) ProtoMessage() {}

func (x *SCDBQueryResultResponse) ProtoReflect() protoreflect.Message {
	mi := &file_api_scdb_api_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SCDBQueryResultResponse.ProtoReflect.Descriptor instead.
func (*SCDBQueryResultResponse) Descriptor() ([]byte, []int) {
	return file_api_scdb_api_proto_rawDescGZIP(), []int{3}
}

func (x *SCDBQueryResultResponse) GetStatus() *Status {
	if x != nil {
		return x.Status
	}
	return nil
}

func (x *SCDBQueryResultResponse) GetOutColumns() []*Tensor {
	if x != nil {
		return x.OutColumns
	}
	return nil
}

func (x *SCDBQueryResultResponse) GetScdbSessionId() string {
	if x != nil {
		return x.ScdbSessionId
	}
	return ""
}

func (x *SCDBQueryResultResponse) GetAffectedRows() int64 {
	if x != nil {
		return x.AffectedRows
	}
	return 0
}

func (x *SCDBQueryResultResponse) GetWarnings() []*SQLWarning {
	if x != nil {
		return x.Warnings
	}
	return nil
}

type User struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	AccountSystemType User_AccountSystemType `protobuf:"varint,1,opt,name=account_system_type,json=accountSystemType,proto3,enum=scql.pb.User_AccountSystemType" json:"account_system_type,omitempty"`
	// Types that are assignable to User:
	//	*User_NativeUser_
	User isUser_User `protobuf_oneof:"user"`
}

func (x *User) Reset() {
	*x = User{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_scdb_api_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *User) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*User) ProtoMessage() {}

func (x *User) ProtoReflect() protoreflect.Message {
	mi := &file_api_scdb_api_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use User.ProtoReflect.Descriptor instead.
func (*User) Descriptor() ([]byte, []int) {
	return file_api_scdb_api_proto_rawDescGZIP(), []int{4}
}

func (x *User) GetAccountSystemType() User_AccountSystemType {
	if x != nil {
		return x.AccountSystemType
	}
	return User_UNKNOWN
}

func (m *User) GetUser() isUser_User {
	if m != nil {
		return m.User
	}
	return nil
}

func (x *User) GetNativeUser() *User_NativeUser {
	if x, ok := x.GetUser().(*User_NativeUser_); ok {
		return x.NativeUser
	}
	return nil
}

type isUser_User interface {
	isUser_User()
}

type User_NativeUser_ struct {
	NativeUser *User_NativeUser `protobuf:"bytes,2,opt,name=native_user,json=nativeUser,proto3,oneof"`
}

func (*User_NativeUser_) isUser_User() {}

type SCDBCredential struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	User *User `protobuf:"bytes,1,opt,name=user,proto3" json:"user,omitempty"`
}

func (x *SCDBCredential) Reset() {
	*x = SCDBCredential{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_scdb_api_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SCDBCredential) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SCDBCredential) ProtoMessage() {}

func (x *SCDBCredential) ProtoReflect() protoreflect.Message {
	mi := &file_api_scdb_api_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SCDBCredential.ProtoReflect.Descriptor instead.
func (*SCDBCredential) Descriptor() ([]byte, []int) {
	return file_api_scdb_api_proto_rawDescGZIP(), []int{5}
}

func (x *SCDBCredential) GetUser() *User {
	if x != nil {
		return x.User
	}
	return nil
}

type User_NativeUser struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Name     string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Password string `protobuf:"bytes,2,opt,name=password,proto3" json:"password,omitempty"`
}

func (x *User_NativeUser) Reset() {
	*x = User_NativeUser{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_scdb_api_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *User_NativeUser) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*User_NativeUser) ProtoMessage() {}

func (x *User_NativeUser) ProtoReflect() protoreflect.Message {
	mi := &file_api_scdb_api_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use User_NativeUser.ProtoReflect.Descriptor instead.
func (*User_NativeUser) Descriptor() ([]byte, []int) {
	return file_api_scdb_api_proto_rawDescGZIP(), []int{4, 0}
}

func (x *User_NativeUser) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *User_NativeUser) GetPassword() string {
	if x != nil {
		return x.Password
	}
	return ""
}

var File_api_scdb_api_proto protoreflect.FileDescriptor

var file_api_scdb_api_proto_rawDesc = []byte{
	0x0a, 0x12, 0x61, 0x70, 0x69, 0x2f, 0x73, 0x63, 0x64, 0x62, 0x5f, 0x61, 0x70, 0x69, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x12, 0x07, 0x73, 0x63, 0x71, 0x6c, 0x2e, 0x70, 0x62, 0x1a, 0x0e, 0x61,
	0x70, 0x69, 0x2f, 0x63, 0x6f, 0x72, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x10, 0x61,
	0x70, 0x69, 0x2f, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a,
	0x10, 0x61, 0x70, 0x69, 0x2f, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x1a, 0x1c, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x61, 0x6e,
	0x6e, 0x6f, 0x74, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a,
	0x1f, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x66, 0x69, 0x65, 0x6c,
	0x64, 0x5f, 0x62, 0x65, 0x68, 0x61, 0x76, 0x69, 0x6f, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x1a, 0x1b, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75,
	0x66, 0x2f, 0x65, 0x6d, 0x70, 0x74, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x8e, 0x02,
	0x0a, 0x10, 0x53, 0x43, 0x44, 0x42, 0x51, 0x75, 0x65, 0x72, 0x79, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x12, 0x2e, 0x0a, 0x06, 0x68, 0x65, 0x61, 0x64, 0x65, 0x72, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x16, 0x2e, 0x73, 0x63, 0x71, 0x6c, 0x2e, 0x70, 0x62, 0x2e, 0x52, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x48, 0x65, 0x61, 0x64, 0x65, 0x72, 0x52, 0x06, 0x68, 0x65, 0x61, 0x64,
	0x65, 0x72, 0x12, 0x30, 0x0a, 0x04, 0x75, 0x73, 0x65, 0x72, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x17, 0x2e, 0x73, 0x63, 0x71, 0x6c, 0x2e, 0x70, 0x62, 0x2e, 0x53, 0x43, 0x44, 0x42, 0x43,
	0x72, 0x65, 0x64, 0x65, 0x6e, 0x74, 0x69, 0x61, 0x6c, 0x42, 0x03, 0xe0, 0x41, 0x02, 0x52, 0x04,
	0x75, 0x73, 0x65, 0x72, 0x12, 0x19, 0x0a, 0x05, 0x71, 0x75, 0x65, 0x72, 0x79, 0x18, 0x03, 0x20,
	0x01, 0x28, 0x09, 0x42, 0x03, 0xe0, 0x41, 0x02, 0x52, 0x05, 0x71, 0x75, 0x65, 0x72, 0x79, 0x12,
	0x39, 0x0a, 0x19, 0x71, 0x75, 0x65, 0x72, 0x79, 0x5f, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x5f,
	0x63, 0x61, 0x6c, 0x6c, 0x62, 0x61, 0x63, 0x6b, 0x5f, 0x75, 0x72, 0x6c, 0x18, 0x04, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x16, 0x71, 0x75, 0x65, 0x72, 0x79, 0x52, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x43,
	0x61, 0x6c, 0x6c, 0x62, 0x61, 0x63, 0x6b, 0x55, 0x72, 0x6c, 0x12, 0x24, 0x0a, 0x0e, 0x62, 0x69,
	0x7a, 0x5f, 0x72, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x5f, 0x69, 0x64, 0x18, 0x05, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x0c, 0x62, 0x69, 0x7a, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x49, 0x64,
	0x12, 0x1c, 0x0a, 0x07, 0x64, 0x62, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x06, 0x20, 0x01, 0x28,
	0x09, 0x42, 0x03, 0xe0, 0x41, 0x02, 0x52, 0x06, 0x64, 0x62, 0x4e, 0x61, 0x6d, 0x65, 0x22, 0x65,
	0x0a, 0x12, 0x53, 0x43, 0x44, 0x42, 0x53, 0x75, 0x62, 0x6d, 0x69, 0x74, 0x52, 0x65, 0x73, 0x70,
	0x6f, 0x6e, 0x73, 0x65, 0x12, 0x27, 0x0a, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x0f, 0x2e, 0x73, 0x63, 0x71, 0x6c, 0x2e, 0x70, 0x62, 0x2e, 0x53,
	0x74, 0x61, 0x74, 0x75, 0x73, 0x52, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x12, 0x26, 0x0a,
	0x0f, 0x73, 0x63, 0x64, 0x62, 0x5f, 0x73, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x5f, 0x69, 0x64,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0d, 0x73, 0x63, 0x64, 0x62, 0x53, 0x65, 0x73, 0x73,
	0x69, 0x6f, 0x6e, 0x49, 0x64, 0x22, 0xa1, 0x01, 0x0a, 0x10, 0x53, 0x43, 0x44, 0x42, 0x46, 0x65,
	0x74, 0x63, 0x68, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x2e, 0x0a, 0x06, 0x68, 0x65,
	0x61, 0x64, 0x65, 0x72, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x16, 0x2e, 0x73, 0x63, 0x71,
	0x6c, 0x2e, 0x70, 0x62, 0x2e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x48, 0x65, 0x61, 0x64,
	0x65, 0x72, 0x52, 0x06, 0x68, 0x65, 0x61, 0x64, 0x65, 0x72, 0x12, 0x30, 0x0a, 0x04, 0x75, 0x73,
	0x65, 0x72, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x17, 0x2e, 0x73, 0x63, 0x71, 0x6c, 0x2e,
	0x70, 0x62, 0x2e, 0x53, 0x43, 0x44, 0x42, 0x43, 0x72, 0x65, 0x64, 0x65, 0x6e, 0x74, 0x69, 0x61,
	0x6c, 0x42, 0x03, 0xe0, 0x41, 0x02, 0x52, 0x04, 0x75, 0x73, 0x65, 0x72, 0x12, 0x2b, 0x0a, 0x0f,
	0x73, 0x63, 0x64, 0x62, 0x5f, 0x73, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x5f, 0x69, 0x64, 0x18,
	0x03, 0x20, 0x01, 0x28, 0x09, 0x42, 0x03, 0xe0, 0x41, 0x02, 0x52, 0x0d, 0x73, 0x63, 0x64, 0x62,
	0x53, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x49, 0x64, 0x22, 0xf2, 0x01, 0x0a, 0x17, 0x53, 0x43,
	0x44, 0x42, 0x51, 0x75, 0x65, 0x72, 0x79, 0x52, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x52, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x27, 0x0a, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0f, 0x2e, 0x73, 0x63, 0x71, 0x6c, 0x2e, 0x70, 0x62, 0x2e,
	0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x52, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x12, 0x30,
	0x0a, 0x0b, 0x6f, 0x75, 0x74, 0x5f, 0x63, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x73, 0x18, 0x02, 0x20,
	0x03, 0x28, 0x0b, 0x32, 0x0f, 0x2e, 0x73, 0x63, 0x71, 0x6c, 0x2e, 0x70, 0x62, 0x2e, 0x54, 0x65,
	0x6e, 0x73, 0x6f, 0x72, 0x52, 0x0a, 0x6f, 0x75, 0x74, 0x43, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x73,
	0x12, 0x26, 0x0a, 0x0f, 0x73, 0x63, 0x64, 0x62, 0x5f, 0x73, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e,
	0x5f, 0x69, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0d, 0x73, 0x63, 0x64, 0x62, 0x53,
	0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x49, 0x64, 0x12, 0x23, 0x0a, 0x0d, 0x61, 0x66, 0x66, 0x65,
	0x63, 0x74, 0x65, 0x64, 0x5f, 0x72, 0x6f, 0x77, 0x73, 0x18, 0x04, 0x20, 0x01, 0x28, 0x03, 0x52,
	0x0c, 0x61, 0x66, 0x66, 0x65, 0x63, 0x74, 0x65, 0x64, 0x52, 0x6f, 0x77, 0x73, 0x12, 0x2f, 0x0a,
	0x08, 0x77, 0x61, 0x72, 0x6e, 0x69, 0x6e, 0x67, 0x73, 0x18, 0x05, 0x20, 0x03, 0x28, 0x0b, 0x32,
	0x13, 0x2e, 0x73, 0x63, 0x71, 0x6c, 0x2e, 0x70, 0x62, 0x2e, 0x53, 0x51, 0x4c, 0x57, 0x61, 0x72,
	0x6e, 0x69, 0x6e, 0x67, 0x52, 0x08, 0x77, 0x61, 0x72, 0x6e, 0x69, 0x6e, 0x67, 0x73, 0x22, 0x8d,
	0x02, 0x0a, 0x04, 0x55, 0x73, 0x65, 0x72, 0x12, 0x4f, 0x0a, 0x13, 0x61, 0x63, 0x63, 0x6f, 0x75,
	0x6e, 0x74, 0x5f, 0x73, 0x79, 0x73, 0x74, 0x65, 0x6d, 0x5f, 0x74, 0x79, 0x70, 0x65, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x0e, 0x32, 0x1f, 0x2e, 0x73, 0x63, 0x71, 0x6c, 0x2e, 0x70, 0x62, 0x2e, 0x55,
	0x73, 0x65, 0x72, 0x2e, 0x41, 0x63, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x53, 0x79, 0x73, 0x74, 0x65,
	0x6d, 0x54, 0x79, 0x70, 0x65, 0x52, 0x11, 0x61, 0x63, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x53, 0x79,
	0x73, 0x74, 0x65, 0x6d, 0x54, 0x79, 0x70, 0x65, 0x12, 0x3b, 0x0a, 0x0b, 0x6e, 0x61, 0x74, 0x69,
	0x76, 0x65, 0x5f, 0x75, 0x73, 0x65, 0x72, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x18, 0x2e,
	0x73, 0x63, 0x71, 0x6c, 0x2e, 0x70, 0x62, 0x2e, 0x55, 0x73, 0x65, 0x72, 0x2e, 0x4e, 0x61, 0x74,
	0x69, 0x76, 0x65, 0x55, 0x73, 0x65, 0x72, 0x48, 0x00, 0x52, 0x0a, 0x6e, 0x61, 0x74, 0x69, 0x76,
	0x65, 0x55, 0x73, 0x65, 0x72, 0x1a, 0x3c, 0x0a, 0x0a, 0x4e, 0x61, 0x74, 0x69, 0x76, 0x65, 0x55,
	0x73, 0x65, 0x72, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x1a, 0x0a, 0x08, 0x70, 0x61, 0x73, 0x73, 0x77,
	0x6f, 0x72, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x70, 0x61, 0x73, 0x73, 0x77,
	0x6f, 0x72, 0x64, 0x22, 0x31, 0x0a, 0x11, 0x41, 0x63, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x53, 0x79,
	0x73, 0x74, 0x65, 0x6d, 0x54, 0x79, 0x70, 0x65, 0x12, 0x0b, 0x0a, 0x07, 0x55, 0x4e, 0x4b, 0x4e,
	0x4f, 0x57, 0x4e, 0x10, 0x00, 0x12, 0x0f, 0x0a, 0x0b, 0x4e, 0x41, 0x54, 0x49, 0x56, 0x45, 0x5f,
	0x55, 0x53, 0x45, 0x52, 0x10, 0x01, 0x42, 0x06, 0x0a, 0x04, 0x75, 0x73, 0x65, 0x72, 0x22, 0x33,
	0x0a, 0x0e, 0x53, 0x43, 0x44, 0x42, 0x43, 0x72, 0x65, 0x64, 0x65, 0x6e, 0x74, 0x69, 0x61, 0x6c,
	0x12, 0x21, 0x0a, 0x04, 0x75, 0x73, 0x65, 0x72, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0d,
	0x2e, 0x73, 0x63, 0x71, 0x6c, 0x2e, 0x70, 0x62, 0x2e, 0x55, 0x73, 0x65, 0x72, 0x52, 0x04, 0x75,
	0x73, 0x65, 0x72, 0x32, 0xc7, 0x02, 0x0a, 0x0b, 0x53, 0x43, 0x44, 0x42, 0x53, 0x65, 0x72, 0x76,
	0x69, 0x63, 0x65, 0x12, 0x61, 0x0a, 0x06, 0x53, 0x75, 0x62, 0x6d, 0x69, 0x74, 0x12, 0x19, 0x2e,
	0x73, 0x63, 0x71, 0x6c, 0x2e, 0x70, 0x62, 0x2e, 0x53, 0x43, 0x44, 0x42, 0x51, 0x75, 0x65, 0x72,
	0x79, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x1b, 0x2e, 0x73, 0x63, 0x71, 0x6c, 0x2e,
	0x70, 0x62, 0x2e, 0x53, 0x43, 0x44, 0x42, 0x53, 0x75, 0x62, 0x6d, 0x69, 0x74, 0x52, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x1f, 0x82, 0xd3, 0xe4, 0x93, 0x02, 0x19, 0x22, 0x14, 0x2f,
	0x70, 0x75, 0x62, 0x6c, 0x69, 0x63, 0x2f, 0x73, 0x75, 0x62, 0x6d, 0x69, 0x74, 0x5f, 0x71, 0x75,
	0x65, 0x72, 0x79, 0x3a, 0x01, 0x2a, 0x12, 0x65, 0x0a, 0x05, 0x46, 0x65, 0x74, 0x63, 0x68, 0x12,
	0x19, 0x2e, 0x73, 0x63, 0x71, 0x6c, 0x2e, 0x70, 0x62, 0x2e, 0x53, 0x43, 0x44, 0x42, 0x46, 0x65,
	0x74, 0x63, 0x68, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x20, 0x2e, 0x73, 0x63, 0x71,
	0x6c, 0x2e, 0x70, 0x62, 0x2e, 0x53, 0x43, 0x44, 0x42, 0x51, 0x75, 0x65, 0x72, 0x79, 0x52, 0x65,
	0x73, 0x75, 0x6c, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x1f, 0x82, 0xd3,
	0xe4, 0x93, 0x02, 0x19, 0x22, 0x14, 0x2f, 0x70, 0x75, 0x62, 0x6c, 0x69, 0x63, 0x2f, 0x66, 0x65,
	0x74, 0x63, 0x68, 0x5f, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x3a, 0x01, 0x2a, 0x12, 0x6e, 0x0a,
	0x0c, 0x53, 0x75, 0x62, 0x6d, 0x69, 0x74, 0x41, 0x6e, 0x64, 0x47, 0x65, 0x74, 0x12, 0x19, 0x2e,
	0x73, 0x63, 0x71, 0x6c, 0x2e, 0x70, 0x62, 0x2e, 0x53, 0x43, 0x44, 0x42, 0x51, 0x75, 0x65, 0x72,
	0x79, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x20, 0x2e, 0x73, 0x63, 0x71, 0x6c, 0x2e,
	0x70, 0x62, 0x2e, 0x53, 0x43, 0x44, 0x42, 0x51, 0x75, 0x65, 0x72, 0x79, 0x52, 0x65, 0x73, 0x75,
	0x6c, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x21, 0x82, 0xd3, 0xe4, 0x93,
	0x02, 0x1b, 0x22, 0x16, 0x2f, 0x70, 0x75, 0x62, 0x6c, 0x69, 0x63, 0x2f, 0x73, 0x75, 0x62, 0x6d,
	0x69, 0x74, 0x5f, 0x61, 0x6e, 0x64, 0x5f, 0x67, 0x65, 0x74, 0x3a, 0x01, 0x2a, 0x32, 0x68, 0x0a,
	0x17, 0x53, 0x43, 0x44, 0x42, 0x51, 0x75, 0x65, 0x72, 0x79, 0x52, 0x65, 0x73, 0x75, 0x6c, 0x74,
	0x43, 0x61, 0x6c, 0x6c, 0x62, 0x61, 0x63, 0x6b, 0x12, 0x4d, 0x0a, 0x11, 0x52, 0x65, 0x70, 0x6f,
	0x72, 0x74, 0x51, 0x75, 0x65, 0x72, 0x79, 0x52, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x12, 0x20, 0x2e,
	0x73, 0x63, 0x71, 0x6c, 0x2e, 0x70, 0x62, 0x2e, 0x53, 0x43, 0x44, 0x42, 0x51, 0x75, 0x65, 0x72,
	0x79, 0x52, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x1a,
	0x16, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75,
	0x66, 0x2e, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x42, 0x10, 0x5a, 0x0e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x2d, 0x67, 0x65, 0x6e, 0x2f, 0x73, 0x63, 0x71, 0x6c, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x33,
}

var (
	file_api_scdb_api_proto_rawDescOnce sync.Once
	file_api_scdb_api_proto_rawDescData = file_api_scdb_api_proto_rawDesc
)

func file_api_scdb_api_proto_rawDescGZIP() []byte {
	file_api_scdb_api_proto_rawDescOnce.Do(func() {
		file_api_scdb_api_proto_rawDescData = protoimpl.X.CompressGZIP(file_api_scdb_api_proto_rawDescData)
	})
	return file_api_scdb_api_proto_rawDescData
}

var file_api_scdb_api_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_api_scdb_api_proto_msgTypes = make([]protoimpl.MessageInfo, 7)
var file_api_scdb_api_proto_goTypes = []interface{}{
	(User_AccountSystemType)(0),     // 0: scql.pb.User.AccountSystemType
	(*SCDBQueryRequest)(nil),        // 1: scql.pb.SCDBQueryRequest
	(*SCDBSubmitResponse)(nil),      // 2: scql.pb.SCDBSubmitResponse
	(*SCDBFetchRequest)(nil),        // 3: scql.pb.SCDBFetchRequest
	(*SCDBQueryResultResponse)(nil), // 4: scql.pb.SCDBQueryResultResponse
	(*User)(nil),                    // 5: scql.pb.User
	(*SCDBCredential)(nil),          // 6: scql.pb.SCDBCredential
	(*User_NativeUser)(nil),         // 7: scql.pb.User.NativeUser
	(*RequestHeader)(nil),           // 8: scql.pb.RequestHeader
	(*Status)(nil),                  // 9: scql.pb.Status
	(*Tensor)(nil),                  // 10: scql.pb.Tensor
	(*SQLWarning)(nil),              // 11: scql.pb.SQLWarning
	(*emptypb.Empty)(nil),           // 12: google.protobuf.Empty
}
var file_api_scdb_api_proto_depIdxs = []int32{
	8,  // 0: scql.pb.SCDBQueryRequest.header:type_name -> scql.pb.RequestHeader
	6,  // 1: scql.pb.SCDBQueryRequest.user:type_name -> scql.pb.SCDBCredential
	9,  // 2: scql.pb.SCDBSubmitResponse.status:type_name -> scql.pb.Status
	8,  // 3: scql.pb.SCDBFetchRequest.header:type_name -> scql.pb.RequestHeader
	6,  // 4: scql.pb.SCDBFetchRequest.user:type_name -> scql.pb.SCDBCredential
	9,  // 5: scql.pb.SCDBQueryResultResponse.status:type_name -> scql.pb.Status
	10, // 6: scql.pb.SCDBQueryResultResponse.out_columns:type_name -> scql.pb.Tensor
	11, // 7: scql.pb.SCDBQueryResultResponse.warnings:type_name -> scql.pb.SQLWarning
	0,  // 8: scql.pb.User.account_system_type:type_name -> scql.pb.User.AccountSystemType
	7,  // 9: scql.pb.User.native_user:type_name -> scql.pb.User.NativeUser
	5,  // 10: scql.pb.SCDBCredential.user:type_name -> scql.pb.User
	1,  // 11: scql.pb.SCDBService.Submit:input_type -> scql.pb.SCDBQueryRequest
	3,  // 12: scql.pb.SCDBService.Fetch:input_type -> scql.pb.SCDBFetchRequest
	1,  // 13: scql.pb.SCDBService.SubmitAndGet:input_type -> scql.pb.SCDBQueryRequest
	4,  // 14: scql.pb.SCDBQueryResultCallback.ReportQueryResult:input_type -> scql.pb.SCDBQueryResultResponse
	2,  // 15: scql.pb.SCDBService.Submit:output_type -> scql.pb.SCDBSubmitResponse
	4,  // 16: scql.pb.SCDBService.Fetch:output_type -> scql.pb.SCDBQueryResultResponse
	4,  // 17: scql.pb.SCDBService.SubmitAndGet:output_type -> scql.pb.SCDBQueryResultResponse
	12, // 18: scql.pb.SCDBQueryResultCallback.ReportQueryResult:output_type -> google.protobuf.Empty
	15, // [15:19] is the sub-list for method output_type
	11, // [11:15] is the sub-list for method input_type
	11, // [11:11] is the sub-list for extension type_name
	11, // [11:11] is the sub-list for extension extendee
	0,  // [0:11] is the sub-list for field type_name
}

func init() { file_api_scdb_api_proto_init() }
func file_api_scdb_api_proto_init() {
	if File_api_scdb_api_proto != nil {
		return
	}
	file_api_core_proto_init()
	file_api_common_proto_init()
	file_api_status_proto_init()
	if !protoimpl.UnsafeEnabled {
		file_api_scdb_api_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SCDBQueryRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_api_scdb_api_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SCDBSubmitResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_api_scdb_api_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SCDBFetchRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_api_scdb_api_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SCDBQueryResultResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_api_scdb_api_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*User); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_api_scdb_api_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SCDBCredential); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_api_scdb_api_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*User_NativeUser); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	file_api_scdb_api_proto_msgTypes[4].OneofWrappers = []interface{}{
		(*User_NativeUser_)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_api_scdb_api_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   7,
			NumExtensions: 0,
			NumServices:   2,
		},
		GoTypes:           file_api_scdb_api_proto_goTypes,
		DependencyIndexes: file_api_scdb_api_proto_depIdxs,
		EnumInfos:         file_api_scdb_api_proto_enumTypes,
		MessageInfos:      file_api_scdb_api_proto_msgTypes,
	}.Build()
	File_api_scdb_api_proto = out.File
	file_api_scdb_api_proto_rawDesc = nil
	file_api_scdb_api_proto_goTypes = nil
	file_api_scdb_api_proto_depIdxs = nil
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConnInterface

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion6

// SCDBServiceClient is the client API for SCDBService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type SCDBServiceClient interface {
	Submit(ctx context.Context, in *SCDBQueryRequest, opts ...grpc.CallOption) (*SCDBSubmitResponse, error)
	Fetch(ctx context.Context, in *SCDBFetchRequest, opts ...grpc.CallOption) (*SCDBQueryResultResponse, error)
	SubmitAndGet(ctx context.Context, in *SCDBQueryRequest, opts ...grpc.CallOption) (*SCDBQueryResultResponse, error)
}

type sCDBServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewSCDBServiceClient(cc grpc.ClientConnInterface) SCDBServiceClient {
	return &sCDBServiceClient{cc}
}

func (c *sCDBServiceClient) Submit(ctx context.Context, in *SCDBQueryRequest, opts ...grpc.CallOption) (*SCDBSubmitResponse, error) {
	out := new(SCDBSubmitResponse)
	err := c.cc.Invoke(ctx, "/scql.pb.SCDBService/Submit", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *sCDBServiceClient) Fetch(ctx context.Context, in *SCDBFetchRequest, opts ...grpc.CallOption) (*SCDBQueryResultResponse, error) {
	out := new(SCDBQueryResultResponse)
	err := c.cc.Invoke(ctx, "/scql.pb.SCDBService/Fetch", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *sCDBServiceClient) SubmitAndGet(ctx context.Context, in *SCDBQueryRequest, opts ...grpc.CallOption) (*SCDBQueryResultResponse, error) {
	out := new(SCDBQueryResultResponse)
	err := c.cc.Invoke(ctx, "/scql.pb.SCDBService/SubmitAndGet", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// SCDBServiceServer is the server API for SCDBService service.
type SCDBServiceServer interface {
	Submit(context.Context, *SCDBQueryRequest) (*SCDBSubmitResponse, error)
	Fetch(context.Context, *SCDBFetchRequest) (*SCDBQueryResultResponse, error)
	SubmitAndGet(context.Context, *SCDBQueryRequest) (*SCDBQueryResultResponse, error)
}

// UnimplementedSCDBServiceServer can be embedded to have forward compatible implementations.
type UnimplementedSCDBServiceServer struct {
}

func (*UnimplementedSCDBServiceServer) Submit(context.Context, *SCDBQueryRequest) (*SCDBSubmitResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Submit not implemented")
}
func (*UnimplementedSCDBServiceServer) Fetch(context.Context, *SCDBFetchRequest) (*SCDBQueryResultResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Fetch not implemented")
}
func (*UnimplementedSCDBServiceServer) SubmitAndGet(context.Context, *SCDBQueryRequest) (*SCDBQueryResultResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SubmitAndGet not implemented")
}

func RegisterSCDBServiceServer(s *grpc.Server, srv SCDBServiceServer) {
	s.RegisterService(&_SCDBService_serviceDesc, srv)
}

func _SCDBService_Submit_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SCDBQueryRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SCDBServiceServer).Submit(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/scql.pb.SCDBService/Submit",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SCDBServiceServer).Submit(ctx, req.(*SCDBQueryRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _SCDBService_Fetch_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SCDBFetchRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SCDBServiceServer).Fetch(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/scql.pb.SCDBService/Fetch",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SCDBServiceServer).Fetch(ctx, req.(*SCDBFetchRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _SCDBService_SubmitAndGet_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SCDBQueryRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SCDBServiceServer).SubmitAndGet(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/scql.pb.SCDBService/SubmitAndGet",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SCDBServiceServer).SubmitAndGet(ctx, req.(*SCDBQueryRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _SCDBService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "scql.pb.SCDBService",
	HandlerType: (*SCDBServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Submit",
			Handler:    _SCDBService_Submit_Handler,
		},
		{
			MethodName: "Fetch",
			Handler:    _SCDBService_Fetch_Handler,
		},
		{
			MethodName: "SubmitAndGet",
			Handler:    _SCDBService_SubmitAndGet_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "api/scdb_api.proto",
}

// SCDBQueryResultCallbackClient is the client API for SCDBQueryResultCallback service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type SCDBQueryResultCallbackClient interface {
	ReportQueryResult(ctx context.Context, in *SCDBQueryResultResponse, opts ...grpc.CallOption) (*emptypb.Empty, error)
}

type sCDBQueryResultCallbackClient struct {
	cc grpc.ClientConnInterface
}

func NewSCDBQueryResultCallbackClient(cc grpc.ClientConnInterface) SCDBQueryResultCallbackClient {
	return &sCDBQueryResultCallbackClient{cc}
}

func (c *sCDBQueryResultCallbackClient) ReportQueryResult(ctx context.Context, in *SCDBQueryResultResponse, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, "/scql.pb.SCDBQueryResultCallback/ReportQueryResult", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// SCDBQueryResultCallbackServer is the server API for SCDBQueryResultCallback service.
type SCDBQueryResultCallbackServer interface {
	ReportQueryResult(context.Context, *SCDBQueryResultResponse) (*emptypb.Empty, error)
}

// UnimplementedSCDBQueryResultCallbackServer can be embedded to have forward compatible implementations.
type UnimplementedSCDBQueryResultCallbackServer struct {
}

func (*UnimplementedSCDBQueryResultCallbackServer) ReportQueryResult(context.Context, *SCDBQueryResultResponse) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReportQueryResult not implemented")
}

func RegisterSCDBQueryResultCallbackServer(s *grpc.Server, srv SCDBQueryResultCallbackServer) {
	s.RegisterService(&_SCDBQueryResultCallback_serviceDesc, srv)
}

func _SCDBQueryResultCallback_ReportQueryResult_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SCDBQueryResultResponse)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SCDBQueryResultCallbackServer).ReportQueryResult(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/scql.pb.SCDBQueryResultCallback/ReportQueryResult",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SCDBQueryResultCallbackServer).ReportQueryResult(ctx, req.(*SCDBQueryResultResponse))
	}
	return interceptor(ctx, in, info, handler)
}

var _SCDBQueryResultCallback_serviceDesc = grpc.ServiceDesc{
	ServiceName: "scql.pb.SCDBQueryResultCallback",
	HandlerType: (*SCDBQueryResultCallbackServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "ReportQueryResult",
			Handler:    _SCDBQueryResultCallback_ReportQueryResult_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "api/scdb_api.proto",
}
