// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.0
// 	protoc        v3.21.12
// source: api/ccl.proto

package scql

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type SecurityConfig_ColumnControl_Visibility int32

const (
	SecurityConfig_ColumnControl_UNKNOWN                   SecurityConfig_ColumnControl_Visibility = 0
	SecurityConfig_ColumnControl_PLAINTEXT                 SecurityConfig_ColumnControl_Visibility = 1
	SecurityConfig_ColumnControl_ENCRYPTED_ONLY            SecurityConfig_ColumnControl_Visibility = 2
	SecurityConfig_ColumnControl_PLAINTEXT_AFTER_JOIN      SecurityConfig_ColumnControl_Visibility = 3
	SecurityConfig_ColumnControl_PLAINTEXT_AFTER_GROUP_BY  SecurityConfig_ColumnControl_Visibility = 4
	SecurityConfig_ColumnControl_PLAINTEXT_AFTER_COMPARE   SecurityConfig_ColumnControl_Visibility = 5
	SecurityConfig_ColumnControl_PLAINTEXT_AFTER_AGGREGATE SecurityConfig_ColumnControl_Visibility = 6
)

// Enum value maps for SecurityConfig_ColumnControl_Visibility.
var (
	SecurityConfig_ColumnControl_Visibility_name = map[int32]string{
		0: "UNKNOWN",
		1: "PLAINTEXT",
		2: "ENCRYPTED_ONLY",
		3: "PLAINTEXT_AFTER_JOIN",
		4: "PLAINTEXT_AFTER_GROUP_BY",
		5: "PLAINTEXT_AFTER_COMPARE",
		6: "PLAINTEXT_AFTER_AGGREGATE",
	}
	SecurityConfig_ColumnControl_Visibility_value = map[string]int32{
		"UNKNOWN":                   0,
		"PLAINTEXT":                 1,
		"ENCRYPTED_ONLY":            2,
		"PLAINTEXT_AFTER_JOIN":      3,
		"PLAINTEXT_AFTER_GROUP_BY":  4,
		"PLAINTEXT_AFTER_COMPARE":   5,
		"PLAINTEXT_AFTER_AGGREGATE": 6,
	}
)

func (x SecurityConfig_ColumnControl_Visibility) Enum() *SecurityConfig_ColumnControl_Visibility {
	p := new(SecurityConfig_ColumnControl_Visibility)
	*p = x
	return p
}

func (x SecurityConfig_ColumnControl_Visibility) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (SecurityConfig_ColumnControl_Visibility) Descriptor() protoreflect.EnumDescriptor {
	return file_api_ccl_proto_enumTypes[0].Descriptor()
}

func (SecurityConfig_ColumnControl_Visibility) Type() protoreflect.EnumType {
	return &file_api_ccl_proto_enumTypes[0]
}

func (x SecurityConfig_ColumnControl_Visibility) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use SecurityConfig_ColumnControl_Visibility.Descriptor instead.
func (SecurityConfig_ColumnControl_Visibility) EnumDescriptor() ([]byte, []int) {
	return file_api_ccl_proto_rawDescGZIP(), []int{0, 0, 0}
}

type SecurityConfig struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ColumnControlList []*SecurityConfig_ColumnControl `protobuf:"bytes,1,rep,name=column_control_list,json=columnControlList,proto3" json:"column_control_list,omitempty"`
}

func (x *SecurityConfig) Reset() {
	*x = SecurityConfig{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_ccl_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SecurityConfig) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SecurityConfig) ProtoMessage() {}

func (x *SecurityConfig) ProtoReflect() protoreflect.Message {
	mi := &file_api_ccl_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SecurityConfig.ProtoReflect.Descriptor instead.
func (*SecurityConfig) Descriptor() ([]byte, []int) {
	return file_api_ccl_proto_rawDescGZIP(), []int{0}
}

func (x *SecurityConfig) GetColumnControlList() []*SecurityConfig_ColumnControl {
	if x != nil {
		return x.ColumnControlList
	}
	return nil
}

type SecurityConfig_ColumnControl struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	PartyCode    string                                  `protobuf:"bytes,1,opt,name=party_code,json=partyCode,proto3" json:"party_code,omitempty"`
	Visibility   SecurityConfig_ColumnControl_Visibility `protobuf:"varint,2,opt,name=visibility,proto3,enum=scql.pb.SecurityConfig_ColumnControl_Visibility" json:"visibility,omitempty"`
	DatabaseName string                                  `protobuf:"bytes,3,opt,name=database_name,json=databaseName,proto3" json:"database_name,omitempty"`
	TableName    string                                  `protobuf:"bytes,4,opt,name=table_name,json=tableName,proto3" json:"table_name,omitempty"`
	ColumnName   string                                  `protobuf:"bytes,5,opt,name=column_name,json=columnName,proto3" json:"column_name,omitempty"`
}

func (x *SecurityConfig_ColumnControl) Reset() {
	*x = SecurityConfig_ColumnControl{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_ccl_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SecurityConfig_ColumnControl) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SecurityConfig_ColumnControl) ProtoMessage() {}

func (x *SecurityConfig_ColumnControl) ProtoReflect() protoreflect.Message {
	mi := &file_api_ccl_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SecurityConfig_ColumnControl.ProtoReflect.Descriptor instead.
func (*SecurityConfig_ColumnControl) Descriptor() ([]byte, []int) {
	return file_api_ccl_proto_rawDescGZIP(), []int{0, 0}
}

func (x *SecurityConfig_ColumnControl) GetPartyCode() string {
	if x != nil {
		return x.PartyCode
	}
	return ""
}

func (x *SecurityConfig_ColumnControl) GetVisibility() SecurityConfig_ColumnControl_Visibility {
	if x != nil {
		return x.Visibility
	}
	return SecurityConfig_ColumnControl_UNKNOWN
}

func (x *SecurityConfig_ColumnControl) GetDatabaseName() string {
	if x != nil {
		return x.DatabaseName
	}
	return ""
}

func (x *SecurityConfig_ColumnControl) GetTableName() string {
	if x != nil {
		return x.TableName
	}
	return ""
}

func (x *SecurityConfig_ColumnControl) GetColumnName() string {
	if x != nil {
		return x.ColumnName
	}
	return ""
}

var File_api_ccl_proto protoreflect.FileDescriptor

var file_api_ccl_proto_rawDesc = []byte{
	0x0a, 0x0d, 0x61, 0x70, 0x69, 0x2f, 0x63, 0x63, 0x6c, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12,
	0x07, 0x73, 0x63, 0x71, 0x6c, 0x2e, 0x70, 0x62, 0x22, 0x82, 0x04, 0x0a, 0x0e, 0x53, 0x65, 0x63,
	0x75, 0x72, 0x69, 0x74, 0x79, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x12, 0x55, 0x0a, 0x13, 0x63,
	0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x5f, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x5f, 0x6c, 0x69,
	0x73, 0x74, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x25, 0x2e, 0x73, 0x63, 0x71, 0x6c, 0x2e,
	0x70, 0x62, 0x2e, 0x53, 0x65, 0x63, 0x75, 0x72, 0x69, 0x74, 0x79, 0x43, 0x6f, 0x6e, 0x66, 0x69,
	0x67, 0x2e, 0x43, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x43, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x52,
	0x11, 0x63, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x43, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x4c, 0x69,
	0x73, 0x74, 0x1a, 0x98, 0x03, 0x0a, 0x0d, 0x43, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x43, 0x6f, 0x6e,
	0x74, 0x72, 0x6f, 0x6c, 0x12, 0x1d, 0x0a, 0x0a, 0x70, 0x61, 0x72, 0x74, 0x79, 0x5f, 0x63, 0x6f,
	0x64, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x70, 0x61, 0x72, 0x74, 0x79, 0x43,
	0x6f, 0x64, 0x65, 0x12, 0x50, 0x0a, 0x0a, 0x76, 0x69, 0x73, 0x69, 0x62, 0x69, 0x6c, 0x69, 0x74,
	0x79, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x30, 0x2e, 0x73, 0x63, 0x71, 0x6c, 0x2e, 0x70,
	0x62, 0x2e, 0x53, 0x65, 0x63, 0x75, 0x72, 0x69, 0x74, 0x79, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67,
	0x2e, 0x43, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x43, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x2e, 0x56,
	0x69, 0x73, 0x69, 0x62, 0x69, 0x6c, 0x69, 0x74, 0x79, 0x52, 0x0a, 0x76, 0x69, 0x73, 0x69, 0x62,
	0x69, 0x6c, 0x69, 0x74, 0x79, 0x12, 0x23, 0x0a, 0x0d, 0x64, 0x61, 0x74, 0x61, 0x62, 0x61, 0x73,
	0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0c, 0x64, 0x61,
	0x74, 0x61, 0x62, 0x61, 0x73, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x1d, 0x0a, 0x0a, 0x74, 0x61,
	0x62, 0x6c, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09,
	0x74, 0x61, 0x62, 0x6c, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x1f, 0x0a, 0x0b, 0x63, 0x6f, 0x6c,
	0x75, 0x6d, 0x6e, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x05, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0a,
	0x63, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x4e, 0x61, 0x6d, 0x65, 0x22, 0xb0, 0x01, 0x0a, 0x0a, 0x56,
	0x69, 0x73, 0x69, 0x62, 0x69, 0x6c, 0x69, 0x74, 0x79, 0x12, 0x0b, 0x0a, 0x07, 0x55, 0x4e, 0x4b,
	0x4e, 0x4f, 0x57, 0x4e, 0x10, 0x00, 0x12, 0x0d, 0x0a, 0x09, 0x50, 0x4c, 0x41, 0x49, 0x4e, 0x54,
	0x45, 0x58, 0x54, 0x10, 0x01, 0x12, 0x12, 0x0a, 0x0e, 0x45, 0x4e, 0x43, 0x52, 0x59, 0x50, 0x54,
	0x45, 0x44, 0x5f, 0x4f, 0x4e, 0x4c, 0x59, 0x10, 0x02, 0x12, 0x18, 0x0a, 0x14, 0x50, 0x4c, 0x41,
	0x49, 0x4e, 0x54, 0x45, 0x58, 0x54, 0x5f, 0x41, 0x46, 0x54, 0x45, 0x52, 0x5f, 0x4a, 0x4f, 0x49,
	0x4e, 0x10, 0x03, 0x12, 0x1c, 0x0a, 0x18, 0x50, 0x4c, 0x41, 0x49, 0x4e, 0x54, 0x45, 0x58, 0x54,
	0x5f, 0x41, 0x46, 0x54, 0x45, 0x52, 0x5f, 0x47, 0x52, 0x4f, 0x55, 0x50, 0x5f, 0x42, 0x59, 0x10,
	0x04, 0x12, 0x1b, 0x0a, 0x17, 0x50, 0x4c, 0x41, 0x49, 0x4e, 0x54, 0x45, 0x58, 0x54, 0x5f, 0x41,
	0x46, 0x54, 0x45, 0x52, 0x5f, 0x43, 0x4f, 0x4d, 0x50, 0x41, 0x52, 0x45, 0x10, 0x05, 0x12, 0x1d,
	0x0a, 0x19, 0x50, 0x4c, 0x41, 0x49, 0x4e, 0x54, 0x45, 0x58, 0x54, 0x5f, 0x41, 0x46, 0x54, 0x45,
	0x52, 0x5f, 0x41, 0x47, 0x47, 0x52, 0x45, 0x47, 0x41, 0x54, 0x45, 0x10, 0x06, 0x42, 0x10, 0x5a,
	0x0e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2d, 0x67, 0x65, 0x6e, 0x2f, 0x73, 0x63, 0x71, 0x6c, 0x62,
	0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_api_ccl_proto_rawDescOnce sync.Once
	file_api_ccl_proto_rawDescData = file_api_ccl_proto_rawDesc
)

func file_api_ccl_proto_rawDescGZIP() []byte {
	file_api_ccl_proto_rawDescOnce.Do(func() {
		file_api_ccl_proto_rawDescData = protoimpl.X.CompressGZIP(file_api_ccl_proto_rawDescData)
	})
	return file_api_ccl_proto_rawDescData
}

var file_api_ccl_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_api_ccl_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_api_ccl_proto_goTypes = []interface{}{
	(SecurityConfig_ColumnControl_Visibility)(0), // 0: scql.pb.SecurityConfig.ColumnControl.Visibility
	(*SecurityConfig)(nil),                       // 1: scql.pb.SecurityConfig
	(*SecurityConfig_ColumnControl)(nil),         // 2: scql.pb.SecurityConfig.ColumnControl
}
var file_api_ccl_proto_depIdxs = []int32{
	2, // 0: scql.pb.SecurityConfig.column_control_list:type_name -> scql.pb.SecurityConfig.ColumnControl
	0, // 1: scql.pb.SecurityConfig.ColumnControl.visibility:type_name -> scql.pb.SecurityConfig.ColumnControl.Visibility
	2, // [2:2] is the sub-list for method output_type
	2, // [2:2] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_api_ccl_proto_init() }
func file_api_ccl_proto_init() {
	if File_api_ccl_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_api_ccl_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SecurityConfig); i {
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
		file_api_ccl_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SecurityConfig_ColumnControl); i {
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
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_api_ccl_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_api_ccl_proto_goTypes,
		DependencyIndexes: file_api_ccl_proto_depIdxs,
		EnumInfos:         file_api_ccl_proto_enumTypes,
		MessageInfos:      file_api_ccl_proto_msgTypes,
	}.Build()
	File_api_ccl_proto = out.File
	file_api_ccl_proto_rawDesc = nil
	file_api_ccl_proto_goTypes = nil
	file_api_ccl_proto_depIdxs = nil
}
