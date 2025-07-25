//*
// Meta

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.2
// 	protoc        v5.29.2
// source: meta.proto

package proto

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

// *
// SegmentState defines the state of a log segment.
type SegmentState int32

const (
	SegmentState_Active    SegmentState = 0
	SegmentState_Completed SegmentState = 2
	SegmentState_Sealed    SegmentState = 3
	SegmentState_Truncated SegmentState = 4
)

// Enum value maps for SegmentState.
var (
	SegmentState_name = map[int32]string{
		0: "Active",
		2: "Completed",
		3: "Sealed",
		4: "Truncated",
	}
	SegmentState_value = map[string]int32{
		"Active":    0,
		"Completed": 2,
		"Sealed":    3,
		"Truncated": 4,
	}
)

func (x SegmentState) Enum() *SegmentState {
	p := new(SegmentState)
	*p = x
	return p
}

func (x SegmentState) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (SegmentState) Descriptor() protoreflect.EnumDescriptor {
	return file_meta_proto_enumTypes[0].Descriptor()
}

func (SegmentState) Type() protoreflect.EnumType {
	return &file_meta_proto_enumTypes[0]
}

func (x SegmentState) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use SegmentState.Descriptor instead.
func (SegmentState) EnumDescriptor() ([]byte, []int) {
	return file_meta_proto_rawDescGZIP(), []int{0}
}

type SegmentCleanupState int32

const (
	SegmentCleanupState_CLEANUP_IN_PROGRESS SegmentCleanupState = 0
	SegmentCleanupState_CLEANUP_COMPLETED   SegmentCleanupState = 1
	SegmentCleanupState_CLEANUP_FAILED      SegmentCleanupState = 2
)

// Enum value maps for SegmentCleanupState.
var (
	SegmentCleanupState_name = map[int32]string{
		0: "CLEANUP_IN_PROGRESS",
		1: "CLEANUP_COMPLETED",
		2: "CLEANUP_FAILED",
	}
	SegmentCleanupState_value = map[string]int32{
		"CLEANUP_IN_PROGRESS": 0,
		"CLEANUP_COMPLETED":   1,
		"CLEANUP_FAILED":      2,
	}
)

func (x SegmentCleanupState) Enum() *SegmentCleanupState {
	p := new(SegmentCleanupState)
	*p = x
	return p
}

func (x SegmentCleanupState) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (SegmentCleanupState) Descriptor() protoreflect.EnumDescriptor {
	return file_meta_proto_enumTypes[1].Descriptor()
}

func (SegmentCleanupState) Type() protoreflect.EnumType {
	return &file_meta_proto_enumTypes[1]
}

func (x SegmentCleanupState) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use SegmentCleanupState.Descriptor instead.
func (SegmentCleanupState) EnumDescriptor() ([]byte, []int) {
	return file_meta_proto_rawDescGZIP(), []int{1}
}

// *
// Unbounded Log meta info.
type LogMeta struct {
	state                     protoimpl.MessageState `protogen:"open.v1"`
	LogId                     int64                  `protobuf:"varint,1,opt,name=logId,proto3" json:"logId,omitempty"`
	MaxSegmentRollTimeSeconds int64                  `protobuf:"varint,2,opt,name=max_segment_roll_time_seconds,json=maxSegmentRollTimeSeconds,proto3" json:"max_segment_roll_time_seconds,omitempty"`
	MaxSegmentRollSizeBytes   int64                  `protobuf:"varint,3,opt,name=max_segment_roll_size_bytes,json=maxSegmentRollSizeBytes,proto3" json:"max_segment_roll_size_bytes,omitempty"`
	CompactionBufferSizeBytes int64                  `protobuf:"varint,4,opt,name=compaction_buffer_size_bytes,json=compactionBufferSizeBytes,proto3" json:"compaction_buffer_size_bytes,omitempty"`
	MaxCompactionFileCount    int64                  `protobuf:"varint,5,opt,name=max_compaction_file_count,json=maxCompactionFileCount,proto3" json:"max_compaction_file_count,omitempty"`
	CreationTimestamp         uint64                 `protobuf:"fixed64,6,opt,name=creation_timestamp,json=creationTimestamp,proto3" json:"creation_timestamp,omitempty"`
	ModificationTimestamp     uint64                 `protobuf:"fixed64,7,opt,name=modification_timestamp,json=modificationTimestamp,proto3" json:"modification_timestamp,omitempty"`
	// The truncated position of the log (inclusive)
	TruncatedSegmentId int64 `protobuf:"varint,8,opt,name=truncated_segment_id,json=truncatedSegmentId,proto3" json:"truncated_segment_id,omitempty"`
	TruncatedEntryId   int64 `protobuf:"varint,9,opt,name=truncated_entry_id,json=truncatedEntryId,proto3" json:"truncated_entry_id,omitempty"`
	unknownFields      protoimpl.UnknownFields
	sizeCache          protoimpl.SizeCache
}

func (x *LogMeta) Reset() {
	*x = LogMeta{}
	mi := &file_meta_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *LogMeta) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LogMeta) ProtoMessage() {}

func (x *LogMeta) ProtoReflect() protoreflect.Message {
	mi := &file_meta_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use LogMeta.ProtoReflect.Descriptor instead.
func (*LogMeta) Descriptor() ([]byte, []int) {
	return file_meta_proto_rawDescGZIP(), []int{0}
}

func (x *LogMeta) GetLogId() int64 {
	if x != nil {
		return x.LogId
	}
	return 0
}

func (x *LogMeta) GetMaxSegmentRollTimeSeconds() int64 {
	if x != nil {
		return x.MaxSegmentRollTimeSeconds
	}
	return 0
}

func (x *LogMeta) GetMaxSegmentRollSizeBytes() int64 {
	if x != nil {
		return x.MaxSegmentRollSizeBytes
	}
	return 0
}

func (x *LogMeta) GetCompactionBufferSizeBytes() int64 {
	if x != nil {
		return x.CompactionBufferSizeBytes
	}
	return 0
}

func (x *LogMeta) GetMaxCompactionFileCount() int64 {
	if x != nil {
		return x.MaxCompactionFileCount
	}
	return 0
}

func (x *LogMeta) GetCreationTimestamp() uint64 {
	if x != nil {
		return x.CreationTimestamp
	}
	return 0
}

func (x *LogMeta) GetModificationTimestamp() uint64 {
	if x != nil {
		return x.ModificationTimestamp
	}
	return 0
}

func (x *LogMeta) GetTruncatedSegmentId() int64 {
	if x != nil {
		return x.TruncatedSegmentId
	}
	return 0
}

func (x *LogMeta) GetTruncatedEntryId() int64 {
	if x != nil {
		return x.TruncatedEntryId
	}
	return 0
}

// *
// SegmentMetadata defines a log segment meta info.
type SegmentMetadata struct {
	state          protoimpl.MessageState `protogen:"open.v1"`
	SegNo          int64                  `protobuf:"varint,1,opt,name=segNo,proto3" json:"segNo,omitempty"`
	CreateTime     int64                  `protobuf:"varint,2,opt,name=createTime,proto3" json:"createTime,omitempty"`
	QuorumId       int64                  `protobuf:"varint,3,opt,name=quorumId,proto3" json:"quorumId,omitempty"`
	State          SegmentState           `protobuf:"varint,4,opt,name=state,proto3,enum=woodpecker.proto.meta.SegmentState" json:"state,omitempty"`
	CompletionTime int64                  `protobuf:"varint,5,opt,name=completionTime,proto3" json:"completionTime,omitempty"`
	LastEntryId    int64                  `protobuf:"varint,6,opt,name=lastEntryId,proto3" json:"lastEntryId,omitempty"`
	Size           int64                  `protobuf:"varint,7,opt,name=size,proto3" json:"size,omitempty"`
	SealedTime     int64                  `protobuf:"varint,8,opt,name=sealedTime,proto3" json:"sealedTime,omitempty"`
	unknownFields  protoimpl.UnknownFields
	sizeCache      protoimpl.SizeCache
}

func (x *SegmentMetadata) Reset() {
	*x = SegmentMetadata{}
	mi := &file_meta_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *SegmentMetadata) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SegmentMetadata) ProtoMessage() {}

func (x *SegmentMetadata) ProtoReflect() protoreflect.Message {
	mi := &file_meta_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SegmentMetadata.ProtoReflect.Descriptor instead.
func (*SegmentMetadata) Descriptor() ([]byte, []int) {
	return file_meta_proto_rawDescGZIP(), []int{1}
}

func (x *SegmentMetadata) GetSegNo() int64 {
	if x != nil {
		return x.SegNo
	}
	return 0
}

func (x *SegmentMetadata) GetCreateTime() int64 {
	if x != nil {
		return x.CreateTime
	}
	return 0
}

func (x *SegmentMetadata) GetQuorumId() int64 {
	if x != nil {
		return x.QuorumId
	}
	return 0
}

func (x *SegmentMetadata) GetState() SegmentState {
	if x != nil {
		return x.State
	}
	return SegmentState_Active
}

func (x *SegmentMetadata) GetCompletionTime() int64 {
	if x != nil {
		return x.CompletionTime
	}
	return 0
}

func (x *SegmentMetadata) GetLastEntryId() int64 {
	if x != nil {
		return x.LastEntryId
	}
	return 0
}

func (x *SegmentMetadata) GetSize() int64 {
	if x != nil {
		return x.Size
	}
	return 0
}

func (x *SegmentMetadata) GetSealedTime() int64 {
	if x != nil {
		return x.SealedTime
	}
	return 0
}

// *
// Version defines the version of meta.
type Version struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Major         int32                  `protobuf:"varint,1,opt,name=major,proto3" json:"major,omitempty"`
	Minor         int32                  `protobuf:"varint,2,opt,name=minor,proto3" json:"minor,omitempty"`
	Patch         int32                  `protobuf:"varint,3,opt,name=patch,proto3" json:"patch,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Version) Reset() {
	*x = Version{}
	mi := &file_meta_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Version) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Version) ProtoMessage() {}

func (x *Version) ProtoReflect() protoreflect.Message {
	mi := &file_meta_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Version.ProtoReflect.Descriptor instead.
func (*Version) Descriptor() ([]byte, []int) {
	return file_meta_proto_rawDescGZIP(), []int{2}
}

func (x *Version) GetMajor() int32 {
	if x != nil {
		return x.Major
	}
	return 0
}

func (x *Version) GetMinor() int32 {
	if x != nil {
		return x.Minor
	}
	return 0
}

func (x *Version) GetPatch() int32 {
	if x != nil {
		return x.Patch
	}
	return 0
}

// *
// Quorum defines a quorum information.
type QuorumInfo struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Id            int64                  `protobuf:"varint,1,opt,name=id,proto3" json:"id,omitempty"`
	Es            int32                  `protobuf:"varint,2,opt,name=es,proto3" json:"es,omitempty"`
	Wq            int32                  `protobuf:"varint,3,opt,name=wq,proto3" json:"wq,omitempty"`
	Aq            int32                  `protobuf:"varint,4,opt,name=aq,proto3" json:"aq,omitempty"`
	Nodes         []string               `protobuf:"bytes,5,rep,name=nodes,proto3" json:"nodes,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *QuorumInfo) Reset() {
	*x = QuorumInfo{}
	mi := &file_meta_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *QuorumInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*QuorumInfo) ProtoMessage() {}

func (x *QuorumInfo) ProtoReflect() protoreflect.Message {
	mi := &file_meta_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use QuorumInfo.ProtoReflect.Descriptor instead.
func (*QuorumInfo) Descriptor() ([]byte, []int) {
	return file_meta_proto_rawDescGZIP(), []int{3}
}

func (x *QuorumInfo) GetId() int64 {
	if x != nil {
		return x.Id
	}
	return 0
}

func (x *QuorumInfo) GetEs() int32 {
	if x != nil {
		return x.Es
	}
	return 0
}

func (x *QuorumInfo) GetWq() int32 {
	if x != nil {
		return x.Wq
	}
	return 0
}

func (x *QuorumInfo) GetAq() int32 {
	if x != nil {
		return x.Aq
	}
	return 0
}

func (x *QuorumInfo) GetNodes() []string {
	if x != nil {
		return x.Nodes
	}
	return nil
}

// *
// ReaderTempInfo defines temporary information for a log reader.
type ReaderTempInfo struct {
	state               protoimpl.MessageState `protogen:"open.v1"`
	ReaderName          string                 `protobuf:"bytes,1,opt,name=reader_name,json=readerName,proto3" json:"reader_name,omitempty"`
	OpenTimestamp       uint64                 `protobuf:"fixed64,2,opt,name=open_timestamp,json=openTimestamp,proto3" json:"open_timestamp,omitempty"`
	LogId               int64                  `protobuf:"varint,3,opt,name=log_id,json=logId,proto3" json:"log_id,omitempty"`
	OpenSegmentId       int64                  `protobuf:"varint,4,opt,name=open_segment_id,json=openSegmentId,proto3" json:"open_segment_id,omitempty"`
	OpenEntryId         int64                  `protobuf:"varint,5,opt,name=open_entry_id,json=openEntryId,proto3" json:"open_entry_id,omitempty"`
	RecentReadSegmentId int64                  `protobuf:"varint,6,opt,name=recent_read_segment_id,json=recentReadSegmentId,proto3" json:"recent_read_segment_id,omitempty"`
	RecentReadEntryId   int64                  `protobuf:"varint,7,opt,name=recent_read_entry_id,json=recentReadEntryId,proto3" json:"recent_read_entry_id,omitempty"`
	RecentReadTimestamp uint64                 `protobuf:"fixed64,8,opt,name=recent_read_timestamp,json=recentReadTimestamp,proto3" json:"recent_read_timestamp,omitempty"`
	unknownFields       protoimpl.UnknownFields
	sizeCache           protoimpl.SizeCache
}

func (x *ReaderTempInfo) Reset() {
	*x = ReaderTempInfo{}
	mi := &file_meta_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ReaderTempInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ReaderTempInfo) ProtoMessage() {}

func (x *ReaderTempInfo) ProtoReflect() protoreflect.Message {
	mi := &file_meta_proto_msgTypes[4]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ReaderTempInfo.ProtoReflect.Descriptor instead.
func (*ReaderTempInfo) Descriptor() ([]byte, []int) {
	return file_meta_proto_rawDescGZIP(), []int{4}
}

func (x *ReaderTempInfo) GetReaderName() string {
	if x != nil {
		return x.ReaderName
	}
	return ""
}

func (x *ReaderTempInfo) GetOpenTimestamp() uint64 {
	if x != nil {
		return x.OpenTimestamp
	}
	return 0
}

func (x *ReaderTempInfo) GetLogId() int64 {
	if x != nil {
		return x.LogId
	}
	return 0
}

func (x *ReaderTempInfo) GetOpenSegmentId() int64 {
	if x != nil {
		return x.OpenSegmentId
	}
	return 0
}

func (x *ReaderTempInfo) GetOpenEntryId() int64 {
	if x != nil {
		return x.OpenEntryId
	}
	return 0
}

func (x *ReaderTempInfo) GetRecentReadSegmentId() int64 {
	if x != nil {
		return x.RecentReadSegmentId
	}
	return 0
}

func (x *ReaderTempInfo) GetRecentReadEntryId() int64 {
	if x != nil {
		return x.RecentReadEntryId
	}
	return 0
}

func (x *ReaderTempInfo) GetRecentReadTimestamp() uint64 {
	if x != nil {
		return x.RecentReadTimestamp
	}
	return 0
}

type SegmentCleanupStatus struct {
	state               protoimpl.MessageState `protogen:"open.v1"`
	LogId               int64                  `protobuf:"varint,1,opt,name=log_id,json=logId,proto3" json:"log_id,omitempty"`
	SegmentId           int64                  `protobuf:"varint,2,opt,name=segment_id,json=segmentId,proto3" json:"segment_id,omitempty"`
	State               SegmentCleanupState    `protobuf:"varint,3,opt,name=state,proto3,enum=woodpecker.proto.meta.SegmentCleanupState" json:"state,omitempty"`
	StartTime           uint64                 `protobuf:"varint,4,opt,name=start_time,json=startTime,proto3" json:"start_time,omitempty"`                                                                                                           // Unix timestamp in milliseconds when cleanup started
	LastUpdateTime      uint64                 `protobuf:"varint,5,opt,name=last_update_time,json=lastUpdateTime,proto3" json:"last_update_time,omitempty"`                                                                                          // Unix timestamp in milliseconds of the last update
	QuorumCleanupStatus map[string]bool        `protobuf:"bytes,6,rep,name=quorum_cleanup_status,json=quorumCleanupStatus,proto3" json:"quorum_cleanup_status,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"varint,2,opt,name=value"` // Map of node address to cleanup status
	ErrorMessage        string                 `protobuf:"bytes,7,opt,name=error_message,json=errorMessage,proto3" json:"error_message,omitempty"`                                                                                                   // Error message if cleanup failed
	unknownFields       protoimpl.UnknownFields
	sizeCache           protoimpl.SizeCache
}

func (x *SegmentCleanupStatus) Reset() {
	*x = SegmentCleanupStatus{}
	mi := &file_meta_proto_msgTypes[5]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *SegmentCleanupStatus) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SegmentCleanupStatus) ProtoMessage() {}

func (x *SegmentCleanupStatus) ProtoReflect() protoreflect.Message {
	mi := &file_meta_proto_msgTypes[5]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SegmentCleanupStatus.ProtoReflect.Descriptor instead.
func (*SegmentCleanupStatus) Descriptor() ([]byte, []int) {
	return file_meta_proto_rawDescGZIP(), []int{5}
}

func (x *SegmentCleanupStatus) GetLogId() int64 {
	if x != nil {
		return x.LogId
	}
	return 0
}

func (x *SegmentCleanupStatus) GetSegmentId() int64 {
	if x != nil {
		return x.SegmentId
	}
	return 0
}

func (x *SegmentCleanupStatus) GetState() SegmentCleanupState {
	if x != nil {
		return x.State
	}
	return SegmentCleanupState_CLEANUP_IN_PROGRESS
}

func (x *SegmentCleanupStatus) GetStartTime() uint64 {
	if x != nil {
		return x.StartTime
	}
	return 0
}

func (x *SegmentCleanupStatus) GetLastUpdateTime() uint64 {
	if x != nil {
		return x.LastUpdateTime
	}
	return 0
}

func (x *SegmentCleanupStatus) GetQuorumCleanupStatus() map[string]bool {
	if x != nil {
		return x.QuorumCleanupStatus
	}
	return nil
}

func (x *SegmentCleanupStatus) GetErrorMessage() string {
	if x != nil {
		return x.ErrorMessage
	}
	return ""
}

var File_meta_proto protoreflect.FileDescriptor

var file_meta_proto_rawDesc = []byte{
	0x0a, 0x0a, 0x6d, 0x65, 0x74, 0x61, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x15, 0x77, 0x6f,
	0x6f, 0x64, 0x70, 0x65, 0x63, 0x6b, 0x65, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x6d,
	0x65, 0x74, 0x61, 0x22, 0xe1, 0x03, 0x0a, 0x07, 0x4c, 0x6f, 0x67, 0x4d, 0x65, 0x74, 0x61, 0x12,
	0x14, 0x0a, 0x05, 0x6c, 0x6f, 0x67, 0x49, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x05,
	0x6c, 0x6f, 0x67, 0x49, 0x64, 0x12, 0x40, 0x0a, 0x1d, 0x6d, 0x61, 0x78, 0x5f, 0x73, 0x65, 0x67,
	0x6d, 0x65, 0x6e, 0x74, 0x5f, 0x72, 0x6f, 0x6c, 0x6c, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x5f, 0x73,
	0x65, 0x63, 0x6f, 0x6e, 0x64, 0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x03, 0x52, 0x19, 0x6d, 0x61,
	0x78, 0x53, 0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x52, 0x6f, 0x6c, 0x6c, 0x54, 0x69, 0x6d, 0x65,
	0x53, 0x65, 0x63, 0x6f, 0x6e, 0x64, 0x73, 0x12, 0x3c, 0x0a, 0x1b, 0x6d, 0x61, 0x78, 0x5f, 0x73,
	0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x5f, 0x72, 0x6f, 0x6c, 0x6c, 0x5f, 0x73, 0x69, 0x7a, 0x65,
	0x5f, 0x62, 0x79, 0x74, 0x65, 0x73, 0x18, 0x03, 0x20, 0x01, 0x28, 0x03, 0x52, 0x17, 0x6d, 0x61,
	0x78, 0x53, 0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x52, 0x6f, 0x6c, 0x6c, 0x53, 0x69, 0x7a, 0x65,
	0x42, 0x79, 0x74, 0x65, 0x73, 0x12, 0x3f, 0x0a, 0x1c, 0x63, 0x6f, 0x6d, 0x70, 0x61, 0x63, 0x74,
	0x69, 0x6f, 0x6e, 0x5f, 0x62, 0x75, 0x66, 0x66, 0x65, 0x72, 0x5f, 0x73, 0x69, 0x7a, 0x65, 0x5f,
	0x62, 0x79, 0x74, 0x65, 0x73, 0x18, 0x04, 0x20, 0x01, 0x28, 0x03, 0x52, 0x19, 0x63, 0x6f, 0x6d,
	0x70, 0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x42, 0x75, 0x66, 0x66, 0x65, 0x72, 0x53, 0x69, 0x7a,
	0x65, 0x42, 0x79, 0x74, 0x65, 0x73, 0x12, 0x39, 0x0a, 0x19, 0x6d, 0x61, 0x78, 0x5f, 0x63, 0x6f,
	0x6d, 0x70, 0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x66, 0x69, 0x6c, 0x65, 0x5f, 0x63, 0x6f,
	0x75, 0x6e, 0x74, 0x18, 0x05, 0x20, 0x01, 0x28, 0x03, 0x52, 0x16, 0x6d, 0x61, 0x78, 0x43, 0x6f,
	0x6d, 0x70, 0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x46, 0x69, 0x6c, 0x65, 0x43, 0x6f, 0x75, 0x6e,
	0x74, 0x12, 0x2d, 0x0a, 0x12, 0x63, 0x72, 0x65, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x74, 0x69,
	0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18, 0x06, 0x20, 0x01, 0x28, 0x06, 0x52, 0x11, 0x63,
	0x72, 0x65, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70,
	0x12, 0x35, 0x0a, 0x16, 0x6d, 0x6f, 0x64, 0x69, 0x66, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e,
	0x5f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18, 0x07, 0x20, 0x01, 0x28, 0x06,
	0x52, 0x15, 0x6d, 0x6f, 0x64, 0x69, 0x66, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x54, 0x69,
	0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x12, 0x30, 0x0a, 0x14, 0x74, 0x72, 0x75, 0x6e, 0x63,
	0x61, 0x74, 0x65, 0x64, 0x5f, 0x73, 0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x5f, 0x69, 0x64, 0x18,
	0x08, 0x20, 0x01, 0x28, 0x03, 0x52, 0x12, 0x74, 0x72, 0x75, 0x6e, 0x63, 0x61, 0x74, 0x65, 0x64,
	0x53, 0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x49, 0x64, 0x12, 0x2c, 0x0a, 0x12, 0x74, 0x72, 0x75,
	0x6e, 0x63, 0x61, 0x74, 0x65, 0x64, 0x5f, 0x65, 0x6e, 0x74, 0x72, 0x79, 0x5f, 0x69, 0x64, 0x18,
	0x09, 0x20, 0x01, 0x28, 0x03, 0x52, 0x10, 0x74, 0x72, 0x75, 0x6e, 0x63, 0x61, 0x74, 0x65, 0x64,
	0x45, 0x6e, 0x74, 0x72, 0x79, 0x49, 0x64, 0x22, 0x9c, 0x02, 0x0a, 0x0f, 0x53, 0x65, 0x67, 0x6d,
	0x65, 0x6e, 0x74, 0x4d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x12, 0x14, 0x0a, 0x05, 0x73,
	0x65, 0x67, 0x4e, 0x6f, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x05, 0x73, 0x65, 0x67, 0x4e,
	0x6f, 0x12, 0x1e, 0x0a, 0x0a, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x54, 0x69, 0x6d, 0x65, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0a, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x54, 0x69, 0x6d,
	0x65, 0x12, 0x1a, 0x0a, 0x08, 0x71, 0x75, 0x6f, 0x72, 0x75, 0x6d, 0x49, 0x64, 0x18, 0x03, 0x20,
	0x01, 0x28, 0x03, 0x52, 0x08, 0x71, 0x75, 0x6f, 0x72, 0x75, 0x6d, 0x49, 0x64, 0x12, 0x39, 0x0a,
	0x05, 0x73, 0x74, 0x61, 0x74, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x23, 0x2e, 0x77,
	0x6f, 0x6f, 0x64, 0x70, 0x65, 0x63, 0x6b, 0x65, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e,
	0x6d, 0x65, 0x74, 0x61, 0x2e, 0x53, 0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x53, 0x74, 0x61, 0x74,
	0x65, 0x52, 0x05, 0x73, 0x74, 0x61, 0x74, 0x65, 0x12, 0x26, 0x0a, 0x0e, 0x63, 0x6f, 0x6d, 0x70,
	0x6c, 0x65, 0x74, 0x69, 0x6f, 0x6e, 0x54, 0x69, 0x6d, 0x65, 0x18, 0x05, 0x20, 0x01, 0x28, 0x03,
	0x52, 0x0e, 0x63, 0x6f, 0x6d, 0x70, 0x6c, 0x65, 0x74, 0x69, 0x6f, 0x6e, 0x54, 0x69, 0x6d, 0x65,
	0x12, 0x20, 0x0a, 0x0b, 0x6c, 0x61, 0x73, 0x74, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x49, 0x64, 0x18,
	0x06, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0b, 0x6c, 0x61, 0x73, 0x74, 0x45, 0x6e, 0x74, 0x72, 0x79,
	0x49, 0x64, 0x12, 0x12, 0x0a, 0x04, 0x73, 0x69, 0x7a, 0x65, 0x18, 0x07, 0x20, 0x01, 0x28, 0x03,
	0x52, 0x04, 0x73, 0x69, 0x7a, 0x65, 0x12, 0x1e, 0x0a, 0x0a, 0x73, 0x65, 0x61, 0x6c, 0x65, 0x64,
	0x54, 0x69, 0x6d, 0x65, 0x18, 0x08, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0a, 0x73, 0x65, 0x61, 0x6c,
	0x65, 0x64, 0x54, 0x69, 0x6d, 0x65, 0x22, 0x4b, 0x0a, 0x07, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f,
	0x6e, 0x12, 0x14, 0x0a, 0x05, 0x6d, 0x61, 0x6a, 0x6f, 0x72, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05,
	0x52, 0x05, 0x6d, 0x61, 0x6a, 0x6f, 0x72, 0x12, 0x14, 0x0a, 0x05, 0x6d, 0x69, 0x6e, 0x6f, 0x72,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x05, 0x52, 0x05, 0x6d, 0x69, 0x6e, 0x6f, 0x72, 0x12, 0x14, 0x0a,
	0x05, 0x70, 0x61, 0x74, 0x63, 0x68, 0x18, 0x03, 0x20, 0x01, 0x28, 0x05, 0x52, 0x05, 0x70, 0x61,
	0x74, 0x63, 0x68, 0x22, 0x62, 0x0a, 0x0a, 0x51, 0x75, 0x6f, 0x72, 0x75, 0x6d, 0x49, 0x6e, 0x66,
	0x6f, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x02, 0x69,
	0x64, 0x12, 0x0e, 0x0a, 0x02, 0x65, 0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x05, 0x52, 0x02, 0x65,
	0x73, 0x12, 0x0e, 0x0a, 0x02, 0x77, 0x71, 0x18, 0x03, 0x20, 0x01, 0x28, 0x05, 0x52, 0x02, 0x77,
	0x71, 0x12, 0x0e, 0x0a, 0x02, 0x61, 0x71, 0x18, 0x04, 0x20, 0x01, 0x28, 0x05, 0x52, 0x02, 0x61,
	0x71, 0x12, 0x14, 0x0a, 0x05, 0x6e, 0x6f, 0x64, 0x65, 0x73, 0x18, 0x05, 0x20, 0x03, 0x28, 0x09,
	0x52, 0x05, 0x6e, 0x6f, 0x64, 0x65, 0x73, 0x22, 0xd5, 0x02, 0x0a, 0x0e, 0x52, 0x65, 0x61, 0x64,
	0x65, 0x72, 0x54, 0x65, 0x6d, 0x70, 0x49, 0x6e, 0x66, 0x6f, 0x12, 0x1f, 0x0a, 0x0b, 0x72, 0x65,
	0x61, 0x64, 0x65, 0x72, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x0a, 0x72, 0x65, 0x61, 0x64, 0x65, 0x72, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x25, 0x0a, 0x0e, 0x6f,
	0x70, 0x65, 0x6e, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x06, 0x52, 0x0d, 0x6f, 0x70, 0x65, 0x6e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61,
	0x6d, 0x70, 0x12, 0x15, 0x0a, 0x06, 0x6c, 0x6f, 0x67, 0x5f, 0x69, 0x64, 0x18, 0x03, 0x20, 0x01,
	0x28, 0x03, 0x52, 0x05, 0x6c, 0x6f, 0x67, 0x49, 0x64, 0x12, 0x26, 0x0a, 0x0f, 0x6f, 0x70, 0x65,
	0x6e, 0x5f, 0x73, 0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x5f, 0x69, 0x64, 0x18, 0x04, 0x20, 0x01,
	0x28, 0x03, 0x52, 0x0d, 0x6f, 0x70, 0x65, 0x6e, 0x53, 0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x49,
	0x64, 0x12, 0x22, 0x0a, 0x0d, 0x6f, 0x70, 0x65, 0x6e, 0x5f, 0x65, 0x6e, 0x74, 0x72, 0x79, 0x5f,
	0x69, 0x64, 0x18, 0x05, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0b, 0x6f, 0x70, 0x65, 0x6e, 0x45, 0x6e,
	0x74, 0x72, 0x79, 0x49, 0x64, 0x12, 0x33, 0x0a, 0x16, 0x72, 0x65, 0x63, 0x65, 0x6e, 0x74, 0x5f,
	0x72, 0x65, 0x61, 0x64, 0x5f, 0x73, 0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x5f, 0x69, 0x64, 0x18,
	0x06, 0x20, 0x01, 0x28, 0x03, 0x52, 0x13, 0x72, 0x65, 0x63, 0x65, 0x6e, 0x74, 0x52, 0x65, 0x61,
	0x64, 0x53, 0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x49, 0x64, 0x12, 0x2f, 0x0a, 0x14, 0x72, 0x65,
	0x63, 0x65, 0x6e, 0x74, 0x5f, 0x72, 0x65, 0x61, 0x64, 0x5f, 0x65, 0x6e, 0x74, 0x72, 0x79, 0x5f,
	0x69, 0x64, 0x18, 0x07, 0x20, 0x01, 0x28, 0x03, 0x52, 0x11, 0x72, 0x65, 0x63, 0x65, 0x6e, 0x74,
	0x52, 0x65, 0x61, 0x64, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x49, 0x64, 0x12, 0x32, 0x0a, 0x15, 0x72,
	0x65, 0x63, 0x65, 0x6e, 0x74, 0x5f, 0x72, 0x65, 0x61, 0x64, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x73,
	0x74, 0x61, 0x6d, 0x70, 0x18, 0x08, 0x20, 0x01, 0x28, 0x06, 0x52, 0x13, 0x72, 0x65, 0x63, 0x65,
	0x6e, 0x74, 0x52, 0x65, 0x61, 0x64, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x22,
	0xbe, 0x03, 0x0a, 0x14, 0x53, 0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x43, 0x6c, 0x65, 0x61, 0x6e,
	0x75, 0x70, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x12, 0x15, 0x0a, 0x06, 0x6c, 0x6f, 0x67, 0x5f,
	0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x05, 0x6c, 0x6f, 0x67, 0x49, 0x64, 0x12,
	0x1d, 0x0a, 0x0a, 0x73, 0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x5f, 0x69, 0x64, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x03, 0x52, 0x09, 0x73, 0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x49, 0x64, 0x12, 0x40,
	0x0a, 0x05, 0x73, 0x74, 0x61, 0x74, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x2a, 0x2e,
	0x77, 0x6f, 0x6f, 0x64, 0x70, 0x65, 0x63, 0x6b, 0x65, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x2e, 0x6d, 0x65, 0x74, 0x61, 0x2e, 0x53, 0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x43, 0x6c, 0x65,
	0x61, 0x6e, 0x75, 0x70, 0x53, 0x74, 0x61, 0x74, 0x65, 0x52, 0x05, 0x73, 0x74, 0x61, 0x74, 0x65,
	0x12, 0x1d, 0x0a, 0x0a, 0x73, 0x74, 0x61, 0x72, 0x74, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x18, 0x04,
	0x20, 0x01, 0x28, 0x04, 0x52, 0x09, 0x73, 0x74, 0x61, 0x72, 0x74, 0x54, 0x69, 0x6d, 0x65, 0x12,
	0x28, 0x0a, 0x10, 0x6c, 0x61, 0x73, 0x74, 0x5f, 0x75, 0x70, 0x64, 0x61, 0x74, 0x65, 0x5f, 0x74,
	0x69, 0x6d, 0x65, 0x18, 0x05, 0x20, 0x01, 0x28, 0x04, 0x52, 0x0e, 0x6c, 0x61, 0x73, 0x74, 0x55,
	0x70, 0x64, 0x61, 0x74, 0x65, 0x54, 0x69, 0x6d, 0x65, 0x12, 0x78, 0x0a, 0x15, 0x71, 0x75, 0x6f,
	0x72, 0x75, 0x6d, 0x5f, 0x63, 0x6c, 0x65, 0x61, 0x6e, 0x75, 0x70, 0x5f, 0x73, 0x74, 0x61, 0x74,
	0x75, 0x73, 0x18, 0x06, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x44, 0x2e, 0x77, 0x6f, 0x6f, 0x64, 0x70,
	0x65, 0x63, 0x6b, 0x65, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x6d, 0x65, 0x74, 0x61,
	0x2e, 0x53, 0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x43, 0x6c, 0x65, 0x61, 0x6e, 0x75, 0x70, 0x53,
	0x74, 0x61, 0x74, 0x75, 0x73, 0x2e, 0x51, 0x75, 0x6f, 0x72, 0x75, 0x6d, 0x43, 0x6c, 0x65, 0x61,
	0x6e, 0x75, 0x70, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x13,
	0x71, 0x75, 0x6f, 0x72, 0x75, 0x6d, 0x43, 0x6c, 0x65, 0x61, 0x6e, 0x75, 0x70, 0x53, 0x74, 0x61,
	0x74, 0x75, 0x73, 0x12, 0x23, 0x0a, 0x0d, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x5f, 0x6d, 0x65, 0x73,
	0x73, 0x61, 0x67, 0x65, 0x18, 0x07, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0c, 0x65, 0x72, 0x72, 0x6f,
	0x72, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x1a, 0x46, 0x0a, 0x18, 0x51, 0x75, 0x6f, 0x72,
	0x75, 0x6d, 0x43, 0x6c, 0x65, 0x61, 0x6e, 0x75, 0x70, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x45,
	0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x08, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01,
	0x2a, 0x44, 0x0a, 0x0c, 0x53, 0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x53, 0x74, 0x61, 0x74, 0x65,
	0x12, 0x0a, 0x0a, 0x06, 0x41, 0x63, 0x74, 0x69, 0x76, 0x65, 0x10, 0x00, 0x12, 0x0d, 0x0a, 0x09,
	0x43, 0x6f, 0x6d, 0x70, 0x6c, 0x65, 0x74, 0x65, 0x64, 0x10, 0x02, 0x12, 0x0a, 0x0a, 0x06, 0x53,
	0x65, 0x61, 0x6c, 0x65, 0x64, 0x10, 0x03, 0x12, 0x0d, 0x0a, 0x09, 0x54, 0x72, 0x75, 0x6e, 0x63,
	0x61, 0x74, 0x65, 0x64, 0x10, 0x04, 0x2a, 0x59, 0x0a, 0x13, 0x53, 0x65, 0x67, 0x6d, 0x65, 0x6e,
	0x74, 0x43, 0x6c, 0x65, 0x61, 0x6e, 0x75, 0x70, 0x53, 0x74, 0x61, 0x74, 0x65, 0x12, 0x17, 0x0a,
	0x13, 0x43, 0x4c, 0x45, 0x41, 0x4e, 0x55, 0x50, 0x5f, 0x49, 0x4e, 0x5f, 0x50, 0x52, 0x4f, 0x47,
	0x52, 0x45, 0x53, 0x53, 0x10, 0x00, 0x12, 0x15, 0x0a, 0x11, 0x43, 0x4c, 0x45, 0x41, 0x4e, 0x55,
	0x50, 0x5f, 0x43, 0x4f, 0x4d, 0x50, 0x4c, 0x45, 0x54, 0x45, 0x44, 0x10, 0x01, 0x12, 0x12, 0x0a,
	0x0e, 0x43, 0x4c, 0x45, 0x41, 0x4e, 0x55, 0x50, 0x5f, 0x46, 0x41, 0x49, 0x4c, 0x45, 0x44, 0x10,
	0x02, 0x42, 0x28, 0x5a, 0x26, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f,
	0x7a, 0x69, 0x6c, 0x6c, 0x69, 0x7a, 0x74, 0x65, 0x63, 0x68, 0x2f, 0x57, 0x6f, 0x6f, 0x64, 0x70,
	0x65, 0x63, 0x6b, 0x65, 0x72, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x06, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x33,
}

var (
	file_meta_proto_rawDescOnce sync.Once
	file_meta_proto_rawDescData = file_meta_proto_rawDesc
)

func file_meta_proto_rawDescGZIP() []byte {
	file_meta_proto_rawDescOnce.Do(func() {
		file_meta_proto_rawDescData = protoimpl.X.CompressGZIP(file_meta_proto_rawDescData)
	})
	return file_meta_proto_rawDescData
}

var file_meta_proto_enumTypes = make([]protoimpl.EnumInfo, 2)
var file_meta_proto_msgTypes = make([]protoimpl.MessageInfo, 7)
var file_meta_proto_goTypes = []any{
	(SegmentState)(0),            // 0: woodpecker.proto.meta.SegmentState
	(SegmentCleanupState)(0),     // 1: woodpecker.proto.meta.SegmentCleanupState
	(*LogMeta)(nil),              // 2: woodpecker.proto.meta.LogMeta
	(*SegmentMetadata)(nil),      // 3: woodpecker.proto.meta.SegmentMetadata
	(*Version)(nil),              // 4: woodpecker.proto.meta.Version
	(*QuorumInfo)(nil),           // 5: woodpecker.proto.meta.QuorumInfo
	(*ReaderTempInfo)(nil),       // 6: woodpecker.proto.meta.ReaderTempInfo
	(*SegmentCleanupStatus)(nil), // 7: woodpecker.proto.meta.SegmentCleanupStatus
	nil,                          // 8: woodpecker.proto.meta.SegmentCleanupStatus.QuorumCleanupStatusEntry
}
var file_meta_proto_depIdxs = []int32{
	0, // 0: woodpecker.proto.meta.SegmentMetadata.state:type_name -> woodpecker.proto.meta.SegmentState
	1, // 1: woodpecker.proto.meta.SegmentCleanupStatus.state:type_name -> woodpecker.proto.meta.SegmentCleanupState
	8, // 2: woodpecker.proto.meta.SegmentCleanupStatus.quorum_cleanup_status:type_name -> woodpecker.proto.meta.SegmentCleanupStatus.QuorumCleanupStatusEntry
	3, // [3:3] is the sub-list for method output_type
	3, // [3:3] is the sub-list for method input_type
	3, // [3:3] is the sub-list for extension type_name
	3, // [3:3] is the sub-list for extension extendee
	0, // [0:3] is the sub-list for field type_name
}

func init() { file_meta_proto_init() }
func file_meta_proto_init() {
	if File_meta_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_meta_proto_rawDesc,
			NumEnums:      2,
			NumMessages:   7,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_meta_proto_goTypes,
		DependencyIndexes: file_meta_proto_depIdxs,
		EnumInfos:         file_meta_proto_enumTypes,
		MessageInfos:      file_meta_proto_msgTypes,
	}.Build()
	File_meta_proto = out.File
	file_meta_proto_rawDesc = nil
	file_meta_proto_goTypes = nil
	file_meta_proto_depIdxs = nil
}
