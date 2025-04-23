package proto

import (
	"fmt"

	pb "google.golang.org/protobuf/proto"
)

// MarshalSegmentCleanupStatus marshals a SegmentCleanupStatus struct to bytes
func MarshalSegmentCleanupStatus(status *SegmentCleanupStatus) ([]byte, error) {
	if status == nil {
		return nil, fmt.Errorf("cannot marshal nil SegmentCleanupStatus")
	}
	return pb.Marshal(status)
}

// UnmarshalSegmentCleanupStatus unmarshals bytes to a SegmentCleanupStatus struct
func UnmarshalSegmentCleanupStatus(data []byte, status *SegmentCleanupStatus) error {
	if status == nil {
		return fmt.Errorf("cannot unmarshal to nil SegmentCleanupStatus")
	}
	return pb.Unmarshal(data, status)
}
