package segment

type SegmentEntry struct {
	SegmentId int64
	EntryId   int64
	Data      []byte
}

func SerializeEntry(entry *SegmentEntry) []byte {
	//TODO implement me
	panic("implement me")
}

func DeserializeEntry(data []byte) *SegmentEntry {
	//TODO implement me
	panic("implement me")
}
