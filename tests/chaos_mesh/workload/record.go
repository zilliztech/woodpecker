package workload

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
)

// AckRecord is the persisted unit of truth for I1/I2/I3 read-back.
type AckRecord struct {
	LogName     string `json:"log"`
	LogId       int64  `json:"logId"`
	SegmentId   int64  `json:"seg"`
	EntryId     int64  `json:"entry"`
	PayloadHash string `json:"hash"`
	Seq         int64  `json:"seq"`
}

// earliestAck returns the lowest (SegmentId, EntryId) in recs — the point a verify reader
// must start from to still see every ack in recs. Callers pass one phase's records, so this
// keeps read-back proportional to that phase instead of to the whole reused log (#259).
// recs must be non-empty.
func earliestAck(recs []AckRecord) (segmentId, entryId int64) {
	segmentId, entryId = recs[0].SegmentId, recs[0].EntryId
	for _, r := range recs[1:] {
		if r.SegmentId < segmentId || (r.SegmentId == segmentId && r.EntryId < entryId) {
			segmentId, entryId = r.SegmentId, r.EntryId
		}
	}
	return segmentId, entryId
}

type recorder struct {
	mu sync.Mutex
	f  *os.File
}

func openRecorder(path string, truncate bool) (*recorder, error) {
	flags := os.O_CREATE | os.O_WRONLY | os.O_APPEND
	if truncate {
		flags = os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	}
	f, err := os.OpenFile(path, flags, 0o644)
	if err != nil {
		return nil, err
	}
	return &recorder{f: f}, nil
}

func (r *recorder) append(rec AckRecord) error {
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	_, err = r.f.Write(append(b, '\n'))
	return err
}

func (r *recorder) close() error { return r.f.Close() }

func loadRecords(path string) ([]AckRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []AckRecord
	s := bufio.NewScanner(f)
	s.Buffer(make([]byte, 0, 1024*1024), 8*1024*1024)
	for s.Scan() {
		line := s.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec AckRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, s.Err()
}
