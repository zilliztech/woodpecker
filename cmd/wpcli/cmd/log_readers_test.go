package cmd

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	pb "google.golang.org/protobuf/proto"

	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/proto"
)

const (
	readersTestLog   = "mylog"
	readersTestLogID = int64(7)
)

// putLogForReaders writes the log record the reader keys hang off.
func putLogForReaders(t *testing.T, cli *clientv3.Client, kb *meta.KeyBuilder) {
	t.Helper()
	b, err := pb.Marshal(&proto.LogMeta{LogId: readersTestLogID})
	require.NoError(t, err)
	_, err = cli.Put(context.Background(), kb.BuildLogKey(readersTestLog), string(b))
	require.NoError(t, err)
}

// putReader writes one reader's published position, as a live reader's lease-held key holds it.
func putReader(t *testing.T, cli *clientv3.Client, kb *meta.KeyBuilder, info *proto.ReaderTempInfo) {
	t.Helper()
	b, err := pb.Marshal(info)
	require.NoError(t, err)
	_, err = cli.Put(context.Background(),
		kb.BuildLogReaderTempInfoKey(info.LogId, info.ReaderName), string(b))
	require.NoError(t, err)
}

// TestLogReaders_ShowsEachReadersPosition is what the command exists for: a reader's own position,
// which nothing else surfaces. The second reader has never reported a read -- its current position
// is still the one it opened at, with the same timestamp -- and that has to be visible as such
// rather than as a position it reached.
func TestLogReaders_ShowsEachReadersPosition(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	oldGlobals := Globals
	defer func() { Globals = oldGlobals }()
	Globals = GlobalFlags{Timeout: 2 * time.Second}

	putLogForReaders(t, cli, kb)
	nowMs := uint64(time.Now().UnixMilli())
	putReader(t, cli, kb, &proto.ReaderTempInfo{
		ReaderName: "reader-moving", LogId: readersTestLogID,
		OpenTimestamp: nowMs - 600_000, OpenSegmentId: 3, OpenEntryId: 100,
		RecentReadSegmentId: 5, RecentReadEntryId: 4821, RecentReadTimestamp: nowMs - 5_000,
	})
	putReader(t, cli, kb, &proto.ReaderTempInfo{
		ReaderName: "reader-at-open", LogId: readersTestLogID,
		OpenTimestamp: nowMs - 20_000, OpenSegmentId: 5, OpenEntryId: 4000,
		RecentReadSegmentId: 5, RecentReadEntryId: 4000, RecentReadTimestamp: nowMs - 20_000,
	})

	cmd, out, _ := markingTestCmd()
	require.NoError(t, runLogReaders(cmd, cli, kb, readersTestLog))

	s := out.String()
	require.Contains(t, s, "reader-moving")
	require.Contains(t, s, "5:4821", "the reader's current position is the whole point")
	require.Contains(t, s, "3:100", "where it opened tells you whether it has moved at all")
	require.Contains(t, s, "reader-at-open")
	require.Contains(t, s, "no read since open",
		"a reader still at its open position has not reached it, it started there")
}

// TestLogReaders_FlagsAStaleReport covers the reading that means stuck. A reader tailing an idle
// log keeps republishing on its interval with an unchanged position, so a fresh report proves the
// reader is alive; a report older than a couple of intervals means it is not reporting at all,
// which is what a reader blocked inside a read looks like.
func TestLogReaders_FlagsAStaleReport(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	oldGlobals := Globals
	defer func() { Globals = oldGlobals }()
	Globals = GlobalFlags{Timeout: 2 * time.Second}

	putLogForReaders(t, cli, kb)
	nowMs := uint64(time.Now().UnixMilli())
	putReader(t, cli, kb, &proto.ReaderTempInfo{
		ReaderName: "reader-wedged", LogId: readersTestLogID,
		OpenTimestamp: nowMs - 900_000, OpenSegmentId: 3, OpenEntryId: 100,
		RecentReadSegmentId: 5, RecentReadEntryId: 4821, RecentReadTimestamp: nowMs - 300_000,
	})
	putReader(t, cli, kb, &proto.ReaderTempInfo{
		ReaderName: "reader-idle", LogId: readersTestLogID,
		OpenTimestamp: nowMs - 900_000, OpenSegmentId: 3, OpenEntryId: 100,
		RecentReadSegmentId: 5, RecentReadEntryId: 4821, RecentReadTimestamp: nowMs - 10_000,
	})

	cmd, out, _ := markingTestCmd()
	require.NoError(t, runLogReaders(cmd, cli, kb, readersTestLog))

	s := out.String()
	wedged := readersRowFor(t, s, "reader-wedged")
	idle := readersRowFor(t, s, "reader-idle")
	require.Contains(t, wedged, "stale report")
	require.NotContains(t, idle, "stale report",
		"a reader republishing on its interval is alive, whether or not its position moved")
}

// TestLogReaders_NoLiveReadersIsNotAnError covers a log nobody is reading. That is an ordinary
// state, and an error would say the query failed when it succeeded and found nothing.
func TestLogReaders_NoLiveReadersIsNotAnError(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	oldGlobals := Globals
	defer func() { Globals = oldGlobals }()
	Globals = GlobalFlags{Timeout: 2 * time.Second}

	putLogForReaders(t, cli, kb)
	cmd, out, _ := markingTestCmd()

	require.NoError(t, runLogReaders(cmd, cli, kb, readersTestLog))
	require.Regexp(t, `(?i)no live readers`, out.String())
}

// TestLogReaders_UnknownLogIsRefused keeps a mistyped name from reading as "nobody is reading it".
func TestLogReaders_UnknownLogIsRefused(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	oldGlobals := Globals
	defer func() { Globals = oldGlobals }()
	Globals = GlobalFlags{Timeout: 2 * time.Second}

	putLogForReaders(t, cli, kb)
	cmd, _, _ := markingTestCmd()

	err := runLogReaders(cmd, cli, kb, "nosuchlog")
	require.Error(t, err)
	require.Contains(t, err.Error(), "log nosuchlog")
}

// TestLogReaders_JSONCarriesTheRawNumbers covers the machine-readable path: a script comparing a
// reader against the confirmed tail needs the positions as numbers, not as a rendered cell.
func TestLogReaders_JSONCarriesTheRawNumbers(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	oldGlobals := Globals
	defer func() { Globals = oldGlobals }()
	Globals = GlobalFlags{Timeout: 2 * time.Second, Output: "json"}

	putLogForReaders(t, cli, kb)
	nowMs := uint64(time.Now().UnixMilli())
	putReader(t, cli, kb, &proto.ReaderTempInfo{
		ReaderName: "reader-moving", LogId: readersTestLogID,
		OpenTimestamp: nowMs - 600_000, OpenSegmentId: 3, OpenEntryId: 100,
		RecentReadSegmentId: 5, RecentReadEntryId: 4821, RecentReadTimestamp: nowMs - 5_000,
	})

	cmd, out, _ := markingTestCmd()
	require.NoError(t, runLogReaders(cmd, cli, kb, readersTestLog))

	var payload struct {
		LogName string `json:"log_name"`
		LogID   int64  `json:"log_id"`
		Readers []struct {
			Name            string `json:"reader_name"`
			CurrentSegment  int64  `json:"recent_read_segment_id"`
			CurrentEntry    int64  `json:"recent_read_entry_id"`
			LastReportAgeMs int64  `json:"last_report_age_ms"`
			State           string `json:"state"`
		} `json:"readers"`
	}
	s := out.String()
	require.NoError(t, json.Unmarshal([]byte(s[strings.Index(s, "{"):]), &payload))
	require.Equal(t, readersTestLogID, payload.LogID)
	require.Len(t, payload.Readers, 1)
	require.Equal(t, int64(5), payload.Readers[0].CurrentSegment)
	require.Equal(t, int64(4821), payload.Readers[0].CurrentEntry)
	// The window excludes the open timestamp, 600s back: an age taken from the wrong field would
	// still be "at least 5s".
	require.GreaterOrEqual(t, payload.Readers[0].LastReportAgeMs, int64(5_000))
	require.Less(t, payload.Readers[0].LastReportAgeMs, int64(60_000),
		"the age comes from the last report, not from when the reader opened")
	require.Equal(t, "live", payload.Readers[0].State)
}

// readersRowFor returns the output line naming a reader, so an assertion about one reader cannot
// be satisfied by another reader's row.
func readersRowFor(t *testing.T, out, readerName string) string {
	t.Helper()
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, readerName) {
			return line
		}
	}
	t.Fatalf("no row for %s in:\n%s", readerName, out)
	return ""
}

// TestLogReaders_UnparseableRecordDoesNotHideTheRest covers one corrupt record among good ones. The
// readers are separate keys, so one bad value says nothing about the others, and abandoning the
// listing would hide every reader behind the first damaged record.
func TestLogReaders_UnparseableRecordDoesNotHideTheRest(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("wptest")
	oldGlobals := Globals
	defer func() { Globals = oldGlobals }()
	Globals = GlobalFlags{Timeout: 2 * time.Second}

	putLogForReaders(t, cli, kb)
	nowMs := uint64(time.Now().UnixMilli())
	putReader(t, cli, kb, &proto.ReaderTempInfo{
		ReaderName: "reader-good", LogId: readersTestLogID,
		OpenTimestamp: nowMs - 60_000, OpenSegmentId: 3, OpenEntryId: 100,
		RecentReadSegmentId: 5, RecentReadEntryId: 4821, RecentReadTimestamp: nowMs - 5_000,
	})
	_, err := cli.Put(context.Background(),
		kb.BuildLogReaderTempInfoKey(readersTestLogID, "reader-corrupt"), "not a protobuf at all")
	require.NoError(t, err)

	cmd, out, errOut := markingTestCmd()
	require.NoError(t, runLogReaders(cmd, cli, kb, readersTestLog))

	require.Contains(t, out.String(), "5:4821", "the readable records must still be listed")
	require.Contains(t, errOut.String(), "reader-corrupt",
		"the record that could not be read has to be named, not silently dropped")
}

// TestFormatAge covers a published timestamp ahead of this host's clock. The two clocks are
// different machines' and need not agree; rendering the difference as negative time would read as a
// reader that reported in the future.
func TestFormatAge(t *testing.T) {
	require.Equal(t, "0s", formatAge(-4_000), "a clock difference is not time running backwards")
	require.Equal(t, "20s", formatAge(20_000))
	require.Equal(t, "30m5s", formatAge(1_805_000))
}
