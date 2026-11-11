package main

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/zilliztech/woodpecker/proto"
)

const testOwner = "native-20261028t000000z-aabbccdd"

type rpcFixture struct {
	proto.UnimplementedLogStoreServer
	appendFn func(*proto.AddEntryRequest, grpc.ServerStreamingServer[proto.AddEntryResponse]) error
	readFn   func(context.Context, *proto.GetBatchEntriesAdvRequest) (*proto.GetBatchEntriesAdvResponse, error)
}

func (f *rpcFixture) AddEntry(r *proto.AddEntryRequest, s grpc.ServerStreamingServer[proto.AddEntryResponse]) error {
	return f.appendFn(r, s)
}

func (f *rpcFixture) GetBatchEntriesAdv(c context.Context, r *proto.GetBatchEntriesAdvRequest) (*proto.GetBatchEntriesAdvResponse, error) {
	return f.readFn(c, r)
}

func clientFor(t *testing.T, f *rpcFixture) proto.LogStoreClient {
	l, e := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, e)
	s := grpc.NewServer()
	proto.RegisterLogStoreServer(s, f)
	go func() { _ = s.Serve(l) }()
	t.Cleanup(s.Stop)
	c, e := grpc.NewClient(l.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, e)
	t.Cleanup(func() { _ = c.Close() })
	return proto.NewLogStoreClient(c)
}

func TestAppendRequiresRealBufferedThenSyncedIdentity(t *testing.T) {
	for _, test := range []struct {
		name   string
		states []proto.AddEntryState
		id     int64
		status *proto.Status
		valid  bool
	}{
		{"success", []proto.AddEntryState{0, 1}, 0, &proto.Status{}, true},
		{"buffered-only", []proto.AddEntryState{0}, 0, &proto.Status{}, false},
		{"synced-only", []proto.AddEntryState{1}, 0, &proto.Status{}, false},
		{"duplicate", []proto.AddEntryState{0, 1, 1}, 0, &proto.Status{}, false},
		{"wrong-id", []proto.AddEntryState{0, 1}, 1, &proto.Status{}, false},
		{"nil-status", []proto.AddEntryState{0, 1}, 0, nil, false},
		{"rejected", []proto.AddEntryState{2}, 0, &proto.Status{Code: 1}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := clientFor(t, &rpcFixture{appendFn: func(r *proto.AddEntryRequest, s grpc.ServerStreamingServer[proto.AddEntryResponse]) error {
				require.Equal(t, "native-acceptance", r.BucketName)
				require.Equal(t, testOwner, r.RootPath)
				require.Equal(t, payload(testOwner, 1001, 1, 0), r.Entry.Values)
				for _, state := range test.states {
					if e := s.Send(&proto.AddEntryResponse{Status: test.status, EntryId: test.id, State: state}); e != nil {
						return e
					}
				}
				return nil
			}})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			e := appendOne(ctx, c, testOwner, 1001, 1, 0)
			if test.valid {
				require.NoError(t, e)
			} else {
				require.Error(t, e)
			}
		})
	}
}

func TestReadPayloadIdentityCountAndEOFBoundary(t *testing.T) {
	for _, test := range []struct {
		name  string
		code  int32
		bad   string
		eof   bool
		valid bool
	}{
		{"working", 0, "", false, true},
		{"payload", 0, "payload", false, false},
		{"segment", 0, "segment", false, false},
		{"extra-row", 0, "extra", false, false},
		{"absent-status", 0, "status", false, false},
		{"entry-not-found", 8102, "", true, true},
		{"file-eof", 2310, "", true, true},
		{"arbitrary-error-not-eof", 500, "", true, false},
		{"success-not-eof", 0, "", true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := clientFor(t, &rpcFixture{readFn: func(_ context.Context, r *proto.GetBatchEntriesAdvRequest) (*proto.GetBatchEntriesAdvResponse, error) {
				require.EqualValues(t, 5, r.MaxEntries)
				if test.eof {
					require.EqualValues(t, 4, r.FromEntryId)
				}
				entries := []*proto.LogEntry{}
				for i := int64(0); i < 4; i++ {
					entries = append(entries, &proto.LogEntry{SegId: 2, EntryId: i, Values: payload(testOwner, 1002, 2, i)})
				}
				status := &proto.Status{Code: test.code}
				switch test.bad {
				case "payload":
					entries[0].Values = []byte("wrong")
				case "segment":
					entries[0].SegId = 3
				case "extra":
					entries = append(entries, entries[0])
				case "status":
					status = nil
				}
				return &proto.GetBatchEntriesAdvResponse{Status: status, Result: &proto.BatchReadResult{Entries: entries}}, nil
			}})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			e := readEntries(ctx, c, testOwner, 1002, 2, 4, test.eof)
			if test.valid {
				require.NoError(t, e)
			} else {
				require.Error(t, e)
			}
		})
	}
}

func TestCancelledRPCDoesNotBecomeAnAcknowledgement(t *testing.T) {
	c := clientFor(t, &rpcFixture{appendFn: func(_ *proto.AddEntryRequest, s grpc.ServerStreamingServer[proto.AddEntryResponse]) error {
		<-s.Context().Done()
		return s.Context().Err()
	}})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.Error(t, appendOne(ctx, c, testOwner, 1001, 1, 0))
}

func TestWorkerRefusesUnboundedOrForeignSelectionBeforeDial(t *testing.T) {
	base := []string{"--endpoint", "127.0.0.1:1", "--owner", testOwner, "--operation", "append"}
	for _, extra := range [][]string{
		{"--endpoint", "10.0.0.1:1"},
		{"--owner", "existing-tenant"},
		{"--log", "9999"},
		{"--segment", "65"},
		{"--entries", "1000000"},
		{"--parallel", "21"},
		{"--segment", "64", "--parallel", "2"},
		{"--timeout", "0s"},
	} {
		require.Error(t, work(context.Background(), append(append([]string{}, base...), extra...)))
	}
}

func newTestGate(t *testing.T) (*gate, *httptest.Server, *atomic.Int32) {
	count := &atomic.Int32{}
	g := &gate{state: gateState{Owner: testOwner}, proxy: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { count.Add(1); _, _ = io.WriteString(w, "real upstream") })}
	s := httptest.NewServer(g)
	t.Cleanup(s.Close)
	return g, s, count
}

func setGate(t *testing.T, g *gate, owner string, blocked bool, hold int) int {
	body, _ := json.Marshal(map[string]any{"owner": owner, "blocked": blocked, "hold_seconds": hold})
	w := httptest.NewRecorder()
	g.control(w, httptest.NewRequest("POST", "/", strings.NewReader(string(body))))
	return w.Code
}

func TestGateOnlyBlocksOwnedObjectsAndRestoresInflightRequests(t *testing.T) {
	g, s, count := newTestGate(t)
	require.Equal(t, 400, setGate(t, g, "foreign", true, 3))
	require.Equal(t, 400, setGate(t, g, testOwner, true, 91))
	require.Equal(t, 200, setGate(t, g, testOwner, true, 3))
	require.Equal(t, 409, setGate(t, g, testOwner, true, 3))
	done := make(chan error, 1)
	go func() {
		r, e := http.Get(s.URL + "/native-acceptance/" + testOwner + "/1001/22/write.lock")
		if e == nil {
			_ = r.Body.Close()
		}
		done <- e
	}()
	end := time.Now().Add(time.Second)
	for g.current().Waiting != 1 && time.Now().Before(end) {
		time.Sleep(time.Millisecond)
	}
	require.Equal(t, 1, g.current().Waiting)
	require.Zero(t, count.Load())
	r, e := http.Get(s.URL + "/native-acceptance/foreign/1001/22/write.lock")
	require.NoError(t, e)
	_ = r.Body.Close()
	require.EqualValues(t, 1, count.Load())
	require.Equal(t, 200, setGate(t, g, testOwner, false, 0))
	require.NoError(t, <-done)
	require.EqualValues(t, 2, count.Load())
	require.Zero(t, g.current().AutomaticReleases)
}

func TestGateLeaseExpiryAndCancellationAreBounded(t *testing.T) {
	g, s, count := newTestGate(t)
	require.Equal(t, 200, setGate(t, g, testOwner, true, 1))
	ctx, cancel := context.WithCancel(context.Background())
	req, e := http.NewRequestWithContext(ctx, "GET", s.URL+"/native-acceptance/"+testOwner+"/1002/2/footer.blk", nil)
	require.NoError(t, e)
	done := make(chan error, 1)
	go func() {
		r, e := http.DefaultClient.Do(req)
		if e == nil {
			_ = r.Body.Close()
		}
		done <- e
	}()
	end := time.Now().Add(time.Second)
	for g.current().Waiting != 1 && time.Now().Before(end) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	require.Error(t, <-done)
	time.Sleep(1100 * time.Millisecond)
	require.False(t, g.current().Blocked)
	require.EqualValues(t, 1, g.current().AutomaticReleases)
	require.Zero(t, count.Load())
}
