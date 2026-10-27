package main

import (
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

// Protocol fixture only: it validates actual serialized RPCs, not Milvus storage.
type fixture struct {
	milvuspb.UnimplementedMilvusServiceServer
	mu      sync.Mutex
	queries []*milvuspb.QueryRequest
	search  []*milvuspb.SearchRequest
	wrong   bool
	fail    bool
	block   bool
	started chan struct{}
}

const serverTS uint64 = 470000000000000123

func (f *fixture) Insert(_ context.Context, r *milvuspb.InsertRequest) (*milvuspb.MutationResult, error) {
	id := r.GetFieldsData()[0].GetScalars().GetLongData().GetData()[0]
	return &milvuspb.MutationResult{Status: &commonpb.Status{}, Timestamp: serverTS, InsertCnt: 1, IDs: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{id}}}}}, nil
}

func (f *fixture) Query(ctx context.Context, r *milvuspb.QueryRequest) (*milvuspb.QueryResults, error) {
	if f.block {
		if f.started != nil {
			close(f.started)
		}
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	if f.fail {
		return nil, status.Error(codes.Unavailable, "sentinel-private-reason")
	}
	f.mu.Lock()
	f.queries = append(f.queries, r)
	f.mu.Unlock()
	var id int64
	_, _ = fmt.Sscanf(r.GetExpr(), "id == %d", &id)
	if f.wrong {
		id++
	}
	return &milvuspb.QueryResults{Status: &commonpb.Status{}, FieldsData: []*schemapb.FieldData{fields(id)[0]}}, nil
}

func (f *fixture) Search(_ context.Context, r *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
	f.mu.Lock()
	f.search = append(f.search, r)
	f.mu.Unlock()
	var id int64
	_, _ = fmt.Sscanf(r.GetDsl(), "id == %d", &id)
	return &milvuspb.SearchResults{Status: &commonpb.Status{}, Results: &schemapb.SearchResultData{NumQueries: 1, Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{id}}}}}}, nil
}

func client(t *testing.T, f *fixture) milvuspb.MilvusServiceClient {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	milvuspb.RegisterMilvusServiceServer(server, f)
	go func() { _ = server.Serve(listener) }()
	conn, err := grpc.NewClient("passthrough:///fixture", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(); server.Stop(); _ = listener.Close() })
	return milvuspb.NewMilvusServiceClient(conn)
}

func TestServerDerivedFenceAndStrongControlsReachActualRPCs(t *testing.T) {
	f := &fixture{}
	w := worker{api: client(t, f), options: options{phase: "future", collection: "owned", offsetMS: 2000, timeout: time.Second}}
	if err := w.batch(context.Background(), 71); err != nil {
		t.Fatal(err)
	}
	if len(w.records) != 5 || len(f.queries) != 2 || len(f.search) != 2 {
		t.Fatal("missing actual insert/query/search RPCs")
	}
	fence := serverTS + (2000 << 18)
	customized, strong := 0, 0
	for _, r := range w.records {
		if !r.OK {
			t.Fatal("successful protocol response lost")
		}
		if r.Operation == "insert" {
			continue
		}
		if r.AckTimestamp != serverTS {
			t.Fatal("timestamp not from actual Insert response")
		}
		if r.Control {
			strong++
			if r.GuaranteeTimestamp != 0 || r.Consistency != "Strong" {
				t.Fatal("strong control was given the future fence")
			}
		} else {
			customized++
			if r.GuaranteeTimestamp != fence || r.Consistency != "Customized" {
				t.Fatal("future guarantee changed or precision lost")
			}
		}
	}
	if customized != 2 || strong != 2 {
		t.Fatal("missing paired read types")
	}
	for _, r := range f.queries {
		if r.UseDefaultConsistency || (r.ConsistencyLevel == commonpb.ConsistencyLevel_Customized && r.GuaranteeTimestamp != fence) || (r.ConsistencyLevel == commonpb.ConsistencyLevel_Strong && r.GuaranteeTimestamp != 0) {
			t.Fatal("query did not send explicit custom consistency/fence")
		}
	}
	for _, r := range f.search {
		var placeholders commonpb.PlaceholderGroup
		if r.UseDefaultConsistency || (r.ConsistencyLevel == commonpb.ConsistencyLevel_Customized && r.GuaranteeTimestamp != fence) || (r.ConsistencyLevel == commonpb.ConsistencyLevel_Strong && r.GuaranteeTimestamp != 0) || proto.Unmarshal(r.GetPlaceholderGroup(), &placeholders) != nil || len(placeholders.GetPlaceholders()) != 1 || len(placeholders.Placeholders[0].Values[0]) != 16*4 {
			t.Fatal("search wire request changed")
		}
	}
}

func TestIncorrectSuccessfulIDsAndTransportErrorsDoNotPass(t *testing.T) {
	for _, f := range []*fixture{{wrong: true}, {fail: true}} {
		w := worker{api: client(t, f), options: options{timeout: time.Second}}
		err := w.read(context.Background(), 5, serverTS, 0, false, true)
		if err == nil || strings.Contains(err.Error(), "sentinel") || len(w.records) != 1 || w.records[0].OK {
			t.Fatal("incorrect/private response passed")
		}
	}
}

func TestCancelledRPCIsRecordedAsFailure(t *testing.T) {
	w := worker{api: client(t, &fixture{block: true}), options: options{timeout: time.Second}}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if w.read(ctx, 5, serverTS, 0, false, true) == nil || w.records[0].OK || w.records[0].GRPCCode != "DeadlineExceeded" {
		t.Fatal("deadline became a successful latency sample")
	}
}

func TestParentPipeEOFStopsTheActualBlockedRPC(t *testing.T) {
	started := make(chan struct{})
	w := worker{api: client(t, &fixture{block: true, started: started}), options: options{timeout: 5 * time.Second}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader, writer := io.Pipe()
	defer reader.Close()
	go cancelOnEOF(reader, cancel)
	done := make(chan error, 1)
	go func() { done <- w.read(ctx, 5, serverTS, 0, false, true) }()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("fixture RPC did not reach its server")
	}
	_ = writer.Close()
	select {
	case err := <-done:
		if err == nil || w.records[0].OK || w.records[0].GRPCCode != "Canceled" {
			t.Fatal("parent loss did not fail/cancel its owned request")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("parent pipe loss left its RPC running")
	}
}

func TestFenceRejectsInvalidOrOverflowedServerTimestamp(t *testing.T) {
	for _, input := range [][2]uint64{{0, 2000}, {math.MaxUint64, 2000}, {serverTS, 5001}} {
		if _, err := guarantee(input[0], input[1]); err == nil {
			t.Fatal("invalid timestamp accepted")
		}
	}
}

func TestPlanDoesNotCreateReceiptAndExecutionDoesNotOverwriteIt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "receipt.json")
	o := options{endpoint: "127.0.0.1:1", collection: "query_accept_20261027T000000Z_aabbccdd", output: path,
		mode: "phase", phase: "future", startID: 64, batches: 24, parallel: 2, rows: 64, offsetMS: 2000, duration: time.Second, timeout: time.Second}
	if err := execute(o); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("offline plan created an artifact")
	}
	if err := os.WriteFile(path, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	o.execute = true
	if execute(o) == nil {
		t.Fatal("existing receipt was overwritten")
	}
	raw, _ := os.ReadFile(path)
	if string(raw) != "original" {
		t.Fatal("original evidence changed")
	}
}
