package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

type fakeMilvus struct {
	milvuspb.UnimplementedMilvusServiceServer
	create func(context.Context, *milvuspb.CreateCollectionRequest) (*commonpb.Status, error)
	insert func(context.Context, *milvuspb.InsertRequest) (*milvuspb.MutationResult, error)
	search func(context.Context, *milvuspb.SearchRequest) (*milvuspb.SearchResults, error)
}

func (f *fakeMilvus) CreateCollection(c context.Context, r *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	return f.create(c, r)
}

func (f *fakeMilvus) Insert(c context.Context, r *milvuspb.InsertRequest) (*milvuspb.MutationResult, error) {
	return f.insert(c, r)
}

func (f *fakeMilvus) Search(c context.Context, r *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
	return f.search(c, r)
}

func client(t *testing.T, f *fakeMilvus) milvuspb.MilvusServiceClient {
	t.Helper()
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	s := grpc.NewServer()
	milvuspb.RegisterMilvusServiceServer(s, f)
	go func() { _ = s.Serve(l) }()
	t.Cleanup(s.Stop)
	c, e := grpc.NewClient(l.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = c.Close() })
	return milvuspb.NewMilvusServiceClient(c)
}

func TestVectorDatasetHasNoMarkerPeriodAndIsBitStable(t *testing.T) {
	seen := map[[32]byte]bool{}
	raw := make([]byte, dimension*4)
	for id := 0; id < 65536; id++ {
		v := vector(id)
		for k, x := range v {
			if x < 0 || x >= 1 {
				t.Fatal("out of bounded float range")
			}
			binary.LittleEndian.PutUint32(raw[k*4:], math.Float32bits(x))
		}
		h := sha256.Sum256(raw)
		if seen[h] {
			t.Fatalf("duplicate vector %d", id)
		}
		seen[h] = true
	}
	for i, x := range vector(251) {
		if math.Float32bits(x) != math.Float32bits(vector(251)[i]) {
			t.Fatal("unstable vector")
		}
	}
}

func TestCreateCarriesOwnedSchemaAndStrongConsistency(t *testing.T) {
	const name = "query_accept_20261028T000000Z_aabbccdd"
	w := worker{o: options{collection: name, timeout: time.Second}, api: client(t, &fakeMilvus{create: func(_ context.Context, r *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
		var schema schemapb.CollectionSchema
		if e := proto.Unmarshal(r.GetSchema(), &schema); e != nil {
			t.Error(e)
		}
		if schema.GetName() != name || schema.GetDescription() != ownerDescription(name) || schema.GetAutoID() || len(schema.GetFields()) != 2 ||
			schema.GetFields()[1].GetTypeParams()[0].GetValue() != "256" || r.GetShardsNum() != 1 || r.GetConsistencyLevel() != commonpb.ConsistencyLevel_Strong {
			t.Error("unowned/changed request plan")
		}
		return &commonpb.Status{}, nil
	}})}
	if e := w.create(context.Background()); e != nil {
		t.Fatal(e)
	}
}

func searchReply(nq, rows int) *milvuspb.SearchResults {
	ids := make([]int64, nq)
	scores := make([]float32, nq)
	topks := make([]int64, nq)
	for i := range ids {
		ids[i] = int64(i % rows)
		topks[i] = 1
	}
	return &milvuspb.SearchResults{Status: &commonpb.Status{}, Results: &schemapb.SearchResultData{
		NumQueries: int64(nq), TopK: 1,
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ids}}}, Scores: scores, Topks: topks,
	}}
}

func TestSearchChecksEveryResultAndRejectsWrongStatusOrScores(t *testing.T) {
	for _, kind := range []string{"valid", "nil-status", "failed-status", "wrong-nq", "wrong-id", "nan-score", "infinite-score", "wrong-distance", "wrong-topks", "extra-result"} {
		t.Run(kind, func(t *testing.T) {
			r := searchReply(8, 1024)
			switch kind {
			case "nil-status":
				r.Status = nil
			case "failed-status":
				r.Status.Code = 1
			case "wrong-nq":
				r.Results.NumQueries = 7
			case "wrong-id":
				r.Results.Ids.GetIntId().Data[7] = 99
			case "nan-score":
				r.Results.Scores[7] = float32(math.NaN())
			case "infinite-score":
				r.Results.Scores[7] = float32(math.Inf(1))
			case "wrong-distance":
				r.Results.Scores[7] = 1
			case "wrong-topks":
				r.Results.Topks[7] = 2
			case "extra-result":
				r.Results.Ids.GetIntId().Data = append(r.Results.Ids.GetIntId().Data, 9)
			}
			if validSearch(r, 8, 1024) != (kind == "valid") {
				t.Fatal("incorrect result qualified")
			}
		})
	}
}

func TestVectorReadVerificationRejectsCorruptionDuplicatesAndExtraRows(t *testing.T) {
	for _, kind := range []string{"valid", "duplicate", "wrong-vector", "wrong-dimension", "extra", "nil-status"} {
		t.Run(kind, func(t *testing.T) {
			r := &milvuspb.QueryResults{Status: &commonpb.Status{}, FieldsData: dataFields(1024, 4)}
			switch kind {
			case "duplicate":
				r.FieldsData[0].GetScalars().GetLongData().Data[1] = 1024
			case "wrong-vector":
				r.FieldsData[1].GetVectors().GetFloatVector().Data[dimension+1] += 0.01
			case "wrong-dimension":
				r.FieldsData[1].GetVectors().Dim = 128
			case "extra":
				r.FieldsData[0].GetScalars().GetLongData().Data = append(r.FieldsData[0].GetScalars().GetLongData().Data, 1028)
			case "nil-status":
				r.Status = nil
			}
			if checkQuery(r, 1024, 4) != (kind == "valid") {
				t.Fatal("incorrect acknowledged payload qualified")
			}
		})
	}
}

func TestSeedCannotTreatCountIDsOrTimestampMismatchAsAcknowledged(t *testing.T) {
	for _, kind := range []string{"wrong-id", "wrong-count", "zero-ts", "nil-status", "valid-until-flush"} {
		t.Run(kind, func(t *testing.T) {
			w := worker{o: options{collection: "owned", rows: 1024, timeout: time.Second}, metadata: map[string]any{}}
			w.api = client(t, &fakeMilvus{insert: func(_ context.Context, r *milvuspb.InsertRequest) (*milvuspb.MutationResult, error) {
				ids := append([]int64(nil), r.GetFieldsData()[0].GetScalars().GetLongData().GetData()...)
				x := &milvuspb.MutationResult{
					Status: &commonpb.Status{}, InsertCnt: int64(r.GetNumRows()), Timestamp: 470000000000000001,
					IDs: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ids}}},
				}
				switch kind {
				case "wrong-id":
					ids[0] = 999999
				case "wrong-count":
					x.InsertCnt--
				case "zero-ts":
					x.Timestamp = 0
				case "nil-status":
					x.Status = nil
				}
				return x, nil
			}})
			if e := w.seed(context.Background()); e == nil {
				t.Fatal("unimplemented flush must not pass")
			}
			want := 0
			if kind == "valid-until-flush" {
				want = 1024
			}
			if w.acknowledged != want {
				t.Fatalf("acknowledged %d want %d", w.acknowledged, want)
			}
		})
	}
}

func TestSearchRPCDeadlineRetainsFailureInsteadOfSuccess(t *testing.T) {
	w := worker{
		o: options{rows: 1024, nq: 1, concurrency: 1, requests: 1, duration: time.Second, timeout: 20 * time.Millisecond}, metadata: map[string]any{},
		api: client(t, &fakeMilvus{search: func(ctx context.Context, r *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
			if r.GetGuaranteeTimestamp() != 0 || r.GetConsistencyLevel() != commonpb.ConsistencyLevel_Strong {
				t.Error("unexpected visibility control")
			}
			<-ctx.Done()
			return nil, ctx.Err()
		}}),
	}
	if e := w.search(context.Background()); e == nil {
		t.Fatal("timeout passed")
	}
	if len(w.records) != 1 || w.records[0].OK {
		t.Fatal("failed RPC not retained")
	}
}

func TestBoundsAndExistingReceiptRefuseBeforeNetwork(t *testing.T) {
	base := []string{"--endpoint", "127.0.0.1:1", "--collection", "query_accept_20261028T000000Z_aabbccdd", "--output", filepath.Join(t.TempDir(), "receipt.json")}
	for _, extra := range [][]string{{"--endpoint", "10.0.0.1:1"}, {"--collection", "existing"}, {"--rows", "65537"}, {"--nq", "16385"}, {"--concurrency", "17"}, {"--requests", "1025"}, {"--duration", "121s"}, {"--timeout", "0s"}} {
		if _, e := parse(append(append([]string{}, base...), extra...)); e == nil {
			t.Fatal("unbounded selection accepted")
		}
	}
	o, e := parse(base)
	if e != nil {
		t.Fatal(e)
	}
	o.execute = true
	if e = os.WriteFile(o.output, []byte("original receipt"), 0o600); e != nil {
		t.Fatal(e)
	}
	if execute(o) == nil {
		t.Fatal("receipt overwrite allowed")
	}
	raw, e := os.ReadFile(o.output)
	if e != nil || string(raw) != "original receipt" {
		t.Fatal("existing artifact changed")
	}
}

func TestParentPipeEOFCancelsAnActualPendingRPC(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	w := worker{
		o: options{rows: 1024, nq: 1, concurrency: 1, requests: 1, duration: time.Second, timeout: time.Second}, metadata: map[string]any{},
		api: client(t, &fakeMilvus{search: func(c context.Context, _ *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
			close(started)
			<-c.Done()
			return nil, c.Err()
		}}),
	}
	r, writer := io.Pipe()
	defer r.Close()
	go cancelOnEOF(r, cancel)
	done := make(chan error, 1)
	go func() { done <- w.search(ctx) }()
	<-started
	_ = writer.Close()
	select {
	case e := <-done:
		if e == nil {
			t.Fatal("cancelled workload passed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("lost parent left RPC running")
	}
}
