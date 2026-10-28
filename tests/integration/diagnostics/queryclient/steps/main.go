// Licensed to the LF AI & Data foundation under the Apache License, Version 2.0.
// Opted-in real Milvus workload for query scheduling/execution acceptance.
package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const dimension = 256

type options struct {
	endpoint, collection, output, mode string
	rows, nq, concurrency, requests    int
	duration, timeout                  time.Duration
	execute, parentPipe                bool
}

type record struct {
	Operation    string  `json:"operation"`
	At           string  `json:"at"`
	FinishedAt   string  `json:"finished_at"`
	LatencyMS    float64 `json:"latency_ms"`
	OK           bool    `json:"ok"`
	GRPCCode     string  `json:"grpc_code"`
	FirstID      int     `json:"first_id,omitempty"`
	Rows         int     `json:"rows,omitempty"`
	AckTimestamp uint64  `json:"ack_timestamp,string,omitempty"`
	VerifiedNQ   int     `json:"verified_nq,omitempty"`
}

type worker struct {
	api          milvuspb.MilvusServiceClient
	o            options
	mu           sync.Mutex
	records      []record
	acknowledged int
	metadata     map[string]any
}

func successful(s *commonpb.Status) bool {
	return s != nil && s.GetCode() == 0 && s.GetErrorCode() == commonpb.ErrorCode_Success
}

func ownerDescription(name string) string { return "wpdiag query steps acceptance " + name }

// The former 16-dimensional marker vector repeats after 251 IDs. This separate
// dataset uses deterministic pseudo-random float32 values for exact L2 self-match.
func vector(id int) []float32 {
	x := uint32(id+1)*747796405 + 2891336453
	v := make([]float32, dimension)
	for k := range v {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		v[k] = float32(x&0xffffff) / float32(1<<24)
	}
	return v
}

func dataFields(start, count int) []*schemapb.FieldData {
	ids := make([]int64, count)
	values := make([]float32, 0, count*dimension)
	for i := range ids {
		ids[i] = int64(start + i)
		values = append(values, vector(start+i)...)
	}
	return []*schemapb.FieldData{
		{Type: schemapb.DataType_Int64, FieldName: "id", Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: ids}}}}},
		{Type: schemapb.DataType_FloatVector, FieldName: "vector", Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: dimension, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: values}}}}},
	}
}

func (w *worker) appendRecord(r record, started time.Time, err error) {
	r.At = started.UTC().Format(time.RFC3339Nano)
	r.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	r.LatencyMS = float64(time.Since(started)) / float64(time.Millisecond)
	r.GRPCCode = status.Code(err).String()
	w.mu.Lock()
	w.records = append(w.records, r)
	w.mu.Unlock()
}

func (w *worker) create(ctx context.Context) error {
	schema := &schemapb.CollectionSchema{
		Name: w.o.collection, Description: ownerDescription(w.o.collection), AutoID: false,
		Fields: []*schemapb.FieldSchema{
			{Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{Name: "vector", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: strconv.Itoa(dimension)}}},
		},
	}
	body, err := proto.Marshal(schema)
	if err != nil {
		return errors.New("cannot encode owned schema")
	}
	ctx, cancel := context.WithTimeout(ctx, w.o.timeout)
	defer cancel()
	s, err := w.api.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName: "default", CollectionName: w.o.collection,
		Schema: body, ShardsNum: 1, ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	if err != nil || !successful(s) {
		return errors.New("owned collection creation failed; inspect ownership before cleanup")
	}
	return nil
}

func (w *worker) describe(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, w.o.timeout)
	defer cancel()
	r, err := w.api.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{DbName: "default", CollectionName: w.o.collection})
	if err != nil || !successful(r.GetStatus()) || r.GetCollectionID() == 0 || r.GetSchema().GetDescription() != ownerDescription(w.o.collection) {
		return errors.New("owned collection identity/description unavailable")
	}
	w.metadata["collection_id"] = strconv.FormatInt(r.GetCollectionID(), 10)
	w.metadata["description"] = r.GetSchema().GetDescription()
	w.metadata["consistency"] = r.GetConsistencyLevel().String()
	return nil
}

func (w *worker) seed(ctx context.Context) error {
	for start := 0; start < w.o.rows; start += 512 {
		count := min(512, w.o.rows-start)
		began := time.Now()
		call, cancel := context.WithTimeout(ctx, w.o.timeout)
		r, err := w.api.Insert(call, &milvuspb.InsertRequest{DbName: "default", CollectionName: w.o.collection, NumRows: uint32(count), FieldsData: dataFields(start, count)})
		cancel()
		ok := err == nil && successful(r.GetStatus()) && r.GetInsertCnt() == int64(count) && len(r.GetIDs().GetIntId().GetData()) == count && r.GetTimestamp() != 0
		if ok {
			for i, id := range r.GetIDs().GetIntId().GetData() {
				if id != int64(start+i) {
					ok = false
					break
				}
			}
		}
		w.appendRecord(record{Operation: "insert", FirstID: start, Rows: count, OK: ok, AckTimestamp: r.GetTimestamp()}, began, err)
		if !ok {
			return errors.New("seed acknowledgement count/ID/timestamp invalid; partial acknowledgements retained")
		}
		w.acknowledged += count
	}
	call, cancel := context.WithTimeout(ctx, w.o.timeout)
	r, err := w.api.Flush(call, &milvuspb.FlushRequest{DbName: "default", CollectionNames: []string{w.o.collection}})
	cancel()
	if err != nil || !successful(r.GetStatus()) || r.GetCollFlushTs()[w.o.collection] == 0 {
		return errors.New("flush sealing acknowledgement failed")
	}
	ts := r.GetCollFlushTs()[w.o.collection]
	w.metadata["flush_timestamp"] = strconv.FormatUint(ts, 10)
	for {
		call, cancel = context.WithTimeout(ctx, w.o.timeout)
		state, e := w.api.GetFlushState(call, &milvuspb.GetFlushStateRequest{DbName: "default", CollectionName: w.o.collection, FlushTs: ts})
		cancel()
		if e != nil || !successful(state.GetStatus()) {
			return errors.New("completed flush state unavailable")
		}
		if state.GetFlushed() {
			break
		}
		if err := wait(ctx, time.Second); err != nil {
			return err
		}
	}
	call, cancel = context.WithTimeout(ctx, w.o.timeout)
	s, e := w.api.CreateIndex(call, &milvuspb.CreateIndexRequest{
		DbName: "default", CollectionName: w.o.collection, FieldName: "vector", IndexName: "vector_flat",
		ExtraParams: []*commonpb.KeyValuePair{{Key: "index_type", Value: "FLAT"}, {Key: "metric_type", Value: "L2"}},
	})
	cancel()
	if e != nil || !successful(s) {
		return errors.New("FLAT index creation failed")
	}
	for {
		call, cancel = context.WithTimeout(ctx, w.o.timeout)
		r, e := w.api.GetIndexState(call, &milvuspb.GetIndexStateRequest{DbName: "default", CollectionName: w.o.collection, IndexName: "vector_flat"})
		cancel()
		if e != nil || !successful(r.GetStatus()) || r.GetState() == commonpb.IndexState_Failed {
			return errors.New("FLAT index unavailable")
		}
		if r.GetState() == commonpb.IndexState_Finished {
			break
		}
		if err := wait(ctx, time.Second); err != nil {
			return err
		}
	}
	call, cancel = context.WithTimeout(ctx, w.o.timeout)
	s, e = w.api.LoadCollection(call, &milvuspb.LoadCollectionRequest{DbName: "default", CollectionName: w.o.collection, ReplicaNumber: 1})
	cancel()
	if e != nil || !successful(s) {
		return errors.New("owned collection loading failed")
	}
	for {
		call, cancel = context.WithTimeout(ctx, w.o.timeout)
		r, e := w.api.GetLoadingProgress(call, &milvuspb.GetLoadingProgressRequest{DbName: "default", CollectionName: w.o.collection})
		cancel()
		if e != nil || !successful(r.GetStatus()) {
			return errors.New("loading progress unavailable")
		}
		if r.GetProgress() == 100 {
			break
		}
		if err := wait(ctx, time.Second); err != nil {
			return err
		}
	}
	return w.inspect(ctx)
}

func wait(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (w *worker) inspect(ctx context.Context) error {
	if err := w.describe(ctx); err != nil {
		return err
	}
	call, cancel := context.WithTimeout(ctx, w.o.timeout)
	r, e := w.api.DescribeIndex(call, &milvuspb.DescribeIndexRequest{DbName: "default", CollectionName: w.o.collection, FieldName: "vector", IndexName: "vector_flat"})
	cancel()
	if e != nil || !successful(r.GetStatus()) || len(r.GetIndexDescriptions()) != 1 {
		return errors.New("FLAT index description unavailable")
	}
	index := r.GetIndexDescriptions()[0]
	params := map[string]string{}
	for _, p := range index.GetParams() {
		params[p.GetKey()] = p.GetValue()
	}
	if params["index_type"] != "FLAT" || params["metric_type"] != "L2" {
		return errors.New("selected query plan is not FLAT/L2")
	}
	w.metadata["index"] = map[string]any{"name": index.GetIndexName(), "params": params, "state": index.GetState().String()}
	call, cancel = context.WithTimeout(ctx, w.o.timeout)
	segments, e := w.api.GetQuerySegmentInfo(call, &milvuspb.GetQuerySegmentInfoRequest{DbName: "default", CollectionName: w.o.collection})
	cancel()
	if e != nil || !successful(segments.GetStatus()) {
		return errors.New("actual loaded-segment state unavailable")
	}
	rows := []map[string]any{}
	for _, s := range segments.GetInfos() {
		rows = append(rows, map[string]any{
			"segment_id":    strconv.FormatInt(s.GetSegmentID(), 10),
			"collection_id": strconv.FormatInt(s.GetCollectionID(), 10), "rows": s.GetNumRows(), "state": s.GetState().String(), "node_ids": s.GetNodeIds(), "index_name": s.GetIndexName(),
		})
	}
	w.metadata["segments"] = rows
	return nil
}

func searchInput(nq, rows int) ([]byte, error) {
	values := make([][]byte, nq)
	for i := range values {
		v := vector(i % rows)
		raw := make([]byte, dimension*4)
		for k, x := range v {
			binary.LittleEndian.PutUint32(raw[k*4:], math.Float32bits(x))
		}
		values[i] = raw
	}
	return proto.Marshal(&commonpb.PlaceholderGroup{Placeholders: []*commonpb.PlaceholderValue{{Tag: "$0", Type: commonpb.PlaceholderType_FloatVector, Values: values}}})
}

func validSearch(r *milvuspb.SearchResults, nq, rows int) bool {
	if !successful(r.GetStatus()) || r.GetResults().GetNumQueries() != int64(nq) || r.GetResults().GetTopK() != 1 {
		return false
	}
	x := r.GetResults()
	ids := x.GetIds().GetIntId().GetData()
	scores := x.GetScores()
	if len(ids) != nq || len(scores) != nq || len(x.GetTopks()) != nq {
		return false
	}
	for i, id := range ids {
		if id != int64(i%rows) || x.GetTopks()[i] != 1 || math.IsNaN(float64(scores[i])) || math.Abs(float64(scores[i])) > 0.001 {
			return false
		}
	}
	return true
}

func (w *worker) search(ctx context.Context) error {
	input, e := searchInput(w.o.nq, w.o.rows)
	if e != nil {
		return errors.New("cannot encode bounded search")
	}
	deadline := time.Now().Add(w.o.duration)
	var wg sync.WaitGroup
	claimed, failed := 0, false
	for range w.o.concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				w.mu.Lock()
				if claimed >= w.o.requests || time.Now().After(deadline) || ctx.Err() != nil {
					w.mu.Unlock()
					return
				}
				claimed++
				w.mu.Unlock()
				start := time.Now()
				call, cancel := context.WithTimeout(ctx, w.o.timeout)
				r, err := w.api.Search(call, &milvuspb.SearchRequest{
					DbName: "default", CollectionName: w.o.collection, Dsl: "id >= 0", DslType: commonpb.DslType_BoolExprV1,
					SearchInput: &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: input}, Nq: int64(w.o.nq), ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
					UseDefaultConsistency: false, SearchParams: []*commonpb.KeyValuePair{{Key: "anns_field", Value: "vector"}, {Key: "topk", Value: "1"}, {Key: "metric_type", Value: "L2"}, {Key: "params", Value: "{}"}},
				})
				cancel()
				ok := err == nil && validSearch(r, w.o.nq, w.o.rows)
				verified := 0
				if ok {
					verified = w.o.nq
				}
				w.appendRecord(record{Operation: "search", OK: ok, VerifiedNQ: verified}, start, err)
				if !ok {
					w.mu.Lock()
					failed = true
					w.mu.Unlock()
					return
				}
			}
		}()
	}
	wg.Wait()
	w.metadata["offered_requests"] = claimed
	if failed || ctx.Err() != nil {
		return errors.New("search transport/status or exact NQ/ID/score correctness failed")
	}
	return nil
}

func checkQuery(r *milvuspb.QueryResults, start, count int) bool {
	if !successful(r.GetStatus()) {
		return false
	}
	var ids []int64
	var vectors []float32
	for _, f := range r.GetFieldsData() {
		switch f.GetFieldName() {
		case "id":
			ids = f.GetScalars().GetLongData().GetData()
		case "vector":
			if f.GetVectors().GetDim() != dimension {
				return false
			}
			vectors = f.GetVectors().GetFloatVector().GetData()
		}
	}
	if len(ids) != count || len(vectors) != count*dimension {
		return false
	}
	seen := map[int64]bool{}
	for i, id := range ids {
		if id < int64(start) || id >= int64(start+count) || seen[id] {
			return false
		}
		seen[id] = true
		for k, x := range vector(int(id)) {
			if math.Float32bits(vectors[i*dimension+k]) != math.Float32bits(x) {
				return false
			}
		}
	}
	return true
}

func (w *worker) verify(ctx context.Context) error {
	for start := 0; start < w.o.rows; start += 512 {
		count := min(512, w.o.rows-start)
		began := time.Now()
		call, cancel := context.WithTimeout(ctx, w.o.timeout)
		r, err := w.api.Query(call, &milvuspb.QueryRequest{
			DbName: "default", CollectionName: w.o.collection,
			Expr: fmt.Sprintf("id >= %d && id < %d", start, start+count), OutputFields: []string{"id", "vector"},
			QueryParams: []*commonpb.KeyValuePair{{Key: "limit", Value: strconv.Itoa(count + 1)}}, ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
		})
		cancel()
		ok := err == nil && checkQuery(r, start, count)
		w.appendRecord(record{Operation: "verify", OK: ok, FirstID: start, Rows: count}, began, err)
		if !ok {
			return errors.New("acknowledged IDs/vector bytes differ")
		}
	}
	call, cancel := context.WithTimeout(ctx, w.o.timeout)
	defer cancel()
	r, err := w.api.Query(call, &milvuspb.QueryRequest{
		DbName: "default", CollectionName: w.o.collection, Expr: fmt.Sprintf("id < 0 || id >= %d", w.o.rows),
		OutputFields: []string{"id"}, QueryParams: []*commonpb.KeyValuePair{{Key: "limit", Value: "1"}}, ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	if err != nil || !successful(r.GetStatus()) {
		return errors.New("extra-ID control unavailable")
	}
	for _, f := range r.GetFieldsData() {
		if len(f.GetScalars().GetLongData().GetData()) != 0 {
			return errors.New("unexpected extra ID")
		}
	}
	return nil
}

func parse(argv []string) (options, error) {
	var o options
	f := flag.NewFlagSet("query-steps", flag.ContinueOnError)
	f.StringVar(&o.endpoint, "endpoint", "", "Explicit loopback gRPC endpoint")
	f.StringVar(&o.collection, "collection", "", "Unique run-owned query_accept collection")
	f.StringVar(&o.output, "output", "", "New local JSON receipt")
	f.StringVar(&o.mode, "mode", "inspect", "create|seed|inspect|search|verify")
	f.IntVar(&o.rows, "rows", 65536, "Dataset rows 1024..65536")
	f.IntVar(&o.nq, "nq", 1, "Search NQ 1..16384, topK=1, FLAT/L2")
	f.IntVar(&o.concurrency, "concurrency", 1, "Closed-loop in-flight requests 1..16")
	f.IntVar(&o.requests, "requests", 96, "Maximum offered requests 1..1024")
	f.DurationVar(&o.duration, "duration", 60*time.Second, "Maximum offering duration 1s..120s; then drain")
	f.DurationVar(&o.timeout, "timeout", 45*time.Second, "Per RPC deadline 1s..60s")
	f.BoolVar(&o.execute, "execute", false, "Opt in to owned collection requests")
	f.BoolVar(&o.parentPipe, "parent-pipe", false, "Cancel on parent stdin EOF")
	if err := f.Parse(argv); err != nil {
		return o, err
	}
	host, port, err := net.SplitHostPort(o.endpoint)
	n, pe := strconv.Atoi(port)
	if err != nil || pe != nil || host != "127.0.0.1" || n < 1 || n > 65535 || !regexp.MustCompile(`^query_accept_[0-9]{8}T[0-9]{6}Z_[0-9a-f]{8}$`).MatchString(o.collection) || o.output == "" || f.NArg() != 0 {
		return o, errors.New("explicit owned collection, loopback endpoint and new output required")
	}
	if o.mode != "create" && o.mode != "seed" && o.mode != "inspect" && o.mode != "search" && o.mode != "verify" {
		return o, errors.New("unsupported mode")
	}
	if o.rows < 1024 || o.rows > 65536 || o.nq < 1 || o.nq > 16384 || o.concurrency < 1 || o.concurrency > 16 || o.requests < 1 || o.requests > 1024 || o.duration < time.Second || o.duration > 120*time.Second || o.timeout < time.Second || o.timeout > 60*time.Second {
		return o, errors.New("bounded dataset/request/deadline selection required")
	}
	return o, nil
}

func execute(o options) (err error) {
	if !o.execute {
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"execute": false, "mode": o.mode, "rows": o.rows, "dimension": dimension, "nq": o.nq, "concurrency": o.concurrency, "max_requests": o.requests})
	}
	f, err := os.OpenFile(o.output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return errors.New("receipt already exists or cannot be created")
	}
	defer f.Close()
	conn, err := grpc.NewClient(o.endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(8<<20), grpc.MaxCallSendMsgSize(32<<20)))
	if err != nil {
		return errors.New("client initialization failed")
	}
	defer conn.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 600*time.Second)
	defer cancel()
	if o.parentPipe {
		go cancelOnEOF(os.Stdin, cancel)
	}
	w := &worker{api: milvuspb.NewMilvusServiceClient(conn), o: o, metadata: map[string]any{}}
	start := time.Now()
	defer func() {
		err = errors.Join(err, json.NewEncoder(f).Encode(map[string]any{
			"schema_version": "wpdiag.query-steps-workload/v1", "mode": o.mode,
			"collection": o.collection, "started_at": start.UTC().Format(time.RFC3339Nano), "finished_at": time.Now().UTC().Format(time.RFC3339Nano), "passed": err == nil,
			"rows": o.rows, "dimension": dimension, "nq": o.nq, "topk": 1, "consistency": "Strong", "plan": "FLAT/L2; id >= 0; exact vector self-match",
			"concurrency": o.concurrency, "max_requests": o.requests, "offering_duration_ms": o.duration.Milliseconds(), "acknowledged_rows": w.acknowledged, "metadata": w.metadata, "records": w.records,
		}))
	}()
	if o.mode == "create" {
		if err = w.create(ctx); err == nil {
			err = w.describe(ctx)
		}
		return err
	}
	if err = w.describe(ctx); err != nil {
		return err
	}
	switch o.mode {
	case "seed":
		return w.seed(ctx)
	case "inspect":
		return w.inspect(ctx)
	case "verify":
		return w.verify(ctx)
	case "search":
		return w.search(ctx)
	}
	return errors.New("unsupported mode")
}

func main() {
	o, err := parse(os.Args[1:])
	if err == nil {
		err = execute(o)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func cancelOnEOF(reader io.Reader, cancel context.CancelFunc) {
	_, _ = io.Copy(io.Discard, reader)
	cancel()
}
