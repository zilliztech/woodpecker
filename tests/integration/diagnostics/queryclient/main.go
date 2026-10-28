// Licensed to the LF AI & Data foundation under the Apache License, Version 2.0.
// This opted-in test client uses the official Milvus protobuf API. It is not a
// diagnosis command and never changes deployment configuration or Pod state.
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
	"sort"
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

type options struct {
	parentPipe                                bool
	endpoint, collection, output, mode, phase string
	startID, batches, parallel, rows          int
	rowsPerBatch                              int
	partition                                 string
	flushGap                                  time.Duration
	offsetMS                                  uint64
	duration, timeout                         time.Duration
	execute                                   bool
}

type record struct {
	Operation          string   `json:"operation"`
	Control            bool     `json:"strong_control"`
	ID                 int64    `json:"expected_id"`
	At                 string   `json:"at"`
	LatencyMS          float64  `json:"latency_ms"`
	OK                 bool     `json:"ok"`
	GRPCCode           string   `json:"grpc_code"`
	AckTimestamp       uint64   `json:"ack_timestamp,string"`
	GuaranteeTimestamp uint64   `json:"guarantee_timestamp,string"`
	Consistency        string   `json:"consistency"`
	FlushTimestamp     uint64   `json:"flush_timestamp,string,omitempty"`
	SegmentIDs         []string `json:"segment_ids,omitempty"`
	Rows               int      `json:"rows,omitempty"`
	IDs                []string `json:"ids,omitempty"`
	Partition          string   `json:"partition,omitempty"`
}

type worker struct {
	api     milvuspb.MilvusServiceClient
	options options
	mu      sync.Mutex
	records []record
}

func successful(s *commonpb.Status) bool {
	return s != nil && s.GetCode() == 0 && s.GetErrorCode() == commonpb.ErrorCode_Success
}

func cancelOnEOF(reader io.Reader, cancel context.CancelFunc) {
	_, _ = io.Copy(io.Discard, reader)
	cancel()
}

func guarantee(ts, offsetMS uint64) (uint64, error) {
	if ts == 0 || offsetMS > 5000 || ts > math.MaxUint64-(offsetMS<<18) {
		return 0, errors.New("invalid server timestamp or bounded fence offset")
	}
	return ts + (offsetMS << 18), nil
}

func vector(id int64) []float32 {
	v := make([]float32, 16)
	for k := range v {
		v[k] = float32((id*17+int64(k)*31)%251+1) / 251
	}
	return v
}

func fields(id int64) []*schemapb.FieldData {
	return []*schemapb.FieldData{
		{Type: schemapb.DataType_Int64, FieldName: "id", Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{id}}}}}},
		{Type: schemapb.DataType_FloatVector, FieldName: "vector", Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 16, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: vector(id)}}}}},
	}
}

func (w *worker) append(r record, started time.Time, err error) {
	r.LatencyMS = float64(time.Since(started)) / float64(time.Millisecond)
	r.GRPCCode = status.Code(err).String()
	w.mu.Lock()
	w.records = append(w.records, r)
	w.mu.Unlock()
}

func (w *worker) insert(ctx context.Context, id int64) (uint64, error) {
	ctx, cancel := context.WithTimeout(ctx, w.options.timeout)
	defer cancel()
	started := time.Now()
	r := record{Operation: "insert", ID: id, At: started.UTC().Format(time.RFC3339Nano), Partition: w.options.partition}
	n := max(1, w.options.rowsPerBatch)
	data := fields(id)
	ids, vectors := make([]int64, n), make([]float32, 0, n*16)
	for i := range ids {
		ids[i] = id + int64(i)
		vectors = append(vectors, vector(ids[i])...)
	}
	data[0].GetScalars().GetLongData().Data = ids
	data[1].GetVectors().GetFloatVector().Data = vectors
	reply, err := w.api.Insert(ctx, &milvuspb.InsertRequest{DbName: "default", CollectionName: w.options.collection, PartitionName: w.options.partition, NumRows: uint32(n), FieldsData: data})
	actual := reply.GetIDs().GetIntId().GetData()
	valid := len(actual) == n
	for i, value := range actual {
		if i >= n || value != ids[i] {
			valid = false
		}
	}
	if err == nil && successful(reply.GetStatus()) && reply.GetInsertCnt() == int64(n) && valid && reply.GetTimestamp() != 0 {
		r.OK, r.AckTimestamp = true, reply.GetTimestamp()
	}
	if n > 1 {
		r.Rows = n
		for _, identity := range actual {
			r.IDs = append(r.IDs, strconv.FormatInt(identity, 10))
		}
	}
	w.append(r, started, err)
	if !r.OK {
		return 0, errors.New("marker insert failed or acknowledgement identity/timestamp invalid; private response withheld")
	}
	return r.AckTimestamp, nil
}

func queryIDs(reply *milvuspb.QueryResults) []int64 {
	for _, field := range reply.GetFieldsData() {
		if field.GetFieldName() == "id" && field.GetType() == schemapb.DataType_Int64 {
			return field.GetScalars().GetLongData().GetData()
		}
	}
	return nil
}

func (w *worker) read(ctx context.Context, id int64, ts, fence uint64, search, control bool) error {
	ctx, cancel := context.WithTimeout(ctx, w.options.timeout)
	defer cancel()
	consistency := commonpb.ConsistencyLevel_Strong
	if fence != 0 {
		consistency = commonpb.ConsistencyLevel_Customized
	}
	started := time.Now()
	r := record{Operation: "query", Control: control, ID: id, At: started.UTC().Format(time.RFC3339Nano),
		AckTimestamp: ts, GuaranteeTimestamp: fence, Consistency: consistency.String()}
	expr := fmt.Sprintf("id == %d", id)
	var err error
	if search {
		r.Operation = "search"
		v := vector(id)
		bytes := make([]byte, len(v)*4)
		for i, value := range v {
			binary.LittleEndian.PutUint32(bytes[i*4:], math.Float32bits(value))
		}
		group, encodeErr := proto.Marshal(&commonpb.PlaceholderGroup{Placeholders: []*commonpb.PlaceholderValue{{Tag: "$0", Type: commonpb.PlaceholderType_FloatVector, Values: [][]byte{bytes}}}})
		if encodeErr != nil {
			return errors.New("placeholder encoding failed")
		}
		var reply *milvuspb.SearchResults
		reply, err = w.api.Search(ctx, &milvuspb.SearchRequest{DbName: "default", CollectionName: w.options.collection, Dsl: expr,
			DslType: commonpb.DslType_BoolExprV1, SearchInput: &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: group}, Nq: 1,
			OutputFields: []string{"id"}, ConsistencyLevel: consistency, UseDefaultConsistency: false, GuaranteeTimestamp: fence,
			SearchParams: []*commonpb.KeyValuePair{{Key: "anns_field", Value: "vector"}, {Key: "topk", Value: "1"}, {Key: "metric_type", Value: "COSINE"}, {Key: "params", Value: "{}"}}})
		ids := reply.GetResults().GetIds().GetIntId().GetData()
		r.OK = err == nil && successful(reply.GetStatus()) && reply.GetResults().GetNumQueries() == 1 && len(ids) == 1 && ids[0] == id
	} else {
		var reply *milvuspb.QueryResults
		reply, err = w.api.Query(ctx, &milvuspb.QueryRequest{DbName: "default", CollectionName: w.options.collection, Expr: expr,
			OutputFields: []string{"id"}, QueryParams: []*commonpb.KeyValuePair{{Key: "limit", Value: "1"}},
			ConsistencyLevel: consistency, UseDefaultConsistency: false, GuaranteeTimestamp: fence})
		ids := queryIDs(reply)
		r.OK = err == nil && successful(reply.GetStatus()) && len(ids) == 1 && ids[0] == id
	}
	w.append(r, started, err)
	if !r.OK {
		return errors.New("query/search transport, status or exact-ID validation failed; private response withheld")
	}
	return nil
}

func (w *worker) batch(ctx context.Context, id int64) error {
	ts, err := w.insert(ctx, id)
	if err != nil {
		return err
	}
	var fence uint64
	if w.options.phase == "future" {
		fence, err = guarantee(ts, w.options.offsetMS)
		if err != nil {
			return err
		}
	}
	var wg sync.WaitGroup
	errorsFound := make(chan error, 4)
	for _, search := range []bool{false, true} {
		for _, control := range []bool{false, true} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				requested := fence
				if control {
					requested = 0
				}
				if err := w.read(ctx, id, ts, requested, search, control); err != nil {
					errorsFound <- err
				}
			}()
		}
	}
	wg.Wait()
	close(errorsFound)
	for err := range errorsFound {
		return err
	}
	return nil
}

func (w *worker) phase(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int)
	var wg sync.WaitGroup
	var failed bool
	var mu sync.Mutex
	for range w.options.parallel {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				if w.batch(ctx, int64(w.options.startID+i)) != nil {
					mu.Lock()
					failed = true
					mu.Unlock()
					cancel()
				}
			}
		}()
	}
	started := time.Now()
schedule:
	for i := 0; i < w.options.batches; i++ {
		timer := time.NewTimer(time.Until(started.Add(time.Duration(i) * w.options.duration / time.Duration(w.options.batches))))
		select {
		case <-ctx.Done():
			timer.Stop()
			break schedule
		case <-timer.C:
		}
		select {
		case jobs <- i:
		case <-ctx.Done():
			break schedule
		}
	}
	close(jobs)
	wg.Wait()
	if failed || ctx.Err() != nil {
		return errors.New("bounded phase failed or cancelled")
	}
	return nil
}

func (w *worker) verify(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, w.options.timeout)
	defer cancel()
	reply, err := w.api.Query(ctx, &milvuspb.QueryRequest{DbName: "default", CollectionName: w.options.collection, Expr: "id >= 0",
		OutputFields: []string{"id"}, QueryParams: []*commonpb.KeyValuePair{{Key: "limit", Value: strconv.Itoa(w.options.rows + 1)}}, ConsistencyLevel: commonpb.ConsistencyLevel_Strong})
	if err != nil || !successful(reply.GetStatus()) {
		return errors.New("exact-ID verification request failed")
	}
	ids := append([]int64(nil), queryIDs(reply)...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	if len(ids) != w.options.rows {
		return errors.New("acknowledged row count mismatch")
	}
	for i, id := range ids {
		if id != int64(i) {
			return errors.New("acknowledged ID set mismatch")
		}
	}
	return w.read(ctx, 0, 0, 0, true, true)
}

func parse() options {
	var o options
	flag.StringVar(&o.endpoint, "endpoint", "", "Explicit loopback host:port from the owned Kubernetes forward")
	flag.StringVar(&o.collection, "collection", "", "Existing uniquely owned query_accept or persist_accept collection")
	flag.StringVar(&o.output, "output", "", "New JSON receipt path")
	flag.StringVar(&o.mode, "mode", "phase", "phase, verify or flush (owned persistence workload)")
	flag.StringVar(&o.phase, "phase", "baseline", "baseline, future or recovery")
	flag.IntVar(&o.startID, "start-id", 64, "First new marker ID")
	flag.IntVar(&o.batches, "batches", 24, "Marker batches, 20..48")
	flag.IntVar(&o.parallel, "parallel", 2, "Concurrent marker batches, 1..4; persistence requires 1")
	flag.IntVar(&o.rows, "rows", 64, "Expected contiguous IDs: query 1..512, persistence 1..4096")
	flag.IntVar(&o.rowsPerBatch, "rows-per-batch", 1, "Rows per persistence batch only, 1..32")
	flag.Uint64Var(&o.offsetMS, "future-ms", 2000, "Bounded future fence, 1000..5000ms")
	flag.DurationVar(&o.duration, "duration", 30*time.Second, "Schedule duration: query 1s..60s, persistence 24s..60s")
	flag.DurationVar(&o.timeout, "timeout", 8*time.Second, "Per-RPC deadline, 1s..15s")
	flag.BoolVar(&o.execute, "execute", false, "Opt in to requests/owned marker data")
	flag.BoolVar(&o.parentPipe, "parent-pipe", false, "Cancel if the owning harness stdin pipe closes")
	flag.Parse()
	return o
}

func execute(o options) (err error) {
	if o.rowsPerBatch == 0 {
		o.rowsPerBatch = 1
	}
	host, port, e := net.SplitHostPort(o.endpoint)
	n, pErr := strconv.Atoi(port)
	if e != nil || pErr != nil || host != "127.0.0.1" || n < 1 || n > 65535 || !regexp.MustCompile(`^(query|persist)_accept_[0-9]{8}T[0-9]{6}Z_[0-9a-f]{8}$`).MatchString(o.collection) || o.output == "" {
		return errors.New("explicit owned collection, loopback endpoint and output required")
	}
	maxRows := 512
	if regexp.MustCompile(`^persist_accept_`).MatchString(o.collection) {
		maxRows = 4096
	}
	if (o.mode != "phase" && o.mode != "verify" && o.mode != "flush") || (o.phase != "baseline" && o.phase != "future" && o.phase != "recovery") || o.batches < 20 || o.batches > 48 || o.parallel < 1 || o.parallel > 4 || o.startID < 64 || o.startID > maxRows || o.startID+o.batches*o.rowsPerBatch > maxRows || o.rows < 1 || o.rows > maxRows || o.rowsPerBatch < 1 || o.rowsPerBatch > 32 || o.offsetMS < 1000 || o.offsetMS > 5000 || o.duration < time.Second || o.duration > time.Minute || o.timeout < time.Second || o.timeout > 15*time.Second {
		return errors.New("request/fence/row/deadline bounds invalid")
	}
	if (o.mode == "flush" && (!regexp.MustCompile(`^persist_accept_`).MatchString(o.collection) || o.phase == "future" || o.parallel != 1 || o.duration < 24*time.Second || o.batches%2 != 0)) || (o.mode == "phase" && (regexp.MustCompile(`^persist_accept_`).MatchString(o.collection) || o.rowsPerBatch != 1)) {
		return errors.New("persistence flush requires its owned collection, sequential batches and no future fence")
	}
	if !o.execute {
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"execute": false, "mode": o.mode, "phase": o.phase, "batches": o.batches, "parallel": o.parallel, "future_ms": o.offsetMS})
	}
	if o.mode == "flush" {
		o.flushGap = 11 * time.Second
	}
	file, err := os.OpenFile(o.output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return errors.New("cannot create a new receipt; existing artifacts are never overwritten")
	}
	defer file.Close()
	conn, err := grpc.NewClient(o.endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(4<<20)))
	if err != nil {
		return errors.New("fixture connection could not be constructed")
	}
	defer conn.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 180*time.Second)
	defer cancel()
	if o.parentPipe {
		go cancelOnEOF(os.Stdin, cancel)
	}
	w := worker{api: milvuspb.NewMilvusServiceClient(conn), options: o}
	started := time.Now().UTC().Format(time.RFC3339Nano)
	defer func() {
		sort.Slice(w.records, func(i, j int) bool { return w.records[i].At < w.records[j].At })
		r := map[string]any{"schema_version": "wpdiag.query-workload/v1", "phase": o.phase, "mode": o.mode, "collection": o.collection,
			"passed": err == nil, "started_at": started, "finished_at": time.Now().UTC().Format(time.RFC3339Nano), "records": w.records,
			"batches": o.batches, "first_marker_id": o.startID, "future_ms": o.offsetMS, "expected_rows": o.rows}
		if err != nil {
			r["failure"] = err.Error()
		}
		err = errors.Join(err, json.NewEncoder(file).Encode(r))
	}()
	if o.mode == "verify" {
		return w.verify(ctx)
	}
	if o.mode == "flush" {
		return w.flushPhase(ctx)
	}
	return w.phase(ctx)
}

func main() {
	if err := execute(parse()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
