// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This opted-in fixture makes real REST requests and exports real client spans.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type record struct {
	Operation string  `json:"operation"`
	TraceID   string  `json:"trace_id"`
	SpanID    string  `json:"span_id"`
	At        string  `json:"at"`
	LatencyMS float64 `json:"client_latency_ms"`
	OK        bool    `json:"ok"`
}

type worker struct {
	client *http.Client
	tracer trace.Tracer
	base   string
	mu     sync.Mutex
	rows   []record
}

func (w *worker) call(ctx context.Context, path string, payload any) (json.RawMessage, error) {
	ctx, span := w.tracer.Start(ctx, "acceptance.client.request", trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.String("milvus.operation", path)))
	started := time.Now()
	r := record{Operation: path, TraceID: span.SpanContext().TraceID().String(), SpanID: span.SpanContext().SpanID().String(), At: started.UTC().Format(time.RFC3339Nano)}
	defer func() {
		r.LatencyMS = float64(time.Since(started)) / float64(time.Millisecond)
		span.End()
		w.mu.Lock()
		w.rows = append(w.rows, r)
		w.mu.Unlock()
	}()
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, errors.New("fixture payload could not be encoded")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.base+"/v2/vectordb/"+path, bytes.NewReader(body))
	if err != nil {
		return nil, errors.New("fixture request could not be constructed")
	}
	req.Header.Set("Content-Type", "application/json")
	propagation.TraceContext{}.Inject(ctx, propagation.HeaderCarrier(req.Header))
	resp, err := w.client.Do(req)
	if err != nil {
		span.SetStatus(codes.Error, "request failed; connection values withheld")
		return nil, errors.New("fixture HTTP request failed; connection values withheld")
	}
	defer resp.Body.Close()
	var reply struct {
		Code int             `json:"code"`
		Data json.RawMessage `json:"data"`
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil || len(raw) > 1<<20 || json.Unmarshal(raw, &reply) != nil || resp.StatusCode != http.StatusOK || reply.Code != 0 {
		span.SetStatus(codes.Error, "response unsuccessful; response body withheld")
		return nil, errors.New("fixture response unsuccessful; body withheld")
	}
	r.OK = true
	return reply.Data, nil
}

func execute() (resultErr error) {
	base := flag.String("milvus", "", "Explicit HTTP base of a disposable Milvus REST service")
	otlp := flag.String("otlp", "", "Local OTLP HTTP host:port (not the Jaeger query port)")
	output := flag.String("output", "", "Required local JSON receipt path")
	count := flag.Int("requests", 8, "Insert requests, 1..32")
	parallel := flag.Int("concurrency", 1, "Concurrent requests, 1..8")
	rows := flag.Int("rows", 16, "Rows per request, 1..64")
	dimension := flag.Int("dimension", 8, "Vector dimensions, 8..128")
	run := flag.Bool("execute", false, "Opt in to creating/reading/dropping one unique fixture collection")
	flag.Parse()
	u, err := url.Parse(*base)
	if err != nil || u.Scheme != "http" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || strings.Trim(u.Path, "/") != "" {
		return errors.New("milvus must be an explicit HTTP base without credentials, query or path")
	}
	if *count < 1 || *count > 32 || *parallel < 1 || *parallel > 8 || *rows < 1 || *rows > 64 || *dimension < 8 || *dimension > 128 || *output == "" {
		return errors.New("fixture request/worker/row/output bounds are invalid")
	}
	if !*run {
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"execute": false, "requests": *count, "rows_per_request": *rows, "concurrency": *parallel})
	}
	interrupt, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(interrupt, 2*time.Minute)
	defer cancel()
	exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpoint(*otlp), otlptracehttp.WithInsecure(), otlptracehttp.WithTimeout(5*time.Second))
	if err != nil {
		return errors.New("fixture exporter initialization failed")
	}
	provider := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter), sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", "wp-trace-acceptance-client"))))
	w := &worker{client: &http.Client{Timeout: 15 * time.Second, Transport: &http.Transport{}, CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}}, tracer: provider.Tracer("wpdiag.acceptance"), base: strings.TrimRight(*base, "/")}
	defer w.client.CloseIdleConnections()
	collection := "trace_accept_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	created := false
	report := map[string]any{"schema_version": "wpdiag.trace-workload/v1", "collection": collection, "passed": false, "failures": []string{}, "requests": *count, "rows_per_request": *rows, "dimension": *dimension, "concurrency": *parallel}
	defer func() {
		cleanup, done := context.WithTimeout(context.Background(), 20*time.Second)
		defer done()
		if created {
			_, dropErr := w.call(cleanup, "collections/drop", map[string]any{"collectionName": collection})
			report["collection_dropped"] = dropErr == nil
			if dropErr != nil {
				resultErr = errors.Join(resultErr, errors.New("owned collection cleanup failed"))
			}
		}
		if shutdownErr := provider.Shutdown(cleanup); shutdownErr != nil {
			resultErr = errors.Join(resultErr, errors.New("client span export shutdown failed"))
		}
		sort.Slice(w.rows, func(i, j int) bool { return w.rows[i].At < w.rows[j].At })
		report["records"], report["passed"] = w.rows, resultErr == nil
		if resultErr != nil {
			report["failures"] = []string{resultErr.Error()}
		}
		encoded, encodeErr := json.MarshalIndent(report, "", "  ")
		if encodeErr == nil {
			encodeErr = os.MkdirAll(filepath.Dir(*output), 0o700)
		}
		if encodeErr == nil {
			encodeErr = os.WriteFile(*output, append(encoded, '\n'), 0o600)
		}
		resultErr = errors.Join(resultErr, encodeErr)
	}()
	created = true // A create timeout can commit; the random name belongs only to this run.
	_, err = w.call(ctx, "collections/create", map[string]any{"collectionName": collection, "dimension": *dimension, "autoID": false, "metricType": "COSINE", "consistencyLevel": "Strong", "params": map[string]any{"shardsNum": 1}})
	if err != nil {
		return err
	}
	rng := rand.New(rand.NewSource(42))
	data := make([]map[string]any, *count**rows)
	for id := range data {
		vector := make([]float32, *dimension)
		for k := range vector {
			vector[k] = rng.Float32()
		}
		data[id] = map[string]any{"id": id, "vector": vector}
	}
	jobs := make(chan int)
	failures := make(chan error, *count)
	var workers sync.WaitGroup
	for range *parallel {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := range jobs {
				raw, callErr := w.call(ctx, "entities/insert", map[string]any{"collectionName": collection, "data": data[i**rows : (i+1)**rows]})
				var inserted struct {
					InsertCount int `json:"insertCount"`
				}
				if callErr == nil && (json.Unmarshal(raw, &inserted) != nil || inserted.InsertCount != *rows) {
					callErr = errors.New("acknowledged insert count differs from submitted rows")
				}
				if callErr != nil {
					failures <- callErr
				}
			}
		}()
	}
	for i := range *count {
		jobs <- i
	}
	close(jobs)
	workers.Wait()
	close(failures)
	for failure := range failures {
		resultErr = errors.Join(resultErr, failure)
	}
	if resultErr != nil {
		return resultErr
	}
	raw, err := w.call(ctx, "entities/query", map[string]any{"collectionName": collection, "filter": "id >= 0", "outputFields": []string{"id"}, "limit": len(data), "consistencyLevel": "Strong"})
	if err != nil {
		return err
	}
	var found []struct {
		ID int `json:"id"`
	}
	if json.Unmarshal(raw, &found) != nil || len(found) != len(data) {
		return errors.New("strong query count does not match acknowledged rows")
	}
	seen := map[int]bool{}
	for _, row := range found {
		if row.ID < 0 || row.ID >= len(data) || seen[row.ID] {
			return errors.New("strong query IDs do not match acknowledged IDs")
		}
		seen[row.ID] = true
	}
	raw, err = w.call(ctx, "entities/search", map[string]any{"collectionName": collection, "data": []any{data[0]["vector"]}, "filter": "id == 0", "limit": 1, "outputFields": []string{"id"}, "consistencyLevel": "Strong"})
	if err != nil {
		return err
	}
	if json.Unmarshal(raw, &found) != nil || len(found) != 1 || found[0].ID != 0 {
		return errors.New("acknowledged sample vector was not searchable")
	}
	report["acknowledged_rows"], report["strong_query_ids"], report["search_passed"] = len(data), len(seen), true
	return nil
}

func main() {
	if err := execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
