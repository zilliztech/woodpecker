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

// Real WP RPC workload with standard gRPC client instrumentation. No fixture
// spans or logs are imported; both endpoints emit their actual RPC telemetry.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"syscall"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/zilliztech/woodpecker/proto"
)

var ownerPattern = regexp.MustCompile(`^source-[0-9]{8}t[0-9]{6}z-[a-f0-9]{8}$`)

type receipt struct {
	Case       string    `json:"case"`
	TraceID    string    `json:"trace_id"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
	StatusCode int32     `json:"status_code"`
	Synced     bool      `json:"synced"`
	Verified   bool      `json:"verified"`
}

func selection(endpoint, owner string) bool {
	host, port, err := net.SplitHostPort(endpoint)
	n, pe := strconv.Atoi(port)
	return err == nil && pe == nil && host == "127.0.0.1" && n > 0 && n <= 65535 && ownerPattern.MatchString(owner)
}

func traced(ctx context.Context) (context.Context, string, error) {
	var traceBytes [16]byte
	var span [8]byte
	if _, err := rand.Read(traceBytes[:]); err != nil {
		return nil, "", err
	}
	if _, err := rand.Read(span[:]); err != nil {
		return nil, "", err
	}
	id := hex.EncodeToString(traceBytes[:])
	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID(traceBytes), SpanID: trace.SpanID(span), TraceFlags: trace.FlagsSampled, Remote: true})
	return trace.ContextWithRemoteSpanContext(ctx, sc), id, nil
}

func appendEntry(ctx context.Context, c proto.LogStoreClient, owner string, entry int64, empty bool, r *receipt) error {
	data := []byte(fmt.Sprintf("%s/entry/%d", owner, entry))
	if empty {
		data = nil
	}
	stream, err := c.AddEntry(ctx, &proto.AddEntryRequest{
		BucketName: "source-acceptance", RootPath: owner, LogId: 1001,
		Entry: &proto.LogEntry{SegId: 1, EntryId: entry, Values: data},
	})
	if err != nil {
		return errors.New("append transport failed")
	}
	buffered := false
	rejected := false
	for {
		response, receiveErr := stream.Recv()
		if receiveErr == io.EOF {
			break
		}
		if receiveErr != nil {
			if empty && rejected && r.StatusCode == 8101 {
				return nil // Expected rejection was actually received, not inferred from transport failure.
			}
			return errors.New("append stream interrupted")
		}
		if response.Status == nil {
			return errors.New("append status absent")
		}
		r.StatusCode = response.Status.Code
		if empty {
			if r.StatusCode != 8101 || response.State != proto.AddEntryState_Failed || rejected {
				return errors.New("empty entry was not explicitly rejected")
			}
			rejected = true
			continue
		}
		if r.StatusCode != 0 || response.EntryId != entry {
			return errors.New("append identity/status mismatch")
		}
		switch response.State {
		case proto.AddEntryState_Buffered:
			if buffered || r.Synced {
				return errors.New("reordered/duplicate buffered acknowledgement")
			}
			buffered = true
		case proto.AddEntryState_Synced:
			if !buffered || r.Synced {
				return errors.New("reordered/duplicate sync acknowledgement")
			}
			r.Synced = true
		default:
			return errors.New("unexpected append state")
		}
	}
	if empty && rejected || !empty && r.Synced {
		return nil
	}
	return errors.New("required acknowledgement absent")
}

func read(ctx context.Context, c proto.LogStoreClient, owner string, eof bool, r *receipt) error {
	from := int64(0)
	if eof {
		from = 2
	}
	response, err := c.GetBatchEntriesAdv(ctx, &proto.GetBatchEntriesAdvRequest{
		BucketName: "source-acceptance", RootPath: owner,
		LogId: 1001, SegmentId: 1, FromEntryId: from, MaxEntries: 3,
	})
	if err != nil || response.Status == nil {
		return errors.New("read transport/status failed")
	}
	r.StatusCode = response.Status.Code
	if eof {
		if r.StatusCode != 2310 && r.StatusCode != 8102 {
			return errors.New("read did not return exact EOF/entry-not-found status")
		}
		r.Verified = true
		return nil
	}
	if r.StatusCode != 0 || response.Result == nil || len(response.Result.Entries) != 2 {
		return errors.New("read count/status mismatch")
	}
	for i, entry := range response.Result.Entries {
		if entry.EntryId != int64(i) || entry.SegId != 1 || string(entry.Values) != fmt.Sprintf("%s/entry/%d", owner, i) {
			return errors.New("read payload differs from acknowledged entry")
		}
	}
	r.Verified = true
	return nil
}

func run(ctx context.Context, endpoint, owner string, output io.Writer) error {
	cases := []string{"baseline"}
	for i := 0; i < 10; i++ {
		cases = append(cases, fmt.Sprintf("rejected_empty_append_%d", i))
	}
	return runCases(ctx, endpoint, owner, output, append(cases, "recovery", "read", "eof"))
}

func verify(ctx context.Context, endpoint, owner string, output io.Writer) error {
	return runCases(ctx, endpoint, owner, output, []string{"read", "eof"})
}

func runCases(ctx context.Context, endpoint, owner string, output io.Writer, cases []string) error {
	if !selection(endpoint, owner) {
		return errors.New("require explicit localhost endpoint and owned source fixture")
	}
	c, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor()), grpc.WithStreamInterceptor(otelgrpc.StreamClientInterceptor()))
	if err != nil {
		return errors.New("client setup failed")
	}
	defer c.Close()
	client := proto.NewLogStoreClient(c)
	for _, name := range cases {
		requestCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		requestCtx, id, traceErr := traced(requestCtx)
		if traceErr != nil {
			cancel()
			return traceErr
		}
		r := receipt{Case: name, TraceID: id, StartedAt: time.Now().UTC()}
		switch name {
		case "baseline":
			err = appendEntry(requestCtx, client, owner, 0, false, &r)
		case "recovery":
			err = appendEntry(requestCtx, client, owner, 1, false, &r)
		case "read", "eof":
			err = read(requestCtx, client, owner, name == "eof", &r)
		default:
			err = appendEntry(requestCtx, client, owner, 1, true, &r)
		}
		cancel()
		r.FinishedAt = time.Now().UTC()
		if e := json.NewEncoder(output).Encode(r); e != nil {
			return e
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func cli() int {
	endpoint := flag.String("endpoint", "", "Explicit loopback WP gRPC port-forward")
	owner := flag.String("owner", "", "Run-owned fixture root")
	otlp := flag.String("otlp", "", "Explicit loopback OTLP HTTP host:port")
	execute := flag.Bool("execute", false, "Opt in to two acknowledged entries and ten rejected empty-entry attempts")
	verifyOnly := flag.Bool("verify-only", false, "Only verify the two previously acknowledged values and EOF; never append")
	idle := flag.Bool("idle", false, "Keep the isolated fixture worker ready for explicit exec")
	flag.Parse()
	if *idle && !*verifyOnly && !*execute && *endpoint == "" && *owner == "" && *otlp == "" && flag.NArg() == 0 {
		ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer cancel()
		<-ctx.Done()
		return 0
	}
	if !*execute || flag.NArg() != 0 || !selection(*endpoint, *owner) || !selection(*otlp, *owner) {
		fmt.Fprintln(os.Stderr, "Require --execute and explicit owned fixture")
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpoint(*otlp), otlptracehttp.WithInsecure())
	if err != nil {
		fmt.Fprintln(os.Stderr, "OTLP client initialization failed")
		return 1
	}
	provider := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter), sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceNameKey.String(*owner+"-client"))))
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	defer func() {
		flushCtx, flushCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer flushCancel()
		_ = provider.Shutdown(flushCtx)
	}()
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	work := run
	if *verifyOnly {
		work = verify
	}
	if err := work(ctx, *endpoint, *owner, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}

func main() { os.Exit(cli()) }
