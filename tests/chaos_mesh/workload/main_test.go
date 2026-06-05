package workload

import (
	"context"
	"flag"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/etcd"
	"github.com/zilliztech/woodpecker/woodpecker"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

var (
	configFile = flag.String("config-file", "/tmp/test-config.yaml", "woodpecker config path")
	phase      = flag.String("phase", "warmup", "warmup|under-chaos|recovery|verify")
	recordFile = flag.String("record-file", "/tmp/wp-chaos-acked.jsonl", "acked records JSONL")
	numLogs    = flag.Int("logs", 8, "number of concurrent logs (1 writer each — refinement A)")
	windowSecs = flag.Int("window", 45, "load-phase duration seconds")
	logPrefix  = flag.String("log-prefix", "chaos-n152", "stable log name prefix across phases")
)

func newClient(ctx context.Context, t *testing.T) (woodpecker.Client, func()) {
	cfg, err := config.NewConfiguration(*configFile)
	require.NoError(t, err)
	etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	require.NoError(t, err)
	cli, err := woodpecker.NewClient(ctx, cfg, etcdCli, true)
	require.NoError(t, err)
	return cli, func() { _ = cli.Close(ctx) }
}

func TestNetworkChaosWorkload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	cli, closeFn := newClient(ctx, t)
	defer closeFn()
	switch *phase {
	case "warmup":
		runLoadPhase(ctx, t, cli, true)
	case "under-chaos", "recovery":
		runLoadPhase(ctx, t, cli, false)
	case "verify":
		runVerifyPhase(ctx, t, cli)
	default:
		t.Fatalf("unknown phase %q", *phase)
	}
}

type metrics struct{ ok, errRetryable, errPermanent int64 }

func runLoadPhase(ctx context.Context, t *testing.T, cli woodpecker.Client, truncate bool) {
	rec, err := openRecorder(*recordFile, truncate)
	require.NoError(t, err)
	defer rec.close()

	var m metrics
	deadline := time.Now().Add(time.Duration(*windowSecs) * time.Second)

	type lh struct {
		name   string
		id     int64
		handle log.LogHandle
		writer log.LogWriter
		seq    int64
	}
	logs := make([]*lh, *numLogs)
	for i := range logs {
		name := fmt.Sprintf("%s-%d", *logPrefix, i)
		_ = cli.CreateLog(ctx, name) // idempotent across phases
		h, err := cli.OpenLog(ctx, name)
		require.NoError(t, err)
		w, err := h.OpenLogWriter(ctx)
		require.NoError(t, err)
		logs[i] = &lh{name: name, id: h.GetId(), handle: h, writer: w}
	}

	var wg sync.WaitGroup
	// ---- one WRITER per log (refinement A: unambiguous ordering) ----
	for _, L := range logs {
		wg.Add(1)
		go func(L *lh) {
			defer wg.Done()
			for time.Now().Before(deadline) {
				seq := atomic.AddInt64(&L.seq, 1)
				payload := []byte(fmt.Sprintf("%s|%d|%d", L.name, time.Now().UnixNano(), seq))
				ph := payloadHash(payload)
				opCtx, opCancel := context.WithTimeout(ctx, 30*time.Second) // I4 bound
				res := <-L.writer.WriteAsync(opCtx, &log.WriteMessage{
					Payload:    payload,
					Properties: map[string]string{"hash": ph},
				})
				opCancel()
				if res.Err != nil {
					if isWoodpeckerPermanent(res.Err) {
						atomic.AddInt64(&m.errPermanent, 1)
						t.Errorf("I5 VIOLATION (write): permanent error under transient fault: %v", res.Err)
					} else {
						atomic.AddInt64(&m.errRetryable, 1) // acceptable degradation
					}
					continue
				}
				atomic.AddInt64(&m.ok, 1)
				if err := rec.append(AckRecord{
					LogName: L.name, LogId: L.id,
					SegmentId: res.LogMessageId.SegmentId, EntryId: res.LogMessageId.EntryId,
					PayloadHash: ph, Seq: seq,
				}); err != nil {
					t.Errorf("failed to persist ack record (logId=%d seg=%d entry=%d): %v", L.id, res.LogMessageId.SegmentId, res.LogMessageId.EntryId, err)
				}
			}
		}(L)
	}
	// ---- one tail READER per log (liveness / no-hang) ----
	for _, L := range logs {
		wg.Add(1)
		go func(L *lh) {
			defer wg.Done()
			earliest := log.EarliestLogMessageID()
			r, err := L.handle.OpenLogReader(ctx, &earliest, "chaos-tail-"+L.name)
			if err != nil {
				return
			}
			defer r.Close(ctx)
			for time.Now().Before(deadline) {
				rCtx, rCancel := context.WithTimeout(ctx, 30*time.Second)
				_, err := r.ReadNext(rCtx)
				rCancel()
				if err != nil && isWoodpeckerPermanent(err) {
					t.Errorf("I5 VIOLATION (read): permanent error under transient fault: %v", err)
				}
			}
		}(L)
	}
	wg.Wait()
	for _, L := range logs {
		_ = L.writer.Close(ctx)
	}

	require.Greater(t, m.ok, int64(0),
		"I7 VIOLATION: zero successful writes in phase %s (no forward progress)", *phase)
	t.Logf("phase=%s ok=%d retryableErr=%d permanentErr=%d",
		*phase, m.ok, m.errRetryable, m.errPermanent)
}

func runVerifyPhase(ctx context.Context, t *testing.T, cli woodpecker.Client) {
	recs, err := loadRecords(*recordFile)
	require.NoError(t, err)
	require.Greater(t, len(recs), 0, "no acked records to verify")

	byLog := map[string][]AckRecord{}
	for _, r := range recs {
		byLog[r.LogName] = append(byLog[r.LogName], r)
	}

	for name, want := range byLog {
		sort.Slice(want, func(i, j int) bool { return want[i].Seq < want[j].Seq })
		h, err := cli.OpenLog(ctx, name)
		require.NoError(t, err)
		earliest := log.EarliestLogMessageID()
		r, err := h.OpenLogReader(ctx, &earliest, "verify-"+name)
		require.NoError(t, err)

		ackedHash := map[[2]int64]string{}
		for _, w := range want {
			ackedHash[[2]int64{w.SegmentId, w.EntryId}] = w.PayloadHash
		}

		seen := map[[2]int64]bool{}
		var lastSeg, lastEntry int64 = -1, -1
		rctx, rcancel := context.WithTimeout(ctx, 5*time.Minute)
		for len(seen) < len(ackedHash) {
			msg, err := r.ReadNext(rctx)
			require.NoError(t, err, "I1/I4 read-back failed for %s (%d/%d)", name, len(seen), len(ackedHash))
			k := [2]int64{msg.Id.SegmentId, msg.Id.EntryId}
			// I2 ordering: strictly increasing (seg,entry) in read order (one writer per log).
			if msg.Id.SegmentId == lastSeg {
				require.Greater(t, msg.Id.EntryId, lastEntry, "I2 VIOLATION: non-increasing entryId in %s", name)
			} else if lastSeg >= 0 {
				require.Greater(t, msg.Id.SegmentId, lastSeg, "I2 VIOLATION: non-increasing segmentId in %s", name)
			}
			lastSeg, lastEntry = msg.Id.SegmentId, msg.Id.EntryId
			if wantHash, ok := ackedHash[k]; ok {
				require.Equal(t, wantHash, payloadHash(msg.Payload),
					"I3 VIOLATION: payload hash mismatch %s seg=%d entry=%d", name, k[0], k[1])
				require.Equal(t, wantHash, msg.Properties["hash"],
					"I3 VIOLATION: property hash mismatch %s", name)
				seen[k] = true
			}
		}
		rcancel()
		require.Equal(t, len(ackedHash), len(seen),
			"I1 VIOLATION: %d acked entries missing in %s", len(ackedHash)-len(seen), name)
		_ = r.Close(ctx)
		t.Logf("VERIFY %s: %d acked entries durable, ordered, hash-matched", name, len(seen))
	}
}
