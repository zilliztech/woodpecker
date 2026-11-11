// Native acceptance uses real WP RPCs and an HTTP transport gate, never synthetic metrics.
package main

import (
	"context"
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
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/zilliztech/woodpecker/proto"
)

var ownerPattern = regexp.MustCompile(`^native-[0-9]{8}t[0-9]{6}z-[a-f0-9]{8}$`)

type receipt struct {
	Operation    string    `json:"operation"`
	LogID        int64     `json:"log_id"`
	SegmentID    int64     `json:"segment_id"`
	StartedAt    time.Time `json:"started_at"`
	FinishedAt   time.Time `json:"finished_at"`
	OK           bool      `json:"ok"`
	Acknowledged []int64   `json:"synced_entry_ids,omitempty"`
	Verified     []int64   `json:"verified_entry_ids,omitempty"`
	EOF          bool      `json:"eof_observed,omitempty"`
}

func payload(owner string, log, segment, entry int64) []byte {
	return []byte(fmt.Sprintf("%s/log/%d/segment/%d/entry/%d", owner, log, segment, entry))
}

func appendOne(ctx context.Context, c proto.LogStoreClient, owner string, log, segment, entry int64) error {
	stream, err := c.AddEntry(ctx, &proto.AddEntryRequest{
		BucketName: "native-acceptance", RootPath: owner, LogId: log,
		Entry: &proto.LogEntry{SegId: segment, EntryId: entry, Values: payload(owner, log, segment, entry)},
	})
	if err != nil {
		return errors.New("append transport failed")
	}
	buffered, synced := false, false
	for {
		r, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return errors.New("append acknowledgement failed")
		}
		if r.Status == nil || r.Status.Code != 0 || r.EntryId != entry {
			return errors.New("invalid append acknowledgement")
		}
		switch r.State {
		case proto.AddEntryState_Buffered:
			if buffered || synced {
				return errors.New("duplicate/reordered buffered acknowledgement")
			}
			buffered = true
		case proto.AddEntryState_Synced:
			if !buffered || synced {
				return errors.New("duplicate/reordered synced acknowledgement")
			}
			synced = true
		default:
			return errors.New("append was rejected")
		}
	}
	if !buffered || !synced {
		return errors.New("append stream ended without durability acknowledgement")
	}
	return nil
}

func readEntries(ctx context.Context, c proto.LogStoreClient, owner string, log, segment int64, count int, eof bool) error {
	from := int64(0)
	if eof {
		from = int64(count)
	}
	r, err := c.GetBatchEntriesAdv(ctx, &proto.GetBatchEntriesAdvRequest{
		BucketName: "native-acceptance", RootPath: owner,
		LogId: log, SegmentId: segment, FromEntryId: from, MaxEntries: int64(count + 1),
	})
	if err != nil {
		return errors.New("read transport failed")
	}
	if r.Status == nil {
		return errors.New("read status absent")
	}
	if eof {
		// Exact WP status codes, not an arbitrary failure/timeout accepted as EOF.
		if r.Status.Code != 2310 && r.Status.Code != 8102 {
			return fmt.Errorf("expected EOF/entry-not-found, got code %d", r.Status.Code)
		}
		return nil
	}
	if r.Status.Code != 0 || r.Result == nil || len(r.Result.Entries) != count {
		return errors.New("read count/status mismatch")
	}
	for i, e := range r.Result.Entries {
		if e.SegId != segment || e.EntryId != int64(i) || string(e.Values) != string(payload(owner, log, segment, int64(i))) {
			return errors.New("acknowledged payload/identity mismatch")
		}
	}
	return nil
}

func work(ctx context.Context, argv []string) error {
	f := flag.NewFlagSet("work", flag.ContinueOnError)
	endpoint := f.String("endpoint", "", "Explicit loopback WP gRPC port-forward")
	owner := f.String("owner", "", "Run-owned root path")
	op := f.String("operation", "", "append|read|eof")
	log := f.Int64("log", 1001, "Owned log: 1001 or 1002")
	segment := f.Int64("segment", 1, "First owned segment (1..64)")
	count := f.Int("entries", 4, "Entries per segment (1..8)")
	parallel := f.Int("parallel", 1, "Concurrent requests (1..20); append uses distinct segments")
	deadline := f.Duration("timeout", 60*time.Second, "Whole worker timeout (1s..90s)")
	if err := f.Parse(argv); err != nil {
		return err
	}
	host, port, err := net.SplitHostPort(*endpoint)
	n, perr := strconv.Atoi(port)
	if err != nil || perr != nil || n < 1 || n > 65535 || host != "127.0.0.1" || !ownerPattern.MatchString(*owner) ||
		(*op != "append" && *op != "read" && *op != "eof") || (*log != 1001 && *log != 1002) ||
		*segment < 1 || *segment > 64 || *count < 1 || *count > 8 || *parallel < 1 || *parallel > 20 ||
		(*op == "append" && *segment+int64(*parallel)-1 > 64) || *deadline < time.Second || *deadline > 90*time.Second || f.NArg() != 0 {
		return errors.New("invalid bounded native worker selection")
	}
	ctx, cancel := context.WithTimeout(ctx, *deadline)
	defer cancel()
	c, err := grpc.NewClient(*endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1<<20)))
	if err != nil {
		return errors.New("client setup failed")
	}
	defer c.Close()
	client := proto.NewLogStoreClient(c)
	var mu sync.Mutex
	var failed bool
	var wg sync.WaitGroup
	for i := 0; i < *parallel; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			seg := *segment
			if *op == "append" {
				seg += int64(i)
			}
			r := receipt{Operation: *op, LogID: *log, SegmentID: seg, StartedAt: time.Now().UTC()}
			var e error
			if *op == "append" {
				for j := 0; j < *count; j++ {
					e = appendOne(ctx, client, *owner, *log, seg, int64(j))
					if e != nil {
						break
					}
					r.Acknowledged = append(r.Acknowledged, int64(j))
				}
			} else {
				e = readEntries(ctx, client, *owner, *log, seg, *count, *op == "eof")
				if e == nil {
					r.EOF = *op == "eof"
					if !r.EOF {
						for j := 0; j < *count; j++ {
							r.Verified = append(r.Verified, int64(j))
						}
					}
				}
			}
			r.OK = e == nil
			r.FinishedAt = time.Now().UTC()
			mu.Lock()
			defer mu.Unlock()
			if !r.OK {
				failed = true
			}
			_ = json.NewEncoder(os.Stdout).Encode(r)
		}(i)
	}
	wg.Wait()
	if failed {
		return errors.New("native worker failed; inspect bounded receipts (private RPC errors withheld)")
	}
	return nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// A lost parent closes stdin. Both client requests and the gate then cancel.
	if len(os.Args) > 1 && os.Args[1] == "work" {
		go func() { _, _ = io.Copy(io.Discard, os.Stdin); stop() }()
	}
	var err error
	if len(os.Args) < 2 {
		err = errors.New("expected gate or work")
	} else if os.Args[1] == "gate" {
		err = serveGate(ctx, os.Args[2:])
	} else if os.Args[1] == "work" {
		err = work(ctx, os.Args[2:])
	} else {
		err = errors.New("unsupported subcommand")
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
