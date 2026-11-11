// Licensed to the LF AI & Data foundation under the Apache License, Version 2.0.
package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
)

// Flush acknowledges the server's sealing boundary; it does not wait for object
// persistence. The owning harness separately verifies actual sync completions.
func (w *worker) flush(ctx context.Context, id int64, ack uint64) error {
	ctx, cancel := context.WithTimeout(ctx, w.options.timeout)
	defer cancel()
	started := time.Now()
	r := record{Operation: "flush", ID: id, AckTimestamp: ack, At: started.UTC().Format(time.RFC3339Nano)}
	reply, err := w.api.Flush(ctx, &milvuspb.FlushRequest{DbName: "default", CollectionNames: []string{w.options.collection}})
	if err == nil && successful(reply.GetStatus()) {
		r.FlushTimestamp = reply.GetCollFlushTs()[w.options.collection]
		ids := reply.GetCollSegIDs()[w.options.collection].GetData()
		r.OK = r.FlushTimestamp != 0 && r.FlushTimestamp >= ack && len(ids) > 0 && len(ids) <= 128
		for _, segment := range ids {
			if segment <= 0 {
				r.OK = false
			}
			r.SegmentIDs = append(r.SegmentIDs, strconv.FormatInt(segment, 10))
		}
	}
	w.append(r, started, err)
	if !r.OK {
		return errors.New("flush transport/status or server boundary invalid; private response withheld")
	}
	return nil
}

func (w *worker) flushPhase(ctx context.Context) error {
	started := time.Now()
	var lastFlush time.Time
	for i := 0; i < w.options.batches; i++ {
		timer := time.NewTimer(time.Until(started.Add(time.Duration(i) * w.options.duration / time.Duration(w.options.batches))))
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.New("flush schedule cancelled")
		case <-timer.C:
		}
		id := int64(w.options.startID + i*max(1, w.options.rowsPerBatch))
		w.options.partition = fmt.Sprintf("persist_%02d", i%(w.options.batches/2))
		ack, err := w.insert(ctx, id)
		if err != nil {
			return err
		}
		for _, search := range []bool{false, true} {
			if err := w.read(ctx, id, ack, 0, search, true); err != nil {
				return err
			}
		}
		// Respect the default per-collection flush quota (one token per 10s).
		// Owned partitions across two shards materialize the source minimum.
		if (i+1)%(w.options.batches/2) == 0 {
			if !lastFlush.IsZero() {
				timer := time.NewTimer(time.Until(lastFlush.Add(w.options.flushGap)))
				select {
				case <-ctx.Done():
					timer.Stop()
					return errors.New("flush quota wait cancelled")
				case <-timer.C:
				}
			}
			if err := w.flush(ctx, id, ack); err != nil {
				return err
			}
			lastFlush = time.Now()
		}
	}
	return nil
}
