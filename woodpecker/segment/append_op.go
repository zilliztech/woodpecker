package segment

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/zilliztech/woodpecker/common/bitset"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/client"
	"github.com/zilliztech/woodpecker/server/segment"
)

// AppendOp represents an operation to append data to a log segment.
// Once all LogStores have successfully acknowledged the append operation,
// it checks if it is at the head of the pending adds queue.
// If it is, it sends an acknowledgment back to the application.
// If a LogStore fails, it retries multiple times.
type AppendOp struct {
	logId     int64
	segmentId int64
	entryId   int64
	value     []byte
	callback  func(segmentId int64, entryId int64, err error)

	clientPool client.LogStoreClientPool
	handle     SegmentHandle
	ackSet     *bitset.BitSet
	quorumInfo *proto.QuorumInfo

	completed atomic.Bool
	err       error

	attempt int // attemptId
}

func NewAppendOp(logId int64, segmentId int64, entryId int64, value []byte, callback func(segmentId int64, entryId int64, err error),
	clientPool client.LogStoreClientPool, handle SegmentHandle, quorumInfo *proto.QuorumInfo, attempt int) *AppendOp {
	op := &AppendOp{
		logId:     logId,
		segmentId: segmentId,
		entryId:   entryId,
		value:     value,
		callback:  callback,

		clientPool: clientPool,
		handle:     handle,
		ackSet:     &bitset.BitSet{},
		quorumInfo: quorumInfo,

		attempt: attempt,
	}
	op.completed.Store(false)
	return op
}

func (op *AppendOp) Execute() {
	// get ES/WQ/AQ
	quorumInfo, err := op.handle.GetQuorumInfo(context.Background())
	if err != nil {
		op.err = err
		op.handle.SendAppendErrorCallbacks(op.entryId, err)
		return
	}

	for i := 0; i < len(quorumInfo.Nodes); i++ {
		// get client from clientPool according node addr
		cli, clientErr := op.clientPool.GetLogStoreClient(quorumInfo.Nodes[i])
		if clientErr != nil {
			op.err = clientErr
			op.handle.SendAppendErrorCallbacks(op.entryId, err)
			return
		}
		// send request to the node
		op.sendWriteRequest(cli, i)
	}
}

func (op *AppendOp) sendWriteRequest(client client.LogStoreClient, serverIndex int) {
	// order request
	entryId, syncedCh, err := client.AppendEntry(context.Background(), op.logId, op.toSegmentEntry())
	// async received ack without order
	go op.receivedAckCallback(entryId, syncedCh, err, serverIndex)
}

func (op *AppendOp) receivedAckCallback(entryId int64, syncedCh <-chan int64, err error, serverIndex int) {
	// sync call error, return directly
	if err != nil {
		op.err = err
		op.handle.SendAppendErrorCallbacks(op.entryId, err)
		return
	}
	// async call error, wait until syncedCh closed
	for {
		select {
		case syncedId, ok := <-syncedCh:
			if !ok {
				logger.Ctx(context.TODO()).Debug(fmt.Sprintf("synced chan for log:%d seg:%d entry:%d closed", op.logId, op.segmentId, op.entryId))
				return
			}
			if syncedId == -1 {
				logger.Ctx(context.TODO()).Debug(fmt.Sprintf("synced failed for log:%d seg:%d entry:%d", op.logId, op.segmentId, op.entryId))
				op.handle.SendAppendErrorCallbacks(op.entryId, werr.ErrSegmentWriteException)
				return
			}
			if syncedId != -1 && syncedId >= op.entryId {
				op.ackSet.Set(serverIndex)
				if op.ackSet.Count() >= int(op.quorumInfo.Wq) {
					op.completed.Store(true)
					op.handle.SendAppendSuccessCallbacks(op.entryId)
				}
				return
			}
			logger.Ctx(context.TODO()).Debug(fmt.Sprintf("synced recieved:%d for log:%d seg:%d entry:%d ,kepp async waiting", syncedId, op.logId, op.segmentId, op.entryId))
		}
	}
}

func (op *AppendOp) toSegmentEntry() *segment.SegmentEntry {
	return &segment.SegmentEntry{
		SegmentId: op.segmentId,
		EntryId:   op.entryId,
		Data:      op.value,
	}
}
