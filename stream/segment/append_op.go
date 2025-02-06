package segment

import (
	"context"
	"sync"

	"github.com/zilliztech/woodpecker/common/bitset"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/client"
	"github.com/zilliztech/woodpecker/server/segment"
)

type AppendOp struct {
	logId     int64
	segmentId int64
	entryId   int64
	value     []byte
	callback  func(segmentId int64, entryId int64, err error)

	sync.RWMutex
	clientPool client.LogStoreClientPool
	handle     SegmentHandle
	ackSet     *bitset.BitSet
	quorumInfo *proto.QuorumInfo
	completed  bool
	err        error
}

func (op *AppendOp) Execute() {
	// 获取ES/WQ/AQ
	quorumInfo, err := op.handle.GetQuorumInfo(context.Background())
	if err != nil {
		op.handle.SendAppendErrorCallbacks(op.entryId, err)
		return
	}
	// 根据clientPool获得client, 执行 WQ次 append操作 不同的 server
	for i := 0; i < len(quorumInfo.Nodes); i++ {
		client, clientErr := op.clientPool.GetLogStoreClient(quorumInfo.Nodes[i])
		if clientErr != nil {
			op.handle.SendAppendErrorCallbacks(op.entryId, err)
			return
		}
		op.sendWriteRequest(client, i)
	}
}

func (op *AppendOp) sendWriteRequest(client client.LogStoreClient, serverIndex int) {
	//fmt.Printf("send  %d request to server:%d \n", op.entryId, serverIndex)
	// order request
	entryId, syncedCh, err := client.AppendEntry(context.Background(), op.logId, op.toSegmentEntry())
	// async received ack without order
	go op.receivedAckCallback(entryId, syncedCh, err, serverIndex)
}

func (op *AppendOp) receivedAckCallback(entryId int64, syncedCh <-chan int64, err error, serverIndex int) {
	for {
		select {
		case syncedId, ok := <-syncedCh:
			if !ok {
				op.handle.SendAppendErrorCallbacks(entryId, err)
			}
			if syncedId != -1 && syncedId >= entryId {
				op.ackSet.Set(serverIndex)
				if op.ackSet.Count() >= int(op.quorumInfo.Wq) {
					op.completed = true
					op.handle.SendAppendSuccessCallbacks(entryId)
				}
				return
			}
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
