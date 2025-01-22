package segment

import (
	"context"
	"github.com/milvus-io/woodpecker/common/bitset"
	"github.com/milvus-io/woodpecker/proto"
	"github.com/milvus-io/woodpecker/server/client"
	"github.com/milvus-io/woodpecker/server/segment"
	"sync"
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
}

func (op *AppendOp) Execute() {
	// 获取ES/WQ/AQ
	quorumInfo, err := op.handle.GetQuorumInfo(context.Background())
	if err != nil {
		op.callback(op.segmentId, op.entryId, err)
		return
	}
	// 根据clientPool获得client, 执行 WQ次 append操作 不同的 server
	for i := 0; i < len(quorumInfo.Nodes); i++ {
		client, clientErr := op.clientPool.GetLogStoreClient(quorumInfo.Nodes[i])
		if clientErr != nil {
			op.callback(op.segmentId, op.entryId, clientErr)
			return
		}
		go op.sendWriteRequest(client, i)
	}
}

func (op *AppendOp) sendWriteRequest(client client.LogStoreClient, serverIndex int) {
	id, err := client.AppendEntry(context.Background(), op.logId, op.toSegmentEntry())
	op.receivedAckCallback(id, err, serverIndex)
}

func (op *AppendOp) receivedAckCallback(entryId int64, err error, serverIndex int) {
	if err == nil {
		// set ackSet
		op.ackSet.Set(serverIndex)
	}
	if err != nil {
		op.handle.SendAppendErrorCallbacks(err)
	}
	if op.ackSet.Count() >= int(op.quorumInfo.Wq) {
		op.completed = true
		op.handle.SendAppendSuccessCallbacks()
	}
}

func (op *AppendOp) toSegmentEntry() *segment.SegmentEntry {
	return &segment.SegmentEntry{
		SegmentId: op.segmentId,
		EntryId:   op.entryId,
		Data:      op.value,
	}
}
