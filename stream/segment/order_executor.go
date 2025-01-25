package segment

import (
	"sync"
)

type OrderExecutor struct {
	appendOpsQueue chan *AppendOp
	wg             sync.WaitGroup
}

// NewOrderExecutor 初始化一个 OrderExecutor
func NewOrderExecutor(bufferSize int) *OrderExecutor {
	return &OrderExecutor{
		appendOpsQueue: make(chan *AppendOp, bufferSize),
	}
}

// Start 启动订单执行器
func (oe *OrderExecutor) Start(workerCount int) {
	for i := 0; i < workerCount; i++ {
		go oe.worker(i)
	}
}

// worker 执行订单的具体逻辑
func (oe *OrderExecutor) worker(workerID int) {
	for appendOp := range oe.appendOpsQueue {
		oe.wg.Done()
		appendOp.Execute()
	}
}

// SubmitOrder 提交订单到队列
func (oe *OrderExecutor) Submit(op *AppendOp) {
	oe.wg.Add(1)
	oe.appendOpsQueue <- op
}

// Stop 停止订单执行器
func (oe *OrderExecutor) Stop() {
	oe.wg.Wait()
	close(oe.appendOpsQueue)
}
