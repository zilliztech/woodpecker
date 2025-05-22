package segment

import (
	"sync"
)

// SequentialExecutor is a sequential append executor
type SequentialExecutor struct {
	appendOpsQueue chan *AppendOp
	wg             sync.WaitGroup
	closeOnce      sync.Once
}

// NewSequentialExecutor initializes a SequentialExecutor
func NewSequentialExecutor(bufferSize int) *SequentialExecutor {
	return &SequentialExecutor{
		appendOpsQueue: make(chan *AppendOp, bufferSize),
	}
}

// Start starts the sequential append executor
func (se *SequentialExecutor) Start() {
	go se.worker()
}

// worker executes the logic for each order
func (se *SequentialExecutor) worker() {
	for appendOp := range se.appendOpsQueue {
		appendOp.Execute()
		se.wg.Done()
	}
}

// Submit an op to the queue
func (se *SequentialExecutor) Submit(op *AppendOp) {
	se.wg.Add(1)
	se.appendOpsQueue <- op
}

// Stop stops the sequential append executor
func (se *SequentialExecutor) Stop() {
	se.closeOnce.Do(func() {
		close(se.appendOpsQueue)
	})
	se.wg.Wait()
}
