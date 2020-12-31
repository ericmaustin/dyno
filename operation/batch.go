package operation

import (
	"context"
	"github.com/ericmaustin/dyno"
	"sync"
	"time"
)

type BatchStatus string

const (
	BatchRunning = BatchStatus("RUNNING")
	BatchPending = BatchStatus("PENDING")
	BatchDone    = BatchStatus("DONE")
)

// BatchResult is the result of the batch request
type BatchResult struct {
	resultBase
	output *BatchCounts
}

// OutputInterface returns the BatchCounts from the BatchResult as an interface
func (b *BatchResult) OutputInterface() interface{} {
	return b.output
}

// Output returns the BatchCounts from the BatchResult
func (b *BatchResult) Output() *BatchCounts {
	return b.output
}

// OutputError returns the BatchCountse and the error from the BatchResult for convenience
func (b *BatchResult) OutputError() (*BatchCounts, error) {
	return b.output, b.err
}

type BatchCounts struct {
	Pending int
	Running int
	Success int
	Errors  int
}

// Batch is a batch request
type Batch struct {
	*baseOperation
	operations []Operation
	workers    int
	timeout    time.Duration
	mu         *sync.RWMutex
}

// NewBatch Creates a batch from a slice of operations
func NewBatch(operations []Operation) *Batch {
	if operations == nil {
		operations = make([]Operation, 0)
	}
	return &Batch{
		baseOperation: newBase(),
		operations:    operations,
		mu:            &sync.RWMutex{},
	}
}

// NewBatchWithContext a batch from a slice of operations with a given context
func NewBatchWithContext(ctx context.Context, operations []Operation) *Batch {
	if operations == nil {
		operations = make([]Operation, 0)
	}
	ctx, done := context.WithCancel(ctx)
	return &Batch{
		baseOperation: &baseOperation{
			ctx:    ctx,
			done:   done,
			mu:     sync.RWMutex{},
			status: StatusPending,
			timing: newTiming(),
		},
		operations: operations,
		mu:         &sync.RWMutex{},
	}
}

// Counts returns counts of all pending, running, and completed tasks in that order
func (b *Batch) Counts() *BatchCounts {
	counts := &BatchCounts{}
	for _, op := range b.operations {
		switch op.Status() {
		case StatusDone:
			counts.Success += 1
		case StatusRunning:
			counts.Running += 1
		case StatusError:
			counts.Errors += 1
		default:
			counts.Pending += 1
		}
	}
	return counts
}

// Cancel the request
func (b *Batch) Cancel() {
	b.done()
}

// Wait for all operations to finish
func (b *Batch) Wait() {
	<-b.ctx.Done()
}

// SetTimeout sets the timeout for the batch operation
func (b *Batch) SetTimeout(timeout time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.timeout = timeout
}

// SetWorkerCount sets the number of workers
func (b *Batch) SetWorkerCount(concurrency int) *Batch {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.workers = concurrency
	return b
}

// AddOperation adds an operation to the batch
func (b *Batch) AddOperation(op Operation) *Batch {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.operations = append(b.operations, op)
	return b
}

// Execute executes batch operations
func (b *Batch) Execute(req *dyno.Request) (out *BatchResult) {
	out = &BatchResult{}
	b.setRunning()
	defer func() {
		b.setDone(out)
		out.output = b.Counts()
	}()

	// workers = num operations if not set
	if b.workers <= 0 {
		b.workers = len(b.operations)
	}

	operations := make(chan Operation)
	resCh := make(chan Result)
	tasks := len(b.operations)

	go b.workEmitter(operations)

	for i := 0; i < b.workers; i++ {
		go b.worker(req, operations, resCh)
	}

	var timerCh <-chan time.Time

	if b.timeout > 0 {
		exeTimer := time.NewTimer(b.timeout)
		timerCh = exeTimer.C
	} else {
		timerCh = make(chan time.Time)
	}

	cnt := 0
	for {
		select {
		case res := <-resCh:
			cnt += 1
			if res != nil && res.Error() != nil {
				b.done() // stop all routines and return the error
				out.err = res.Error()
				return
			}
		case <-timerCh:
			b.done()
			out.err = dyno.Error{
				Code:    dyno.ErrBatchOperationTimedOut,
				Message: "batch execution timed putItemOutput",
			}
		case <-b.ctx.Done():
			out.err = dyno.Error{
				Code:    dyno.ErrBatchOutputContextCancelled,
				Message: "batch execution context was cancelled",
			}
		case <-req.Context().Done():
			b.done()
			out.err = dyno.Error{
				Code:    dyno.ErrRequestExecutionContextCancelled,
				Message: "batch execution request context was cancelled",
			}
		default:
			// keep looping
		}
		if cnt >= tasks {
			// all tasks done
			break
		}
	}
	return
}

// workEmitter emits work to the workers
func (b *Batch) workEmitter(operations chan<- Operation) {
	for _, req := range b.operations {
		select {
		case operations <- req:
			// do nothing
		case <-b.ctx.Done():
			// return early if we've cancelled the req
			return
		}
	}
}

// worker runs
func (b *Batch) worker(req *dyno.Request, operations <-chan Operation, resCh chan<- Result) {
	for {
		select {
		case op := <-operations:
			resCh <- op.ExecuteInBatch(req)
		case <-b.ctx.Done():
			return
		}
	}
}
