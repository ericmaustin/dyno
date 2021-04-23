package operation

import (
	"context"
	"github.com/ericmaustin/dyno"
	"sync"
)

//BatchStatus is the status of the Batch operation
type BatchStatus string

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

// OutputError returns the BatchCounts and the error from the BatchResult for convenience
func (b *BatchResult) OutputError() (*BatchCounts, error) {
	return b.output, b.err
}

//BatchCounts holds all number of Pending, Running, Successful, and Errors
// from this batch
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
	mu         *sync.RWMutex
	wg         *sync.WaitGroup
}

// NewBatch creates a batch from a slice of operations with a given context
func NewBatch(ctx context.Context, operations []Operation) *Batch {
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
		wg:         &sync.WaitGroup{},
	}
}

// Counts returns counts of all pending, running, and completed tasks in that order
func (b *Batch) Counts() *BatchCounts {
	counts := &BatchCounts{}
	for _, op := range b.operations {
		switch op.Status() {
		case StatusDone:
			counts.Success++
		case StatusRunning:
			counts.Running++
		case StatusError:
			counts.Errors++
		default:
			counts.Pending++
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

// SetWorkerCount sets the number of workers
func (b *Batch) SetWorkerCount(concurrency int) *Batch {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.workers = concurrency
	return b
}

// AddOperations adds one or more operations to the batch
func (b *Batch) AddOperations(ops ...Operation) *Batch {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.operations = append(b.operations, ops...)
	return b
}

// Execute executes batch operations
func (b *Batch) Execute(sess *dyno.Session) (out *BatchResult) {
	out = &BatchResult{}
	b.setRunning()
	defer func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		out.SetTiming(b.timing)
		out.output = b.Counts()
		if out.Error() != nil {
			b.status = StatusError
		} else {
			b.status = StatusDone
		}
	}()

	// workers = num operations if not set
	if b.workers <= 0 || len(b.operations) < b.workers {
		b.workers = len(b.operations)
	}

	operations := make(chan Operation)
	resCh := make(chan Result)

	go b.workEmitter(operations)

	for i := 0; i < b.workers; i++ {
		go b.worker(sess, operations, resCh)
	}

	cnt := 0
	for {
		select {
		case res := <-resCh:
			cnt++
			if res != nil && res.Error() != nil {
				b.done() // stop all routines and return the error
				out.err = res.Error()
				return
			}
		case <-b.ctx.Done():
			return
		}
	}
}

// workEmitter emits work to the workers
func (b *Batch) workEmitter(operations chan<- Operation) {
	defer func() {
		close(operations)
		b.mu.Lock()
		defer b.mu.Unlock()
		b.timing.done()
		b.done()
	}()
	for _, req := range b.operations {
		select {
		case <-b.ctx.Done():
			// early return if context is cancelled
			return
		default:
			//pass
		}
		b.wg.Add(1)
		operations <- req
	}
	b.wg.Wait()
}

// worker runs
func (b *Batch) worker(sess *dyno.Session, operations <-chan Operation, resCh chan<- Result) {
	for {
		select {
		case op, ok := <-operations:
			if !ok {
				// no more work
				return
			}
			resCh <- op.ExecuteInBatch(sess.Request())
			b.wg.Done()
		case <-b.ctx.Done():
			return
		}
	}
}
