package operation

import (
	"context"
	"github.com/ericmaustin/dyno"
	"sync"
)

type Executer func(req *dyno.Request) error

//BatchStatus is the status of the Batch operation
type BatchStatus string

//BatchCounts holds all number of Pending, Running, Successful, and Errors
// from this batch
type BatchCounts struct {
	Pending int
	Running int
	Success int
}

// Batch is a batch request
type Batch struct {
	ctx        context.Context
	done       context.CancelFunc
	operations []Executer
	workers    int
	mu         *sync.RWMutex
	wg         *sync.WaitGroup
	counts     *BatchCounts
	status     Status
}

// NewBatch creates a batch from a slice of operations with a given context
func NewBatch(ctx context.Context) *Batch {
	ctx, done := context.WithCancel(ctx)
	return &Batch{
		ctx: 		ctx,
		done: 		done,
		mu:         &sync.RWMutex{},
		wg:         &sync.WaitGroup{},
	}
}

// Counts returns counts of all pending, running, and completed tasks in that order
func (b *Batch) Counts() *BatchCounts {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.counts
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

// AddFunc adds one or more operations to the batch
func (b *Batch) AddFunc(ops ...Executer) *Batch {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.operations = append(b.operations, ops...)
	b.counts.Pending += len(ops)
	return b
}

// Execute executes batch operations
func (b *Batch) Execute(sess *dyno.Session) (*BatchCounts, error) {
	b.status = StatusRunning
	defer func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		b.status = StatusDone
	}()

	// workers = num operations if not set
	if b.workers <= 0 || len(b.operations) < b.workers {
		b.workers = len(b.operations)
	}

	funcs := make(chan Executer)
	resCh := make(chan error)

	go b.workEmitter(funcs)

	for i := 0; i < b.workers; i++ {
		go b.worker(sess, funcs, resCh)
	}

	for {
		select {
		case err := <-resCh:
			if err != nil {
				b.done()
				return b.counts, err
			}
		case <-b.ctx.Done():
			return b.counts, nil
		}
	}
}

// workEmitter emits work to the workers
func (b *Batch) workEmitter(funcs chan<- Executer) {
	defer func() {
		close(funcs)
		b.mu.Lock()
		defer b.mu.Unlock()
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
		b.counts.Running++
		funcs <- req
	}
	b.wg.Wait()
}

// worker runs
func (b *Batch) worker(sess *dyno.Session, funcChan <-chan Executer, resCh chan<- error) {
	for {
		select {
		case f, ok := <-funcChan:
			if !ok {
				// no more work
				return
			}
			resCh <- f(sess.Request())
			b.wg.Done()
		case <-b.ctx.Done():
			return
		}
	}
}
