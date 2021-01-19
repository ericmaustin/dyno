package operation

import (
	"context"
	"github.com/ericmaustin/dyno"
	"sync"
	"time"
)


// NewBatch creates a new parallel execution batch
func NewBatch(ctx context.Context, maxRoutines int) *Batch {
	if maxRoutines == 0 {
		maxRoutines = 1
	}
	thisCtx, cancel := context.WithCancel(ctx)
	return &Batch{
		ctx:  thisCtx,
		done: cancel,
		wg:   &sync.WaitGroup{},
		mu:   &sync.RWMutex{},
		sem:  make(chan struct{}, maxRoutines),
	}
}

// Batch is a batch request
type Batch struct {
	ctx       context.Context
	done      context.CancelFunc
	wg        *sync.WaitGroup
	mu        *sync.RWMutex
	ops       []Operation
	sem       chan struct{}
	err       error
	running   bool
	opTimeout time.Duration
}

// IsRunning checks if the batch is currently running
func (b *Batch) IsRunning() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.running
}

func (b *Batch) setRunning(isRunning bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.running = isRunning
}

// SetOperationTimeout sets the timeout for each operation
func (b *Batch) SetOperationTimeout(duration time.Duration) *Batch {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.opTimeout = duration
	return b
}

// Error gets the error from the batch
func (b *Batch) Error() error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.err
}

func (b *Batch) setError(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.err = err
}

// AddOperations adds one or more operations to the batch
func (b *Batch) AddOperations(ops ...Operation) *Batch {
	b.wg.Add(len(ops))
	b.ops = append(b.ops, ops...)
	return b
}

// Execute runs the batch operation with the given request
func (b *Batch) Execute(sess *dyno.Session) error {
	b.setRunning(true)
	for _, op := range b.ops {
		select {
		case <-b.ctx.Done():
			b.setRunning(false)
			return b.Error()
		default:
			// pass
		}
		b.sem <- struct{}{}
		go func(op Operation) {
			var (
				req *dyno.Request
				err error
			)
			defer func() {
				b.wg.Done()
				<-b.sem
			}()
			if b.opTimeout > 0 {
				req = sess.RequestWithTimeout(b.opTimeout)
			} else {
				req = sess.Request()
			}
			err = op.ExecuteInBatch(req).Error()
			if err != nil {
				b.setError(err)
				b.ctx.Done()
			}
		}(op)
	}
	b.wg.Wait()
	return b.Error()
}