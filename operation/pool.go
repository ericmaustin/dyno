package operation

import (
	"context"
	"github.com/ericmaustin/dyno"
	"sync"
)

type PoolNotRunningError struct {}

func (e *PoolNotRunningError) Error() string {
	return "Pool is not running"
}

// poolOperation is a single pool operation used internally
type poolOperation struct {
	op    Operation
	req   *dyno.Request
	resCh chan<- Result
}

// PoolResult used used as the result from a pool operation
type PoolResult struct {
	res Result
	out <-chan Result
	mu *sync.Mutex
}

// Out returns the output of the result as soon as the task completes
func (p *PoolResult) Out() Result {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.res
}

func (p *PoolResult) wait() {
	p.mu.Lock()
	defer p.mu.Unlock()
	r, ok := <- p.out
	if !ok {
		return
	}
	p.res = r
	return
}

// NewPool creates a new pool with a given context and number of workers as an int
func NewPool(ctx context.Context, workers int) *Pool {
	ctx, done := context.WithCancel(ctx)
	p := &Pool{
		ctx:     ctx,
		done:    done,
		workers: workers,
		input:   make(chan *poolOperation),
		mu:      &sync.RWMutex{},
		wg:      &sync.WaitGroup{},
	}
	p.Start()
	return p
}

// Pool is a batch request handler that spawns a number of workers to handle requests
type Pool struct {
	running     bool
	ctx         context.Context
	done        context.CancelFunc
	workers     int
	active      int64
	input       chan *poolOperation
	mu          *sync.RWMutex
	wg          *sync.WaitGroup
}

// IsRunning returns true if the pool is running
func (p *Pool) IsRunning() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.running
}

func (p *Pool) setRunning(running bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.running = running
}

func (p *Pool) addActive(cnt int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.active += cnt
}

func (p *Pool) subActive(cnt int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.active -= cnt
}

// ActiveCount returns number of active operations
func (p *Pool) ActiveCount() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.active
}

func (p *Pool) executeOperation(op *poolOperation) {
	// close the channel after result is passed
	p.addActive(1)
	defer func() {
		close(op.resCh)
		p.subActive(1)
	}()
	select {
	case op.resCh<-op.op.ExecuteInBatch(op.req):
		// result done
	case <-p.ctx.Done():
		// context cancelled, exit early
	}
}

// Stop stops the worker pool
func (p *Pool) Stop() {
	p.done() // cancel the context
	p.wg.Wait() // wait for workers to exit
}


// Do adds the operation to the pool and returns a PoolResult
func (p *Pool) Do(op Operation, req *dyno.Request) (*PoolResult, error) {
	if !p.IsRunning() {
		return nil, &PoolNotRunningError{}
	}
	resCh := make(chan Result)
	res := &PoolResult{
		out: resCh,
		mu: &sync.Mutex{},
	}
	go res.wait() // PoolResult begins waiting for a result from the worker pool
	p.input <- &poolOperation{
		op:    op,
		resCh: resCh,
		req:   req,
	}
	return res, nil
}


// MustDo wraps Do and panics on error
func (p *Pool) MustDo(op Operation, req *dyno.Request) *PoolResult {
	res, err := p.Do(op, req)
	if err != nil {
		panic(err)
	}
	return res
}

// Start the pool and all workers
func (p *Pool) Start() {
	// set as running
	p.setRunning(true)

	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
}

// worker represents a operation execution worker
func (p *Pool) worker() {
	defer func(){
		p.setRunning(false)
		p.wg.Done()
	}()
	for {
		select {
		case op, ok := <-p.input:
			if !ok {
				// no more work
				return
			}
			p.executeOperation(op)
		case <-p.ctx.Done():
			return
		}
	}
}
