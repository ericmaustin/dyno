package operation

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ericmaustin/dyno"
	"sync"
	"time"
)

var (
	ErrPoolOperationDidNotRun = errors.New("pool operation never ran")
)

//PoolFunc is a func that can be run in a Pool with Pool.Do()
type PoolFunc func(req *dyno.Request) (interface{}, error)

//poolOperationResult used as an intermediary result output for Pool workers
type poolOperationResult struct {
	err error
	out interface{}
}

//newPoolOperationResult creates a new poolOperationResult with a given output interface and error
func newPoolOperationResult(out interface{}, err error) *poolOperationResult {
	return &poolOperationResult{
		out: out,
		err: err,
	}
}

// poolOperation is a single pool operation used internally
type poolOperation struct {
	f       PoolFunc
	timeout *time.Duration
	resCh   chan<- *poolOperationResult
}

// PoolResult used used as the default result from a pool operation
type PoolResult struct {
	res   *poolOperationResult
	resCh <-chan *poolOperationResult
	sig   chan struct{}
	mu    *sync.Mutex
}

// Output waits for the output as an interface and the error
func (p *PoolResult) output() (interface{}, error) {
	if p.res != nil {
		return p.res.out, p.res.err
	}
	<-p.sig
	if p.res == nil {
		return nil, ErrPoolOperationDidNotRun
	}
	return p.res.out, p.res.err
}

// Output waits for the output as an interface and the error
func (p *PoolResult) Output() (interface{}, error) {
	return p.output()
}

// Error waits for the output and returns the error
func (p *PoolResult) Error() error {
	_, err := p.Output()
	return err
}

//wait waits for this result to be completed
func (p *PoolResult) wait() {
	defer func() {
		p.sig <- struct{}{}
		close(p.sig)
	}()
	r, ok := <-p.resCh
	if !ok {
		return
	}
	p.res = r
}

// NewPool creates a new pool with a given context and number of workers as an int
func NewPool(session *dyno.Session, workers int) *Pool {
	ctx, done := context.WithCancel(session.Ctx())
	p := &Pool{
		sess:    session,
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
	sess    *dyno.Session
	running bool
	ctx     context.Context
	done    context.CancelFunc
	workers int
	active  int64
	input   chan *poolOperation
	mu      *sync.RWMutex
	wg      *sync.WaitGroup
}

// IsRunning returns true if the pool is running
func (p *Pool) IsRunning() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.running
}

//setRunning sets the Pool.running flag to the given bool value
func (p *Pool) setRunning(running bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.running = running
}

//addActive adds a cnt to the active process count Pool.active
func (p *Pool) addActive(cnt int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.active += cnt
}

//subActive subtracts a cnt from the active process count Pool.active
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

//executeOperation executes a given poolOperation
func (p *Pool) executeOperation(op *poolOperation) {
	var req *dyno.Request
	// close the channel after result is passed
	p.addActive(1)
	defer func() {
		close(op.resCh)
		p.subActive(1)
	}()
	if op.timeout != nil {
		req = p.sess.RequestWithTimeout(*op.timeout)
	} else {
		req = p.sess.Request()
	}
	select {
	case op.resCh <- newPoolOperationResult(op.f(req)):
		// result done
	case <-p.ctx.Done():
		// context cancelled, exit early
	}
}

// Stop stops the worker pool
func (p *Pool) Stop() {
	p.done()    // cancel the context
	p.wg.Wait() // wait for workers to exit
}

// Do adds the operation to the pool with a given timeout time.Duration and returns a PoolResult
// panics if pool is not running
func (p *Pool) Do(f PoolFunc, timeout *time.Duration) *PoolResult {
	if !p.IsRunning() {
		panic(errors.New("pool is not running"))
	}
	resCh := make(chan *poolOperationResult)

	res := &PoolResult{
		resCh: resCh,
		sig:   make(chan struct{}, 1),
		mu:    &sync.Mutex{},
	}
	go res.wait() // PoolResult begins waiting for a result from the worker pool
	p.input <- &poolOperation{
		f:       f,
		timeout: timeout,
		resCh:   resCh,
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
	defer func() {
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

//Scan runs a Scan operation in this Pool with a given dynamodb.ScanInput, dyno.ScanHandler, and timeout
// if timeout is nil, no timeout is used
// returns a ScanResult
func (p *Pool) Scan(input *dynamodb.ScanInput, handler dyno.ScanHandler, timeout *time.Duration) *ScanResult {
	return &ScanResult{p.Do(ScanFunc(input, handler), timeout)}
}

//ScanAll runs multiple Scan operations in this Pool with a given slice of dynamodb.ScanInput, dyno.ScanHandler and timeout
// timeout is used for each scan request
// if timeout is nil, no timeout is used
// returns a ScanAllResult
func (p *Pool) ScanAll(inputs []*dynamodb.ScanInput, handler dyno.ScanHandler, timeout *time.Duration) *ScanAllResult {

	res := &ScanAllResult{
		results: make([]*ScanResult, len(inputs)),
	}

	for i, in := range inputs {
		res.results[i] = p.Scan(in, handler, timeout)
	}

	return res
}

//ScanCount runs a Scan operations in this Pool with a given slice of dynamodb.ScanInput, dyno.ScanHandler, and timeout
// if timeout is nil, no timeout is used
// returns a ScanCountResult
func (p *Pool) ScanCount(input *dynamodb.ScanInput, handler dyno.ScanHandler, timeout *time.Duration) *ScanCountResult {
	return &ScanCountResult{p.Do(ScanFunc(input, handler), timeout)}
}

//ScanCountAll runs multiple Scan operations in this Pool with a given slice of dynamodb.ScanInput, dyno.ScanHandler, and timeout
// if timeout is nil, no timeout is used
// returns a ScanCountAllResult
func (p *Pool) ScanCountAll(inputs []*dynamodb.ScanInput, handler dyno.ScanHandler, timeout *time.Duration) *ScanCountAllResult {

	res := &ScanCountAllResult{
		results: make([]*ScanCountResult, len(inputs)),
	}

	for i, in := range inputs {
		res.results[i] = p.ScanCount(in, handler, timeout)
	}

	return res
}

//GetItemTimeout runs a GetItem operation in this Pool with a given dynamodb.GetItemInput, dyno.GetItemHandler, and timeout
// if timeout is nil, no timeout is used
// returns a GetItemResult or an error if the Pool is in an invalid state
func (p *Pool) GetItemTimeout(input *dynamodb.ScanInput, handler dyno.ScanHandler, timeout *time.Duration) *GetItemResult {
	return &GetItemResult{p.Do(ScanFunc(input, handler), timeout)}
}

//BatchGetItem runs a BatchGetItem operation in this Pool with a given dynamodb.BatchGetItemInput, dyno.BatchGetItemHandler, and timeout
// if timeout is nil, no timeout is used
// returns a BatchGetItemResult
func (p *Pool) BatchGetItem(input *dynamodb.BatchGetItemInput, handler dyno.BatchGetItemHandler, timeout *time.Duration) *BatchGetItemResult {
	return &BatchGetItemResult{p.Do(BatchGetItemFunc(input, handler), timeout)}
}

//BatchGetItemAll runs multiple BatchGetItem operations in this Pool with a given slice of dynamodb.BatchGetItemInput,
//dyno.BatchGetItemHandler and timeout
// if timeout is nil, no timeout is used
// returns a BatchGetItemResult
func (p *Pool) BatchGetItemAll(inputs []*dynamodb.BatchGetItemInput, handler dyno.BatchGetItemHandler, timeout *time.Duration) *BatchGetItemAllResult {

	res := &BatchGetItemAllResult{
		results: make([]*BatchGetItemResult, len(inputs)),
	}

	for i, in := range inputs {
		res.results[i] = p.BatchGetItem(in, handler, timeout)
	}

	return res
}

//CreateTable runs a CreateTable operation in this Pool with a given dynamodb.CreateTableInput, dyno.CreateTableHandler and timeout
// if timeout is nil, no timeout is used
// returns a GetItemResult
func (p *Pool) CreateTable(input *dynamodb.CreateTableInput, handler dyno.CreateTableHandler, timeout *time.Duration) *CreateTableResult {
	return &CreateTableResult{p.Do(CreateTableFunc(input, handler), timeout)}
}

//DeleteTable runs a DeleteTable operation in this Pool with a given dynamodb.DeleteTableInput and dyno.DeleteTableHandler
// if timeout is nil, no timeout is used
// returns a GetItemResult or an error if the Pool is in an invalid state
func (p *Pool) DeleteTable(input *dynamodb.DeleteTableInput, handler dyno.DeleteTableHandler, timeout *time.Duration) *DeleteTableResult {
	return &DeleteTableResult{p.Do(DeleteTableFunc(input, handler), timeout)}
}

//ListenForTableDeletion runs a ListenForTableDeletion operation in this Pool with a given table name string and timeout
// returns a ListenForTableDeletionResult
func (p *Pool) ListenForTableDeletion(tableName string, timeout *time.Duration) *DeleteTableResult {
	return &DeleteTableResult{p.Do(ListenForTableDeletionFunc(tableName), timeout)}
}

//ListenForTableReady runs a ListenForTableReady operation in this Pool with a given table name string and timeout
// returns a ListenForTableDeletionResult
func (p *Pool) ListenForTableReady(tableName string, timeout *time.Duration) *ListenForTableReadyResult {
	return &ListenForTableReadyResult{p.Do(ListenForTableReadyFunc(tableName), timeout)}
}
