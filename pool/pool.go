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
	ErrPoolNotRunning = errors.New("pool is not running")
)

//PoolFunc is a func that can be run in a Pool with Pool.Do()
type PoolFunc func(req *dyno.Request) (interface{}, error)

type poolOperationResult struct {
	err error
	out interface{}
}

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

// Do adds the operation to the pool and returns a PoolResult
func (p *Pool) Do(f PoolFunc) (*PoolResult, error) {
	if !p.IsRunning() {
		return nil, ErrPoolNotRunning
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
		resCh:   resCh,
	}
	return res, nil
}

// DoTimeout adds the operation to the pool with a given timeout and returns a PoolResult
func (p *Pool) DoTimeout(f PoolFunc, timeout time.Duration) (*PoolResult, error) {
	if !p.IsRunning() {
		return nil, ErrPoolNotRunning
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
		timeout: &timeout,
		resCh:   resCh,
	}
	return res, nil
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

//Scan runs a Scan operation in this Pool with a given dynamodb.ScanInput and dyno.ScanHandler
// returns a ScanResult or an error if the Pool is in an invalid state
func (p *Pool) Scan(input *dynamodb.ScanInput, handler dyno.ScanHandler) (*ScanResult, error) {
	out, err := p.Do(ScanFunc(input, handler))
	return &ScanResult{out}, err
}

//ScanTimeout runs a Scan operation in this Pool with a given dynamodb.ScanInput, dyno.ScanHandler,
// and request timeout
// returns a ScanResult or an error if the Pool is in an invalid state
func (p *Pool) ScanTimeout(input *dynamodb.ScanInput, handler dyno.ScanHandler, timeout time.Duration) (*ScanResult, error) {
	out, err := p.DoTimeout(ScanFunc(input, handler), timeout)
	return &ScanResult{out}, err
}

//ScanAll runs multiple Scan operations in this Pool with a given slice of dynamodb.ScanInput and dyno.ScanHandler
// returns a ScanAllResult or an error if the Pool is in an invalid state
func (p *Pool) ScanAll(inputs []*dynamodb.ScanInput, handler dyno.ScanHandler) (*ScanAllResult, error) {

	var (
		merr MultiError
		err error
	)

	res := &ScanAllResult{
		results: make([]*ScanResult, len(inputs)),
	}

	for i, in := range inputs {
		res.results[i], err = p.Scan(in, handler)
		merr.AddError(err)
	}

	return res, merr.Return()
}

//ScanAllTimeout runs multiple Scan operations in this Pool with a given slice of dynamodb.ScanInput and dyno.ScanHandler,
// and request timeout
// returns a ScanAllResult or an error if the Pool is in an invalid state
func (p *Pool) ScanAllTimeout(inputs []*dynamodb.ScanInput, handler dyno.ScanHandler, timeout time.Duration) (*ScanAllResult, error) {

	var (
		merr MultiError
		err error
	)

	res := &ScanAllResult{
		results: make([]*ScanResult, len(inputs)),
	}

	for i, in := range inputs {
		res.results[i], err = p.ScanTimeout(in, handler, timeout)
		merr.AddError(err)
	}

	return res, merr.Return()
}


//ScanCount runs a Scan operations in this Pool with a given slice of dynamodb.ScanInput and dyno.ScanHandler
// returns a ScanCountResult or an error if the Pool is in an invalid state
func (p *Pool) ScanCount(input *dynamodb.ScanInput, handler dyno.ScanHandler) (*ScanCountResult, error) {
	out, err := p.Do(ScanFunc(input, handler))
	return &ScanCountResult{out}, err
}

//ScanCountTimeout runs a Scan operations in this Pool with a given slice of dynamodb.ScanInput and dyno.ScanHandler,
// and request timeout
// returns a ScanCountResult or an error if the Pool is in an invalid state
func (p *Pool) ScanCountTimeout(input *dynamodb.ScanInput, handler dyno.ScanHandler, timeout time.Duration) (*ScanCountResult, error) {
	out, err := p.DoTimeout(ScanFunc(input, handler), timeout)
	return &ScanCountResult{out}, err
}

//ScanCountAll runs multiple Scan operations in this Pool with a given slice of dynamodb.ScanInput and dyno.ScanHandler
// returns a ScanCountAllResult or an error if the Pool is in an invalid state
func (p *Pool) ScanCountAll(inputs []*dynamodb.ScanInput, handler dyno.ScanHandler) (*ScanCountAllResult, error) {

	var (
		merr MultiError
		err error
	)

	res := &ScanCountAllResult{
		results: make([]*ScanCountResult, len(inputs)),
	}

	for i, in := range inputs {
		res.results[i], err = p.ScanCount(in, handler)
		merr.AddError(err)
	}

	return res, merr.Return()
}


//ScanCountAllTimeout runs multiple Scan operations in this Pool with a given slice of dynamodb.ScanInput and dyno.ScanHandler,
// and request timeout
// returns a ScanCountAllResult or an error if the Pool is in an invalid state
func (p *Pool) ScanCountAllTimeout(inputs []*dynamodb.ScanInput, handler dyno.ScanHandler) (*ScanCountAllResult, error) {

	var (
		merr MultiError
		err error
	)

	res := &ScanCountAllResult{
		results: make([]*ScanCountResult, len(inputs)),
	}

	for i, in := range inputs {
		res.results[i], err = p.ScanCount(in, handler)
		merr.AddError(err)
	}

	return res, merr.Return()
}

//Get runs a Get operation in this Pool with a given dynamodb.GetItemInput and dyno.GetItemHandler
// returns a GetResult or an error if the Pool is in an invalid state
func (p *Pool) Get(input *dynamodb.ScanInput, handler dyno.ScanHandler) (*ScanResult, error) {
	out, err := p.Do(ScanFunc(input, handler))
	return &ScanResult{out}, err
}

//GetTimeout runs a Get operation in this Pool with a given dynamodb.GetItemInput and dyno.GetItemHandler,
// and request timeout
// returns a GetResult or an error if the Pool is in an invalid state
func (p *Pool) GetTimeout(input *dynamodb.ScanInput, handler dyno.ScanHandler, timeout time.Duration) (*ScanResult, error) {
	out, err := p.DoTimeout(ScanFunc(input, handler), timeout)
	return &ScanResult{out}, err
}

//BatchGetItem runs a BatchGetItem operation in this Pool with a given dynamodb.BatchGetItemInput and dyno.BatchGetItemHandler
// returns a BatchGetItemResult or an error if the Pool is in an invalid state
func (p *Pool) BatchGetItem(input *dynamodb.BatchGetItemInput, handler dyno.BatchGetItemHandler) (*BatchGetItemResult, error) {
	out, err := p.Do(BatchGetItemFunc(input, handler))
	return &BatchGetItemResult{out}, err
}


//BatchGetItemTimeout runs a BatchGetItem operation in this Pool with a given dynamodb.BatchGetItemInput and dyno.BatchGetItemHandler
// and request timeout
// returns a BatchGetItemResult or an error if the Pool is in an invalid state
func (p *Pool) BatchGetItemTimeout(input *dynamodb.BatchGetItemInput, handler dyno.BatchGetItemHandler, timeout time.Duration) (*BatchGetItemResult, error) {
	out, err := p.DoTimeout(BatchGetItemFunc(input, handler), timeout)
	return &BatchGetItemResult{out}, err
}

//BatchGetItemAll runs multiple BatchGetItem operations in this Pool with a given slice of dynamodb.BatchGetItemInput and dyno.BatchGetItemHandler
// returns a BatchGetItemResult or an error if the Pool is in an invalid state
func (p *Pool) BatchGetItemAll(inputs []*dynamodb.BatchGetItemInput, handler dyno.BatchGetItemHandler) (*BatchGetItemAllResult, error) {

	var (
		merr MultiError
		err error
	)

	res := &BatchGetItemAllResult{
		results: make([]*BatchGetItemResult, len(inputs)),
	}

	for i, in := range inputs {
		res.results[i], err = p.BatchGetItem(in, handler)
		merr.AddError(err)
	}

	return res, merr.Return()
}