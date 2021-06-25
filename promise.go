package dyno

import (
	"github.com/segmentio/ksuid"
	"sync"
	"sync/atomic"
	"time"
)

// PromiseError is an error returned by a Promise
type PromiseError string

// Error implements the error interface
func (e PromiseError) Error() string { return string(e) }

// ErrResultNotSet returned by Promise.GetResponse() when a promise was not set yet
const ErrResultNotSet = PromiseError("result not set")

//Promise represents a request promise
// a Promise is returned from any Client Execution
// use <-Promise.Done() to wait for a promise to be done
// use Promise.Await() to wait for and receive the output from a promise
type Promise struct {
	id        string
	mu        sync.RWMutex
	startTime time.Time
	endTime   time.Time
	flag      uint32
	value     interface{}
	err       error
	doneCh    chan struct{}
}

//NewPromise creates a new Promise with a startTime of Now
func NewPromise() *Promise {
	return &Promise{
		id:        ksuid.New().String(),
	}
}

func (p *Promise) Start() {
	p.mu.Lock()
	p.startTime = time.Now()
	p.mu.Unlock()
}

// Done returns a channel that will emit a struct{}{} when Promise contains a result, an error is encountered,
// or context was cancelled
func (p *Promise) Done() <-chan struct{} {
	p.mu.Lock()

	if p.doneCh == nil {

		p.doneCh = make(chan struct{})

		go func() {
			defer func() {
				p.doneCh <- struct{}{}
				close(p.doneCh)
			}()

			for {
				if atomic.LoadUint32(&p.flag) > 0 {
					// done!
					return
				}
			}
		}()
	}

	p.mu.Unlock()

	return p.doneCh
}

func (p *Promise) ID() string {
	return p.id
}

// SetResponse sets the response value interface and error
// and sets the flag to the done state
func (p *Promise) SetResponse(val interface{}, err error) {
	p.mu.Lock()
	p.value = val
	p.err = err
	p.endTime = time.Now()
	p.mu.Unlock()
	atomic.StoreUint32(&p.flag, 1)
}

// Await waits for the Promise's result to complete or for the context to be cancelled
// whichever comes first
func (p *Promise) Await() (interface{}, error) {
	<-p.Done()
	p.mu.Lock()
	err := p.err
	out := p.value
	p.mu.Unlock()

	return out, err
}

// GetResponse returns the GetResponse output and error
// if Output has not been set yet nil is returned
func (p *Promise) GetResponse() (interface{}, error) {
	if atomic.LoadUint32(&p.flag) == 0 {
		// response not set
		return nil, nil
	}

	p.mu.Lock()
	err := p.err
	out := p.value
	p.mu.Unlock()

	return out, err
}

// Duration returns the duration of the promise execution
func (p *Promise) Duration() time.Duration {
	p.mu.Lock()

	defer p.mu.Unlock()

	if p.endTime.IsZero() {
		return time.Since(p.startTime)
	}

	return p.endTime.Sub(p.startTime)
}
