package dyno

import (
	"github.com/segmentio/ksuid"
	"sync"
	"sync/atomic"
	"time"
)

// PromiseState represents the current state of the Promise
type PromiseState uint32

const (
	// PromisePending is the default state
	PromisePending PromiseState = iota
	// PromiseWaiting is the state when a promise is waiting for a response
	PromiseWaiting
	// PromiseReady is the state when a promise has returned
	PromiseReady
)

var promiseStateStringMap = map[PromiseState]string{
	PromisePending: "PENDING",
	PromiseWaiting: "WAITING",
	PromiseReady:   "READY",
}

// String implements stringer
func (p PromiseState) String() string {
	return promiseStateStringMap[p]
}

// PromiseError is an error returned by a Promise
type PromiseError string

// Error implements the error interface
func (e PromiseError) Error() string { return string(e) }

const (
	ErrAlreadyWaiting = PromiseError("promise already waiting")
	ErrResultAlreadySet = PromiseError("result already set")
	ErrCannotAwait = PromiseError("promise cannot await while promise is still pending")
)

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
		id: ksuid.New().String(),
	}
}

// SetWaiting sets this Promise to PromiseWaiting state
func (p *Promise) SetWaiting() {
	if p.GetState() == PromiseWaiting {
		// dont set the state a second time
		return
	}

	p.SetState(PromiseWaiting)
}

// SetState sets the state of the Promise
func (p *Promise) SetState(state PromiseState) {
	p.mu.Lock()
	switch state {
	case PromiseWaiting:
		p.startTime = time.Now()
	case PromiseReady:
		p.endTime = time.Now()
	}
	p.mu.Unlock()

	atomic.StoreUint32(&p.flag, uint32(state))
}

// GetState gets the current state of the Promise
func (p *Promise) GetState() PromiseState {
	return PromiseState(atomic.LoadUint32(&p.flag))
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
				if p.GetState() == PromiseReady {
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
	if p.GetState() == PromiseReady {
		panic(ErrResultAlreadySet)
	}

	p.mu.Lock()
	p.value = val
	p.err = err
	p.mu.Unlock()

	p.SetState(PromiseReady)
}

// Await waits for the Promise's result to complete or for the context to be cancelled
// whichever comes first
func (p *Promise) Await() (interface{}, error) {
	if p.GetState() == PromisePending {
		return nil, ErrCannotAwait
	}

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
	if p.GetState() != PromiseReady {
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
