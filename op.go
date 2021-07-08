package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/segmentio/ksuid"
	"sync"
	"sync/atomic"
	"time"
)

// OperationState represents the current state of the BaseOperation
type OperationState uint32

const (
	// OperationPending is the default state
	OperationPending OperationState = iota
	// OperationRunning is the state when a operation is waiting for a response
	OperationRunning
	// OperationDone is the state when a operation has returned
	OperationDone
)

var opStateMap = map[OperationState]string{
	OperationPending: "PENDING",
	OperationRunning: "RUNNING",
	OperationDone:    "DONE",
}

// String implements stringer
func (p OperationState) String() string {
	return opStateMap[p]
}

// OperationError is an error returned by a BaseOperation
type OperationError string

// Error implements the error interface
func (e OperationError) Error() string { return string(e) }

const (
	ErrResultAlreadySet = OperationError("result already set")
	ErrCannotAwait      = OperationError("operation cannot await while operation is still pending")
	ErrAlreadyDone      = OperationError("operation cannot be set to waiting when done")
)

// Operation must be implemented on types that can be executed as an operation in Session or Pool
type Operation interface {
	InvokeDynoOperation(ctx context.Context, client *ddb.Client)
	SetResponse(output interface{}, err error)
	SetRunning()
}

//BaseOperation represents a request operation
// a BaseOperation is returned from any Session Execution
// use <-BaseOperation.Done() to wait for a operation to be done
// use BaseOperation.Await() to wait for and receive the output from a operation
type BaseOperation struct {
	id        string
	mu        sync.RWMutex
	startTime time.Time
	endTime   time.Time
	flag      uint32
	value     interface{}
	err       error
	doneCh    chan struct{}
}

//NewOperation creates a new BaseOperation with a startTime of Now
func NewOperation() *BaseOperation {
	return &BaseOperation{
		id: ksuid.New().String(),
	}
}

// SetRunning sets this BaseOperation to OperationRunning state
func (p *BaseOperation) SetRunning() {
	switch p.GetState() {
	case OperationRunning:
		return
	case OperationDone:
		panic(ErrAlreadyDone)
	}

	p.SetState(OperationRunning)
}

// SetState sets the state of the BaseOperation
func (p *BaseOperation) SetState(state OperationState) {
	p.mu.Lock()
	switch state {
	case OperationRunning:
		p.startTime = time.Now()
	case OperationDone:
		p.endTime = time.Now()
	}
	p.mu.Unlock()

	atomic.StoreUint32(&p.flag, uint32(state))
}

// GetState gets the current state of the BaseOperation
func (p *BaseOperation) GetState() OperationState {
	return OperationState(atomic.LoadUint32(&p.flag))
}

// Done returns a channel that will emit a struct{}{} when BaseOperation contains a result, an error is encountered,
// or context was cancelled
func (p *BaseOperation) Done() <-chan struct{} {
	p.mu.Lock()

	if p.doneCh == nil {
		p.doneCh = make(chan struct{})

		go func() {
			defer func() {
				p.doneCh <- struct{}{}
				close(p.doneCh)
			}()

			for {
				if p.GetState() == OperationDone {
					// done!
					return
				}
			}
		}()
	}

	p.mu.Unlock()

	return p.doneCh
}

func (p *BaseOperation) ID() string {
	return p.id
}

// SetResponse sets the response value interface and error
// and sets the flag to the done state
func (p *BaseOperation) SetResponse(val interface{}, err error) {
	if p.GetState() == OperationDone {
		panic(ErrResultAlreadySet)
	}

	p.mu.Lock()
	p.value = val
	p.err = err
	p.mu.Unlock()

	p.SetState(OperationDone)
}

// Await waits for the BaseOperation's result to complete or for the context to be cancelled
// whichever comes first
func (p *BaseOperation) Await() (interface{}, error) {
	if p.GetState() == OperationPending {
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
func (p *BaseOperation) GetResponse() (interface{}, error) {
	if p.GetState() != OperationDone {
		// response not set
		return nil, nil
	}

	p.mu.Lock()
	err := p.err
	out := p.value
	p.mu.Unlock()

	return out, err
}

// Duration returns the duration of the operation execution
func (p *BaseOperation) Duration() time.Duration {
	p.mu.Lock()

	defer p.mu.Unlock()

	if p.endTime.IsZero() {
		return time.Since(p.startTime)
	}

	return p.endTime.Sub(p.startTime)
}


// OperationF is an operation function that implements Operation
type OperationF func(context.Context, *ddb.Client)

// InvokeDynoOperation implements Operation
func (op OperationF) InvokeDynoOperation(ctx context.Context, client *ddb.Client) {
	op(ctx, client)
}

// SetRunning implements Operation but effectively does nothing
func (op OperationF) SetRunning() {}

// SetResponse implements Operation but effectively does nothing
func (op OperationF) SetResponse(interface{}, error) {}
