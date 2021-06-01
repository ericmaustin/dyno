package operation

import (
	"context"
	"github.com/ericmaustin/dyno"
	"sync"
)

//Status is the status of the operation
type Status string

const (
	//StatusRunning set as the operation status when running
	StatusRunning = Status("RUNNING")
	//StatusPending set as the operation status when pending
	StatusPending = Status("PENDING")
	//StatusDone set as the operation status when operation is finished
	StatusDone = Status("DONE")
)

// Operation interface that an operation must satisfy in order to be able to be used in an Batch or Pool
type Operation interface {
	ExecuteInBatch(req *dyno.Request) Result
	Status() Status
	Reset()
}

// Result represents the res of a completed operation
type Result interface {
	OutputInterface() (interface{}, error)
}

// ResultBase is the base of all result structs in the operation module
type ResultBase struct {
	Err error
}

// SetError sets the results error
func (r *ResultBase) SetError(err error) {
	r.Err = err
}

//// Error returns the  Operation Result's  error
//func (r *ResultBase) Error() error {
//	return r.err
//}

// BaseOperation used as the BaseOperation struct type for all operations
type BaseOperation struct {
	Ctx    context.Context
	Done   context.CancelFunc
	Mu     sync.RWMutex
	status Status
}

//NewBase creates a new base operation
func NewBase() *BaseOperation {
	ctx, done := context.WithCancel(context.Background())
	return &BaseOperation{
		Ctx:    ctx,
		Done:   done,
		Mu:     sync.RWMutex{},
		status: StatusPending,
	}
}

// IsPending returns true if operation is pending execution
func (b *BaseOperation) IsPending() bool {
	b.Mu.RLock()
	defer b.Mu.RUnlock()
	return b.status == StatusPending
}

// IsRunning returns true if operation is currently being executed
func (b *BaseOperation) IsRunning() bool {
	b.Mu.RLock()
	defer b.Mu.RUnlock()
	return b.status == StatusRunning
}

// IsDone returns true if operation is done
func (b *BaseOperation) IsDone() bool {
	b.Mu.RLock()
	defer b.Mu.RUnlock()
	return b.status == StatusDone
}

func (b *BaseOperation) SetRunning() {
	if b.status == StatusRunning {
		// already running
		return
	}
	b.Mu.Lock()
	defer b.Mu.Unlock()
	b.status = StatusRunning
}

func (b *BaseOperation) SetDone() {
	b.Mu.Lock()
	defer b.Mu.Unlock()
	b.status = StatusDone
	b.Done()
}

// GetStatus returns the current status of the operation
func (b *BaseOperation) GetStatus() Status {
	b.Mu.RLock()
	defer b.Mu.RUnlock()
	return b.status
}

// Reset resets this operation
// panics with an ErrInvalidState error if operation is running
func (b *BaseOperation) Reset() error {
	if b.IsRunning() {
		return ErrInvalidState
	}
	b.Mu.Lock()
	defer b.Mu.Unlock()
	ctx, done := context.WithCancel(context.Background())
	b.status = StatusPending
	b.Ctx = ctx
	b.Done = done
	return nil
}
