package operation

import (
	"context"
	"fmt"
	"git-codecommit.us-east-1.amazonaws.com/v1/repos/dyno.git"
	"sync"
	"time"
)

const LogFieldOperation = "operation"

type Status string

type MissingTableError struct{}

func (e *MissingTableError) Error() string {
	return "operation is missing a table"
}

type MissingRequestError struct{}

func (e *MissingRequestError) Error() string {
	return "operation is missing a request"
}

type InvalidState struct{}

func (e *InvalidState) Error() string {
	return "operation is an invalid state"
}

const (
	StatusRunning = Status("RUNNING")
	StatusPending = Status("PENDING")
	StatusDone    = Status("DONE")
	StatusError   = Status("ERROR")
)

// Operation interface that an operation must satisfy in order to be able to be used in an Batch
type Operation interface {
	ExecuteInBatch(req *dyno.Request) Result
	Status() Status
	RunningTime() time.Duration
	Reset()
}

// Result represents the res of a completed operation
type Result interface {
	Error() error
	Timing() *Timing
	OutputInterface() interface{}
	SetTiming(*Timing)
	SetError(error)
}

type ResultBase struct {
	timing *Timing
	err    error
}

// SetError sets the results error
func (r *ResultBase) SetError(err error) {
	r.err = err
}

// Error returns the  Operation Result's  error
func (r *ResultBase) Error() error {
	return r.err
}

// Timing returns the Operation Result's timing
func (r *ResultBase) Timing() *Timing {
	return r.timing
}

// SetTiming sets the Operation Result's timing
func (r *ResultBase) SetTiming(timing *Timing) {
	r.timing = timing
	return
}

// Timing is used to store operation timing
type Timing struct {
	created  *time.Time
	started  *time.Time
	finished *time.Time
}

func newTiming() *Timing {
	return &Timing{
		created: dyno.TimePtr(time.Now()),
	}
}

func (t *Timing) Created() time.Time {
	return *t.created
}

func (t *Timing) Started() (time.Time, error) {
	if t.started == nil {
		return time.Time{}, &dyno.Error{
			Code:    dyno.ErrOperationNeverStarted,
			Message: fmt.Sprintf("never started"),
		}
	}
	return *t.started, nil
}

func (t *Timing) Finished() (time.Time, error) {
	if t.finished == nil {
		return time.Time{}, &dyno.Error{
			Code:    dyno.ErrOperationNeverFinished,
			Message: fmt.Sprintf("never finished"),
		}
	}
	return *t.finished, nil
}

func (t *Timing) start() {
	t.started = dyno.TimePtr(time.Now())
}

func (t *Timing) done() {
	t.finished = dyno.TimePtr(time.Now())
}

func (t *Timing) RunningTime() time.Duration {
	if t.started == nil {
		return 0
	}
	if t.finished != nil {
		return time.Now().Sub(*t.started)
	}
	return t.finished.Sub(*t.started)
}

func (t *Timing) TotalTime() time.Duration {
	if t.finished != nil {
		return time.Now().Sub(*t.created)
	}
	return t.finished.Sub(*t.created)
}

// Base used as the Base struct type for all operations
type Base struct {
	ctx     context.Context
	done    context.CancelFunc
	mu      sync.RWMutex
	status  Status
	timing  *Timing
	created *time.Time
}

func newBase() *Base {
	ctx, done := context.WithCancel(context.Background())
	return &Base{
		ctx:    ctx,
		done:   done,
		mu:     sync.RWMutex{},
		status: StatusPending,
		timing: newTiming(),
	}
}

// IsPending returns true if operation is pending execution
func (b *Base) IsPending() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.status == StatusPending
}

// IsRunning returns true if operation is currently being executed
func (b *Base) IsRunning() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.status == StatusRunning
}

// IsDone returns true if operation is done
func (b *Base) IsDone() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.status == StatusDone
}

func (b *Base) setRunning() {
	if b.status == StatusRunning {
		panic(&InvalidState{})
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.status = StatusRunning
	b.timing.start()
}

func (b *Base) setDone(result Result) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.timing.done()
	if result.Error() != nil {
		b.status = StatusError
	} else {
		b.status = StatusDone
	}
	result.SetTiming(b.timing)
	b.done()
}

//
//func (b *Base) setDone() {
//	b.mu.Lock()
//	defer b.mu.Unlock()
//	b.timing.done()
//	b.status = StatusDone
//	b.done()
//}

// RunningTime returns the execution duration of this operation
// if operation is currently running, will return duration up to now
func (b *Base) RunningTime() time.Duration {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.timing.RunningTime()
}

// Status returns the current status of the operation
func (b *Base) Status() Status {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.status
}

// Reset resets this operation
// panics with an InvalidState error if operation is running
func (b *Base) Reset() {
	if b.IsRunning() {
		panic(&InvalidState{})
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	ctx, done := context.WithCancel(context.Background())
	b.status = StatusPending
	b.timing = newTiming()
	b.ctx = ctx
	b.done = done
}
