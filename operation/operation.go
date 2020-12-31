package operation

import (
	"context"
	"sync"
	"time"

	"github.com/ericmaustin/dyno"
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

// resultBase is the base of all result structs in the operation module
type resultBase struct {
	timing *Timing
	err    error
}

// SetError sets the results error
func (r *resultBase) SetError(err error) {
	r.err = err
}

// Error returns the  Operation Result's  error
func (r *resultBase) Error() error {
	return r.err
}

// Timing returns the Operation Result's timing
func (r *resultBase) Timing() *Timing {
	return r.timing
}

// SetTiming sets the Operation Result's timing
func (r *resultBase) SetTiming(timing *Timing) {
	r.timing = timing
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
		return time.Since(*t.started)
	}
	return t.finished.Sub(*t.started)
}

func (t *Timing) TotalTime() time.Duration {
	if t.finished != nil {
		return time.Since(*t.created)
	}
	return t.finished.Sub(*t.created)
}

// baseOperation used as the baseOperation struct type for all operations
type baseOperation struct {
	ctx    context.Context
	done   context.CancelFunc
	mu     sync.RWMutex
	status Status
	timing *Timing
}

func newBase() *baseOperation {
	ctx, done := context.WithCancel(context.Background())
	return &baseOperation{
		ctx:    ctx,
		done:   done,
		mu:     sync.RWMutex{},
		status: StatusPending,
		timing: newTiming(),
	}
}

// IsPending returns true if operation is pending execution
func (b *baseOperation) IsPending() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.status == StatusPending
}

// IsRunning returns true if operation is currently being executed
func (b *baseOperation) IsRunning() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.status == StatusRunning
}

// IsDone returns true if operation is done
func (b *baseOperation) IsDone() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.status == StatusDone
}

func (b *baseOperation) setRunning() {
	if b.status == StatusRunning {
		panic(&InvalidState{})
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.status = StatusRunning
	b.timing.start()
}

func (b *baseOperation) setDone(result Result) {
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

// RunningTime returns the execution duration of this operation
// if operation is currently running, will return duration up to now
func (b *baseOperation) RunningTime() time.Duration {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.timing.RunningTime()
}

// Status returns the current status of the operation
func (b *baseOperation) Status() Status {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.status
}

// Reset resets this operation
// panics with an InvalidState error if operation is running
func (b *baseOperation) Reset() {
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
