package operation

import (
	"context"
	"github.com/ericmaustin/dyno"
	"sync"
)

type NotRunningError struct{}

func (n *NotRunningError) Error() string {
	return "the runner is not running"
}

// NewRunner creates a new parallel execution runner with a given number of maxRoutines
// the runner go routine dispatcher is started immediately
func NewRunner(ctx context.Context, sess *dyno.Session, maxRoutines int) *Runner {
	if maxRoutines == 0 {
		maxRoutines = 1
	}

	thisCtx, cancel := context.WithCancel(ctx)

	r := &Runner{
		maxRoutines: maxRoutines,
		session:     sess,
		ctx:         thisCtx,
		done:        cancel,
		wg:          &sync.WaitGroup{},
		mu:          &sync.RWMutex{},
		input:       make(chan Operation, maxRoutines),
		sem:         make(chan struct{}, maxRoutines),
	}
	go r.runner()
	return r
}

// Batch is a batch request
type Runner struct {
	ctx         context.Context
	done        context.CancelFunc
	maxRoutines int
	session     *dyno.Session
	wg          *sync.WaitGroup
	mu          *sync.RWMutex
	input       chan Operation
	sem         chan struct{}
	err         error
	running     bool
}

func (r *Runner) IsRunning() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.running
}

func (r *Runner) setRunning(isRunning bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.running = isRunning
}

// Error gets the error from the batch
func (r *Runner) Error() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.err
}

func (r *Runner) setError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.err = err
}

// Stop stops the runner
// Stop should always be called to turn off the runner when the runner is no longer needed
func (r *Runner) Stop() *Runner {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.running {
		r.done()
		close(r.input) // close the channel for writing
		r.running = false
	}
	return r
}

// Run sends operations to the runner
// blocks until the buffer is clear
// panics on channel being closed if Stop() is called before Run()
func (r *Runner) Run(ops ...Operation) error {
	if !r.IsRunning() {
		return &NotRunningError{}
	}
	r.wg.Add(len(ops))
	for _, op := range ops {
		r.input <- op
	}
	return nil
}

// Run runs an operation
func (r *Runner) runner() {
	r.setRunning(true)
	for {
		select {
		case op, ok := <-r.input:
			if !ok {
				// channel is closed. nothing left to do.
				return
			}
			r.sem <- struct{}{}
			go func() {
				defer func() {
					r.wg.Done()
					<-r.sem
				}()
				err := op.ExecuteInBatch(r.session.Request()).Error()
				if err != nil {
					r.setError(err)
					r.ctx.Done()
				}
			}()
		case <-r.ctx.Done():
			r.setRunning(false)
			return
		}
	}
}

// Wait waits for the runner to finish
func (r *Runner) Wait() *Runner {
	r.wg.Wait()
	return r
}
