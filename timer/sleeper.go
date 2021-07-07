package timer

import (
	"context"
	"math"
	"math/rand"
	"sync"
	"time"
)

//ErrSleeperCancelled returned when a sleeper is cancelled early
type ErrSleeperCancelled struct{}

func (e *ErrSleeperCancelled) Error() string {
	return "sleeper context has been cancelled"
}

//NextSleepDuration provided to set a custom func to determine the next sleep duration
type NextSleepDuration func(sleepDuration time.Duration, count int) time.Duration

// Sleeper is a more complex version of the base timer
type Sleeper struct {
	sleepDuration time.Duration
	count         int
	mu            sync.Mutex
	durationFunc  NextSleepDuration
	started       bool
	ctx           context.Context
	done          context.CancelFunc
	randAdd       time.Duration
}

// WithAddRandom waits for an additional duration from 0 to the given duration every interval
func (s *Sleeper) WithAddRandom(add time.Duration) *Sleeper {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.randAdd = add

	return s
}

// WithContext adds an external context to this sleeper
func (s *Sleeper) WithContext(ctx context.Context) *Sleeper {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ctx = ctx

	return s
}

// NewSleeper returns a sleeper that will sleep for the given sleep duration every time Sleep() is called
func NewSleeper(sleepDuration time.Duration) *Sleeper {
	return &Sleeper{
		sleepDuration: sleepDuration,
		durationFunc: func(lastDuration time.Duration, count int) time.Duration {
			return lastDuration
		},
	}
}

// Cancel cancels the sleeper, causing any running sleeper routines to return
func (s *Sleeper) Cancel() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.done != nil {
		s.done()
	}
}

// Sleep returns a channel that will return a ErrSleeperCancelled error if context is cancelled before the next
// sleep interval completes, nil otherwise
func (s *Sleeper) Sleep() <-chan error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started {
		if s.ctx == nil {
			s.ctx = context.Background()
		}

		s.ctx, s.done = context.WithCancel(s.ctx)
		s.started = true
	}

	nextSleep := s.durationFunc(s.sleepDuration, s.count)

	if s.randAdd > 0 {
		nextSleep += RandomDuration(s.randAdd)
	}

	timer := time.NewTimer(nextSleep)
	errorCh := make(chan error)
	s.count++

	go func() {
		defer close(errorCh)
		select {
		case <-timer.C:
			// sleep interval complete
			errorCh <- nil
		case <-s.ctx.Done():
			// timeout was reached or context was cancelled
			errorCh <- &ErrSleeperCancelled{}
		}
	}()

	return errorCh
}

// NewExponentialSleeper returns a sleeper that will sleep with an exponentially increasing interval
// useful for exponential backoff
func NewExponentialSleeper(sleepDuration time.Duration, alpha float64) *Sleeper {
	return &Sleeper{
		sleepDuration: sleepDuration,
		durationFunc: func(sleepDuration time.Duration, count int) time.Duration {
			return time.Duration(math.Pow(alpha, float64(count))) * sleepDuration
		},
	}
}

// NewLinearSleeper returns a sleeper that will sleep with an linearly increasing interval
// useful for linear backoff
func NewLinearSleeper(sleepDuration time.Duration, alpha int64) *Sleeper {
	return &Sleeper{
		sleepDuration: sleepDuration,
		durationFunc: func(sleepDuration time.Duration, count int) time.Duration {
			if count == 0 {
				return sleepDuration
			}
			return time.Duration(count << alpha) * sleepDuration
		},
	}
}

// NewCustomSleeper returns a sleeper that will use a custom sleeper function to get the interval duration
func NewCustomSleeper(sleepDuration time.Duration, sleepFunc NextSleepDuration) *Sleeper {
	return &Sleeper{
		sleepDuration: sleepDuration,
		durationFunc:  sleepFunc,
	}
}

// RandomDuration creates a random duration from 0 to the given max duration
func RandomDuration(max time.Duration) time.Duration {
	return time.Duration(rand.Int63n(max.Nanoseconds()))
}
