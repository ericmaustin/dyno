package timer

import (
	"context"
	"math"
	"math/rand"
	"sync"
	"time"
)

type SleepContextCancelled struct{}

func (e *SleepContextCancelled) Error() string {
	return "sleeper context has been cancelled"
}

type NextSleepDuration func(sleepDuration time.Duration, count int) time.Duration

// Sleeper is a more complex version of the base timer
type Sleeper struct {
	timeout       time.Duration
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

// WithTimeout adds a timeout to this sleeper
func (s *Sleeper) WithTimeout(to time.Duration) *Sleeper {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.timeout = to
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
		mu:            sync.Mutex{},
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

// Sleep returns a channel that will return a SleepContextCancelled error if context is cancelled before the next
// sleep interval completes, nil otherwise
func (s *Sleeper) Sleep() <-chan error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.started {
		if s.ctx == nil {
			s.ctx = context.Background()
		}
		if s.timeout > 0 {
			s.ctx, s.done = context.WithTimeout(s.ctx, s.timeout)
		} else {
			s.ctx, s.done = context.WithCancel(s.ctx)
		}
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
			errorCh <- &SleepContextCancelled{}
		}
	}()

	return errorCh
}

// NewExponentialSleeper returns a sleeper that will sleep with an exponentially increasing interval
// useful for exponential backoff
func NewExponentialSleeper(sleepDuration time.Duration) *Sleeper {
	return &Sleeper{
		sleepDuration: sleepDuration,
		mu:            sync.Mutex{},
		durationFunc: func(sleepDuration time.Duration, count int) time.Duration {
			return time.Duration(math.Pow(2, float64(count))) * sleepDuration
		},
	}
}

// NewLinearSleeper returns a sleeper that will sleep with an linearly increasing interval
// useful for linear backoff
func NewLinearSleeper(sleepDuration time.Duration, alpha int) *Sleeper {
	return &Sleeper{
		sleepDuration: sleepDuration,
		mu:            sync.Mutex{},
		durationFunc: func(sleepDuration time.Duration, count int) time.Duration {
			return time.Duration(alpha*count) * sleepDuration
		},
	}
}

// NewCustomSleeper returns a sleeper that will use a custom sleeper function to get the interval duration
func NewCustomSleeper(sleepDuration time.Duration, sleepFunc NextSleepDuration) *Sleeper {
	return &Sleeper{
		sleepDuration: sleepDuration,
		mu:            sync.Mutex{},
		durationFunc:  sleepFunc,
	}
}

// RandomDuration creates a random duration from 0 to the given max duration
func RandomDuration(max time.Duration) time.Duration {
	return time.Duration(rand.Int63n(max.Nanoseconds()))
}
