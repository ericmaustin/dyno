package timer

import (
	"sync"
	"time"
)

type TimeSlice struct {
	started  time.Time
	ended    time.Time
	duration time.Duration
}

func (ts *TimeSlice) Start() {
	ts.started = time.Now()
}

func (ts *TimeSlice) End() {
	ts.ended = time.Now()
	ts.duration = ts.ended.Sub(ts.started)
}

/*
NewTimer starts and then returns a new Timer
*/
func NewTimer() *Timer {
	t := &Timer{
		mu: &sync.Mutex{},
	}
	// ignore error as error just means timer is already running
	return t
}

/*
Timer is a Simple timer that doesn't rely on channels or routines, and solely uses timestamps with
TimeSlices
*/
type Timer struct {
	slices   []*TimeSlice
	running  bool
	done     bool
	duration time.Duration
	mu       *sync.Mutex
}

// Start starts this timer
func (t *Timer) Start() *Timer {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.running == true {
		return t
	}
	// create a new time slice
	slice := &TimeSlice{}
	slice.Start()
	t.slices = append(t.slices, slice)
	return t
}

// Stop stops this timer and returns it
func (t *Timer) Stop() *Timer {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.running = false
	lastSlice := t.slices[len(t.slices)-1]
	lastSlice.End()
	// add the slices duration
	t.duration += lastSlice.duration
	return t
}

// Duration returns the total elapsed duration for the timer
func (t *Timer) Duration() time.Duration {
	// if we're running...
	if t.running == true {
		return t.duration + time.Now().Sub(t.slices[len(t.slices)-1].started)
	}
	return t.duration
}

// Slices returns the time slices for this timer
func (t *Timer) Slices() []*TimeSlice {
	return t.slices
}
