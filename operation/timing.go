package operation

import (
	"github.com/ericmaustin/dyno"
	"time"
)

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

// Created returns the time when this Timing was flagged as created
func (t *Timing) Created() time.Time {
	return *t.created
}

// Started returns the time when this Timing was flagged as started
func (t *Timing) Started() (time.Time, error) {
	if t.started == nil {
		return time.Time{}, &dyno.Error{
			Code:    dyno.ErrOperationNeverStarted,
			Message: "never started",
		}
	}
	return *t.started, nil
}

// Finished returns the time when this Timing was flagged as finished
func (t *Timing) Finished() (time.Time, error) {
	if t.finished == nil {
		return time.Time{}, &dyno.Error{
			Code:    dyno.ErrOperationNeverFinished,
			Message: "never finished",
		}
	}
	return *t.finished, nil
}
