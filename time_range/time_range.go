package time_range

import (
	"fmt"
	// "json"
	"math"
	"time"
)

/*
TimeRange represents a time range from ``start`` to ``End``
``start`` represents the actual start time for the time range
``End`` represents the actual end time for this time time range
``PeriodStart`` is the time range's period start that this range belongs to
``Period`` is the duration of the period that this range belongs to
*/
type TimeRange struct {
	Start       time.Time     `json:"start"`
	End         time.Time     `json:"end"`
	PeriodStart time.Time     `json:"period_start"`
	Period      time.Duration `json:"duration"`
}

// DurationInt returns the duration between the time range's ``start`` and ``End`` properties.
func (tr *TimeRange) Duration() time.Duration {
	return tr.End.Sub(tr.Start)
}

// done returns true if period is ``complete``
func (tr *TimeRange) Complete() bool {
	return tr.Start == tr.PeriodStart && tr.End == tr.PeriodEnd()
}

// PeriodEnd returns the period's end time which is always offset by one nanosecond
func (tr *TimeRange) PeriodEnd() time.Time {
	return tr.PeriodStart.Add(time.Duration(tr.Period) - time.Nanosecond)
}

/*
RemainderToStart returns the period's remainder as a timePeriod beteween the period's start
and the actual start time.
If there is no remainder this returns nil instead
*/
func (tr *TimeRange) RemainderToStart() *TimeRange {
	if tr.Start == tr.PeriodStart {
		return nil
	}
	// remove a nanosecond from current period's start
	end := tr.Start.Add(-time.Nanosecond)

	return &TimeRange{
		Start:       tr.PeriodStart,
		End:         end,
		PeriodStart: tr.PeriodStart,
		Period:      tr.Period,
	}
}

/*
RemainderFromEnd returns the period's remainder as a timePeriod beteween the period's end
and the actual end time.
If there is no remainder this returns nil instead
*/
func (tr *TimeRange) RemainderFromEnd() *TimeRange {

	periodEnd := tr.PeriodEnd()

	if tr.End == periodEnd {
		return nil
	}
	// add a nanosecond to current period's end
	start := tr.End.Add(time.Nanosecond)

	return &TimeRange{
		Start:       start,
		End:         periodEnd,
		PeriodStart: tr.PeriodStart,
		Period:      tr.Period,
	}
}

// NewTimeRangeFromStart create's a new time range with a given start time
func NewTimeRangeFromStart(start time.Time, duration time.Duration, round bool) *TimeRange {
	// start should be rounded down, so we use Truncate instead of Round

	roundedStart := start.Truncate(duration)
	end := roundedStart.Add(duration - time.Nanosecond)

	// if we're rounding...
	if round {
		start = roundedStart
	}

	return &TimeRange{
		Start:       start,
		End:         end,
		PeriodStart: roundedStart,
		Period:      duration,
	}
}

// NewTimeRangeFromEnd create's a new time range with a given start time
func NewTimeRangeFromEnd(end time.Time, duration time.Duration, round bool) *TimeRange {
	// end should be rounded up, so we use Round
	roundedEnd := end.Round(duration)
	start := roundedEnd.Add(-duration)
	// end time should ALWAYS be
	roundedEnd = roundedEnd.Add(-time.Nanosecond)

	// if we're not rounding...
	if round {
		end = roundedEnd
	}

	return &TimeRange{
		Start:       start,
		End:         end,
		PeriodStart: start,
		Period:      duration,
	}
}

// Stringify a time range
func (tr *TimeRange) String() string {
	return fmt.Sprintf("s%de%dp%v", tr.Start.UnixNano(), tr.End.UnixNano(), tr.Period)
}

// Next getActive's the next time range adjacent to current time rangem, offset by ``periods```
func (tr *TimeRange) Next(periods int) *TimeRange {
	nextStart := tr.PeriodStart.Add(time.Duration(tr.Period) * time.Duration(periods))
	// next period starts at nextStart
	return &TimeRange{
		Start:       nextStart,
		End:         nextStart.Add(time.Duration(tr.Period) - time.Nanosecond),
		PeriodStart: nextStart,
		Period:      tr.Period,
	}
}

// Prev getActive's the previous time range offset by ``periods``
func (tr *TimeRange) Prev(periods int) *TimeRange {
	prevStart := tr.Start.Add(-(time.Duration(tr.Period) * time.Duration(periods)))
	// previous period starts at prevStart
	return &TimeRange{
		Start:       prevStart,
		End:         prevStart.Add(time.Duration(tr.Period) - time.Nanosecond),
		PeriodStart: prevStart,
		Period:      tr.Period,
	}
}

/*
TimeSeries represents a series of ``TimeRange`` objects (``Periods``)
starting at ``start`` and ending at ``End``
*/
type TimeSeries struct {
	Period  time.Duration `json:"period"`
	Periods []*TimeRange  `json:"periods"`
	Start   time.Time     `json:"start"`
	End     time.Time     `json:"end"`
}

/*
NewTimeSeriesBetween returns a new time series given a start time, and end time, and a period
 If roundStart is ``true`` then round the start time DOWN to the nearest period
 If roundEnd is ``true`` then round the end time UP to the nearest period
*/
func NewTimeSeriesBetween(start time.Time, end time.Time, period time.Duration, roundStart bool,
	roundEnd bool) (*TimeSeries, error) {

	roundedStart := start.Truncate(period)
	roundedEnd := end.Round(period - time.Nanosecond)
	ts := &TimeSeries{
		Period: period,
		Start:  start,
		End:    end,
	}

	if roundStart {
		start = roundedStart
	}

	if roundEnd {
		end = roundedEnd
	}

	firstEnd := roundedStart.Add(period - time.Nanosecond)

	// if first range's end is after or == the end time for the entire series, then return
	// the time series with just the single range
	if firstEnd.After(end) || end == firstEnd {
		first := &TimeRange{
			Start:       start,
			End:         end,
			PeriodStart: roundedStart,
			Period:      period,
		}
		ts.Periods = []*TimeRange{first}
		return ts, nil
	}

	thisTimeRange := &TimeRange{
		Start:       start,
		End:         firstEnd,
		PeriodStart: roundedStart,
		Period:      period,
	}
	ts.Periods = []*TimeRange{thisTimeRange}
	// otherwise keep looping until we hit the end.
	lastEnd := firstEnd
	for {
		if lastEnd.After(end) || lastEnd == end {
			return ts, nil
		}
		next := thisTimeRange.Next(1)
		if next.End.After(end) {
			next.End = end
		}
		ts.Periods = append(ts.Periods, next)
		lastEnd = next.End
		thisTimeRange = next
	}
}

/*
FindTimeRange searches this ``TimeSeries`` for a ``TimeRange``` provided a ``time.AsTime``
Returns a ptr to the found ``TimeRange`` or nil if not found
*/
func (ts *TimeSeries) FindTimeRange(t time.Time) *TimeRange {
	rounded := t.Truncate(time.Duration(ts.Period))
	// check the difference to the first element

	// if series is empty return nil
	if len(ts.Periods) == 0 {
		return nil
	}

	diffFromStart := rounded.Sub(ts.Periods[0].PeriodStart)
	// if negative difference, time is not in the series, as it comes BEFORE the series starts
	if diffFromStart < time.Duration(0) {
		return nil
	}

	n := int(math.Floor(float64(diffFromStart) / float64(time.Duration(ts.Period))))

	if len(ts.Periods) < n {
		return nil
	}

	return ts.Periods[n]
}
