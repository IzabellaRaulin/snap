package schedule

import (
	"time"

	log "github.com/Sirupsen/logrus"
)

var (
	logger = log.WithField("_module", "schedule")
)

// WindowedSchedule is a schedule that waits on an interval within a specific time window
type WindowedSchedule struct {
	Interval   time.Duration
	StartTime  *time.Time
	StopTime   *time.Time
	Count      uint
	state      ScheduleState
	stopOnTime *time.Time
}

// NewWindowedSchedule returns an instance of WindowedSchedule given duration,
// start and stop time
func NewWindowedSchedule(i time.Duration, start *time.Time, stop *time.Time, count uint) *WindowedSchedule {
	// if stop and count were both defined, ignore the `count`
	if count != 0 && stop != nil {
		count = 0
		// log about ignoring the `count`
		logger.WithFields(log.Fields{
			"_block": "NewWindowedSchedule",
		}).Warning("Specifying both the window stop and the count is not allowed. Ignoring the count param.")
	}

	return &WindowedSchedule{
		Interval:  i,
		StartTime: start,
		StopTime:  stop,
		Count:     count,
	}
}

// setStopOnTime calculates and set the value of the windowed `stopOnTime` which is the right window boundary.
// `stopOnTime` is determined by `StopTime` or, if it is not provided, calculated based on count and interval.
func (w *WindowedSchedule) setStopOnTime() {
	if w.StopTime == nil && w.Count != 0 {
		// determine the window stop based on the `count` and `interval`
		var newStop time.Time

		// if start is not set or points in the past,
		// use the current time to calculate stopOnTime
		if w.StartTime != nil && time.Now().Before(*w.StartTime) {
			newStop = w.StartTime.Add(time.Duration(w.Count) * w.Interval)
		} else {
			// set a new stop timestamp from this point in time
			newStop = time.Now().Add(time.Duration(w.Count) * w.Interval)
		}
		// set calculated new stop
		w.stopOnTime = &newStop
		return
	}

	// stopOnTime is determined by StopTime
	w.stopOnTime = w.StopTime
}

// GetState returns ScheduleState of WindowedSchedule
func (w *WindowedSchedule) GetState() ScheduleState {
	return w.state
}

// Validate validates the start, stop and duration interval of WindowedSchedule
func (w *WindowedSchedule) Validate() error {
	// if the stop time was set but it is in the past, return an error
	if w.StopTime != nil && time.Now().After(*w.StopTime) {
		return ErrInvalidStopTime
	}

	// if the start and stop time were both set and the stop time is before
	// the start time, return an error
	if w.StopTime != nil && w.StartTime != nil && w.StopTime.Before(*w.StartTime) {
		return ErrStopBeforeStart
	}
	// if the interval is less than zero, return an error
	if w.Interval <= 0 {
		return ErrInvalidInterval
	}

	// the schedule passed validation, set is as an active
	w.state = Active
	return nil
}

// Wait waits the window interval and return.
// Otherwise, it exits with a completed state
func (w *WindowedSchedule) Wait(last time.Time) Response {
	// If within the window we wait our interval and return
	// otherwise we exit with a completed state.
	var m uint

	if (last == time.Time{}) {
		// set a value of windowed `stopOnTime` determining the right-window boundary
		w.setStopOnTime()
	}

	// Do we even have a specific start time?
	if w.StartTime != nil {
		// Wait till it is time to start if before the window start
		if time.Now().Before(*w.StartTime) {
			wait := w.StartTime.Sub(time.Now())
			logger.WithFields(log.Fields{
				"_block":         "windowed-wait",
				"sleep-duration": wait,
			}).Debug("Waiting for window to start")
			time.Sleep(wait)
		}
	} else {
		// This has no start like a simple schedule, so execution starts immediately
		logger.WithFields(log.Fields{
			"_block":         "windowed-wait",
			"sleep-duration": 0,
		}).Debug("Window start time not defined, start execution immediately")
	}

	// Do we even have a stop time?
	if w.stopOnTime != nil {
		if time.Now().Before(*w.stopOnTime) {
			logger.WithFields(log.Fields{
				"_block":           "windowed-wait",
				"time-before-stop": w.stopOnTime.Sub(time.Now()),
			}).Debug("Within window, calling interval")
			logger.WithFields(log.Fields{
				"_block":   "windowed-wait",
				"last":     last,
				"interval": w.Interval,
			}).Debug("waiting for interval")
			m, _ = waitOnInterval(last, w.Interval)

			// check if the schedule should be ended after waiting on interval
			if time.Now().After(*w.stopOnTime) {
				logger.WithFields(log.Fields{
					"_block": "windowed-wait",
				}).Debug("schedule is ended")
				w.state = Ended
			}
		} else {
			w.state = Ended
			m = 0
		}
	} else {
		// This has no end like a simple schedule
		m, _ = waitOnInterval(last, w.Interval)

	}
	return &WindowedScheduleResponse{
		state:    w.GetState(),
		missed:   m,
		lastTime: time.Now(),
	}
}

// WindowedScheduleResponse is the response from SimpleSchedule
// conforming to ScheduleResponse interface
type WindowedScheduleResponse struct {
	state    ScheduleState
	missed   uint
	lastTime time.Time
}

// State returns the state of the Schedule
func (w *WindowedScheduleResponse) State() ScheduleState {
	return w.state
}

// Error returns last error
func (w *WindowedScheduleResponse) Error() error {
	return nil
}

// Missed returns any missed intervals
func (w *WindowedScheduleResponse) Missed() uint {
	return w.missed
}

// LastTime returns the last windowed schedule response time
func (w *WindowedScheduleResponse) LastTime() time.Time {
	return w.lastTime
}
