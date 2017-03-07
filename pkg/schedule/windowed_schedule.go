package schedule

import (
	"time"

	log "github.com/Sirupsen/logrus"
	"fmt"
)

var (
	logger = log.WithField("_module", "schedule")
)

// WindowedSchedule is a schedule that waits on an interval within a specific time window
type WindowedSchedule struct {
	Interval  time.Duration
	StartTime *time.Time
	StopTime  *time.Time
	state     ScheduleState
}

// NewWindowedSchedule returns an instance of WindowedSchedule given duration,
// start and stop time
func NewWindowedSchedule(i time.Duration, start *time.Time, stop *time.Time, count uint) *WindowedSchedule {
	count = uint(1)
	if count != 0 {

		if stop != nil {
			//give here some err
		} else {
			if start != nil {
				newStop := start.Add(10*i)
				stop = &newStop
				fmt.Println("\n\n Debug iza - new stopA=%v\n", newStop)
			} else{
				newStop := time.Now().Add(10*i)
				stop = &newStop
				fmt.Println("\n\n Debug iza - new stopB=%v\n", newStop)
			}
			fmt.Println("\n\n Debug iza - stop=%v\n", stop)
		}
	} else {
		panic("Iza")
	}

	logger.WithFields(log.Fields{
			"_block":         "windowed-wait",
			"stop-time": stop,
		}).Debug("Iza - Creating window swith top time ")

	return &WindowedSchedule{
		Interval:  i,
		StartTime: start,
		StopTime:  stop,
	}
}

// NewSimpleSchedule returns an instance of WindowedSchedule without determined start and stop time
func NewSimpleSchedule(i time.Duration) *WindowedSchedule {
	return &WindowedSchedule{
		Interval:  i,
		StartTime: nil,
		StopTime:  nil,
	}
}

// GetState returns ScheduleState of WindowedSchedule
func (w *WindowedSchedule) GetState() ScheduleState {
	return w.state
}

// Validate validates the start, stop and duration interval of
// WindowedSchedule
func (w *WindowedSchedule) Validate() error {
	// if the stop time was set but it is in the past, return an error
	if w.StopTime != nil && time.Now().After(*w.StopTime) {
		return ErrInvalidStopTime
	}
	// if the start and stop time were both set and the the stop time is before
	// the start time, return an error
	if w.StopTime != nil && w.StartTime != nil && w.StopTime.Before(*w.StartTime) {
		return ErrStopBeforeStart
	}
	// if the interval is less than zero, return an error
	if w.Interval <= 0 {
		return ErrInvalidInterval
	}
	return nil
}

// Wait waits the window interval and return.
// Otherwise, it exits with a completed state
func (w *WindowedSchedule) Wait(last time.Time) Response {
	// If within the window we wait our interval and return
	// otherwise we exit with a completed state.
	var m uint

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
	if w.StopTime != nil {
		logger.WithFields(log.Fields{
			"_block":         "windowed-wait",
			"stop-time": w.StopTime,
		}).Debug("Iza - Window stop time is defined")
		if time.Now().Before((*w.StopTime)) {
			logger.WithFields(log.Fields{
				"_block":           "windowed-wait",
				"time-before-stop": w.StopTime.Sub(time.Now()),
			}).Debug("Within window, calling interval")
			logger.WithFields(log.Fields{
				"_block":   "windowed-wait",
				"last":     last,
				"interval": w.Interval,
			}).Debug("waiting for interval")
			m, _ = waitOnInterval(last, w.Interval)

			if time.Now().After(*w.StopTime) {
				logger.WithFields(log.Fields{
					"_block":   "windowed-wait",
				}).Debug("Debug Iza!!!! Schedule is Ended2")
				w.state = Ended
				m = 0
			}
		} else {
			logger.WithFields(log.Fields{
				"_block":   "windowed-wait",
			}).Debug("Debug Iza!!!! Schedule is Ended")
			w.state = Ended
			m = 0
		}

	} else {
		logger.WithFields(log.Fields{
			"_block":         "windowed-wait",
			"stop-time": 0,
		}).Debug("Iza - Window stop time is not defined")
		logger.WithFields(log.Fields{
			"_block":   "windowed-wait",
			"last":     last,
			"interval": w.Interval,
		}).Debug("waiting for interval")
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
