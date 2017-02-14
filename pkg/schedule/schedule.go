package schedule

import (
	"errors"
	"time"

	log "github.com/Sirupsen/logrus"
)

var (
	// ErrInvalidInterval - Error message for the valid schedule interval must ne greater than 0
	ErrInvalidInterval = errors.New("Interval must be greater than 0")
	// ErrInvalidStopTime - Error message for the stop tome is in the past
	ErrInvalidStopTime = errors.New("Stop time is in the past")
	// ErrStopBeforeStart - Error message for the stop time cannot occur before start time
	ErrStopBeforeStart = errors.New("Stop time cannot occur before start time")
)

// ScheduleState int type
type ScheduleState int

const (
	// Active - set by the scheduler once a schedule has been validated and is in use in a task
	Active ScheduleState = iota
	// Ended - Schedule is ended
	Ended
	// Error - Schedule is halted with an error
	Error
)

// Schedule interface
type Schedule interface {
	// Returns the current state of the schedule
	GetState() ScheduleState
	// Returns where a schedule is still valid for this point in time.
	Validate() error
	// Blocks until time to fire and returns a schedule.Response
	Wait(time.Time) Response
}

// Response interface defines the behavior of schedule response
type Response interface {
	// Contains any errors captured during a schedule.Wait()
	Error() error
	// Returns the schedule state
	State() ScheduleState
	// Returns any intervals that were missed since the call to Wait()
	Missed() uint
	// The time the interval fired
	LastTime() time.Time
}

func waitOnInterval(last time.Time, i time.Duration) (uint, time.Time) {
	log.WithFields(log.Fields{
		"block": "pkg/schedule/schedule.go",
		"module": "waitOnInterval",
		"last_time": last,
		"interval": i,
	}).Info("Debug Iza, waiting on interval")
	if (last == time.Time{}) {
		log.WithFields(log.Fields{
		"block": "pkg/schedule/schedule.go",
		"module": "waitOnInterval",
	}).Info("Debug Iza, start DO NOT SLEEPING 1")
		time.Sleep(i)
		log.WithFields(log.Fields{
		"block": "pkg/schedule/schedule.go",
		"module": "waitOnInterval",
	}).Info("Debug Iza, end DO NOT SLEEPING 1")
		return uint(0), time.Now()
	}
	// Get the difference in time.Duration since last in nanoseconds (int64)
	timeDiff := time.Since(last).Nanoseconds()
	// cache our schedule interval in nanoseconds
	nanoInterval := i.Nanoseconds()
	// use modulo operation to obtain the remainder of time over last interval
	remainder := timeDiff % nanoInterval
	// subtract remainder from
	missed := (timeDiff - remainder) / nanoInterval // timeDiff.Nanoseconds() % s.Interval.Nanoseconds()
	waitDuration := nanoInterval - remainder

	log.WithFields(log.Fields{
		"block": "pkg/schedule/schedule.go",
		"module": "waitOnInterval",
		"last": last,
		"interval": i,
		"timeDiff": timeDiff,
		"nanoInterval": nanoInterval,
		"remainder": remainder,
		"missed": missed,
		"waitDuration": waitDuration,
	}).Info("Debug Iza, values taken into account")

	// Wait until predicted interval fires
	log.WithFields(log.Fields{
		"block": "pkg/schedule/schedule.go",
		"module": "waitOnInterval",
		"last": last,
		"start_time": time.Now(),
		"waitDuration": waitDuration,
	}).Info("Debug Iza, start SLEEPING2")
	time.Sleep(time.Duration(waitDuration))
	log.WithFields(log.Fields{
		"block": "pkg/schedule/schedule.go",
		"module": "waitOnInterval",
		"last": last,
		"end_time": time.Now(),
		"waitDuration": waitDuration,
	}).Info("Debug Iza, end SLEEPING2")
	return uint(missed), time.Now()
}
