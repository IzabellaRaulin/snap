// +build small

package schedule

import (
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"

	. "github.com/smartystreets/goconvey/convey"
)

func TestWindowedScheduleValidation(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	Convey("invalid an interval", t, func() {
		Convey("zero value", func() {
			interval := time.Millisecond * 0
			w := NewWindowedSchedule(interval, nil, nil, 0)
			err := w.Validate()
			So(err, ShouldEqual, ErrInvalidInterval)
		})
		Convey("negative value", func() {
			interval := time.Millisecond * -1
			w := NewWindowedSchedule(interval, nil, nil, 0)
			err := w.Validate()
			So(err, ShouldEqual, ErrInvalidInterval)
		})
	})
	Convey("start time in past is ok (as long as window ends in the future)", t, func() {
		start := time.Now().Add(time.Second * -10)
		stop := time.Now().Add(time.Second * 10)
		w := NewWindowedSchedule(time.Millisecond*100, &start, &stop, 0)
		err := w.Validate()
		So(err, ShouldEqual, nil)
	})
	Convey("window in past", t, func() {
		start := time.Now().Add(time.Second * -20)
		stop := time.Now().Add(time.Second * -10)
		w := NewWindowedSchedule(time.Millisecond*100, &start, &stop, 0)
		err := w.Validate()
		So(err, ShouldEqual, ErrInvalidStopTime)
	})
	Convey("cart before the horse", t, func() {
		start := time.Now().Add(time.Second * 100)
		stop := time.Now().Add(time.Second * 10)
		w := NewWindowedSchedule(time.Millisecond*100, &start, &stop, 0)
		err := w.Validate()
		So(err, ShouldEqual, ErrStopBeforeStart)
	})
}
