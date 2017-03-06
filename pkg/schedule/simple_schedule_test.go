// +build legacy legacyiza

package schedule

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestSimpleSchedule(t *testing.T) {
	Convey("Simple Schedule", t, func() {
		Convey("test Wait()", func() {
			interval := 100
			overage := 467
			shouldWait := float64(500 - overage)
			last := time.Now()

			time.Sleep(time.Millisecond * time.Duration(overage))
			s := NewSimpleSchedule(time.Millisecond * time.Duration(interval))
			err := s.Validate()
			So(err, ShouldBeNil)

			before := time.Now()
			r := s.Wait(last)
			after := time.Since(before)

			So(r.Error(), ShouldEqual, nil)
			So(r.State(), ShouldEqual, Active)
			So(r.Missed(), ShouldEqual, 4)

			// We are ok at this precision with being within 10% over or under (10ms)
			afterMS := after.Nanoseconds() / 1000 / 1000
			So(afterMS, ShouldBeGreaterThan, shouldWait-10)
			So(afterMS, ShouldBeLessThan, shouldWait+10)
		})

		Convey("invalid schedule", func() {
			s := NewSimpleSchedule(0)
			err := s.Validate()
			So(err, ShouldResemble, ErrInvalidInterval)
		})

	})

		Convey("start immediately without determined stop", t, func() {
			interval := time.Millisecond * 10
			s := NewSimpleSchedule(interval)


			err := s.Validate()
			So(err, ShouldBeNil)

			var r []Response
			last := *new(time.Time)

			before := time.Now()
			for len(r) <= 10 {
				r1 := s.Wait(last)
				last = time.Now()
				r = append(r, r1)
			}

			var missed uint
			for _, x := range r {
				missed += x.Missed()
			}
			So(missed, ShouldEqual, 0)

			// the task should start immediately
			So(
				r[0].LastTime().Sub(before).Seconds(),
				ShouldBeBetweenOrEqual,
				0,
				(interval).Seconds(),
			)

		})
}
