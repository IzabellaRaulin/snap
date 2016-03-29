/*
http://www.apache.org/licenses/LICENSE-2.0.txt


Copyright 2015-2016 Intel Corporation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package control

import (
	"testing"
	"time"

	"github.com/intelsdi-x/snap/control/plugin"

	. "github.com/smartystreets/goconvey/convey"
)

func TestMetricType(t *testing.T) {
	Convey("newMetricType()", t, func() {
		Convey("returns a metricType", func() {
			mt := newMetricType([]string{"test"}, time.Now(), new(loadedPlugin))
			So(mt, ShouldHaveSameTypeAs, new(metricType))
		})
	})
	Convey("metricType.Namespace()", t, func() {
		Convey("returns the namespace of a metricType", func() {
			ns := []string{"test"}
			mt := newMetricType(ns, time.Now(), new(loadedPlugin))
			So(mt.Namespace(), ShouldHaveSameTypeAs, ns)
			So(mt.Namespace(), ShouldResemble, ns)
		})
	})
	Convey("metricType.Version()", t, func() {
		Convey("returns the namespace of a metricType", func() {
			ns := []string{"test"}
			lp := &loadedPlugin{Meta: plugin.PluginMeta{Version: 1}}
			mt := newMetricType(ns, time.Now(), lp)
			So(mt.Version(), ShouldEqual, 1)
		})
	})
	Convey("metricType.LastAdvertisedTimestamp()", t, func() {
		Convey("returns the LastAdvertisedTimestamp for the metricType", func() {
			ts := time.Now()
			mt := newMetricType([]string{"test"}, ts, new(loadedPlugin))
			So(mt.LastAdvertisedTime(), ShouldHaveSameTypeAs, ts)
			So(mt.LastAdvertisedTime(), ShouldResemble, ts)
		})
	})
	Convey("metricType.Key()", t, func() {
		Convey("returns the key for the metricType", func() {
			ts := time.Now()
			lp := new(loadedPlugin)
			mt := newMetricType([]string{"foo", "bar"}, ts, lp)
			key := mt.Key()
			So(key, ShouldEqual, "/foo/bar/0")
		})
		Convey("returns the key for the queried version", func() {
			ts := time.Now()
			lp2 := new(loadedPlugin)
			lp2.Meta.Version = 2
			mt := newMetricType([]string{"foo", "bar"}, ts, lp2)
			key := mt.Key()
			So(key, ShouldEqual, "/foo/bar/2")
		})
	})
}

func TestMetricMatching(t *testing.T) {
	Convey("metricCatalog.GetMatchedMetrics()", t, func() {
		Convey("verify query support for static metrics", func() {
			mc := newMetricCatalog()
			ns := [][]string{
				{"mock", "foo", "bar"},
				{"mock", "foo", "baz"},
				{"mock", "asdf", "bar"},
				{"mock", "asdf", "baz"},
				{"mock", "test", "1"},
				{"mock", "test", "2"},
				{"mock", "test", "3"},
				{"mock", "test", "4"},
			}
			lp := new(loadedPlugin)
			ts := time.Now()
			mt := []*metricType{
				newMetricType(ns[0], ts, lp),
				newMetricType(ns[1], ts, lp),
				newMetricType(ns[2], ts, lp),
				newMetricType(ns[3], ts, lp),
				newMetricType(ns[4], ts, lp),
				newMetricType(ns[5], ts, lp),
				newMetricType(ns[6], ts, lp),
				newMetricType(ns[7], ts, lp),
			}

			for _, v := range mt {
				mc.Add(v)
			}
			Convey("get /mock/foo/*", func() {
				nss, err := mc.GetMatchedMetrics([]string{"mock", "foo", "*"})
				So(err, ShouldBeNil)
				So(len(nss), ShouldEqual, 2)
				So(nss, ShouldResemble, [][]string{
					{"mock", "foo", "bar"},
					{"mock", "foo", "baz"},
				})
			})
			Convey("get /mock/test/*", func() {
				nss, err := mc.GetMatchedMetrics([]string{"mock", "test", "*"})
				So(err, ShouldBeNil)
				So(len(nss), ShouldEqual, 4)
				So(nss, ShouldResemble, [][]string{
					{"mock", "test", "1"},
					{"mock", "test", "2"},
					{"mock", "test", "3"},
					{"mock", "test", "4"},
				})

			})
			Convey("get /mock/*/bar", func() {
				nss, err := mc.GetMatchedMetrics([]string{"mock", "*", "bar"})
				So(err, ShouldBeNil)
				So(len(nss), ShouldEqual, 2)
				So(nss, ShouldResemble, [][]string{
					{"mock", "foo", "bar"},
					{"mock", "asdf", "bar"},
				})
			})
			Convey("get /mock/*", func() {
				nss, err := mc.GetMatchedMetrics([]string{"mock", "*"})
				So(err, ShouldBeNil)
				So(len(nss), ShouldEqual, len(ns))
				So(nss, ShouldResemble, ns)
			})
			Convey("get /mock/(foo|asdf)/baz", func() {
				nss, err := mc.GetMatchedMetrics([]string{"mock", "(foo|asdf)", "baz"})
				So(err, ShouldBeNil)
				So(len(nss), ShouldEqual, 2)
				So(nss, ShouldResemble, [][]string{
					{"mock", "foo", "baz"},
					{"mock", "asdf", "baz"},
				})
			})
			Convey("get /mock/test/(1|2|3)", func() {
				nss, err := mc.GetMatchedMetrics([]string{"mock", "test", "(1|2|3)"})
				So(err, ShouldBeNil)
				So(len(nss), ShouldEqual, 3)
				So(nss, ShouldResemble, [][]string{
					{"mock", "test", "1"},
					{"mock", "test", "2"},
					{"mock", "test", "3"},
				})
			})
			Convey("try to get not exist metric", func() {
				nss, err := mc.GetMatchedMetrics([]string{"mock", "not", "exist", "metric"})
				So(err, ShouldNotBeNil)
				So(nss, ShouldBeEmpty)
				So(err.Error(), ShouldContainSubstring, "Metric not found:")
			})
		})
		Convey("verify query support for dynamic metrics", func() {
			mc := newMetricCatalog()
			ns := [][]string{
				{"mock", "cgroups", "*", "bar"},
				{"mock", "cgroups", "*", "baz"},
				{"mock", "cgroups", "*", "in"},
				{"mock", "cgroups", "*", "out"},
				{"mock", "cgroups", "*", "test", "1"},
				{"mock", "cgroups", "*", "test", "2"},
				{"mock", "cgroups", "*", "test", "3"},
				{"mock", "cgroups", "*", "test", "4"},
			}
			lp := new(loadedPlugin)
			ts := time.Now()
			mt := []*metricType{
				newMetricType(ns[0], ts, lp),
				newMetricType(ns[1], ts, lp),
				newMetricType(ns[2], ts, lp),
				newMetricType(ns[3], ts, lp),
				newMetricType(ns[4], ts, lp),
				newMetricType(ns[5], ts, lp),
				newMetricType(ns[6], ts, lp),
				newMetricType(ns[7], ts, lp),
			}

			for _, v := range mt {
				mc.Add(v)
			}
			// check if metrics were added to metric catalog
			So(len(mc.Keys()), ShouldEqual, len(ns))

			Convey("get /mock/cgroups/*", func() {
				nss, err := mc.GetMatchedMetrics([]string{"mock", "cgroups", "*"})
				So(err, ShouldBeNil)
				So(len(nss), ShouldEqual, len(ns))
				So(nss, ShouldResemble, ns)
			})
			Convey("get /mock/cgroups/*/bar", func() {
				nss, err := mc.GetMatchedMetrics([]string{"mock", "cgroups", "*", "bar"})
				So(err, ShouldBeNil)
				So(len(nss), ShouldEqual, 1)
				So(nss, ShouldResemble, [][]string{
					{"mock", "cgroups", "*", "bar"},
				})
			})
			Convey("get /mock/cgroups/*/(bar|baz)", func() {
				nss, err := mc.GetMatchedMetrics([]string{"mock", "cgroups", "*", "(bar|baz)"})
				So(err, ShouldBeNil)
				So(len(nss), ShouldEqual, 2)
				So(nss, ShouldResemble, [][]string{
					{"mock", "cgroups", "*", "bar"},
					{"mock", "cgroups", "*", "baz"},
				})
			})
			Convey("get /mock/cgroups/*/test/*", func() {
				nss, err := mc.GetMatchedMetrics([]string{"mock", "cgroups", "*", "test", "*"})
				So(err, ShouldBeNil)
				So(len(nss), ShouldEqual, 4)
				So(nss, ShouldResemble, [][]string{
					{"mock", "cgroups", "*", "test", "1"},
					{"mock", "cgroups", "*", "test", "2"},
					{"mock", "cgroups", "*", "test", "3"},
					{"mock", "cgroups", "*", "test", "4"},
				})
			})
			Convey("get /mock/cgroups/*/test/(1|2|3)", func() {
				nss, err := mc.GetMatchedMetrics([]string{"mock", "cgroups", "*", "test", "(1|2|3)"})
				So(err, ShouldBeNil)
				So(len(nss), ShouldEqual, 3)
				So(nss, ShouldResemble, [][]string{
					{"mock", "cgroups", "*", "test", "1"},
					{"mock", "cgroups", "*", "test", "2"},
					{"mock", "cgroups", "*", "test", "3"},
				})
			})
		})
	})
}

func TestMetricCatalog(t *testing.T) {
	Convey("newMetricCatalog()", t, func() {
		Convey("returns a metricCatalog", func() {
			mc := newMetricCatalog()
			So(mc, ShouldHaveSameTypeAs, new(metricCatalog))
		})
	})
	Convey("metricCatalog.Add()", t, func() {
		Convey("adds a metricType to the metricCatalog", func() {
			ns := []string{"test"}
			mt := newMetricType(ns, time.Now(), new(loadedPlugin))
			mc := newMetricCatalog()
			mc.Add(mt)
			_mt, err := mc.GetMetric(ns, -1)
			So(_mt, ShouldResemble, mt)
			So(err, ShouldBeNil)
		})
	})
	Convey("metricCatalog.GetMetric()", t, func() {
		mc := newMetricCatalog()
		ts := time.Now()
		Convey("add multiple metricTypes and get them back", func() {
			ns := [][]string{
				{"test1"},
				{"test2"},
				{"test3"},
			}
			lp := new(loadedPlugin)
			mt := []*metricType{
				newMetricType(ns[0], ts, lp),
				newMetricType(ns[1], ts, lp),
				newMetricType(ns[2], ts, lp),
			}
			for _, v := range mt {
				mc.Add(v)
			}
			for k, v := range ns {
				_mt, err := mc.GetMetric(v, -1)
				So(_mt, ShouldEqual, mt[k])
				So(err, ShouldBeNil)
			}
		})
		Convey("it returns the latest version", func() {
			lp2 := new(loadedPlugin)
			lp2.Meta.Version = 2
			lp35 := new(loadedPlugin)
			lp35.Meta.Version = 35
			m2 := newMetricType([]string{"foo", "bar"}, ts, lp2)
			mc.Add(m2)
			m35 := newMetricType([]string{"foo", "bar"}, ts, lp35)
			mc.Add(m35)
			m, err := mc.GetMetric([]string{"foo", "bar"}, -1)
			So(err, ShouldBeNil)
			So(m, ShouldEqual, m35)
		})
		Convey("it returns the queried version", func() {
			lp2 := new(loadedPlugin)
			lp2.Meta.Version = 2
			lp35 := new(loadedPlugin)
			lp35.Meta.Version = 35
			m2 := newMetricType([]string{"foo", "bar"}, ts, lp2)
			mc.Add(m2)
			m35 := newMetricType([]string{"foo", "bar"}, ts, lp35)
			mc.Add(m35)
			m, err := mc.GetMetric([]string{"foo", "bar"}, 2)
			So(err, ShouldBeNil)
			So(m, ShouldEqual, m2)
		})
		Convey("it returns metric not found if version doesn't exist", func() {
			lp2 := new(loadedPlugin)
			lp2.Meta.Version = 2
			lp35 := new(loadedPlugin)
			lp35.Meta.Version = 35
			m2 := newMetricType([]string{"foo", "bar"}, ts, lp2)
			mc.Add(m2)
			m35 := newMetricType([]string{"foo", "bar"}, ts, lp35)
			mc.Add(m35)
			_, err := mc.GetMetric([]string{"foo", "bar"}, 7)
			So(err.Error(), ShouldContainSubstring, "Metric not found:")
		})
	})
	Convey("metricCatalog.Table()", t, func() {
		Convey("returns a copy of the table", func() {
			mc := newMetricCatalog()
			mt := newMetricType([]string{"foo", "bar"}, time.Now(), &loadedPlugin{})
			mc.Add(mt)
			//TODO test tree
			//So(mc.Table(), ShouldHaveSameTypeAs, map[string][]*metricType{})
			//So(mc.Table()["foo.bar"], ShouldResemble, []*metricType{mt})
		})
	})
	Convey("metricCatalog.Next()", t, func() {
		ns := []string{"test"}
		mt := newMetricType(ns, time.Now(), new(loadedPlugin))
		mc := newMetricCatalog()
		Convey("returns false on empty table", func() {
			ok := mc.Next()
			So(ok, ShouldEqual, false)
		})
		Convey("returns true on populated table", func() {
			mc.Add(mt)
			ok := mc.Next()
			So(ok, ShouldEqual, true)
		})
	})
	Convey("metricCatalog.Item()", t, func() {
		ns := [][]string{
			{"test1"},
			{"test2"},
			{"test3"},
		}
		lp := new(loadedPlugin)
		t := time.Now()
		mt := []*metricType{
			newMetricType(ns[0], t, lp),
			newMetricType(ns[1], t, lp),
			newMetricType(ns[2], t, lp),
		}
		mc := newMetricCatalog()
		for _, v := range mt {
			mc.Add(v)
		}
		Convey("return first key and item in table", func() {
			mc.Next()
			key, item := mc.Item()
			So(key, ShouldEqual, getMetricKey(ns[0]))
			So(item, ShouldResemble, []*metricType{mt[0]})
		})
		Convey("return second key and item in table", func() {
			mc.Next()
			mc.Next()
			key, item := mc.Item()
			So(key, ShouldEqual, getMetricKey(ns[1]))
			So(item, ShouldResemble, []*metricType{mt[1]})
		})
		Convey("return third key and item in table", func() {
			mc.Next()
			mc.Next()
			mc.Next()
			key, item := mc.Item()
			So(key, ShouldEqual, getMetricKey(ns[2]))
			So(item, ShouldResemble, []*metricType{mt[2]})
		})
	})
	Convey("metricCatalog.GetMetrics()", t, func() {
		mc := newMetricCatalog()
		ts := time.Now()
		nss := [][]string{
			{"mock", "test", "1"},
			{"mock", "test", "2"},
			{"mock", "test", "3"},
			{"mock", "cgroups", "*", "in"},
			{"mock", "cgroups", "*", "out"},
		}
		Convey("add metricTypes and get them back", func() {
			// adding metrics to metric catalog
			mt := []*metricType{}
			for i, ns := range nss {
				mt = append(mt, newMetricType(ns, ts, new(loadedPlugin)))
				mc.Add(mt[i])
			}
			Convey("get metrics explicitly, one by one", func() {
				for k, m := range mt {
					_mt, err := mc.GetMetrics(m.namespace, -1)
					So(err, ShouldBeNil)
					So(len(_mt), ShouldEqual, 1)
					So(_mt, ShouldContain, mt[k])
				}
			})
			Convey("get metrics with namespace /mock/test/*", func() {
				ns := []string{"mock", "test", "*"}
				_mt, err := mc.GetMetrics(ns, -1)
				So(err, ShouldBeNil)
				So(len(_mt), ShouldEqual, 3)
				So(_mt, ShouldResemble, mt[:3])
			})
			Convey("get metrics with namespace /mock/test/(1|2)", func() {
				ns := []string{"mock", "test", "(1|2)"}
				_mt, err := mc.GetMetrics(ns, -1)
				So(err, ShouldBeNil)
				So(len(_mt), ShouldEqual, 2)
				So(_mt, ShouldResemble, mt[:2])
			})
			Convey("get metrics with namespace /mock/cgroups/*/in", func() {
				ns := []string{"mock", "cgroups", "*", "in"}
				_mt, err := mc.GetMetrics(ns, -1)
				So(err, ShouldBeNil)
				So(len(_mt), ShouldEqual, 1)
				So(_mt, ShouldContain, mt[3])
			})
			Convey("get metrics with namespace /mock/cgroups/*/(in|out|bad)", func() {
				ns := []string{"mock", "cgroups", "*", "(in|out|bad)"}
				_mt, err := mc.GetMetrics(ns, -1)
				So(err, ShouldBeNil)
				// two metrics should be returned ("bad" does not exist)
				So(len(_mt), ShouldEqual, 2)
				So(_mt, ShouldResemble, mt[3:])
			})
			Convey("get metrics with namespace /mock/cgroups/*/bad", func() {
				ns := []string{"mock", "cgroups", "*", "bad"}
				_mt, err := mc.GetMetrics(ns, -1)
				So(err, ShouldNotBeNil)
				So(_mt, ShouldBeEmpty)
				So(err.Error(), ShouldContainSubstring, "Metric not found:")
			})
			Convey("get metrics with namespace /mock/*", func() {
				ns := []string{"mock", "*"}
				_mt, err := mc.GetMetrics(ns, -1)
				So(err, ShouldBeNil)
				// all cataloged metrics should be returned
				So(len(_mt), ShouldEqual, len(mt))
				So(_mt, ShouldResemble, mt)
			})
		})
		Convey("get metrics with specific version", func() {
			// adding metrics to metric catalog with different versions
			mt2 := []*metricType{} //mock metrics type which versions equal 2
			mt3 := []*metricType{} //mock metrics type which versions equal 3

			lp2 := new(loadedPlugin)
			lp2.Meta.Version = 2

			lp3 := new(loadedPlugin)
			lp3.Meta.Version = 3

			for i, ns := range nss {
				mt2 = append(mt2, newMetricType(ns, ts, lp2))
				mt3 = append(mt3, newMetricType(ns, ts, lp3))
				// adding metrics to metric catalog in two versions
				mc.Add(mt2[i])
				mc.Add(mt3[i])
			}
			Convey("it returns the latest version of metrics", func() {
				_mts, err := mc.GetMetrics([]string{"mock", "*"}, -1)
				So(err, ShouldBeNil)
				So(_mts, ShouldResemble, mt3)
			})
			Convey("it returns the queried version", func() {
				_mts, err := mc.GetMetrics([]string{"mock", "*"}, 2)
				So(err, ShouldBeNil)
				So(_mts, ShouldResemble, mt2)
			})
			Convey("it returns metric not found if version doesn't exist", func() {
				_mts, err := mc.GetMetrics([]string{"mock", "*"}, 40)
				So(_mts, ShouldBeEmpty)
				So(err.Error(), ShouldContainSubstring, "Metric not found:")
				So(err.Error(), ShouldContainSubstring, "version: 40")
			})
		})
	})
	Convey("metricCatalog.Remove()", t, func() {
		mc := newMetricCatalog()
		ts := time.Now()
		nss := [][]string{
			{"mock", "test", "1"},
			{"mock", "test", "2"},
			{"mock", "test", "3"},
			{"mock", "cgroups", "*", "in"},
			{"mock", "cgroups", "*", "out"},
		}
		Convey("removes a metricType from the catalog", func() {
			// adding metrics to the catalog
			mt := []*metricType{}
			for i, ns := range nss {
				mt = append(mt, newMetricType(ns, ts, new(loadedPlugin)))
				mc.Add(mt[i])
			}
			Convey("validate adding metrics to the catalog", func() {
				for _, ns := range nss {
					// check if metric is in metric catalog
					_mts, err := mc.GetMetrics(ns, -1)
					So(_mts, ShouldNotBeEmpty)
					So(err, ShouldBeNil)
				}
			})

			// remove a single metric from the catalog
			mc.Remove(nss[0])

			Convey("validate removing a single metric from the catalog", func() {
				_mts, err := mc.GetMetrics([]string{"mock", "*"}, -1)
				So(len(_mts), ShouldEqual, len(nss)-1)
				So(err, ShouldBeNil)

				_, err = mc.GetMetrics(nss[0], -1)
				So(err, ShouldNotBeNil)
			})

			// remove the rest metrics from the catalog
			for _, ns := range nss[1:] {
				mc.Remove(ns)
			}

			Convey("validate removing all metrics from the catalog", func() {
				_mts, err := mc.GetMetrics([]string{"mock", "*"}, -1)
				So(_mts, ShouldBeEmpty)
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "Metric not found:")
			})
		})
	})
}

func TestSubscribe(t *testing.T) {
	ns := [][]string{
		{"test1"},
		{"test2"},
		{"test3"},
	}
	lp := new(loadedPlugin)
	ts := time.Now()
	mt := []*metricType{
		newMetricType(ns[0], ts, lp),
		newMetricType(ns[1], ts, lp),
		newMetricType(ns[2], ts, lp),
	}
	mc := newMetricCatalog()
	for _, v := range mt {
		mc.Add(v)
	}
	Convey("when the metric is not in the table", t, func() {
		Convey("then it returns an error", func() {
			err := mc.Subscribe([]string{"test4"}, -1)
			So(err.Error(), ShouldContainSubstring, "Metric not found:")
		})
	})
	Convey("when the metric is in the table", t, func() {
		Convey("then it gets correctly increments the count", func() {
			err := mc.Subscribe([]string{"test1"}, -1)
			So(err, ShouldBeNil)
			m, err2 := mc.GetMetric([]string{"test1"}, -1)
			So(err2, ShouldBeNil)
			So(m.subscriptions, ShouldEqual, 1)
		})
	})
}

func TestUnsubscribe(t *testing.T) {
	ns := [][]string{
		{"test1"},
		{"test2"},
		{"test3"},
	}
	lp := new(loadedPlugin)
	ts := time.Now()
	mt := []*metricType{
		newMetricType(ns[0], ts, lp),
		newMetricType(ns[1], ts, lp),
		newMetricType(ns[2], ts, lp),
	}
	mc := newMetricCatalog()
	for _, v := range mt {
		mc.Add(v)
	}
	Convey("when the metric is in the table", t, func() {
		Convey("then its subscription count is decremented", func() {
			err := mc.Subscribe([]string{"test1"}, -1)
			So(err, ShouldBeNil)
			err1 := mc.Unsubscribe([]string{"test1"}, -1)
			So(err1, ShouldBeNil)
			m, err2 := mc.GetMetric([]string{"test1"}, -1)
			So(err2, ShouldBeNil)
			So(m.subscriptions, ShouldEqual, 0)
		})
	})
	Convey("when the metric is not in the table", t, func() {
		Convey("then it returns metric not found error", func() {
			err := mc.Unsubscribe([]string{"test4"}, -1)
			So(err.Error(), ShouldContainSubstring, "Metric not found:")
		})
	})
	Convey("when the metric's count is already 0", t, func() {
		Convey("then it returns negative subscription count error", func() {
			err := mc.Unsubscribe([]string{"test1"}, -1)
			So(err, ShouldResemble, errNegativeSubCount)
		})
	})
}

func TestSubscriptionCount(t *testing.T) {
	m := newMetricType([]string{"test"}, time.Now(), &loadedPlugin{})
	Convey("it returns the subscription count", t, func() {
		m.Subscribe()
		So(m.SubscriptionCount(), ShouldEqual, 1)
		m.Subscribe()
		m.Subscribe()
		So(m.SubscriptionCount(), ShouldEqual, 3)
		m.Unsubscribe()
		So(m.SubscriptionCount(), ShouldEqual, 2)
	})
}

func TestMetricNamespaceValidation(t *testing.T) {
	Convey("validateMetricNamespace()", t, func() {
		Convey("validation passes", func() {
			ns := []string{"mock", "foo", "bar"}
			err := validateMetricNamespace(ns)
			So(err, ShouldBeNil)
		})
		Convey("contains not allowed characters", func() {
			ns := []string{"mock", "foo", "(bar)"}
			err := validateMetricNamespace(ns)
			So(err, ShouldNotBeNil)
		})
		Convey("contains unacceptable wildcardat at the end", func() {
			ns := []string{"mock", "foo", "*"}
			err := validateMetricNamespace(ns)
			So(err, ShouldNotBeNil)
		})

	})
}
