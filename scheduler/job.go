/*
http://www.apache.org/licenses/LICENSE-2.0.txt


Copyright 2015 Intel Corporation

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

package scheduler

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/intelsdi-x/snap/control/plugin"
	"github.com/intelsdi-x/snap/core"
	"github.com/intelsdi-x/snap/core/cdata"
	"github.com/intelsdi-x/snap/core/ctypes"
	. "github.com/intelsdi-x/snap/pkg/promise"
)

const (
	collectJobType jobType = iota
	publishJobType
	processJobType
)

const (
	defaultDeadline = time.Duration(5 * time.Second)
)

// Represents a queued job, together with a synchronization
// barrier to signal job completion (successful or otherwise).
//
// Functions that operate on this type (IsComplete, Complete,
// Await) are idempotent and thread-safe.
type queuedJob interface {
	Job() job
	Promise() Promise
}

type qj struct {
	job     job
	promise Promise
}

func newQueuedJob(job job) queuedJob {
	return &qj{
		job:     job,
		promise: NewPromise(),
	}
}

// Returns the underlying job.
func (j *qj) Job() job {
	return j.job
}

// Returns the underlying promise.
func (j *qj) Promise() Promise {
	return j.promise
}

// Primary type for job inside
// the scheduler.  Job encompasses all
// all job types -- collect, process, and publish.
type job interface {
	AddErrors(errs ...error)
	Errors() []error
	StartTime() time.Time
	Deadline() time.Time
	Type() jobType
	Run()
}

type jobType int

type coreJob struct {
	sync.Mutex

	jtype     jobType
	deadline  time.Time
	starttime time.Time
	errors    []error
}

func newCoreJob(t jobType, deadline time.Time) *coreJob {
	return &coreJob{
		jtype:     t,
		deadline:  deadline,
		errors:    make([]error, 0),
		starttime: time.Now(),
	}
}

func (c *coreJob) StartTime() time.Time {
	return c.starttime
}

func (c *coreJob) Deadline() time.Time {
	return c.deadline
}

func (c *coreJob) Type() jobType {
	return c.jtype
}

func (c *coreJob) AddErrors(errs ...error) {
	c.Lock()
	defer c.Unlock()
	c.errors = append(c.errors, errs...)
}

func (c *coreJob) Errors() []error {
	return c.errors
}

type collectorJob struct {
	*coreJob
	collector      collectsMetrics
	metricTypes    []core.RequestedMetric
	metrics        []core.Metric
	configDataTree *cdata.ConfigDataTree
}

func newCollectorJob(metricTypes []core.RequestedMetric, deadlineDuration time.Duration, collector collectsMetrics, cdt *cdata.ConfigDataTree) job {
	return &collectorJob{
		collector:      collector,
		metricTypes:    metricTypes,
		metrics:        []core.Metric{},
		coreJob:        newCoreJob(collectJobType, time.Now().Add(deadlineDuration)),
		configDataTree: cdt,
	}
}

type metric struct {
	namespace []string
	version   int
	source	  string
	config    *cdata.ConfigDataNode
}

func (m *metric) Namespace() []string {
	return m.namespace
}

func (m *metric) Config() *cdata.ConfigDataNode {
	return m.config
}

func (m *metric) Version() int {
	return m.version
}

func (m *metric) Source() string {
	return m.source
}

func (m *metric) Data() interface{}             { return nil }
func (m *metric) Tags() map[string]string       { return nil }
func (m *metric) Labels() []core.Label          { return nil }
func (m *metric) LastAdvertisedTime() time.Time { return time.Unix(0, 0) }
func (m *metric) Timestamp() time.Time          { return time.Unix(0, 0) }

func (c *collectorJob) Run() {
	log.WithFields(log.Fields{
		"_module":      "scheduler-job",
		"block":        "run",
		"job-type":     "collector",
		"metric-count": len(c.metricTypes),
	}).Debug("starting collector job")
	metrics := make([]core.Metric, len(c.metricTypes))
	for i, rmt := range c.metricTypes {
		config := c.configDataTree.Get(rmt.Namespace())
		if config == nil {
			config = cdata.NewNode()
		}
		metrics[i] = &metric{
			namespace: rmt.Namespace(),
			version:   rmt.Version(),
			source:	   rmt.Source(),
			config:    config,
		}
	}
	ret, errs := c.collector.CollectMetrics(metrics, c.Deadline())

	log.WithFields(log.Fields{
		"_module":      "scheduler-job",
		"block":        "run",
		"job-type":     "collector",
		"metric-count": len(ret),
	}).Debug("collector run completed")

	c.metrics = ret
	if errs != nil {
		for _, e := range errs {
			log.WithFields(log.Fields{
				"_module":  "scheduler-job",
				"block":    "run",
				"job-type": "collector",
				"error":    e,
			}).Error("collector run error")
		}
		c.AddErrors(errs...)
	}
}

type processJob struct {
	*coreJob
	processor     processesMetrics
	parentJob     job
	metrics       []core.Metric
	pluginName    string
	pluginVersion int
	config        map[string]ctypes.ConfigValue
	contentType   string
	content       []byte
}

func newProcessJob(parentJob job, pluginName string, pluginVersion int, contentType string, config map[string]ctypes.ConfigValue, processor processesMetrics) job {
	return &processJob{
		parentJob:     parentJob,
		pluginName:    pluginName,
		pluginVersion: pluginVersion,
		metrics:       []core.Metric{},
		coreJob:       newCoreJob(processJobType, parentJob.Deadline()),
		config:        config,
		processor:     processor,
		contentType:   contentType,
	}
}

func (p *processJob) Run() {
	log.WithFields(log.Fields{
		"_module":        "scheduler-job",
		"block":          "run",
		"job-type":       "processor",
		"content-type":   p.contentType,
		"plugin-name":    p.pluginName,
		"plugin-version": p.pluginVersion,
		"plugin-config":  p.config,
	}).Debug("starting processor job")

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	switch p.parentJob.Type() {
	case collectJobType:
		switch p.contentType {
		case plugin.SnapGOBContentType:
			metrics := make([]plugin.PluginMetricType, len(p.parentJob.(*collectorJob).metrics))
			for i, m := range p.parentJob.(*collectorJob).metrics {
				switch mt := m.(type) {
				case plugin.PluginMetricType:
					metrics[i] = mt
				default:
					panic("unsupported type")
				}
			}
			enc.Encode(metrics)
			_, content, errs := p.processor.ProcessMetrics(p.contentType, buf.Bytes(), p.pluginName, p.pluginVersion, p.config)
			if errs != nil {
				for _, e := range errs {
					log.WithFields(log.Fields{
						"_module":        "scheduler-job",
						"block":          "run",
						"job-type":       "processor",
						"content-type":   p.contentType,
						"plugin-name":    p.pluginName,
						"plugin-version": p.pluginVersion,
						"plugin-config":  p.config,
						"error":          e.Error(),
					}).Error("error with processor job")
				}
				p.AddErrors(errs...)
			}
			p.content = content
		default:
			log.WithFields(log.Fields{
				"_module":        "scheduler-job",
				"block":          "run",
				"job-type":       "processor",
				"content-type":   p.contentType,
				"plugin-name":    p.pluginName,
				"plugin-version": p.pluginVersion,
				"plugin-config":  p.config,
			}).Fatal("unsupported content type")
			panic(fmt.Sprintf("unsupported content type. {plugin name: %s version: %v content-type: '%v'}", p.pluginName, p.pluginVersion, p.contentType))
		}
	default:
		log.WithFields(log.Fields{
			"_module":         "scheduler-job",
			"block":           "run",
			"job-type":        "processor",
			"content-type":    p.contentType,
			"plugin-name":     p.pluginName,
			"plugin-version":  p.pluginVersion,
			"plugin-config":   p.config,
			"parent-job-type": p.parentJob.Type(),
		}).Fatal("unsupported parent job type")
		panic("unsupported parent job type")
	}
}

type publisherJob struct {
	*coreJob
	parentJob     job
	publisher     publishesMetrics
	pluginName    string
	pluginVersion int
	config        map[string]ctypes.ConfigValue
	contentType   string
}

func newPublishJob(parentJob job, pluginName string, pluginVersion int, contentType string, config map[string]ctypes.ConfigValue, publisher publishesMetrics) job {
	return &publisherJob{
		parentJob:     parentJob,
		publisher:     publisher,
		pluginName:    pluginName,
		pluginVersion: pluginVersion,
		coreJob:       newCoreJob(publishJobType, parentJob.Deadline()),
		config:        config,
		contentType:   contentType,
	}
}

func (p *publisherJob) Run() {
	log.WithFields(log.Fields{
		"_module":        "scheduler-job",
		"block":          "run",
		"job-type":       "publisher",
		"content-type":   p.contentType,
		"plugin-name":    p.pluginName,
		"plugin-version": p.pluginVersion,
		"plugin-config":  p.config,
	}).Debug("starting publisher job")
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	switch p.parentJob.Type() {
	case collectJobType:
		switch p.contentType {
		case plugin.SnapGOBContentType:
			metrics := make([]plugin.PluginMetricType, len(p.parentJob.(*collectorJob).metrics))
			for i, m := range p.parentJob.(*collectorJob).metrics {
				switch mt := m.(type) {
				case plugin.PluginMetricType:
					metrics[i] = mt
				default:
					panic("unsupported type")
				}
			}
			enc.Encode(metrics)
			errs := p.publisher.PublishMetrics(p.contentType, buf.Bytes(), p.pluginName, p.pluginVersion, p.config)
			if errs != nil {
				for _, e := range errs {
					log.WithFields(log.Fields{
						"_module":        "scheduler-job",
						"block":          "run",
						"job-type":       "publisher",
						"content-type":   p.contentType,
						"plugin-name":    p.pluginName,
						"plugin-version": p.pluginVersion,
						"plugin-config":  p.config,
						"error":          e.Error(),
					}).Error("error with publisher job")
				}
				p.AddErrors(errs...)
			}
		default:
			log.WithFields(log.Fields{
				"_module":        "scheduler-job",
				"block":          "run",
				"job-type":       "publisher",
				"content-type":   p.contentType,
				"plugin-name":    p.pluginName,
				"plugin-version": p.pluginVersion,
				"plugin-config":  p.config,
			}).Fatal("unsupported content type")
			panic(fmt.Sprintf("unsupported content type. {plugin name: %s version: %v content-type: '%v'}", p.pluginName, p.pluginVersion, p.contentType))
		}
	case processJobType:
		switch p.contentType {
		case plugin.SnapGOBContentType:
			errs := p.publisher.PublishMetrics(p.contentType, p.parentJob.(*processJob).content, p.pluginName, p.pluginVersion, p.config)
			if errs != nil {
				for _, e := range errs {
					log.WithFields(log.Fields{
						"_module":        "scheduler-job",
						"block":          "run",
						"job-type":       "publisher",
						"content-type":   p.contentType,
						"plugin-name":    p.pluginName,
						"plugin-version": p.pluginVersion,
						"plugin-config":  p.config,
						"error":          e.Error(),
					}).Error("error with publisher job")
				}
				p.AddErrors(errs...)
			}
		}
	default:
		log.WithFields(log.Fields{
			"_module":         "scheduler-job",
			"block":           "run",
			"job-type":        "publisher",
			"content-type":    p.contentType,
			"plugin-name":     p.pluginName,
			"plugin-version":  p.pluginVersion,
			"plugin-config":   p.config,
			"parent-job-type": p.parentJob.Type(),
		}).Fatal("unsupported parent job type")
		panic("unsupported job type")
	}
}
