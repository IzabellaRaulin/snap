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

package control

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
	"os"

	log "github.com/Sirupsen/logrus"

	"github.com/intelsdi-x/snap/control/plugin/cpolicy"
	"github.com/intelsdi-x/snap/core"
	"github.com/intelsdi-x/snap/core/cdata"
	"github.com/intelsdi-x/snap/core/ctypes"
	"github.com/intelsdi-x/snap/core/serror"
	"regexp"
)

var (
	errMetricNotFound   = errors.New("metric not found")
	errNegativeSubCount = serror.New(errors.New("subscription count cannot be < 0"))
)

func errorMetricNotFound(ns []string, ver ...int) error {
	if len(ver) > 0 {
		return fmt.Errorf("Metric not found: %s (version: %d)", core.JoinNamespace(ns), ver[0])
	}
	return fmt.Errorf("Metric not found: %s", core.JoinNamespace(ns))
}

type metricCatalogItem struct {
	namespace string
	versions  map[int]core.Metric
}

func (m *metricCatalogItem) Namespace() string {
	return m.namespace
}

func (m *metricCatalogItem) Versions() map[int]core.Metric {
	return m.versions
}

type metricType struct {
	Plugin             *loadedPlugin
	namespace          []string
	version            int
	lastAdvertisedTime time.Time
	subscriptions      int
	policy             processesConfigData
	config             *cdata.ConfigDataNode
	data               interface{}
	source             string
	labels             []core.Label
	tags               map[string]string
	timestamp          time.Time
}

type processesConfigData interface {
	Process(map[string]ctypes.ConfigValue) (*map[string]ctypes.ConfigValue, *cpolicy.ProcessingErrors)
	HasRules() bool
}

func newMetricType(ns []string, last time.Time, plugin *loadedPlugin) *metricType {
	return &metricType{
		Plugin: plugin,

		namespace:          ns,
		lastAdvertisedTime: last,
	}
}

func (m *metricType) Key() string {
	return fmt.Sprintf("%s/%d", m.NamespaceAsString(), m.Version())
}

func (m *metricType) Namespace() []string {
	return m.namespace
}

func (m *metricType) NamespaceAsString() string {
	return core.JoinNamespace(m.Namespace())
}

func (m *metricType) Data() interface{} {
	return m.data
}

func (m *metricType) LastAdvertisedTime() time.Time {
	return m.lastAdvertisedTime
}

func (m *metricType) Subscribe() {
	m.subscriptions++
}

func (m *metricType) Unsubscribe() serror.SnapError {
	if m.subscriptions == 0 {
		return errNegativeSubCount
	}
	m.subscriptions--
	return nil
}

func (m *metricType) SubscriptionCount() int {
	return m.subscriptions
}

func (m *metricType) Version() int {
	if m.version > 0 {
		return m.version
	}
	if m.Plugin == nil {
		return -1
	}
	return m.Plugin.Version()
}

func (m *metricType) Config() *cdata.ConfigDataNode {
	return m.config
}

func (m *metricType) Policy() *cpolicy.ConfigPolicyNode {
	return m.policy.(*cpolicy.ConfigPolicyNode)
}

func (m *metricType) Source() string {
	return m.source
}

func (m *metricType) Tags() map[string]string {
	return m.tags
}

func (m *metricType) Labels() []core.Label {
	return m.labels
}

func (m *metricType) Timestamp() time.Time {
	return m.timestamp
}

type metricCatalog struct {
	tree         *MTTrie
	mutex        *sync.Mutex
	keys         []string
	mKeys        map[string][]string 	// mKeys holds metricCatalog keys which conform to given metric key which can include wildcards
	currentIter  int
}

func newMetricCatalog() *metricCatalog {
	var k []string
	return &metricCatalog{
		tree:        NewMTTrie(),
		mutex:       &sync.Mutex{},
		currentIter: 0,
		keys:        k,
		mKeys: 	     make(map[string][]string),
	}
}


// TODO still need?
func (mc *metricCatalog) Keys() ([]string) {
	return mc.keys
}

// GetMatchedMetrics returns all matched keys for metric with namespace 'ns'
func (mc *metricCatalog) GetMatchedMetrics(ns []string) ([][]string, error) {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()

	// get metric key (might contain wildcard(s))
	wkey := getMetricKey(ns)

	if _, exist := mc.mKeys[wkey]; !exist {
		// if not exist, match `wkey` with cataloged keys
		mc.updateOrAddNewItemToMatchingMap(wkey)
	}

	// mkeys means matched metrics keys
	mkeys := mc.mKeys[wkey]

	// convert matched keys to a slice of namespaces
	nss := convertKeysToNamespaces(mkeys)

	if len(nss) == 0 {
		return nil, errorMetricNotFound(ns)
	}

	return nss, nil
}


func convertKeysToNamespaces(keys []string) ([][]string) {
	// nss is a slice of slices which holds metrics namespaces
	nss := [][]string{}
	for _, key := range keys {
		ns := getMetricNamespace(key)
		if len(ns) != 0 {
			nss = append(nss, ns)
		}
	}

	return nss
}

/*
// recognizeRequiredTupleItems recognizes tuple with comma between items for which there is requirement that all of them have to be collected
func recognizeRequiredTupleItems (wkey string) []string {
	reqItems := []string{}

	regex := regexp.MustCompile(`^[\[]\w+[,]\w+[\]].*$`)


	match := regex.FindStringSubmatch(wkey)
	if match != nil {

	}

}
*/

// UpdateOrAddNewItemToMatchingMap adds `wkey` and matched for it keys to mKeys map
func (mc *metricCatalog) updateOrAddNewItemToMatchingMap(wkey string) {
	matchedKeys := []string{}

	//fmt.Fprintf(os.Stderr, "Debug in /control/metrics.go, AddKeyToMatchingMap\n")

	// for interpreting tuples which are put between square brackets

	// recognize type of tuples ("and" or "or")

	exp := wkey
	//exp := strings.Replace(wkey, "[", "(", -1)
	//exp = strings.Replace(exp, "]", ")", -1)

	// wkey contains `.` which should not be interpreted as regexp tokens, but as a single character
	exp = strings.Replace(exp, ".", "[.]", -1)

	// change `*` into regexp `.*` which matches any characters
	exp = strings.Replace(exp, "*", ".*", -1)

	regex := regexp.MustCompile("^" + exp + "$")

	for _, key := range mc.keys {

		//fmt.Fprintf(os.Stderr, "Debug match with key=%+v, ", key)
		match := regex.FindStringSubmatch(key)

		if match == nil {
			//fmt.Fprintf(os.Stderr, "try next...\n")
			continue
		}

		matchedKeys = appendIfMissing(matchedKeys, key)

		//fmt.Fprintf(os.Stderr, "Matched!\n\n\n\n\n")
	}

	// if "or"

	if len(matchedKeys) == 0 {
		mc.removeItemFromMatchingMap(wkey)
	} else {
		mc.mKeys[wkey] = matchedKeys
	}
}



func (mc *metricCatalog) removeItemFromMatchingMap(wkey string) {

	if _, exist := mc.mKeys[wkey]; exist {
		delete(mc.mKeys, wkey)
	}
}

func (mc *metricCatalog) updateMatchingMap() {

	for wkey := range mc.mKeys{
		mc.updateOrAddNewItemToMatchingMap(wkey)
	}

}

func (mc *metricCatalog) removeMatchedKey(key string) {
	mc.mutex.Lock()

	fmt.Fprintf(os.Stderr, "Debug in /control/metrics.go, AddMatchedKey\n")

	defer mc.mutex.Unlock()

	for wkey, mkeys := range mc.mKeys {

		for index, mkey := range mkeys {
			if mkey == key {
				// remove this key under index `i' from slice
				mc.mKeys[wkey] = append(mkeys[:index], mkeys[index + 1:]...)
			}
		}
	}
}

func (mc *metricCatalog) AddLoadedMetricType(lp *loadedPlugin, mt core.Metric) error {
	if lp.ConfigPolicy == nil {
		err := errors.New("Config policy is nil")
		log.WithFields(log.Fields{
			"_module": "control",
			"_file":   "metrics.go,",
			"_block":  "add-loaded-metric-type",
			"error":   err,
		}).Error("error adding loaded metric type")
		return err
	}

	newMt := metricType{
		Plugin:             lp,
		namespace:          mt.Namespace(),
		version:            mt.Version(),
		lastAdvertisedTime: mt.LastAdvertisedTime(),
		tags:               mt.Tags(),
		labels:             mt.Labels(),
		policy:             lp.ConfigPolicy.Get(mt.Namespace()),
	}
	mc.Add(&newMt)
	return nil
}

func (mc *metricCatalog) RmUnloadedPluginMetrics(lp *loadedPlugin) {
	fmt.Fprintf(os.Stderr, "Debug in metrics.go, RmUnloadedPluginMetrics\n")
	mc.mutex.Lock()
	defer mc.mutex.Unlock()
	mc.tree.DeleteByPlugin(lp)
	// todo tutaj musi by update mKey

	mc.updateMatchingMap()
}

// Add adds a metricType
func (mc *metricCatalog) Add(m *metricType) {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()

	key := getMetricKey(m.Namespace())
	mc.keys = appendIfMissing(mc.keys, key)

	mc.tree.Add(m)
}

// GetMetric retrieves a metric given a namespace and version.
// If provided a version of -1 the latest plugin will be returned.

func (mc *metricCatalog) GetMetric(ns []string, version int) (*metricType, error) {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()

	return mc.get(ns, version)
}



// GetMetrics retrieves all metrics given a namespace (can specify many namespaces using wildcard characters) and version.
// If provided a version of -1 the latest plugin will be returned.

func (mc *metricCatalog) GetMetrics(ns []string, version int) ([]*metricType, error) {


	//fmt.Fprintf(os.Stderr, "Debug in /control/metrics.go \n")
	//fmt.Fprintf(os.Stderr, "Debug in metricCatalog.keys_len=%+v\n", len(mc.keys))
	//fmt.Fprintf(os.Stderr, "Debug ns=%+v\n", strings.Join(ns, "/"))


	mnss, err := mc.GetMatchedMetrics(ns)

	if err != nil {
		return  nil, err
	}



	var mts []*metricType

	for _, mns := range mnss {

		m, err := mc.get(mns, version)

		if err != nil {
			fmt.Fprintf(os.Stderr, "Debug in control/metrics, Cannot get metric with ns=%+v, err=%+v\n", mns, err)
			continue
		}

		mts = append(mts,m)
		//fmt.Fprintf(os.Stderr, "Matched!\n\n\n\n\n")
	}


	//fmt.Fprintf(os.Stderr, "Debug: Matched %d metrics\n", len(mts))

	return mts, nil
}

// Fetch transactionally retrieves all metrics which fall under namespace ns
func (mc *metricCatalog) Fetch(ns []string) ([]*metricType, error) {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()

	mtsi, err := mc.tree.Fetch(ns)
	if err != nil {
		log.WithFields(log.Fields{
			"_module": "control",
			"_file":   "metrics.go,",
			"_block":  "fetch",
			"error":   err,
		}).Error("error fetching metrics")
		return nil, err
	}
	return mtsi, nil
}

// GetVersions retrieves all versions of a given metric namespace.
func (mc *metricCatalog) GetVersions(ns []string) ([]*metricType, error) {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()
	return mc.getVersions(ns)
}



func (mc *metricCatalog) Remove(ns []string) {
	mc.mutex.Lock()
	mc.tree.Remove(ns)

	// todo consider tree
	key := getMetricKey(ns)
	mc.removeMatchedKey(key)
	mc.mutex.Unlock()
}

// Item returns the current metricType in the collection.  The method Next()
// provides the  means to move the iterator forward.
func (mc *metricCatalog) Item() (string, []*metricType) {
	key := mc.keys[mc.currentIter-1]
	ns := strings.Split(key, ".")
	mtsi, _ := mc.tree.Get(ns)
	var mts []*metricType
	for _, mt := range mtsi {
		mts = append(mts, mt)
	}
	return key, mts
}

// Next returns true until the "end" of the collection is reached.  When
// the end of the collection is reached the iterator is reset back to the
// head of the collection.
func (mc *metricCatalog) Next() bool {
	mc.currentIter++
	if mc.currentIter > len(mc.keys) {
		mc.currentIter = 0
		return false
	}
	return true
}

// Subscribe atomically increments a metric's subscription count in the table.
func (mc *metricCatalog) Subscribe(ns []string, version int) error {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()

	m, err := mc.get(ns, version)
	if err != nil {
		log.WithFields(log.Fields{
			"_module": "control",
			"_file":   "metrics.go,",
			"_block":  "subscribe",
			"error":   err,
		}).Error("error getting metrics")
		return err
	}

	m.Subscribe()
	return nil
}

// Unsubscribe atomically decrements a metric's count in the table
func (mc *metricCatalog) Unsubscribe(ns []string, version int) error {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()

	m, err := mc.get(ns, version)
	if err != nil {
		log.WithFields(log.Fields{
			"_module": "control",
			"_file":   "metrics.go,",
			"_block":  "unsubscribe",
			"error":   err,
		}).Error("error getting metrics")
		return err
	}

	return m.Unsubscribe()
}

func (mc *metricCatalog) GetPlugin(mns []string, ver int) (*loadedPlugin, error) {
	m, err := mc.GetMetric(mns, ver)
	if err != nil {
		log.WithFields(log.Fields{
			"_module": "control",
			"_file":   "metrics.go,",
			"_block":  "get-plugin",
			"error":   err,
		}).Error("error getting plugin")
		return nil, err
	}
	return m.Plugin, nil
}


func (mc *metricCatalog) get(ns []string, ver int) (*metricType, error) {
	mts, err := mc.getVersions(ns)
	if err != nil {
		log.WithFields(log.Fields{
			"_module": "control",
			"_file":   "metrics.go,",
			"_block":  "get",
			"error":   err,
		}).Error("error getting plugin version from metric catalog")
		return nil, err
	}
	// a version IS given
	if ver > 0 {
		l, err := getVersion(mts, ver)
		if err != nil {
			log.WithFields(log.Fields{
				"_module": "control",
				"_file":   "metrics.go,",
				"_block":  "get",
				"error":   err,
			}).Error("error getting plugin version")
			return nil, errorMetricNotFound(ns, ver)
		}
		return l, nil
	}
	// ver is less than or equal to 0 get the latest
	return getLatest(mts), nil
}

func (mc *metricCatalog) getVersions(ns []string) ([]*metricType, error) {
	mts, err := mc.tree.Get(ns)
	if err != nil {
		log.WithFields(log.Fields{
			"_module": "control",
			"_file":   "metrics.go,",
			"_block":  "getVersions",
			"error":   err,
		}).Error("error getting plugin version")
		return nil, err
	}
	if mts == nil {
		return nil, errMetricNotFound
	}
	return mts, nil
}

func getMetricKey(metric []string) string {
	return strings.Join(metric, ".")
}

func getMetricNamespace(key string) []string {
	return strings.Split(key, ".")
}

func getLatest(c []*metricType) *metricType {
	cur := c[0]
	for _, mt := range c {
		if mt.Version() > cur.Version() {
			cur = mt
		}
	}
	return cur
}

func appendIfMissing(keys []string, ns string) []string {
	for _, key := range keys {
		if ns == key {
			return keys
		}
	}
	return append(keys, ns)
}

func getVersion(c []*metricType, ver int) (*metricType, error) {
	for _, m := range c {
		if m.Plugin.Version() == ver {
			return m, nil
		}
	}
	return nil, errMetricNotFound
}
