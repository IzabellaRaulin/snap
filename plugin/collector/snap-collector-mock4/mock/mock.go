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

package mock

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/intelsdi-x/snap/control/plugin"
	"github.com/intelsdi-x/snap/control/plugin/cpolicy"
	"github.com/intelsdi-x/snap/core"
	"github.com/intelsdi-x/snap-plugin-utilities/config"
	"strconv"
)

const (
	// Name of plugin
	Name = "mock"
	// Version of plugin
	Version = 4
	// Type of plugin
	Type = plugin.CollectorPluginType
)

// Mock collector implementation used for testing
type Mock struct {
}

// CollectMetrics collects metrics for testing
func (f *Mock) CollectMetrics(mts []plugin.PluginMetricType) ([]plugin.PluginMetricType, error) {
	fmt.Fprintf(os.Stderr, "CollectMetrics()")

	items, err := config.GetConfigItems(mts[0], []string{"host", "port"})
	if err != nil {
		panic(err)
	}

	host := items["host"].(string)
	port := items["port"].(int)
	url := "http://"+host+":"+strconv.Itoa(port)

	fmt.Fprintf(os.Stderr, "Host=%+v", host)
	fmt.Fprintf(os.Stderr, "Port=%+v", port)
	fmt.Fprintf(os.Stderr, "URL=%+v", url)



	// the rest of code
	metrics := []plugin.PluginMetricType{}
	for i := range mts {

		if mts[i].Namespace()[2] == "*" {
			for j := 0; j < 10; j++ {
				v := fmt.Sprintf("host%d", j)
				data := randInt(65, 90)
				mt := plugin.PluginMetricType{
					Data_:      data,
					Namespace_: []string{"intel", "mock", v, "baz"},
					Source_:    host,
					Timestamp_: time.Now(),
					Labels_:    mts[i].Labels(),
					Version_:   mts[i].Version(),
				}
				metrics = append(metrics, mt)
			}
		} else {
			data := randInt(65, 90)
			mts[i].Data_ = data
			mts[i].Source_ = host
			mts[i].Timestamp_ = time.Now()
			metrics = append(metrics, mts[i])
		}
	}
	return metrics, nil
}

//GetMetricTypes returns metric types for testing
func (f *Mock) GetMetricTypes(cfg plugin.PluginConfigType) ([]plugin.PluginMetricType, error) {
	fmt.Fprintf(os.Stderr, "GetMetricTypes()\n")
	items, err := config.GetConfigItems(cfg, []string{"host", "port"})
	fmt.Fprintf(os.Stderr, "err_get_config=%+v\n", err)
	if err != nil {
		panic(err)
	}

	host := items["host"].(string)
	port := items["port"].(int)
	url := "http://"+host+":"+strconv.Itoa(port)

	fmt.Fprintf(os.Stderr, "Host=%+v\n", host)
	fmt.Fprintf(os.Stderr, "Port=%+v\n", port)
	fmt.Fprintf(os.Stderr, "URL=%+v\n", url)

	// the rest of code
	mts := []plugin.PluginMetricType{}
	mts = append(mts, plugin.PluginMetricType{Namespace_: []string{"intel", "mock", "foo"}})
	mts = append(mts, plugin.PluginMetricType{Namespace_: []string{"intel", "mock", "bar"}})
	mts = append(mts, plugin.PluginMetricType{
		Namespace_: []string{"intel", "mock", "*", "baz"},
		Labels_:    []core.Label{{Index: 2, Name: "host"}},
	})
	return mts, nil
}

//GetConfigPolicy returns a ConfigPolicy for testing
func (f *Mock) GetConfigPolicy() (*cpolicy.ConfigPolicy, error) {
	return cpolicy.New(), nil
}

//Meta returns meta data for testing
func Meta() *plugin.PluginMeta {
	return plugin.NewPluginMeta(
		Name,
		Version,
		Type,
		[]string{plugin.SnapGOBContentType},
		[]string{plugin.SnapGOBContentType},
		plugin.CacheTTL(100*time.Millisecond),
		plugin.RoutingStrategy(plugin.StickyRouting),
	)
}

//Random number generator
func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}
