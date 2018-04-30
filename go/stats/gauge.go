/*
Copyright 2018 The Vitess Authors

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

package stats

type Gauge interface {
	Metric

	InitializeGaugeLabelValues(values ...string) Gauge

	Set(value int64, labelValues ...string)

	Counts() map[string]int64
}

// NewGauge create a new Counters instance.
// If name is set, the variable gets published.
// The function also accepts an optional list of tags that pre-creates them
// initialized to 0.
// label is a category name used to organize the tags. It is currently only
// used by Prometheus, but not by the expvar package.
func NewGauge(name, help string, labels ...string) Gauge {
	c := &StoredInt{
		counts: make(map[string]*int64),
		metric: metric{
			help:   help,
			kind:   MetricKind_Gauge,
			labels: labels,
		},
	}
	if name != "" {
		publish(name, c)
	}
	return c
}

func (c *StoredInt) InitializeGaugeLabelValues(values ...string) Gauge {
	return c.initializeLabelValues(values)
}
