/*
Copyright 2017 Google Inc.

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

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	// If there are no labels, we will store the value under this key.
	UnlabeledValueKey = "unlabeled"
)

type Counter interface {
	Metric

	InitializeCounterLabelValues(values ...string) Counter

	Add(value int64, labelValues ...string)

	Counts() map[string]int64
}

// NewCountersWithSingleLabel create a new Counters instance.
// If name is set, the variable gets published.
// The function also accepts an optional list of tags that pre-creates them
// initialized to 0.
// label is a category name used to organize the tags. It is currently only
// used by Prometheus, but not by the expvar package.
func NewCounter(name, help string, labels ...string) Counter {
	c := &StoredInt{
		counts: make(map[string]*int64),
		metric: metric{
			help:   help,
			kind:   MetricKind_Counter,
			labels: labels,
		},
	}
	if name != "" {
		publish(name, c)
	}
	return c
}

// counters is similar to expvar.Map, except that it doesn't allow floats.
// It is used to build CountersWithSingleLabel and GaugesWithSingleLabel.
type StoredInt struct {
	metric

	// mu only protects adding and retrieving the value (*int64) from the
	// map.
	// The modification to the actual number (int64) must be done with
	// atomic funcs.
	// If a value for a given name already exists in the map, we only have
	// to use a read-lock to retrieve it. This is an important performance
	// optimizations because it allows to concurrently increment a counter.
	mu     sync.RWMutex
	counts map[string]*int64
	help   string
}

// String implements the expvar.Var interface.
func (c *StoredInt) String() string {
	if len(c.labels) == 0 {
		a := c.getValueAddr(UnlabeledValueKey)
		return strconv.FormatInt(atomic.LoadInt64(a), 10)
	}

	b := bytes.NewBuffer(make([]byte, 0, 4096))

	c.mu.RLock()
	defer c.mu.RUnlock()

	fmt.Fprintf(b, "{")
	firstValue := true
	for k, a := range c.counts {
		if firstValue {
			firstValue = false
		} else {
			fmt.Fprintf(b, ", ")
		}
		fmt.Fprintf(b, "%q: %v", k, atomic.LoadInt64(a))
	}
	fmt.Fprintf(b, "}")
	return b.String()
}

func (c *StoredInt) getValueAddr(name string) *int64 {
	c.mu.RLock()
	a, ok := c.counts[name]
	c.mu.RUnlock()

	if ok {
		return a
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	// we need to check the existence again
	// as it may be created by other goroutine.
	a, ok = c.counts[name]
	if ok {
		return a
	}
	a = new(int64)
	c.counts[name] = a
	return a
}

// Add adds a value to a named counter.
func (c *StoredInt) Add(value int64, labels ...string) {
	if len(labels) != len(c.labels) {
		panic("wrong number of values in Add")
	}

	if c.kind == MetricKind_Counter && value < 0 {
		logCounterNegative.Warningf("Adding a negative value to a counter, %v should be a gauge instead", c)
	}

	a := c.getValueAddr(mapKey(labels))
	atomic.AddInt64(a, value)
}

// Add adds a value to a named counter.
func (c *StoredInt) Set(value int64, labels ...string) {
	if len(labels) != len(c.labels) {
		panic("wrong number of values in Add")
	}

	if c.kind == MetricKind_Counter && value < 0 {
		logCounterNegative.Warningf("Adding a negative value to a counter, %v should be a gauge instead", c)
	}

	a := c.getValueAddr(mapKey(labels))
	atomic.StoreInt64(a, value)
}

// ResetAll resets all values.
func (c *StoredInt) ResetAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts = make(map[string]*int64)
}

// Reset resets a specific value to 0.
func (c *StoredInt) Reset(labels ...string) {
	a := c.getValueAddr(mapKey(labels))
	atomic.StoreInt64(a, int64(0))
}

// Counts returns a copy of the Counters' map.
func (c *StoredInt) Counts() map[string]int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	counts := make(map[string]int64, len(c.counts))
	for k, a := range c.counts {
		counts[k] = atomic.LoadInt64(a)
	}
	return counts
}

func (c *StoredInt) initializeLabelValues(values []string) *StoredInt {
	if len(c.labels) != 1 {
		panic(fmt.Sprintf("InitializeLabelValues() only works for labels with a single label. labels = %#v", c.labels))
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.counts) != 0 {
		panic(fmt.Sprintf("InitializeLabelValues() cannot init a metric which is already in use. existing data: %#v", c.counts))
	}

	for _, v := range values {
		c.counts[v] = new(int64)
	}
	return c
}

func (c *StoredInt) InitializeCounterLabelValues(values ...string) Counter {
	return c.initializeLabelValues(values)
}

//
//// String implements the expvar.Var interface.
//func (c CountersFuncWithMultiLabels) String() string {
//	m := c.f()
//	if m == nil {
//		return "{}"
//	}
//	b := bytes.NewBuffer(make([]byte, 0, 4096))
//	fmt.Fprintf(b, "{")
//	firstValue := true
//	for k, v := range m {
//		if firstValue {
//			firstValue = false
//		} else {
//			fmt.Fprintf(b, ", ")
//		}
//		fmt.Fprintf(b, "%q: %v", k, v)
//	}
//	fmt.Fprintf(b, "}")
//	return b.String()
//}

var escaper = strings.NewReplacer(".", "\\.", "\\", "\\\\")

func mapKey(ss []string) string {
	if len(ss) == 0 {
		return UnlabeledValueKey
	}

	esc := make([]string, len(ss))
	for i, f := range ss {
		esc[i] = escaper.Replace(f)
	}
	return strings.Join(esc, ".")
}
