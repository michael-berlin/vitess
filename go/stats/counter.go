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

import (
	"strconv"
	"time"

	"vitess.io/vitess/go/vt/logutil"
)

// logCounterNegative is for throttling adding a negative value to a counter messages in logs
var logCounterNegative = logutil.NewThrottledLogger("StatsCounterNegative", 1*time.Minute)

// CounterFunc allows to provide the counter value via a custom function.
// For implementations that differentiate between Counters/Gauges,
// CounterFunc's values only go up (or are reset to 0).
type CounterFunc struct {
	F    func() int64
	help string
}

// NewCounterFunc creates a new CounterFunc instance and publishes it if name is
// set.
func NewCounterFunc(name string, help string, f func() int64) *CounterFunc {
	c := &CounterFunc{
		F:    f,
		help: help,
	}

	if name != "" {
		publish(name, c)
	}
	return c
}

// Help returns the help string.
func (cf CounterFunc) Help() string {
	return cf.help
}

// String implements expvar.Var.
func (cf CounterFunc) String() string {
	return strconv.FormatInt(cf.F(), 10)
}
