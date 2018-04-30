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

import "testing"

func TestGaugeTypeSwitch(t *testing.T) {
	v := NewGauge("", "help", "label1")
	switch v := v.(type) {
	case *StoredInt:
		if got, want := MetricKind_Gauge, v.Kind(); got != want {
			t.Errorf("wrong Kind for gauge. got = %v, want = %v", got, want)
		}
	default:
		t.Errorf("Gauge has wrong type: %T", v)
	}
}
