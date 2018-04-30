package prometheusbackend

import (
	"fmt"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"vitess.io/vitess/go/stats"
)

func kindToValueType(kind stats.MetricKind) prometheus.ValueType {
	switch kind {
	case stats.MetricKind_Counter:
		return prometheus.CounterValue
	case stats.MetricKind_Gauge:
		return prometheus.GaugeValue
	default:
		panic(fmt.Sprintf("unknown stats.MetricKind: %#v", kind))
	}
}

type metricFuncCollector struct {
	// f returns the floating point value of the metric.
	f    func() float64
	desc *prometheus.Desc
	vt   prometheus.ValueType
}

// Describe implements Collector.
func (mc *metricFuncCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- mc.desc
}

// Collect implements Collector.
func (mc *metricFuncCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(
		mc.desc,
		mc.vt,
		float64(mc.f()))
}

type storedIntCollector struct {
	metric *stats.StoredInt
	desc   *prometheus.Desc
}

// Describe implements Collector.
func (c *storedIntCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

// Collect implements Collector.
func (c *storedIntCollector) Collect(ch chan<- prometheus.Metric) {
	for joinedLabelValues, value := range c.metric.Counts() {
		fmt.Println(joinedLabelValues, value)
		var labelValues []string
		if joinedLabelValues != stats.UnlabeledValueKey {
			labelValues = strings.Split(joinedLabelValues, ".")
		}
		ch <- prometheus.MustNewConstMetric(
			c.desc,
			kindToValueType(c.metric.Kind()),
			float64(value),
			labelValues...)
	}
}

type timingsCollector struct {
	t    *stats.Timings
	desc *prometheus.Desc
}

// Describe implements Collector.
func (c *timingsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

// Collect implements Collector.
func (c *timingsCollector) Collect(ch chan<- prometheus.Metric) {
	for cat, his := range c.t.Histograms() {
		ch <- prometheus.MustNewConstHistogram(
			c.desc,
			uint64(his.Count()),
			float64(his.Total()),
			makePromBucket(his.Cutoffs(), his.Buckets()),
			cat)
	}
}

func makePromBucket(cutoffs []int64, buckets []int64) map[float64]uint64 {
	output := make(map[float64]uint64)
	last := uint64(0)
	for i := range cutoffs {
		key := float64(cutoffs[i]) / 1000000000
		//TODO(zmagg): int64 => uint64 conversion. error if it overflows?
		output[key] = uint64(buckets[i]) + last
		last = output[key]
	}
	return output
}

type multiTimingsCollector struct {
	mt   *stats.MultiTimings
	desc *prometheus.Desc
}

// Describe implements Collector.
func (c *multiTimingsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

// Collect implements Collector.
func (c *multiTimingsCollector) Collect(ch chan<- prometheus.Metric) {
	for cat, his := range c.mt.Timings.Histograms() {
		labelValues := strings.Split(cat, ".")
		ch <- prometheus.MustNewConstHistogram(
			c.desc,
			uint64(his.Count()),
			float64(his.Total()),
			makePromBucket(his.Cutoffs(), his.Buckets()),
			labelValues...)
	}
}
