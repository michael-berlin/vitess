package throttler

import (
	"fmt"
	"math"
	"time"
)

// history is a bounded list of observed values over time.
// Unlike a time series, it doesn't bucket the observations into intervals
// and instead preserves all records to retain as much information as possible.
// In general, the history should reflect only a short period of time (on the
// order of minutes). It is used by the MaxReplicationLagModule.
type history interface {
	add(record record)

	dataCollection
}

type aggregatedHistory interface {
	addPerThread(threadID int, record record)

	dataCollection
}

type dataCollection interface {
	// average returns the average value of all observed values within the time
	// range from "from" to "to".
	average(from, to time.Time) float64

	before(time time.Time) record

	first() record
}

type historyImpl struct {
	records []record
}

// record is a single observation.
type record struct {
	// time is the time at which replicationLag was reported.
	time time.Time
	// value is the value of interest at the given time e.g. the replication lag
	// in seconds or the number of transactions per seconds.
	value int64
}

func (r *record) isZero() bool {
	return r.value == 0 && r.time.IsZero()
}

func newHistoryImpl(capacity int64) *historyImpl {
	return &historyImpl{
		records: make([]record, 0, capacity),
	}
}

func (h *historyImpl) add(record record) {
	// TODO(mberlin): Bound the list.
	h.records = append(h.records, record)
}

// average calculates the average value of all entries in the range [from, to].
// Note that it may return NaN if there aren't any observations in that range.
func (h *historyImpl) average(from, to time.Time) float64 {
	sum := int64(0)
	count := 0.0
	for i := len(h.records) - 1; i >= 0; i-- {
		t := h.records[i].time
		if (t.After(from) || t.Equal(from)) && (t.Before(to) || t.Equal(to)) {
			sum += h.records[i].value
			count++
		}
	}

	result := float64(sum) / count
	fmt.Println("average:", result, sum, count, "from:", from, "to:", to)
	if math.IsNaN(result) {
		for i, v := range h.records {
			fmt.Println(i, "record value:", v.value, "time:", v.time)
		}
	}
	return result
}

func (h *historyImpl) before(time time.Time) record {
	for i := len(h.records) - 1; i >= 0; i-- {
		if h.records[i].time.Before(time) || h.records[i].time.Equal(time) {
			return h.records[i]
		}
	}
	return record{}
}

func (h *historyImpl) first() record {
	if len(h.records) > 0 {
		return h.records[0]
	}
	return record{}
}
