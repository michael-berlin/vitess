package throttler

import (
	"fmt"
	"time"
)

// intervalHistory is a more strict history container which resembles a
// time series. We expect that there is a data point after every interval e.g.
// after every second. If not, we'll assume that the value is 0 for this
// interval (which affects the average value).
// It is up to the programmer to ensure that an interval is covered by one
// observation at most.
type intervalHistory struct {
	historyImpl
	interval          time.Duration
	nextIntervalStart time.Time
}

func newIntervalHistory(capacity int64, interval time.Duration) *intervalHistory {
	return &intervalHistory{
		historyImpl: *newHistoryImpl(capacity),
		interval:    interval,
	}
}

func (h *intervalHistory) add(record record) {
	if record.time.Before(h.nextIntervalStart) {
		panic(fmt.Sprintf("cannot add observation because it is already covered by a previous entry. record: %v next expected interval start: %v", record, h.nextIntervalStart))
	}
	h.historyImpl.add(record)
	h.nextIntervalStart = record.time.Add(h.interval)
}

// average returns the average value across all observations which span
// the range [from, to).
// Partially included observations are accounted by their included fraction.
// Missing observations are assumed with the value zero.
func (h *intervalHistory) average(from, to time.Time) float64 {
	// Search only entries whose time of observation is in [start, end).
	// Example: [from, to) = [1.5s, 2.5s) => [start, end) = [1s, 2s)
	start := from.Truncate(h.interval)
	end := to.Truncate(h.interval)

	sum := 0.0
	count := 0.0
	var nextIntervalStart time.Time
	for i := len(h.records) - 1; i >= 0; i-- {
		t := h.records[i].time

		if t.After(end) {
			continue
		}
		if t.Before(start) {
			break
		}

		// Account for intervals which were not recorded.
		if !nextIntervalStart.IsZero() {
			uncoveredRange := nextIntervalStart.Sub(t)
			count += float64(uncoveredRange / h.interval)
		}

		// If an interval is only partially included, count only that fraction.
		durationAfterTo := t.Add(h.interval).Sub(to)
		if durationAfterTo < 0 {
			durationAfterTo = 0
		}
		durationBeforeFrom := from.Sub(t)
		if durationBeforeFrom < 0 {
			durationBeforeFrom = 0
		}
		weight := float64((h.interval - durationBeforeFrom - durationAfterTo).Nanoseconds()) / float64(h.interval.Nanoseconds())

		sum += weight * float64(h.records[i].value)
		//		fmt.Println("adding value:", weight*float64(h.records[i].value), "weight", weight, "at:", t)
		count += weight
		nextIntervalStart = t.Add(-1 * h.interval)
	}

	//	result := float64(sum) / count
	//	fmt.Println("average:", result, "from:", from, "to:", to)
	return float64(sum) / count
}
