package throttler

import "time"

type aggregatedIntervalHistory struct {
	threadCount      int
	historyPerThread []*intervalHistory
}

func newAggregatedIntervalHistory(capacity int64, interval time.Duration, threadCount int) *aggregatedIntervalHistory {
	historyPerThread := make([]*intervalHistory, threadCount)
	for i := 0; i < threadCount; i++ {
		historyPerThread[i] = newIntervalHistory(capacity, interval)
	}
	return &aggregatedIntervalHistory{
		threadCount:      threadCount,
		historyPerThread: historyPerThread,
	}
}

// addPerThread is similar to history.add() with the following differences:
// - If time is identical, values across multiple threads will be aggregated.
// - Aggregation only occurs when *all* (threadCount) threads added a value
//   at "time" or a more recent time.
// - Consequently, a thread must call this method for a given "time" value
//   at most once.
func (h *aggregatedIntervalHistory) addPerThread(threadID int, record record) {
	h.historyPerThread[threadID].add(record)
}

func (h *aggregatedIntervalHistory) average(from, to time.Time) float64 {
	sum := 0.0
	for i := 0; i < h.threadCount; i++ {
		sum += h.historyPerThread[i].average(from, to)
	}
	result := sum / float64(h.threadCount)
	//	fmt.Println("average:", result)
	return result
}

func (h *aggregatedIntervalHistory) before(time time.Time) record {
	sum := int64(0)
	for i := 0; i < h.threadCount; i++ {
		sum += h.historyPerThread[i].before(time).value
	}
	return record{h.historyPerThread[0].before(time).time, sum}
}

func (h *aggregatedIntervalHistory) first() record {
	for i := 0; i < h.threadCount; i++ {
		record := h.historyPerThread[i].first()
		if !record.isZero() {
			return record
		}
	}
	return record{}
}
