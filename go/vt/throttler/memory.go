package throttler

import (
	"sort"

	log "github.com/golang/glog"
)

// memory tracks "good" and "bad" throttler rates where good rates are below
// the system capacity and bad are above it.
//
// It is based on the fact that the MySQL performance degrades with an
// increasing number of rows. Therefore, the implementation stores only one
// bad rate which will get lower over time and turn known good rates into
// bad ones.
//
// To simplify tracking all possible rates, they are slotted into buckets
// e.g. of the size 10.
type memory struct {
	bucketSize int

	good []int64
	bad  int64
}

func newMemory(bucketSize int) *memory {
	if bucketSize == 0 {
		bucketSize = 1
	}
	return &memory{
		bucketSize: bucketSize,
		good:       make([]int64, 0),
	}
}

// int64Slice is used to sort int64 slices.
type int64Slice []int64

func (a int64Slice) Len() int           { return len(a) }
func (a int64Slice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a int64Slice) Less(i, j int) bool { return a[i] < a[j] }

func SearchInt64s(a []int64, x int64) int {
	return sort.Search(len(a), func(i int) bool { return a[i] >= x })
}

func (m *memory) markGood(rate int64) {
	rate = m.roundDown(rate)

	if lowestBad := m.lowestBad(); lowestBad != 0 && rate > lowestBad {
		log.Warningf("markGood(): ignoring higher good rate of %v because we assume that the known maximum capacity (currently at %v) can only degrade.", rate, lowestBad)
		return
	}

	// Skip rates which already exist.
	i := SearchInt64s(m.good, rate)
	if i < len(m.good) && m.good[i] == rate {
		return
	}

	m.good = append(m.good, rate)
	sort.Sort(int64Slice(m.good))
}

func (m *memory) markBad(rate int64) {
	rate = m.roundDown(rate)

	// Ignore higher bad rates than the current one.
	if m.bad != 0 && rate >= m.bad {
		return
	}

	// Ignore bad rates which are too drastic. This prevents that temporary
	// hiccups e.g. during a reparent, are stored in the memory.
	// TODO(mberlin): Remove this once we let bad values expire over time.
	highestGood := m.highestGood()
	if rate < highestGood {
		decrease := float64(highestGood) - float64(rate)
		degradation := decrease / float64(highestGood)
		if degradation > 0.1 {
			log.Warningf("markBad(): ignoring lower bad rate of %v because such a high degradation (%.1f%%) is unlikely (current highest good: %v)", rate, degradation*100, highestGood)
			return
		}
	}

	// Delete all good values which turned bad.
	goodLength := len(m.good)
	for i := goodLength - 1; i >= 0; i-- {
		goodRate := m.good[i]
		if goodRate >= rate {
			goodLength = i
		} else {
			break
		}
	}
	m.good = m.good[:goodLength]

	m.bad = rate
}

func (m *memory) highestGood() int64 {
	if len(m.good) == 0 {
		return 0
	}

	return m.good[len(m.good)-1]
}

func (m *memory) lowestBad() int64 {
	return m.bad
}

func (m *memory) roundDown(rate int64) int64 {
	return rate / int64(m.bucketSize) * int64(m.bucketSize)
}
