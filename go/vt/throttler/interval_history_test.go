package throttler

import (
	"strings"
	"testing"
	"time"
)

func TestIntervalHistory_AverageIncludesPartialIntervals(t *testing.T) {
	// average() should include intervals which aren't fully covered by from and
	// to at least partially.
	h := newIntervalHistory(10, 1*time.Second)

	h.add(record{sinceZero(0 * time.Second), 10000000})
	h.add(record{sinceZero(1 * time.Second), 1000})
	h.add(record{sinceZero(2 * time.Second), 2000})
	h.add(record{sinceZero(3 * time.Second), 10000000})
	// Rate within [1s, 2s) = 1000 and within [2s, 3s) = 2000 = average of 1500
	want := 1500.0
	if got := h.average(sinceZero(1500*time.Millisecond), sinceZero(2500*time.Millisecond)); got != want {
		t.Errorf("average(1.5s, 2.5s) = %v, want = %v", got, want)
	}
}

func TestIntervalHistory_AverageRangeSmallerThanInterval(t *testing.T) {
	h := newIntervalHistory(10, 1*time.Second)

	h.add(record{sinceZero(0 * time.Second), 10000})
	want := 10000.0
	if got := h.average(sinceZero(250*time.Millisecond), sinceZero(750*time.Millisecond)); got != want {
		t.Errorf("average(0.25s, 0.75s) = %v, want = %v", got, want)
	}
}

func TestIntervalHistory_GapsCountedAsZero(t *testing.T) {
	h := newIntervalHistory(10, 1*time.Second)

	h.add(record{sinceZero(0 * time.Second), 1000})
	h.add(record{sinceZero(3 * time.Second), 1000})

	want := 500.0
	if got := h.average(sinceZero(0*time.Second), sinceZero(4*time.Second)); got != want {
		t.Errorf("average(0s, 4s) = %v, want = %v", got, want)
	}
}

func TestIntervalHistory_addNoDuplicateInterval(t *testing.T) {
	defer func() {
		r := recover()

		if r == nil {
			t.Fatal("add() did not panic")
		}
		want := "bla"
		if strings.Contains(r.(string), want) {
			t.Fatalf("add() did panic for the wrong reason: got = %v, want = %v", r, want)
		}
	}()

	h := newIntervalHistory(10, 1*time.Second)

	h.add(record{sinceZero(0 * time.Second), 1000})
	h.add(record{sinceZero(100 * time.Millisecond), 1000})
}
