// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package throttler

import (
	"fmt"
	"net/http"
	"sort"
	"sync"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/stats"
)

// GlobalManager is the per-process manager which manages all active throttlers.
var GlobalManager = newManager()

func init() {
	http.Handle("/throttler", GlobalManager)
}

// Manager defines the public interface of the throttler manager. It is used
// for example by the different RPC implementations.
type Manager interface {
	// MaxRates returns the max rate of all known throttlers.
	MaxRates() map[string]int64

	// SetMaxRate sets the max rate on all known throttlers.
	SetMaxRate(rate int64) []string
}

// managerImpl controls multiple throttlers and also aggregates their
// statistics. It implements the "Manager" interface.
type managerImpl struct {
	// mu guards all fields in this group.
	mu sync.Mutex
	// throttlers tracks all running throttlers (by their name).
	throttlers map[string]*Throttler
	// ratesCount tracks per throttler name a statistics counter variable.
	// This counter has one entry, the cumulative counter 'ActualRate'.
	// Note that this Manager owns the stats objects and never returns them
	// because it is not possible to unpublish them. Instead, they may get
	// reused (and reset) if two throttlers have the same name (and hence the
	// same stats objects.) The manager will prevent that two throttlers
	// can be registered with the same name at the same time.
	ratesCount map[string]*stats.Counters
	// statsRates tracks per throttler name a statistics rates variable.
	// The Rates object uses the respective "ratesCount" object as input and
	// peridodically polls it to calculate the recent rate.
	// The same caveats mentioned for "ratesCount" apply to "statsRates" as well.
	statsRates map[string]*stats.Rates
}

func newManager() *managerImpl {
	return &managerImpl{
		throttlers: make(map[string]*Throttler),
	}
}

func

// registerThrottler returns the stats.Counters object which the throttler
// must use to keep track of the number of requests which were not throttled.
func (m *managerImpl) registerThrottler(name string, throttler *Throttler) (*stats.Counters, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.throttlers[name]; ok {
		return fmt.Errorf("registerThrottler(): throttler with name '%v' is already registered", name)
	}
	m.throttlers[name] = throttler

	if c, ok := m.statsCou[name]; ok {


	return nil
}

func (m *managerImpl) unregisterThrottler(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.throttlers[name]; !ok {
		log.Errorf("unregisterThrottler(): throttler with name '%v' is not registered", name)
		return
	}
	delete(m.throttlers, name)
}

// MaxRates returns the max rate of all known throttlers.
func (m *managerImpl) MaxRates() map[string]int64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	rates := make(map[string]int64, len(m.throttlers))
	for name, t := range m.throttlers {
		rates[name] = t.MaxRate()
	}
	return rates
}

// SetMaxRate sets the max rate on all known throttlers.
func (m *managerImpl) SetMaxRate(rate int64) []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	var names []string
	for name, t := range m.throttlers {
		t.SetMaxRate(rate)
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
