package throttler

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/youtube/vitess/go/sync2"

	log "github.com/golang/glog"
)

type state int

const (
	increaseRate state = iota
	decreaseAndGuessRate
	emergency
)

type replicationLagChange int

const (
	unknown replicationLagChange = iota
	less
	equal
	greater
)

// memoryGranularity is the granularity at which values are rounded down in the
// memory instance (see memory.go).
const memoryGranularity = 5

// MaxReplicationLagModule calculates the maximum rate based on observed
// replication lag and throttler rate changes.
// It implements the Module interface.
type MaxReplicationLagModule struct {
	// mu guards all fields in this group.
	mu     sync.Mutex
	config MaxReplicationLagModuleConfig

	// maxRate is the rate calculated for the throttler.
	maxRate      sync2.AtomicInt64
	currentState state
	// lastRateChange is the the when maxRate was adjusted the last time.
	lastRateChange       time.Time
	lastRateChangeReason string
	nextAllowedIncrease  time.Time
	nextAllowedDecrease  time.Time
	memory               *memory

	// rateUpdateChan is the notification channel to tell the throttler when our
	// max rate calculation has changed. The field is immutable (set in Start().)
	rateUpdateChan chan<- struct{}
	nowFunc        func() time.Time

	// records buffers the replication lag records received by the HealthCheck
	// listener. ProcessRecords() will process them.
	records chan record
	wg      sync.WaitGroup

	replicationLagHistory history
	actualRatesHistory    dataCollection
}

// NewMaxReplicationLagModule will create a new module instance and set the
// initial max replication lag limit to maxReplicationLag.
func NewMaxReplicationLagModule(config MaxReplicationLagModuleConfig, actualRatesHistory dataCollection, nowFunc func() time.Time) (*MaxReplicationLagModule, error) {
	if err := config.Verify(); err != nil {
		return nil, fmt.Errorf("invalid NewMaxReplicationLagModuleConfig: %v", err)
	}

	maxRate := int64(ReplicationLagModuleDisabled)
	if config.MaxReplicationLag != ReplicationLagModuleDisabled {
		maxRate = config.InitialRate
	}
	m := &MaxReplicationLagModule{
		config:                config,
		maxRate:               sync2.NewAtomicInt64(maxRate),
		memory:                newMemory(memoryGranularity),
		nowFunc:               nowFunc,
		records:               make(chan record, 10),
		replicationLagHistory: newHistoryImpl(10),
		actualRatesHistory:    actualRatesHistory,
	}
	now := nowFunc()
	m.lastRateChange = now
	m.updateNextAllowedIncrease(now, m.config.MaxIncrease)
	return m, nil
}

// Start launches a Go routine which reacts on replication lag records.
// It implements the Module interface.
func (m *MaxReplicationLagModule) Start(rateUpdateChan chan<- struct{}) {
	m.rateUpdateChan = rateUpdateChan
	m.wg.Add(1)
	go m.ProcessRecords()
}

// Stop blocks until the module's Go routine is stopped.
// It implements the Module interface.
func (m *MaxReplicationLagModule) Stop() {
	close(m.records)
	m.wg.Wait()
}

// MaxReplicationLag returns the current maximum allowed rate.
// It implements the Module interface.
func (m *MaxReplicationLagModule) MaxRate() int64 {
	return m.maxRate.Get()
}

func (m *MaxReplicationLagModule) RecordReplicationLag(lag int64, time time.Time) {
	if m.config.MaxReplicationLag == ReplicationLagModuleDisabled {
		return
	}

	// Buffer data point for now to unblock the HealthCheck listener and process
	// it asynchronously in ProcessRecords().
	m.records <- record{time, lag}
}

func (m *MaxReplicationLagModule) ProcessRecords() {
	defer m.wg.Done()

	for record := range m.records {
		m.replicationLagHistory.add(record)

		m.recalculateMaxRate()
	}
}

func (m *MaxReplicationLagModule) recalculateMaxRate() {
	now := m.nowFunc()

	lagRecordNow := m.replicationLagHistory.before(now)
	if lagRecordNow.isZero() {
		panic("maxRate recalculation was triggered but no replication lag observation is available in the history")
	}
	lagNow := lagRecordNow.value

	if lagNow > m.config.MaxReplicationLag {
		// Lag in range: (max, infinite]
		m.emergency(now, lagNow)
	} else if lagNow > m.config.TargetReplicationLag {
		// Lag in range: (target, max]
		m.decreaseAndGuessRate(now, lagRecordNow)
	} else {
		// Lag in range: [0, target]
		m.increaseRate(now)
	}
}

func (m *MaxReplicationLagModule) increaseRate(now time.Time) {
	// Any increase has to wait for a previous decrease first.
	if !m.nextAllowedDecrease.IsZero() && now.Before(m.nextAllowedDecrease) {
		return
	}
	// Increase rate again only if the last increase was in effect long enough.
	if !m.nextAllowedIncrease.IsZero() && now.Before(m.nextAllowedIncrease) {
		return
	}

	oldMaxRate := m.maxRate.Get()

	m.analyzeStateTransition(now, increaseRate, unknown)
	m.resetCurrentState(now)
	highestGood := m.memory.highestGood()
	previousRateSource := "highest known good rate"
	previousRate := float64(highestGood)
	if previousRate == 0.0 && !m.lastRateChange.IsZero() {
		// No known high good rate. Use the actual value instead.
		// (It might be lower because the system was slower or the throttler rate was
		// set by a different module and not us.)
		previousRateSource = "previous actual rate"
		previousRate = m.actualRatesHistory.average(m.lastRateChange, now)
	}
	if previousRate == 0.0 || math.IsNaN(previousRate) {
		// NaN (0.0/0.0) occurs when no observations were in the timespan.
		// Use the set rate in this case.
		previousRateSource = "previous set rate"
		previousRate = float64(oldMaxRate)
	}

	// Lag is within bounds. Keep increasing the rate
	// a) by doubling it if we're below config.InitialRate
	// b) by MaxIncrease at most otherwise,
	// but always halfway between [highestGood (or previousRate), lowestBad].
	var increaseReason string
	var maxRate float64
	initialRate := float64(m.config.InitialRate)
	if previousRate < initialRate {
		increaseReason = fmt.Sprintf("doubling the current rate (of %.f) (but always capping it at %.f%% of the initial rate of %.f)", previousRate, (1+m.config.MaxIncrease)*100, initialRate)
		doubleRate := previousRate * 2
		initialRatePlusIncrease := initialRate * (1 + m.config.MaxIncrease)
		if doubleRate <= initialRatePlusIncrease {
			maxRate = doubleRate
		} else {
			maxRate = initialRatePlusIncrease
		}
	} else {
		increaseReason = fmt.Sprintf("a max increase of %.1f%%", m.config.MaxIncrease*100)
		maxRate = previousRate * (1 + m.config.MaxIncrease)
	}
	// Due to an emergency the rate might be very low. Start at least 100 which is
	// safe because we're in the increaseRate state and the current lag is low.
	if maxRate < 100 {
		maxRate = 100
		increaseReason = "a minimum rate of 100"
	}
	// if MaxIncrease is too low, the rate might not increase. Bump it minimally
	// such that we make progress.
	if maxRate <= float64(oldMaxRate) {
		maxRate = float64(oldMaxRate) + memoryGranularity
		increaseReason += fmt.Sprintf(" (minimum progress by %v)", memoryGranularity)
		previousRate = float64(oldMaxRate)
		previousRateSource = "previous rate limit"
	}
	lowestBad := float64(m.memory.lowestBad())
	if lowestBad != 0 {
		if maxRate >= lowestBad {
			maxRate -= (maxRate - lowestBad) / 2
			increaseReason += fmt.Sprintf(" (but limited to the middle between [wanted rate, first known bad rate] of [%.0f, %.0f])", maxRate, lowestBad)
			if maxRate > lowestBad {
				// We might still be too high due to the doubling before. Always limit
				// it to the bad rate at most.
				maxRate = lowestBad
				increaseReason += fmt.Sprintf(" (but limited up to the first known bad rate (%.0f))", lowestBad)
			}
		}
	}

	increase := (maxRate - previousRate) / previousRate
	m.maxRate.Set(int64(maxRate))
	m.currentState = increaseRate
	m.lastRateChange = now
	m.updateNextAllowedIncrease(now, increase)
	m.lastRateChangeReason = fmt.Sprintf("periodic increase of the %v from %.0f to %.0f (by %.1f%%) based on %v to find out the maximum - next allowed increase in %.0f seconds",
		previousRateSource, previousRate, maxRate, increase*100, increaseReason, m.nextAllowedIncrease.Sub(now).Seconds())
	m.updateRate(oldMaxRate)
}

func (m *MaxReplicationLagModule) updateNextAllowedIncrease(now time.Time, increase float64) {
	minDuration := m.config.MinDurationBetweenChanges
	// We may have to wait longer than the configured minimum duration
	// until we see an effect of the increase.
	// Example: If the increase was fully over the capacity, it will take
	// 1 / increase seconds until the replication lag goes up by 1 second.
	// E.g.
	// (If the system was already at its maximum capacity (e.g. 1k QPS) and we
	// increase the rate by e.g. 5% to 1050 QPS, it will take 20 seconds until
	// 1000 extra queries are buffered and the lag increases by 1 second.)
	// On top of that, add 2 extra seconds to account for a delayed propagation
	// of the data (because the throttler takes over the updated rate only every
	// second and it publishes its rate history only after a second is over).
	minPropagationTime := time.Duration(1.0/increase+2) * time.Second
	if minPropagationTime > minDuration {
		minDuration = minPropagationTime
	}
	if minDuration > 61*time.Second {
		// Cap the rate to a reasonable amount of time (very small increases may
		// result into a 20 minutes wait otherwise.)
		minDuration = 61 * time.Second
	}
	m.nextAllowedIncrease = now.Add(minDuration)
}

func (m *MaxReplicationLagModule) decreaseAndGuessRate(now time.Time, lagRecordNow record) {
	// Decrease the rate only if the last decrease was in effect long enough.
	if !m.nextAllowedDecrease.IsZero() && now.Before(m.nextAllowedDecrease) {
		return
	}

	// Try to find a replication lag which is close to the last rate change.
	var lagRecordBefore record
	if m.lastRateChange.IsZero() {
		lagRecordBefore = m.replicationLagHistory.first()
	} else {
		lagRecordBefore = m.replicationLagHistory.before(m.lastRateChange)
	}
	if lagRecordBefore.isZero() {
		// No record before the current available. Not possible to calculate a diff.
		return
	}
	if lagRecordBefore.time == lagRecordNow.time {
		// First record is the same as the last record. Not possible to calculate a
		// diff. Wait for the next health stats update.
		return
	}

	// Analyze if the past rate was good or bad.
	lagBefore := lagRecordBefore.value
	lagNow := lagRecordNow.value
	replicationLagChange := less
	if lagNow == lagBefore {
		replicationLagChange = equal
	} else if lagNow > lagBefore {
		replicationLagChange = greater
	}
	m.analyzeStateTransition(now, decreaseAndGuessRate, replicationLagChange)
	m.resetCurrentState(now)

	// Find out the average rate (per second) at which we inserted data
	// at the master during the observed timespan.
	from := lagRecordBefore.time
	to := lagRecordNow.time
	avgMasterRate := m.actualRatesHistory.average(from, to)
	if math.IsNaN(avgMasterRate) {
		// NaN (0.0/0.0) occurs when no observations were in the timespan.
		// Wait for more rate observations.
		return
	}

	// Sanity check and correct the data points if necessary.
	d := lagRecordNow.time.Sub(lagRecordBefore.time)
	lagDifference := time.Duration(lagRecordNow.value-lagRecordBefore.value) * time.Second
	if lagDifference > d {
		log.Warningf("Replication lag increase is higher than the elapsed time: %v > %v. This should not happen. Replication Lag Data points: Before: %+v Now: %+v", lagDifference, d, lagRecordBefore, lagRecordNow)
		d = lagDifference
	}

	// Guess the slave capacity based on the replication lag change.
	avgSlaveRate := m.guessSlaveRate(avgMasterRate, lagBefore, lagNow, lagDifference, d)

	oldMaxRate := m.maxRate.Get()
	m.maxRate.Set(int64(avgSlaveRate))
	m.currentState = decreaseAndGuessRate
	m.lastRateChange = now
	m.nextAllowedDecrease = now.Add(m.config.MinDurationBetweenChanges + 2*time.Second)
	m.lastRateChangeReason = "reaction to replication lag change"
	m.updateRate(oldMaxRate)
}

func (m *MaxReplicationLagModule) guessSlaveRate(avgMasterRate float64, lagBefore, lagNow int64, lagDifference, d time.Duration) float64 {
	// avgSlaveRate is the average rate (per second) at which the slave
	// applied transactions from the replication stream. We infer the value
	// from the relative change in the replication lag.
	avgSlaveRate := avgMasterRate * (d - lagDifference).Seconds() / d.Seconds()
	log.Infof("d : %v lag diff: %v master: %v slave: %v", d, lagDifference, avgMasterRate, avgSlaveRate)

	oldRequestsBehind := 0.0
	// If the old lag was > 0s, the slave needs to catch up on that as well.
	if lagNow > lagBefore {
		oldRequestsBehind = avgSlaveRate * float64(lagBefore)
	}
	newRequestsBehind := 0.0
	// If the lag increased (i.e. slave rate was slower), the slave must make up
	// for the difference in the future.
	if avgSlaveRate < avgMasterRate {
		newRequestsBehind = (avgMasterRate - avgSlaveRate) * d.Seconds()
	}
	requestsBehind := oldRequestsBehind + newRequestsBehind
	log.Infof("old reqs: %v new reqs: %v total: %v", oldRequestsBehind, newRequestsBehind, requestsBehind)
	//	if requestsBehind > 0 {
	//		// It's okay if it's a little behind e.g. by half the max replication lag.
	//		allowedRequestsBehind := avgSlaveRate * (float64(m.config.MaxReplicationLag) / 2)
	//		requestsBehind -= allowedRequestsBehind
	//		if requestsBehind < 0 {
	//			requestsBehind = 0
	//		}
	//		log.Infof("allowed reqs: %v reqs: %v", allowedRequestsBehind, requestsBehind)
	//	}

	//	// Let's assume we guessed the maximum system capacity. Stay below it by
	//	// a safe margin.
	//	avgSlaveRate *= m.config.CapacityFactor
	//	log.Infof("slave after cap adj: %v", avgSlaveRate)

	// Reduce the new rate such that it has time to catch up the requests it's
	// behind within the next interval.
	futureRequests := avgSlaveRate * m.config.MinDurationBetweenChanges.Seconds()
	if futureRequests > 0 {
		avgSlaveRate *= (futureRequests - requestsBehind) / futureRequests
		if avgSlaveRate < 1 {
			// Backlog is too high. Reduce rate to 1 request/second.
			avgSlaveRate = 1.0
		}
		log.Infof("slave after future reqs adj: %v", avgSlaveRate)
	}

	return avgSlaveRate
}

func (m *MaxReplicationLagModule) emergency(now time.Time, lagNow int64) {
	m.analyzeStateTransition(now, emergency, unknown)
	m.resetCurrentState(now)
	oldMaxRate := m.maxRate.Get()

	m.maxRate.Set(2)
	m.currentState = emergency
	m.lastRateChange = now
	m.lastRateChangeReason = fmt.Sprintf("replication lag went beyond max: %v > %v", lagNow, m.config.MaxReplicationLag)
	m.updateRate(oldMaxRate)
}

func (m *MaxReplicationLagModule) updateRate(oldMaxRate int64) {
	log.Infof("updating rate: %v old rate: %v reason: %v", m.maxRate.Get(), oldMaxRate, m.lastRateChangeReason)
	if oldMaxRate != m.maxRate.Get() {
		// Notify the throttler that we updated our max rate.
		m.rateUpdateChan <- struct{}{}
	}
}

func (m *MaxReplicationLagModule) analyzeStateTransition(now time.Time, newState state, replicationLagChange replicationLagChange) {
	if m.lastRateChange.IsZero() {
		// Module was just started. We don't have any data points yet.
		return
	}

	// Use the actual rate instead of the set rate.
	// (It might be lower because the system was slower or the throttler rate was
	// set by a different module and not us.)
	rate := m.actualRatesHistory.average(m.lastRateChange, now)
	if math.IsNaN(rate) {
		// NaN (0.0/0.0) occurs when no observations were in the timespan.
		// Wait for more observations.
		return
	}

	var rateIsGood bool = false

	switch m.currentState {
	case increaseRate:
		switch newState {
		case increaseRate:
			rateIsGood = true
		case decreaseAndGuessRate:
			rateIsGood = false
		case emergency:
			rateIsGood = false
		}
	case decreaseAndGuessRate:
		switch newState {
		case increaseRate:
			rateIsGood = true
		case decreaseAndGuessRate:
			switch replicationLagChange {
			case unknown:
				return
			case less:
				rateIsGood = true
			case equal:
				// Replication lag kept constant. Impossible to judge if the rate is good or bad.
				return
			case greater:
				rateIsGood = false
			}
		case emergency:
			rateIsGood = false
		}
	case emergency:
		// Rate changes initiated during an "emergency" phase provide no meaningful data point.
		return
	}

	if rateIsGood {
		log.Infof("marking rate %.f as good state: %v", rate, m.currentState)
		m.memory.markGood(int64(rate))
	} else {
		log.Infof("marking rate %.f as bad state: %v", rate, m.currentState)
		m.memory.markBad(int64(rate))
	}
}

// resetCurrentState ensures that local state from a previous state does not
// carry over e.g. during a switch from 1) increase to 2) decrease to
// 3) increase the nextAllowed time for the 1) increase should not be seen by
// 3) increase.
func (m *MaxReplicationLagModule) resetCurrentState(now time.Time) {
	switch m.currentState {
	case increaseRate:
		m.nextAllowedIncrease = now
	case decreaseAndGuessRate:
		m.nextAllowedDecrease = now
	}
}
