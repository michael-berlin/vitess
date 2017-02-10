package txthrottler

import (
	"fmt"
	"sync"
	"time"

	log "github.com/golang/glog"

	"github.com/golang/protobuf/proto"

	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletenv"
	"github.com/youtube/vitess/go/vt/throttler"
	"github.com/youtube/vitess/go/vt/topo"

	throttlerdatapb "github.com/youtube/vitess/go/vt/proto/throttlerdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// TxThrottler throttles transactions based on replication lag.
// It's a thin wrapper around the throttler found in vitess/go/vt/throttler.
// It uses a discovery.HealthCheck to send replication-lag updates to the wrapped throttler.
//
// Intended Usage:
//   t := CreateTxThrottlerFromTabletConfig()
//
//   // A transaction throttler must be opened before its first use:
//   if err := t.Open(keyspace, shard); err != nil {
//     return err
//   }
//
//   // Checking whether to throttle can be done as follows before starting a transaction.
//   if t.Throttle() {
//     return fmt.Errorf("Transaction throttled!")
//   } else {
//     // execute transaction.
//   }
//
//   // To release the resources used by the throttler the caller should call Close().
//   t.Close()
//
// A TxThrottler object is generally not thread-safe: at any given time at most one goroutine should
// be executing a method. The only exception is the 'Throttle' method where multiple goroutines are
// allowed to execute it concurrently.
type TxThrottler struct {
	// config stores the transaction throttler's configuration.
	// It is populated in NewTxThrottler and is not modified
	// since.
	config *txThrottlerConfig

	// state holds an open transaction throttler state. It is nil
	// if the TransactionThrottler is closed.
	state *txThrottlerState
}

// CreateTxThrottlerFromTabletConfig tries to construct a TxThrottler from the
// relevant fields in the global tabletserver.Config object. It returns a disabled TxThrottler if
// any error occurs.
// This function calls tryCreateTxThrottler that does the actual creation work
// and returns an error if one occurred.
// TODO(erez): This function should return an error instead of returning a disabled
// transaction throttler. Fix this after NewTabletServer is changed to return an error as well.
func CreateTxThrottlerFromTabletConfig() *TxThrottler {
	txThrottler, err := tryCreateTxThrottler()
	if err != nil {
		log.Errorf("Error creating transaction throttler. Transaction throttling will"+
			" be disabled. Error: %v", err)
		config := &txThrottlerConfig{
			enabled: false,
		}
		txThrottler, err = newTxThrottler(config)
		if err != nil {
			panic("BUG: Can't create a disabled transaction throttler")
		}
	} else {
		log.Infof("Initialized transaction throttler with config: %+v", txThrottler.config)
	}
	return txThrottler
}

func tryCreateTxThrottler() (*TxThrottler, error) {
	config := &txThrottlerConfig{
		enabled: tabletenv.Config.EnableTxThrottler,
	}
	if !tabletenv.Config.EnableTxThrottler {
		return newTxThrottler(config)
	}
	var throttlerConfig throttlerdatapb.Configuration

	if err := proto.UnmarshalText(tabletenv.Config.TxThrottlerConfig, &throttlerConfig); err != nil {
		return nil, err
	}

	// Clone tsv.TxThrottlerHealthCheckCells so that we don't assume tsv.TxThrottlerHealthCheckCells
	// is immutable.
	config.healthCheckCells = make([]string, len(tabletenv.Config.TxThrottlerHealthCheckCells))
	copy(config.healthCheckCells, tabletenv.Config.TxThrottlerHealthCheckCells)
	config.throttlerConfig = &throttlerConfig
	return newTxThrottler(config)
}

// txThrottlerConfig holds the parameters that need to be
// passed when constructing a TxThrottler object.
type txThrottlerConfig struct {
	// enabled is true if the transaction throttler is enabled. All methods
	// of a disabled transaction throttler do nothing and Throttle() always
	// returns false.
	enabled bool

	throttlerConfig *throttlerdatapb.Configuration
	// healthCheckCells stores the cell names in which running vttablets will be monitored for
	// replication lag.
	healthCheckCells []string
}

// txThrottlerState holds the state of an open TxThrottler object.
type txThrottlerState struct {
	// throttleMu serializes calls to throttler.Throttler.Throttle(threadId).
	// That method is required to be called in serial for each threadId.
	throttleMu sync.Mutex
	throttler  ThrottlerInterface

	topoServer       topo.Server
	healthCheck      discovery.HealthCheck
	topologyWatchers []TopologyWatcherInterface
}

// These vars store the functions used to create the topo server, healthcheck
// topology watchers and go/vt/throttler. These are provided here so that they can be overridden
// in tests to generate mocks.
type topoServerFactoryFunc func() topo.Server
type healthCheckFactoryFunc func() discovery.HealthCheck
type topologyWatcherFactoryFunc func(topoServer topo.Server, tr discovery.TabletRecorder, cell, keyspace, shard string, refreshInterval time.Duration, topoReadConcurrency int) TopologyWatcherInterface
type vtThrottlerFactoryFunc func(name, unit string, threadCount int, maxRate, maxReplicationLag int64) (ThrottlerInterface, error)

var (
	topoServerFactory      topoServerFactoryFunc
	healthCheckFactory     healthCheckFactoryFunc
	topologyWatcherFactory topologyWatcherFactoryFunc
	vtThrottlerFactory     vtThrottlerFactoryFunc
)

func init() {
	resetTxThrottlerFactories()
}

func resetTxThrottlerFactories() {
	topoServerFactory = topo.Open
	healthCheckFactory = discovery.NewDefaultHealthCheck
	topologyWatcherFactory = func(topoServer topo.Server, tr discovery.TabletRecorder, cell, keyspace, shard string, refreshInterval time.Duration, topoReadConcurrency int) TopologyWatcherInterface {
		return discovery.NewShardReplicationWatcher(
			topoServer, tr, cell, keyspace, shard, refreshInterval, topoReadConcurrency)
	}
	vtThrottlerFactory = func(name, unit string, threadCount int, maxRate, maxReplicationLag int64) (ThrottlerInterface, error) {
		return throttler.NewThrottler(name, unit, threadCount, maxRate, maxReplicationLag)
	}
}

// TxThrottlerName is the name the wrapped go/vt/throttler object will be registered
// with go/vt/throttler.GlobalManager.
const TxThrottlerName = "TransactionThrottler"

func newTxThrottler(config *txThrottlerConfig) (*TxThrottler, error) {
	if config.enabled {
		// Verify config.
		err := throttler.MaxReplicationLagModuleConfig{Configuration: *config.throttlerConfig}.Verify()
		if err != nil {
			return nil, err
		}
		if len(config.healthCheckCells) == 0 {
			return nil, fmt.Errorf("Empty healthCheckCells given. %+v", config)
		}
	}
	return &TxThrottler{
		config: config,
	}, nil
}

// Open opens the transaction throttler. It must be called prior to 'Throttle'.
func (t *TxThrottler) Open(keyspace, shard string) error {
	if !t.config.enabled {
		return nil
	}
	if t.state != nil {
		return fmt.Errorf("Transaction throttler already opened")
	}
	var err error
	t.state, err = newTxThrottlerState(t.config, keyspace, shard)
	return err
}

// Close closes the TxThrottler object and releases resources.
// It should be called after the throttler is no longer needed.
// It's ok to call this method on a closed throttler--in which case the method does nothing.
func (t *TxThrottler) Close() {
	if !t.config.enabled {
		return
	}
	if t.state == nil {
		return
	}
	log.Infof("Shutting down transaction throttler.")
	t.state.deallocateResources()
	t.state = nil
}

// Throttle should be called before a new transaction is started.
// It returns true if the transaction should not proceed (the caller
// should back off). Throttle requires that Open() was previously called
// successfuly.
func (t *TxThrottler) Throttle() (result bool) {
	if !t.config.enabled {
		return false
	}
	if t.state == nil {
		panic("BUG: Throttle() called on a closed TxThrottler")
	}
	return t.state.throttle()
}

func newTxThrottlerState(config *txThrottlerConfig, keyspace, shard string,
) (*txThrottlerState, error) {
	t, err := vtThrottlerFactory(
		TxThrottlerName,
		"TPS", /* unit */
		1,     /* threadCount */
		throttler.MaxRateModuleDisabled, /* maxRate */
		config.throttlerConfig.MaxReplicationLagSec /* maxReplicationLag */)
	if err != nil {
		return nil, err
	}
	if t.UpdateConfiguration(config.throttlerConfig, true /* copyZeroValues */); err != nil {
		t.Close()
		return nil, err
	}
	result := &txThrottlerState{
		throttler: t,
	}
	result.topoServer = topoServerFactory()
	result.healthCheck = healthCheckFactory()
	result.healthCheck.SetListener(result, false /* sendDownEvents */)
	result.topologyWatchers = make(
		[]TopologyWatcherInterface, 0, len(config.healthCheckCells))
	for _, cell := range config.healthCheckCells {
		result.topologyWatchers = append(
			result.topologyWatchers,
			topologyWatcherFactory(
				result.topoServer,
				result.healthCheck, /* TabletRecorder */
				cell,
				keyspace,
				shard,
				discovery.DefaultTopologyWatcherRefreshInterval,
				discovery.DefaultTopoReadConcurrency))
	}
	return result, nil
}

func (ts *txThrottlerState) throttle() bool {
	if ts.throttler == nil {
		panic("BUG: throttle called after deallocateResources was called.")
	}
	// Serialize calls to ts.throttle.Throttle()
	ts.throttleMu.Lock()
	defer ts.throttleMu.Unlock()
	return ts.throttler.Throttle(0 /* threadId */) > 0
}

func (ts *txThrottlerState) deallocateResources() {
	// We don't really need to nil out the fields here
	// as deallocateResources is not expected to be called
	// more than once, but it doesn't hurt to do so.
	for _, watcher := range ts.topologyWatchers {
		watcher.Stop()
	}
	ts.topologyWatchers = nil

	ts.healthCheck.Close()
	ts.healthCheck = nil

	ts.topoServer.Close()

	// After ts.healthCheck is closed txThrottlerState.StatsUpdate() is guaranteed not
	// to be executing, so we can safely close the throttler.
	ts.throttler.Close()
	ts.throttler = nil
}

// StatsUpdate is part of the HealthCheckStatsListener interface.
func (ts *txThrottlerState) StatsUpdate(tabletStats *discovery.TabletStats) {
	// Ignore MASTER and RDONLY stats.
	// We currently do not monitor RDONLY tablets for replication lag. RDONLY tablets are not
	// candidates for becoming master during failover, and it's acceptable to serve somewhat
	// stale date from these.
	// TODO(erez): If this becomes necessary, we can add a configuration option that would
	// determine whether we consider RDONLY tablets here, as well.
	if tabletStats.Target.TabletType != topodatapb.TabletType_REPLICA {
		return
	}
	ts.throttler.RecordReplicationLag(time.Now(), tabletStats)
}

// The following interfaces are only defined to allow the test to mock them out.
// Do NOT use them outside of this package.

// ThrottlerInterface is a subset of the public API of throttler.Throttler.
//
// NOTE: The mockgen command below fails with the error:
// "Failed to format generated source code: 42:69: expected 'IDENT', found '{' (and 2 more errors)"
//
// You need to patch https://github.com/golang/mock/pull/45 into your mockgen
// to fix it. See https://github.com/golang/mock/issues/46.
//go:generate mockgen -destination mock_throttler_test.go -package txthrottler github.com/youtube/vitess/go/vt/tabletserver/txthrottler ThrottlerInterface
type ThrottlerInterface interface {
	Throttle(threadID int) time.Duration
	Close()
	RecordReplicationLag(time time.Time, ts *discovery.TabletStats)
	UpdateConfiguration(configuration *throttlerdatapb.Configuration, copyZeroValues bool) error
}

// TopologyWatcherInterface has the same API as discovery.TopologyWatcher.
//go:generate mockgen -destination mock_topology_watcher_test.go -package txthrottler github.com/youtube/vitess/go/vt/tabletserver/txthrottler TopologyWatcherInterface
type TopologyWatcherInterface interface {
	WaitForInitialTopology() error
	Stop()
}

// Generate mocks for Topology and HealthCheck from existing interfaces.
//go:generate mockgen -destination mock_healthcheck_test.go -package txthrottler github.com/youtube/vitess/go/vt/discovery HealthCheck
//go:generate mockgen -destination mock_topo_impl_test.go -package txthrottler github.com/youtube/vitess/go/vt/topo Impl
// TODO(mberlin): Remove the next line once we use the Go 1.7 package "context" everywhere.
//go:generate sed -i s,github.com/youtube/vitess/vendor/,,g mock_topo_impl_test.go
