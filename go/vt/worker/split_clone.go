// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"errors"
	"fmt"
	"html/template"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/event"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/binlog/binlogplayer"
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/mysqlctl/tmutils"
	"github.com/youtube/vitess/go/vt/throttler"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/topotools"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"
	"github.com/youtube/vitess/go/vt/worker/events"
	"github.com/youtube/vitess/go/vt/wrangler"

	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// SplitCloneWorker will clone the data within a keyspace from a
// source set of shards to a destination set of shards.
type SplitCloneWorker struct {
	StatusWorker

	wr                      *wrangler.Wrangler
	cell                    string
	keyspace                string
	shard                   string
	online                  bool
	offline                 bool
	excludeTables           []string
	strategy                *splitStrategy
	sourceReaderCount       int
	writeQueryMaxRows       int
	writeQueryMaxSize       int
	minTableSizeForSplit    uint64
	destinationWriterCount  int
	minHealthyRdonlyTablets int
	maxTPS                  int64
	cleaner                 *wrangler.Cleaner
	tabletTracker           *TabletTracker

	// populated during WorkerStateInit, read-only after that
	keyspaceInfo      *topo.KeyspaceInfo
	sourceShards      []*topo.ShardInfo
	destinationShards []*topo.ShardInfo
	keyspaceSchema    *vindexes.KeyspaceSchema

	// populated during WorkerStateFindTargets, read-only after that
	sourceAliases []*topodatapb.TabletAlias
	sourceTablets []*topo.TabletInfo
	// healthCheck tracks the health of all MASTER and REPLICA tablets.
	// It must be closed at the end of the command.
	healthCheck discovery.HealthCheck
	// destinationShardWatchers contains a TopologyWatcher for each destination
	// shard. It updates the list of tablets in the healthcheck if replicas are
	// added/removed.
	// Each watcher must be stopped at the end of the command.
	destinationShardWatchers []*discovery.TopologyWatcher
	// destinationDbNames stores for each destination keyspace/shard the MySQL
	// database name.
	// Example Map Entry: test_keyspace/-80 => vt_test_keyspace
	destinationDbNames map[string]string
	// destionThrottlers stores for each destination keyspace/shard the
	// Throttler instance which will limit the write throughput.
	destinationThrottlers map[string]*throttler.Throttler

	// populated during WorkerStateCloneOffline
	// tableStatusList holds the status for each table.
	// TODO(mberlin): Have one per phase or reset it after the online phase.
	tableStatusList tableStatusList
	// aliases of tablets that need to have their state refreshed.
	// Only populated once, read-only after that.
	refreshAliases [][]*topodatapb.TabletAlias
	refreshTablets []map[topodatapb.TabletAlias]*topo.TabletInfo

	ev *events.SplitClone
}

// NewSplitCloneWorker returns a new SplitCloneWorker object.
func NewSplitCloneWorker(wr *wrangler.Wrangler, cell, keyspace, shard string, online, offline bool, excludeTables []string, strategyStr string, sourceReaderCount, writeQueryMaxRows, writeQueryMaxSize int, minTableSizeForSplit uint64, destinationWriterCount, minHealthyRdonlyTablets int, maxTPS int64) (Worker, error) {
	strategy, err := newSplitStrategy(wr.Logger(), strategyStr)
	if err != nil {
		return nil, err
	}
	if maxTPS != throttler.MaxRateModuleDisabled {
		wr.Logger().Infof("throttling enabled and set to a max of %v transactions/second", maxTPS)
	}
	if maxTPS != throttler.MaxRateModuleDisabled && maxTPS < int64(destinationWriterCount) {
		return nil, fmt.Errorf("-max_tps must be >= -destination_writer_count: %v >= %v", maxTPS, destinationWriterCount)
	}
	if !online && !offline {
		return nil, errors.New("at least one clone phase (-online, -offline) must be enabled (and not set to false)")
	}
	return &SplitCloneWorker{
		StatusWorker:            NewStatusWorker(),
		wr:                      wr,
		cell:                    cell,
		keyspace:                keyspace,
		shard:                   shard,
		online:                  online,
		offline:                 offline,
		excludeTables:           excludeTables,
		strategy:                strategy,
		sourceReaderCount:       sourceReaderCount,
		writeQueryMaxRows:       writeQueryMaxRows,
		writeQueryMaxSize:       writeQueryMaxSize,
		minTableSizeForSplit:    minTableSizeForSplit,
		destinationWriterCount:  destinationWriterCount,
		minHealthyRdonlyTablets: minHealthyRdonlyTablets,
		maxTPS:                  maxTPS,
		cleaner:                 &wrangler.Cleaner{},
		tabletTracker:           NewTabletTracker(),

		destinationDbNames:    make(map[string]string),
		destinationThrottlers: make(map[string]*throttler.Throttler),

		ev: &events.SplitClone{
			Cell:          cell,
			Keyspace:      keyspace,
			Shard:         shard,
			ExcludeTables: excludeTables,
			Strategy:      strategy.String(),
		},
	}, nil
}

func (scw *SplitCloneWorker) setState(state StatusWorkerState) {
	scw.SetState(state)
	event.DispatchUpdate(scw.ev, state.String())
}

func (scw *SplitCloneWorker) setErrorState(err error) {
	scw.SetState(WorkerStateError)
	event.DispatchUpdate(scw.ev, "error: "+err.Error())
}

func (scw *SplitCloneWorker) formatSources() string {
	result := ""
	for _, alias := range scw.sourceAliases {
		result += " " + topoproto.TabletAliasString(alias)
	}
	return result
}

// StatusAsHTML implements the Worker interface
func (scw *SplitCloneWorker) StatusAsHTML() template.HTML {
	state := scw.State()

	result := "<b>Working on:</b> " + scw.keyspace + "/" + scw.shard + "</br>\n"
	result += "<b>State:</b> " + state.String() + "</br>\n"
	switch state {
	case WorkerStateCloneOnline, WorkerStateCloneOffline:
		result += "<b>Running</b>:</br>\n"
		result += "<b>Copying from</b>: " + scw.formatSources() + "</br>\n"
		statuses, eta := scw.tableStatusList.format()
		result += "<b>ETA</b>: " + eta.String() + "</br>\n"
		result += strings.Join(statuses, "</br>\n")
	case WorkerStateDone:
		result += "<b>Success</b>:</br>\n"
		statuses, _ := scw.tableStatusList.format()
		result += strings.Join(statuses, "</br>\n")
	}

	return template.HTML(result)
}

// StatusAsText implements the Worker interface
func (scw *SplitCloneWorker) StatusAsText() string {
	state := scw.State()

	result := "Working on: " + scw.keyspace + "/" + scw.shard + "\n"
	result += "State: " + state.String() + "\n"
	switch state {
	case WorkerStateCloneOnline, WorkerStateCloneOffline:
		result += "Running:\n"
		result += "Copying from: " + scw.formatSources() + "\n"
		statuses, eta := scw.tableStatusList.format()
		result += "ETA: " + eta.String() + "\n"
		result += strings.Join(statuses, "\n")
	case WorkerStateDone:
		result += "Success:\n"
		statuses, _ := scw.tableStatusList.format()
		result += strings.Join(statuses, "\n")
	}
	return result
}

// Run implements the Worker interface
func (scw *SplitCloneWorker) Run(ctx context.Context) error {
	resetVars()

	// Run the command.
	err := scw.run(ctx)

	// Cleanup.
	scw.setState(WorkerStateCleanUp)
	// Reverse any changes e.g. setting the tablet type of a source RDONLY tablet.
	cerr := scw.cleaner.CleanUp(scw.wr)
	if cerr != nil {
		if err != nil {
			scw.wr.Logger().Errorf("CleanUp failed in addition to job error: %v", cerr)
		} else {
			err = cerr
		}
	}

	// Stop Throttlers.
	for _, throttler := range scw.destinationThrottlers {
		throttler.Close()
	}
	// Stop healthcheck.
	for _, watcher := range scw.destinationShardWatchers {
		watcher.Stop()
	}
	if scw.healthCheck != nil {
		if err := scw.healthCheck.Close(); err != nil {
			scw.wr.Logger().Errorf("HealthCheck.Close() failed: %v", err)
		}
	}

	if err != nil {
		scw.setErrorState(err)
		return err
	}
	scw.setState(WorkerStateDone)
	return nil
}

func (scw *SplitCloneWorker) run(ctx context.Context) error {
	// Phase 1: read what we need to do.
	if err := scw.init(ctx); err != nil {
		return fmt.Errorf("init() failed: %v", err)
	}
	if err := checkDone(ctx); err != nil {
		return err
	}

	// Phase 2: Find destination master tablets.
	if err := scw.findDestinationMasters(ctx); err != nil {
		return fmt.Errorf("findDestinationMasters() failed: %v", err)
	}
	if err := checkDone(ctx); err != nil {
		return err
	}

	// TODO(mberlin): Uncomment once online clone is implemented.
	//	// Phase 3: (optional) online clone.
	//	if scw.online {
	//    // 3a: TODO(mberlin): Select source tablets.
	//		// 3b: Clone the data.
	//		if err := scw.clone(ctx, online); err != nil {
	//			return fmt.Errorf("online clone() failed: %v", err)
	//		}
	//		if err := checkDone(ctx); err != nil {
	//			return err
	//		}
	//	}

	// Phase 4: offline clone.
	if scw.offline {
		// 4a: Take source tablets out of serving for an exact snapshot.
		if err := scw.findOfflineSourceTablets(ctx); err != nil {
			return fmt.Errorf("findSourceTablets() failed: %v", err)
		}
		if err := checkDone(ctx); err != nil {
			return err
		}

		// 4b: Clone the data.
		if err := scw.clone(ctx, WorkerStateCloneOffline); err != nil {
			return fmt.Errorf("offline clone() failed: %v", err)
		}
		if err := checkDone(ctx); err != nil {
			return err
		}
	}

	return nil
}

// init phase:
// - read the destination keyspace, make sure it has 'servedFrom' values
func (scw *SplitCloneWorker) init(ctx context.Context) error {
	scw.setState(WorkerStateInit)
	var err error

	// read the keyspace and validate it
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	scw.keyspaceInfo, err = scw.wr.TopoServer().GetKeyspace(shortCtx, scw.keyspace)
	cancel()
	if err != nil {
		return fmt.Errorf("cannot read keyspace %v: %v", scw.keyspace, err)
	}

	// find the OverlappingShards in the keyspace
	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	osList, err := topotools.FindOverlappingShards(shortCtx, scw.wr.TopoServer(), scw.keyspace)
	cancel()
	if err != nil {
		return fmt.Errorf("cannot FindOverlappingShards in %v: %v", scw.keyspace, err)
	}

	// find the shard we mentioned in there, if any
	os := topotools.OverlappingShardsForShard(osList, scw.shard)
	if os == nil {
		return fmt.Errorf("the specified shard %v/%v is not in any overlapping shard", scw.keyspace, scw.shard)
	}
	scw.wr.Logger().Infof("Found overlapping shards: %+v\n", os)

	// one side should have served types, the other one none,
	// figure out wich is which, then double check them all
	if len(os.Left[0].ServedTypes) > 0 {
		scw.sourceShards = os.Left
		scw.destinationShards = os.Right
	} else {
		scw.sourceShards = os.Right
		scw.destinationShards = os.Left
	}

	// Verify that filtered replication is not already enabled.
	for _, si := range scw.destinationShards {
		if len(si.SourceShards) > 0 {
			return fmt.Errorf("destination shard %v/%v has filtered replication already enabled from a previous resharding (ShardInfo is set)."+
				" This requires manual intervention e.g. use vtctl SourceShardDelete to remove it",
				si.Keyspace(), si.ShardName())
		}
	}

	// validate all serving types
	servingTypes := []topodatapb.TabletType{topodatapb.TabletType_MASTER, topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY}
	for _, st := range servingTypes {
		for _, si := range scw.sourceShards {
			if si.GetServedType(st) == nil {
				return fmt.Errorf("source shard %v/%v is not serving type %v", si.Keyspace(), si.ShardName(), st)
			}
		}
	}
	for _, si := range scw.destinationShards {
		if len(si.ServedTypes) > 0 {
			return fmt.Errorf("destination shard %v/%v is serving some types", si.Keyspace(), si.ShardName())
		}
	}

	// read the vschema if needed
	var keyspaceSchema *vindexes.KeyspaceSchema
	if *useV3ReshardingMode {
		kschema, err := scw.wr.TopoServer().GetVSchema(ctx, scw.keyspace)
		if err != nil {
			return fmt.Errorf("cannot load VSchema for keyspace %v: %v", scw.keyspace, err)
		}
		if kschema == nil {
			return fmt.Errorf("no VSchema for keyspace %v", scw.keyspace)
		}

		keyspaceSchema, err = vindexes.BuildKeyspaceSchema(kschema, scw.keyspace)
		if err != nil {
			return fmt.Errorf("cannot build vschema for keyspace %v: %v", scw.keyspace, err)
		}
		scw.keyspaceSchema = keyspaceSchema
	}

	return nil
}

// findOfflineSourceTablets phase:
// - find one rdonly in the source shard
// - mark it as 'worker' pointing back to us
// - get the aliases of all the source tablets
func (scw *SplitCloneWorker) findOfflineSourceTablets(ctx context.Context) error {
	scw.setState(WorkerStateFindTargets)

	// find an appropriate tablet in the source shards
	scw.sourceAliases = make([]*topodatapb.TabletAlias, len(scw.sourceShards))
	for i, si := range scw.sourceShards {
		var err error
		scw.sourceAliases[i], err = FindWorkerTablet(ctx, scw.wr, scw.cleaner, scw.cell, si.Keyspace(), si.ShardName(), scw.minHealthyRdonlyTablets)
		if err != nil {
			return fmt.Errorf("FindWorkerTablet() failed for %v/%v/%v: %v", scw.cell, si.Keyspace(), si.ShardName(), err)
		}
		scw.wr.Logger().Infof("Using tablet %v as source for %v/%v", topoproto.TabletAliasString(scw.sourceAliases[i]), si.Keyspace(), si.ShardName())
	}

	// get the tablet info for them, and stop their replication
	scw.sourceTablets = make([]*topo.TabletInfo, len(scw.sourceAliases))
	for i, alias := range scw.sourceAliases {
		var err error
		shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
		scw.sourceTablets[i], err = scw.wr.TopoServer().GetTablet(shortCtx, alias)
		cancel()
		if err != nil {
			return fmt.Errorf("cannot read tablet %v: %v", topoproto.TabletAliasString(alias), err)
		}

		shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
		err = scw.wr.TabletManagerClient().StopSlave(shortCtx, scw.sourceTablets[i].Tablet)
		cancel()
		if err != nil {
			return fmt.Errorf("cannot stop replication on tablet %v", topoproto.TabletAliasString(alias))
		}

		wrangler.RecordStartSlaveAction(scw.cleaner, scw.sourceTablets[i].Tablet)
	}

	return nil
}

// findDestinationMasters finds for each destination shard the current master.
// It also initializes the healthCheck instance which is used by the write
// throttlers.
func (scw *SplitCloneWorker) findDestinationMasters(ctx context.Context) error {
	scw.setState(WorkerStateFindTargets)

	// Initialize healthcheck and add destination shards to it.
	scw.healthCheck = discovery.NewHealthCheck(*remoteActionsTimeout, *healthcheckRetryDelay, *healthCheckTimeout)
	for _, si := range scw.destinationShards {
		watcher := discovery.NewShardReplicationWatcher(scw.wr.TopoServer(), scw.healthCheck,
			scw.cell, si.Keyspace(), si.ShardName(),
			*healthCheckTopologyRefresh, discovery.DefaultTopoReadConcurrency)
		scw.destinationShardWatchers = append(scw.destinationShardWatchers, watcher)
	}

	// Make sure we find a master for each destination shard and log it.
	scw.wr.Logger().Infof("Finding a MASTER tablet for each destination shard...")
	for _, si := range scw.destinationShards {
		waitCtx, waitCancel := context.WithTimeout(ctx, 10*time.Second)
		defer waitCancel()
		if err := discovery.WaitForTablets(waitCtx, scw.healthCheck,
			scw.cell, si.Keyspace(), si.ShardName(), []topodatapb.TabletType{topodatapb.TabletType_MASTER}); err != nil {
			return fmt.Errorf("cannot find MASTER tablet for destination shard for %v/%v: %v", si.Keyspace(), si.ShardName(), err)
		}
		masters := discovery.GetCurrentMaster(
			scw.healthCheck.GetTabletStatsFromTarget(si.Keyspace(), si.ShardName(), topodatapb.TabletType_MASTER))
		if len(masters) == 0 {
			return fmt.Errorf("cannot find MASTER tablet for destination shard for %v/%v in HealthCheck: empty TabletStats list", si.Keyspace(), si.ShardName())
		}
		master := masters[0]

		// Get the MySQL database name of the tablet.
		shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
		ti, err := scw.wr.TopoServer().GetTablet(shortCtx, master.Tablet.Alias)
		cancel()
		if err != nil {
			return fmt.Errorf("cannot get the TabletInfo for destination master (%v) to find out its db name: %v", topoproto.TabletAliasString(master.Tablet.Alias), err)
		}
		keyspaceAndShard := topoproto.KeyspaceShardString(si.Keyspace(), si.ShardName())
		scw.destinationDbNames[keyspaceAndShard] = ti.DbName()

		// TODO(mberlin): Verify on the destination master that the
		// _vt.blp_checkpoint table has the latest schema.

		scw.wr.Logger().Infof("Using tablet %v as destination master for %v/%v", topoproto.TabletAliasString(master.Tablet.Alias), si.Keyspace(), si.ShardName())
	}
	scw.wr.Logger().Infof("NOTE: The used master of a destination shard might change over the course of the copy e.g. due to a reparent. The HealthCheck module will track and log master changes and any error message will always refer the actually used master address.")

	// Set up the throttler for each destination shard.
	for _, si := range scw.destinationShards {
		keyspaceAndShard := topoproto.KeyspaceShardString(si.Keyspace(), si.ShardName())
		t, err := throttler.NewThrottler(
			keyspaceAndShard, "transactions", scw.destinationWriterCount, scw.maxTPS, throttler.ReplicationLagModuleDisabled)
		if err != nil {
			return fmt.Errorf("cannot instantiate throttler: %v", err)
		}
		scw.destinationThrottlers[keyspaceAndShard] = t
	}

	return nil
}

// Find all tablets on all destination shards. This should be done immediately before refreshing
// state on these tablets, to minimize the chances of the topo changing in between.
func (scw *SplitCloneWorker) findRefreshTargets(ctx context.Context) error {
	scw.refreshAliases = make([][]*topodatapb.TabletAlias, len(scw.destinationShards))
	scw.refreshTablets = make([]map[topodatapb.TabletAlias]*topo.TabletInfo, len(scw.destinationShards))

	for shardIndex, si := range scw.destinationShards {
		refreshAliases, refreshTablets, err := resolveRefreshTabletsForShard(ctx, si.Keyspace(), si.ShardName(), scw.wr)
		if err != nil {
			return err
		}
		scw.refreshAliases[shardIndex], scw.refreshTablets[shardIndex] = refreshAliases, refreshTablets
	}

	return nil
}

// copy phase:
//	- copy the data from source tablets to destination masters (with replication on)
// Assumes that the schema has already been created on each destination tablet
// (probably from vtctl's CopySchemaShard)
func (scw *SplitCloneWorker) clone(ctx context.Context, state StatusWorkerState) error {
	if state != WorkerStateCloneOnline && state != WorkerStateCloneOffline {
		panic(fmt.Sprintf("invalid state passed to clone(): %v", state))
	}
	scw.setState(state)
	start := time.Now()
	defer func() {
		statsStateDurationsNs.Set(string(state), time.Now().Sub(start).Nanoseconds())
	}()

	// get source schema from the first shard
	// TODO(alainjobart): for now, we assume the schema is compatible
	// on all source shards. Furthermore, we estimate the number of rows
	// in each source shard for each table to be about the same
	// (rowCount is used to estimate an ETA)
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	sourceSchemaDefinition, err := scw.wr.GetSchema(shortCtx, scw.sourceAliases[0], nil, scw.excludeTables, true)
	cancel()
	if err != nil {
		return fmt.Errorf("cannot get schema from source %v: %v", topoproto.TabletAliasString(scw.sourceAliases[0]), err)
	}
	if len(sourceSchemaDefinition.TableDefinitions) == 0 {
		return fmt.Errorf("no tables matching the table filter in tablet %v", topoproto.TabletAliasString(scw.sourceAliases[0]))
	}
	scw.wr.Logger().Infof("Source tablet 0 has %v tables to copy", len(sourceSchemaDefinition.TableDefinitions))
	scw.tableStatusList.initialize(sourceSchemaDefinition)

	// In parallel, setup the channels to send SQL data chunks to for each destination tablet:
	//
	// mu protects the context for cancelation, and firstError
	mu := sync.Mutex{}
	var firstError error

	ctx, cancelCopy := context.WithCancel(ctx)
	processError := func(format string, args ...interface{}) {
		scw.wr.Logger().Errorf(format, args...)
		mu.Lock()
		if firstError == nil {
			firstError = fmt.Errorf(format, args...)
			cancelCopy()
		}
		mu.Unlock()
	}

	insertChannels := make([]chan string, len(scw.destinationShards))
	destinationWaitGroup := sync.WaitGroup{}
	for shardIndex, si := range scw.destinationShards {
		// we create one channel per destination tablet.  It
		// is sized to have a buffer of a maximum of
		// destinationWriterCount * 2 items, to hopefully
		// always have data. We then have
		// destinationWriterCount go routines reading from it.
		insertChannels[shardIndex] = make(chan string, scw.destinationWriterCount*2)

		go func(keyspace, shard string, insertChannel chan string) {
			for j := 0; j < scw.destinationWriterCount; j++ {
				destinationWaitGroup.Add(1)
				go func(threadID int) {
					defer destinationWaitGroup.Done()

					keyspaceAndShard := topoproto.KeyspaceShardString(keyspace, shard)
					throttler := scw.destinationThrottlers[keyspaceAndShard]
					defer throttler.ThreadFinished(threadID)

					executor := newExecutor(scw.wr, scw.healthCheck, throttler, keyspace, shard, threadID)
					if err := executor.fetchLoop(ctx, insertChannel); err != nil {
						processError("executer.FetchLoop failed: %v", err)
					}
				}(j)
			}
		}(si.Keyspace(), si.ShardName(), insertChannels[shardIndex])
	}

	// Now for each table, read data chunks and send them to all
	// insertChannels
	sourceWaitGroup := sync.WaitGroup{}
	sema := sync2.NewSemaphore(scw.sourceReaderCount, 0)
	for tableIndex, td := range sourceSchemaDefinition.TableDefinitions {
		if td.Type == tmutils.TableView {
			continue
		}

		var keyResolver keyspaceIDResolver
		if *useV3ReshardingMode {
			keyResolver, err = newV3ResolverFromTableDefinition(scw.keyspaceSchema, td)
			if err != nil {
				return fmt.Errorf("cannot resolve v3 sharding keys for keyspace %v: %v", scw.keyspace, err)
			}
		} else {
			keyResolver, err = newV2Resolver(scw.keyspaceInfo, td)
			if err != nil {
				return fmt.Errorf("cannot resolve sharding keys for keyspace %v: %v", scw.keyspace, err)
			}
		}

		// TODO(mberlin): Re-think how MIN, MAX of the primary key should be determined when we no longer loop over the source shards.
		chunks, err := FindChunks(ctx, scw.wr, scw.sourceTablets[0], td, scw.minTableSizeForSplit, scw.sourceReaderCount)
		if err != nil {
			return err
		}
		scw.tableStatusList.setThreadCount(tableIndex, len(chunks)-1)

		for chunkIndex := 0; chunkIndex < len(chunks)-1; chunkIndex++ {
			sourceWaitGroup.Add(1)
			go func(td *tabletmanagerdatapb.TableDefinition, tableIndex, chunkIndex int) {
				defer sourceWaitGroup.Done()

				sema.Acquire()
				defer sema.Release()

				scw.tableStatusList.threadStarted(tableIndex)

				// Set up readers for the diff. There will be one reader for every
				// source and destination shard.
				sourceReaders := make([]ResultReader, len(scw.sourceShards))
				destReaders := make([]ResultReader, len(scw.destinationShards))
				for shardIndex := range scw.sourceShards {
					selectSQL := buildSQLFromChunks(scw.wr, td, chunks, chunkIndex, scw.sourceAliases[shardIndex].String())
					sourceResultReader, err := NewQueryResultReaderForTablet(ctx, scw.wr.TopoServer(), scw.sourceAliases[shardIndex], selectSQL)
					if err != nil {
						processError("NewQueryResultReaderForTablet for source tablets failed: %v", err)
						return
					}
					defer sourceResultReader.Close()
					sourceReaders[shardIndex] = sourceResultReader
				}
				for shardIndex, si := range scw.destinationShards {
					// Pick a destination tablet.
					tablets := discovery.RemoveUnhealthyTablets(
						scw.healthCheck.GetTabletStatsFromTarget(si.Keyspace(), si.ShardName(), topodatapb.TabletType_RDONLY))
					if len(tablets) == 0 {
						// TODO(mberlin): Retry here? For how long? 2 hours similar as fetchWithRetries does?
						processError("no healthy RDONLY tablets in destination shard (%v) available", topoproto.KeyspaceShardString(si.Keyspace(), si.ShardName()))
						return
					}
					destAlias := scw.tabletTracker.Track(tablets)
					defer scw.tabletTracker.Untrack(destAlias)

					selectSQL := buildSQLFromChunks(scw.wr, td, chunks, chunkIndex, destAlias.String())
					destResultReader, err := NewQueryResultReaderForTablet(ctx, scw.wr.TopoServer(), destAlias, selectSQL)
					if err != nil {
						processError("NewQueryResultReaderForTablet for dest tablets failed: %v", err)
						return
					}
					defer destResultReader.Close()
					destReaders[shardIndex] = destResultReader
				}

				var sourceReader ResultReader
				var destReader ResultReader
				if len(sourceReaders) >= 2 {
					sourceReader, err = NewResultMerger(sourceReaders, len(td.PrimaryKeyColumns))
					if err != nil {
						processError("NewResultMerger for source tablets failed: %v", err)
						return
					}
				} else {
					sourceReader = sourceReaders[0]
				}
				if len(destReaders) >= 2 {
					destReader, err = NewResultMerger(destReaders, len(td.PrimaryKeyColumns))
					if err != nil {
						processError("NewResultMerger for dest tablets failed: %v", err)
						return
					}
				} else {
					destReader = destReaders[0]
				}

				dbNames := make([]string, len(scw.destinationShards))
				for i, si := range scw.destinationShards {
					keyspaceAndShard := topoproto.KeyspaceShardString(si.Keyspace(), si.ShardName())
					dbNames[i] = scw.destinationDbNames[keyspaceAndShard]
				}
				// Compare the data and repair any differences.
				differ, err := NewRowDiffer2(sourceReader, destReader, td, scw.tableStatusList, tableIndex,
					scw.destinationShards, keyResolver,
					insertChannels, ctx.Done(), dbNames, scw.writeQueryMaxRows, scw.writeQueryMaxSize)
				if err != nil {
					processError("NewRowDiffer2 failed: %v", err)
					return
				}
				// Ignore the diff report because all diffs should get repaired.
				_ /* DiffReport */, err = differ.Go(scw.wr.Logger())
				if err != nil {
					processError("RowDiffer2 failed: %v", err)
					return
				}

				scw.tableStatusList.threadDone(tableIndex)
			}(td, tableIndex, chunkIndex)
		}
	}
	sourceWaitGroup.Wait()

	for shardIndex := range scw.destinationShards {
		close(insertChannels[shardIndex])
	}
	destinationWaitGroup.Wait()
	if firstError != nil {
		return firstError
	}

	if state == WorkerStateCloneOffline {
		// Create and populate the blp_checkpoint table to give filtered replication
		// a starting point.
		if scw.strategy.skipPopulateBlpCheckpoint {
			scw.wr.Logger().Infof("Skipping populating the blp_checkpoint table")
		} else {
			queries := make([]string, 0, 4)
			queries = append(queries, binlogplayer.CreateBlpCheckpoint()...)
			flags := ""
			if scw.strategy.dontStartBinlogPlayer {
				flags = binlogplayer.BlpFlagDontStart
			}

			// get the current position from the sources
			for shardIndex := range scw.sourceShards {
				shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
				status, err := scw.wr.TabletManagerClient().SlaveStatus(shortCtx, scw.sourceTablets[shardIndex].Tablet)
				cancel()
				if err != nil {
					return err
				}

				queries = append(queries, binlogplayer.PopulateBlpCheckpoint(uint32(shardIndex), status.Position, scw.maxTPS, throttler.ReplicationLagModuleDisabled, time.Now().Unix(), flags))
			}

			for _, si := range scw.destinationShards {
				destinationWaitGroup.Add(1)
				go func(keyspace, shard string) {
					defer destinationWaitGroup.Done()
					scw.wr.Logger().Infof("Making and populating blp_checkpoint table")
					keyspaceAndShard := topoproto.KeyspaceShardString(keyspace, shard)
					if err := runSQLCommands(ctx, scw.wr, scw.healthCheck, keyspace, shard, scw.destinationDbNames[keyspaceAndShard], queries); err != nil {
						processError("blp_checkpoint queries failed: %v", err)
					}
				}(si.Keyspace(), si.ShardName())
			}
			destinationWaitGroup.Wait()
			if firstError != nil {
				return firstError
			}
		}

		// Configure filtered replication by setting the SourceShard info.
		// The master tablets won't enable filtered replication (the binlog player)
		//  until they re-read the topology due to a restart or a reload.
		// TODO(alainjobart) this is a superset, some shards may not
		// overlap, have to deal with this better (for N -> M splits
		// where both N>1 and M>1)
		if scw.strategy.skipSetSourceShards {
			scw.wr.Logger().Infof("Skipping setting SourceShard on destination shards.")
		} else {
			for _, si := range scw.destinationShards {
				scw.wr.Logger().Infof("Setting SourceShard on shard %v/%v", si.Keyspace(), si.ShardName())
				shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
				err := scw.wr.SetSourceShards(shortCtx, si.Keyspace(), si.ShardName(), scw.sourceAliases, nil)
				cancel()
				if err != nil {
					return fmt.Errorf("failed to set source shards: %v", err)
				}
			}
		}

	  err = scw.findRefreshTargets(ctx)
  	if err != nil {
	  	return fmt.Errorf("failed before refreshing state on destination tablets: %v", err)
  	}
  	// Force a state refresh (re-read topo) on all destination tablets.
	  // The master tablet will end up starting filtered replication
  	// at this point.
		for shardIndex := range scw.destinationShards {
			for _, tabletAlias := range scw.refreshAliases[shardIndex] {
				destinationWaitGroup.Add(1)
				go func(ti *topo.TabletInfo) {
					defer destinationWaitGroup.Done()
					scw.wr.Logger().Infof("Refreshing state on tablet %v", ti.AliasString())
					shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
					err := scw.wr.TabletManagerClient().ReloadSchema(shortCtx, ti.Tablet)
					cancel()
					if err != nil {
						processError("RefreshState failed on tablet %v: %v", ti.AliasString(), err)
					}
				}(scw.refreshTablets[shardIndex][*tabletAlias])
			}
		}
	} // clonePhase == offline

	destinationWaitGroup.Wait()
	return firstError
}
