package worker

import (
	"context"
	"fmt"

	"github.com/youtube/vitess/go/stats"
	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
)

type cloneChunkWorker struct {
	*SplitCloneWorker
	ctx          context.Context
	processError func(format string, args ...interface{})

	insertChannels  []chan string
	tableStatusList *tableStatusList
	statsCounters   []*stats.Counters
}

type workItem struct {
	state       StatusWorkerState
	td          *tabletmanagerdatapb.TableDefinition
	tableIndex  int
	keyResolver keyspaceIDResolver
	chunk       chunk
}

func (w *cloneChunkWorker) run() {
	//	sourceWaitGroup.Add(1)
	//	defer sourceWaitGroup.Done()
}

func (w *cloneChunkWorker) processChunk(i workItem) {
	errPrefix := fmt.Sprintf("table=%v chunk=%v", i.td.Name, i.chunk)

	// We need our own error per Go routine to avoid races.
	var err error

	w.tableStatusList.threadStarted(i.tableIndex)

	if i.state == WorkerStateCloneOnline {
		// Wait for enough healthy tablets (they might have become unhealthy
		// and their replication lag might have increased since we started.)
		if err := w.waitForTablets(w.ctx, w.sourceShards, *retryDuration); err != nil {
			w.processError("%v: No healthy source tablets found (gave up after %v): ", errPrefix, *retryDuration, err)
			return
		}
	}

	// Set up readers for the diff. There will be one reader for every
	// source and destination shard.
	sourceReaders := make([]ResultReader, len(w.sourceShards))
	destReaders := make([]ResultReader, len(w.destinationShards))
	for shardIndex, si := range w.sourceShards {
		var tp tabletProvider
		allowMultipleRetries := true
		if i.state == WorkerStateCloneOffline {
			tp = newSingleTabletProvider(w.ctx, w.wr.TopoServer(), w.offlineSourceAliases[shardIndex])
			// allowMultipleRetries is false to avoid that we'll keep retrying
			// on the same tablet alias for hours. This guards us against the
			// situation that an offline tablet gets restarted and serves again.
			// In that case we cannot use it because its replication is no
			// longer stopped at the same point as we took it offline initially.
			allowMultipleRetries = false
		} else {
			tp = newShardTabletProvider(w.tsc, w.tabletTracker, si.Keyspace(), si.ShardName())
		}
		sourceResultReader, err := NewRestartableResultReader(w.ctx, w.wr.Logger(), tp, i.td, i.chunk, allowMultipleRetries)
		if err != nil {
			w.processError("%v: NewRestartableResultReader for source: %v failed", errPrefix, tp.description())
			return
		}
		defer sourceResultReader.Close()
		sourceReaders[shardIndex] = sourceResultReader
	}

	// Wait for enough healthy tablets (they might have become unhealthy
	// and their replication lag might have increased due to a previous
	// chunk pipeline.)
	if err := w.waitForTablets(w.ctx, w.destinationShards, *retryDuration); err != nil {
		processError("%v: No healthy destination tablets found (gave up after %v): ", errPrefix, *retryDuration, err)
		return
	}

	for shardIndex, si := range w.destinationShards {
		tp := newShardTabletProvider(w.tsc, w.tabletTracker, si.Keyspace(), si.ShardName())
		destResultReader, err := NewRestartableResultReader(w.ctx, w.wr.Logger(), tp, i.td, i.chunk, true /* allowMultipleRetries */)
		if err != nil {
			processError("%v: NewRestartableResultReader for destination: %v failed: %v", errPrefix, tp.description(), err)
			return
		}
		defer destResultReader.Close()
		destReaders[shardIndex] = destResultReader
	}

	var sourceReader ResultReader
	var destReader ResultReader
	if len(sourceReaders) >= 2 {
		sourceReader, err = NewResultMerger(sourceReaders, len(i.td.PrimaryKeyColumns))
		if err != nil {
			processError("%v: NewResultMerger for source tablets failed: %v", errPrefix, err)
			return
		}
	} else {
		sourceReader = sourceReaders[0]
	}
	if len(destReaders) >= 2 {
		destReader, err = NewResultMerger(destReaders, len(i.td.PrimaryKeyColumns))
		if err != nil {
			processError("%v: NewResultMerger for destination tablets failed: %v", errPrefix, err)
			return
		}
	} else {
		destReader = destReaders[0]
	}

	dbNames := make([]string, len(w.destinationShards))
	for i, si := range w.destinationShards {
		keyspaceAndShard := topoproto.KeyspaceShardString(si.Keyspace(), si.ShardName())
		dbNames[i] = w.destinationDbNames[keyspaceAndShard]
	}
	// Compare the data and reconcile any differences.
	differ, err := NewRowDiffer2(w.ctx, sourceReader, destReader, i.td, w.tableStatusList, i.tableIndex,
		w.destinationShards, i.keyResolver,
		w.insertChannels, w.ctx.Done(), dbNames, w.writeQueryMaxRows, w.writeQueryMaxSize, w.writeQueryMaxRowsDelete, w.statsCounters)
	if err != nil {
		processError("%v: NewRowDiffer2 failed: %v", errPrefix, err)
		return
	}
	// Ignore the diff report because all diffs should get reconciled.
	_ /* DiffReport */, err = differ.Diff()
	if err != nil {
		processError("%v: RowDiffer2 failed: %v", errPrefix, err)
		return
	}

	w.tableStatusList.threadDone(i.tableIndex)
}
