// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/dbconnpool"
	"github.com/youtube/vitess/go/vt/mysqlctl/replication"
	"github.com/youtube/vitess/go/vt/mysqlctl/tmutils"
	"github.com/youtube/vitess/go/vt/tabletserver/grpcqueryservice"
	"github.com/youtube/vitess/go/vt/tabletserver/queryservice/fakes"
	"github.com/youtube/vitess/go/vt/vttest/fakesqldb"
	"github.com/youtube/vitess/go/vt/wrangler/testlib"
	"github.com/youtube/vitess/go/vt/zktopo/zktestserver"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// testQueryService is a local QueryService implementation to support the tests.
type testQueryService struct {
	t *testing.T

	*fakes.StreamHealthQueryService
}

func (sq *testQueryService) StreamExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}, sessionID int64, sendReply func(reply *sqltypes.Result) error) error {
	// Custom parsing of the query we expect
	min := 100
	max := 200
	var err error
	parts := strings.Split(sql, " ")
	for _, part := range parts {
		if strings.HasPrefix(part, "id>=") {
			min, err = strconv.Atoi(part[4:])
			if err != nil {
				return err
			}
		} else if strings.HasPrefix(part, "id<") {
			max, err = strconv.Atoi(part[3:])
		}
	}
	sq.t.Logf("testQueryService: got query: %v with min %v max %v", sql, min, max)

	// Send the headers
	if err := sendReply(&sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "id",
				Type: sqltypes.Int64,
			},
			{
				Name: "msg",
				Type: sqltypes.VarChar,
			},
			{
				Name: "keyspace_id",
				Type: sqltypes.Int64,
			},
		},
	}); err != nil {
		return err
	}

	// Send the values
	ksids := []uint64{0x2000000000000000, 0x6000000000000000}
	for i := min; i < max; i++ {
		if err := sendReply(&sqltypes.Result{
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeString([]byte(fmt.Sprintf("%v", i))),
					sqltypes.MakeString([]byte(fmt.Sprintf("Text for %v", i))),
					sqltypes.MakeString([]byte(fmt.Sprintf("%v", ksids[i%2]))),
				},
			},
		}); err != nil {
			return err
		}
	}
	// SELECT id, msg, keyspace_id FROM table1 WHERE id>=180 AND id<190 ORDER BY id
	return nil
}

type ExpectedExecuteFetch struct {
	Query       string
	MaxRows     int
	WantFields  bool
	QueryResult *sqltypes.Result
	Error       error
}

// FakePoolConnection implements dbconnpool.PoolConnection
type FakePoolConnection struct {
	t      *testing.T
	Closed bool

	ExpectedExecuteFetch      []ExpectedExecuteFetch
	ExpectedExecuteFetchIndex int
}

func NewFakePoolConnectionQuery(t *testing.T, query string, err error) *FakePoolConnection {
	return &FakePoolConnection{
		t: t,
		ExpectedExecuteFetch: []ExpectedExecuteFetch{
			{
				Query:       query,
				QueryResult: &sqltypes.Result{},
				Error:       err,
			},
		},
	}
}

func (fpc *FakePoolConnection) ExecuteFetch(query string, maxrows int, wantfields bool) (*sqltypes.Result, error) {
	if fpc.ExpectedExecuteFetchIndex >= len(fpc.ExpectedExecuteFetch) {
		fpc.t.Errorf("got unexpected out of bound fetch: %v >= %v", fpc.ExpectedExecuteFetchIndex, len(fpc.ExpectedExecuteFetch))
		return nil, fmt.Errorf("unexpected out of bound fetch")
	}
	expected := fpc.ExpectedExecuteFetch[fpc.ExpectedExecuteFetchIndex].Query
	if strings.HasSuffix(expected, "*") {
		if !strings.HasPrefix(query, expected[0:len(expected)-1]) {
			fpc.t.Errorf("got unexpected query start: %v != %v", query, expected)
			return nil, fmt.Errorf("unexpected query")
		}
	} else {
		if query != expected {
			fpc.t.Errorf("got unexpected query: %v != %v", query, expected)
			return nil, fmt.Errorf("unexpected query")
		}
	}
	fpc.t.Logf("ExecuteFetch: %v", query)
	defer func() {
		fpc.ExpectedExecuteFetchIndex++
	}()
	if fpc.ExpectedExecuteFetch[fpc.ExpectedExecuteFetchIndex].Error != nil {
		return nil, fpc.ExpectedExecuteFetch[fpc.ExpectedExecuteFetchIndex].Error
	}
	return fpc.ExpectedExecuteFetch[fpc.ExpectedExecuteFetchIndex].QueryResult, nil
}

func (fpc *FakePoolConnection) ExecuteStreamFetch(query string, callback func(*sqltypes.Result) error, streamBufferSize int) error {
	return nil
}

func (fpc *FakePoolConnection) ID() int64 {
	return 1
}

func (fpc *FakePoolConnection) Close() {
	fpc.Closed = true
}

func (fpc *FakePoolConnection) IsClosed() bool {
	return fpc.Closed
}

func (fpc *FakePoolConnection) Recycle() {
}

func (fpc *FakePoolConnection) Reconnect() error {
	return nil
}

// on the source rdonly guy, should only have one query to find min & max
func SourceRdonlyFactory(t *testing.T) func() (dbconnpool.PoolConnection, error) {
	return func() (dbconnpool.PoolConnection, error) {
		return &FakePoolConnection{
			t: t,
			ExpectedExecuteFetch: []ExpectedExecuteFetch{
				{
					Query: "SELECT MIN(id), MAX(id) FROM vt_ks.table1",
					QueryResult: &sqltypes.Result{
						Fields: []*querypb.Field{
							{
								Name: "min",
								Type: sqltypes.Int64,
							},
							{
								Name: "max",
								Type: sqltypes.Int64,
							},
						},
						Rows: [][]sqltypes.Value{
							{
								sqltypes.MakeString([]byte("100")),
								sqltypes.MakeString([]byte("200")),
							},
						},
					},
				},
			},
		}, nil
	}
}

// on the destinations
func DestinationsFactory(t *testing.T, insertCount int64) func() (dbconnpool.PoolConnection, error) {
	var queryIndex int64 = -1

	return func() (dbconnpool.PoolConnection, error) {
		qi := atomic.AddInt64(&queryIndex, 1)
		switch {
		// Return an error on the first query, to make sure that it's retried successfully
		case qi == 0:
			return NewFakePoolConnectionQuery(t,
				"INSERT INTO `vt_ks`.table1(id, msg, keyspace_id) VALUES (*",
				fmt.Errorf("The MariaDB server is running with the --read-only option so it cannot execute this statement (errno 1290) during query:"),
			), nil
		case qi <= insertCount:
			return NewFakePoolConnectionQuery(t, "INSERT INTO `vt_ks`.table1(id, msg, keyspace_id) VALUES (*", nil), nil
		case qi == insertCount+1:
			return NewFakePoolConnectionQuery(t, "CREATE DATABASE IF NOT EXISTS _vt", nil), nil
		case qi == insertCount+2:
			return NewFakePoolConnectionQuery(t, "CREATE TABLE IF NOT EXISTS _vt.blp_checkpoint (\n"+
				"  source_shard_uid INT(10) UNSIGNED NOT NULL,\n"+
				"  pos VARCHAR(250) DEFAULT NULL,\n"+
				"  time_updated BIGINT UNSIGNED NOT NULL,\n"+
				"  transaction_timestamp BIGINT UNSIGNED NOT NULL,\n"+
				"  flags VARCHAR(250) DEFAULT NULL,\n"+
				"  PRIMARY KEY (source_shard_uid)) ENGINE=InnoDB", nil), nil
		case qi == insertCount+3:
			return NewFakePoolConnectionQuery(t, "INSERT INTO _vt.blp_checkpoint (source_shard_uid, pos, time_updated, transaction_timestamp, flags) VALUES (0, 'MariaDB/12-34-5678', *", nil), nil
		}

		return nil, fmt.Errorf("Unexpected connection")
	}
}

func testSplitClone(t *testing.T, v3 bool) {
	*useV3ReshardingMode = v3
	db := fakesqldb.Register()
	ts := zktestserver.New(t, []string{"cell1", "cell2"})
	ctx := context.Background()
	wi := NewInstance(ctx, ts, "cell1", time.Second)

	if v3 {
		// FIXME(alainjobart): ShardingColumnName and ShardingColumnType
		// are not used by v3 split_clone, but they are required
		// by tabletmanager to setup the rules. We have b/27901260
		// open internally to fix this.
		if err := ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{
			ShardingColumnName: "keyspace_id",
			ShardingColumnType: topodatapb.KeyspaceIdType_UINT64,
		}); err != nil {
			t.Fatalf("CreateKeyspace v3 failed: %v", err)
		}

		if err := ts.SaveVSchema(ctx, "ks", `{
  "Sharded": true,
  "Vindexes": {
    "table1_index": {
      "Type": "numeric"
    }
  },
  "Tables": {
    "table1": {
      "ColVindexes": [
        {
          "Col": "keyspace_id",
          "Name": "table1_index"
        }
      ]
    }
  }
}`); err != nil {
			t.Fatalf("SaveVSchema v3 failed: %v", err)
		}
	} else {
		if err := ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{
			ShardingColumnName: "keyspace_id",
			ShardingColumnType: topodatapb.KeyspaceIdType_UINT64,
		}); err != nil {
			t.Fatalf("CreateKeyspace v2 failed: %v", err)
		}
	}

	sourceMaster := testlib.NewFakeTablet(t, wi.wr, "cell1", 0,
		topodatapb.TabletType_MASTER, db, testlib.TabletKeyspaceShard(t, "ks", "-80"))
	sourceRdonly1 := testlib.NewFakeTablet(t, wi.wr, "cell1", 1,
		topodatapb.TabletType_RDONLY, db, testlib.TabletKeyspaceShard(t, "ks", "-80"))
	sourceRdonly2 := testlib.NewFakeTablet(t, wi.wr, "cell1", 2,
		topodatapb.TabletType_RDONLY, db, testlib.TabletKeyspaceShard(t, "ks", "-80"))

	leftMaster := testlib.NewFakeTablet(t, wi.wr, "cell1", 10,
		topodatapb.TabletType_MASTER, db, testlib.TabletKeyspaceShard(t, "ks", "-40"))
	leftRdonly := testlib.NewFakeTablet(t, wi.wr, "cell1", 11,
		topodatapb.TabletType_RDONLY, db, testlib.TabletKeyspaceShard(t, "ks", "-40"))

	rightMaster := testlib.NewFakeTablet(t, wi.wr, "cell1", 20,
		topodatapb.TabletType_MASTER, db, testlib.TabletKeyspaceShard(t, "ks", "40-80"))
	rightRdonly := testlib.NewFakeTablet(t, wi.wr, "cell1", 21,
		topodatapb.TabletType_RDONLY, db, testlib.TabletKeyspaceShard(t, "ks", "40-80"))

	for _, ft := range []*testlib.FakeTablet{sourceMaster, sourceRdonly1, sourceRdonly2, leftMaster, leftRdonly, rightMaster, rightRdonly} {
		ft.StartActionLoop(t, wi.wr)
		defer ft.StopActionLoop(t)
	}

	// add the topo and schema data we'll need
	if err := ts.CreateShard(ctx, "ks", "80-"); err != nil {
		t.Fatalf("CreateShard(\"-80\") failed: %v", err)
	}
	if err := wi.wr.SetKeyspaceShardingInfo(ctx, "ks", "keyspace_id", topodatapb.KeyspaceIdType_UINT64, 4, false); err != nil {
		t.Fatalf("SetKeyspaceShardingInfo failed: %v", err)
	}
	if err := wi.wr.RebuildKeyspaceGraph(ctx, "ks", nil, true); err != nil {
		t.Fatalf("RebuildKeyspaceGraph failed: %v", err)
	}

	for _, sourceRdonly := range []*testlib.FakeTablet{sourceRdonly1, sourceRdonly2} {
		sourceRdonly.FakeMysqlDaemon.Schema = &tabletmanagerdatapb.SchemaDefinition{
			DatabaseSchema: "",
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
				{
					Name:              "table1",
					Columns:           []string{"id", "msg", "keyspace_id"},
					PrimaryKeyColumns: []string{"id"},
					Type:              tmutils.TableBaseTable,
					// This informs how many rows we can pack into a single insert
					DataLength: 2048,
				},
			},
		}
		sourceRdonly.FakeMysqlDaemon.DbAppConnectionFactory = SourceRdonlyFactory(t)
		sourceRdonly.FakeMysqlDaemon.CurrentMasterPosition = replication.Position{
			GTIDSet: replication.MariadbGTID{Domain: 12, Server: 34, Sequence: 5678},
		}
		sourceRdonly.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
			"STOP SLAVE",
			"START SLAVE",
		}
		qs := fakes.NewStreamHealthQueryService(sourceRdonly.Target())
		qs.AddDefaultHealthResponse()
		grpcqueryservice.RegisterForTest(sourceRdonly.RPCServer, &testQueryService{
			t: t,
			StreamHealthQueryService: qs,
		})
	}

	// We read 100 source rows. sourceReaderCount is set to 10, so
	// we'll have 100/10=10 rows per table chunk.
	// destinationPackCount is set to 4, so we take 4 source rows
	// at once. So we'll process 4 + 4 + 2 rows to get to 10.
	// That means 3 insert statements on each target (each
	// containing half of the rows, i.e. 2 + 2 + 1 rows). So 3 * 10
	// = 30 insert statements on each destination.
	leftMaster.FakeMysqlDaemon.DbAppConnectionFactory = DestinationsFactory(t, 30)
	leftRdonly.FakeMysqlDaemon.DbAppConnectionFactory = DestinationsFactory(t, 30)
	rightMaster.FakeMysqlDaemon.DbAppConnectionFactory = DestinationsFactory(t, 30)
	rightRdonly.FakeMysqlDaemon.DbAppConnectionFactory = DestinationsFactory(t, 30)

	// Only wait 1 ms between retries, so that the test passes faster
	*executeFetchRetryTime = (1 * time.Millisecond)

	// Run the vtworker command.
	args := []string{
		"SplitClone",
		"-source_reader_count", "10",
		"-destination_pack_count", "4",
		"-min_table_size_for_split", "1",
		"-destination_writer_count", "10",
		"ks/-80"}
	if err := runCommand(t, wi, wi.wr, args); err != nil {
		t.Fatal(err)
	}

	if statsDestinationAttemptedResolves.String() != "3" {
		t.Errorf("Wrong statsDestinationAttemptedResolves: wanted %v, got %v", "3", statsDestinationAttemptedResolves.String())
	}
	if statsDestinationActualResolves.String() != "1" {
		t.Errorf("Wrong statsDestinationActualResolves: wanted %v, got %v", "1", statsDestinationActualResolves.String())
	}
	if statsRetryCounters.String() != "{\"ReadOnly\": 2}" {
		t.Errorf("Wrong statsRetryCounters: wanted %v, got %v", "{\"ReadOnly\": 2}", statsRetryCounters.String())
	}
}

func TestSplitCloneV2(t *testing.T) {
	testSplitClone(t, false)
}

func TestSplitCloneV3(t *testing.T) {
	testSplitClone(t, true)
}
