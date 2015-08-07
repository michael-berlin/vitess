// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/key"
	kproto "github.com/youtube/vitess/go/vt/key"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	"golang.org/x/net/context"
)

// This file uses the sandbox_test framework.

func init() {
	schema := createTestSchema(`
{
  "Keyspaces": {
    "TestUnsharded": {
      "Sharded": false,
      "Tables": {
        "t1": ""
      }
    }
  }
}
`)
	Init(new(sandboxTopo), schema, "aa", 1*time.Second, 10, 2*time.Millisecond, 1*time.Millisecond, 24*time.Hour, 0)
}

func TestVTGateExecute(t *testing.T) {
	sandbox := createSandbox(KsTestUnsharded)
	sbc := &sandboxConn{}
	sandbox.MapTestConn("0", sbc)
	q := proto.Query{
		Sql:        "select * from t1",
		TabletType: topo.TYPE_MASTER,
	}
	qr := new(proto.QueryResult)
	err := rpcVTGate.Execute(context.Background(), &q, qr)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	wantqr := new(proto.QueryResult)
	wantqr.Result = singleRowResult
	if !reflect.DeepEqual(wantqr, qr) {
		t.Errorf("want \n%+v, got \n%+v", singleRowResult, qr)
	}
	if qr.Session != nil {
		t.Errorf("want nil, got %+v\n", qr.Session)
	}

	q.Session = new(proto.Session)
	rpcVTGate.Begin(context.Background(), q.Session)
	if !q.Session.InTransaction {
		t.Errorf("want true, got false")
	}
	rpcVTGate.Execute(context.Background(), &q, qr)
	wantSession := &proto.Session{
		InTransaction: true,
		ShardSessions: []*proto.ShardSession{{
			Keyspace:      KsTestUnsharded,
			Shard:         "0",
			TabletType:    topo.TYPE_MASTER,
			TransactionId: 1,
		}},
	}
	if !reflect.DeepEqual(wantSession, q.Session) {
		t.Errorf("want \n%+v, got \n%+v", wantSession, q.Session)
	}

	rpcVTGate.Commit(context.Background(), q.Session)
	if commitCount := sbc.CommitCount.Get(); commitCount != 1 {
		t.Errorf("want 1, got %d", commitCount)
	}

	q.Session = new(proto.Session)
	rpcVTGate.Begin(context.Background(), q.Session)
	rpcVTGate.Execute(context.Background(), &q, qr)
	rpcVTGate.Rollback(context.Background(), q.Session)
}

func TestVTGateExecuteShard(t *testing.T) {
	sandbox := createSandbox("TestVTGateExecuteShard")
	sbc := &sandboxConn{}
	sandbox.MapTestConn("0", sbc)
	q := proto.QueryShard{
		Sql:      "query",
		Keyspace: "TestVTGateExecuteShard",
		Shards:   []string{"0"},
	}
	qr := new(proto.QueryResult)
	err := rpcVTGate.ExecuteShard(context.Background(), &q, qr)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	wantqr := new(proto.QueryResult)
	wantqr.Result = singleRowResult
	if !reflect.DeepEqual(wantqr, qr) {
		t.Errorf("want \n%+v, got \n%+v", singleRowResult, qr)
	}
	if qr.Session != nil {
		t.Errorf("want nil, got %+v\n", qr.Session)
	}

	q.Session = new(proto.Session)
	rpcVTGate.Begin(context.Background(), q.Session)
	if !q.Session.InTransaction {
		t.Errorf("want true, got false")
	}
	rpcVTGate.ExecuteShard(context.Background(), &q, qr)
	wantSession := &proto.Session{
		InTransaction: true,
		ShardSessions: []*proto.ShardSession{{
			Keyspace:      "TestVTGateExecuteShard",
			Shard:         "0",
			TransactionId: 1,
		}},
	}
	if !reflect.DeepEqual(wantSession, q.Session) {
		t.Errorf("want \n%+v, got \n%+v", wantSession, q.Session)
	}

	rpcVTGate.Commit(context.Background(), q.Session)
	if commitCount := sbc.CommitCount.Get(); commitCount != 1 {
		t.Errorf("want 1, got %d", commitCount)
	}

	q.Session = new(proto.Session)
	rpcVTGate.Begin(context.Background(), q.Session)
	rpcVTGate.ExecuteShard(context.Background(), &q, qr)
	rpcVTGate.Rollback(context.Background(), q.Session)
	/*
		// Flaky: This test should be run manually.
		runtime.Gosched()
		if sbc.RollbackCount != 1 {
			t.Errorf("want 1, got %d", sbc.RollbackCount)
		}
	*/
}

func TestVTGateExecuteKeyspaceIds(t *testing.T) {
	s := createSandbox("TestVTGateExecuteKeyspaceIds")
	sbc1 := &sandboxConn{}
	sbc2 := &sandboxConn{}
	s.MapTestConn("-20", sbc1)
	s.MapTestConn("20-40", sbc2)
	kid10, err := key.HexKeyspaceId("10").Unhex()
	if err != nil {
		t.Errorf("want nil, got %+v", err)
	}
	q := proto.KeyspaceIdQuery{
		Sql:         "query",
		Keyspace:    "TestVTGateExecuteKeyspaceIds",
		KeyspaceIds: []key.KeyspaceId{kid10},
		TabletType:  topo.TYPE_MASTER,
	}
	// Test for successful execution
	qr := new(proto.QueryResult)
	err = rpcVTGate.ExecuteKeyspaceIds(context.Background(), &q, qr)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	wantqr := new(proto.QueryResult)
	wantqr.Result = singleRowResult
	if !reflect.DeepEqual(wantqr, qr) {
		t.Errorf("want \n%+v, got \n%+v", singleRowResult, qr)
	}
	if qr.Session != nil {
		t.Errorf("want nil, got %+v\n", qr.Session)
	}
	if execCount := sbc1.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v\n", execCount)
	}
	// Test for successful execution in transaction
	q.Session = new(proto.Session)
	rpcVTGate.Begin(context.Background(), q.Session)
	if !q.Session.InTransaction {
		t.Errorf("want true, got false")
	}
	rpcVTGate.ExecuteKeyspaceIds(context.Background(), &q, qr)
	wantSession := &proto.Session{
		InTransaction: true,
		ShardSessions: []*proto.ShardSession{{
			Keyspace:      "TestVTGateExecuteKeyspaceIds",
			Shard:         "-20",
			TransactionId: 1,
			TabletType:    topo.TYPE_MASTER,
		}},
	}
	if !reflect.DeepEqual(wantSession, q.Session) {
		t.Errorf("want \n%+v, got \n%+v", wantSession, q.Session)
	}
	rpcVTGate.Commit(context.Background(), q.Session)
	if commitCount := sbc1.CommitCount.Get(); commitCount != 1 {
		t.Errorf("want 1, got %d", commitCount)
	}
	// Test for multiple shards
	kid30, err := key.HexKeyspaceId("30").Unhex()
	if err != nil {
		t.Errorf("want nil, got %+v", err)
	}
	q.KeyspaceIds = []key.KeyspaceId{kid10, kid30}
	rpcVTGate.ExecuteKeyspaceIds(context.Background(), &q, qr)
	if qr.Result.RowsAffected != 2 {
		t.Errorf("want 2, got %v", qr.Result.RowsAffected)
	}
}

func TestVTGateExecuteKeyRanges(t *testing.T) {
	s := createSandbox("TestVTGateExecuteKeyRanges")
	sbc1 := &sandboxConn{}
	sbc2 := &sandboxConn{}
	s.MapTestConn("-20", sbc1)
	s.MapTestConn("20-40", sbc2)
	kr, err := key.ParseKeyRangeParts("", "20")
	q := proto.KeyRangeQuery{
		Sql:        "query",
		Keyspace:   "TestVTGateExecuteKeyRanges",
		KeyRanges:  []key.KeyRange{kr},
		TabletType: topo.TYPE_MASTER,
	}
	// Test for successful execution
	qr := new(proto.QueryResult)
	err = rpcVTGate.ExecuteKeyRanges(context.Background(), &q, qr)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	wantqr := new(proto.QueryResult)
	wantqr.Result = singleRowResult
	if !reflect.DeepEqual(wantqr, qr) {
		t.Errorf("want \n%+v, got \n%+v", singleRowResult, qr)
	}
	if qr.Session != nil {
		t.Errorf("want nil, got %+v\n", qr.Session)
	}
	if execCount := sbc1.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v\n", execCount)
	}
	// Test for successful execution in transaction
	q.Session = new(proto.Session)
	rpcVTGate.Begin(context.Background(), q.Session)
	if !q.Session.InTransaction {
		t.Errorf("want true, got false")
	}
	err = rpcVTGate.ExecuteKeyRanges(context.Background(), &q, qr)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	wantSession := &proto.Session{
		InTransaction: true,
		ShardSessions: []*proto.ShardSession{{
			Keyspace:      "TestVTGateExecuteKeyRanges",
			Shard:         "-20",
			TransactionId: 1,
			TabletType:    topo.TYPE_MASTER,
		}},
	}
	if !reflect.DeepEqual(wantSession, q.Session) {
		t.Errorf("want \n%+v, got \n%+v", wantSession, q.Session)
	}
	rpcVTGate.Commit(context.Background(), q.Session)
	if commitCount := sbc1.CommitCount.Get(); commitCount != 1 {
		t.Errorf("want 1, got %v", commitCount)
	}
	// Test for multiple shards
	kr, err = key.ParseKeyRangeParts("10", "30")
	q.KeyRanges = []key.KeyRange{kr}
	rpcVTGate.ExecuteKeyRanges(context.Background(), &q, qr)
	if qr.Result.RowsAffected != 2 {
		t.Errorf("want 2, got %v", qr.Result.RowsAffected)
	}
}

func TestVTGateExecuteEntityIds(t *testing.T) {
	s := createSandbox("TestVTGateExecuteEntityIds")
	sbc1 := &sandboxConn{}
	sbc2 := &sandboxConn{}
	s.MapTestConn("-20", sbc1)
	s.MapTestConn("20-40", sbc2)
	kid10, err := key.HexKeyspaceId("10").Unhex()
	if err != nil {
		t.Errorf("want nil, got %+v", err)
	}
	q := proto.EntityIdsQuery{
		Sql:              "query",
		Keyspace:         "TestVTGateExecuteEntityIds",
		EntityColumnName: "kid",
		EntityKeyspaceIDs: []proto.EntityId{
			proto.EntityId{
				ExternalID: "id1",
				KeyspaceID: kid10,
			},
		},
		TabletType: topo.TYPE_MASTER,
	}
	// Test for successful execution
	qr := new(proto.QueryResult)
	err = rpcVTGate.ExecuteEntityIds(context.Background(), &q, qr)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	wantqr := new(proto.QueryResult)
	wantqr.Result = singleRowResult
	if !reflect.DeepEqual(wantqr, qr) {
		t.Errorf("want \n%+v, got \n%+v", singleRowResult, qr)
	}
	if qr.Session != nil {
		t.Errorf("want nil, got %+v\n", qr.Session)
	}
	if execCount := sbc1.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v\n", execCount)
	}
	// Test for successful execution in transaction
	q.Session = new(proto.Session)
	rpcVTGate.Begin(context.Background(), q.Session)
	if !q.Session.InTransaction {
		t.Errorf("want true, got false")
	}
	rpcVTGate.ExecuteEntityIds(context.Background(), &q, qr)
	wantSession := &proto.Session{
		InTransaction: true,
		ShardSessions: []*proto.ShardSession{{
			Keyspace:      "TestVTGateExecuteEntityIds",
			Shard:         "-20",
			TransactionId: 1,
			TabletType:    topo.TYPE_MASTER,
		}},
	}
	if !reflect.DeepEqual(wantSession, q.Session) {
		t.Errorf("want \n%+v, got \n%+v", wantSession, q.Session)
	}
	rpcVTGate.Commit(context.Background(), q.Session)
	if commitCount := sbc1.CommitCount.Get(); commitCount != 1 {
		t.Errorf("want 1, got %d", commitCount)
	}
	// Test for multiple shards
	kid30, err := key.HexKeyspaceId("30").Unhex()
	if err != nil {
		t.Errorf("want nil, got %+v", err)
	}
	q.EntityKeyspaceIDs = append(q.EntityKeyspaceIDs, proto.EntityId{ExternalID: "id2", KeyspaceID: kid30})
	rpcVTGate.ExecuteEntityIds(context.Background(), &q, qr)
	if qr.Result.RowsAffected != 2 {
		t.Errorf("want 2, got %v", qr.Result.RowsAffected)
	}
}

func TestVTGateExecuteBatchShard(t *testing.T) {
	s := createSandbox("TestVTGateExecuteBatchShard")
	s.MapTestConn("-20", &sandboxConn{})
	s.MapTestConn("20-40", &sandboxConn{})
	q := proto.BatchQueryShard{
		Queries: []proto.BoundShardQuery{{
			Sql:           "query",
			BindVariables: nil,
			Keyspace:      "TestVTGateExecuteBatchShard",
			Shards:        []string{"-20", "20-40"},
		}, {
			Sql:           "query",
			BindVariables: nil,
			Keyspace:      "TestVTGateExecuteBatchShard",
			Shards:        []string{"-20", "20-40"},
		}},
	}
	qrl := new(proto.QueryResultList)
	err := rpcVTGate.ExecuteBatchShard(context.Background(), &q, qrl)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	if len(qrl.List) != 2 {
		t.Errorf("want 2, got %v", len(qrl.List))
	}
	if qrl.List[0].RowsAffected != 2 {
		t.Errorf("want 2, got %v", qrl.List[0].RowsAffected)
	}
	if qrl.Session != nil {
		t.Errorf("want nil, got %+v\n", qrl.Session)
	}

	q.Session = new(proto.Session)
	rpcVTGate.Begin(context.Background(), q.Session)
	rpcVTGate.ExecuteBatchShard(context.Background(), &q, qrl)
	if len(q.Session.ShardSessions) != 2 {
		t.Errorf("want 2, got %d", len(q.Session.ShardSessions))
	}
}

func TestVTGateExecuteBatchKeyspaceIds(t *testing.T) {
	s := createSandbox("TestVTGateExecuteBatchKeyspaceIds")
	s.MapTestConn("-20", &sandboxConn{})
	s.MapTestConn("20-40", &sandboxConn{})
	kid10, err := key.HexKeyspaceId("10").Unhex()
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	kid30, err := key.HexKeyspaceId("30").Unhex()
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	q := proto.KeyspaceIdBatchQuery{
		Queries: []proto.BoundKeyspaceIdQuery{{
			Sql:           "query",
			BindVariables: nil,
			Keyspace:      "TestVTGateExecuteBatchKeyspaceIds",
			KeyspaceIds:   []key.KeyspaceId{kid10, kid30},
		}, {
			Sql:           "query",
			BindVariables: nil,
			Keyspace:      "TestVTGateExecuteBatchKeyspaceIds",
			KeyspaceIds:   []key.KeyspaceId{kid10, kid30},
		}},
		TabletType: topo.TYPE_MASTER,
	}
	qrl := new(proto.QueryResultList)
	err = rpcVTGate.ExecuteBatchKeyspaceIds(context.Background(), &q, qrl)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	if len(qrl.List) != 2 {
		t.Errorf("want 2, got %v", len(qrl.List))
	}
	if qrl.List[0].RowsAffected != 2 {
		t.Errorf("want 2, got %v", qrl.List[0].RowsAffected)
	}
	if qrl.Session != nil {
		t.Errorf("want nil, got %+v\n", qrl.Session)
	}

	q.Session = new(proto.Session)
	rpcVTGate.Begin(context.Background(), q.Session)
	rpcVTGate.ExecuteBatchKeyspaceIds(context.Background(), &q, qrl)
	if len(q.Session.ShardSessions) != 2 {
		t.Errorf("want 2, got %d", len(q.Session.ShardSessions))
	}
}

func TestVTGateStreamExecute(t *testing.T) {
	sandbox := createSandbox(KsTestUnsharded)
	sbc := &sandboxConn{}
	sandbox.MapTestConn("0", sbc)
	q := proto.Query{
		Sql:        "select * from t1",
		TabletType: topo.TYPE_MASTER,
	}
	var qrs []*proto.QueryResult
	err := rpcVTGate.StreamExecute(context.Background(), &q, func(r *proto.QueryResult) error {
		qrs = append(qrs, r)
		return nil
	})
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	row := new(proto.QueryResult)
	row.Result = singleRowResult
	want := []*proto.QueryResult{row}
	if !reflect.DeepEqual(want, qrs) {
		t.Errorf("want \n%+v, got \n%+v", want, qrs)
	}
}

func TestVTGateStreamExecuteKeyspaceIds(t *testing.T) {
	s := createSandbox("TestVTGateStreamExecuteKeyspaceIds")
	sbc := &sandboxConn{}
	s.MapTestConn("-20", sbc)
	sbc1 := &sandboxConn{}
	s.MapTestConn("20-40", sbc1)
	kid10, err := key.HexKeyspaceId("10").Unhex()
	if err != nil {
		t.Errorf("want nil, got %+v", err)
	}
	sq := proto.KeyspaceIdQuery{
		Sql:         "query",
		Keyspace:    "TestVTGateStreamExecuteKeyspaceIds",
		KeyspaceIds: []key.KeyspaceId{kid10},
		TabletType:  topo.TYPE_MASTER,
	}
	// Test for successful execution
	var qrs []*proto.QueryResult
	err = rpcVTGate.StreamExecuteKeyspaceIds(context.Background(), &sq, func(r *proto.QueryResult) error {
		qrs = append(qrs, r)
		return nil
	})
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	row := new(proto.QueryResult)
	row.Result = singleRowResult
	want := []*proto.QueryResult{row}
	if !reflect.DeepEqual(want, qrs) {
		t.Errorf("want \n%+v, got \n%+v", want, qrs)
	}

	// Test for successful execution in transaction
	sq.Session = new(proto.Session)
	qrs = nil
	rpcVTGate.Begin(context.Background(), sq.Session)
	err = rpcVTGate.StreamExecuteKeyspaceIds(context.Background(), &sq, func(r *proto.QueryResult) error {
		qrs = append(qrs, r)
		return nil
	})
	want = []*proto.QueryResult{
		row,
		&proto.QueryResult{
			Session: &proto.Session{
				InTransaction: true,
				ShardSessions: []*proto.ShardSession{{
					Keyspace:      "TestVTGateStreamExecuteKeyspaceIds",
					Shard:         "-20",
					TransactionId: 1,
					TabletType:    topo.TYPE_MASTER,
				}},
			},
		},
	}
	if !reflect.DeepEqual(want, qrs) {
		t.Errorf("want\n%#v\ngot\n%#v", want, qrs)
	}
	rpcVTGate.Commit(context.Background(), sq.Session)
	if commitCount := sbc.CommitCount.Get(); commitCount != 1 {
		t.Errorf("want 1, got %d", commitCount)
	}
	// Test for successful execution - multiple keyspaceids in single shard
	sq.Session = nil
	qrs = nil
	kid15, err := key.HexKeyspaceId("15").Unhex()
	if err != nil {
		t.Errorf("want nil, got %+v", err)
	}
	sq.KeyspaceIds = []key.KeyspaceId{kid10, kid15}
	err = rpcVTGate.StreamExecuteKeyspaceIds(context.Background(), &sq, func(r *proto.QueryResult) error {
		qrs = append(qrs, r)
		return nil
	})
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	row = new(proto.QueryResult)
	row.Result = singleRowResult
	want = []*proto.QueryResult{row}
	if !reflect.DeepEqual(want, qrs) {
		t.Errorf("want \n%+v, got \n%+v", want, qrs)
	}
	// Test for successful execution - multiple keyspaceids in multiple shards
	kid30, err := key.HexKeyspaceId("30").Unhex()
	if err != nil {
		t.Errorf("want nil, got %+v", err)
	}
	sq.KeyspaceIds = []key.KeyspaceId{kid10, kid30}
	err = rpcVTGate.StreamExecuteKeyspaceIds(context.Background(), &sq, func(r *proto.QueryResult) error {
		qrs = append(qrs, r)
		return nil
	})
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
}

func TestVTGateStreamExecuteKeyRanges(t *testing.T) {
	s := createSandbox("TestVTGateStreamExecuteKeyRanges")
	sbc := &sandboxConn{}
	s.MapTestConn("-20", sbc)
	sbc1 := &sandboxConn{}
	s.MapTestConn("20-40", sbc1)
	kr, err := key.ParseKeyRangeParts("", "20")
	sq := proto.KeyRangeQuery{
		Sql:        "query",
		Keyspace:   "TestVTGateStreamExecuteKeyRanges",
		KeyRanges:  []key.KeyRange{kr},
		TabletType: topo.TYPE_MASTER,
	}
	// Test for successful execution
	var qrs []*proto.QueryResult
	err = rpcVTGate.StreamExecuteKeyRanges(context.Background(), &sq, func(r *proto.QueryResult) error {
		qrs = append(qrs, r)
		return nil
	})
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	row := new(proto.QueryResult)
	row.Result = singleRowResult
	want := []*proto.QueryResult{row}
	if !reflect.DeepEqual(want, qrs) {
		t.Errorf("want \n%+v, got \n%+v", want, qrs)
	}

	sq.Session = new(proto.Session)
	qrs = nil
	rpcVTGate.Begin(context.Background(), sq.Session)
	err = rpcVTGate.StreamExecuteKeyRanges(context.Background(), &sq, func(r *proto.QueryResult) error {
		qrs = append(qrs, r)
		return nil
	})
	want = []*proto.QueryResult{
		row,
		&proto.QueryResult{
			Session: &proto.Session{
				InTransaction: true,
				ShardSessions: []*proto.ShardSession{{
					Keyspace:      "TestVTGateStreamExecuteKeyRanges",
					Shard:         "-20",
					TransactionId: 1,
					TabletType:    topo.TYPE_MASTER,
				}},
			},
		},
	}
	if !reflect.DeepEqual(want, qrs) {
		t.Errorf("want \n%+v, got \n%+v", want, qrs)
	}

	// Test for successful execution - multiple shards
	kr, err = key.ParseKeyRangeParts("10", "40")
	sq.KeyRanges = []key.KeyRange{kr}
	err = rpcVTGate.StreamExecuteKeyRanges(context.Background(), &sq, func(r *proto.QueryResult) error {
		qrs = append(qrs, r)
		return nil
	})
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
}

func TestVTGateStreamExecuteShard(t *testing.T) {
	s := createSandbox("TestVTGateStreamExecuteShard")
	sbc := &sandboxConn{}
	s.MapTestConn("0", sbc)
	q := proto.QueryShard{
		Sql:        "query",
		Keyspace:   "TestVTGateStreamExecuteShard",
		Shards:     []string{"0"},
		TabletType: topo.TYPE_MASTER,
	}
	// Test for successful execution
	var qrs []*proto.QueryResult
	err := rpcVTGate.StreamExecuteShard(context.Background(), &q, func(r *proto.QueryResult) error {
		qrs = append(qrs, r)
		return nil
	})
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	row := new(proto.QueryResult)
	row.Result = singleRowResult
	want := []*proto.QueryResult{row}
	if !reflect.DeepEqual(want, qrs) {
		t.Errorf("want \n%+v, got \n%+v", want, qrs)
	}

	q.Session = new(proto.Session)
	qrs = nil
	rpcVTGate.Begin(context.Background(), q.Session)
	err = rpcVTGate.StreamExecuteShard(context.Background(), &q, func(r *proto.QueryResult) error {
		qrs = append(qrs, r)
		return nil
	})
	want = []*proto.QueryResult{
		row,
		&proto.QueryResult{
			Session: &proto.Session{
				InTransaction: true,
				ShardSessions: []*proto.ShardSession{{
					Keyspace:      "TestVTGateStreamExecuteShard",
					Shard:         "0",
					TransactionId: 1,
					TabletType:    topo.TYPE_MASTER,
				}},
			},
		},
	}
	if !reflect.DeepEqual(want, qrs) {
		t.Errorf("want \n%+v, got \n%+v", want, qrs)
	}
}

func TestVTGateSplitQuery(t *testing.T) {
	keyspace := "TestVTGateSplitQuery"
	keyranges, _ := key.ParseShardingSpec(DefaultShardSpec)
	s := createSandbox(keyspace)
	for _, kr := range keyranges {
		s.MapTestConn(fmt.Sprintf("%s-%s", kr.Start, kr.End), &sandboxConn{})
	}
	sql := "select col1, col2 from table"
	splitCount := 24
	req := proto.SplitQueryRequest{
		Keyspace: keyspace,
		Query: tproto.BoundQuery{
			Sql: sql,
		},
		SplitCount: splitCount,
	}
	result := new(proto.SplitQueryResult)
	err := rpcVTGate.SplitQuery(context.Background(), &req, result)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	_, err = getAllShards(DefaultShardSpec)
	// Total number of splits should be number of shards * splitsPerShard
	if splitCount != len(result.Splits) {
		t.Errorf("wrong number of splits, want \n%+v, got \n%+v", splitCount, len(result.Splits))
	}
	actualSqlsByKeyRange := map[kproto.KeyRange][]string{}
	for _, split := range result.Splits {
		if split.Size != sandboxSQRowCount {
			t.Errorf("wrong split size, want \n%+v, got \n%+v", sandboxSQRowCount, split.Size)
		}
		if split.Query.Keyspace != keyspace {
			t.Errorf("wrong split size, want \n%+v, got \n%+v", keyspace, split.Query.Keyspace)
		}
		if len(split.Query.KeyRanges) != 1 {
			t.Errorf("wrong number of keyranges, want \n%+v, got \n%+v", 1, len(split.Query.KeyRanges))
		}
		if split.Query.TabletType != topo.TYPE_RDONLY {
			t.Errorf("wrong tablet type, want \n%+v, got \n%+v", topo.TYPE_RDONLY, split.Query.TabletType)
		}
		kr := split.Query.KeyRanges[0]
		actualSqlsByKeyRange[kr] = append(actualSqlsByKeyRange[kr], split.Query.Sql)
	}
	expectedSqlsByKeyRange := map[kproto.KeyRange][]string{}
	for _, kr := range keyranges {
		expectedSqlsByKeyRange[kr] = []string{
			"select col1, col2 from table /*split 0 */",
			"select col1, col2 from table /*split 1 */",
			"select col1, col2 from table /*split 2 */",
		}
	}
	if !reflect.DeepEqual(actualSqlsByKeyRange, expectedSqlsByKeyRange) {
		t.Errorf("splits contain the wrong sqls and/or keyranges, got: %v, want: %v", actualSqlsByKeyRange, expectedSqlsByKeyRange)
	}
}

func TestIsErrorCausedByVTGate(t *testing.T) {
	unknownError := fmt.Errorf("unknown error")
	serverError := &tabletconn.ServerError{
		Code: tabletconn.ERR_RETRY,
		Err:  "vttablet: retry: error message",
	}
	shardConnUnknownErr := &ShardConnError{Err: unknownError}
	shardConnServerErr := &ShardConnError{Err: serverError}
	shardConnCancelledErr := &ShardConnError{Err: tabletconn.Cancelled}

	scatterConnErrAllUnknownErrs := &ScatterConnError{
		Errs: []error{unknownError, unknownError, unknownError},
	}
	scatterConnErrMixed := &ScatterConnError{
		Errs: []error{unknownError, shardConnServerErr, shardConnCancelledErr},
	}
	scatterConnErrAllNonVTGateErrs := &ScatterConnError{
		Errs: []error{shardConnServerErr, shardConnServerErr, shardConnCancelledErr},
	}

	inputToWant := map[error]bool{
		unknownError:         true,
		serverError:          false,
		tabletconn.Cancelled: false,
		// OperationalErrors that are not tabletconn.Cancelled might be from VTGate
		tabletconn.ConnClosed: true,
		// Errors wrapped in ShardConnError should get unwrapped
		shardConnUnknownErr:   true,
		shardConnServerErr:    false,
		shardConnCancelledErr: false,
		// We consider a ScatterConnErr with all unknown errors to be from VTGate
		scatterConnErrAllUnknownErrs: true,
		// We consider a ScatterConnErr with a mix of errors to be from VTGate
		scatterConnErrMixed: true,
		// If every error in ScatterConnErr list is caused by external components, we shouldn't
		// consider the error to be from VTGate
		scatterConnErrAllNonVTGateErrs: false,
	}

	for input, want := range inputToWant {
		got := isErrorCausedByVTGate(input)
		if got != want {
			t.Errorf("isErrorCausedByVTGate(%v) => %v, want %v",
				input, got, want)
		}
	}
}
