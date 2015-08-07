// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package vtgate provides query routing rpc services
// for vttablets.
package vtgate

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	"golang.org/x/net/context"
)

// This file uses the sandbox_test framework.

func TestResolverExecuteKeyspaceIds(t *testing.T) {
	testResolverGeneric(t, "TestResolverExecuteKeyspaceIds", func() (*mproto.QueryResult, error) {
		kid10, err := key.HexKeyspaceId("10").Unhex()
		if err != nil {
			return nil, err
		}
		kid25, err := key.HexKeyspaceId("25").Unhex()
		if err != nil {
			return nil, err
		}
		query := &proto.KeyspaceIdQuery{
			Sql:         "query",
			Keyspace:    "TestResolverExecuteKeyspaceIds",
			KeyspaceIds: []key.KeyspaceId{kid10, kid25},
			TabletType:  topo.TYPE_MASTER,
		}
		res := NewResolver(new(sandboxTopo), "", "aa", 1*time.Millisecond, 0, 2*time.Millisecond, 1*time.Millisecond, 24*time.Hour)
		return res.ExecuteKeyspaceIds(context.Background(), query)
	})
}

func TestResolverExecuteKeyRanges(t *testing.T) {
	testResolverGeneric(t, "TestResolverExecuteKeyRanges", func() (*mproto.QueryResult, error) {
		kid10, err := key.HexKeyspaceId("10").Unhex()
		if err != nil {
			return nil, err
		}
		kid25, err := key.HexKeyspaceId("25").Unhex()
		if err != nil {
			return nil, err
		}
		query := &proto.KeyRangeQuery{
			Sql:        "query",
			Keyspace:   "TestResolverExecuteKeyRanges",
			KeyRanges:  []key.KeyRange{key.KeyRange{Start: kid10, End: kid25}},
			TabletType: topo.TYPE_MASTER,
		}
		res := NewResolver(new(sandboxTopo), "", "aa", 1*time.Millisecond, 0, 2*time.Millisecond, 1*time.Millisecond, 24*time.Hour)
		return res.ExecuteKeyRanges(context.Background(), query)
	})
}

func TestResolverExecuteEntityIds(t *testing.T) {
	testResolverGeneric(t, "TestResolverExecuteEntityIds", func() (*mproto.QueryResult, error) {
		kid10, err := key.HexKeyspaceId("10").Unhex()
		if err != nil {
			return nil, err
		}
		kid25, err := key.HexKeyspaceId("25").Unhex()
		if err != nil {
			return nil, err
		}
		query := &proto.EntityIdsQuery{
			Sql:              "query",
			Keyspace:         "TestResolverExecuteEntityIds",
			EntityColumnName: "col",
			EntityKeyspaceIDs: []proto.EntityId{
				proto.EntityId{
					ExternalID: 0,
					KeyspaceID: kid10,
				},
				proto.EntityId{
					ExternalID: "1",
					KeyspaceID: kid25,
				},
			},
			TabletType: topo.TYPE_MASTER,
		}
		res := NewResolver(new(sandboxTopo), "", "aa", 1*time.Millisecond, 0, 2*time.Millisecond, 1*time.Millisecond, 24*time.Hour)
		return res.ExecuteEntityIds(context.Background(), query)
	})
}

func TestResolverExecuteBatchKeyspaceIds(t *testing.T) {
	testResolverGeneric(t, "TestResolverExecuteBatchKeyspaceIds", func() (*mproto.QueryResult, error) {
		kid10, err := key.HexKeyspaceId("10").Unhex()
		if err != nil {
			return nil, err
		}
		kid25, err := key.HexKeyspaceId("25").Unhex()
		if err != nil {
			return nil, err
		}
		query := &proto.KeyspaceIdBatchQuery{
			Queries: []proto.BoundKeyspaceIdQuery{{
				Sql:           "query",
				BindVariables: nil,
				Keyspace:      "TestResolverExecuteBatchKeyspaceIds",
				KeyspaceIds:   []key.KeyspaceId{kid10, kid25},
			}},
			TabletType:    topo.TYPE_MASTER,
			AsTransaction: false,
		}
		res := NewResolver(new(sandboxTopo), "", "aa", 1*time.Millisecond, 0, 2*time.Millisecond, 1*time.Millisecond, 24*time.Hour)
		qrs, err := res.ExecuteBatchKeyspaceIds(context.Background(), query)
		if err != nil {
			return nil, err
		}
		return &qrs.List[0], err
	})
}

func TestResolverStreamExecuteKeyspaceIds(t *testing.T) {
	kid10, err := key.HexKeyspaceId("10").Unhex()
	if err != nil {
		t.Errorf(err.Error())
	}
	kid15, err := key.HexKeyspaceId("15").Unhex()
	if err != nil {
		t.Errorf(err.Error())
	}
	kid25, err := key.HexKeyspaceId("25").Unhex()
	if err != nil {
		t.Errorf(err.Error())
	}
	query := &proto.KeyspaceIdQuery{
		Sql:         "query",
		Keyspace:    "TestResolverStreamExecuteKeyspaceIds",
		KeyspaceIds: []key.KeyspaceId{kid10, kid15},
		TabletType:  topo.TYPE_MASTER,
	}
	createSandbox("TestResolverStreamExecuteKeyspaceIds")
	testResolverStreamGeneric(t, "TestResolverStreamExecuteKeyspaceIds", func() (*mproto.QueryResult, error) {
		res := NewResolver(new(sandboxTopo), "", "aa", 1*time.Millisecond, 0, 2*time.Millisecond, 1*time.Millisecond, 24*time.Hour)
		qr := new(mproto.QueryResult)
		err = res.StreamExecuteKeyspaceIds(context.Background(), query, func(r *mproto.QueryResult) error {
			appendResult(qr, r)
			return nil
		})
		return qr, err
	})
	testResolverStreamGeneric(t, "TestResolverStreamExecuteKeyspaceIds", func() (*mproto.QueryResult, error) {
		query.KeyspaceIds = []key.KeyspaceId{kid10, kid15, kid25}
		res := NewResolver(new(sandboxTopo), "", "aa", 1*time.Millisecond, 0, 2*time.Millisecond, 1*time.Millisecond, 24*time.Hour)
		qr := new(mproto.QueryResult)
		err = res.StreamExecuteKeyspaceIds(context.Background(), query, func(r *mproto.QueryResult) error {
			appendResult(qr, r)
			return nil
		})
		return qr, err
	})
}

func TestResolverStreamExecuteKeyRanges(t *testing.T) {
	kid10, err := key.HexKeyspaceId("10").Unhex()
	if err != nil {
		t.Errorf(err.Error())
	}
	kid15, err := key.HexKeyspaceId("15").Unhex()
	if err != nil {
		t.Errorf(err.Error())
	}
	kid25, err := key.HexKeyspaceId("25").Unhex()
	if err != nil {
		t.Errorf(err.Error())
	}
	query := &proto.KeyRangeQuery{
		Sql:        "query",
		Keyspace:   "TestResolverStreamExecuteKeyRanges",
		KeyRanges:  []key.KeyRange{key.KeyRange{Start: kid10, End: kid15}},
		TabletType: topo.TYPE_MASTER,
	}
	createSandbox("TestResolverStreamExecuteKeyRanges")
	// streaming a single shard
	testResolverStreamGeneric(t, "TestResolverStreamExecuteKeyRanges", func() (*mproto.QueryResult, error) {
		res := NewResolver(new(sandboxTopo), "", "aa", 1*time.Millisecond, 0, 2*time.Millisecond, 1*time.Millisecond, 24*time.Hour)
		qr := new(mproto.QueryResult)
		err = res.StreamExecuteKeyRanges(context.Background(), query, func(r *mproto.QueryResult) error {
			appendResult(qr, r)
			return nil
		})
		return qr, err
	})
	// streaming multiple shards
	testResolverStreamGeneric(t, "TestResolverStreamExecuteKeyRanges", func() (*mproto.QueryResult, error) {
		query.KeyRanges = []key.KeyRange{key.KeyRange{Start: kid10, End: kid25}}
		res := NewResolver(new(sandboxTopo), "", "aa", 1*time.Millisecond, 0, 2*time.Millisecond, 1*time.Millisecond, 24*time.Hour)
		qr := new(mproto.QueryResult)
		err = res.StreamExecuteKeyRanges(context.Background(), query, func(r *mproto.QueryResult) error {
			appendResult(qr, r)
			return nil
		})
		return qr, err
	})
}

func testResolverGeneric(t *testing.T, name string, action func() (*mproto.QueryResult, error)) {
	// successful execute
	s := createSandbox(name)
	sbc0 := &sandboxConn{}
	s.MapTestConn("-20", sbc0)
	sbc1 := &sandboxConn{}
	s.MapTestConn("20-40", sbc1)
	_, err := action()
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	if execCount := sbc0.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v", execCount)
	}
	if execCount := sbc1.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v", execCount)
	}

	// non-retryable failure
	s.Reset()
	sbc0 = &sandboxConn{mustFailServer: 1}
	s.MapTestConn("-20", sbc0)
	sbc1 = &sandboxConn{mustFailRetry: 1}
	s.MapTestConn("20-40", sbc1)
	_, err = action()
	want1 := fmt.Sprintf("shard, host: %s.-20.master, host:\"-20\" port_map:<key:\"vt\" value:1 > , error: err", name)
	want2 := fmt.Sprintf("shard, host: %s.20-40.master, host:\"20-40\" port_map:<key:\"vt\" value:1 > , retry: err", name)
	want := []string{want1, want2}
	sort.Strings(want)
	if err == nil {
		t.Errorf("want\n%v\ngot\n%v", want, err)
	} else {
		got := strings.Split(err.Error(), "\n")
		sort.Strings(got)
		if !reflect.DeepEqual(want, got) {
			t.Errorf("want\n%v\ngot\n%v", want, got)
		}
	}
	// Ensure that we tried only once
	if execCount := sbc0.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v", execCount)
	}
	if execCount := sbc1.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v", execCount)
	}
	// Ensure that we tried topo only once when mapping KeyspaceId/KeyRange to shards
	if s.SrvKeyspaceCounter != 1 {
		t.Errorf("want 1, got %v", s.SrvKeyspaceCounter)
	}

	// retryable failure, no sharding event
	s.Reset()
	sbc0 = &sandboxConn{mustFailRetry: 1}
	s.MapTestConn("-20", sbc0)
	sbc1 = &sandboxConn{mustFailFatal: 1}
	s.MapTestConn("20-40", sbc1)
	_, err = action()
	want1 = fmt.Sprintf("shard, host: %s.-20.master, host:\"-20\" port_map:<key:\"vt\" value:1 > , retry: err", name)
	want2 = fmt.Sprintf("shard, host: %s.20-40.master, host:\"20-40\" port_map:<key:\"vt\" value:1 > , fatal: err", name)
	want = []string{want1, want2}
	sort.Strings(want)
	if err == nil {
		t.Errorf("want\n%v\ngot\n%v", want, err)
	} else {
		got := strings.Split(err.Error(), "\n")
		sort.Strings(got)
		if !reflect.DeepEqual(want, got) {
			t.Errorf("want\n%v\ngot\n%v", want, got)
		}
	}
	// Ensure that we tried only once.
	if execCount := sbc0.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v", execCount)
	}
	if execCount := sbc1.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v", execCount)
	}
	// Ensure that we tried topo only twice.
	if s.SrvKeyspaceCounter != 2 {
		t.Errorf("want 2, got %v", s.SrvKeyspaceCounter)
	}

	// no failure, initial vertical resharding
	s.Reset()
	addSandboxServedFrom(name, name+"ServedFrom0")
	sbc0 = &sandboxConn{}
	s.MapTestConn("-20", sbc0)
	sbc1 = &sandboxConn{}
	s.MapTestConn("20-40", sbc1)
	s0 := createSandbox(name + "ServedFrom0") // make sure we have a fresh copy
	s0.ShardSpec = "-80-"
	sbc2 := &sandboxConn{}
	s0.MapTestConn("-80", sbc2)
	_, err = action()
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	// Ensure original keyspace is not used.
	if execCount := sbc0.ExecCount.Get(); execCount != 0 {
		t.Errorf("want 0, got %v", execCount)
	}
	if execCount := sbc1.ExecCount.Get(); execCount != 0 {
		t.Errorf("want 0, got %v", execCount)
	}
	// Ensure redirected keyspace is accessed once.
	if execCount := sbc2.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v", execCount)
	}
	// Ensure that we tried each keyspace only once.
	if s.SrvKeyspaceCounter != 1 {
		t.Errorf("want 1, got %v", s.SrvKeyspaceCounter)
	}
	if s0.SrvKeyspaceCounter != 1 {
		t.Errorf("want 1, got %v", s0.SrvKeyspaceCounter)
	}
	s0.Reset()

	// retryable failure, vertical resharding
	s.Reset()
	sbc0 = &sandboxConn{}
	s.MapTestConn("-20", sbc0)
	sbc1 = &sandboxConn{mustFailFatal: 1}
	s.MapTestConn("20-40", sbc1)
	i := 0
	s.SrvKeyspaceCallback = func() {
		if i == 1 {
			addSandboxServedFrom(name, name+"ServedFrom")
		}
		i++
	}
	_, err = action()
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	// Ensure that we tried only twice.
	if execCount := sbc0.ExecCount.Get(); execCount != 2 {
		t.Errorf("want 2, got %v", execCount)
	}
	if execCount := sbc1.ExecCount.Get(); execCount != 2 {
		t.Errorf("want 2, got %v", execCount)
	}
	// Ensure that we tried topo only 3 times.
	if s.SrvKeyspaceCounter != 3 {
		t.Errorf("want 3, got %v", s.SrvKeyspaceCounter)
	}

	// retryable failure, horizontal resharding
	s.Reset()
	sbc0 = &sandboxConn{}
	s.MapTestConn("-20", sbc0)
	sbc1 = &sandboxConn{mustFailRetry: 1}
	s.MapTestConn("20-40", sbc1)
	i = 0
	s.SrvKeyspaceCallback = func() {
		if i == 1 {
			s.ShardSpec = "-20-30-40-60-80-a0-c0-e0-"
			s.MapTestConn("-20", sbc0)
			s.MapTestConn("20-30", sbc1)
		}
		i++
	}
	_, err = action()
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	// Ensure that we tried only twice.
	if execCount := sbc0.ExecCount.Get(); execCount != 2 {
		t.Errorf("want 2, got %v", execCount)
	}
	if execCount := sbc1.ExecCount.Get(); execCount != 2 {
		t.Errorf("want 2, got %v", execCount)
	}
	// Ensure that we tried topo only twice.
	if s.SrvKeyspaceCounter != 2 {
		t.Errorf("want 2, got %v", s.SrvKeyspaceCounter)
	}
}

func testResolverStreamGeneric(t *testing.T, name string, action func() (*mproto.QueryResult, error)) {
	// successful execute
	s := createSandbox(name)
	sbc0 := &sandboxConn{}
	s.MapTestConn("-20", sbc0)
	sbc1 := &sandboxConn{}
	s.MapTestConn("20-40", sbc1)
	_, err := action()
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	if execCount := sbc0.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v", execCount)
	}

	// failure
	s.Reset()
	sbc0 = &sandboxConn{mustFailRetry: 1}
	s.MapTestConn("-20", sbc0)
	sbc1 = &sandboxConn{}
	s.MapTestConn("20-40", sbc1)
	_, err = action()
	want := fmt.Sprintf("shard, host: %s.-20.master, host:\"-20\" port_map:<key:\"vt\" value:1 > , retry: err", name)
	if err == nil || err.Error() != want {
		t.Errorf("want\n%s\ngot\n%v", want, err)
	}
	// Ensure that we tried only once.
	if execCount := sbc0.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v", execCount)
	}
	// Ensure that we tried topo only once
	if s.SrvKeyspaceCounter != 1 {
		t.Errorf("want 1, got %v", s.SrvKeyspaceCounter)
	}
}

func TestResolverInsertSqlClause(t *testing.T) {
	clause := "col in (:col1, :col2)"
	tests := [][]string{
		[]string{
			"select a from table",
			"select a from table where " + clause},
		[]string{
			"select a from table where id = 1",
			"select a from table where id = 1 and " + clause},
		[]string{
			"select a from table group by a",
			"select a from table where " + clause + " group by a"},
		[]string{
			"select a from table where id = 1 order by a limit 10",
			"select a from table where id = 1 and " + clause + " order by a limit 10"},
		[]string{
			"select a from table where id = 1 for update",
			"select a from table where id = 1 and " + clause + " for update"},
	}
	for _, test := range tests {
		got := insertSQLClause(test[0], clause)
		if got != test[1] {
			t.Errorf("want '%v', got '%v'", test[1], got)
		}
	}
}

func TestResolverBuildEntityIds(t *testing.T) {
	shardMap := make(map[string][]interface{})
	shardMap["-20"] = []interface{}{"0", 1}
	shardMap["20-40"] = []interface{}{"2"}
	sql := "select a from table where id=:id"
	entityColName := "uid"
	bindVar := make(map[string]interface{})
	bindVar["id"] = 10
	shards, sqls, bindVars := buildEntityIds(shardMap, sql, entityColName, bindVar)
	wantShards := []string{"-20", "20-40"}
	wantSqls := map[string]string{
		"-20":   "select a from table where id=:id and uid in (:uid0, :uid1)",
		"20-40": "select a from table where id=:id and uid in (:uid0)",
	}
	wantBindVars := map[string]map[string]interface{}{
		"-20":   map[string]interface{}{"id": 10, "uid0": "0", "uid1": 1},
		"20-40": map[string]interface{}{"id": 10, "uid0": "2"},
	}
	sort.Strings(wantShards)
	sort.Strings(shards)
	if !reflect.DeepEqual(wantShards, shards) {
		t.Errorf("want %+v, got %+v", wantShards, shards)
	}
	if !reflect.DeepEqual(wantSqls, sqls) {
		t.Errorf("want %+v, got %+v", wantSqls, sqls)
	}
	if !reflect.DeepEqual(wantBindVars, bindVars) {
		t.Errorf("want %+v, got %+v", wantBindVars, bindVars)
	}
}

func TestResolverDmlOnMultipleKeyspaceIds(t *testing.T) {
	kid10, err := key.HexKeyspaceId("10").Unhex()
	if err != nil {
		t.Errorf("Error encoding keyspace id")
	}
	kid25, err := key.HexKeyspaceId("25").Unhex()
	if err != nil {
		t.Errorf("Error encoding keyspace id")
	}
	query := &proto.KeyspaceIdQuery{
		Sql:         "update table set a = b",
		Keyspace:    "TestResolverExecuteKeyspaceIds",
		KeyspaceIds: []key.KeyspaceId{kid10, kid25},
		TabletType:  topo.TYPE_MASTER,
	}
	res := NewResolver(new(sandboxTopo), "", "aa", 1*time.Millisecond, 0, 2*time.Millisecond, 1*time.Millisecond, 24*time.Hour)

	s := createSandbox("TestResolverDmlOnMultipleKeyspaceIds")
	sbc0 := &sandboxConn{}
	s.MapTestConn("-20", sbc0)
	sbc1 := &sandboxConn{}
	s.MapTestConn("20-40", sbc1)

	errStr := "DML should not span multiple keyspace_ids"
	_, err = res.ExecuteKeyspaceIds(context.Background(), query)
	if err == nil {
		t.Errorf("want %v, got nil", errStr)
	}
}

func TestResolverExecBatchAsTransaction(t *testing.T) {
	s := createSandbox(KsTestUnshardedServedFrom)
	sbc := &sandboxConn{mustFailRetry: 20}
	s.MapTestConn("0", sbc)

	res := NewResolver(new(sandboxTopo), "", "aa", 1*time.Millisecond, 2, 2*time.Millisecond, 1*time.Millisecond, 24*time.Hour)

	callcount := 0
	buildBatchRequest := func() (*scatterBatchRequest, error) {
		callcount++
		queries := []proto.BoundShardQuery{{
			Sql:           "query",
			BindVariables: nil,
			Keyspace:      KsTestUnshardedServedFrom,
			Shards:        []string{"0"},
		}}
		return boundShardQueriesToScatterBatchRequest(queries), nil
	}

	_, err := res.ExecuteBatch(context.Background(), topo.TYPE_MASTER, false, nil, buildBatchRequest)
	if err == nil {
		t.Errorf("want got, got none")
	}
	// Ensure scatter tried a re-resolve
	if callcount != 2 {
		t.Errorf("want 2, got %v", callcount)
	}
	if count := sbc.AsTransactionCount.Get(); count != 0 {
		t.Errorf("want 0, got %v", count)
	}

	callcount = 0
	_, err = res.ExecuteBatch(context.Background(), topo.TYPE_MASTER, true, nil, buildBatchRequest)
	if err == nil {
		t.Errorf("want got, got none")
	}
	// Ensure scatter did not re-resolve
	if callcount != 1 {
		t.Errorf("want 1, got %v", callcount)
	}
	if count := sbc.AsTransactionCount.Get(); count != 1 {
		t.Errorf("want 1, got %v", count)
	}
}

func TestIsConnError(t *testing.T) {
	var connErrorTests = []struct {
		in      error
		outCode int
		outBool bool
	}{
		{fmt.Errorf("generic error"), 0, false},
		{&ScatterConnError{Code: 9}, 9, true},
		{&ShardConnError{Code: 9}, 9, true},
		{&tabletconn.ServerError{Code: 9}, 0, false},
	}

	for _, tt := range connErrorTests {
		gotCode, gotBool := isConnError(tt.in)
		if (gotCode != tt.outCode) || (gotBool != tt.outBool) {
			t.Errorf("isConnError(%v) => (%v, %v), want (%v, %v)",
				tt.in, gotCode, gotBool, tt.outCode, tt.outBool)
		}
	}
}
