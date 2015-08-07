// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package etcdtopo

import (
	"encoding/json"
	"fmt"

	"github.com/youtube/vitess/go/event"
	"github.com/youtube/vitess/go/jscfg"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/events"
	"golang.org/x/net/context"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// CreateShard implements topo.Server.
func (s *Server) CreateShard(ctx context.Context, keyspace, shard string, value *pb.Shard) error {
	data := jscfg.ToJSON(value)
	global := s.getGlobal()

	resp, err := global.Create(shardFilePath(keyspace, shard), data, 0 /* ttl */)
	if err != nil {
		return convertError(err)
	}
	if err := initLockFile(global, shardDirPath(keyspace, shard)); err != nil {
		return err
	}

	// We don't return ErrBadResponse in this case because the Create() suceeeded
	// and we don't really need the version to satisfy our contract - we're only
	// logging it.
	version := int64(-1)
	if resp.Node != nil {
		version = int64(resp.Node.ModifiedIndex)
	}
	event.Dispatch(&events.ShardChange{
		ShardInfo: *topo.NewShardInfo(keyspace, shard, value, version),
		Status:    "created",
	})
	return nil
}

// UpdateShard implements topo.Server.
func (s *Server) UpdateShard(ctx context.Context, si *topo.ShardInfo, existingVersion int64) (int64, error) {
	data := jscfg.ToJSON(si.Shard)

	resp, err := s.getGlobal().CompareAndSwap(shardFilePath(si.Keyspace(), si.ShardName()),
		data, 0 /* ttl */, "" /* prevValue */, uint64(existingVersion))
	if err != nil {
		return -1, convertError(err)
	}
	if resp.Node == nil {
		return -1, ErrBadResponse
	}

	event.Dispatch(&events.ShardChange{
		ShardInfo: *si,
		Status:    "updated",
	})
	return int64(resp.Node.ModifiedIndex), nil
}

// ValidateShard implements topo.Server.
func (s *Server) ValidateShard(ctx context.Context, keyspace, shard string) error {
	_, err := s.GetShard(ctx, keyspace, shard)
	return err
}

// GetShard implements topo.Server.
func (s *Server) GetShard(ctx context.Context, keyspace, shard string) (*topo.ShardInfo, error) {
	resp, err := s.getGlobal().Get(shardFilePath(keyspace, shard), false /* sort */, false /* recursive */)
	if err != nil {
		return nil, convertError(err)
	}
	if resp.Node == nil {
		return nil, ErrBadResponse
	}

	value := &pb.Shard{}
	if err := json.Unmarshal([]byte(resp.Node.Value), value); err != nil {
		return nil, fmt.Errorf("bad shard data (%v): %q", err, resp.Node.Value)
	}

	return topo.NewShardInfo(keyspace, shard, value, int64(resp.Node.ModifiedIndex)), nil
}

// GetShardNames implements topo.Server.
func (s *Server) GetShardNames(ctx context.Context, keyspace string) ([]string, error) {
	resp, err := s.getGlobal().Get(shardsDirPath(keyspace), true /* sort */, false /* recursive */)
	if err != nil {
		return nil, convertError(err)
	}
	return getNodeNames(resp)
}

// DeleteShard implements topo.Server.
func (s *Server) DeleteShard(ctx context.Context, keyspace, shard string) error {
	_, err := s.getGlobal().Delete(shardDirPath(keyspace, shard), true /* recursive */)
	if err != nil {
		return convertError(err)
	}

	event.Dispatch(&events.ShardChange{
		ShardInfo: *topo.NewShardInfo(keyspace, shard, nil, -1),
		Status:    "deleted",
	})
	return nil
}
