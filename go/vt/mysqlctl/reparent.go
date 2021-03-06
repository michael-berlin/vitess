// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

/*
This file contains the reparenting methods for mysqlctl.

TODO(alainjobart) Once refactoring is done, remove unused code paths.
*/

import (
	"fmt"
	"time"

	"github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"golang.org/x/net/context"
)

// CreateReparentJournal returns the commands to execute to create
// the _vt.reparent_journal table. It is safe to run these commands
// even if the table already exists.
func CreateReparentJournal() []string {
	return []string{
		"CREATE DATABASE IF NOT EXISTS _vt",
		`CREATE TABLE IF NOT EXISTS _vt.reparent_journal (
  time_created_ns BIGINT UNSIGNED NOT NULL,
  action_name VARCHAR(250) NOT NULL,
  master_alias VARCHAR(32) NOT NULL,
  replication_position VARCHAR(250) DEFAULT NULL,
  PRIMARY KEY (time_created_ns)) ENGINE=InnoDB`}
}

// PopulateReparentJournal returns the SQL command to use to populate
// the _vt.reparent_journal table, as well as the time_created_ns
// value used.
func PopulateReparentJournal(timeCreatedNS int64, actionName, masterAlias string, pos proto.ReplicationPosition) string {
	return fmt.Sprintf("INSERT INTO _vt.reparent_journal "+
		"(time_created_ns, action_name, master_alias, replication_position) "+
		"VALUES (%v, '%v', '%v', '%v')",
		timeCreatedNS, actionName, masterAlias, proto.EncodeReplicationPosition(pos))
}

// queryReparentJournal returns the SQL query to use to query the database
// for a reparent_journal row.
func queryReparentJournal(timeCreatedNS int64) string {
	return fmt.Sprintf("SELECT action_name, master_alias, replication_position FROM _vt.reparent_journal WHERE time_created_ns=%v", timeCreatedNS)
}

// WaitForReparentJournal will wait until the context is done for
// the row in the reparent_journal table.
func (mysqld *Mysqld) WaitForReparentJournal(ctx context.Context, timeCreatedNS int64) error {
	for {
		qr, err := mysqld.FetchSuperQuery(queryReparentJournal(timeCreatedNS))
		if err == nil && len(qr.Rows) == 1 {
			// we have the row, we're done
			return nil
		}

		// wait a little bit, interrupt if context is done
		t := time.After(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t:
		}
	}
}

// DemoteMaster will gracefully demote a master mysql instance to read only.
// If the master is still alive, then we need to demote it gracefully
// make it read-only, flush the writes and get the position
func (mysqld *Mysqld) DemoteMaster() (rp proto.ReplicationPosition, err error) {
	cmds := []string{
		"FLUSH TABLES WITH READ LOCK",
		"UNLOCK TABLES",
	}
	if err = mysqld.ExecuteSuperQueryList(cmds); err != nil {
		return rp, err
	}
	return mysqld.MasterPosition()
}

// PromoteSlave will promote a slave to be the new master.
func (mysqld *Mysqld) PromoteSlave(hookExtraEnv map[string]string) (proto.ReplicationPosition, error) {
	// we handle replication, just stop it
	cmds := []string{SQLStopSlave}

	// Promote to master.
	flavor, err := mysqld.flavor()
	if err != nil {
		err = fmt.Errorf("PromoteSlave needs flavor: %v", err)
		return proto.ReplicationPosition{}, err
	}
	cmds = append(cmds, flavor.PromoteSlaveCommands()...)
	if err := mysqld.ExecuteSuperQueryList(cmds); err != nil {
		return proto.ReplicationPosition{}, err
	}

	rp, err := mysqld.MasterPosition()
	if err != nil {
		return proto.ReplicationPosition{}, err
	}

	return rp, nil
}
