// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package testlib

import (
	"fmt"
	"testing"
	"time"

	"github.com/youtube/vitess/go/event"
	"github.com/youtube/vitess/go/vt/topotools/events"
)

//func TestTabletExternallyReparented(t *testing.T) {
//	tabletmanager.SetReparentFlags(time.Minute /* finalizeTimeout */)
//
//	ctx := context.Background()
//	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
//	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient(), time.Second)
//	vp := NewVtctlPipe(t, ts)
//	defer vp.Close()
//
//	// Create an old master, a new master, two good slaves, one bad slave
//	oldMaster := NewFakeTablet(t, wr, "cell1", 0, topo.TYPE_MASTER)
//	newMaster := NewFakeTablet(t, wr, "cell1", 1, topo.TYPE_REPLICA)
//	goodSlave1 := NewFakeTablet(t, wr, "cell1", 2, topo.TYPE_REPLICA)
//	goodSlave2 := NewFakeTablet(t, wr, "cell2", 3, topo.TYPE_REPLICA)
//	badSlave := NewFakeTablet(t, wr, "cell1", 4, topo.TYPE_REPLICA)
//
//	// Add a new Cell to the Shard, that doesn't map to any read topo cell,
//	// to simulate a data center being unreachable.
//	si, err := ts.GetShard(ctx, "test_keyspace", "0")
//	if err != nil {
//		t.Fatalf("GetShard failed: %v", err)
//	}
//	si.Cells = append(si.Cells, "cell666")
//	if err := topo.UpdateShard(ctx, ts, si); err != nil {
//		t.Fatalf("UpdateShard failed: %v", err)
//	}
//
//	// Slightly unrelated test: make sure we can find the tablets
//	// even with a datacenter being down.
//	tabletMap, err := topo.GetTabletMapForShardByCell(ctx, ts, "test_keyspace", "0", []string{"cell1"})
//	if err != nil {
//		t.Fatalf("GetTabletMapForShardByCell should have worked but got: %v", err)
//	}
//	master, err := topotools.FindTabletByIPAddrAndPort(tabletMap, oldMaster.Tablet.IPAddr, "vt", oldMaster.Tablet.Portmap["vt"])
//	if err != nil || master != oldMaster.Tablet.Alias {
//		t.Fatalf("FindTabletByIPAddrAndPort(master) failed: %v %v", err, master)
//	}
//	slave1, err := topotools.FindTabletByIPAddrAndPort(tabletMap, goodSlave1.Tablet.IPAddr, "vt", goodSlave1.Tablet.Portmap["vt"])
//	if err != nil || slave1 != goodSlave1.Tablet.Alias {
//		t.Fatalf("FindTabletByIPAddrAndPort(slave1) failed: %v %v", err, master)
//	}
//	slave2, err := topotools.FindTabletByIPAddrAndPort(tabletMap, goodSlave2.Tablet.IPAddr, "vt", goodSlave2.Tablet.Portmap["vt"])
//	if err != topo.ErrNoNode {
//		t.Fatalf("FindTabletByIPAddrAndPort(slave2) worked: %v %v", err, slave2)
//	}
//
//	// Make sure the master is not exported in other cells
//	tabletMap, err = topo.GetTabletMapForShardByCell(ctx, ts, "test_keyspace", "0", []string{"cell2"})
//	master, err = topotools.FindTabletByIPAddrAndPort(tabletMap, oldMaster.Tablet.IPAddr, "vt", oldMaster.Tablet.Portmap["vt"])
//	if err != topo.ErrNoNode {
//		t.Fatalf("FindTabletByIPAddrAndPort(master) worked in cell2: %v %v", err, master)
//	}
//
//	tabletMap, err = topo.GetTabletMapForShard(ctx, ts, "test_keyspace", "0")
//	if err != topo.ErrPartialResult {
//		t.Fatalf("GetTabletMapForShard should have returned ErrPartialResult but got: %v", err)
//	}
//	master, err = topotools.FindTabletByIPAddrAndPort(tabletMap, oldMaster.Tablet.IPAddr, "vt", oldMaster.Tablet.Portmap["vt"])
//	if err != nil || master != oldMaster.Tablet.Alias {
//		t.Fatalf("FindTabletByIPAddrAndPort(master) failed: %v %v", err, master)
//	}
//
//	// On the elected master, we will respond to
//	// TabletActionSlaveWasPromoted
//	newMaster.StartActionLoop(t, wr)
//	defer newMaster.StopActionLoop(t)
//
//	// On the old master, we will only respond to
//	// TabletActionSlaveWasRestarted.
//	oldMaster.StartActionLoop(t, wr)
//	defer oldMaster.StopActionLoop(t)
//
//	// On the good slaves, we will respond to
//	// TabletActionSlaveWasRestarted.
//	goodSlave1.StartActionLoop(t, wr)
//	defer goodSlave1.StopActionLoop(t)
//
//	goodSlave2.StartActionLoop(t, wr)
//	defer goodSlave2.StopActionLoop(t)
//
//	// On the bad slave, we will respond to
//	// TabletActionSlaveWasRestarted with bad data.
//	badSlave.StartActionLoop(t, wr)
//	defer badSlave.StopActionLoop(t)
//
//	// First test: reparent to the same master, make sure it works
//	// as expected.
//	tmc := tmclient.NewTabletManagerClient()
//	ti, err := ts.GetTablet(ctx, oldMaster.Tablet.Alias)
//	if err != nil {
//		t.Fatalf("GetTablet failed: %v", err)
//	}
//	if err := vp.Run([]string{"TabletExternallyReparented", oldMaster.Tablet.Alias.String()}); err != nil {
//		t.Fatalf("TabletExternallyReparented(same master) should have worked")
//	}
//
//	// Second test: reparent to a replica, and pretend the old
//	// master is still good to go.
//
//	// This tests a bad case; the new designated master is a slave,
//	// but we should do what we're told anyway
//	ti, err = ts.GetTablet(ctx, goodSlave1.Tablet.Alias)
//	if err != nil {
//		t.Fatalf("GetTablet failed: %v", err)
//	}
//	if err := tmc.TabletExternallyReparented(context.Background(), ti, ""); err != nil {
//		t.Fatalf("TabletExternallyReparented(slave) error: %v", err)
//	}
//
//	// This tests the good case, where everything works as planned
//	t.Logf("TabletExternallyReparented(new master) expecting success")
//	ti, err = ts.GetTablet(ctx, newMaster.Tablet.Alias)
//	if err != nil {
//		t.Fatalf("GetTablet failed: %v", err)
//	}
//	waitID := makeWaitID()
//	if err := tmc.TabletExternallyReparented(context.Background(), ti, waitID); err != nil {
//		t.Fatalf("TabletExternallyReparented(replica) failed: %v", err)
//	}
//	waitForExternalReparent(t, waitID)
//
//	// Now double-check the serving graph is good.
//	// Should only have one good replica left.
//	addrs, _, err := ts.GetEndPoints(ctx, "cell1", "test_keyspace", "0", topo.TYPE_REPLICA)
//	if err != nil {
//		t.Fatalf("GetEndPoints failed at the end: %v", err)
//	}
//	if len(addrs.Entries) != 1 {
//		t.Fatalf("GetEndPoints has too many entries: %v", addrs)
//	}
//}
//
//// TestTabletExternallyReparentedWithDifferentMysqlPort makes sure
//// that if mysql is restarted on the master-elect tablet and has a different
//// port, we pick it up correctly.
//func TestTabletExternallyReparentedWithDifferentMysqlPort(t *testing.T) {
//	tabletmanager.SetReparentFlags(time.Minute /* finalizeTimeout */)
//
//	ctx := context.Background()
//	ts := zktopo.NewTestServer(t, []string{"cell1"})
//	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient(), time.Second)
//
//	// Create an old master, a new master, two good slaves, one bad slave
//	oldMaster := NewFakeTablet(t, wr, "cell1", 0, topo.TYPE_MASTER)
//	newMaster := NewFakeTablet(t, wr, "cell1", 1, topo.TYPE_REPLICA)
//	goodSlave := NewFakeTablet(t, wr, "cell1", 2, topo.TYPE_REPLICA)
//
//	// Now we're restarting mysql on a different port, 3301->3303
//	// but without updating the Tablet record in topology.
//
//	// On the elected master, we will respond to
//	// TabletActionSlaveWasPromoted, so we need a MysqlDaemon
//	// that returns no master, and the new port (as returned by mysql)
//	newMaster.FakeMysqlDaemon.MysqlPort = 3303
//	newMaster.StartActionLoop(t, wr)
//	defer newMaster.StopActionLoop(t)
//
//	// On the old master, we will only respond to
//	// TabletActionSlaveWasRestarted and point to the new mysql port
//	oldMaster.StartActionLoop(t, wr)
//	defer oldMaster.StopActionLoop(t)
//
//	// On the good slaves, we will respond to
//	// TabletActionSlaveWasRestarted and point to the new mysql port
//	goodSlave.StartActionLoop(t, wr)
//	defer goodSlave.StopActionLoop(t)
//
//	// This tests the good case, where everything works as planned
//	t.Logf("TabletExternallyReparented(new master) expecting success")
//	tmc := tmclient.NewTabletManagerClient()
//	ti, err := ts.GetTablet(ctx, newMaster.Tablet.Alias)
//	if err != nil {
//		t.Fatalf("GetTablet failed: %v", err)
//	}
//	if err := tmc.TabletExternallyReparented(context.Background(), ti, ""); err != nil {
//		t.Fatalf("TabletExternallyReparented(replica) failed: %v", err)
//	}
//}
//
//// TestTabletExternallyReparentedContinueOnUnexpectedMaster makes sure
//// that we ignore mysql's master if the flag is set
//func TestTabletExternallyReparentedContinueOnUnexpectedMaster(t *testing.T) {
//	tabletmanager.SetReparentFlags(time.Minute /* finalizeTimeout */)
//
//	ctx := context.Background()
//	ts := zktopo.NewTestServer(t, []string{"cell1"})
//	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient(), time.Second)
//
//	// Create an old master, a new master, two good slaves, one bad slave
//	oldMaster := NewFakeTablet(t, wr, "cell1", 0, topo.TYPE_MASTER)
//	newMaster := NewFakeTablet(t, wr, "cell1", 1, topo.TYPE_REPLICA)
//	goodSlave := NewFakeTablet(t, wr, "cell1", 2, topo.TYPE_REPLICA)
//
//	// On the elected master, we will respond to
//	// TabletActionSlaveWasPromoted, so we need a MysqlDaemon
//	// that returns no master, and the new port (as returned by mysql)
//	newMaster.StartActionLoop(t, wr)
//	defer newMaster.StopActionLoop(t)
//
//	// On the old master, we will only respond to
//	// TabletActionSlaveWasRestarted and point to a bad host
//	oldMaster.StartActionLoop(t, wr)
//	defer oldMaster.StopActionLoop(t)
//
//	// On the good slave, we will respond to
//	// TabletActionSlaveWasRestarted and point to a bad host
//	goodSlave.StartActionLoop(t, wr)
//	defer goodSlave.StopActionLoop(t)
//
//	// This tests the good case, where everything works as planned
//	t.Logf("TabletExternallyReparented(new master) expecting success")
//	tmc := tmclient.NewTabletManagerClient()
//	ti, err := ts.GetTablet(ctx, newMaster.Tablet.Alias)
//	if err != nil {
//		t.Fatalf("GetTablet failed: %v", err)
//	}
//	if err := tmc.TabletExternallyReparented(context.Background(), ti, ""); err != nil {
//		t.Fatalf("TabletExternallyReparented(replica) failed: %v", err)
//	}
//}
//
//func TestTabletExternallyReparentedFailedOldMaster(t *testing.T) {
//	tabletmanager.SetReparentFlags(time.Minute /* finalizeTimeout */)
//
//	ctx := context.Background()
//	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
//	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient(), time.Second)
//
//	// Create an old master, a new master, and a good slave.
//	oldMaster := NewFakeTablet(t, wr, "cell1", 0, topo.TYPE_MASTER)
//	newMaster := NewFakeTablet(t, wr, "cell1", 1, topo.TYPE_REPLICA)
//	goodSlave := NewFakeTablet(t, wr, "cell1", 2, topo.TYPE_REPLICA)
//
//	// Reparent to a replica, and pretend the old master is not responding.
//
//	// On the elected master, we will respond to
//	// TabletActionSlaveWasPromoted
//	newMaster.StartActionLoop(t, wr)
//	defer newMaster.StopActionLoop(t)
//
//	// On the old master, we will only get a
//	// TabletActionSlaveWasRestarted call, let's just not
//	// respond to it at all
//
//	// On the good slave, we will respond to
//	// TabletActionSlaveWasRestarted.
//	goodSlave.StartActionLoop(t, wr)
//	defer goodSlave.StopActionLoop(t)
//
//	// The reparent should work as expected here
//	t.Logf("TabletExternallyReparented(new master) expecting success")
//	tmc := tmclient.NewTabletManagerClient()
//	ti, err := ts.GetTablet(ctx, newMaster.Tablet.Alias)
//	if err != nil {
//		t.Fatalf("GetTablet failed: %v", err)
//	}
//	waitID := makeWaitID()
//	if err := tmc.TabletExternallyReparented(context.Background(), ti, waitID); err != nil {
//		t.Fatalf("TabletExternallyReparented(replica) failed: %v", err)
//	}
//	waitForExternalReparent(t, waitID)
//
//	// Now double-check the serving graph is good.
//	// Should only have one good replica left.
//	addrs, _, err := ts.GetEndPoints(ctx, "cell1", "test_keyspace", "0", topo.TYPE_REPLICA)
//	if err != nil {
//		t.Fatalf("GetEndPoints failed at the end: %v", err)
//	}
//	if len(addrs.Entries) != 1 {
//		t.Fatalf("GetEndPoints has too many entries: %v", addrs)
//	}
//
//	// check the old master was converted to spare
//	tablet, err := ts.GetTablet(ctx, oldMaster.Tablet.Alias)
//	if err != nil {
//		t.Fatalf("GetTablet(%v) failed: %v", oldMaster.Tablet.Alias, err)
//	}
//	if tablet.Type != topo.TYPE_SPARE {
//		t.Fatalf("old master should be spare but is: %v", tablet.Type)
//	}
//}

var externalReparents = make(map[string]chan struct{})

// makeWaitID generates a unique externalID that can be passed to
// TabletExternallyReparented, and then to waitForExternalReparent.
func makeWaitID() string {
	id := fmt.Sprintf("wait id %v", len(externalReparents))
	externalReparents[id] = make(chan struct{})
	return id
}

func init() {
	event.AddListener(func(ev *events.Reparent) {
		if ev.Status == "finished" {
			if c, ok := externalReparents[ev.ExternalID]; ok {
				close(c)
			}
		}
	})
}

// waitForExternalReparent waits up to a fixed duration for the external
// reparent with the given ID to finish. The ID must have been previously
// generated by makeWaitID().
//
// The TabletExternallyReparented RPC returns as soon as the
// new master is visible in the serving graph. Before checking things like
// replica endpoints and old master status, we should wait for the finalize
// stage, which happens in the background.
func waitForExternalReparent(t *testing.T, externalID string) {
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()

	select {
	case <-externalReparents[externalID]:
		return
	case <-timer.C:
		t.Fatalf("deadline exceeded waiting for finalized external reparent %q", externalID)
	}
}
