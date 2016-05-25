package main

import (
	"flag"
	"sync"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/tabletserver/grpcqueryservice"
	"github.com/youtube/vitess/go/vt/tabletserver/queryservice/fakes"
	"github.com/youtube/vitess/go/vt/throttler"
	"github.com/youtube/vitess/go/vt/vttest/fakesqldb"
	"github.com/youtube/vitess/go/vt/wrangler"
	"github.com/youtube/vitess/go/vt/wrangler/testlib"
	"github.com/youtube/vitess/go/vt/zktopo/zktestserver"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"

	log "github.com/golang/glog"
)

var (
	rate              = flag.Int64("rate", 1000, "start rate of the throttled demo server")
	duration          = flag.Duration("duration", 600*time.Second, "total duration the demo runs")
	lagUpdateInterval = flag.Duration("lag_update_interval", 1*time.Second, "interval at which the current replication lag will be broadcasted to the throttler")
	reparentInterval  = flag.Duration("reparent_interval", 0*time.Second, "simulate a reparent every X interval (i.e. the master won't write for -reparent_duration and the replication lag might go up)")
	reparentDuration  = flag.Duration("reparent_duration", 10*time.Second, "time a simulated reparent should take")
)

// master fakes out an *unthrottled* MySQL master which replicates every
// received call to a known "replica".
type master struct {
	replica *replica
}

func (m *master) call(msg time.Time) {
	m.replica.replicate(msg)
}

// replica fakes out a *throttled* MySQL replica.
// If it cannot keep up with applying the master writes, it will report a
// replication lag > 0 seconds.
type replica struct {
	fakeTablet        *testlib.FakeTablet
	qs                *fakes.StreamHealthQueryService
	throttler         *throttler.Throttler
	replicationStream chan time.Time
	lastHealthUpdate  time.Time
	stop              chan struct{}
	wg                sync.WaitGroup
	lagUpdateInterval time.Duration
}

func newReplica(lagUpdateInterval time.Duration) *replica {
	t := &testing.T{}
	ts := zktestserver.New(t, []string{"cell1"})
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	db := fakesqldb.Register()
	fakeTablet := testlib.NewFakeTablet(t, wr, "cell1", 0,
		topodatapb.TabletType_REPLICA, db, testlib.TabletKeyspaceShard(t, "ks", "-80"))
	fakeTablet.StartActionLoop(t, wr)

	target := querypb.Target{
		Keyspace:   "ks",
		Shard:      "-80",
		TabletType: topodatapb.TabletType_REPLICA,
	}
	qs := fakes.NewStreamHealthQueryService(target)
	grpcqueryservice.RegisterForTest(fakeTablet.RPCServer, qs)

	throttler, err := throttler.NewThrottler("demo", "TPS", 1, *rate,
		throttler.ReplicationLagModuleDisabled)
	if err != nil {
		log.Fatal(err)
	}

	r := &replica{
		fakeTablet:        fakeTablet,
		qs:                qs,
		throttler:         throttler,
		replicationStream: make(chan time.Time, 1*1024*1024),
		stop:              make(chan struct{}),
		lagUpdateInterval: lagUpdateInterval,
	}
	r.wg.Add(1)
	go r.processReplicationStream()
	return r
}

func (r *replica) processReplicationStream() {
	defer r.wg.Done()

	rate := 0
	for msg := range r.replicationStream {
		select {
		case <-r.stop:
			return
		default:
		}

		now := time.Now()
		if now.Sub(r.lastHealthUpdate) > r.lagUpdateInterval {
			// Broadcast current lag every second.
			lag := now.Sub(msg).Seconds()
			lagTruncated := uint32(lag)
			log.Infof("current lag: %1ds (%1.1fs) replica rate: % 7.1f chan len: % 6d", lagTruncated, lag, float64(rate)/r.lagUpdateInterval.Seconds(), len(r.replicationStream))
			r.qs.AddHealthResponseWithSecondsBehindMaster(lagTruncated)
			r.lastHealthUpdate = now
			rate = 0
		}

		for {
			backoff := r.throttler.Throttle(0 /* threadID */)
			if backoff == throttler.NotThrottled {
				break
			}
			//			log.Infof("replica throttled: %v", backoff)
			time.Sleep(backoff)
		}
		rate++
	}
}

func (r *replica) close() {
	close(r.stop)
	log.Info("closing channel")
	r.wg.Wait()
	r.fakeTablet.StopActionLoop(&testing.T{})
}

func (r *replica) replicate(msg time.Time) {
	r.replicationStream <- msg
}

// client fakes out a client which should throttle itself based on the
// replication lag of all replicas.
type client struct {
	healthCheck      discovery.HealthCheck
	throttler        *throttler.Throttler
	stop             chan struct{}
	wg               sync.WaitGroup
	nextReparent     time.Time
	reparentInterval time.Duration
	reparentDuration time.Duration

	master *master
}

func newClient(master *master, replica *replica, reparentInterval, reparentDuration time.Duration) *client {
	t, err := throttler.NewThrottler("demo", "TPS", 1, throttler.MaxRateModuleDisabled,
		5 /* seconds */)
	if err != nil {
		log.Fatal(err)
	}

	healthCheck := discovery.NewHealthCheck(1*time.Minute, 5*time.Second, 1*time.Minute, "" /* statsSuffix */)
	var nextReparent time.Time
	if reparentInterval != time.Duration(0) {
		nextReparent = time.Now().Add(reparentInterval)
	}
	c := &client{
		healthCheck:      healthCheck,
		throttler:        t,
		stop:             make(chan struct{}),
		nextReparent:     nextReparent,
		reparentInterval: reparentInterval,
		reparentDuration: reparentDuration,
		master:           master,
	}
	c.healthCheck.SetListener(c)
	c.healthCheck.AddTablet("cell1", "name", replica.fakeTablet.Tablet)
	return c
}

func (c *client) run() {
	c.wg.Add(1)
	go c.loop()
}

func (c *client) loop() {
	defer c.wg.Done()

	for {
		select {
		case <-c.stop:
			return
		default:
		}

		for {
			backoff := c.throttler.Throttle(0 /* threadID */)
			if backoff == throttler.NotThrottled {
				break
			}
			//			log.Infof("client throttled for: %v", backoff)
			time.Sleep(backoff)
		}

		c.master.call(time.Now())
		if !c.nextReparent.IsZero() && time.Now().After(c.nextReparent) {
			log.Infof("stopping the client for %.f seconds (simulating a reparent)", c.reparentDuration.Seconds())
			time.Sleep(c.reparentDuration)
			c.nextReparent = time.Now().Add(c.reparentInterval)
		}
	}
}

func (c *client) close() {
	close(c.stop)
	c.wg.Wait()

	c.healthCheck.Close()
	c.throttler.Close()
}

func (c *client) StatsUpdate(ts *discovery.TabletStats) {
	//	log.Infof("update from healthCheck: %v", ts)
	c.throttler.RecordReplicationLag(int64(ts.Stats.SecondsBehindMaster), time.Now())
}

func main() {
	flag.Parse()

	log.Infof("rate set to: %v", *rate)
	replica := newReplica(*lagUpdateInterval)
	master := &master{replica: replica}
	client := newClient(master, replica, *reparentInterval, *reparentDuration)
	client.run()

	time.Sleep(*duration)
	client.close()
	replica.close()
}
