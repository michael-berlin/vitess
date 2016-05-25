package throttler

import "testing"

func TestMaxReplicatonLagModule(t *testing.T) {
	//	fc := &fakeClock{}
	//
	//	rateUpdateChan := make(chan struct{}, 1)
	//	actualTPS := newHistory(100)
	//	m := NewMaxReplicationLagModule(DefaultMaxReplicationLagModuleConfig, actualTPS, fc.now)
	//	m.Start(rateUpdateChan)
	//
	//	if got := m.MaxRate(); got != 100 {
	//		t.Fatalf("wrong initial rate: got = %v, want = %v", got, 100)
	//	}
	//
	//	actualTPS.add(record{0, sinceZero(0 * time.Second)})
	//	m.RecordReplicationLag(0, sinceZero(0*time.Second))
	//	<-rateUpdateChan
	//
	//	if got := m.MaxRate(); got != 100 {
	//		t.Fatalf("wrong initial rate: got = %v, want = %v", got, 100)
	//	}
}
