package throttler

import (
	"fmt"
	"time"
)

type MaxReplicationLagModuleConfig struct {
	// TargetReplicationLag is the replication lag (in seconds) this module tries
	// to aim for. If we're within the target, we try to increase the throttler
	// rate, otherwise we'll lower it based on an educated guess.
	TargetReplicationLag int64

	// MaxReplicationLag is meant as a last resort.
	// By default, this module tries to find out the system maximum capacity while
	// trying to keep the replication lag around "targetReplicationLag".
	// Usually, we'll wait MinDurationBetweenChanges to see the effect of a
	// throttler rate change on the replication lag.
	// But if the lag goes above MaxReplicationLag we'll take extra measures and
	// throttle all requests (setting the rate to 0).
	// This is the only way to ensure that the system will recover.
	MaxReplicationLag int64

	// InitialRate is the rate at which the module will start.
	// Up to this rate, rate increases are always a doubling. Beyond this rate,
	// rate increases happen in steps of MaxIncrease instead.
	InitialRate int64
	// MaxIncrease defines by how much we will increase the rate
	// e.g. 0.05 increases the rate by 5%.
	// Note that any increase will let the system wait for at least
	// (1 / MaxIncrease) seconds. If we wait for shorter periods of time, we
	// won't notice if the rate increase also increases the replication lag.
	// (If the system was already at its maximum capacity (e.g. 1k QPS) and we
	// increase the rate by e.g. 5% to 1050 QPS, it will take 20 seconds until
	// 1000 extra queries are buffered and the lag increases by 1 second.)
	MaxIncrease float64

	// MinDurationBetweenIncreases specifies how long we'll wait for the last rate
	// increase to have an effect on the system.
	MinDurationBetweenChanges time.Duration
}

var DefaultMaxReplicationLagModuleConfig = MaxReplicationLagModuleConfig{
	TargetReplicationLag: 1,
	MaxReplicationLag:    ReplicationLagModuleDisabled,

	InitialRate: 750,
	MaxIncrease: 0.05,

	MinDurationBetweenChanges: 10 * time.Second,
}

func NewMaxReplicationLagModuleConfig(maxReplicationLag int64) MaxReplicationLagModuleConfig {
	config := DefaultMaxReplicationLagModuleConfig
	config.MaxReplicationLag = maxReplicationLag
	return config
}

// Verify returns an error if the config is invalid.
func (c MaxReplicationLagModuleConfig) Verify() error {
	if c.TargetReplicationLag > c.MaxReplicationLag {
		return fmt.Errorf("target replication lag must not be higher than the configured max replication lag: invalid: %v > %v",
			c.TargetReplicationLag, c.MaxReplicationLag)
	}
	return nil
}
