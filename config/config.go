// Package config defines configuration for distributed semaphore instances.
package config

import "time"

// QueuePolicy selects how waiters are ordered.
type QueuePolicy int

const (
	// QueueFIFO is first-come-first-served ordering.
	QueueFIFO QueuePolicy = iota
	// QueuePriority orders waiters by descending priority, then arrival time.
	QueuePriority
)

// Config holds the runtime configuration for one semaphore instance.
type Config struct {
	// InstanceID uniquely identifies this instance within the in-process cluster.
	InstanceID string
	// DataDir is the directory used for durable write-ahead log and snapshots.
	DataDir string
	// HeartbeatInterval is how often the leader broadcasts heartbeats.
	HeartbeatInterval time.Duration
	// ElectionTimeout bounds how long a follower waits before starting an election.
	ElectionTimeout time.Duration
	// LeaseTTL is the default permit lease lifetime used when a request omits one.
	LeaseTTL time.Duration
}

// DefaultConfig returns a config populated with sensible defaults.
func DefaultConfig() Config {
	return Config{
		HeartbeatInterval: 50 * time.Millisecond,
		ElectionTimeout:   200 * time.Millisecond,
		LeaseTTL:          5 * time.Second,
	}
}
