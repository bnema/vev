// Package broker implements vev's per-user connection broker.
//
// The broker is the machine-wide owner of connectivity: configured hosts,
// daemon reachability and catalogue snapshots, pooled physical transports,
// and multiplexed logical streams. It never owns picker UI, sessions,
// PTYs, terminal geometry, or attachment semantics. See ADR 001.
package broker
