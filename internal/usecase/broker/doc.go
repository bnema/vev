// Package broker owns per-user connectivity: configured hosts, daemon
// reachability and catalogue snapshots, pooled physical transports, and
// multiplexed logical streams. It does not own picker UI, sessions, PTYs,
// terminal geometry, or attachment semantics.
package broker
