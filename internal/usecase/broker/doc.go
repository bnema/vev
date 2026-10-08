// Package broker owns per-user connectivity: configured hosts, daemon
// reachability and catalogue snapshots, pooled physical transports, and
// multiplexed logical streams. It does not own picker UI, sessions, PTYs,
// terminal geometry, or attachment semantics.
//
// Registry state lives in registry.go and is guarded by one mutex. Its
// behaviour is split by responsibility: durable membership mutation
// (registry_membership.go), probe scheduling and result application
// (registry_probe.go), local daemon observation (registry_local.go),
// snapshot publication and subscriptions (registry_publish.go), and the
// asynchronous observation writer (registry_snapshot_writer.go).
package broker
