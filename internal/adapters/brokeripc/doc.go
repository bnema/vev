// Package brokeripc adapts the per-user broker service to private Unix IPC.
//
// The client and listener exchange brokerwire messages over a shared framed
// carriage. Each endpoint admits only same-user peers: the socket resides in
// an owner-only directory, is restricted to mode 0600, and both dial and accept
// verify kernel peer credentials. A live socket owner is never evicted.
//
// Each accepted connection has its own epoch, identity, bounded handshake,
// queues, operation tracking, and stream relays. Frames with stale identities
// are refused without touching broker state; malformed envelopes terminate
// only their own connection. Disconnect closes bridged streams and joins all
// workers before returning.
package brokeripc
