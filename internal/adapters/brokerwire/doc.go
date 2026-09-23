// Package brokerwire owns the broker conversation's directional Protobuf codec.
//
// Client and server envelopes are disjoint closed unions, separate from the
// session protocol. Every connection validates a bounded preamble with exact
// version and role matching before application traffic. The codec strictly
// scans each complete envelope before unmarshalling; connection state tracks
// multipart snapshots, operations, and stream lifecycle. Socket ownership and
// framing pumps belong to the transport adapters, not this package.
package brokerwire
