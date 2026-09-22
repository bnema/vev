// Package brokerwire adapts typed broker messages to Protobuf envelopes.
//
// The broker is a separate conversation from the session protocol: client
// tags 101-113 and server tags 201-210 are disjoint from every session tag,
// and each direction is its own closed oneof union. Every connection starts
// with the bounded broker preamble (roles 3/4); magic, epoch, exact
// protocol version, and negotiated limits are validated before application
// messages.
//
// This package is stateful only where P3.1 requires it: connection state,
// multipart snapshot assembly, operation tracking, and stream lifecycle
// live in snapshot.go and state.go. EncodeClient/DecodeClient and
// EncodeServer/DecodeServer remain stateless, converting one complete
// serialized envelope at a time using strict wire.ScanEnvelope before
// generated unmarshal. There is no socket, framing pump, or P3.2 transport
// here.
package brokerwire
