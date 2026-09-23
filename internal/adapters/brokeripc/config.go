package brokeripc

import (
	"time"

	"github.com/bnema/vev/internal/adapters/brokerwire"
	"github.com/bnema/vev/internal/protocol"
)

// Bounds. Every limit below is mandatory: no listener, session, or stream
// grows an unbounded queue, and every default is small enough that a stalled
// peer is refused rather than absorbed.
const (
	// DefaultMaxClients bounds concurrent accepted client connections on one
	// listener. A listener acquires one slot before accepting, so the bound is
	// enforced before a socket is admitted rather than after.
	DefaultMaxClients = 64
	// DefaultStreamInboundChunks bounds one logical stream's inbound chunk
	// queue: frames already accepted from the wire and not yet consumed by the
	// local session carriage.
	DefaultStreamInboundChunks = 64
	// DefaultStreamInboundBytes bounds one logical stream's inbound chunk queue
	// in bytes (4 MiB).
	DefaultStreamInboundBytes = 4 << 20
)

// Config bounds one endpoint. The zero value is valid and selects every
// default; a negative or zero explicit bound is replaced by its default.
type Config struct {
	// MaxClients bounds concurrent accepted client connections.
	MaxClients int
	// StreamInboundChunks bounds one logical stream's inbound chunk queue.
	StreamInboundChunks int
	// StreamInboundBytes bounds one logical stream's inbound chunk bytes.
	StreamInboundBytes uint64
	// HandshakeTimeout bounds one accepted connection's broker preamble, broker
	// admission, and the wait for its Register. The client dialer also bounds
	// its whole setup (Unix dial, preamble, Register send, and wait for
	// Registered) with it. On the server the registration phase shares the same
	// accept-time budget as the preamble and admission: a peer that completes
	// the preamble and admission but never sends Register is settled when that
	// deadline elapses, so it cannot hold a core client lease or a listener slot
	// indefinitely. It is deliberately separate from the session handshake
	// budget that runs inside a logical stream.
	HandshakeTimeout time.Duration
	// MaxPendingOperations bounds mutating operations this side is waiting for
	// on one connection. It never exceeds the brokerwire per-connection bound.
	MaxPendingOperations int
	// PassiveSubscribe marks every snapshot subscription of a client
	// connection as a read-only reader: it receives publications but never
	// counts as observation demand, so a CLI read never triggers a probe.
	PassiveSubscribe bool
	Build            string
	OnRetire         func()
}

// withDefaults fills every unset bound with its package default.
func (c Config) withDefaults() Config {
	if c.Build == "" {
		c.Build = defaultBuildIdentity()
	}
	if c.MaxClients <= 0 {
		c.MaxClients = DefaultMaxClients
	}
	if c.StreamInboundChunks <= 0 {
		c.StreamInboundChunks = DefaultStreamInboundChunks
	}
	if c.StreamInboundBytes == 0 {
		c.StreamInboundBytes = DefaultStreamInboundBytes
	}
	if c.HandshakeTimeout <= 0 {
		c.HandshakeTimeout = protocol.HandshakeTimeout
	}
	if c.MaxPendingOperations <= 0 {
		c.MaxPendingOperations = brokerwire.MaxPendingOperations
	}
	return c
}

// validate refuses a configuration whose explicit bounds cannot be honored.
// The zero value is always valid because withDefaults has already been applied.
func (c Config) validate() error {
	if c.MaxClients <= 0 || c.StreamInboundChunks <= 0 || c.StreamInboundBytes == 0 || c.HandshakeTimeout <= 0 {
		return ErrConfig
	}
	if c.MaxPendingOperations <= 0 || c.MaxPendingOperations > brokerwire.MaxPendingOperations {
		return ErrConfig
	}
	// A stream inbound byte bound below one maximum chunk can never accept a
	// full stream frame, which would starve a legal peer.
	if c.StreamInboundBytes < brokerwire.MaxStreamChunkBytes {
		return ErrConfig
	}
	return nil
}
