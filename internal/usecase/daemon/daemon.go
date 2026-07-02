// Package daemon holds vev's server-side session multiplexer use case.
//
// The daemon core — accepting client connections, spawning PTY-backed
// sessions, and fanning I/O between them — is implemented in a later
// milestone (Task 11). This file provides only the Serve entry point that
// the app layer wires the hidden --daemon path to, so the client, CLI, and
// auto-spawn machinery can be built and tested against a stable seam now.
package daemon

import (
	"errors"

	"github.com/bnema/vev/internal/ports"
)

// ErrNotImplemented is returned by Serve until the daemon core lands in
// Task 11. It is deliberately a distinct sentinel so the app layer (and
// tests) can recognise the not-yet-implemented state.
var ErrNotImplemented = errors.New("vev: daemon core not implemented yet (Task 11)")

// Serve runs the daemon's accept loop over ln, owning ln for its lifetime.
//
// TODO(Task 11): implement the real accept/session/dispatch loop. For now
// it accepts the listener and immediately reports ErrNotImplemented without
// closing it (the caller owns ln and closes it).
func Serve(ln ports.Listener) error {
	_ = ln
	return ErrNotImplemented
}
