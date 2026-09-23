package brokeripc

// Test-only accessors (export_test.go pattern): these observe listener
// internals that only this package's own tests need. They are kept out of
// the production build so a live server's diagnostics or capacity never
// depend on a test-only accessor.

// refusal reports the most recent accept refusal, or nil if the listener has
// refused nothing yet.
func (l *listener) refusal() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastRefuse
}

// inFlight reports the count of accepted carriages still completing their
// handshake.
func (l *listener) inFlight() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.pending)
}

// liveSessions reports the count of admitted, still-open sessions.
func (l *listener) liveSessions() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.sessions)
}

// DefaultSocketPath returns the broker endpoint path under SocketFileName's
// convenience default runtime directory, for tests that need it without
// composing SocketPath(SocketDir()) themselves.
func DefaultSocketPath() string { return SocketPath(SocketDir()) }
