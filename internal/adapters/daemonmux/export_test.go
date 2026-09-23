package daemonmux

import (
	"google.golang.org/protobuf/proto"

	"github.com/bnema/vev/internal/adapters/clock"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

// Test-only convenience constructors and accessors (export_test.go pattern):
// production always supplies its own explicit ceilings, clock, or budget
// (see pump.go, supervisor.go), so these package-default shorthands only
// exist for tests that don't care about a non-default value.

// Accepts reports whether policy matches an admitted entry, for tests over the
// legacy single-policy binding shape.
func (b ServerBinding) Accepts(policy ports.BrokerPolicy) bool {
	_, ok := b.Accepted(policy)
	return ok
}

// Policy returns the sole entry's policy for legacy single-policy test
// fixtures. It returns the zero value for a multi-entry authority.
func (b ServerBinding) Policy() ports.BrokerPolicy {
	if len(b.entries) == 1 {
		return b.entries[0].Policy
	}
	return ports.BrokerPolicy{}
}

// Origin returns the sole entry's locality for legacy single-policy test
// fixtures. It returns the unknown origin for a multi-entry authority.
func (b ServerBinding) Origin() ports.SessionConnectionOrigin {
	if len(b.entries) == 1 {
		return b.entries[0].Origin
	}
	return ports.SessionOriginUnknown
}

// NewStreamEngine returns an open engine bound to one physical connection
// under the package default MuxCeilings.
func NewStreamEngine() *StreamEngine {
	return newStreamEngine(DefaultMuxCeilings())
}

// NewListener returns a listener under the package default handshake budget,
// accept-queue bound, and wall clock.
func NewListener(pump *Pump, accepted ServerPolicyAdmission) (*Listener, error) {
	return newListener(pump, protocol.HandshakeTimeout, MaxAcceptQueue, clock.New(), accepted)
}

// NewScheduler returns an empty outbound scheduler under the package default
// MuxCeilings.
func NewScheduler(engine *StreamEngine) *Scheduler {
	return newScheduler(engine, DefaultMuxCeilings())
}

// Children reports the count of currently owned physical children.
func (s *ServerSupervisor) Children() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.children)
}

// DecodePreambleRequestBytes strictly decodes one serialized daemonmux client
// preamble under the 4 KiB bound, the strict scan, the generated unmarshal,
// and DecodePreambleRequest's semantic checks. Production decodes a preamble
// inline through the same steps (see handshake.go); this composed helper
// exists for tests and the package fuzz target.
func DecodePreambleRequestBytes(payload []byte) (MuxPreambleRequest, error) {
	if err := CheckPreambleSize(payload); err != nil {
		return MuxPreambleRequest{}, err
	}
	message := &wire.MuxPreambleRequest{}
	if err := wire.ScanEnvelope(message, payload); err != nil {
		return MuxPreambleRequest{}, err
	}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}.Unmarshal(payload, message)); err != nil {
		return MuxPreambleRequest{}, err
	}
	return DecodePreambleRequest(message)
}
