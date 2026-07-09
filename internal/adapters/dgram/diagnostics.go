package dgram

import (
	"log/slog"
	"time"

	"github.com/bnema/vev/internal/ports"
)

const diagnosticBufferSize = 16

// Diagnostic is an immutable, privacy-safe snapshot of UDP link health.
// It intentionally contains no packet payloads, addresses, keys, or identities.
type Diagnostic struct {
	At                       time.Time
	State                    ports.LinkState
	SinceAuthenticatedPacket time.Duration
	SinceCompleteRecord      time.Duration
	SinceACKProgress         time.Duration
	PendingRecords           int
	PendingBytes             int
	Retransmits              uint64
	ReassemblyInflight       int
}

// DiagnosticObserver receives bounded asynchronous UDP health snapshots.
type DiagnosticObserver func(Diagnostic)

// DiagnosticLogObserver returns an observer suitable for composition roots. It
// logs only the privacy-safe diagnostic counters and ages.
func DiagnosticLogObserver(log *slog.Logger) DiagnosticObserver {
	if log == nil {
		return nil
	}
	return func(d Diagnostic) {
		log.Debug("udp health",
			"state", d.State,
			"since_authenticated_packet", d.SinceAuthenticatedPacket,
			"since_complete_record", d.SinceCompleteRecord,
			"since_ack_progress", d.SinceACKProgress,
			"pending_records", d.PendingRecords,
			"pending_bytes", d.PendingBytes,
			"retransmits", d.Retransmits,
			"reassembly_inflight", d.ReassemblyInflight,
		)
	}
}
