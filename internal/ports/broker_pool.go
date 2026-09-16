package ports

import "context"

// BrokerMaxAddressBytes bounds the opaque adapter route.
const BrokerMaxAddressBytes = 4096

// BrokerStreamPurpose keeps observation and control independent from attachments.
type BrokerStreamPurpose uint8

const (
	BrokerStreamAttachment BrokerStreamPurpose = iota + 1
	BrokerStreamControl
	BrokerStreamObservation
)

// BrokerResolvedEndpoint is an authenticated service binding, never a hostname
// guess. Resolver owns registration fencing and machine-wide policy authority.
// Address is an opaque adapter route, not a pooling identity. Resolution must
// not start a daemon for observation. Connect must verify Identity and Policy;
// the pool verifies them again before publication. Incarnation is learned on
// connection and is deliberately not a stable pooling key.
type BrokerResolvedEndpoint struct {
	Identity BrokerDaemonIdentity
	Policy   BrokerPolicy
	Address  string
}

func (e BrokerResolvedEndpoint) Validate() error {
	if err := e.Identity.Validate(); err != nil {
		return err
	}
	if err := e.Policy.Validate(); err != nil {
		return err
	}
	return validateBrokerToken(e.Address, BrokerMaxAddressBytes, "resolved address")
}

// Resolve must validate current registration and reject conflicting requested
// policy. It must honor cancellation and return bounded, authenticated values.
// No resolver cache or alias normalization in the pool can bypass this check.
// Resolver, connector and OpenStream adapters should return BrokerError for
// semantic rejection and context errors for cancellation/deadline. Unknown
// errors become Unavailable; causes remain accessible through errors.Is/As.
// Never place secrets or unbounded adapter diagnostics in BrokerError.Text.
type BrokerEndpointResolver interface {
	Resolve(context.Context, BrokerOpenStreamRequest) (BrokerResolvedEndpoint, error)
}

// BrokerLogicalConnection supplies terminal notification independent of reads.
// Done closes exactly once; Err is stable afterwards (nil for orderly close).
// Close is concurrent-safe, prompt, and unblocks all I/O. Physical Close and
// connector/resolver/open cancellation must likewise complete promptly. The
// pool does not buffer traffic: adapters must isolate bounded per-stream queues.
type BrokerLogicalConnection interface {
	ClientConnection
	Done() <-chan struct{}
	Err() error
}

// BrokerAdmissionError distinguishes resource/identity rejection from transport
// failure. Rejected or failed reservations still consume the monotonic stream
// ID; callers must use a fresh ID. A connection ID is never reused in an epoch.
type BrokerAdmissionError string

func (e BrokerAdmissionError) Error() string { return "vev: broker admission: " + string(e) }

const (
	BrokerAdmissionLimit   BrokerAdmissionError = "limit"
	BrokerAdmissionClosed  BrokerAdmissionError = "closed"
	BrokerAdmissionStale   BrokerAdmissionError = "stale connection or stream"
	BrokerAdmissionInvalid BrokerAdmissionError = "invalid request"
)
