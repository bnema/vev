package brokeripc

import (
	"context"
	"errors"

	"github.com/bnema/vev/internal/adapters/brokerwire"
	"github.com/bnema/vev/internal/ports"
)

// Adapter sentinels. They are typed so a caller can classify a listener,
// session, framing, or backpressure outcome without matching on message text.
// Semantic broker refusals stay ports.BrokerError, and admission refusals stay
// ports.BrokerAdmissionError; this adapter wraps them rather than redefining
// them.
var (
	// ErrConfig reports an invalid listener or dialer configuration.
	ErrConfig = errors.New("brokeripc: invalid configuration")
	// ErrListenerClosed reports Accept on a closed listener.
	ErrListenerClosed = errors.New("brokeripc: listener is closed")
	// ErrSessionClosed reports session work presented after the session reached
	// its terminal outcome.
	ErrSessionClosed = errors.New("brokeripc: session is closed")
	// ErrMalformedFrame reports an outer envelope this side refuses to decode:
	// an oversize length prefix, a truncated or trailing payload, an unknown or
	// repeated field, or a wrong-direction payload. A malformed frame is a
	// protocol violation and settles exactly the connection that sent it.
	ErrMalformedFrame = errors.New("brokeripc: malformed broker frame")
	// ErrProtocol reports a well-formed frame that violates connection
	// ordering: a duplicate Register, a frame for an unallocated stream, or
	// stream data before the stream was established.
	ErrProtocol = errors.New("brokeripc: broker protocol violation")
	// ErrStreamBackpressure reports a logical stream whose local consumer is
	// not draining its bounded inbound queue. The affected stream is settled
	// alone; the connection and every sibling stream stay up.
	ErrStreamBackpressure = errors.New("brokeripc: stream consumer is not keeping up")
	// ErrStreamGone reports a stream the peer retired while this side was still
	// starting or using it.
	ErrStreamGone = errors.New("brokeripc: logical stream is gone")
	// ErrConnectionClosed reports a client adapter whose connection reached its
	// terminal outcome.
	ErrConnectionClosed = errors.New("brokeripc: broker connection is closed")
	// ErrScopeMismatch reports a frame carrying an epoch or connection identity
	// that is not the one assigned at accept. It is refused as stale and never
	// applied.
	ErrScopeMismatch = errors.New("brokeripc: frame scope does not match the accepted connection")
	// ErrRegistrationTimeout reports a client that completed the broker preamble
	// and admission but did not send its Register within the accept-time
	// handshake budget. The session is settled, its admitted core service is
	// closed, and its listener slot is released; context.DeadlineExceeded also
	// matches it. It is an orderly peer disconnect, so a listener draining its
	// sessions does not report a clean shutdown as this timeout.
	ErrRegistrationTimeout = errors.New("brokeripc: broker registration timed out")
)

// transportFailure classifies one carriage error that ended a connection. A
// closed carriage is an orderly disconnect; anything else is a transport
// failure.
func transportFailure(err error) error {
	if err == nil {
		return ErrConnectionClosed
	}
	return err
}

// cancelOrCause prefers the context outcome when a wait was cancelled, so a
// caller sees cancellation rather than a synthetic transport error.
func cancelOrCause(ctx context.Context, cause error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return cause
}

// admissionError maps one brokerwire tracker refusal onto the closed ports
// admission taxonomy. An unrecognized refusal is invalid-request, never a
// retryable limit.
func admissionError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, brokerwire.ErrTooManyStreams), errors.Is(err, brokerwire.ErrTooManyPendingOperations):
		return ports.BrokerAdmissionLimit
	case errors.Is(err, brokerwire.ErrConnectionClosed):
		return ports.BrokerAdmissionClosed
	case errors.Is(err, brokerwire.ErrStreamIDReused), errors.Is(err, brokerwire.ErrConnectionState),
		errors.Is(err, brokerwire.ErrNotSubscribed), errors.Is(err, brokerwire.ErrDuplicateRegister),
		errors.Is(err, brokerwire.ErrOperationCompleted), errors.Is(err, brokerwire.ErrOperationPending):
		return ports.BrokerAdmissionStale
	default:
		return ports.BrokerAdmissionInvalid
	}
}

// errorDetail converts one broker failure into the bounded, presentation-safe
// wire detail. The diagnostic cause stays local: only the typed code, bounded
// display text, closed admission code, and sanitized failure kind travel.
func errorDetail(err error) brokerwire.ErrorDetail {
	if err == nil {
		return brokerwire.ErrorDetail{}
	}
	var failure ports.BrokerError
	if errors.As(err, &failure) {
		return brokerwire.ErrorDetail{Code: failure.Code, Text: failure.Text}
	}
	var admission ports.BrokerAdmissionError
	if errors.As(err, &admission) {
		detail := brokerwire.ErrorDetail{Code: ports.BrokerErrorUnavailable}
		switch {
		case errors.Is(err, ports.BrokerAdmissionLimit):
			detail.AdmissionCode = 1
		case errors.Is(err, ports.BrokerAdmissionClosed):
			detail.AdmissionCode = 2
		case errors.Is(err, ports.BrokerAdmissionStale):
			detail.AdmissionCode = 3
		default:
			detail.AdmissionCode = 4
		}
		return detail
	}
	// The ports type carries value-receiver methods, so both the value and the
	// pointer form are legal errors; classify either rather than letting the
	// pointer form fall through to a retryable-looking code.
	var outcomeUnknown ports.BrokerStoreOutcomeUnknownError
	var outcomeUnknownPtr *ports.BrokerStoreOutcomeUnknownError
	switch {
	case errors.As(err, &outcomeUnknown), errors.As(err, &outcomeUnknownPtr):
		return brokerwire.ErrorDetail{Code: ports.BrokerErrorOutcomeUnknown}
	case errors.Is(err, context.DeadlineExceeded):
		return brokerwire.ErrorDetail{Code: ports.BrokerErrorTimeout}
	case errors.Is(err, context.Canceled):
		return brokerwire.ErrorDetail{Code: ports.BrokerErrorCancelled}
	default:
		return brokerwire.ErrorDetail{Code: ports.BrokerErrorUnavailable}
	}
}

// failureFromDetail converts one received detail back into the typed local
// failure. A detail with no code is the absent-detail case and yields nil.
func failureFromDetail(detail brokerwire.ErrorDetail) error {
	switch detail.AdmissionCode {
	case 1:
		return ports.BrokerAdmissionLimit
	case 2:
		return ports.BrokerAdmissionClosed
	case 3:
		return ports.BrokerAdmissionStale
	case 4:
		return ports.BrokerAdmissionInvalid
	}
	if detail.Code == 0 {
		return nil
	}
	return ports.BrokerError{Code: detail.Code, Text: detail.Text}
}
