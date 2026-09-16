package brokeripc

import (
	"context"
	"errors"
	"io"

	"google.golang.org/protobuf/proto"

	"github.com/bnema/vev/internal/adapters/brokerwire"
	"github.com/bnema/vev/internal/protocol/wire"
)

// Broker preamble exchange (P3.3).
//
// The first client frame on a broker connection is always the broker
// PreambleRequest (role 3) and the first server frame is always the broker
// PreambleResponse (role 4). Both are built, validated, and refusal-coded by
// brokerwire: this file only moves one bounded preamble envelope in each
// direction over the carriage and returns the negotiated effective ceilings.
//
// The preamble envelope is bounded to brokerwire.BrokerPreambleLimit (4 KiB)
// before allocation, strict-scanned with wire.ScanEnvelope before generated
// unmarshal, and is never restarted: the caller's context (the configured
// handshake timeout) bounds the whole exchange.

// runClientPreamble sends the local broker PreambleRequest offer and accepts
// exactly one PreambleResponse. It returns the effective negotiated ceilings.
func runClientPreamble(ctx context.Context, transport wire.Transport, offer brokerwire.Ceilings) (brokerwire.Ceilings, error) {
	if err := offer.Validate(); err != nil {
		return brokerwire.Ceilings{}, errors.Join(ErrConfig, err)
	}
	type result struct {
		ceilings brokerwire.Ceilings
		err      error
	}
	done := make(chan result, 1)
	go func() {
		request := brokerwire.EncodePreambleRequest(offer)
		raw, err := proto.Marshal(request)
		if err != nil {
			done <- result{err: err}
			return
		}
		if err := transport.Send(wire.Envelope{Payload: raw}); err != nil {
			done <- result{err: transportFailure(err)}
			return
		}
		payload, err := recvPreamble(transport)
		if err != nil {
			done <- result{err: err}
			return
		}
		var response wire.PreambleResponse
		if err := wire.ScanEnvelope(&response, payload); err != nil {
			done <- result{err: errors.Join(ErrMalformedFrame, err)}
			return
		}
		if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(payload, &response); err != nil {
			done <- result{err: errors.Join(ErrMalformedFrame, err)}
			return
		}
		accepted, err := brokerwire.DecodePreambleResponse(&response)
		if err != nil {
			done <- result{err: err}
			return
		}
		done <- result{ceilings: brokerwire.EffectiveCeilings(offer, accepted.Ceilings)}
	}()
	select {
	case <-ctx.Done():
		return brokerwire.Ceilings{}, ctx.Err()
	case outcome := <-done:
		return outcome.ceilings, outcome.err
	}
}

// runServerPreamble accepts exactly one broker PreambleRequest, answers it with
// acceptance or the precise brokerwire refusal code, and returns the effective
// negotiated ceilings. A refused request is answered before the carriage is
// closed by the caller, so the client sees the typed refusal rather than a
// bare disconnect.
func runServerPreamble(ctx context.Context, transport wire.Transport, offer brokerwire.Ceilings) (brokerwire.Ceilings, error) {
	if err := offer.Validate(); err != nil {
		return brokerwire.Ceilings{}, errors.Join(ErrConfig, err)
	}
	type result struct {
		ceilings brokerwire.Ceilings
		err      error
	}
	done := make(chan result, 1)
	go func() {
		payload, err := recvPreamble(transport)
		if err != nil {
			done <- result{err: err}
			return
		}
		var request wire.PreambleRequest
		if err := wire.ScanEnvelope(&request, payload); err != nil {
			_ = sendPreambleResponse(transport, brokerwire.EncodePreambleResponse(false, offer, brokerwire.RejectionCodeFor(nil, err)))
			done <- result{err: errors.Join(ErrMalformedFrame, err)}
			return
		}
		if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(payload, &request); err != nil {
			_ = sendPreambleResponse(transport, brokerwire.EncodePreambleResponse(false, offer, brokerwire.RejectionCodeFor(nil, err)))
			done <- result{err: errors.Join(ErrMalformedFrame, err)}
			return
		}
		accepted, err := brokerwire.DecodePreambleRequest(&request)
		if err != nil {
			_ = sendPreambleResponse(transport, brokerwire.EncodePreambleResponse(false, offer, brokerwire.RejectionCodeFor(&request, err)))
			done <- result{err: err}
			return
		}
		effective := brokerwire.EffectiveCeilings(offer, accepted.Ceilings)
		if err := sendPreambleResponse(transport, brokerwire.EncodePreambleResponse(true, effective, 0)); err != nil {
			done <- result{err: transportFailure(err)}
			return
		}
		done <- result{ceilings: effective}
	}()
	select {
	case <-ctx.Done():
		return brokerwire.Ceilings{}, ctx.Err()
	case outcome := <-done:
		return outcome.ceilings, outcome.err
	}
}

// recvPreamble reads exactly one length-delimited preamble envelope, enforcing
// the 4 KiB broker preamble bound against the length prefix before any body
// allocation and before any scan or unmarshal.
func recvPreamble(transport wire.Transport) ([]byte, error) {
	bounded, ok := transport.(wire.BoundedTransport)
	if !ok {
		return nil, errors.Join(ErrConfig, errors.New("brokeripc: carriage is not bounded"))
	}
	envelope, err := bounded.RecvBounded(brokerwire.BrokerPreambleLimit)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, transportFailure(err)
		}
		if errors.Is(err, wire.ErrScanLength) {
			return nil, errors.Join(ErrMalformedFrame, err)
		}
		return nil, transportFailure(err)
	}
	if err := brokerwire.CheckPreambleSize(envelope.Payload); err != nil {
		return nil, errors.Join(ErrMalformedFrame, err)
	}
	return envelope.Payload, nil
}

// sendPreambleResponse writes one already-validated broker preamble response.
func sendPreambleResponse(transport wire.Transport, response *wire.PreambleResponse) error {
	raw, err := proto.Marshal(response)
	if err != nil {
		return err
	}
	if err := brokerwire.CheckPreambleSize(raw); err != nil {
		return err
	}
	return transport.Send(wire.Envelope{Payload: raw})
}
