package sessionwire

// Preamble state machine (P3.1, wired at the P3.3 cutover).
//
// The first client frame is always PreambleRequest; the first server frame
// is always PreambleResponse. Exact version equality is mandatory. The
// preamble negotiates immutable per-connection ceilings once;
// Hello.MaxOutputInFlight stays attachment-scoped and renegotiable on a
// reused stream. One absolute deadline covers authentication, preamble,
// Hello/Welcome, and first committed publication; layers receive the same
// deadline and never restart it.

import (
	"context"
	"encoding/binary"
	"errors"
	"io"

	"google.golang.org/protobuf/proto"

	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

var (
	ErrPreambleRejected = errors.New("sessionwire: preamble rejected")
	ErrPreambleTimeout  = errors.New("sessionwire: preamble deadline exceeded")
	ErrPreambleOrder    = errors.New("sessionwire: preamble out of order")
)

// protoCeilings are the immutable per-connection ceilings negotiated once
// by the preamble.
type protoCeilings struct {
	maxReceiveEnvelopeBytes uint64
	outputDataLimit         uint64
}

// defaultProtoCeilings advertises the local receive policy.
func defaultProtoCeilings() protoCeilings {
	return protoCeilings{
		maxReceiveEnvelopeBytes: wire.AbsoluteEnvelopeLimit,
		outputDataLimit:         uint64(protocol.MaxOutputDataLen),
	}
}

// effectiveCeilings takes the minima of both sides' advertisements.
func effectiveCeilings(local, remote protoCeilings) protoCeilings {
	return protoCeilings{
		maxReceiveEnvelopeBytes: min(local.maxReceiveEnvelopeBytes, remote.maxReceiveEnvelopeBytes),
		outputDataLimit:         min(local.outputDataLimit, remote.outputDataLimit),
	}
}

func preambleRequestToWire(ceilings protoCeilings) *wire.PreambleRequest {
	return &wire.PreambleRequest{
		Magic:                   wire.PreambleMagic,
		Epoch:                   wire.ProtocolEpoch,
		Version:                 uint32(protocol.Version),
		Role:                    &wire.PreambleRole{Role: 1},
		MaxReceiveEnvelopeBytes: ceilings.maxReceiveEnvelopeBytes,
		OutputDataLimit:         ceilings.outputDataLimit,
	}
}

func checkPreambleRequest(message *wire.PreambleRequest) (protoCeilings, error) {
	if message == nil {
		return protoCeilings{}, ErrPreambleRejected
	}
	if message.GetMagic() != wire.PreambleMagic {
		return protoCeilings{}, ErrPreambleRejected
	}
	if message.GetEpoch() != wire.ProtocolEpoch {
		return protoCeilings{}, ErrPreambleRejected
	}
	if message.GetVersion() != uint32(protocol.Version) {
		return protoCeilings{}, ErrPreambleRejected
	}
	if message.GetRole().GetRole() != 1 {
		return protoCeilings{}, ErrPreambleRejected
	}
	remote := protoCeilings{
		maxReceiveEnvelopeBytes: message.GetMaxReceiveEnvelopeBytes(),
		outputDataLimit:         message.GetOutputDataLimit(),
	}
	if remote.maxReceiveEnvelopeBytes == 0 || remote.maxReceiveEnvelopeBytes > wire.AbsoluteEnvelopeLimit {
		return protoCeilings{}, ErrPreambleRejected
	}
	if remote.outputDataLimit == 0 || remote.outputDataLimit > uint64(protocol.MaxOutputDataLen) {
		return protoCeilings{}, ErrPreambleRejected
	}
	return remote, nil
}

func preambleResponseToWire(accepted bool, ceilings protoCeilings, code uint32) *wire.PreambleResponse {
	response := &wire.PreambleResponse{
		Magic:                   wire.PreambleMagic,
		Epoch:                   wire.ProtocolEpoch,
		Version:                 uint32(protocol.Version),
		Role:                    &wire.PreambleRole{Role: 2},
		MaxReceiveEnvelopeBytes: ceilings.maxReceiveEnvelopeBytes,
		OutputDataLimit:         ceilings.outputDataLimit,
		Accepted:                accepted,
	}
	if !accepted {
		response.Rejection = &wire.PreambleRejectionCode{Code: code}
	}
	return response
}

func checkPreambleResponse(message *wire.PreambleResponse) (protoCeilings, error) {
	if message == nil {
		return protoCeilings{}, ErrPreambleRejected
	}
	if message.GetMagic() != wire.PreambleMagic {
		return protoCeilings{}, ErrPreambleRejected
	}
	if message.GetEpoch() != wire.ProtocolEpoch {
		return protoCeilings{}, ErrPreambleRejected
	}
	if message.GetVersion() != uint32(protocol.Version) {
		return protoCeilings{}, ErrPreambleRejected
	}
	if message.GetRole().GetRole() != 2 {
		return protoCeilings{}, ErrPreambleRejected
	}
	if !message.GetAccepted() {
		return protoCeilings{}, ErrPreambleRejected
	}
	remote := protoCeilings{
		maxReceiveEnvelopeBytes: message.GetMaxReceiveEnvelopeBytes(),
		outputDataLimit:         message.GetOutputDataLimit(),
	}
	if remote.maxReceiveEnvelopeBytes == 0 || remote.maxReceiveEnvelopeBytes > wire.AbsoluteEnvelopeLimit {
		return protoCeilings{}, ErrPreambleRejected
	}
	if remote.outputDataLimit == 0 || remote.outputDataLimit > uint64(protocol.MaxOutputDataLen) {
		return protoCeilings{}, ErrPreambleRejected
	}
	return remote, nil
}

// readPreambleFrame reads one length-delimited preamble envelope, enforcing
// the preamble ceiling before allocation. wire.ScanEnvelope runs first so
// unknown, repeated, concatenated, or trailing fields are rejected before
// generated unmarshal; magic/epoch/version/role are checked afterwards.
func readPreambleFrame(reader io.Reader, message proto.Message) error {
	var header [4]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return err
	}
	length := binary.BigEndian.Uint32(header[:])
	if length == 0 || length > wire.PreambleLimit {
		return wire.ErrScanLength
	}
	raw := make([]byte, length)
	if _, err := io.ReadFull(reader, raw); err != nil {
		return err
	}
	if err := wire.ScanEnvelope(message, raw); err != nil {
		return err
	}
	return proto.UnmarshalOptions{DiscardUnknown: false}.Unmarshal(raw, message)
}

// runProtoClientPreamble performs the client side over one raw envelope
// transport: send PreambleRequest first, then accept exactly one
// PreambleResponse. The caller owns the absolute deadline via ctx; it is
// never restarted here.
func runProtoClientPreamble(ctx context.Context, transport wire.Transport, ceilings protoCeilings) (protoCeilings, error) {
	type result struct {
		ceilings protoCeilings
		err      error
	}
	done := make(chan result, 1)
	go func() {
		raw, err := (proto.Marshal(preambleRequestToWire(ceilings)))
		if err != nil {
			done <- result{err: err}
			return
		}
		if err := transport.Send(wire.Envelope{Payload: raw}); err != nil {
			done <- result{err: err}
			return
		}
		envelope, err := transport.Recv()
		if err != nil {
			done <- result{err: err}
			return
		}
		if uint64(len(envelope.Payload)) > wire.PreambleLimit {
			done <- result{err: wire.ErrScanLength}
			return
		}
		var response wire.PreambleResponse
		if err := wire.ScanEnvelope(&response, envelope.Payload); err != nil {
			done <- result{err: err}
			return
		}
		if err := (proto.UnmarshalOptions{DiscardUnknown: false}.Unmarshal(envelope.Payload, &response)); err != nil {
			done <- result{err: err}
			return
		}
		remote, err := checkPreambleResponse(&response)
		if err != nil {
			done <- result{err: err}
			return
		}
		done <- result{ceilings: effectiveCeilings(ceilings, remote)}
	}()
	select {
	case <-ctx.Done():
		return protoCeilings{}, errors.Join(ErrPreambleTimeout, ctx.Err())
	case outcome := <-done:
		return outcome.ceilings, outcome.err
	}
}

// runProtoServerPreamble performs the server side: the first envelope must
// be a valid PreambleRequest, answered with acceptance or typed refusal.
func runProtoServerPreamble(ctx context.Context, transport wire.Transport, ceilings protoCeilings) (protoCeilings, error) {
	type result struct {
		ceilings protoCeilings
		err      error
	}
	done := make(chan result, 1)
	go func() {
		envelope, err := transport.Recv()
		if err != nil {
			done <- result{err: err}
			return
		}
		if uint64(len(envelope.Payload)) > wire.PreambleLimit {
			_ = sendPreambleResponse(transport, preambleResponseToWire(false, ceilings, 7))
			done <- result{err: wire.ErrScanLength}
			return
		}
		var request wire.PreambleRequest
		if err := wire.ScanEnvelope(&request, envelope.Payload); err != nil {
			_ = sendPreambleResponse(transport, preambleResponseToWire(false, ceilings, rejectionCodeFor(err)))
			done <- result{err: err}
			return
		}
		if err := (proto.UnmarshalOptions{DiscardUnknown: false}.Unmarshal(envelope.Payload, &request)); err != nil {
			_ = sendPreambleResponse(transport, preambleResponseToWire(false, ceilings, rejectionCodeFor(err)))
			done <- result{err: err}
			return
		}
		remote, err := checkPreambleRequest(&request)
		if err != nil {
			_ = sendPreambleResponse(transport, preambleResponseToWire(false, ceilings, rejectionCodeFor(err)))
			done <- result{err: err}
			return
		}
		effective := effectiveCeilings(ceilings, remote)
		if err := sendPreambleResponse(transport, preambleResponseToWire(true, effective, 0)); err != nil {
			done <- result{err: err}
			return
		}
		done <- result{ceilings: effective}
	}()
	select {
	case <-ctx.Done():
		return protoCeilings{}, errors.Join(ErrPreambleTimeout, ctx.Err())
	case outcome := <-done:
		return outcome.ceilings, outcome.err
	}
}

func sendPreambleResponse(transport wire.Transport, response *wire.PreambleResponse) error {
	raw, err := (proto.Marshal(response))
	if err != nil {
		return err
	}
	return transport.Send(wire.Envelope{Payload: raw})
}

func rejectionCodeFor(err error) uint32 {
	if errors.Is(err, ErrPreambleRejected) {
		return 3 // version mismatch is the common typed refusal
	}
	return 7
}
