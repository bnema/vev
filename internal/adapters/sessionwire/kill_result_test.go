package sessionwire

// Kill/KillResult wire contract: a Kill request carries a unique nonzero
// RequestID, exactly one correlated KillResult answers it, and both directions
// fail closed on malformed, wrong-direction, truncated, trailing, or oversize
// bytes. The result is a bounded control envelope: it never rides the
// Output/bulk ceiling exemption.

import (
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
)

func testKillResult() protocol.KillResult {
	return protocol.KillResult{
		RequestID: 0x1122334455,
		Outcome:   protocol.KillFailed,
		Code:      protocol.ErrNoSuchSession,
		Text:      "no such session: work",
		Failures: []protocol.KillFailure{
			{Class: "live", Name: "one", Text: "boom"},
			{Class: "stopped", Name: "two", Text: "gone"},
		},
	}
}

// TestKillResultByteRoundTrip proves one semantic KillResult re-encodes to
// byte-identical bytes after a scan + generated unmarshal + semantic decode.
func TestKillResultByteRoundTrip(t *testing.T) {
	message := testKillResult()
	raw, err := EncodeServerMessage(message)
	require.NoError(t, err)
	require.NoError(t, wire.ScanEnvelope(&wire.ServerEnvelope{}, raw))

	decoded, err := DecodeServerEnvelope(raw)
	require.NoError(t, err)
	require.Equal(t, message, decoded)

	reencoded, err := EncodeServerMessage(decoded)
	require.NoError(t, err)
	require.Equal(t, raw, reencoded, "KillResult must re-encode byte-for-byte")
}

// TestKillRequestByteRoundTrip proves one Kill request with a unique nonzero
// RequestID survives the client direction byte-for-byte.
func TestKillRequestByteRoundTrip(t *testing.T) {
	message := protocol.Kill{RequestID: 0xDEADBEEF, Name: "work", Scope: protocol.KillSession}
	raw, err := EncodeClientMessage(message)
	require.NoError(t, err)
	require.NoError(t, wire.ScanEnvelope(&wire.ClientEnvelope{}, raw))

	decoded, err := DecodeClientEnvelope(raw)
	require.NoError(t, err)
	require.Equal(t, message, decoded)

	reencoded, err := EncodeClientMessage(decoded)
	require.NoError(t, err)
	require.Equal(t, raw, reencoded, "Kill must re-encode byte-for-byte")
}

// TestKillResultWrongDirectionFailsClosed proves a server-only KillResult
// payload is refused on the client direction and the server decoder rejects a
// nil envelope, so a result can never be consumed as a request.
func TestKillResultWrongDirectionFailsClosed(t *testing.T) {
	raw, err := proto.Marshal(&wire.ServerEnvelope{Payload: &wire.ServerEnvelope_KillResult{KillResult: killResultToWire(testKillResult())}})
	require.NoError(t, err)

	var clientEnvelope wire.ClientEnvelope
	err = wire.ScanEnvelope(&clientEnvelope, raw)
	require.Error(t, err, "a server KillResult must not scan as a client envelope")

	_, err = decodeProtoClient(&wire.ClientEnvelope{Payload: nil})
	require.ErrorIs(t, err, ErrWrongDirection)
	_, err = decodeProtoServer(nil)
	require.ErrorIs(t, err, ErrInvalidMessage)
}

// TestKillAndKillResultRejectMissingOrOutOfRangeIdentity proves the decode
// boundary refuses a zero RequestID, an unknown outcome, an out-of-range code,
// and a nil failure entry instead of aliasing them into valid values.
func TestKillAndKillResultRejectMissingOrOutOfRangeIdentity(t *testing.T) {
	tests := []struct {
		name    string
		run     func(t *testing.T) error
		wantErr error
	}{
		{
			name: "kill zero request id",
			run: func(t *testing.T) error {
				_, err := killFromWire(&wire.Kill{Name: "work", Scope: uint32(protocol.KillSession)})
				return err
			},
			wantErr: errProtoConvertRange,
		},
		{
			name:    "kill nil",
			run:     func(t *testing.T) error { _, err := killFromWire(nil); return err },
			wantErr: errProtoConvertRange,
		},
		{
			name:    "kill result nil",
			run:     func(t *testing.T) error { _, err := killResultFromWire(nil); return err },
			wantErr: errProtoConvertRange,
		},
		{
			name: "kill result zero request id",
			run: func(t *testing.T) error {
				_, err := killResultFromWire(&wire.KillResult{RequestId: 0, Outcome: uint32(protocol.KillSucceeded)})
				return err
			},
			wantErr: errProtoConvertRange,
		},
		{
			name: "kill result unknown outcome below range",
			run: func(t *testing.T) error {
				_, err := killResultFromWire(&wire.KillResult{RequestId: 1, Outcome: 0})
				return err
			},
			wantErr: errProtoConvertRange,
		},
		{
			name: "kill result unknown outcome above range",
			run: func(t *testing.T) error {
				_, err := killResultFromWire(&wire.KillResult{RequestId: 1, Outcome: uint32(protocol.KillOutcomeUnknown) + 1})
				return err
			},
			wantErr: errProtoConvertRange,
		},
		{
			name: "kill result code overflow",
			run: func(t *testing.T) error {
				_, err := killResultFromWire(&wire.KillResult{RequestId: 1, Outcome: uint32(protocol.KillFailed), Code: 1 << 16})
				return err
			},
			wantErr: errProtoConvertRange,
		},
		{
			name: "kill result nil failure entry",
			run: func(t *testing.T) error {
				_, err := killResultFromWire(&wire.KillResult{RequestId: 1, Outcome: uint32(protocol.KillFailed), Failures: []*wire.KillFailure{nil}})
				return err
			},
			wantErr: errProtoConvertRange,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.ErrorIs(t, tt.run(t), tt.wantErr)
		})
	}
}

// TestKillResultConversionArmInventory is the sessionwire arm inventory for the
// explicit kill result: every arm of the kill encoders is exercised (server
// result value/pointer, client request value/pointer), each arm shares the
// value arm's bytes, and a typed nil pointer arm fails closed with
// ErrInvalidMessage instead of panicking. A new arm added without coverage fails
// here.
func TestKillResultConversionArmInventory(t *testing.T) {
	result := testKillResult()
	resultRaw, err := EncodeServerMessage(result)
	require.NoError(t, err)
	request := protocol.Kill{RequestID: 0xDEADBEEF, Name: "work", Scope: protocol.KillSession}
	requestRaw, err := EncodeClientMessage(request)
	require.NoError(t, err)

	tests := []struct {
		name string
		// run encodes one arm and returns the bytes and any error.
		run     func() ([]byte, error)
		wantRaw []byte
		// wantErr, when non-nil, is the required failure; otherwise run must
		// succeed with wantRaw.
		wantErr error
	}{
		{name: "result value arm", run: func() ([]byte, error) { return EncodeServerMessage(result) }, wantRaw: resultRaw},
		{name: "result pointer arm", run: func() ([]byte, error) { return EncodeServerMessage(&result) }, wantRaw: resultRaw},
		{name: "result typed nil pointer arm", run: func() ([]byte, error) { return EncodeServerMessage((*protocol.KillResult)(nil)) }, wantErr: ErrInvalidMessage},
		{name: "request value arm", run: func() ([]byte, error) { return EncodeClientMessage(request) }, wantRaw: requestRaw},
		{name: "request pointer arm", run: func() ([]byte, error) { return EncodeClientMessage(&request) }, wantRaw: requestRaw},
		{name: "request typed nil pointer arm", run: func() ([]byte, error) { return EncodeClientMessage((*protocol.Kill)(nil)) }, wantErr: ErrInvalidMessage},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, err := tt.run()
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantRaw, raw, "every arm must share the value arm's bytes")
		})
	}

	// The pointer arm decodes back to the same semantic value, so it is not a
	// side channel around validation.
	decoded, err := DecodeServerEnvelope(resultRaw)
	require.NoError(t, err)
	require.Equal(t, result, decoded)
}

// TestKillResultEnvelopeStrictScan proves the scan boundary rejects a
// truncated payload, trailing bytes after a complete variant, and a
// concatenated duplicate before any semantic decode.
func TestKillResultEnvelopeStrictScan(t *testing.T) {
	valid, err := EncodeServerMessage(testKillResult())
	require.NoError(t, err)
	require.NoError(t, wire.ScanEnvelope(&wire.ServerEnvelope{}, valid))

	require.Error(t, wire.ScanEnvelope(&wire.ServerEnvelope{}, valid[:len(valid)-1]))
	require.Error(t, wire.ScanEnvelope(&wire.ServerEnvelope{}, append(append([]byte(nil), valid...), 0xFF)))
	require.Error(t, wire.ScanEnvelope(&wire.ServerEnvelope{}, append(append([]byte(nil), valid...), valid...)))

	_, err = DecodeServerEnvelope(valid[:len(valid)-1])
	require.Error(t, err)
	var failure *protocol.DecodeFailure
	require.ErrorAs(t, err, &failure)
	require.Equal(t, protocol.DecodeMalformed, failure.Category)
}

// TestKillResultIsBoundedControlEnvelope proves a KillResult rides the strict
// control ceiling, never the bulk exemption, on both send and receive.
func TestKillResultIsBoundedControlEnvelope(t *testing.T) {
	message := testKillResult()

	raw, err := EncodeServerMessage(message)
	require.NoError(t, err)
	require.Equal(t, categoryControl, serverEnvelopeCategory(raw), "a KillResult is a control envelope")

	// A text sized so the serialized control envelope crosses the 1 MiB
	// ceiling while the semantic value itself is only the declared cap.
	oversize, err := EncodeServerMessage(protocol.KillResult{RequestID: 1, Outcome: protocol.KillSucceeded, Text: string(make([]byte, wire.ControlEnvelopeLimit))})
	require.NoError(t, err)
	require.ErrorIs(t, checkCategoryCeiling(oversize, categoryControl, wire.AbsoluteEnvelopeLimit), wire.ErrScanLength,
		"a KillResult above the control ceiling must fail even under an absolute negotiated ceiling")

	send := &serverConnection{raw: &scriptedTransport{}, ceilings: defaultProtoCeilings()}
	require.NoError(t, send.SendServer(message))
	require.ErrorIs(t, send.SendServer(protocol.KillResult{RequestID: 1, Outcome: protocol.KillSucceeded, Text: string(make([]byte, wire.ControlEnvelopeLimit))}), wire.ErrScanLength)

	recv := &clientConnection{raw: &scriptedTransport{recv: []wire.Envelope{{Payload: oversize}}}, ceilings: defaultProtoCeilings()}
	recv.preambleOnce.Do(func() {})
	_, err = recv.ReceiveServer()
	require.Error(t, err)
}
