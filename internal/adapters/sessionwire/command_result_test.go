package sessionwire

// CommandResult wire contract: one command request carries exactly one closed
// terminal outcome (Succeeded, Failed, or Unknown), correlation is preserved
// byte-for-byte, and the decode boundary refuses a malformed, unspecified, or
// out-of-range outcome instead of aliasing it into a valid one. The result is a
// bounded control envelope: a failure's Code and Text can never ride the
// Output/bulk ceiling exemption.

import (
	"encoding/hex"
	"math"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
)

func testCommandResults() []protocol.CommandResult {
	return []protocol.CommandResult{
		{RequestID: 0x1122334455, Outcome: protocol.CommandSucceeded, Output: "work\tsh\n"},
		{RequestID: 7, Outcome: protocol.CommandFailed, Code: protocol.ErrNoSuchTarget, Text: "no live sessions"},
		{RequestID: 8, Outcome: protocol.CommandOutcomeUnknown, Text: "command request timed out"},
	}
}

// commandResultPinnedBytes pins the exact serialized bytes of each fixture in
// testCommandResults order, so a schema, encoder, or envelope-variant change
// that silently re-lays-out a CommandResult fails loudly here.
var commandResultPinnedBytes = []string{
	"3a1308d588cd9192022a08776f726b0973680a3001",
	"3a180807180922106e6f206c6976652073657373696f6e733002",
	"3a1f08082219636f6d6d616e6420726571756573742074696d6564206f75743003",
}

// TestCommandResultByteRoundTrip proves every closed outcome has pinned
// serialized bytes and re-encodes to byte-identical bytes after a strict scan +
// generated unmarshal + semantic decode, with the request correlation id and the
// bounded text/output intact.
func TestCommandResultByteRoundTrip(t *testing.T) {
	for index, message := range testCommandResults() {
		require.True(t, message.Valid(), "fixture %+v must obey the outcome contract", message)
		raw, err := EncodeServerMessage(message)
		require.NoError(t, err)
		require.Equal(t, commandResultPinnedBytes[index], hex.EncodeToString(raw), "CommandResult %d must keep its pinned bytes", index)
		require.Equal(t, categoryControl, serverEnvelopeCategory(raw), "a CommandResult is a control envelope")
		require.NoError(t, wire.ScanEnvelope(&wire.ServerEnvelope{}, raw))

		decoded, err := DecodeServerEnvelope(raw)
		require.NoError(t, err)
		require.Equal(t, message, decoded)

		reencoded, err := EncodeServerMessage(decoded)
		require.NoError(t, err)
		require.Equal(t, raw, reencoded, "CommandResult must re-encode byte-for-byte")
	}
}

// TestCommandResultEnvelopeStrictScan proves the scan boundary rejects a
// truncated prefix, a truncated payload, trailing garbage, and a concatenated
// duplicate before any semantic decode, and that each rejected frame surfaces
// as a malformed decode failure rather than a valid result.
func TestCommandResultEnvelopeStrictScan(t *testing.T) {
	valid, err := EncodeServerMessage(testCommandResults()[0])
	require.NoError(t, err)
	require.NoError(t, wire.ScanEnvelope(&wire.ServerEnvelope{}, valid))

	clone := func(raw []byte) []byte { return append([]byte(nil), raw...) }
	tests := []struct {
		name    string
		raw     []byte
		wantErr error
	}{
		{name: "truncated tag prefix", raw: valid[:1], wantErr: wire.ErrScanTruncated},
		{name: "truncated length prefix", raw: valid[:3], wantErr: wire.ErrScanTruncated},
		{name: "truncated payload", raw: valid[:len(valid)-1], wantErr: wire.ErrScanTruncated},
		{name: "trailing garbage", raw: append(clone(valid), 0xFF), wantErr: wire.ErrScanTruncated},
		{name: "concatenated duplicate", raw: append(clone(valid), valid...), wantErr: wire.ErrScanDuplicate},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.ErrorIs(t, wire.ScanEnvelope(&wire.ServerEnvelope{}, tt.raw), tt.wantErr)

			_, decodeErr := DecodeServerEnvelope(tt.raw)
			var failure *protocol.DecodeFailure
			require.ErrorAs(t, decodeErr, &failure)
			require.Equal(t, protocol.DecodeMalformed, failure.Category, "a corrupt CommandResult frame is malformed, never a result")
		})
	}

	// Every proper prefix of a valid frame is a truncated prefix in one of the
	// leading tag, length, or nested-payload bytes; none may scan or decode.
	t.Run("every truncated prefix", func(t *testing.T) {
		for length := 1; length < len(valid); length++ {
			require.ErrorIs(t, wire.ScanEnvelope(&wire.ServerEnvelope{}, valid[:length]), wire.ErrScanTruncated, "prefix %d", length)
			_, decodeErr := DecodeServerEnvelope(valid[:length])
			var failure *protocol.DecodeFailure
			require.ErrorAs(t, decodeErr, &failure, "prefix %d", length)
			require.Equal(t, protocol.DecodeMalformed, failure.Category, "prefix %d", length)
		}
	})
}

// TestCommandResultWrongDirectionFailsClosed proves a server-only CommandResult
// payload is refused on the client direction and the server decoder rejects a
// nil envelope, so a result can never be consumed as a request.
func TestCommandResultWrongDirectionFailsClosed(t *testing.T) {
	converted, err := commandResultToWire(testCommandResults()[0])
	require.NoError(t, err)
	raw, err := proto.Marshal(&wire.ServerEnvelope{Payload: &wire.ServerEnvelope_CommandResult{
		CommandResult: converted,
	}})
	require.NoError(t, err)

	var clientEnvelope wire.ClientEnvelope
	err = wire.ScanEnvelope(&clientEnvelope, raw)
	require.Error(t, err, "a server CommandResult must not scan as a client envelope")

	_, err = decodeProtoClient(&wire.ClientEnvelope{Payload: nil})
	require.ErrorIs(t, err, ErrWrongDirection)
	_, err = decodeProtoServer(nil)
	require.ErrorIs(t, err, ErrInvalidMessage)
}

// TestCommandResultRejectsMalformedZeroAndOutOfRangeOutcome proves the decode
// boundary refuses an unspecified (zero) outcome, an outcome outside the closed
// enumeration, and every outcome/code/text combination the contract forbids.
// The out-of-range cases include values whose low byte would otherwise truncate
// into a valid outcome (257 -> 1, 259 -> 3, 65537 -> 1), so a hostile or corrupt
// frame can never alias success or unknown.
func TestCommandResultRejectsMalformedZeroAndOutOfRangeOutcome(t *testing.T) {
	failed := wire.CommandOutcome_COMMAND_OUTCOME_FAILED
	tests := []struct {
		name string
		raw  *wire.CommandResult
	}{
		{name: "nil result"},
		{name: "unspecified zero outcome", raw: &wire.CommandResult{RequestId: 1}},
		{name: "outcome one above range", raw: &wire.CommandResult{RequestId: 1, Outcome: failed + 2}},
		{name: "outcome far above range", raw: &wire.CommandResult{RequestId: 1, Outcome: 255}},
		{name: "outcome truncating into succeeded", raw: &wire.CommandResult{RequestId: 1, Outcome: 257}},
		{name: "outcome truncating into unknown", raw: &wire.CommandResult{RequestId: 1, Outcome: 259}},
		{name: "outcome truncating at 16 bits", raw: &wire.CommandResult{RequestId: 1, Outcome: 65537}},
		{name: "outcome at int32 maximum", raw: &wire.CommandResult{RequestId: 1, Outcome: wire.CommandOutcome(math.MaxInt32)}},
		{name: "negative outcome", raw: &wire.CommandResult{RequestId: 1, Outcome: -1}},
		{name: "code overflows uint16", raw: &wire.CommandResult{RequestId: 1, Outcome: wire.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED, Code: 1 << 16}},
		{name: "succeeded with a code", raw: &wire.CommandResult{RequestId: 1, Outcome: wire.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED, Code: uint32(protocol.ErrInternal)}},
		{name: "succeeded with text", raw: &wire.CommandResult{RequestId: 1, Outcome: wire.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED, Text: "boom"}},
		{name: "failed without a code", raw: &wire.CommandResult{RequestId: 1, Outcome: failed}},
		{name: "failed with output", raw: &wire.CommandResult{RequestId: 1, Outcome: failed, Code: uint32(protocol.ErrInternal), Output: "leak"}},
		{name: "unknown with a code", raw: &wire.CommandResult{RequestId: 1, Outcome: wire.CommandOutcome_COMMAND_OUTCOME_UNKNOWN, Code: uint32(protocol.ErrInternal)}},
		{name: "unknown with output", raw: &wire.CommandResult{RequestId: 1, Outcome: wire.CommandOutcome_COMMAND_OUTCOME_UNKNOWN, Output: "leak"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := commandResultFromWire(tt.raw)
			require.ErrorIs(t, err, errProtoConvertRange)

			if tt.raw == nil {
				return
			}
			// The serialized boundary must refuse the same frame end to end:
			// a well-formed protobuf with a forbidden value is still refused.
			payload, marshalErr := proto.Marshal(&wire.ServerEnvelope{Payload: &wire.ServerEnvelope_CommandResult{CommandResult: tt.raw}})
			require.NoError(t, marshalErr)
			_, decodeErr := DecodeServerEnvelope(payload)
			require.Error(t, decodeErr, "a forbidden outcome must never decode into a semantic result")
			var failure *protocol.DecodeFailure
			require.ErrorAs(t, decodeErr, &failure)
		})
	}
}

// TestCommandResultConversionArmInventory is the sessionwire arm inventory for
// the explicit command result: value and pointer arms share the value arm's
// bytes, and a typed nil pointer arm fails closed with ErrInvalidMessage instead
// of panicking. A new arm added without coverage fails here.
func TestCommandResultConversionArmInventory(t *testing.T) {
	message := testCommandResults()[0]
	valueRaw, err := EncodeServerMessage(message)
	require.NoError(t, err)

	tests := []struct {
		name    string
		run     func() ([]byte, error)
		wantRaw []byte
		wantErr error
	}{
		{name: "value arm", run: func() ([]byte, error) { return EncodeServerMessage(message) }, wantRaw: valueRaw},
		{name: "pointer arm", run: func() ([]byte, error) { return EncodeServerMessage(&message) }, wantRaw: valueRaw},
		{name: "typed nil pointer arm", run: func() ([]byte, error) { return EncodeServerMessage((*protocol.CommandResult)(nil)) }, wantErr: ErrInvalidMessage},
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

	decoded, err := DecodeServerEnvelope(valueRaw)
	require.NoError(t, err)
	require.Equal(t, message, decoded)
}

// TestCommandResultIsBoundedControlEnvelope proves a CommandResult rides the
// strict control ceiling, never the bulk exemption, on send and receive, so a
// failure's error text stays inside the documented 1 MiB control bound.
func TestCommandResultIsBoundedControlEnvelope(t *testing.T) {
	message := testCommandResults()[1]

	oversize := protocol.CommandResult{
		RequestID: 1,
		Outcome:   protocol.CommandFailed,
		Code:      protocol.ErrInternal,
		Text:      string(make([]byte, wire.ControlEnvelopeLimit)),
	}
	oversizeRaw, err := EncodeServerMessage(oversize)
	require.NoError(t, err)
	require.ErrorIs(t, checkCategoryCeiling(oversizeRaw, categoryControl, wire.AbsoluteEnvelopeLimit), wire.ErrScanLength,
		"a CommandResult above the control ceiling must fail even under an absolute negotiated ceiling")

	send := &serverConnection{raw: &scriptedTransport{}, ceilings: defaultProtoCeilings()}
	require.NoError(t, send.SendServer(message))
	require.ErrorIs(t, send.SendServer(oversize), wire.ErrScanLength)

	recv := &clientConnection{raw: &scriptedTransport{recv: []wire.Envelope{{Payload: oversizeRaw}}}, ceilings: defaultProtoCeilings()}
	recv.preambleOnce.Do(func() {})
	_, err = recv.ReceiveServer()
	require.Error(t, err)
}
