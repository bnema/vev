package brokerwire

// Membership wire contract (Phase C1).
//
// AddHost carries a required policy, RemoveHost carries exact registration
// authority instead of an endpoint name, UpdateHostPolicy carries exact
// authority plus the replacement policy, and every result carries the
// presence-carrying outcome of the request it completes. These tests pin the
// stateless codec's half of that contract: byte round trips, direction
// closure, strict scan before unmarshal, exact generation preservation, the
// request/result presence rules, and the typed error codes 11/12.

import (
	"math"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
)

// membershipRequests are the three mutating client requests with their frozen
// envelope tags.
func membershipRequests(connection ports.BrokerConnectionID, operation ports.BrokerOperationID) []struct {
	name    string
	message ClientMessage
	tag     protowire.Number
} {
	return []struct {
		name    string
		message ClientMessage
		tag     protowire.Number
	}{
		{"add_host", AddHost{Epoch: 7, Connection: connection, Operation: operation, Endpoint: "dev@host:22", Policy: testPolicy()}, 105},
		{"remove_host", RemoveHost{Epoch: 7, Connection: connection, Operation: operation, Registration: testRegistration()}, 106},
		{"update_host_policy", UpdateHostPolicy{Epoch: 7, Connection: connection, Operation: operation, Registration: testRegistration(), Policy: testPolicy()}, 111},
	}
}

// TestMembershipRequestByteRoundTrip proves each mutating request travels on
// its frozen tag and survives an encode/decode/re-encode cycle byte for byte,
// including the generation that fences a removal and the full policy.
func TestMembershipRequestByteRoundTrip(t *testing.T) {
	connection := testConnectionID(0x21)
	operation := testOperationID(0x31)
	for _, tc := range membershipRequests(connection, operation) {
		t.Run(tc.name, func(t *testing.T) {
			raw := mustEncodeClient(t, tc.message)
			number, wireType, consumed := protowire.ConsumeTag(raw)
			require.Greater(t, consumed, 0)
			require.Equal(t, tc.tag, number)
			require.Equal(t, protowire.BytesType, wireType)

			decoded, err := DecodeClient(raw, testEnvelopeCeiling, testChunkCeiling)
			require.NoError(t, err)
			require.Equal(t, tc.message, decoded)

			again, err := EncodeClient(decoded, testEnvelopeCeiling, testChunkCeiling)
			require.NoError(t, err)
			require.Equal(t, raw, again, "re-encode must be byte-for-byte identical")
		})
	}
}

// TestMembershipResultByteRoundTrip proves every legal result shape round-trips
// byte for byte: an authoritative success carrying its registration, an
// idempotent removal success carrying none, and a failed result carrying only
// its typed error.
func TestMembershipResultByteRoundTrip(t *testing.T) {
	connection := testConnectionID(0x22)
	operation := testOperationID(0x32)
	messages := []struct {
		name    string
		message OperationResult
	}{
		{"add success carries registration", OperationResult{
			Epoch: 7, Connection: connection, Operation: operation,
			Outcome: ports.BrokerOutcomeOK, Registration: testRegistration(),
		}},
		{"update success carries advanced registration", OperationResult{
			Epoch: 7, Connection: connection, Operation: operation,
			Outcome: ports.BrokerOutcomeOK, Registration: domain.RemoteRegistration{
				Endpoint: "dev@host:22", Incarnation: testRegistration().Incarnation, Generation: 10,
			},
		}},
		{"remove success reports removal only", OperationResult{
			Epoch: 7, Connection: connection, Operation: operation,
			Outcome: ports.BrokerOutcomeOK, Removed: true,
		}},
		{"idempotent remove reports not removed", OperationResult{
			Epoch: 7, Connection: connection, Operation: operation,
			Outcome: ports.BrokerOutcomeOK,
		}},
		{"failed carries only its error", OperationResult{
			Epoch: 7, Connection: connection, Operation: operation,
			Outcome: ports.BrokerOutcomeFailed, Error: testErrorDetail(), HasError: true,
		}},
		{"unknown carries only its error", OperationResult{
			Epoch: 7, Connection: connection, Operation: operation,
			Outcome: ports.BrokerOutcomeUnknown,
			Error:   ErrorDetail{Code: ports.BrokerErrorOutcomeUnknown}, HasError: true,
		}},
	}
	for _, tc := range messages {
		t.Run(tc.name, func(t *testing.T) {
			raw := mustEncodeServer(t, tc.message)
			number, wireType, consumed := protowire.ConsumeTag(raw)
			require.Greater(t, consumed, 0)
			require.Equal(t, protowire.Number(203), number)
			require.Equal(t, protowire.BytesType, wireType)

			decoded, err := DecodeServer(raw, testEnvelopeCeiling, testChunkCeiling)
			require.NoError(t, err)
			require.Equal(t, tc.message, decoded)

			again, err := EncodeServer(decoded, testEnvelopeCeiling, testChunkCeiling)
			require.NoError(t, err)
			require.Equal(t, raw, again, "re-encode must be byte-for-byte identical")
		})
	}
}

// TestMembershipExactGenerationPreserved proves the generation that fences a
// remove/re-add race survives the wire exactly, including its extremes: a
// generation is never widened, truncated, or re-derived from time.
func TestMembershipExactGenerationPreserved(t *testing.T) {
	connection := testConnectionID(0x23)
	operation := testOperationID(0x33)
	base := testRegistration()
	for name, generation := range map[string]domain.RemoteGeneration{
		"one":        1,
		"large":      1 << 40,
		"max uint64": math.MaxUint64,
	} {
		t.Run(name, func(t *testing.T) {
			registration := base
			registration.Generation = generation

			request := RemoveHost{Epoch: 7, Connection: connection, Operation: operation, Registration: registration}
			decoded, err := DecodeClient(mustEncodeClient(t, request), testEnvelopeCeiling, testChunkCeiling)
			require.NoError(t, err)
			require.Equal(t, request, decoded)
			require.Equal(t, generation, decoded.(RemoveHost).Registration.Generation)

			update := UpdateHostPolicy{Epoch: 7, Connection: connection, Operation: operation, Registration: registration, Policy: testPolicy()}
			decodedUpdate, err := DecodeClient(mustEncodeClient(t, update), testEnvelopeCeiling, testChunkCeiling)
			require.NoError(t, err)
			require.Equal(t, update, decodedUpdate)
			require.Equal(t, generation, decodedUpdate.(UpdateHostPolicy).Registration.Generation)

			result := OperationResult{Epoch: 7, Connection: connection, Operation: operation, Outcome: ports.BrokerOutcomeOK, Registration: registration}
			decodedResult, err := DecodeServer(mustEncodeServer(t, result), testEnvelopeCeiling, testChunkCeiling)
			require.NoError(t, err)
			require.Equal(t, result, decodedResult)
			require.Equal(t, generation, decodedResult.(OperationResult).Registration.Generation)
		})
	}
}

// TestMembershipWrongDirection proves direction stays structural: a mutating
// request never decodes as server output and a result never decodes as a
// request. Misuse is refused by the strict scanner (the tag is unknown to the
// other envelope) before generated unmarshal; the typed direction refusal is
// already covered by the closed directional interfaces and
// TestBrokerWrongDirection.
func TestMembershipWrongDirection(t *testing.T) {
	connection := testConnectionID(0x24)
	operation := testOperationID(0x34)
	for _, tc := range membershipRequests(connection, operation) {
		t.Run("request/"+tc.name, func(t *testing.T) {
			raw := mustEncodeClient(t, tc.message)
			decoded, err := DecodeServer(raw, testEnvelopeCeiling, testChunkCeiling)
			require.Error(t, err)
			require.Nil(t, decoded, "no server message may be decoded from a client frame")
		})
	}
	result := OperationResult{Epoch: 7, Connection: connection, Operation: operation, Outcome: ports.BrokerOutcomeOK, Registration: testRegistration()}
	raw := mustEncodeServer(t, result)
	decoded, err := DecodeClient(raw, testEnvelopeCeiling, testChunkCeiling)
	require.Error(t, err)
	require.Nil(t, decoded, "no client message may be decoded from a server frame")
}

// TestMembershipMissingAuthorityRefused proves an absent policy or an absent
// registration is not authority: the decode path refuses both rather than
// inventing a default, on encode and on receive.
func TestMembershipMissingAuthorityRefused(t *testing.T) {
	connection := testConnectionID(0x25)
	operation := testOperationID(0x35)

	clearFields := func(t *testing.T, message ClientMessage, clear func(*wire.BrokerClientEnvelope)) []byte {
		t.Helper()
		raw := mustEncodeClient(t, message)
		envelope := &wire.BrokerClientEnvelope{}
		require.NoError(t, wire.ScanEnvelope(envelope, raw))
		require.NoError(t, proto.Unmarshal(raw, envelope))
		clear(envelope)
		mutated, err := proto.Marshal(envelope)
		require.NoError(t, err)
		return mutated
	}

	t.Run("add_host without policy", func(t *testing.T) {
		_, err := EncodeClient(AddHost{Epoch: 7, Connection: connection, Operation: operation, Endpoint: "dev@host:22"}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
		mutated := clearFields(t, AddHost{Epoch: 7, Connection: connection, Operation: operation, Endpoint: "dev@host:22", Policy: testPolicy()}, func(envelope *wire.BrokerClientEnvelope) {
			envelope.GetAddHost().Policy = nil
		})
		_, err = DecodeClient(mutated, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
	})

	t.Run("add_host with invalid policy", func(t *testing.T) {
		_, err := EncodeClient(AddHost{Epoch: 7, Connection: connection, Operation: operation, Endpoint: "dev@host:22", Policy: ports.BrokerPolicy{}}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
		mutated := clearFields(t, AddHost{Epoch: 7, Connection: connection, Operation: operation, Endpoint: "dev@host:22", Policy: testPolicy()}, func(envelope *wire.BrokerClientEnvelope) {
			envelope.GetAddHost().GetPolicy().EnvironmentPolicy = 99
		})
		_, err = DecodeClient(mutated, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
	})

	t.Run("remove_host without registration", func(t *testing.T) {
		_, err := EncodeClient(RemoveHost{Epoch: 7, Connection: connection, Operation: operation}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
		mutated := clearFields(t, RemoveHost{Epoch: 7, Connection: connection, Operation: operation, Registration: testRegistration()}, func(envelope *wire.BrokerClientEnvelope) {
			envelope.GetRemoveHost().Registration = nil
		})
		_, err = DecodeClient(mutated, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
	})

	t.Run("remove_host with partial registration", func(t *testing.T) {
		partial := testRegistration()
		partial.Incarnation = [16]byte{}
		_, err := EncodeClient(RemoveHost{Epoch: 7, Connection: connection, Operation: operation, Registration: partial}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)

		shortIncarnation := clearFields(t, RemoveHost{Epoch: 7, Connection: connection, Operation: operation, Registration: testRegistration()}, func(envelope *wire.BrokerClientEnvelope) {
			envelope.GetRemoveHost().GetRegistration().Incarnation = []byte{1, 2, 3}
		})
		_, err = DecodeClient(shortIncarnation, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)

		zeroGeneration := clearFields(t, RemoveHost{Epoch: 7, Connection: connection, Operation: operation, Registration: testRegistration()}, func(envelope *wire.BrokerClientEnvelope) {
			envelope.GetRemoveHost().GetRegistration().Generation = 0
		})
		_, err = DecodeClient(zeroGeneration, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)

		badEndpoint := clearFields(t, RemoveHost{Epoch: 7, Connection: connection, Operation: operation, Registration: testRegistration()}, func(envelope *wire.BrokerClientEnvelope) {
			envelope.GetRemoveHost().GetRegistration().Endpoint = "two words"
		})
		_, err = DecodeClient(badEndpoint, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
	})

	t.Run("update_host_policy missing registration or policy", func(t *testing.T) {
		_, err := EncodeClient(UpdateHostPolicy{Epoch: 7, Connection: connection, Operation: operation, Policy: testPolicy()}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
		_, err = EncodeClient(UpdateHostPolicy{Epoch: 7, Connection: connection, Operation: operation, Registration: testRegistration()}, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)

		update := UpdateHostPolicy{Epoch: 7, Connection: connection, Operation: operation, Registration: testRegistration(), Policy: testPolicy()}
		missingPolicy := clearFields(t, update, func(envelope *wire.BrokerClientEnvelope) {
			envelope.GetUpdateHostPolicy().Policy = nil
		})
		_, err = DecodeClient(missingPolicy, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
		missingRegistration := clearFields(t, update, func(envelope *wire.BrokerClientEnvelope) {
			envelope.GetUpdateHostPolicy().Registration = nil
		})
		_, err = DecodeClient(missingRegistration, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
	})
}

// TestMembershipResultPresenceMismatchRefused proves the result presence rules
// fail closed in both directions of the codec: removal never travels with a
// registration, a failed or outcome-unknown result carries neither authority
// and must carry its error, and a success carries no error.
func TestMembershipResultPresenceMismatchRefused(t *testing.T) {
	connection := testConnectionID(0x26)
	operation := testOperationID(0x36)
	clearRegistration := func(t *testing.T) []byte {
		t.Helper()
		raw := mustEncodeServer(t, OperationResult{Epoch: 7, Connection: connection, Operation: operation, Outcome: ports.BrokerOutcomeOK, Registration: testRegistration()})
		envelope := &wire.BrokerServerEnvelope{}
		require.NoError(t, wire.ScanEnvelope(envelope, raw))
		require.NoError(t, proto.Unmarshal(raw, envelope))
		envelope.GetOperationResult().Registration.Generation = 0
		mutated, err := proto.Marshal(envelope)
		require.NoError(t, err)
		return mutated
	}

	for name, message := range map[string]OperationResult{
		"removed and registration together": {
			Epoch: 7, Connection: connection, Operation: operation,
			Outcome: ports.BrokerOutcomeOK, Removed: true, Registration: testRegistration(),
		},
		"failed with registration": {
			Epoch: 7, Connection: connection, Operation: operation,
			Outcome: ports.BrokerOutcomeFailed, Registration: testRegistration(), Error: testErrorDetail(), HasError: true,
		},
		"failed with removal": {
			Epoch: 7, Connection: connection, Operation: operation,
			Outcome: ports.BrokerOutcomeFailed, Removed: true, Error: testErrorDetail(), HasError: true,
		},
		"failed without error": {
			Epoch: 7, Connection: connection, Operation: operation,
			Outcome: ports.BrokerOutcomeFailed,
		},
		"unknown without error": {
			Epoch: 7, Connection: connection, Operation: operation,
			Outcome: ports.BrokerOutcomeUnknown,
		},
		"success with error": {
			Epoch: 7, Connection: connection, Operation: operation,
			Outcome: ports.BrokerOutcomeOK, Registration: testRegistration(), Error: testErrorDetail(), HasError: true,
		},
		"success with invalid registration": {
			Epoch: 7, Connection: connection, Operation: operation,
			Outcome: ports.BrokerOutcomeOK, Registration: domain.RemoteRegistration{Endpoint: "dev@host:22"},
		},
	} {
		t.Run("encode/"+name, func(t *testing.T) {
			_, err := EncodeServer(message, testEnvelopeCeiling, testChunkCeiling)
			require.ErrorIs(t, err, ErrInvalidMessage)
		})
	}

	t.Run("decode refuses a registration that cannot fence", func(t *testing.T) {
		_, err := DecodeServer(clearRegistration(t), testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, ErrInvalidMessage)
	})
}

// TestMembershipResultAuthorityPerMutationKind proves the per-kind rule names
// the right authority for each request, so a removal success can never be read
// as an addition and an addition success can never be read as a removal.
func TestMembershipResultAuthorityPerMutationKind(t *testing.T) {
	connection := testConnectionID(0x27)
	operation := testOperationID(0x37)
	add := OperationResult{Epoch: 7, Connection: connection, Operation: operation, Outcome: ports.BrokerOutcomeOK, Registration: testRegistration()}
	remove := OperationResult{Epoch: 7, Connection: connection, Operation: operation, Outcome: ports.BrokerOutcomeOK, Removed: true}
	removeNoop := OperationResult{Epoch: 7, Connection: connection, Operation: operation, Outcome: ports.BrokerOutcomeOK}
	failed := OperationResult{Epoch: 7, Connection: connection, Operation: operation, Outcome: ports.BrokerOutcomeFailed, Error: testErrorDetail(), HasError: true}

	require.NoError(t, add.ValidateForMutation(MutationKindAddHost))
	require.NoError(t, add.ValidateForMutation(MutationKindUpdateHostPolicy))
	require.ErrorIs(t, add.ValidateForMutation(MutationKindRemoveHost), ErrInvalidMessage)

	require.NoError(t, remove.ValidateForMutation(MutationKindRemoveHost))
	require.NoError(t, removeNoop.ValidateForMutation(MutationKindRemoveHost), "an absent host is an idempotent no-op success")
	require.ErrorIs(t, remove.ValidateForMutation(MutationKindAddHost), ErrInvalidMessage)
	require.ErrorIs(t, removeNoop.ValidateForMutation(MutationKindAddHost), ErrInvalidMessage, "an addition success must carry its registration")
	require.ErrorIs(t, removeNoop.ValidateForMutation(MutationKindUpdateHostPolicy), ErrInvalidMessage)

	for _, kind := range []RegisterMutationKind{MutationKindAddHost, MutationKindRemoveHost, MutationKindUpdateHostPolicy} {
		require.NoError(t, failed.ValidateForMutation(kind))
	}
	require.ErrorIs(t, add.ValidateForMutation(RegisterMutationKind(0)), errConvertRange)
	require.ErrorIs(t, add.ValidateForMutation(RegisterMutationKind(9)), errConvertRange)
}

// TestMembershipErrorCodes proves the two membership codes travel and return
// as the exact closed enumeration: 11 is host conflict and 12 is immutable
// membership, on both the encode and decode paths.
func TestMembershipErrorCodes(t *testing.T) {
	connection := testConnectionID(0x28)
	operation := testOperationID(0x38)
	require.Equal(t, ports.BrokerErrorCode(11), ports.BrokerErrorHostConflict)
	require.Equal(t, ports.BrokerErrorCode(12), ports.BrokerErrorMembershipImmutable)

	for name, code := range map[string]ports.BrokerErrorCode{
		"host_conflict":        ports.BrokerErrorHostConflict,
		"membership_immutable": ports.BrokerErrorMembershipImmutable,
	} {
		t.Run(name, func(t *testing.T) {
			message := OperationResult{
				Epoch: 7, Connection: connection, Operation: operation,
				Outcome: ports.BrokerOutcomeFailed,
				Error:   ErrorDetail{Code: code, Text: "refused"}, HasError: true,
			}
			raw := mustEncodeServer(t, message)
			decoded, err := DecodeServer(raw, testEnvelopeCeiling, testChunkCeiling)
			require.NoError(t, err)
			require.Equal(t, message, decoded)
			require.Equal(t, code, decoded.(OperationResult).Error.Code)

			envelope := &wire.BrokerServerEnvelope{}
			require.NoError(t, proto.Unmarshal(raw, envelope))
			require.Equal(t, uint32(code), envelope.GetOperationResult().GetError().GetCode())
		})
	}
}

// TestMembershipMalformedTruncatedTrailing proves the strict scanner runs
// before generated unmarshal on the new variants too: unknown fields,
// truncation, trailing garbage, and an oversize envelope are all refused.
func TestMembershipMalformedTruncatedTrailing(t *testing.T) {
	connection := testConnectionID(0x29)
	operation := testOperationID(0x39)
	requests := membershipRequests(connection, operation)

	for _, tc := range requests {
		t.Run("truncated/"+tc.name, func(t *testing.T) {
			raw := mustEncodeClient(t, tc.message)
			for size := range len(raw) {
				_, err := DecodeClient(raw[:size], testEnvelopeCeiling, testChunkCeiling)
				require.Error(t, err, "prefix[:%d] accepted", size)
			}
		})
		t.Run("trailing/"+tc.name, func(t *testing.T) {
			raw := mustEncodeClient(t, tc.message)
			_, err := DecodeClient(append(append([]byte(nil), raw...), 0xFF), testEnvelopeCeiling, testChunkCeiling)
			require.Error(t, err)
			concatenated := append(append([]byte(nil), raw...), raw...)
			_, err = DecodeClient(concatenated, testEnvelopeCeiling, testChunkCeiling)
			require.ErrorIs(t, err, wire.ErrScanDuplicate)
		})
		t.Run("oversize/"+tc.name, func(t *testing.T) {
			raw := mustEncodeClient(t, tc.message)
			_, err := DecodeClient(raw, uint64(len(raw)-1), testChunkCeiling)
			require.ErrorIs(t, err, wire.ErrScanLength)
		})
	}

	t.Run("unknown nested field", func(t *testing.T) {
		// UpdateHostPolicy → RemoteRegistration with an undeclared field.
		registration := protowire.AppendTag(nil, 1, protowire.BytesType)
		registration = protowire.AppendBytes(registration, []byte("dev@host:22"))
		registration = protowire.AppendTag(registration, 9, protowire.VarintType)
		registration = protowire.AppendVarint(registration, 1)
		update := protowire.AppendTag(nil, 3, protowire.BytesType) // UpdateHostPolicy.registration
		update = protowire.AppendBytes(update, registration)
		outer := protowire.AppendTag(nil, 111, protowire.BytesType) // BrokerClientEnvelope.update_host_policy
		outer = protowire.AppendBytes(outer, update)
		_, err := DecodeClient(outer, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, wire.ErrScanUnknown)
	})

	t.Run("unknown request field", func(t *testing.T) {
		raw := mustEncodeClient(t, AddHost{Epoch: 7, Connection: connection, Operation: operation, Endpoint: "dev@host:22", Policy: testPolicy()})
		envelope := &wire.BrokerClientEnvelope{}
		require.NoError(t, wire.ScanEnvelope(envelope, raw))
		require.NoError(t, proto.Unmarshal(raw, envelope))
		envelope.GetAddHost().Policy = nil
		mutated, err := proto.Marshal(envelope)
		require.NoError(t, err)
		mutated = protowire.AppendTag(mutated, 9, protowire.VarintType)
		mutated = protowire.AppendVarint(mutated, 1)
		_, err = DecodeClient(mutated, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, wire.ErrScanUnknown)
	})

	t.Run("wrong wire type on the new tag", func(t *testing.T) {
		wrongType := protowire.AppendTag(nil, 111, protowire.VarintType)
		wrongType = protowire.AppendVarint(wrongType, 1)
		_, err := DecodeClient(wrongType, testEnvelopeCeiling, testChunkCeiling)
		require.ErrorIs(t, err, wire.ErrScanWireType)
	})

	t.Run("result truncated", func(t *testing.T) {
		raw := mustEncodeServer(t, OperationResult{Epoch: 7, Connection: connection, Operation: operation, Outcome: ports.BrokerOutcomeOK, Registration: testRegistration()})
		for size := range len(raw) {
			_, err := DecodeServer(raw[:size], testEnvelopeCeiling, testChunkCeiling)
			require.Error(t, err, "server prefix[:%d] accepted", size)
		}
	})
}

// TestMembershipUnknownTaxonomyRefused proves an out-of-range request tag or a
// result outcome outside the closed taxonomy is refused rather than coerced.
func TestMembershipUnknownTaxonomyRefused(t *testing.T) {
	connection := testConnectionID(0x2A)
	operation := testOperationID(0x3A)
	raw := mustEncodeServer(t, OperationResult{Epoch: 7, Connection: connection, Operation: operation, Outcome: ports.BrokerOutcomeFailed, Error: testErrorDetail(), HasError: true})
	envelope := &wire.BrokerServerEnvelope{}
	require.NoError(t, wire.ScanEnvelope(envelope, raw))
	require.NoError(t, proto.Unmarshal(raw, envelope))
	envelope.GetOperationResult().Outcome = 9
	mutated, err := proto.Marshal(envelope)
	require.NoError(t, err)
	_, err = DecodeServer(mutated, testEnvelopeCeiling, testChunkCeiling)
	require.ErrorIs(t, err, ErrInvalidMessage)

	// A malformed error code on a result is refused like one on an error frame.
	envelope.GetOperationResult().Outcome = 2
	envelope.GetOperationResult().GetError().Code = 99
	mutated, err = proto.Marshal(envelope)
	require.NoError(t, err)
	_, err = DecodeServer(mutated, testEnvelopeCeiling, testChunkCeiling)
	require.ErrorIs(t, err, ErrInvalidMessage)
}
