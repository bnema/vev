package daemonmux

import (
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
)

func testIdentity() ports.BrokerDaemonIdentity {
	return ports.BrokerDaemonIdentity("daemon-identity-1")
}

func testIncarnation() ports.BrokerDaemonIncarnation {
	return ports.BrokerDaemonIncarnation{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
}

func mustEncodePreambleRequest(t *testing.T, ceilings MuxCeilings, policy ports.BrokerPolicy) *wire.MuxPreambleRequest {
	t.Helper()
	request, err := EncodePreambleRequest(ceilings, policy)
	require.NoError(t, err)
	return request
}

func mustEncodePreambleResponse(t *testing.T, accepted bool, ceilings MuxCeilings, policy ports.BrokerPolicy, identity ports.BrokerDaemonIdentity, incarnation ports.BrokerDaemonIncarnation, code uint32) *wire.MuxPreambleResponse {
	t.Helper()
	response, err := EncodePreambleResponse(accepted, ceilings, policy, identity, incarnation, code)
	require.NoError(t, err)
	return response
}

// TestMuxPreambleByteLengthHelper proves the exposed 4 KiB byte-length helper
// accepts at the boundary and refuses one byte over, for both encoded and
// received preambles.
func TestMuxPreambleByteLengthHelper(t *testing.T) {
	require.Equal(t, wire.PreambleLimit, MuxPreambleLimit)
	require.Equal(t, 4<<10, MuxPreambleLimit)
	require.NoError(t, CheckPreambleSize(nil))
	require.NoError(t, CheckPreambleSize(make([]byte, MuxPreambleLimit)))
	require.ErrorIs(t, CheckPreambleSize(make([]byte, MuxPreambleLimit+1)), ErrPreambleRejected)

	request, err := proto.Marshal(mustEncodePreambleRequest(t, DefaultMuxCeilings(), testPolicy()))
	require.NoError(t, err)
	require.NoError(t, CheckPreambleSize(request))
	response, err := proto.Marshal(mustEncodePreambleResponse(t, true, DefaultMuxCeilings(), testPolicy(), testIdentity(), testIncarnation(), 0))
	require.NoError(t, err)
	require.NoError(t, CheckPreambleSize(response))
}

// TestMuxPreambleRoles proves the daemonmux preamble uses roles 5/6, the
// shared magic/epoch/exact version, zero capability bits, the 4 KiB preamble
// bound, and precise refusal mapping.
func TestMuxPreambleRoles(t *testing.T) {
	t.Run("client role is 5 and server role is 6", func(t *testing.T) {
		request := mustEncodePreambleRequest(t, DefaultMuxCeilings(), testPolicy())
		require.Equal(t, MuxRoleClient, request.GetRole().GetRole())
		require.Equal(t, uint32(5), request.GetRole().GetRole())
		response := mustEncodePreambleResponse(t, true, DefaultMuxCeilings(), testPolicy(), testIdentity(), testIncarnation(), 0)
		require.Equal(t, MuxRoleServer, response.GetRole().GetRole())
		require.Equal(t, uint32(6), response.GetRole().GetRole())
		// The daemonmux roles are disjoint from the session (1/2) and broker
		// (3/4) roles.
		require.NotEqual(t, MuxRoleClient, MuxRoleServer)
		require.NotContains(t, []uint32{1, 2, 3, 4}, MuxRoleClient)
		require.NotContains(t, []uint32{1, 2, 3, 4}, MuxRoleServer)
	})
	t.Run("magic epoch version match the shared contract", func(t *testing.T) {
		request := mustEncodePreambleRequest(t, DefaultMuxCeilings(), testPolicy())
		require.Equal(t, wire.PreambleMagic, request.GetMagic())
		require.Equal(t, wire.ProtocolEpoch, request.GetEpoch())
		require.Equal(t, uint32(protocol.Version), request.GetVersion())
		require.Zero(t, request.GetCapabilityBits())
		response := mustEncodePreambleResponse(t, true, DefaultMuxCeilings(), testPolicy(), testIdentity(), testIncarnation(), 0)
		require.Equal(t, wire.PreambleMagic, response.GetMagic())
		require.Equal(t, wire.ProtocolEpoch, response.GetEpoch())
		require.Equal(t, uint32(protocol.Version), response.GetVersion())
		require.Zero(t, response.GetCapabilityBits())
	})
	t.Run("preamble fits 4KiB with a maximum identity", func(t *testing.T) {
		identity := ports.BrokerDaemonIdentity(string(make([]byte, 0, ports.BrokerMaxIdentityBytes)))
		for range ports.BrokerMaxIdentityBytes {
			identity += "a"
		}
		require.NoError(t, identity.Validate())
		raw, err := proto.Marshal(mustEncodePreambleResponse(t, true, DefaultMuxCeilings(), testPolicy(), identity, testIncarnation(), 0))
		require.NoError(t, err)
		require.LessOrEqual(t, len(raw), MuxPreambleLimit)
		require.Equal(t, 4<<10, MuxPreambleLimit)
		raw, err = proto.Marshal(mustEncodePreambleRequest(t, DefaultMuxCeilings(), testPolicy()))
		require.NoError(t, err)
		require.LessOrEqual(t, len(raw), MuxPreambleLimit)
	})
	t.Run("foreign roles rejected", func(t *testing.T) {
		request := mustEncodePreambleRequest(t, DefaultMuxCeilings(), testPolicy())
		for _, role := range []uint32{1, 2, 3, 4, 6, 9} {
			request.Role.Role = role
			_, err := DecodePreambleRequest(request)
			require.ErrorIs(t, err, ErrPreambleRejected)
			require.Equal(t, RejectionWrongRole, RejectionCodeFor(request, err))
		}
		response := mustEncodePreambleResponse(t, true, DefaultMuxCeilings(), testPolicy(), testIdentity(), testIncarnation(), 0)
		for _, role := range []uint32{1, 2, 3, 4, 5, 9} {
			response.Role.Role = role
			_, err := DecodePreambleResponse(response)
			require.ErrorIs(t, err, ErrPreambleRejected)
		}
	})
	t.Run("caps zero", func(t *testing.T) {
		request := mustEncodePreambleRequest(t, DefaultMuxCeilings(), testPolicy())
		request.CapabilityBits = 1
		_, err := DecodePreambleRequest(request)
		require.ErrorIs(t, err, ErrPreambleRejected)
		require.Equal(t, RejectionLimitRefused, RejectionCodeFor(request, err))
		response := mustEncodePreambleResponse(t, true, DefaultMuxCeilings(), testPolicy(), testIdentity(), testIncarnation(), 0)
		response.CapabilityBits = 7
		_, err = DecodePreambleResponse(response)
		require.ErrorIs(t, err, ErrPreambleRejected)
	})
	t.Run("nil preambles rejected", func(t *testing.T) {
		_, err := DecodePreambleRequest(nil)
		require.ErrorIs(t, err, ErrPreambleRejected)
		_, err = DecodePreambleResponse(nil)
		require.ErrorIs(t, err, ErrPreambleRejected)
	})
}

// TestMuxPreambleCeilingWindows proves every advertised window is refused at
// max-1 outside the floor, accepted at the window edges, and refused one byte
// above the ceiling.
func TestMuxPreambleCeilingWindows(t *testing.T) {
	mutate := func(name string, mutate func(*wire.MuxPreambleRequest)) {
		t.Run(name, func(t *testing.T) {
			request := mustEncodePreambleRequest(t, DefaultMuxCeilings(), testPolicy())
			mutate(request)
			_, err := DecodePreambleRequest(request)
			require.ErrorIs(t, err, ErrPreambleRejected)
			require.Equal(t, RejectionLimitRefused, RejectionCodeFor(request, err))
		})
	}
	accept := func(name string, mutate func(*wire.MuxPreambleRequest), want MuxCeilings) {
		t.Run(name, func(t *testing.T) {
			request := mustEncodePreambleRequest(t, DefaultMuxCeilings(), testPolicy())
			mutate(request)
			decoded, err := DecodePreambleRequest(request)
			require.NoError(t, err)
			require.Equal(t, want, decoded.Ceilings)
		})
	}
	defaults := DefaultMuxCeilings()

	t.Run("envelope window 1MiB..16MiB", func(t *testing.T) {
		require.Equal(t, uint64(1<<20), MinMuxEnvelopeBytes)
		require.Equal(t, uint64(16<<20), MaxMuxEnvelopeBytes)
		mutate("below floor", func(r *wire.MuxPreambleRequest) { r.MaxReceiveEnvelopeBytes = MinMuxEnvelopeBytes - 1 })
		mutate("above ceiling", func(r *wire.MuxPreambleRequest) { r.MaxReceiveEnvelopeBytes = MaxMuxEnvelopeBytes + 1 })
		mutate("zero", func(r *wire.MuxPreambleRequest) { r.MaxReceiveEnvelopeBytes = 0 })
		accept("at floor", func(r *wire.MuxPreambleRequest) { r.MaxReceiveEnvelopeBytes = MinMuxEnvelopeBytes }, MuxCeilings{
			MaxReceiveEnvelopeBytes: MinMuxEnvelopeBytes,
			StreamChunkLimit:        defaults.StreamChunkLimit,
			MaxStreams:              defaults.MaxStreams,
			MaxAggregateBytes:       defaults.MaxAggregateBytes,
		})
		accept("floor+1", func(r *wire.MuxPreambleRequest) { r.MaxReceiveEnvelopeBytes = MinMuxEnvelopeBytes + 1 }, MuxCeilings{
			MaxReceiveEnvelopeBytes: MinMuxEnvelopeBytes + 1,
			StreamChunkLimit:        defaults.StreamChunkLimit,
			MaxStreams:              defaults.MaxStreams,
			MaxAggregateBytes:       defaults.MaxAggregateBytes,
		})
		accept("at ceiling", func(r *wire.MuxPreambleRequest) { r.MaxReceiveEnvelopeBytes = MaxMuxEnvelopeBytes }, defaults)
		accept("ceiling-1", func(r *wire.MuxPreambleRequest) { r.MaxReceiveEnvelopeBytes = MaxMuxEnvelopeBytes - 1 }, MuxCeilings{
			MaxReceiveEnvelopeBytes: MaxMuxEnvelopeBytes - 1,
			StreamChunkLimit:        defaults.StreamChunkLimit,
			MaxStreams:              defaults.MaxStreams,
			MaxAggregateBytes:       defaults.MaxAggregateBytes,
		})
	})
	t.Run("chunk window 1..64KiB", func(t *testing.T) {
		require.Equal(t, uint64(1), MinMuxChunkBytes)
		require.Equal(t, uint64(64<<10), MaxMuxChunkBytes)
		mutate("zero", func(r *wire.MuxPreambleRequest) { r.StreamChunkLimit = 0 })
		mutate("above ceiling", func(r *wire.MuxPreambleRequest) { r.StreamChunkLimit = MaxMuxChunkBytes + 1 })
		accept("at floor", func(r *wire.MuxPreambleRequest) { r.StreamChunkLimit = MinMuxChunkBytes }, MuxCeilings{
			MaxReceiveEnvelopeBytes: defaults.MaxReceiveEnvelopeBytes,
			StreamChunkLimit:        MinMuxChunkBytes,
			MaxStreams:              defaults.MaxStreams,
			MaxAggregateBytes:       defaults.MaxAggregateBytes,
		})
		accept("floor+1", func(r *wire.MuxPreambleRequest) { r.StreamChunkLimit = MinMuxChunkBytes + 1 }, MuxCeilings{
			MaxReceiveEnvelopeBytes: defaults.MaxReceiveEnvelopeBytes,
			StreamChunkLimit:        MinMuxChunkBytes + 1,
			MaxStreams:              defaults.MaxStreams,
			MaxAggregateBytes:       defaults.MaxAggregateBytes,
		})
		accept("at ceiling", func(r *wire.MuxPreambleRequest) { r.StreamChunkLimit = MaxMuxChunkBytes }, defaults)
		accept("ceiling-1", func(r *wire.MuxPreambleRequest) { r.StreamChunkLimit = MaxMuxChunkBytes - 1 }, MuxCeilings{
			MaxReceiveEnvelopeBytes: defaults.MaxReceiveEnvelopeBytes,
			StreamChunkLimit:        MaxMuxChunkBytes - 1,
			MaxStreams:              defaults.MaxStreams,
			MaxAggregateBytes:       defaults.MaxAggregateBytes,
		})
	})
	t.Run("stream window 1..128", func(t *testing.T) {
		require.Equal(t, uint64(1), MinMuxStreams)
		require.Equal(t, uint64(128), MaxMuxStreams)
		mutate("zero", func(r *wire.MuxPreambleRequest) { r.MaxStreams = 0 })
		mutate("above ceiling", func(r *wire.MuxPreambleRequest) { r.MaxStreams = MaxMuxStreams + 1 })
		accept("at floor", func(r *wire.MuxPreambleRequest) { r.MaxStreams = MinMuxStreams }, MuxCeilings{
			MaxReceiveEnvelopeBytes: defaults.MaxReceiveEnvelopeBytes,
			StreamChunkLimit:        defaults.StreamChunkLimit,
			MaxStreams:              MinMuxStreams,
			MaxAggregateBytes:       defaults.MaxAggregateBytes,
		})
		accept("floor+1", func(r *wire.MuxPreambleRequest) { r.MaxStreams = MinMuxStreams + 1 }, MuxCeilings{
			MaxReceiveEnvelopeBytes: defaults.MaxReceiveEnvelopeBytes,
			StreamChunkLimit:        defaults.StreamChunkLimit,
			MaxStreams:              MinMuxStreams + 1,
			MaxAggregateBytes:       defaults.MaxAggregateBytes,
		})
		accept("at ceiling", func(r *wire.MuxPreambleRequest) { r.MaxStreams = MaxMuxStreams }, defaults)
		accept("ceiling-1", func(r *wire.MuxPreambleRequest) { r.MaxStreams = MaxMuxStreams - 1 }, MuxCeilings{
			MaxReceiveEnvelopeBytes: defaults.MaxReceiveEnvelopeBytes,
			StreamChunkLimit:        defaults.StreamChunkLimit,
			MaxStreams:              MaxMuxStreams - 1,
			MaxAggregateBytes:       defaults.MaxAggregateBytes,
		})
	})
	t.Run("aggregate window floor..64MiB", func(t *testing.T) {
		require.Equal(t, MaxMuxChunkBytes+MuxEnvelopeOverheadBytes, MinMuxAggregateBytes)
		require.Equal(t, uint64(64<<20), MaxMuxAggregateBytes)
		mutate("zero", func(r *wire.MuxPreambleRequest) { r.MaxAggregateBytes = 0 })
		mutate("above ceiling", func(r *wire.MuxPreambleRequest) { r.MaxAggregateBytes = MaxMuxAggregateBytes + 1 })
		accept("at floor", func(r *wire.MuxPreambleRequest) { r.MaxAggregateBytes = MinMuxAggregateBytes }, MuxCeilings{
			MaxReceiveEnvelopeBytes: defaults.MaxReceiveEnvelopeBytes,
			StreamChunkLimit:        defaults.StreamChunkLimit,
			MaxStreams:              defaults.MaxStreams,
			MaxAggregateBytes:       MinMuxAggregateBytes,
		})
		accept("at ceiling", func(r *wire.MuxPreambleRequest) { r.MaxAggregateBytes = MaxMuxAggregateBytes }, defaults)
		accept("ceiling-1", func(r *wire.MuxPreambleRequest) { r.MaxAggregateBytes = MaxMuxAggregateBytes - 1 }, MuxCeilings{
			MaxReceiveEnvelopeBytes: defaults.MaxReceiveEnvelopeBytes,
			StreamChunkLimit:        defaults.StreamChunkLimit,
			MaxStreams:              defaults.MaxStreams,
			MaxAggregateBytes:       MaxMuxAggregateBytes - 1,
		})
	})
}

// TestMuxPreambleNegotiatesMinima proves the effective ceilings are the
// element-wise minima of both advertisements, round-tripped through the wire.
func TestMuxPreambleNegotiatesMinima(t *testing.T) {
	local := DefaultMuxCeilings()
	remote := MuxCeilings{
		MaxReceiveEnvelopeBytes: MinMuxEnvelopeBytes,
		StreamChunkLimit:        1024,
		MaxStreams:              8,
		MaxAggregateBytes:       1 << 20,
	}
	decoded, err := DecodePreambleRequest(mustEncodePreambleRequest(t, remote, testPolicy()))
	require.NoError(t, err)
	require.Equal(t, remote, decoded.Ceilings)
	require.Equal(t, remote, EffectiveMuxCeilings(local, decoded.Ceilings))

	// A response advertising the same minima round-trips them.
	response := mustEncodePreambleResponse(t, true, remote, testPolicy(), testIdentity(), testIncarnation(), 0)
	decodedResponse, err := DecodePreambleResponse(response)
	require.NoError(t, err)
	require.True(t, decodedResponse.Accepted)
	require.Equal(t, remote, decodedResponse.Ceilings)

	// The local default is never above the absolute ceilings.
	require.Equal(t, MaxMuxEnvelopeBytes, local.MaxReceiveEnvelopeBytes)
	require.Equal(t, MaxMuxChunkBytes, local.StreamChunkLimit)
	require.Equal(t, MaxMuxStreams, local.MaxStreams)
	require.Equal(t, MaxMuxAggregateBytes, local.MaxAggregateBytes)
}

// TestEffectiveMuxCeilings proves the effective ceilings are the element-wise
// minima of both advertisements: a lower remote offer lowers only that field,
// and the result is symmetric and idempotent.
func TestEffectiveMuxCeilings(t *testing.T) {
	local := DefaultMuxCeilings()
	remote := MuxCeilings{
		MaxReceiveEnvelopeBytes: local.MaxReceiveEnvelopeBytes - 1,
		StreamChunkLimit:        local.StreamChunkLimit,
		MaxStreams:              4,
		MaxAggregateBytes:       local.MaxAggregateBytes,
	}
	effective := EffectiveMuxCeilings(local, remote)
	require.Equal(t, remote.MaxReceiveEnvelopeBytes, effective.MaxReceiveEnvelopeBytes)
	require.Equal(t, local.StreamChunkLimit, effective.StreamChunkLimit)
	require.Equal(t, uint64(4), effective.MaxStreams)
	require.Equal(t, local.MaxAggregateBytes, effective.MaxAggregateBytes)

	require.Equal(t, effective, EffectiveMuxCeilings(remote, local))
	require.Equal(t, effective, EffectiveMuxCeilings(effective, effective))
}

// TestEffectiveMuxCeilingsWindowsFitAggregate proves the negotiated stream
// ceiling never lets the sum of every stream's full window exceed the
// aggregate budget, so a peer that respects its credit can never trip the
// aggregate bound and be reset for it.
func TestEffectiveMuxCeilingsWindowsFitAggregate(t *testing.T) {
	cases := []struct {
		name        string
		chunk       uint64
		streams     uint64
		aggregate   uint64
		wantStreams uint64
	}{
		{"defaults unchanged", MaxMuxChunkBytes, MaxMuxStreams, MaxMuxAggregateBytes, MaxMuxStreams},
		{"aggregate floor admits one stream", MaxMuxChunkBytes, MaxMuxStreams, MinMuxAggregateBytes, 1},
		{"1 MiB aggregate with max chunks", MaxMuxChunkBytes, MaxMuxStreams, 1 << 20, (1 << 20) / chunkCredit(int(MaxMuxChunkBytes))},
		{"small chunks keep every stream", 1024, MaxMuxStreams, 1 << 20, MaxMuxStreams},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			offer := MuxCeilings{
				MaxReceiveEnvelopeBytes: MinMuxEnvelopeBytes,
				StreamChunkLimit:        tc.chunk,
				MaxStreams:              tc.streams,
				MaxAggregateBytes:       tc.aggregate,
			}
			effective := EffectiveMuxCeilings(DefaultMuxCeilings(), offer)
			require.NoError(t, effective.Validate())
			require.Equal(t, tc.wantStreams, effective.MaxStreams)
			require.LessOrEqual(t, effective.MaxStreams*effective.StreamWindow(), effective.MaxAggregateBytes)
			require.Equal(t, effective, EffectiveMuxCeilings(effective, effective), "idempotent, so both peers agree")
			require.NoError(t, ValidatePreambleResponseAgainstOffer(MuxPreambleResponse{Accepted: true, Ceilings: effective}, offer))
		})
	}
}

// TestValidatePreambleResponseAgainstOffer proves an accepted response is
// refused when any effective ceiling exceeds the local offer, and accepted
// when every ceiling is at or below it. A refusal, or an invalid offer, is a
// protocol violation.
func TestValidatePreambleResponseAgainstOffer(t *testing.T) {
	offered := MuxCeilings{
		MaxReceiveEnvelopeBytes: MinMuxEnvelopeBytes + 1,
		StreamChunkLimit:        MinMuxChunkBytes + 1,
		MaxStreams:              MinMuxStreams + 1,
		MaxAggregateBytes:       MinMuxAggregateBytes + 1,
	}
	accepted := func(mutate func(*MuxCeilings)) MuxPreambleResponse {
		ceilings := offered
		if mutate != nil {
			mutate(&ceilings)
		}
		return MuxPreambleResponse{Accepted: true, Ceilings: ceilings}
	}

	cases := []struct {
		name     string
		response MuxPreambleResponse
		wantErr  bool
	}{
		{"at offer", accepted(nil), false},
		{"below offer", accepted(func(c *MuxCeilings) {
			c.MaxReceiveEnvelopeBytes = MinMuxEnvelopeBytes
			c.StreamChunkLimit = MinMuxChunkBytes
			c.MaxStreams = MinMuxStreams
			c.MaxAggregateBytes = MinMuxAggregateBytes
		}), false},
		{"envelope above offer", accepted(func(c *MuxCeilings) { c.MaxReceiveEnvelopeBytes++ }), true},
		{"chunk above offer", accepted(func(c *MuxCeilings) { c.StreamChunkLimit++ }), true},
		{"streams above offer", accepted(func(c *MuxCeilings) { c.MaxStreams++ }), true},
		{"aggregate above offer", accepted(func(c *MuxCeilings) { c.MaxAggregateBytes++ }), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidatePreambleResponseAgainstOffer(tc.response, offered)
			if tc.wantErr {
				require.ErrorIs(t, err, ErrPreambleRejected)
				return
			}
			require.NoError(t, err)
		})
	}

	t.Run("refusal", func(t *testing.T) {
		response := MuxPreambleResponse{Accepted: false, Code: RejectionLimitRefused}
		require.ErrorIs(t, ValidatePreambleResponseAgainstOffer(response, offered), ErrPreambleRejected)
	})

	t.Run("invalid offer", func(t *testing.T) {
		require.ErrorIs(t, ValidatePreambleResponseAgainstOffer(accepted(nil), MuxCeilings{}), ErrPreambleRejected)
	})
}

// TestMuxPreambleAcceptCarriesAuthority proves an acceptance carries the
// authenticated daemon identity, the 16-byte incarnation, and the accepted
// policy losslessly, and that a request carries the requested policy.
func TestMuxPreambleAcceptCarriesAuthority(t *testing.T) {
	request, err := proto.Marshal(mustEncodePreambleRequest(t, DefaultMuxCeilings(), testPolicy()))
	require.NoError(t, err)
	decodedRequest := &wire.MuxPreambleRequest{}
	require.NoError(t, proto.Unmarshal(request, decodedRequest))
	parsedRequest, err := DecodePreambleRequest(decodedRequest)
	require.NoError(t, err)
	require.Equal(t, testPolicy(), parsedRequest.Policy)

	response, err := proto.Marshal(mustEncodePreambleResponse(t, true, DefaultMuxCeilings(), testPolicy(), testIdentity(), testIncarnation(), 0))
	require.NoError(t, err)
	decodedResponse := &wire.MuxPreambleResponse{}
	require.NoError(t, proto.Unmarshal(response, decodedResponse))
	parsedResponse, err := DecodePreambleResponse(decodedResponse)
	require.NoError(t, err)
	require.True(t, parsedResponse.Accepted)
	require.Equal(t, testPolicy(), parsedResponse.Policy)
	require.Equal(t, testIdentity(), parsedResponse.Identity)
	require.Equal(t, testIncarnation(), parsedResponse.Incarnation)
	require.Zero(t, parsedResponse.Code)

	// The accepted policy is exactly the requested policy when they match;
	// a different accepted policy stays visible to the caller.
	other := testPolicy()
	other.Trust = "tofu"
	acceptedOther := mustEncodePreambleResponse(t, true, DefaultMuxCeilings(), other, testIdentity(), testIncarnation(), 0)
	parsedOther, err := DecodePreambleResponse(acceptedOther)
	require.NoError(t, err)
	require.Equal(t, other, parsedOther.Policy)
	require.NotEqual(t, parsedRequest.Policy, parsedOther.Policy)
	require.False(t, parsedRequest.Policy.Compatible(parsedOther.Policy))
}

// TestMuxPreambleIdentityChecks proves the accepted identity and incarnation
// are mandatory, exact, and validated: they authorize the physical
// connection, so a malformed one is never accepted silently.
func TestMuxPreambleIdentityChecks(t *testing.T) {
	t.Run("encode refuses a missing or invalid authority", func(t *testing.T) {
		_, err := EncodePreambleResponse(true, DefaultMuxCeilings(), testPolicy(), "", testIncarnation(), 0)
		require.ErrorIs(t, err, ErrPreambleRejected)
		_, err = EncodePreambleResponse(true, DefaultMuxCeilings(), testPolicy(), ports.BrokerDaemonIdentity("has space"), testIncarnation(), 0)
		require.ErrorIs(t, err, ErrPreambleRejected)
		_, err = EncodePreambleResponse(true, DefaultMuxCeilings(), testPolicy(), testIdentity(), ports.BrokerDaemonIncarnation{}, 0)
		require.ErrorIs(t, err, ErrPreambleRejected)
	})
	t.Run("decode refuses mutated authority", func(t *testing.T) {
		cases := map[string]func(*wire.MuxPreambleResponse){
			"empty identity":       func(r *wire.MuxPreambleResponse) { r.DaemonIdentity = "" },
			"blank identity":       func(r *wire.MuxPreambleResponse) { r.DaemonIdentity = " " },
			"control identity":     func(r *wire.MuxPreambleResponse) { r.DaemonIdentity = "id\x01" },
			"short incarnation":    func(r *wire.MuxPreambleResponse) { r.Incarnation = r.Incarnation[:15] },
			"long incarnation":     func(r *wire.MuxPreambleResponse) { r.Incarnation = append(r.Incarnation, 0) },
			"zero incarnation":     func(r *wire.MuxPreambleResponse) { r.Incarnation = make([]byte, 16) },
			"missing policy":       func(r *wire.MuxPreambleResponse) { r.AcceptedPolicy = nil },
			"invalid policy token": func(r *wire.MuxPreambleResponse) { r.AcceptedPolicy.Trust = "" },
		}
		for name, mutate := range cases {
			t.Run(name, func(t *testing.T) {
				response := mustEncodePreambleResponse(t, true, DefaultMuxCeilings(), testPolicy(), testIdentity(), testIncarnation(), 0)
				mutate(response)
				_, err := DecodePreambleResponse(response)
				require.ErrorIs(t, err, ErrPreambleRejected)
			})
		}
	})
	t.Run("oversize authority is refused", func(t *testing.T) {
		identity := ports.BrokerDaemonIdentity(string(make([]byte, 0, ports.BrokerMaxIdentityBytes+1)))
		for range ports.BrokerMaxIdentityBytes + 1 {
			identity += "a"
		}
		_, err := EncodePreambleResponse(true, DefaultMuxCeilings(), testPolicy(), identity, testIncarnation(), 0)
		require.ErrorIs(t, err, ErrPreambleRejected)
	})
}

// TestMuxPreambleRefusal proves refusals carry the precise code, that a
// code-less refusal is refused locally, and that a request with an
// unnegotiable policy is a refusal, not a decode error class of its own.
func TestMuxPreambleRefusal(t *testing.T) {
	for name, code := range map[string]uint32{
		"bad magic":         RejectionBadMagic,
		"epoch":             RejectionEpochMismatch,
		"version":           RejectionVersionMismatch,
		"wrong role":        RejectionWrongRole,
		"duplicate":         RejectionDuplicate,
		"out of order":      RejectionOutOfOrder,
		"limit refused":     RejectionLimitRefused,
		"invalid policy":    RejectionLimitRefused,
		"capability bits":   RejectionLimitRefused,
		"envelope floor":    RejectionLimitRefused,
		"chunk limit":       RejectionLimitRefused,
		"stream limit":      RejectionLimitRefused,
		"aggregate limit":   RejectionLimitRefused,
		"zero refusal code": RejectionLimitRefused,
	} {
		t.Run("response/"+name, func(t *testing.T) {
			response := mustEncodePreambleResponse(t, false, DefaultMuxCeilings(), testPolicy(), testIdentity(), testIncarnation(), code)
			require.False(t, response.GetAccepted())
			require.Equal(t, code, response.GetRejection().GetCode())
			decoded, err := DecodePreambleResponse(response)
			require.ErrorIs(t, err, ErrPreambleRejected)
			require.Equal(t, code, decoded.Code)
			require.False(t, decoded.Accepted)
			// A refusal never claims accepted authority.
			require.Nil(t, response.GetAcceptedPolicy())
			require.Empty(t, response.GetDaemonIdentity())
		})
	}
	t.Run("code-less refusal is refused locally", func(t *testing.T) {
		_, err := EncodePreambleResponse(false, DefaultMuxCeilings(), testPolicy(), testIdentity(), testIncarnation(), 0)
		require.ErrorIs(t, err, ErrPreambleRejected)
	})
	t.Run("accepted-flag refusal without a code is refused", func(t *testing.T) {
		response := mustEncodePreambleResponse(t, false, DefaultMuxCeilings(), testPolicy(), testIdentity(), testIncarnation(), RejectionVersionMismatch)
		response.Rejection = nil
		_, err := DecodePreambleResponse(response)
		require.ErrorIs(t, err, ErrPreambleRejected)
	})
	t.Run("request refusals map precisely", func(t *testing.T) {
		for name, tc := range map[string]struct {
			mutate func(*wire.MuxPreambleRequest)
			want   uint32
		}{
			"bad magic":       {func(r *wire.MuxPreambleRequest) { r.Magic = 0xDEAD }, RejectionBadMagic},
			"epoch":           {func(r *wire.MuxPreambleRequest) { r.Epoch++ }, RejectionEpochMismatch},
			"version":         {func(r *wire.MuxPreambleRequest) { r.Version++ }, RejectionVersionMismatch},
			"role":            {func(r *wire.MuxPreambleRequest) { r.Role.Role = 9 }, RejectionWrongRole},
			"caps":            {func(r *wire.MuxPreambleRequest) { r.CapabilityBits = 1 }, RejectionLimitRefused},
			"envelope floor":  {func(r *wire.MuxPreambleRequest) { r.MaxReceiveEnvelopeBytes = 1 }, RejectionLimitRefused},
			"chunk limit":     {func(r *wire.MuxPreambleRequest) { r.StreamChunkLimit = 0 }, RejectionLimitRefused},
			"stream limit":    {func(r *wire.MuxPreambleRequest) { r.MaxStreams = 0 }, RejectionLimitRefused},
			"aggregate limit": {func(r *wire.MuxPreambleRequest) { r.MaxAggregateBytes = 0 }, RejectionLimitRefused},
			"missing policy":  {func(r *wire.MuxPreambleRequest) { r.Policy = nil }, RejectionLimitRefused},
			"invalid policy":  {func(r *wire.MuxPreambleRequest) { r.Policy.Transport = "" }, RejectionLimitRefused},
		} {
			t.Run(name, func(t *testing.T) {
				request := mustEncodePreambleRequest(t, DefaultMuxCeilings(), testPolicy())
				tc.mutate(request)
				_, err := DecodePreambleRequest(request)
				require.ErrorIs(t, err, ErrPreambleRejected)
				require.Equal(t, tc.want, RejectionCodeFor(request, err))
			})
		}
	})
	t.Run("nil request maps to limit refusal", func(t *testing.T) {
		require.Equal(t, RejectionLimitRefused, RejectionCodeFor(nil, ErrPreambleRejected))
		require.Zero(t, RejectionCodeFor(nil, nil))
	})
}

// TestMuxPreambleEncodeFailClosed proves the local encoder refuses an
// out-of-window advertisement or an unacceptably empty policy instead of
// emitting a preamble the peer must reject.
func TestMuxPreambleEncodeFailClosed(t *testing.T) {
	bad := DefaultMuxCeilings()
	bad.MaxStreams = MaxMuxStreams + 1
	_, err := EncodePreambleRequest(bad, testPolicy())
	require.ErrorIs(t, err, ErrPreambleRejected)

	_, err = EncodePreambleRequest(DefaultMuxCeilings(), ports.BrokerPolicy{})
	require.ErrorIs(t, err, ErrPreambleRejected)

	_, err = EncodePreambleResponse(true, bad, testPolicy(), testIdentity(), testIncarnation(), 0)
	require.ErrorIs(t, err, ErrPreambleRejected)

	_, err = EncodePreambleResponse(true, DefaultMuxCeilings(), ports.BrokerPolicy{}, testIdentity(), testIncarnation(), 0)
	require.ErrorIs(t, err, ErrPreambleRejected)

	// A refusal is legal without any accepted authority, but it still
	// carries the shared magic, epoch, version, and server role.
	refusal := mustEncodePreambleResponse(t, false, MuxCeilings{}, ports.BrokerPolicy{}, "", ports.BrokerDaemonIncarnation{}, RejectionLimitRefused)
	require.Equal(t, wire.PreambleMagic, refusal.GetMagic())
	require.Equal(t, MuxRoleServer, refusal.GetRole().GetRole())
	require.Equal(t, RejectionLimitRefused, refusal.GetRejection().GetCode())
}

// TestMuxPreambleRefusalCodeRange proves a refusal code is exactly 1..7 on
// both encode and decode: 0 and anything above 7 fail closed, 6 is accepted as
// a well-formed wire code even though this codec never emits it.
func TestMuxPreambleRefusalCodeRange(t *testing.T) {
	for code := RejectionBadMagic; code <= RejectionLimitRefused; code++ {
		response, err := EncodePreambleResponse(false, DefaultMuxCeilings(), testPolicy(), testIdentity(), testIncarnation(), code)
		require.NoError(t, err, "code %d", code)
		require.False(t, response.GetAccepted())
		require.Equal(t, code, response.GetRejection().GetCode())

		decoded, err := DecodePreambleResponse(response)
		require.ErrorIs(t, err, ErrPreambleRejected)
		require.Equal(t, code, decoded.Code)
	}

	for _, code := range []uint32{0, 8, 99, 0xFFFFFFFF} {
		_, err := EncodePreambleResponse(false, DefaultMuxCeilings(), testPolicy(), testIdentity(), testIncarnation(), code)
		require.ErrorIs(t, err, ErrPreambleRejected, "encode code %d", code)
	}

	// A well-formed refusal frame with an out-of-range code still fails the
	// semantic decode.
	base := mustEncodePreambleResponse(t, false, DefaultMuxCeilings(), testPolicy(), testIdentity(), testIncarnation(), RejectionLimitRefused)
	for _, code := range []uint32{0, 8, 99} {
		mutated := proto.Clone(base).(*wire.MuxPreambleResponse)
		mutated.Rejection = &wire.PreambleRejectionCode{Code: code}
		_, err := DecodePreambleResponse(mutated)
		require.ErrorIs(t, err, ErrPreambleRejected, "decode code %d", code)
	}

	// An acceptance never carries a rejection code.
	_, err := EncodePreambleResponse(true, DefaultMuxCeilings(), testPolicy(), testIdentity(), testIncarnation(), RejectionDuplicate)
	require.ErrorIs(t, err, ErrPreambleRejected)
}

// TestMuxPreambleResponseExclusivity proves an answer is an acceptance or a
// refusal but never a blend: an acceptance carrying a rejection field and a
// refusal carrying accepted-only authority are both rejected.
func TestMuxPreambleResponseExclusivity(t *testing.T) {
	t.Run("acceptance rejects a rejection field", func(t *testing.T) {
		response := mustEncodePreambleResponse(t, true, DefaultMuxCeilings(), testPolicy(), testIdentity(), testIncarnation(), 0)
		require.NotNil(t, response.GetAcceptedPolicy())
		response.Rejection = &wire.PreambleRejectionCode{Code: RejectionDuplicate}
		_, err := DecodePreambleResponse(response)
		require.ErrorIs(t, err, ErrPreambleRejected)
	})

	t.Run("refusal rejects accepted-only authority", func(t *testing.T) {
		incarnation := testIncarnation()
		mutations := map[string]func(*wire.MuxPreambleResponse){
			"accepted policy": func(r *wire.MuxPreambleResponse) { r.AcceptedPolicy = policyToWire(testPolicy()) },
			"daemon identity": func(r *wire.MuxPreambleResponse) {
				r.DaemonIdentity = string(testIdentity())
			},
			"incarnation": func(r *wire.MuxPreambleResponse) {
				r.Incarnation = append([]byte(nil), incarnation[:]...)
			},
		}
		for name, mutate := range mutations {
			t.Run(name, func(t *testing.T) {
				response := mustEncodePreambleResponse(t, false, DefaultMuxCeilings(), testPolicy(), testIdentity(), testIncarnation(), RejectionLimitRefused)
				mutate(response)
				_, err := DecodePreambleResponse(response)
				require.ErrorIs(t, err, ErrPreambleRejected)
			})
		}
	})
}

// TestMuxPreambleStrictBytes proves the byte helpers enforce the 4 KiB bound,
// the strict scanner, generated unmarshal, and semantic decode for both
// directions: oversize, empty, unknown, duplicate, trailing/concatenated,
// truncated, wrong-wire-type, and semantic failures are all refused.
func TestMuxPreambleStrictBytes(t *testing.T) {
	requestRaw, err := proto.Marshal(mustEncodePreambleRequest(t, DefaultMuxCeilings(), testPolicy()))
	require.NoError(t, err)
	responseRaw, err := proto.Marshal(mustEncodePreambleResponse(t, true, DefaultMuxCeilings(), testPolicy(), testIdentity(), testIncarnation(), 0))
	require.NoError(t, err)

	t.Run("valid request round-trips", func(t *testing.T) {
		decoded, err := DecodePreambleRequestBytes(requestRaw)
		require.NoError(t, err)
		require.Equal(t, DefaultMuxCeilings(), decoded.Ceilings)
		require.Equal(t, testPolicy(), decoded.Policy)
	})
	t.Run("valid response round-trips", func(t *testing.T) {
		decoded, err := DecodePreambleResponseBytes(responseRaw)
		require.NoError(t, err)
		require.True(t, decoded.Accepted)
		require.Equal(t, testIdentity(), decoded.Identity)
		require.Equal(t, testIncarnation(), decoded.Incarnation)
	})
	t.Run("refusal frame surfaces its code", func(t *testing.T) {
		refusal, err := proto.Marshal(mustEncodePreambleResponse(t, false, DefaultMuxCeilings(), testPolicy(), testIdentity(), testIncarnation(), RejectionLimitRefused))
		require.NoError(t, err)
		decoded, err := DecodePreambleResponseBytes(refusal)
		require.ErrorIs(t, err, ErrPreambleRejected)
		require.Equal(t, RejectionLimitRefused, decoded.Code)
	})

	corruptions := []struct {
		name   string
		mutate func(raw []byte) []byte
		want   error
	}{
		{"oversize", func([]byte) []byte { return make([]byte, MuxPreambleLimit+1) }, ErrPreambleRejected},
		{"empty", func([]byte) []byte { return nil }, wire.ErrScanEmpty},
		{"unknown field", func(raw []byte) []byte {
			out := protowire.AppendTag(append([]byte(nil), raw...), 399, protowire.VarintType)
			return protowire.AppendVarint(out, 1)
		}, wire.ErrScanUnknown},
		{"duplicate singular field", func(raw []byte) []byte {
			out := protowire.AppendTag(append([]byte(nil), raw...), 1, protowire.VarintType)
			return protowire.AppendVarint(out, uint64(wire.PreambleMagic))
		}, wire.ErrScanDuplicate},
		{"concatenated frame", func(raw []byte) []byte {
			return append(append([]byte(nil), raw...), raw...)
		}, wire.ErrScanDuplicate},
		{"wrong wire type", func(raw []byte) []byte {
			out := protowire.AppendTag(append([]byte(nil), raw...), 1, protowire.BytesType)
			return protowire.AppendString(out, "x")
		}, wire.ErrScanWireType},
		{"truncated varint", func(raw []byte) []byte {
			return append(append([]byte(nil), raw...), 0x80)
		}, wire.ErrScanTruncated},
	}

	directions := []struct {
		name   string
		raw    []byte
		decode func([]byte) error
	}{
		{"request", requestRaw, func(payload []byte) error { _, err := DecodePreambleRequestBytes(payload); return err }},
		{"response", responseRaw, func(payload []byte) error { _, err := DecodePreambleResponseBytes(payload); return err }},
	}
	for _, direction := range directions {
		for _, corruption := range corruptions {
			t.Run(direction.name+"/"+corruption.name, func(t *testing.T) {
				err := direction.decode(corruption.mutate(direction.raw))
				require.Error(t, err)
				require.ErrorIs(t, err, corruption.want)
			})
		}
	}

	t.Run("truncated prefixes", func(t *testing.T) {
		for size := range len(requestRaw) {
			_, err := DecodePreambleRequestBytes(requestRaw[:size])
			require.Error(t, err, "request prefix[:%d] accepted", size)
		}
		_, err := DecodePreambleResponseBytes(requestRaw)
		require.Error(t, err, "client preamble accepted by the response helper")
		for size := range len(responseRaw) {
			_, err := DecodePreambleResponseBytes(responseRaw[:size])
			require.Error(t, err, "response prefix[:%d] accepted", size)
		}
	})

	t.Run("semantic failure from valid carriage", func(t *testing.T) {
		bad := proto.Clone(mustEncodePreambleRequest(t, DefaultMuxCeilings(), testPolicy())).(*wire.MuxPreambleRequest)
		bad.Magic = 0xDEADBEEF
		raw, err := proto.Marshal(bad)
		require.NoError(t, err)
		_, err = DecodePreambleRequestBytes(raw)
		require.ErrorIs(t, err, ErrPreambleRejected)
	})
}

// FuzzDecodePreambleRequestBytes proves the strict byte helper never panics on
// arbitrary input.
func FuzzDecodePreambleRequestBytes(f *testing.F) {
	request, err := EncodePreambleRequest(DefaultMuxCeilings(), testPolicy())
	require.NoError(f, err)
	raw, err := proto.Marshal(request)
	require.NoError(f, err)
	for _, seed := range appendStrictScanSeeds([][]byte{raw}) {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, payload []byte) {
		_, _ = DecodePreambleRequestBytes(payload)
	})
}

// FuzzDecodePreambleResponseBytes proves the strict byte helper never panics on
// arbitrary input.
func FuzzDecodePreambleResponseBytes(f *testing.F) {
	response, err := EncodePreambleResponse(true, DefaultMuxCeilings(), testPolicy(), testIdentity(), testIncarnation(), 0)
	require.NoError(f, err)
	raw, err := proto.Marshal(response)
	require.NoError(f, err)
	for _, seed := range appendStrictScanSeeds([][]byte{raw}) {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, payload []byte) {
		_, _ = DecodePreambleResponseBytes(payload)
	})
}
