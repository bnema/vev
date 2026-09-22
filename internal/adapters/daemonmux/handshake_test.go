package daemonmux

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

// handshakeEndpoint is the independently resolved endpoint the client verifies
// the daemon response against.
func handshakeEndpoint(identity ports.BrokerDaemonIdentity, policy ports.BrokerPolicy) ports.BrokerDialTarget {
	return ports.BrokerDialTarget{Fence: ports.BrokerEndpointFence{Local: true}, Policy: policy, Address: "quic://127.0.0.1:7777", StartMode: ports.BrokerDaemonExistingOnly, ExpectedIdentity: ports.BrokerExpectedIdentity{Identity: identity, Bound: true}}
}

func mustServerBinding(t *testing.T) ServerBinding {
	t.Helper()
	binding, err := NewServerBinding(testIdentity(), testIncarnation(), testPolicy())
	require.NoError(t, err)
	return binding
}

// mustRemoteServerBinding provisions the same authority with the remote
// locality, for tests whose Open requests carry a validated remote
// registration rather than a local endpoint.
func mustRemoteServerBinding(t *testing.T) ServerBinding {
	t.Helper()
	binding, err := NewServerBindings(testIdentity(), testIncarnation(), []ServerPolicyAdmission{remoteAcceptance(testPolicy())})
	require.NoError(t, err)
	return binding
}

// encodedAcceptedResponse builds one valid acceptance and lets the caller
// corrupt exactly one field before marshaling.
func encodedAcceptedResponse(t *testing.T, mutate func(*wire.MuxPreambleResponse)) []byte {
	t.Helper()
	response, err := EncodePreambleResponse(true, DefaultMuxCeilings(), testPolicy(), testIdentity(), testIncarnation(), 0)
	require.NoError(t, err)
	if mutate != nil {
		mutate(response)
	}
	raw, err := proto.Marshal(response)
	require.NoError(t, err)
	return raw
}

// encodedAcceptedPolicy corrupts one field of the accepted policy.
func encodedAcceptedPolicy(t *testing.T, mutate func(*wire.BrokerWirePolicy)) []byte {
	t.Helper()
	return encodedAcceptedResponse(t, func(r *wire.MuxPreambleResponse) { mutate(r.AcceptedPolicy) })
}

func encodedRefusal(t *testing.T, code uint32) []byte {
	t.Helper()
	response, err := EncodePreambleResponse(false, DefaultMuxCeilings(), ports.BrokerPolicy{}, "", ports.BrokerDaemonIncarnation{}, code)
	require.NoError(t, err)
	raw, err := proto.Marshal(response)
	require.NoError(t, err)
	return raw
}

// encodedRequest builds one valid request and lets the caller corrupt exactly
// one field before marshaling.
func encodedRequest(t *testing.T, ceilings MuxCeilings, policy ports.BrokerPolicy, mutate func(*wire.MuxPreambleRequest)) []byte {
	t.Helper()
	request, err := EncodePreambleRequest(ceilings, policy)
	require.NoError(t, err)
	if mutate != nil {
		mutate(request)
	}
	raw, err := proto.Marshal(request)
	require.NoError(t, err)
	return raw
}

func awaitSentFrame(t *testing.T, carrier *fakeCarrier) []byte {
	t.Helper()
	select {
	case payload := <-carrier.sent:
		return payload
	case <-time.After(time.Second):
		t.Fatal("handshake sent no frame")
		return nil
	}
}

// TestClientHandshakeAcceptsVerifiedResponse proves a matching acceptance is
// accepted, yields the server authority, leaves the carrier open, and offers
// exactly the requested policy and ceilings.
func TestClientHandshakeAcceptsVerifiedResponse(t *testing.T) {
	carrier := newFakeCarrier()
	carrier.inbound <- fakeFrame{payload: encodedAcceptedResponse(t, nil)}

	result, err := RunClientHandshake(context.Background(), carrier, handshakeEndpoint(testIdentity(), testPolicy()), DefaultMuxCeilings())
	require.NoError(t, err)
	require.Equal(t, testIdentity(), result.Identity)
	require.Equal(t, testIncarnation(), result.Incarnation)
	require.Equal(t, testPolicy(), result.Policy)
	require.Equal(t, DefaultMuxCeilings(), result.Ceilings)
	require.Equal(t, 0, carrier.closeCalls())

	request, err := DecodePreambleRequestBytes(awaitSentFrame(t, carrier))
	require.NoError(t, err)
	require.Equal(t, DefaultMuxCeilings(), request.Ceilings)
	require.Equal(t, testPolicy(), request.Policy)
}

// TestClientHandshakeRejectsUntrustedResponses is the client authority table:
// wrong identity, every policy field, zero incarnation, wrong role/version/
// magic/epoch, a typed refusal, an acceptance carrying a rejection field, and
// excess ceilings are each refused and each close the carrier.
func TestClientHandshakeRejectsUntrustedResponses(t *testing.T) {
	smallOffer := MuxCeilings{
		MaxReceiveEnvelopeBytes: MinMuxEnvelopeBytes,
		StreamChunkLimit:        MinMuxChunkBytes,
		MaxStreams:              MinMuxStreams,
		MaxAggregateBytes:       MinMuxAggregateBytes,
	}

	tests := []struct {
		name     string
		offered  MuxCeilings
		response func(t *testing.T) []byte
		code     uint32
	}{
		{
			name:    "wrong identity",
			offered: DefaultMuxCeilings(),
			response: func(t *testing.T) []byte {
				return encodedAcceptedResponse(t, func(r *wire.MuxPreambleResponse) { r.DaemonIdentity = "other-identity" })
			},
		},
		{
			name:    "policy protocol version",
			offered: DefaultMuxCeilings(),
			response: func(t *testing.T) []byte {
				return encodedAcceptedPolicy(t, func(p *wire.BrokerWirePolicy) { p.ProtocolVersion++ })
			},
		},
		{
			name:    "policy catalogue version",
			offered: DefaultMuxCeilings(),
			response: func(t *testing.T) []byte {
				return encodedAcceptedPolicy(t, func(p *wire.BrokerWirePolicy) { p.CatalogueVersion++ })
			},
		},
		{
			name:    "policy environment",
			offered: DefaultMuxCeilings(),
			response: func(t *testing.T) []byte {
				return encodedAcceptedPolicy(t, func(p *wire.BrokerWirePolicy) { p.EnvironmentPolicy++ })
			},
		},
		{
			name:    "policy transport",
			offered: DefaultMuxCeilings(),
			response: func(t *testing.T) []byte {
				return encodedAcceptedPolicy(t, func(p *wire.BrokerWirePolicy) { p.Transport = "stdio" })
			},
		},
		{
			name:    "policy trust",
			offered: DefaultMuxCeilings(),
			response: func(t *testing.T) []byte {
				return encodedAcceptedPolicy(t, func(p *wire.BrokerWirePolicy) { p.Trust = "tofu" })
			},
		},
		{
			name:    "policy launch",
			offered: DefaultMuxCeilings(),
			response: func(t *testing.T) []byte {
				return encodedAcceptedPolicy(t, func(p *wire.BrokerWirePolicy) { p.Launch = "manual" })
			},
		},
		{
			name:    "policy isolation",
			offered: DefaultMuxCeilings(),
			response: func(t *testing.T) []byte {
				return encodedAcceptedPolicy(t, func(p *wire.BrokerWirePolicy) { p.Isolation = "system" })
			},
		},
		{
			name:    "zero incarnation",
			offered: DefaultMuxCeilings(),
			response: func(t *testing.T) []byte {
				return encodedAcceptedResponse(t, func(r *wire.MuxPreambleResponse) { r.Incarnation = make([]byte, 16) })
			},
		},
		{
			name:    "wrong role",
			offered: DefaultMuxCeilings(),
			response: func(t *testing.T) []byte {
				return encodedAcceptedResponse(t, func(r *wire.MuxPreambleResponse) { r.Role = &wire.PreambleRole{Role: MuxRoleClient} })
			},
		},
		{
			name:    "wrong version",
			offered: DefaultMuxCeilings(),
			response: func(t *testing.T) []byte {
				return encodedAcceptedResponse(t, func(r *wire.MuxPreambleResponse) { r.Version = uint32(protocol.Version) + 1 })
			},
		},
		{
			name:    "bad magic",
			offered: DefaultMuxCeilings(),
			response: func(t *testing.T) []byte {
				return encodedAcceptedResponse(t, func(r *wire.MuxPreambleResponse) { r.Magic = 0 })
			},
		},
		{
			name:    "epoch mismatch",
			offered: DefaultMuxCeilings(),
			response: func(t *testing.T) []byte {
				return encodedAcceptedResponse(t, func(r *wire.MuxPreambleResponse) { r.Epoch = wire.ProtocolEpoch + 1 })
			},
		},
		{
			name:    "refused response",
			offered: DefaultMuxCeilings(),
			response: func(t *testing.T) []byte {
				return encodedRefusal(t, RejectionVersionMismatch)
			},
			code: RejectionVersionMismatch,
		},
		{
			name:    "acceptance carrying rejection",
			offered: DefaultMuxCeilings(),
			response: func(t *testing.T) []byte {
				return encodedAcceptedResponse(t, func(r *wire.MuxPreambleResponse) {
					r.Rejection = &wire.PreambleRejectionCode{Code: RejectionLimitRefused}
				})
			},
		},
		{
			name:    "excess ceilings",
			offered: smallOffer,
			response: func(t *testing.T) []byte {
				return encodedAcceptedResponse(t, nil)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			carrier := newFakeCarrier()
			carrier.inbound <- fakeFrame{payload: test.response(t)}

			_, err := RunClientHandshake(context.Background(), carrier, handshakeEndpoint(testIdentity(), testPolicy()), test.offered)
			require.ErrorIs(t, err, ErrHandshakeRejected)
			if test.code != 0 {
				var refusal *HandshakeRefusal
				require.ErrorAs(t, err, &refusal)
				require.Equal(t, test.code, refusal.Code)
			}
			require.Equal(t, 1, carrier.closeCalls())
		})
	}
}

// TestServerHandshakeAcceptsAndNegotiates proves the server answers an exact
// request with the immutable binding and the element-wise minima, and leaves
// the carrier open.
func TestServerHandshakeAcceptsAndNegotiates(t *testing.T) {
	binding := mustServerBinding(t)
	clientCeilings := MuxCeilings{
		MaxReceiveEnvelopeBytes: MinMuxEnvelopeBytes,
		StreamChunkLimit:        8,
		MaxStreams:              4,
		MaxAggregateBytes:       MinMuxAggregateBytes,
	}
	serverCeilings := DefaultMuxCeilings()
	serverCeilings.MaxStreams = 2

	carrier := newFakeCarrier()
	carrier.inbound <- fakeFrame{payload: encodedRequest(t, clientCeilings, testPolicy(), nil)}

	result, err := RunServerHandshake(context.Background(), carrier, binding, serverCeilings)
	require.NoError(t, err)
	require.Equal(t, binding, result.Binding)
	require.Equal(t, EffectiveMuxCeilings(clientCeilings, serverCeilings), result.Ceilings)
	require.Equal(t, testPolicy(), result.Policy, "the accepted policy is the provisioned member")
	require.Equal(t, ports.SessionOriginLocal, result.Origin, "the accepted origin is the provisioned locality")
	require.Equal(t, 0, carrier.closeCalls())

	decoded, err := DecodePreambleResponseBytes(awaitSentFrame(t, carrier))
	require.NoError(t, err)
	require.True(t, decoded.Accepted)
	require.Equal(t, testIdentity(), decoded.Identity)
	require.Equal(t, testIncarnation(), decoded.Incarnation)
	require.Equal(t, testPolicy(), decoded.Policy)
	require.Equal(t, result.Ceilings, decoded.Ceilings)
}

// TestServerHandshakeEnforcesBindingPolicy proves a request differing in one
// policy field is refused with code 7, no accepted authority is echoed, and
// the carrier is closed.
func TestServerHandshakeEnforcesBindingPolicy(t *testing.T) {
	binding := mustServerBinding(t)
	tests := []struct {
		name   string
		mutate func(*ports.BrokerPolicy)
	}{
		{name: "protocol version", mutate: func(p *ports.BrokerPolicy) { p.ProtocolVersion++ }},
		{name: "catalogue schema version", mutate: func(p *ports.BrokerPolicy) { p.CatalogSchemaVersion++ }},
		{name: "environment policy", mutate: func(p *ports.BrokerPolicy) {
			p.EnvironmentPolicy = protocol.EnvironmentPolicyClientOwned
		}},
		{name: "transport", mutate: func(p *ports.BrokerPolicy) { p.Transport = "stdio" }},
		{name: "trust", mutate: func(p *ports.BrokerPolicy) { p.Trust = "tofu" }},
		{name: "launch", mutate: func(p *ports.BrokerPolicy) { p.Launch = "manual" }},
		{name: "isolation", mutate: func(p *ports.BrokerPolicy) { p.Isolation = "system" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requested := testPolicy()
			test.mutate(&requested)

			carrier := newFakeCarrier()
			carrier.inbound <- fakeFrame{payload: encodedRequest(t, DefaultMuxCeilings(), requested, nil)}

			_, err := RunServerHandshake(context.Background(), carrier, binding, DefaultMuxCeilings())
			var refusal *HandshakeRefusal
			require.ErrorAs(t, err, &refusal)
			require.Equal(t, uint32(RejectionLimitRefused), refusal.Code)
			require.Equal(t, 1, carrier.closeCalls())

			raw := &wire.MuxPreambleResponse{}
			require.NoError(t, proto.Unmarshal(awaitSentFrame(t, carrier), raw))
			require.False(t, raw.GetAccepted())
			require.Nil(t, raw.GetAcceptedPolicy())
			require.Empty(t, raw.GetDaemonIdentity())
			require.Empty(t, raw.GetIncarnation())
		})
	}
}

// TestServerHandshakeRefusesMalformedRequest proves the precise refusal code
// taxonomy for a bad magic, epoch, version, or role, plus oversize and
// undecodable payloads, and that every refusal closes the carrier.
func TestServerHandshakeRefusesMalformedRequest(t *testing.T) {
	binding := mustServerBinding(t)
	tests := []struct {
		name    string
		request func(t *testing.T) []byte
		code    uint32
	}{
		{
			name: "bad magic",
			request: func(t *testing.T) []byte {
				return encodedRequest(t, DefaultMuxCeilings(), testPolicy(), func(r *wire.MuxPreambleRequest) { r.Magic = 0 })
			},
			code: RejectionBadMagic,
		},
		{
			name: "epoch mismatch",
			request: func(t *testing.T) []byte {
				return encodedRequest(t, DefaultMuxCeilings(), testPolicy(), func(r *wire.MuxPreambleRequest) { r.Epoch = wire.ProtocolEpoch + 1 })
			},
			code: RejectionEpochMismatch,
		},
		{
			name: "wrong version",
			request: func(t *testing.T) []byte {
				return encodedRequest(t, DefaultMuxCeilings(), testPolicy(), func(r *wire.MuxPreambleRequest) { r.Version = uint32(protocol.Version) + 1 })
			},
			code: RejectionVersionMismatch,
		},
		{
			name: "wrong role",
			request: func(t *testing.T) []byte {
				return encodedRequest(t, DefaultMuxCeilings(), testPolicy(), func(r *wire.MuxPreambleRequest) { r.Role = &wire.PreambleRole{Role: MuxRoleServer} })
			},
			code: RejectionWrongRole,
		},
		{
			name: "oversize payload",
			request: func(t *testing.T) []byte {
				return make([]byte, MuxPreambleLimit+1)
			},
			code: RejectionLimitRefused,
		},
		{
			name: "undecodable payload",
			request: func(t *testing.T) []byte {
				return []byte{0xff, 0xff, 0xff}
			},
			code: RejectionLimitRefused,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			carrier := newFakeCarrier()
			carrier.inbound <- fakeFrame{payload: test.request(t)}

			_, err := RunServerHandshake(context.Background(), carrier, binding, DefaultMuxCeilings())
			var refusal *HandshakeRefusal
			require.ErrorAs(t, err, &refusal)
			require.Equal(t, test.code, refusal.Code)
			require.Equal(t, 1, carrier.closeCalls())

			decoded, err := DecodePreambleResponseBytes(awaitSentFrame(t, carrier))
			require.ErrorIs(t, err, ErrPreambleRejected)
			require.Equal(t, test.code, decoded.Code)
		})
	}
}

// TestHandshakeRefusesInvalidConfiguration proves nil carriers, invalid
// endpoints, invalid bindings, and invalid local ceilings fail closed with the
// configuration sentinel.
func TestHandshakeRefusesInvalidConfiguration(t *testing.T) {
	t.Run("client nil carrier", func(t *testing.T) {
		_, err := RunClientHandshake(context.Background(), nil, handshakeEndpoint(testIdentity(), testPolicy()), DefaultMuxCeilings())
		require.ErrorIs(t, err, ErrHandshakeConfig)
	})
	t.Run("client invalid endpoint", func(t *testing.T) {
		carrier := newFakeCarrier()
		_, err := RunClientHandshake(context.Background(), carrier, handshakeEndpoint("", testPolicy()), DefaultMuxCeilings())
		require.ErrorIs(t, err, ErrHandshakeConfig)
		require.Equal(t, 1, carrier.closeCalls())
	})
	t.Run("client invalid ceilings", func(t *testing.T) {
		carrier := newFakeCarrier()
		_, err := RunClientHandshake(context.Background(), carrier, handshakeEndpoint(testIdentity(), testPolicy()), MuxCeilings{})
		require.ErrorIs(t, err, ErrHandshakeConfig)
		require.Equal(t, 1, carrier.closeCalls())
	})
	t.Run("server nil carrier", func(t *testing.T) {
		_, err := RunServerHandshake(context.Background(), nil, mustServerBinding(t), DefaultMuxCeilings())
		require.ErrorIs(t, err, ErrHandshakeConfig)
	})
	t.Run("server invalid binding", func(t *testing.T) {
		carrier := newFakeCarrier()
		_, err := RunServerHandshake(context.Background(), carrier, ServerBinding{}, DefaultMuxCeilings())
		require.ErrorIs(t, err, ErrHandshakeConfig)
		require.Equal(t, 1, carrier.closeCalls())
	})
	t.Run("server invalid ceilings", func(t *testing.T) {
		carrier := newFakeCarrier()
		_, err := RunServerHandshake(context.Background(), carrier, mustServerBinding(t), MuxCeilings{})
		require.ErrorIs(t, err, ErrHandshakeConfig)
		require.Equal(t, 1, carrier.closeCalls())
	})
}

// TestHandshakeHonorsCallerContext proves the caller's context is the single
// absolute bound on the exchange: a cancelled or already-expired context ends
// the handshake promptly and closes the carrier.
func TestHandshakeHonorsCallerContext(t *testing.T) {
	endpoint := handshakeEndpoint(testIdentity(), testPolicy())
	binding := mustServerBinding(t)

	tests := []struct {
		name string
		ctx  func() (context.Context, context.CancelFunc)
		run  func(context.Context, *fakeCarrier) error
		want error
	}{
		{
			name: "client cancelled",
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, func() {}
			},
			run: func(ctx context.Context, carrier *fakeCarrier) error {
				_, err := RunClientHandshake(ctx, carrier, endpoint, DefaultMuxCeilings())
				return err
			},
			want: context.Canceled,
		},
		{
			name: "client deadline exceeded",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			},
			run: func(ctx context.Context, carrier *fakeCarrier) error {
				_, err := RunClientHandshake(ctx, carrier, endpoint, DefaultMuxCeilings())
				return err
			},
			want: context.DeadlineExceeded,
		},
		{
			name: "server cancelled",
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, func() {}
			},
			run: func(ctx context.Context, carrier *fakeCarrier) error {
				_, err := RunServerHandshake(ctx, carrier, binding, DefaultMuxCeilings())
				return err
			},
			want: context.Canceled,
		},
		{
			name: "server deadline exceeded",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			},
			run: func(ctx context.Context, carrier *fakeCarrier) error {
				_, err := RunServerHandshake(ctx, carrier, binding, DefaultMuxCeilings())
				return err
			},
			want: context.DeadlineExceeded,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := test.ctx()
			defer cancel()
			carrier := newFakeCarrier()
			err := test.run(ctx, carrier)
			require.ErrorIs(t, err, test.want)
			require.Equal(t, 1, carrier.closeCalls())
		})
	}
}

// TestHandshakeEndToEndNegotiates runs the broker and daemon sides over one
// in-memory link and proves they agree on the element-wise minima and the
// binding the daemon enforced.
func TestHandshakeEndToEndNegotiates(t *testing.T) {
	clientCarrier, serverCarrier := newMemCarrierPair(4)
	clientCeilings := MuxCeilings{
		MaxReceiveEnvelopeBytes: MinMuxEnvelopeBytes,
		StreamChunkLimit:        8,
		MaxStreams:              4,
		MaxAggregateBytes:       MinMuxAggregateBytes,
	}
	serverCeilings := DefaultMuxCeilings()
	serverCeilings.MaxStreams = 2
	binding := mustServerBinding(t)
	endpoint := handshakeEndpoint(testIdentity(), testPolicy())

	type clientOutcome struct {
		result ClientHandshakeResult
		err    error
	}
	type serverOutcome struct {
		result ServerHandshakeResult
		err    error
	}
	clientCh := make(chan clientOutcome, 1)
	serverCh := make(chan serverOutcome, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		result, err := RunClientHandshake(ctx, clientCarrier, endpoint, clientCeilings)
		clientCh <- clientOutcome{result, err}
	}()
	go func() {
		result, err := RunServerHandshake(ctx, serverCarrier, binding, serverCeilings)
		serverCh <- serverOutcome{result, err}
	}()

	client := <-clientCh
	server := <-serverCh
	require.NoError(t, client.err)
	require.NoError(t, server.err)
	require.Equal(t, testIdentity(), client.result.Identity)
	require.Equal(t, testIncarnation(), client.result.Incarnation)
	require.Equal(t, testPolicy(), client.result.Policy)
	require.Equal(t, binding, server.result.Binding)
	require.Equal(t, EffectiveMuxCeilings(clientCeilings, serverCeilings), client.result.Ceilings)
	require.Equal(t, server.result.Ceilings, client.result.Ceilings)
}
