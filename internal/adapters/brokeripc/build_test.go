package brokeripc

import (
	"context"
	"testing"
	"time"

	"github.com/bnema/vev/internal/adapters/brokerwire"
	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

func TestBuildRegistration(t *testing.T) {
	for _, tc := range []struct {
		name           string
		server, client string
		livePeer       bool
		retire         bool
		stale          string
	}{
		{name: "same build", server: "build-a", client: "build-a"},
		{name: "sole different build", server: "build-a", client: "build-b", retire: true},
		{name: "different build with live peer", server: "build-a", client: "build-b", livePeer: true, stale: "build-a"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			retired := make(chan struct{}, 1)
			e := startEndpoint(t, Config{Build: tc.server, OnRetire: func() { retired <- struct{}{} }})
			if tc.livePeer {
				peer, err := Dial(context.Background(), e.path, Config{Build: tc.server})
				require.NoError(t, err)
				defer peer.Close()
			}
			service, err := Dial(context.Background(), e.path, Config{Build: tc.client})
			if tc.retire {
				require.ErrorIs(t, err, ErrBrokerRetired)
				select {
				case <-retired:
				case <-time.After(5 * time.Second):
					t.Fatal("retire callback not called")
				}
				_, err = Dial(context.Background(), e.path, Config{Build: tc.client})
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			defer service.Close()
			stale, ok := StaleBuild(service)
			require.Equal(t, tc.stale != "", ok)
			require.Equal(t, tc.stale, stale)
			select {
			case <-retired:
				t.Fatal("unexpected retirement")
			default:
			}
		})
	}
}

func TestRetiringRefusesConcurrentAdmission(t *testing.T) {
	const epoch = ports.BrokerEpoch(0x51)
	path := testSocketPath(t)
	gated := &gatedAuthority{
		inner:   newFakeAuthority(epoch, ports.BrokerSnapshot{}),
		entered: make(chan struct{}), gate: make(chan struct{}),
	}
	bound, err := listen(path, epoch, gated, Config{Build: "build-a"}, ipc.SameUserPeerVerifier())
	require.NoError(t, err)
	defer bound.Close()
	l := bound.(*listener)
	raw := rawDial(t, &endpoint{t: t, path: path, epoch: epoch}, brokerwire.DefaultCeilings())
	select {
	case <-gated.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("admission not reached")
	}
	// The registration decision and the admission gate share l.mu. A pending
	// carriage prevents retirement; once retiring, the in-flight admission
	// cannot publish a new session even if its authority returns success.
	l.mu.Lock()
	require.Len(t, l.pending, 1)
	l.mu.Unlock()
	// Commit retirement under the listener lock after admission has entered
	// the authority, then release the authority gate.
	l.mu.Lock()
	l.retiring = true
	l.mu.Unlock()
	close(gated.gate)
	result := make(chan error, 1)
	go func() { _, err := raw.transport.RecvBounded(raw.ceilings.MaxReceiveEnvelopeBytes); result <- err }()
	select {
	case err := <-result:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("retiring listener admitted a pending session")
	}
}
