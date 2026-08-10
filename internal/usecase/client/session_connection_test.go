package client_test

import (
	"testing"

	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/usecase/client"
	"github.com/stretchr/testify/require"
)

func TestSessionConnectionUsesOneAttachShapeForLocalAndRemoteTransports(t *testing.T) {
	target := client.SessionTarget{Intent: ports.IntentAttach, SessionName: "work"}
	local := portsmocks.NewMockTransport(t)
	remote := portsmocks.NewMockTransport(t)

	localConnection, err := client.NewSessionConnection(local, target)
	require.NoError(t, err)
	remoteConnection, err := client.NewSessionConnection(remote, target)
	require.NoError(t, err)

	require.Equal(t, localConnection.AttachRequest(), remoteConnection.AttachRequest())
	require.Equal(t, client.AttachRequest{Intent: ports.IntentAttach, SessionName: "work"}, localConnection.AttachRequest())
	require.Same(t, local, localConnection.Transport())
	require.Same(t, remote, remoteConnection.Transport())
}

func TestSessionTargetValidation(t *testing.T) {
	tests := []struct {
		name      string
		target    client.SessionTarget
		wantErr   bool
		wantErrIs error
	}{
		{name: "ephemeral", target: client.SessionTarget{Intent: ports.IntentEphemeral}},
		{name: "named ephemeral", target: client.SessionTarget{Intent: ports.IntentEphemeral, SessionName: "work"}, wantErr: true, wantErrIs: client.ErrEphemeralSessionName},
		{name: "new", target: client.SessionTarget{Intent: ports.IntentNew, SessionName: "work"}},
		{name: "attach", target: client.SessionTarget{Intent: ports.IntentAttach, SessionName: "work"}},
		{name: "resume", target: client.SessionTarget{Intent: ports.IntentResume, SessionName: "work"}},
		{name: "missing named session", target: client.SessionTarget{Intent: ports.IntentAttach}, wantErr: true},
		{name: "unsafe session", target: client.SessionTarget{Intent: ports.IntentAttach, SessionName: "my work"}, wantErr: true},
		{name: "unknown intent", target: client.SessionTarget{Intent: 99, SessionName: "work"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := portsmocks.NewMockTransport(t)
			_, err := client.NewSessionConnection(transport, tt.target)
			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrIs != nil {
					require.ErrorIs(t, err, tt.wantErrIs)
				}
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestSessionConnectionRequiresTransport(t *testing.T) {
	_, err := client.NewSessionConnection(nil, client.SessionTarget{Intent: ports.IntentEphemeral})
	require.Error(t, err)
}
