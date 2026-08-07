package client_test

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/usecase/client"
	"github.com/stretchr/testify/require"
)

func TestSessionConnectionOwnsCompleteAttachRequestForLocalAndRemoteTransports(t *testing.T) {
	var lifecycle domain.SessionLifecycleID
	lifecycle[0] = 1
	target := &domain.RemoteSessionTarget{
		Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: lifecycle,
		SessionName: "work", LiveTabID: "tab-1",
	}
	wantTarget := *target
	request := client.AttachRequest{
		Intent:            ports.IntentAttach,
		SessionName:       "work",
		RenderMode:        ports.RenderModeProxiedContent,
		Remote:            true,
		RemoteTarget:      target,
		EnvironmentPolicy: ports.EnvironmentPolicyDaemonOwned,
	}
	wantRequest := request
	wantRequest.RemoteTarget = &wantTarget
	local := portsmocks.NewMockTransport(t)
	remote := portsmocks.NewMockTransport(t)

	localConnection, err := client.NewSessionConnection(local, request)
	require.NoError(t, err)
	remoteConnection, err := client.NewSessionConnection(remote, request)
	require.NoError(t, err)

	target.LiveTabID = "mutated-tab"
	target.LifecycleID = domain.SessionLifecycleID{}
	returned := localConnection.AttachRequest()
	returned.RemoteTarget.LiveTabID = "mutated-returned-tab"

	require.Equal(t, localConnection.AttachRequest(), remoteConnection.AttachRequest())
	require.Equal(t, wantRequest, localConnection.AttachRequest())
	require.Same(t, local, localConnection.Transport())
	require.Same(t, remote, remoteConnection.Transport())
}

func TestSessionConnectionAttachRequestValidation(t *testing.T) {
	lifecycle := domain.SessionLifecycleID{1}
	remoteTarget := &domain.RemoteSessionTarget{
		Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: lifecycle,
		SessionName: "work", LiveTabID: "tab-1",
	}
	tests := []struct {
		name    string
		request client.AttachRequest
		wantErr bool
	}{
		{name: "ephemeral", request: client.AttachRequest{Intent: ports.IntentEphemeral}},
		{name: "new", request: client.AttachRequest{Intent: ports.IntentNew, SessionName: "work"}},
		{name: "attach", request: client.AttachRequest{Intent: ports.IntentAttach, SessionName: "work"}},
		{name: "resume", request: client.AttachRequest{Intent: ports.IntentResume, SessionName: "work"}},
		{name: "missing named session", request: client.AttachRequest{Intent: ports.IntentAttach}, wantErr: true},
		{name: "unsafe session", request: client.AttachRequest{Intent: ports.IntentAttach, SessionName: "my work"}, wantErr: true},
		{name: "unknown intent", request: client.AttachRequest{Intent: 99, SessionName: "work"}, wantErr: true},
		{name: "invalid render mode", request: client.AttachRequest{Intent: ports.IntentAttach, SessionName: "work", RenderMode: ports.RenderMode(99)}, wantErr: true},
		{name: "proxied without remote target", request: client.AttachRequest{Intent: ports.IntentAttach, SessionName: "work", RenderMode: ports.RenderModeProxiedContent}, wantErr: true},
		{name: "remote target with new intent", request: client.AttachRequest{Intent: ports.IntentNew, SessionName: "work", RemoteTarget: remoteTarget, EnvironmentPolicy: ports.EnvironmentPolicyDaemonOwned}, wantErr: true},
		{name: "daemon-owned without remote target", request: client.AttachRequest{Intent: ports.IntentAttach, SessionName: "work", EnvironmentPolicy: ports.EnvironmentPolicyDaemonOwned}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := portsmocks.NewMockTransport(t)
			_, err := client.NewSessionConnection(transport, tt.request)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestSessionConnectionRequiresTransport(t *testing.T) {
	_, err := client.NewSessionConnection(nil, client.AttachRequest{Intent: ports.IntentEphemeral})
	require.Error(t, err)
}
