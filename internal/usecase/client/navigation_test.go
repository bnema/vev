package client

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestSyncReconnectRemoteKeepsRequestAndToastClassificationTogether(t *testing.T) {
	reconnect := &reconnectUI{}
	for _, tt := range []struct {
		name   string
		remote bool
	}{
		{name: "local picker", remote: false},
		{name: "remote handoff", remote: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.remote, syncReconnectRemote(reconnect, tt.remote))
			require.Equal(t, tt.remote, reconnect.remote)
		})
	}
}

func TestValidateAttachRequestNavigationTable(t *testing.T) {
	var lifecycle domain.SessionLifecycleID
	lifecycle[0] = 1
	remoteRoute := &domain.RemoteSessionTarget{
		Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: lifecycle,
		SessionName: "work", LiveTabID: "tab-1",
	}
	tests := []struct {
		name    string
		request AttachRequest
		valid   bool
	}{
		{name: "ordinary route", request: AttachRequest{}, valid: true},
		{name: "home picker without remote route", request: AttachRequest{Intent: protocol.IntentAttach, SessionName: "work", NavigationCapabilities: protocol.NavigationCapabilityHomePicker}, valid: false},
		{name: "home picker on daemon-owned remote route", request: AttachRequest{Intent: protocol.IntentAttach, SessionName: "work", Remote: true, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned, NavigationCapabilities: protocol.NavigationCapabilityHomePicker}, valid: true},
		{name: "home picker on client-owned remote new route", request: AttachRequest{Intent: protocol.IntentNew, SessionName: "example", Remote: true, EnvironmentPolicy: protocol.EnvironmentPolicyClientOwned, NavigationCapabilities: protocol.NavigationCapabilityHomePicker}, valid: true},
		{name: "back on client-owned route", request: AttachRequest{StartupOverlay: protocol.StartupOverlaySessionPicker, NavigationCapabilities: protocol.NavigationCapabilityBack}, valid: true},
		{name: "back on remote-target route", request: AttachRequest{Intent: protocol.IntentAttach, SessionName: "work", RemoteTarget: remoteRoute, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned, StartupOverlay: protocol.StartupOverlaySessionPicker, NavigationCapabilities: protocol.NavigationCapabilityBack}, valid: false},
		{name: "unknown capability", request: AttachRequest{NavigationCapabilities: 4}, valid: false},
		{name: "back without startup picker", request: AttachRequest{NavigationCapabilities: protocol.NavigationCapabilityBack}, valid: false},
		{name: "startup picker without back", request: AttachRequest{StartupOverlay: protocol.StartupOverlaySessionPicker}, valid: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateAttachRequest(tt.request)
			if tt.valid {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.ErrorContains(t, err, "navigation")
		})
	}
}
