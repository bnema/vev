package client

import (
	"testing"

	"github.com/bnema/vev/internal/ports"
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
	tests := []struct {
		name    string
		request AttachRequest
		valid   bool
	}{
		{name: "ordinary route", request: AttachRequest{}, valid: true},
		{name: "unknown capability", request: AttachRequest{NavigationCapabilities: 4}, valid: false},
		{name: "back without startup picker", request: AttachRequest{NavigationCapabilities: ports.NavigationCapabilityBack}, valid: false},
		{name: "startup picker without back", request: AttachRequest{StartupOverlay: ports.StartupOverlaySessionPicker}, valid: false},
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
