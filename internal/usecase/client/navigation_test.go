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
	tests := []struct {
		name    string
		request AttachRequest
		valid   bool
	}{
		{name: "ordinary route", request: AttachRequest{}, valid: true},
		{name: "inventory capability", request: AttachRequest{NavigationCapabilities: protocol.NavigationCapabilityInventory}, valid: true},
		{name: "unknown capability", request: AttachRequest{NavigationCapabilities: 16}, valid: false},
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
