package wire

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestHelloNavigationValidationTable(t *testing.T) {
	base := protocol.Hello{Version: protocol.Version, Intent: protocol.IntentAttach, Size: domain.Size{Cols: 80, Rows: 24}}
	remoteTarget := &domain.RemoteSessionTarget{
		Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: domain.SessionLifecycleID{1},
		SessionName: "work", LiveTabID: "tab-1",
	}
	tests := []struct {
		name       string
		hello      protocol.Hello
		valid      bool
		capability protocol.NavigationCapabilities
	}{
		{name: "ordinary", hello: base, valid: true},
		{name: "resume route", hello: protocol.Hello{Version: protocol.Version, Intent: protocol.IntentResume, Size: domain.Size{Cols: 80, Rows: 24}}, valid: true},
		{name: "inventory capability attached", hello: protocol.Hello{Version: protocol.Version, Intent: protocol.IntentAttach, Size: domain.Size{Cols: 80, Rows: 24}, NavigationCapabilities: protocol.NavigationCapabilityInventory}, valid: true, capability: protocol.NavigationCapabilityInventory},
		{name: "inventory capability on a remote target", hello: protocol.Hello{Version: protocol.Version, Intent: protocol.IntentAttach, Name: "work", Size: domain.Size{Cols: 80, Rows: 24}, RemoteTarget: remoteTarget, NavigationCapabilities: protocol.NavigationCapabilityInventory, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned}, valid: true, capability: protocol.NavigationCapabilityInventory},
		{name: "new accepts inventory", hello: protocol.Hello{Version: protocol.Version, Intent: protocol.IntentNew, Size: domain.Size{Cols: 80, Rows: 24}, NavigationCapabilities: protocol.NavigationCapabilityInventory}, valid: true, capability: protocol.NavigationCapabilityInventory},
		{name: "ephemeral rejects inventory", hello: protocol.Hello{Version: protocol.Version, Intent: protocol.IntentEphemeral, Size: domain.Size{Cols: 80, Rows: 24}, NavigationCapabilities: protocol.NavigationCapabilityInventory}, valid: false},
		{name: "unknown capability", hello: protocol.Hello{Version: protocol.Version, Intent: protocol.IntentAttach, Size: domain.Size{Cols: 80, Rows: 24}, NavigationCapabilities: 8}, valid: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := protocol.ValidateHello(tt.hello)
			if tt.valid {
				require.NoError(t, err)
				payload := MarshalHello(tt.hello)
				require.NotNil(t, payload)
				decoded, decodeErr := UnmarshalHello(payload)
				require.NoError(t, decodeErr)
				require.Equal(t, tt.capability, decoded.NavigationCapabilities)
				return
			}
			require.Error(t, err)
			require.Nil(t, MarshalHello(tt.hello))
		})
	}
}
