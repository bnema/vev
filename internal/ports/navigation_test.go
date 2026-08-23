package ports

import (
	"encoding/hex"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestNavigationActionWireTable(t *testing.T) {
	tests := []struct {
		name    string
		action  NavigationAction
		payload []byte
		valid   bool
	}{
		{name: "open home", action: NavigationOpenHomePicker, payload: []byte{1}, valid: true},
		{name: "back", action: NavigationBack, payload: []byte{2}, valid: true},
		{name: "zero", action: 0, payload: []byte{0}, valid: false},
		{name: "unknown", action: 3, payload: []byte{3}, valid: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MarshalNavigationAction(tt.action)
			if !tt.valid {
				require.Nil(t, got)
				_, err := UnmarshalNavigationAction(tt.payload)
				require.ErrorIs(t, err, ErrInvalidNavigation)
				return
			}
			require.Equal(t, tt.payload, got)
			decoded, err := UnmarshalNavigationAction(got)
			require.NoError(t, err)
			require.Equal(t, tt.action, decoded)
			assertAllPrefixesFail(t, got, UnmarshalNavigationAction)
			_, err = UnmarshalNavigationAction(append(append([]byte(nil), got...), 0))
			require.ErrorIs(t, err, ErrInvalidNavigation)
		})
	}
}

func TestHelloNavigationValidationTable(t *testing.T) {
	base := Hello{Version: ProtocolVersion, Intent: IntentAttach, Size: domain.Size{Cols: 80, Rows: 24}}
	remoteTarget := &domain.RemoteSessionTarget{
		Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: domain.SessionLifecycleID{1},
		SessionName: "work", LiveTabID: "tab-1",
	}
	tests := []struct {
		name        string
		hello       Hello
		valid       bool
		capability  NavigationCapabilities
		overlay     StartupOverlay
		wantPayload string
	}{
		{name: "ordinary", hello: base, valid: true},
		{name: "resume route", hello: Hello{Version: ProtocolVersion, Intent: IntentResume, Size: domain.Size{Cols: 80, Rows: 24}}, valid: true},
		{name: "daemon-owned home picker", hello: Hello{Version: ProtocolVersion, Intent: IntentAttach, Size: domain.Size{Cols: 80, Rows: 24}, NavigationCapabilities: NavigationCapabilityHomePicker, EnvironmentPolicy: EnvironmentPolicyDaemonOwned}, valid: true, capability: NavigationCapabilityHomePicker},
		{name: "remote-target home picker", hello: Hello{Version: ProtocolVersion, Intent: IntentAttach, Name: "work", Size: domain.Size{Cols: 80, Rows: 24}, RemoteTarget: remoteTarget, NavigationCapabilities: NavigationCapabilityHomePicker, EnvironmentPolicy: EnvironmentPolicyDaemonOwned}, valid: true, capability: NavigationCapabilityHomePicker},
		{name: "client-owned home picker rejected", hello: Hello{Version: ProtocolVersion, Intent: IntentAttach, Size: domain.Size{Cols: 80, Rows: 24}, NavigationCapabilities: NavigationCapabilityHomePicker}, valid: false},
		{name: "daemon-owned back picker rejected", hello: Hello{Version: ProtocolVersion, Intent: IntentAttach, Size: domain.Size{Cols: 80, Rows: 24}, NavigationCapabilities: NavigationCapabilityBack, StartupOverlay: StartupOverlaySessionPicker, EnvironmentPolicy: EnvironmentPolicyDaemonOwned}, valid: false},
		{name: "remote-target back picker rejected", hello: Hello{Version: ProtocolVersion, Intent: IntentAttach, Name: "work", Size: domain.Size{Cols: 80, Rows: 24}, RemoteTarget: remoteTarget, NavigationCapabilities: NavigationCapabilityBack, StartupOverlay: StartupOverlaySessionPicker, EnvironmentPolicy: EnvironmentPolicyDaemonOwned}, valid: false},
		{name: "back with startup picker", hello: Hello{Version: ProtocolVersion, Intent: IntentAttach, Size: domain.Size{Cols: 80, Rows: 24}, NavigationCapabilities: NavigationCapabilityBack, StartupOverlay: StartupOverlaySessionPicker}, valid: true, capability: NavigationCapabilityBack, overlay: StartupOverlaySessionPicker, wantPayload: "002102" + "00000000000000000000000000000000" + "0000000000000000" + "0000" + "00500018" + "0000" + "0000" + "00" + "00" + "00000000" + "00" + "00" + "00" + "0000" + "0201"},
		{name: "new rejects navigation", hello: Hello{Version: ProtocolVersion, Intent: IntentNew, Size: domain.Size{Cols: 80, Rows: 24}, NavigationCapabilities: NavigationCapabilityBack, StartupOverlay: StartupOverlaySessionPicker}, valid: false},
		{name: "unknown capability", hello: Hello{Version: ProtocolVersion, Intent: IntentAttach, Size: domain.Size{Cols: 80, Rows: 24}, NavigationCapabilities: 4}, valid: false},
		{name: "back without picker", hello: Hello{Version: ProtocolVersion, Intent: IntentAttach, Size: domain.Size{Cols: 80, Rows: 24}, NavigationCapabilities: NavigationCapabilityBack}, valid: false},
		{name: "picker without back", hello: Hello{Version: ProtocolVersion, Intent: IntentAttach, Size: domain.Size{Cols: 80, Rows: 24}, StartupOverlay: StartupOverlaySessionPicker}, valid: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateHello(tt.hello)
			if tt.valid {
				require.NoError(t, err)
				payload := MarshalHello(tt.hello)
				require.NotNil(t, payload)
				if tt.wantPayload != "" {
					expected, decodeErr := hex.DecodeString(tt.wantPayload)
					require.NoError(t, decodeErr)
					require.Equal(t, expected, payload)
					assertAllPrefixesFail(t, expected, UnmarshalHello)
					assertTrailingGarbageFails(t, expected, UnmarshalHello)
				}
				decoded, decodeErr := UnmarshalHello(payload)
				require.NoError(t, decodeErr)
				require.Equal(t, tt.capability, decoded.NavigationCapabilities)
				require.Equal(t, tt.overlay, decoded.StartupOverlay)
				return
			}
			require.Error(t, err)
			require.Nil(t, MarshalHello(tt.hello))
		})
	}
}
