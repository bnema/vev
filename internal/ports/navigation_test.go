package ports

import (
	"encoding/hex"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestNavigationDirectiveWireTable(t *testing.T) {
	lease := protocol.ParkedRouteLeaseID{1, 2, 3}
	tests := []struct {
		name      string
		directive protocol.NavigationDirective
		valid     bool
	}{
		{name: "open home", directive: protocol.NavigationDirective{Action: protocol.NavigationOpenHomePicker, LeaseID: lease}, valid: true},
		{name: "back", directive: protocol.NavigationDirective{Action: protocol.NavigationBack}, valid: true},
		{name: "open home without lease", directive: protocol.NavigationDirective{Action: protocol.NavigationOpenHomePicker}},
		{name: "back with lease", directive: protocol.NavigationDirective{Action: protocol.NavigationBack, LeaseID: lease}},
		{name: "zero", directive: protocol.NavigationDirective{}},
		{name: "unknown", directive: protocol.NavigationDirective{Action: 3}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MarshalNavigationDirective(tt.directive)
			if !tt.valid {
				require.Nil(t, got)
				return
			}
			decoded, err := UnmarshalNavigationDirective(got)
			require.NoError(t, err)
			require.Equal(t, tt.directive, decoded)
			assertAllPrefixesFail(t, got, UnmarshalNavigationDirective)
			_, err = UnmarshalNavigationDirective(append(append([]byte(nil), got...), 0))
			require.ErrorIs(t, err, protocol.ErrInvalidNavigation)
		})
	}
}

func TestParkedRouteWireRoundTripsStrictly(t *testing.T) {
	lease := protocol.ParkedRouteLeaseID{1, 2, 3}
	target := &domain.RemoteSessionTarget{
		Endpoint: "remote", DisplayOrigin: "remote", LifecycleID: domain.SessionLifecycleID{4},
		SessionName: "work", LiveTabID: "tab-1",
	}
	requests := []struct {
		name   string
		value  protocol.ParkedRouteRequest
		golden string
	}{
		{name: "prepare", value: protocol.ParkedRouteRequest{RequestID: 1, LeaseID: lease, Action: protocol.ParkedRoutePrepare}, golden: "0000000000000001" + "01020300000000000000000000000000" + "010001"},
		{name: "resume", value: protocol.ParkedRouteRequest{RequestID: 2, LeaseID: lease, Action: protocol.ParkedRouteResume}},
		{name: "switch", value: protocol.ParkedRouteRequest{RequestID: 3, LeaseID: lease, Action: protocol.ParkedRouteSwitch, Target: target}},
	}
	for _, tt := range requests {
		t.Run("request "+tt.name, func(t *testing.T) {
			payload := MarshalParkedRouteRequest(tt.value)
			require.NotNil(t, payload)
			if tt.golden != "" {
				golden, err := hex.DecodeString(tt.golden)
				require.NoError(t, err)
				require.Equal(t, golden, payload)
			}
			decoded, err := UnmarshalParkedRouteRequest(payload)
			require.NoError(t, err)
			require.Equal(t, tt.value, decoded)
			assertAllPrefixesFail(t, payload, UnmarshalParkedRouteRequest)
			assertTrailingGarbageFails(t, payload, UnmarshalParkedRouteRequest)
		})
	}

	responses := []struct {
		name   string
		value  protocol.ParkedRouteResponse
		golden string
	}{
		{name: "ready", value: protocol.ParkedRouteResponse{RequestID: 1, Status: protocol.ParkedRouteReady}, golden: "000000000000000101"},
		{name: "resumed", value: protocol.ParkedRouteResponse{RequestID: 2, Status: protocol.ParkedRouteResumed}},
		{name: "switched", value: protocol.ParkedRouteResponse{RequestID: 3, Status: protocol.ParkedRouteSwitched}},
		{name: "rejected", value: protocol.ParkedRouteResponse{RequestID: 4, Status: protocol.ParkedRouteRejected}},
		{name: "expired", value: protocol.ParkedRouteResponse{RequestID: 5, Status: protocol.ParkedRouteExpired}},
		{name: "stale target", value: protocol.ParkedRouteResponse{RequestID: 6, Status: protocol.ParkedRouteStaleTarget}},
	}
	for _, tt := range responses {
		t.Run("response "+tt.name, func(t *testing.T) {
			payload := MarshalParkedRouteResponse(tt.value)
			if tt.golden != "" {
				golden, err := hex.DecodeString(tt.golden)
				require.NoError(t, err)
				require.Equal(t, golden, payload)
			}
			decoded, err := UnmarshalParkedRouteResponse(payload)
			require.NoError(t, err)
			require.Equal(t, tt.value, decoded)
			assertAllPrefixesFail(t, payload, UnmarshalParkedRouteResponse)
			assertTrailingGarbageFails(t, payload, UnmarshalParkedRouteResponse)
		})
	}

	invalidRequests := []struct {
		name  string
		value protocol.ParkedRouteRequest
	}{
		{name: "zero request ID", value: protocol.ParkedRouteRequest{LeaseID: lease, Action: protocol.ParkedRoutePrepare}},
		{name: "zero lease", value: protocol.ParkedRouteRequest{RequestID: 1, Action: protocol.ParkedRoutePrepare}},
		{name: "unknown action", value: protocol.ParkedRouteRequest{RequestID: 1, LeaseID: lease, Action: 99}},
		{name: "prepare target", value: protocol.ParkedRouteRequest{RequestID: 1, LeaseID: lease, Action: protocol.ParkedRoutePrepare, Target: target}},
		{name: "resume target", value: protocol.ParkedRouteRequest{RequestID: 1, LeaseID: lease, Action: protocol.ParkedRouteResume, Target: target}},
		{name: "missing switch target", value: protocol.ParkedRouteRequest{RequestID: 1, LeaseID: lease, Action: protocol.ParkedRouteSwitch}},
		{name: "invalid switch target", value: protocol.ParkedRouteRequest{RequestID: 1, LeaseID: lease, Action: protocol.ParkedRouteSwitch, Target: &domain.RemoteSessionTarget{}}},
	}
	for _, tt := range invalidRequests {
		t.Run("invalid request "+tt.name, func(t *testing.T) {
			require.Nil(t, MarshalParkedRouteRequest(tt.value))
		})
	}
	for _, tt := range []struct {
		name  string
		value protocol.ParkedRouteResponse
	}{
		{name: "zero request ID", value: protocol.ParkedRouteResponse{Status: protocol.ParkedRouteReady}},
		{name: "zero status", value: protocol.ParkedRouteResponse{RequestID: 1}},
		{name: "unknown status", value: protocol.ParkedRouteResponse{RequestID: 1, Status: 99}},
	} {
		t.Run("invalid response "+tt.name, func(t *testing.T) {
			require.Nil(t, MarshalParkedRouteResponse(tt.value))
		})
	}
}

func TestHelloNavigationValidationTable(t *testing.T) {
	base := protocol.Hello{Version: protocol.Version, Intent: protocol.IntentAttach, Size: domain.Size{Cols: 80, Rows: 24}}
	remoteTarget := &domain.RemoteSessionTarget{
		Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: domain.SessionLifecycleID{1},
		SessionName: "work", LiveTabID: "tab-1",
	}
	tests := []struct {
		name        string
		hello       protocol.Hello
		valid       bool
		capability  protocol.NavigationCapabilities
		overlay     protocol.StartupOverlay
		wantPayload string
	}{
		{name: "ordinary", hello: base, valid: true},
		{name: "resume route", hello: protocol.Hello{Version: protocol.Version, Intent: protocol.IntentResume, Size: domain.Size{Cols: 80, Rows: 24}}, valid: true},
		{name: "daemon-owned home picker", hello: protocol.Hello{Version: protocol.Version, Intent: protocol.IntentAttach, Size: domain.Size{Cols: 80, Rows: 24}, NavigationCapabilities: protocol.NavigationCapabilityHomePicker, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned}, valid: true, capability: protocol.NavigationCapabilityHomePicker},
		{name: "remote-target home picker", hello: protocol.Hello{Version: protocol.Version, Intent: protocol.IntentAttach, Name: "work", Size: domain.Size{Cols: 80, Rows: 24}, RemoteTarget: remoteTarget, NavigationCapabilities: protocol.NavigationCapabilityHomePicker, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned}, valid: true, capability: protocol.NavigationCapabilityHomePicker},
		{name: "client-owned home picker rejected", hello: protocol.Hello{Version: protocol.Version, Intent: protocol.IntentAttach, Size: domain.Size{Cols: 80, Rows: 24}, NavigationCapabilities: protocol.NavigationCapabilityHomePicker}, valid: false},
		{name: "daemon-owned back picker rejected", hello: protocol.Hello{Version: protocol.Version, Intent: protocol.IntentAttach, Size: domain.Size{Cols: 80, Rows: 24}, NavigationCapabilities: protocol.NavigationCapabilityBack, StartupOverlay: protocol.StartupOverlaySessionPicker, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned}, valid: false},
		{name: "remote-target back picker rejected", hello: protocol.Hello{Version: protocol.Version, Intent: protocol.IntentAttach, Name: "work", Size: domain.Size{Cols: 80, Rows: 24}, RemoteTarget: remoteTarget, NavigationCapabilities: protocol.NavigationCapabilityBack, StartupOverlay: protocol.StartupOverlaySessionPicker, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned}, valid: false},
		{name: "back with startup picker", hello: protocol.Hello{Version: protocol.Version, Intent: protocol.IntentAttach, Size: domain.Size{Cols: 80, Rows: 24}, NavigationCapabilities: protocol.NavigationCapabilityBack, StartupOverlay: protocol.StartupOverlaySessionPicker}, valid: true, capability: protocol.NavigationCapabilityBack, overlay: protocol.StartupOverlaySessionPicker, wantPayload: "002502" + "00000000000000000000000000000000" + "0000000000000000" + "0000" + "00500018" + "00000000" + "0000" + "0000" + "00" + "00" + "00000000" + "00" + "00" + "00" + "0000" + "02010000"},
		{name: "new rejects navigation", hello: protocol.Hello{Version: protocol.Version, Intent: protocol.IntentNew, Size: domain.Size{Cols: 80, Rows: 24}, NavigationCapabilities: protocol.NavigationCapabilityBack, StartupOverlay: protocol.StartupOverlaySessionPicker}, valid: false},
		{name: "unknown capability", hello: protocol.Hello{Version: protocol.Version, Intent: protocol.IntentAttach, Size: domain.Size{Cols: 80, Rows: 24}, NavigationCapabilities: 4}, valid: false},
		{name: "back without picker", hello: protocol.Hello{Version: protocol.Version, Intent: protocol.IntentAttach, Size: domain.Size{Cols: 80, Rows: 24}, NavigationCapabilities: protocol.NavigationCapabilityBack}, valid: false},
		{name: "picker without back", hello: protocol.Hello{Version: protocol.Version, Intent: protocol.IntentAttach, Size: domain.Size{Cols: 80, Rows: 24}, StartupOverlay: protocol.StartupOverlaySessionPicker}, valid: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := protocol.ValidateHello(tt.hello)
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
