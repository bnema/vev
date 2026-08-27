package ports

import (
	"encoding/hex"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestNavigationDirectiveWireTable(t *testing.T) {
	lease := ParkedRouteLeaseID{1, 2, 3}
	tests := []struct {
		name      string
		directive NavigationDirective
		valid     bool
	}{
		{name: "open home", directive: NavigationDirective{Action: NavigationOpenHomePicker, LeaseID: lease}, valid: true},
		{name: "back", directive: NavigationDirective{Action: NavigationBack}, valid: true},
		{name: "open home without lease", directive: NavigationDirective{Action: NavigationOpenHomePicker}},
		{name: "back with lease", directive: NavigationDirective{Action: NavigationBack, LeaseID: lease}},
		{name: "zero", directive: NavigationDirective{}},
		{name: "unknown", directive: NavigationDirective{Action: 3}},
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
			require.ErrorIs(t, err, ErrInvalidNavigation)
		})
	}
}

func TestParkedRouteWireRoundTripsStrictly(t *testing.T) {
	lease := ParkedRouteLeaseID{1, 2, 3}
	target := &domain.RemoteSessionTarget{
		Endpoint: "remote", DisplayOrigin: "remote", LifecycleID: domain.SessionLifecycleID{4},
		SessionName: "work", LiveTabID: "tab-1",
	}
	requests := []struct {
		name   string
		value  ParkedRouteRequest
		golden string
	}{
		{name: "prepare", value: ParkedRouteRequest{RequestID: 1, LeaseID: lease, Action: ParkedRoutePrepare}, golden: "0000000000000001" + "01020300000000000000000000000000" + "010001"},
		{name: "resume", value: ParkedRouteRequest{RequestID: 2, LeaseID: lease, Action: ParkedRouteResume}},
		{name: "switch", value: ParkedRouteRequest{RequestID: 3, LeaseID: lease, Action: ParkedRouteSwitch, Target: target}},
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
		value  ParkedRouteResponse
		golden string
	}{
		{name: "ready", value: ParkedRouteResponse{RequestID: 1, Status: ParkedRouteReady}, golden: "000000000000000101"},
		{name: "resumed", value: ParkedRouteResponse{RequestID: 2, Status: ParkedRouteResumed}},
		{name: "switched", value: ParkedRouteResponse{RequestID: 3, Status: ParkedRouteSwitched}},
		{name: "rejected", value: ParkedRouteResponse{RequestID: 4, Status: ParkedRouteRejected}},
		{name: "expired", value: ParkedRouteResponse{RequestID: 5, Status: ParkedRouteExpired}},
		{name: "stale target", value: ParkedRouteResponse{RequestID: 6, Status: ParkedRouteStaleTarget}},
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
		value ParkedRouteRequest
	}{
		{name: "zero request ID", value: ParkedRouteRequest{LeaseID: lease, Action: ParkedRoutePrepare}},
		{name: "zero lease", value: ParkedRouteRequest{RequestID: 1, Action: ParkedRoutePrepare}},
		{name: "unknown action", value: ParkedRouteRequest{RequestID: 1, LeaseID: lease, Action: 99}},
		{name: "prepare target", value: ParkedRouteRequest{RequestID: 1, LeaseID: lease, Action: ParkedRoutePrepare, Target: target}},
		{name: "resume target", value: ParkedRouteRequest{RequestID: 1, LeaseID: lease, Action: ParkedRouteResume, Target: target}},
		{name: "missing switch target", value: ParkedRouteRequest{RequestID: 1, LeaseID: lease, Action: ParkedRouteSwitch}},
		{name: "invalid switch target", value: ParkedRouteRequest{RequestID: 1, LeaseID: lease, Action: ParkedRouteSwitch, Target: &domain.RemoteSessionTarget{}}},
	}
	for _, tt := range invalidRequests {
		t.Run("invalid request "+tt.name, func(t *testing.T) {
			require.Nil(t, MarshalParkedRouteRequest(tt.value))
		})
	}
	for _, tt := range []struct {
		name  string
		value ParkedRouteResponse
	}{
		{name: "zero request ID", value: ParkedRouteResponse{Status: ParkedRouteReady}},
		{name: "zero status", value: ParkedRouteResponse{RequestID: 1}},
		{name: "unknown status", value: ParkedRouteResponse{RequestID: 1, Status: 99}},
	} {
		t.Run("invalid response "+tt.name, func(t *testing.T) {
			require.Nil(t, MarshalParkedRouteResponse(tt.value))
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
		{name: "back with startup picker", hello: Hello{Version: ProtocolVersion, Intent: IntentAttach, Size: domain.Size{Cols: 80, Rows: 24}, NavigationCapabilities: NavigationCapabilityBack, StartupOverlay: StartupOverlaySessionPicker}, valid: true, capability: NavigationCapabilityBack, overlay: StartupOverlaySessionPicker, wantPayload: "002502" + "00000000000000000000000000000000" + "0000000000000000" + "0000" + "00500018" + "00000000" + "0000" + "0000" + "00" + "00" + "00000000" + "00" + "00" + "00" + "0000" + "02010000"},
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
