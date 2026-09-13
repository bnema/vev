package sessionwire

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
)

func testWireHello(t *testing.T) *wire.Hello {
	t.Helper()
	hello, err := helloToWire(protocol.Hello{Version: protocol.Version, Intent: protocol.IntentAttach, Size: domain.Size{Cols: 80, Rows: 24}})
	require.NoError(t, err)
	return hello
}

func testExactWireTarget() *wire.ExactTarget {
	lifecycle := domain.SessionLifecycleID{1}
	return &wire.ExactTarget{LifecycleId: &wire.LifecycleID{Value: lifecycle[:]}, SessionName: "work"}
}

// TestNarrowingConversionsRejectOverflow proves every wire uint32 -> smaller
// semantic conversion is validated before the cast: hostile high bits can no
// longer truncate into a valid enum or geometry range.
func TestNarrowingConversionsRejectOverflow(t *testing.T) {
	tests := []struct {
		name    string
		run     func(t *testing.T) error
		wantErr error
	}{
		{
			name: "hello environment policy",
			run: func(t *testing.T) error {
				hello := testWireHello(t)
				hello.EnvironmentPolicy = 256
				_, err := helloFromWire(hello)
				return err
			},
			wantErr: errProtoConvertRange,
		},
		{
			name: "hello navigation capabilities",
			run: func(t *testing.T) error {
				hello := testWireHello(t)
				hello.NavigationCapabilities = 256
				_, err := helloFromWire(hello)
				return err
			},
			wantErr: errProtoConvertRange,
		},
		{
			name: "hello output window",
			run: func(t *testing.T) error {
				hello := testWireHello(t)
				hello.MaxOutputInFlight = 256
				_, err := helloFromWire(hello)
				return err
			},
			wantErr: errProtoConvertRange,
		},
		{
			name: "hello geometry",
			run: func(t *testing.T) error {
				hello := testWireHello(t)
				hello.Cols = 70000
				_, err := helloFromWire(hello)
				return err
			},
			wantErr: errProtoConvertRange,
		},
		{
			name: "attach target environment policy",
			run: func(t *testing.T) error {
				_, err := attachTargetFromWire(&wire.AttachTarget{Session: "work", Intent: uint32(protocol.IntentAttach), EnvironmentPolicy: 256})
				return err
			},
			wantErr: protocol.ErrInvalidAttachTarget,
		},
		{
			name: "attach target intent",
			run: func(t *testing.T) error {
				_, err := attachTargetFromWire(&wire.AttachTarget{Session: "work", EnvironmentPolicy: uint32(protocol.EnvironmentPolicyClientOwned), Intent: 256})
				return err
			},
			wantErr: protocol.ErrInvalidAttachTarget,
		},
		{
			name:    "kill scope aliasing",
			run:     func(t *testing.T) error { _, err := killFromWire(&wire.Kill{Name: "work", Scope: 257}); return err },
			wantErr: errProtoConvertRange,
		},
		{
			name: "session state",
			run: func(t *testing.T) error {
				_, err := sessionsFromWire(&wire.Sessions{Sessions: []*wire.SessionInfo{{State: 256}}})
				return err
			},
			wantErr: errProtoConvertRange,
		},
		{
			name: "session tab count",
			run: func(t *testing.T) error {
				_, err := sessionsFromWire(&wire.Sessions{Sessions: []*wire.SessionInfo{{Tabs: 70000}}})
				return err
			},
			wantErr: errProtoConvertRange,
		},
		{
			name:    "ui receipt outcome aliasing",
			run:     func(t *testing.T) error { _, err := uiReceiptFromWire(&wire.UIReceipt{Outcome: 257}); return err },
			wantErr: errProtoConvertRange,
		},
		{
			name: "recent route kind",
			run: func(t *testing.T) error {
				_, err := recentRouteEntryFromWire(&wire.RecentRouteEntry{Target: testExactWireTarget(), Kind: 256})
				return err
			},
			wantErr: errProtoConvertRange,
		},
		{
			name: "recent route reachability",
			run: func(t *testing.T) error {
				_, err := recentRouteEntryFromWire(&wire.RecentRouteEntry{Target: testExactWireTarget(), Reachability: 257})
				return err
			},
			wantErr: errProtoConvertRange,
		},
		{
			name: "route failure code",
			run: func(t *testing.T) error {
				_, err := navigationFailureFromWire(&wire.RouteNavigationFailure{Code: 256})
				return err
			},
			wantErr: errProtoConvertRange,
		},
		{
			name: "same peer switch failure code",
			run: func(t *testing.T) error {
				_, err := samePeerSwitchFailureFromWire(&wire.SamePeerSwitchFailure{Code: 256})
				return err
			},
			wantErr: protocol.ErrInvalidRouteWire,
		},
		{
			name: "session creation failure code",
			run: func(t *testing.T) error {
				_, err := sessionCreationFailureFromWire(&wire.SessionCreationFailure{Code: 256})
				return err
			},
			wantErr: protocol.ErrInvalidRouteWire,
		},
		{
			name: "inventory request operation",
			run: func(t *testing.T) error {
				_, err := inventoryRequestFromWire(&wire.NavigationInventoryRequest{Operation: 257})
				return err
			},
			wantErr: protocol.ErrInvalidNavigation,
		},
		{
			name: "inventory response status",
			run: func(t *testing.T) error {
				_, err := inventoryResponseFromWire(&wire.NavigationInventoryResponse{Operation: 1, Status: 257})
				return err
			},
			wantErr: protocol.ErrInvalidNavigation,
		},
		{
			name: "inventory failure code",
			run: func(t *testing.T) error {
				_, err := inventoryFailureFromWire(&wire.NavigationInventoryFailure{Code: 256})
				return err
			},
			wantErr: protocol.ErrInvalidNavigation,
		},
		{
			name: "inventory source status",
			run: func(t *testing.T) error {
				_, err := inventoryGroupsFromWire([]*wire.InventorySourceGroup{{Status: 257}})
				return err
			},
			wantErr: errProtoConvertRange,
		},
		{
			name: "preview cell rune",
			run: func(t *testing.T) error {
				_, err := previewCellFromWire(&wire.PreviewCell{RuneValue: 0x110000})
				return err
			},
			wantErr: errProtoConvertRange,
		},
		{
			name:    "picker line kind",
			run:     func(t *testing.T) error { _, err := pickerLineFromWire(&wire.PickerLine{Kind: 256}); return err },
			wantErr: protocol.ErrInvalidNavigation,
		},
		{
			name: "picker selection action",
			run: func(t *testing.T) error {
				_, err := pickerSelectionFromWire(&wire.PickerSelection{Action: 257})
				return err
			},
			wantErr: errProtoConvertRange,
		},
		{
			name: "picker control request operation",
			run: func(t *testing.T) error {
				_, err := pickerControlRequestFromWire(&wire.PickerControlRequest{Operation: 257})
				return err
			},
			wantErr: protocol.ErrInvalidNavigation,
		},
		{
			name: "remote preview status",
			run: func(t *testing.T) error {
				_, err := remotePreviewFromWire(&wire.RemotePreview{Status: 257})
				return err
			},
			wantErr: protocol.ErrInvalidRemotePreview,
		},
		{
			name: "picker preview status",
			run: func(t *testing.T) error {
				_, err := pickerPreviewFromWire(&wire.PickerPreview{Version: uint32(protocol.PickerPreviewSchemaVersion), Width: 1, Height: 1, Status: 257})
				return err
			},
			wantErr: protocol.ErrInvalidPickerPreview,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.ErrorIs(t, tt.run(t), tt.wantErr)
		})
	}
}
