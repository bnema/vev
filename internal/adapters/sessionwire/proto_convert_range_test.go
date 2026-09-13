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
			name: "route failure decode identity",
			run: func(t *testing.T) error {
				_, err := navigationFailureFromWire(&wire.RouteNavigationFailure{Code: uint32(protocol.RouteFailureUnavailable)})
				return err
			},
			wantErr: protocol.ErrInvalidRouteWire,
		},
		{
			name: "route failure encode identity",
			run: func(t *testing.T) error {
				_, err := navigationFailureToWire(protocol.RouteNavigationFailure{Code: protocol.RouteFailureUnavailable})
				return err
			},
			wantErr: protocol.ErrInvalidRouteWire,
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
			name: "underline style narrowing",
			run: func(t *testing.T) error {
				_, err := cellStyleFromWire(&wire.CellStyle{UnderlineStyle: 256})
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

func testWireRouteEntry(key, generation uint64) *wire.RecentRouteEntry {
	return &wire.RecentRouteEntry{
		Key:          key,
		Generation:   generation,
		Target:       testExactWireTarget(),
		Name:         "work",
		Kind:         uint32(protocol.RouteKindLocal),
		Reachability: uint32(protocol.RouteReachabilityReachable),
	}
}

func testSemanticRouteEntry(key, generation uint64) protocol.RecentRouteEntry {
	lifecycle := domain.SessionLifecycleID{1}
	return protocol.RecentRouteEntry{
		Key:          key,
		Generation:   generation,
		Target:       protocol.ExactSessionTarget{LifecycleID: lifecycle, SessionName: "work"},
		Name:         "work",
		Kind:         protocol.RouteKindLocal,
		Reachability: protocol.RouteReachabilityReachable,
	}
}

// TestRouteSemanticBoundsRestored proves the old strict route codec bounds
// and invariants are restored on both conversion directions: encode refuses
// to emit a malformed value, decode refuses to accept one, and valid bounded
// values still round-trip unchanged.
func TestRouteSemanticBoundsRestored(t *testing.T) {
	entry := testSemanticRouteEntry(1, 1)
	ref := protocol.RouteRef{Key: 1, Generation: 1}
	validSnapshot := protocol.RecentRouteSnapshot{Generation: 1, Entries: []protocol.RecentRouteEntry{entry}}
	validSubscription := protocol.RouteAttentionSubscription{Targets: []protocol.RouteAttentionTarget{{Ref: ref, Target: entry.Target}}}
	validAction := protocol.RouteNavigationAction{SnapshotGeneration: 1, Key: 1, Generation: 1}

	oversized := validSnapshot
	for len(oversized.Entries) <= protocol.RouteSnapshotMaxEntries {
		next := uint64(len(oversized.Entries) + 1)
		oversized.Entries = append(oversized.Entries, testSemanticRouteEntry(next, next))
	}
	mismatched := protocol.RecentRouteSnapshot{
		Generation:  1,
		Active:      protocol.RouteRef{Key: 2, Generation: 2},
		ActiveEntry: entry,
	}
	duplicateSubscription := protocol.RouteAttentionSubscription{Targets: []protocol.RouteAttentionTarget{
		{Ref: ref, Target: entry.Target},
		{Ref: ref, Target: entry.Target},
	}}

	decodedEntries := make([]*wire.RecentRouteEntry, 0, protocol.RouteSnapshotMaxEntries+1)
	for i := 1; i <= protocol.RouteSnapshotMaxEntries+1; i++ {
		decodedEntries = append(decodedEntries, testWireRouteEntry(uint64(i), uint64(i)))
	}

	tests := []struct {
		name    string
		run     func(t *testing.T) error
		wantErr error
	}{
		{
			name: "encode oversized snapshot",
			run: func(t *testing.T) error {
				_, err := recentRouteSnapshotToWire(oversized)
				return err
			},
			wantErr: protocol.ErrInvalidRouteWire,
		},
		{
			name: "encode mismatched active presentation",
			run: func(t *testing.T) error {
				_, err := recentRouteSnapshotToWire(mismatched)
				return err
			},
			wantErr: protocol.ErrInvalidRouteWire,
		},
		{
			name: "encode snapshot missing home reference",
			run: func(t *testing.T) error {
				broken := validSnapshot
				broken.Home = protocol.RouteRef{Key: 9, Generation: 9}
				_, err := recentRouteSnapshotToWire(broken)
				return err
			},
			wantErr: protocol.ErrInvalidRouteWire,
		},
		{
			name: "encode duplicate attention reference",
			run: func(t *testing.T) error {
				_, err := attentionSubscriptionToWire(duplicateSubscription)
				return err
			},
			wantErr: protocol.ErrInvalidRouteWire,
		},
		{
			name: "encode zero navigation action",
			run: func(t *testing.T) error {
				_, err := routeNavigationActionToWire(protocol.RouteNavigationAction{})
				return err
			},
			wantErr: protocol.ErrInvalidRouteWire,
		},
		{
			name: "decode oversized snapshot",
			run: func(t *testing.T) error {
				_, err := recentRouteSnapshotFromWire(&wire.RecentRouteSnapshot{Generation: 1, Entries: decodedEntries})
				return err
			},
			wantErr: protocol.ErrInvalidRouteWire,
		},
		{
			name: "decode mismatched active presentation",
			run: func(t *testing.T) error {
				_, err := recentRouteSnapshotFromWire(&wire.RecentRouteSnapshot{
					Generation:  1,
					Active:      &wire.RouteRef{Key: 2, Generation: 2},
					ActiveEntry: testWireRouteEntry(1, 1),
				})
				return err
			},
			wantErr: protocol.ErrInvalidRouteWire,
		},
		{
			name: "decode duplicate attention reference",
			run: func(t *testing.T) error {
				_, err := attentionSubscriptionFromWire(&wire.RouteAttentionSubscription{Targets: []*wire.RouteAttentionTarget{
					{Ref: &wire.RouteRef{Key: 1, Generation: 1}, Target: testExactWireTarget()},
					{Ref: &wire.RouteRef{Key: 1, Generation: 1}, Target: testExactWireTarget()},
				}})
				return err
			},
			wantErr: protocol.ErrInvalidRouteWire,
		},
		{
			name: "decode zero navigation action",
			run: func(t *testing.T) error {
				_, err := routeNavigationActionFromWire(&wire.RouteNavigationAction{})
				return err
			},
			wantErr: protocol.ErrInvalidRouteWire,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.ErrorIs(t, tt.run(t), tt.wantErr)
		})
	}

	t.Run("valid snapshot round trip", func(t *testing.T) {
		converted, err := recentRouteSnapshotToWire(validSnapshot)
		require.NoError(t, err)
		decoded, err := recentRouteSnapshotFromWire(converted)
		require.NoError(t, err)
		require.Equal(t, validSnapshot, decoded)
	})
	t.Run("valid attention subscription round trip", func(t *testing.T) {
		converted, err := attentionSubscriptionToWire(validSubscription)
		require.NoError(t, err)
		decoded, err := attentionSubscriptionFromWire(converted)
		require.NoError(t, err)
		require.Equal(t, validSubscription, decoded)
	})
	t.Run("valid navigation action round trip", func(t *testing.T) {
		converted, err := routeNavigationActionToWire(validAction)
		require.NoError(t, err)
		decoded, err := routeNavigationActionFromWire(converted)
		require.NoError(t, err)
		require.Equal(t, validAction, decoded)
	})
}
