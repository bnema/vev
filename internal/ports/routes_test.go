package ports

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func testExactTarget() ExactSessionTarget {
	return ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1, 2, 3}, SessionName: "work"}
}

func TestExactSessionTargetCodecIsStrict(t *testing.T) {
	want := testExactTarget()
	encoded, err := MarshalExactSessionTarget(want)
	require.NoError(t, err)

	got, err := UnmarshalExactSessionTarget(encoded)
	require.NoError(t, err)
	require.Equal(t, want, got)

	for _, payload := range [][]byte{encoded[:len(encoded)-1], append(append([]byte(nil), encoded...), 0)} {
		_, err := UnmarshalExactSessionTarget(payload)
		require.Error(t, err)
	}

	_, err = MarshalExactSessionTarget(ExactSessionTarget{SessionName: "work"})
	require.ErrorIs(t, err, ErrInvalidRouteWire)
}

func TestHelloExactTargetRoundTrip(t *testing.T) {
	target := testExactTarget()
	msg := Hello{
		Version:     ProtocolVersion,
		Intent:      IntentAttach,
		Name:        target.SessionName,
		Size:        domain.Size{Cols: 80, Rows: 24},
		ExactTarget: &target,
	}

	encoded := MarshalHello(msg)
	require.NotEmpty(t, encoded)
	got, err := UnmarshalHello(encoded)
	require.NoError(t, err)
	require.Equal(t, msg, got)

	invalid := msg
	invalid.Intent = IntentNew
	require.Error(t, ValidateHello(invalid))
}

func TestWelcomeCarriesCommittedRouteIdentity(t *testing.T) {
	identity := &CommittedRouteIdentity{Target: testExactTarget(), Ephemeral: true}
	want := Welcome{SessionID: "daemon-session", SessionName: "work", Ephemeral: true, CommittedIdentity: identity}

	got, err := UnmarshalWelcome(MarshalWelcome(want))
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestWelcomeRejectsMismatchedCommittedIdentity(t *testing.T) {
	identity := &CommittedRouteIdentity{Target: testExactTarget()}
	payload := MarshalWelcome(Welcome{SessionID: "daemon-session", SessionName: "other", CommittedIdentity: identity})

	_, err := UnmarshalWelcome(payload)
	require.ErrorIs(t, err, ErrInvalidRouteWire)
}

func TestCommittedRouteIdentityCodec(t *testing.T) {
	want := CommittedRouteIdentity{Target: testExactTarget(), Ephemeral: true}
	encoded, err := MarshalCommittedRouteIdentity(want)
	require.NoError(t, err)

	got, err := UnmarshalCommittedRouteIdentity(encoded)
	require.NoError(t, err)
	require.Equal(t, want, got)

	mutated := append([]byte(nil), encoded...)
	mutated[len(mutated)-1] = 2
	_, err = UnmarshalCommittedRouteIdentity(mutated)
	require.Error(t, err)
}

func testRouteSnapshot() RecentRouteSnapshot {
	return RecentRouteSnapshot{
		Generation: 9,
		Active:     RouteRef{Key: 11, Generation: 3},
		Previous:   RouteRef{Key: 12, Generation: 4},
		Home:       RouteRef{Key: 11, Generation: 3},
		Entries: []RecentRouteEntry{
			{Key: 12, Generation: 4, Name: "logs", HostLabel: "edge", Kind: RouteKindRemote, Attention: true, Reachability: RouteReachabilityUnknown},
		},
	}
}

func TestRecentRouteSnapshotCodecRoundTripAndBounds(t *testing.T) {
	want := testRouteSnapshot()
	encoded, err := MarshalRecentRouteSnapshot(want)
	require.NoError(t, err)

	got, err := UnmarshalRecentRouteSnapshot(encoded)
	require.NoError(t, err)
	require.Equal(t, want, got)

	for _, payload := range [][]byte{encoded[:len(encoded)-1], append(append([]byte(nil), encoded...), 0)} {
		_, err := UnmarshalRecentRouteSnapshot(payload)
		require.Error(t, err)
	}

	invalid := want
	invalid.Previous = RouteRef{Key: 99, Generation: 99}
	_, err = MarshalRecentRouteSnapshot(invalid)
	require.ErrorIs(t, err, ErrInvalidRouteWire)

	invalid = want
	invalid.Entries = append(invalid.Entries, RecentRouteEntry{Key: 11, Generation: 3, Name: "work", Kind: RouteKindLocal})
	_, err = MarshalRecentRouteSnapshot(invalid)
	require.ErrorIs(t, err, ErrInvalidRouteWire)

	invalid = want
	invalid.Entries = append(invalid.Entries, invalid.Entries[0])
	_, err = MarshalRecentRouteSnapshot(invalid)
	require.ErrorIs(t, err, ErrInvalidRouteWire)

	invalid = want
	invalid.Entries = make([]RecentRouteEntry, RouteSnapshotMaxEntries+1)
	for i := range invalid.Entries {
		invalid.Entries[i] = RecentRouteEntry{Key: uint64(i + 1), Generation: uint64(i + 1), Name: "x", Kind: RouteKindLocal}
	}
	_, err = MarshalRecentRouteSnapshot(invalid)
	require.ErrorIs(t, err, ErrInvalidRouteWire)
}

func TestRouteLabelsRejectTerminalUnsafeText(t *testing.T) {
	for _, value := range []string{"line\nfeed", "\x1b[31mred", string([]byte{0xff, 0xfe})} {
		_, err := MarshalRecentRouteSnapshot(RecentRouteSnapshot{
			Generation: 1,
			Entries:    []RecentRouteEntry{{Key: 1, Generation: 1, Name: value, Kind: RouteKindLocal}},
		})
		require.Error(t, err)
	}
}

func TestRouteActionAndFailureCodecsAreBounded(t *testing.T) {
	action := RouteNavigationAction{SnapshotGeneration: 9, Key: 7, Generation: 8}
	encoded, err := MarshalRouteNavigationAction(action)
	require.NoError(t, err)
	got, err := UnmarshalRouteNavigationAction(encoded)
	require.NoError(t, err)
	require.Equal(t, action, got)

	failure := RouteNavigationFailure{Key: 7, Generation: 8, Code: RouteFailureStaleSelection}
	encoded, err = MarshalRouteNavigationFailure(failure)
	require.NoError(t, err)
	gotFailure, err := UnmarshalRouteNavigationFailure(encoded)
	require.NoError(t, err)
	require.Equal(t, failure, gotFailure)

	for _, invalid := range []RouteNavigationAction{{}, {SnapshotGeneration: 1, Key: 1}, {SnapshotGeneration: 1, Generation: 1}, {Key: 1, Generation: 1}} {
		_, err := MarshalRouteNavigationAction(invalid)
		require.Error(t, err)
	}
	for _, invalid := range []RouteNavigationFailure{{}, {Key: 1, Generation: 1}, {Key: 1, Generation: 1, Code: 255}} {
		_, err := MarshalRouteNavigationFailure(invalid)
		require.Error(t, err)
	}
}
