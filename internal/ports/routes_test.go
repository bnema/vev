package ports

import (
	"encoding/hex"
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
	require.Equal(t, "010203000000000000000000000000000004776f726b", hex.EncodeToString(encoded))
	assertAllPrefixesFail(t, encoded, UnmarshalExactSessionTarget)
	_, err = UnmarshalExactSessionTarget(append(append([]byte(nil), encoded...), 0))
	require.Error(t, err)

	got, err := UnmarshalExactSessionTarget(encoded)
	require.NoError(t, err)
	require.Equal(t, want, got)

	_, err = MarshalExactSessionTarget(ExactSessionTarget{SessionName: "work"})
	require.ErrorIs(t, err, ErrInvalidRouteWire)
}

func TestHelloExactTargetRoundTrip(t *testing.T) {
	target := testExactTarget()
	msg := Hello{
		Version:        ProtocolVersion,
		Intent:         IntentAttach,
		Name:           target.SessionName,
		Size:           domain.Size{Cols: 80, Rows: 24},
		ExactTarget:    &target,
		PreferredTabID: "tab-2",
	}

	encoded := MarshalHello(msg)
	require.NotEmpty(t, encoded)
	got, err := UnmarshalHello(encoded)
	require.NoError(t, err)
	require.Equal(t, msg, got)

	invalid := msg
	invalid.Intent = IntentNew
	require.Error(t, ValidateHello(invalid))

	invalid = msg
	invalid.PreferredTabID = "bad tab"
	require.Error(t, ValidateHello(invalid))
}

func TestRoutePositionCodecIsStrict(t *testing.T) {
	want := RoutePosition{Target: testExactTarget(), ActiveTabID: "tab-2"}
	encoded, err := MarshalRoutePosition(want)
	require.NoError(t, err)
	require.Equal(t, "010203000000000000000000000000000004776f726b00057461622d32", hex.EncodeToString(encoded))
	assertAllPrefixesFail(t, encoded, UnmarshalRoutePosition)
	_, err = UnmarshalRoutePosition(append(append([]byte(nil), encoded...), 0))
	require.Error(t, err)

	got, err := UnmarshalRoutePosition(encoded)
	require.NoError(t, err)
	require.Equal(t, want, got)

	_, err = MarshalRoutePosition(RoutePosition{Target: testExactTarget()})
	require.ErrorIs(t, err, ErrInvalidRouteWire)
	_, err = MarshalRoutePosition(RoutePosition{Target: testExactTarget(), ActiveTabID: "bad tab"})
	require.ErrorIs(t, err, ErrInvalidRouteWire)
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

	require.Nil(t, payload)
}

func TestCommittedRouteIdentityCodec(t *testing.T) {
	want := CommittedRouteIdentity{Target: testExactTarget(), Ephemeral: true}
	encoded, err := MarshalCommittedRouteIdentity(want)
	require.NoError(t, err)

	require.Equal(t, "010203000000000000000000000000000004776f726b01", hex.EncodeToString(encoded))
	assertAllPrefixesFail(t, encoded, UnmarshalCommittedRouteIdentity)
	_, err = UnmarshalCommittedRouteIdentity(append(append([]byte(nil), encoded...), 0))
	require.Error(t, err)

	got, err := UnmarshalCommittedRouteIdentity(encoded)
	require.NoError(t, err)
	require.Equal(t, want, got)

	mutated := append([]byte(nil), encoded...)
	mutated[len(mutated)-1] = 2
	_, err = UnmarshalCommittedRouteIdentity(mutated)
	require.Error(t, err)
}

func testRouteSnapshot() RecentRouteSnapshot {
	activeTarget := testExactTarget()
	logsTarget := ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{4, 5, 6}, SessionName: "logs"}
	return RecentRouteSnapshot{
		Generation:  9,
		Active:      RouteRef{Key: 11, Generation: 3},
		ActiveEntry: RecentRouteEntry{Key: 11, Generation: 3, Target: activeTarget, Name: "work", Kind: RouteKindLocal, Reachability: RouteReachabilityUnknown},
		Previous:    RouteRef{Key: 12, Generation: 4},
		Home:        RouteRef{Key: 11, Generation: 3},
		Entries: []RecentRouteEntry{
			{Key: 12, Generation: 4, Target: logsTarget, Name: "logs", HostLabel: "edge", Kind: RouteKindRemote, Attention: true, Reachability: RouteReachabilityUnknown},
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

	require.Equal(t, "0000000000000009000000000000000b0000000000000003000000000000000b0000000000000003010203000000000000000000000000000004776f726b0004776f726b000001000000000000000000000c0000000000000004000000000000000b000000000000000301000000000000000c00000000000000040405060000000000000000000000000000046c6f677300046c6f677300046564676502000100", hex.EncodeToString(encoded))
	assertAllPrefixesFail(t, encoded, UnmarshalRecentRouteSnapshot)
	_, err = UnmarshalRecentRouteSnapshot(append(append([]byte(nil), encoded...), 0))
	require.Error(t, err)

	invalidCases := []struct {
		name   string
		mutate func(*RecentRouteSnapshot)
	}{
		{name: "missing previous reference", mutate: func(snapshot *RecentRouteSnapshot) { snapshot.Previous = RouteRef{Key: 99, Generation: 99} }},
		{name: "missing active presentation", mutate: func(snapshot *RecentRouteSnapshot) { snapshot.ActiveEntry = RecentRouteEntry{} }},
		{name: "route name differs from lifecycle target", mutate: func(snapshot *RecentRouteSnapshot) { snapshot.Entries[0].Name = "other" }},
		{name: "entry is active", mutate: func(snapshot *RecentRouteSnapshot) {
			snapshot.Entries = append(snapshot.Entries, snapshot.ActiveEntry)
		}},
		{name: "duplicate entry", mutate: func(snapshot *RecentRouteSnapshot) { snapshot.Entries = append(snapshot.Entries, snapshot.Entries[0]) }},
		{name: "too many entries", mutate: func(snapshot *RecentRouteSnapshot) {
			snapshot.Entries = make([]RecentRouteEntry, RouteSnapshotMaxEntries+1)
			for i := range snapshot.Entries {
				target := ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{byte(i + 1)}, SessionName: "x"}
				snapshot.Entries[i] = RecentRouteEntry{Key: uint64(i + 1), Generation: uint64(i + 1), Target: target, Name: "x", Kind: RouteKindLocal}
			}
		}},
	}
	for _, tc := range invalidCases {
		t.Run(tc.name, func(t *testing.T) {
			invalid := want
			tc.mutate(&invalid)
			_, err := MarshalRecentRouteSnapshot(invalid)
			require.ErrorIs(t, err, ErrInvalidRouteWire)
		})
	}
}

func TestRouteAttentionSubscriptionCodecIsStrict(t *testing.T) {
	want := RouteAttentionSubscription{Targets: []RouteAttentionTarget{{
		Ref:    RouteRef{Key: 7, Generation: 3},
		Target: testExactTarget(),
	}}}
	encoded, err := MarshalRouteAttentionSubscription(want)
	require.NoError(t, err)
	require.Equal(t, "0100000000000000070000000000000003010203000000000000000000000000000004776f726b", hex.EncodeToString(encoded))
	assertAllPrefixesFail(t, encoded, UnmarshalRouteAttentionSubscription)
	_, err = UnmarshalRouteAttentionSubscription(append(append([]byte(nil), encoded...), 0))
	require.Error(t, err)

	got, err := UnmarshalRouteAttentionSubscription(encoded)
	require.NoError(t, err)
	require.Equal(t, want, got)

	_, err = MarshalRouteAttentionSubscription(RouteAttentionSubscription{Targets: []RouteAttentionTarget{
		want.Targets[0], want.Targets[0],
	}})
	require.ErrorIs(t, err, ErrInvalidRouteWire)
}

func TestRecentRouteSnapshotRejectsOversizedFrameBeforeParsing(t *testing.T) {
	_, err := UnmarshalRecentRouteSnapshot(make([]byte, MaxFrameLen))
	require.ErrorIs(t, err, ErrInvalidRouteWire)
}

func TestRouteLabelsRejectTerminalUnsafeText(t *testing.T) {
	values := []string{"line\nfeed", "\u2028line-separator", "\u2029paragraph-separator", "\x1b[31mred", "\u202eoverride", string([]byte{0xff, 0xfe})}
	for _, value := range values {
		t.Run("name/"+value, func(t *testing.T) {
			_, err := MarshalRecentRouteSnapshot(RecentRouteSnapshot{
				Generation: 1,
				Entries:    []RecentRouteEntry{{Key: 1, Generation: 1, Target: ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: value}, Name: value, Kind: RouteKindLocal}},
			})
			require.Error(t, err)
		})
		t.Run("host/"+value, func(t *testing.T) {
			_, err := MarshalRecentRouteSnapshot(RecentRouteSnapshot{
				Generation: 1,
				Entries:    []RecentRouteEntry{{Key: 1, Generation: 1, Target: testExactTarget(), Name: "work", HostLabel: value, Kind: RouteKindRemote}},
			})
			require.Error(t, err)
		})
	}
}

func TestRouteActionAndFailureCodecsAreBounded(t *testing.T) {
	action := RouteNavigationAction{SnapshotGeneration: 9, Key: 7, Generation: 8}
	encoded, err := MarshalRouteNavigationAction(action)
	require.NoError(t, err)
	require.Equal(t, "000000000000000900000000000000070000000000000008", hex.EncodeToString(encoded))
	assertAllPrefixesFail(t, encoded, UnmarshalRouteNavigationAction)
	_, err = UnmarshalRouteNavigationAction(append(append([]byte(nil), encoded...), 0))
	require.Error(t, err)
	got, err := UnmarshalRouteNavigationAction(encoded)
	require.NoError(t, err)
	require.Equal(t, action, got)

	failure := RouteNavigationFailure{Key: 7, Generation: 8, Code: RouteFailureStaleSelection}
	encoded, err = MarshalRouteNavigationFailure(failure)
	require.NoError(t, err)
	require.Equal(t, "0000000000000007000000000000000801", hex.EncodeToString(encoded))
	assertAllPrefixesFail(t, encoded, UnmarshalRouteNavigationFailure)
	_, err = UnmarshalRouteNavigationFailure(append(append([]byte(nil), encoded...), 0))
	require.Error(t, err)
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
