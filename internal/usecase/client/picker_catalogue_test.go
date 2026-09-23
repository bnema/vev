package client

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

// Client-owned picker catalogue tests (Plan 001 P5.2b). Every case is
// deterministic: the fake clock, not wall time, decides freshness, and no case
// sleeps.

const pickerTestFreshness = 30 * time.Second

// pickerCatalogueErrorIs reports whether err is a typed catalogue refusal with
// the supplied code, so tests classify a resolution failure without matching
// message text.
func pickerCatalogueErrorIs(err error, code pickerCatalogueErrorCode) bool {
	var typed pickerCatalogueError
	return errors.As(err, &typed) && typed.Code == code
}

func pickerTestPolicy() ports.BrokerPolicy {
	return ports.BrokerPolicy{
		ProtocolVersion:      protocol.Version,
		CatalogSchemaVersion: catalogue.RemoteCatalogSchemaVersion,
		EnvironmentPolicy:    protocol.EnvironmentPolicyDaemonOwned,
		Transport:            "quic",
		Trust:                "trust",
		Launch:               "launch",
		Isolation:            "user",
	}
}

func pickerTestIncarnation(seed byte) [16]byte {
	var id [16]byte
	id[0] = seed
	return id
}

func pickerTestRegistration(endpoint string, seed byte) domain.RemoteRegistration {
	return domain.RemoteRegistration{Endpoint: endpoint, Incarnation: pickerTestIncarnation(seed), Generation: 1}
}

func pickerTestLifecycle(seed byte) domain.SessionLifecycleID { return pickerTestIncarnation(seed) }

func pickerTestSession(name string, seed byte, state catalogue.RemoteCatalogSessionState) catalogue.RemoteCatalogSession {
	return catalogue.RemoteCatalogSession{LifecycleID: pickerTestLifecycle(seed), Name: name, State: state}
}

func pickerTestSessionWithLifecycle(name string, lifecycle domain.SessionLifecycleID) catalogue.RemoteCatalogSession {
	return catalogue.RemoteCatalogSession{LifecycleID: lifecycle, Name: name, State: catalogue_Up}
}

func pickerTestLocalObservation(now time.Time, sessions ...catalogue.RemoteCatalogSession) ports.BrokerDaemonObservation {
	return ports.BrokerDaemonObservation{
		Local:           true,
		DisplayOrigin:   "local",
		Policy:          pickerTestPolicy(),
		Identity:        "local-daemon",
		Incarnation:     ports.BrokerDaemonIncarnation(pickerTestIncarnation(9)),
		ProtocolVersion: protocol.Version,
		Availability:    domain.RemoteAvailabilityReachable,
		LastSuccess:     now,
		InventoryKnown:  true,
		Sessions:        sessions,
	}
}

func pickerTestRemoteObservation(endpoint string, registrationSeed, incarnationSeed byte, now time.Time, sessions ...catalogue.RemoteCatalogSession) ports.BrokerDaemonObservation {
	return ports.BrokerDaemonObservation{
		Endpoint:        endpoint,
		DisplayOrigin:   endpoint,
		Registration:    pickerTestRegistration(endpoint, registrationSeed),
		Policy:          pickerTestPolicy(),
		Identity:        ports.BrokerDaemonIdentity("identity-" + endpoint),
		Incarnation:     ports.BrokerDaemonIncarnation(pickerTestIncarnation(incarnationSeed)),
		ProtocolVersion: protocol.Version,
		Availability:    domain.RemoteAvailabilityReachable,
		LastSuccess:     now,
		InventoryKnown:  true,
		Sessions:        sessions,
	}
}

func pickerTestUnobservedObservation(now time.Time, sessions ...catalogue.RemoteCatalogSession) ports.BrokerDaemonObservation {
	return ports.BrokerDaemonObservation{
		Local:          true,
		DisplayOrigin:  "local",
		Policy:         pickerTestPolicy(),
		Availability:   domain.RemoteAvailabilityUnknown,
		InventoryKnown: len(sessions) > 0,
		Sessions:       sessions,
	}
}

func advancePickerClock(clock *supervisorTestClock, delta time.Duration) {
	clock.mu.Lock()
	clock.now = clock.now.Add(delta)
	clock.mu.Unlock()
}

func pickerTestCatalogue(t *testing.T) (*pickerCatalogue, *supervisorTestClock) {
	t.Helper()
	clock := newSupervisorTestClock()
	return newPickerCatalogue(pickerCatalogueConfig{Clock: clock, Freshness: pickerTestFreshness}), clock
}

func pickerLineByLabel(lines []protocol.PickerLine, label string) (protocol.PickerLine, bool) {
	for _, line := range lines {
		if line.Kind != protocol.PickerLineSection && line.Label == label {
			return line, true
		}
	}
	return protocol.PickerLine{}, false
}

func pickerHostLine(lines []protocol.PickerLine) (protocol.PickerLine, bool) {
	for _, line := range lines {
		if line.Kind == protocol.PickerLineHost {
			return line, true
		}
	}
	return protocol.PickerLine{}, false
}

func pickerSectionLabels(lines []protocol.PickerLine) []string {
	sections := make([]string, 0)
	for _, line := range lines {
		if line.Kind == protocol.PickerLineSection {
			sections = append(sections, line.Label)
		}
	}
	return sections
}

func pickerSessionLabels(lines []protocol.PickerLine) []string {
	labels := make([]string, 0)
	for _, line := range lines {
		if line.Kind == protocol.PickerLineSession {
			labels = append(labels, line.Label)
		}
	}
	return labels
}

func pickerTestBase() pickerResolveBase {
	return pickerResolveBase{Connection: ports.BrokerConnectionID{1}, Stream: ports.BrokerStreamID(1)}
}

// localDaemonSnapshot publishes one fresh, reachable, compatible local daemon
// observation with no sessions, which is what the supervisor's readiness gate
// and its initial navigation both need.
func localDaemonSnapshot(epoch ports.BrokerEpoch, revision ports.BrokerRevision) ports.BrokerSnapshot {
	return ports.BrokerSnapshot{Epoch: epoch, Revision: revision, Daemons: []ports.BrokerDaemonObservation{
		pickerTestLocalObservation(time.Unix(1000, 0)),
	}}
}

func TestPickerCatalogueEmptySnapshot(t *testing.T) {
	catalogue, _ := pickerTestCatalogue(t)
	require.False(t, catalogue.Apply(ports.BrokerSnapshot{Epoch: 0, Revision: 1}), "an invalid snapshot is never applied")
	require.True(t, catalogue.Apply(ports.BrokerSnapshot{Epoch: 5, Revision: 1}))
	require.Empty(t, catalogue.Lines())
	require.Equal(t, ports.BrokerEpoch(5), catalogue.Epoch())
	require.Equal(t, ports.BrokerRevision(1), catalogue.Revision())
}

func TestPickerCatalogueProjectionCases(t *testing.T) {
	clock := newSupervisorTestClock()
	now := clock.Now()
	navigateKill := protocol.PickerCanNavigate | protocol.PickerCanKill

	tests := []struct {
		name         string
		daemons      []ports.BrokerDaemonObservation
		wantSections []string
		wantSessions []string
		wantActions  map[string]protocol.PickerLineActions
		wantStatus   map[string]protocol.PickerLineStatus
		wantDim      map[string]bool
		wantHostRow  bool
	}{
		{
			name:         "local only",
			daemons:      []ports.BrokerDaemonObservation{pickerTestLocalObservation(now, pickerTestSession("alpha", 1, catalogue_Up), pickerTestSession("beta", 2, catalogue_Up))},
			wantSections: []string{"local"},
			wantSessions: []string{"alpha", "beta"},
			wantActions:  map[string]protocol.PickerLineActions{"alpha": navigateKill, "beta": navigateKill},
		},
		{
			name:         "unobserved local daemon with no session shows its status",
			daemons:      []ports.BrokerDaemonObservation{pickerTestUnobservedObservation(now)},
			wantSections: []string{"local"},
			wantSessions: []string{},
			wantStatus:   map[string]protocol.PickerLineStatus{"local": protocol.PickerLineStatusStale},
			wantHostRow:  true,
		},
		{
			name:         "no local daemon entry",
			daemons:      []ports.BrokerDaemonObservation{pickerTestRemoteObservation("user@arch", 1, 1, now, pickerTestSession("remote-a", 3, catalogue_Up))},
			wantSections: []string{"user@arch"},
			wantSessions: []string{"remote-a"},
			wantActions:  map[string]protocol.PickerLineActions{"remote-a": protocol.PickerCanNavigate},
		},
		{
			name: "stale observation stays explicitly selectable",
			daemons: []ports.BrokerDaemonObservation{
				func() ports.BrokerDaemonObservation {
					observation := pickerTestLocalObservation(now, pickerTestSession("alpha", 1, catalogue_Up))
					observation.LastSuccess = now.Add(-2 * pickerTestFreshness)
					return observation
				}(),
			},
			wantSections: []string{"local"},
			wantSessions: []string{"alpha"},
			wantActions:  map[string]protocol.PickerLineActions{"alpha": navigateKill},
			wantStatus:   map[string]protocol.PickerLineStatus{"alpha": protocol.PickerLineStatusUp},
			wantDim:      map[string]bool{"alpha": true},
		},
		{
			name: "unavailable host keeps its rows and hides the redundant status row",
			daemons: []ports.BrokerDaemonObservation{
				func() ports.BrokerDaemonObservation {
					observation := pickerTestRemoteObservation("user@arch", 1, 1, now, pickerTestSession("remote-a", 3, catalogue_Up))
					observation.Availability = domain.RemoteAvailabilityUnreachable
					return observation
				}(),
			},
			wantSections: []string{"user@arch"},
			wantSessions: []string{"remote-a"},
			wantActions:  map[string]protocol.PickerLineActions{"remote-a": protocol.PickerCanNavigate},
			wantStatus:   map[string]protocol.PickerLineStatus{"remote-a": protocol.PickerLineStatusUp},
			wantDim:      map[string]bool{"remote-a": true},
		},
		{
			name: "unavailable host without sessions shows its status row",
			daemons: []ports.BrokerDaemonObservation{
				func() ports.BrokerDaemonObservation {
					observation := pickerTestRemoteObservation("user@arch", 1, 1, now)
					observation.Availability = domain.RemoteAvailabilityUnreachable
					return observation
				}(),
			},
			wantSections: []string{"user@arch"},
			wantSessions: []string{},
			wantStatus:   map[string]protocol.PickerLineStatus{"user@arch": protocol.PickerLineStatusDown},
			wantHostRow:  true,
		},
		{
			name: "incompatible host",
			daemons: []ports.BrokerDaemonObservation{
				func() ports.BrokerDaemonObservation {
					observation := pickerTestRemoteObservation("user@arch", 1, 1, now, pickerTestSession("remote-a", 3, catalogue_Up))
					observation.ProtocolVersion = protocol.Version + 1
					return observation
				}(),
			},
			wantSections: []string{"user@arch"},
			wantSessions: []string{"remote-a"},
			wantActions:  map[string]protocol.PickerLineActions{"remote-a": 0},
			wantStatus:   map[string]protocol.PickerLineStatus{"remote-a": protocol.PickerLineStatusUp},
			wantDim:      map[string]bool{"remote-a": true},
		},
		{
			name:         "broken session is only killable",
			daemons:      []ports.BrokerDaemonObservation{pickerTestLocalObservation(now, pickerTestSession("alpha", 1, catalogue_Broken))},
			wantSections: []string{"local"},
			wantSessions: []string{"alpha"},
			wantActions:  map[string]protocol.PickerLineActions{"alpha": protocol.PickerCanKill},
			wantStatus:   map[string]protocol.PickerLineStatus{"alpha": protocol.PickerLineStatusError},
			wantDim:      map[string]bool{"alpha": true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			catalogue, _ := pickerTestCatalogue(t)
			require.True(t, catalogue.Apply(ports.BrokerSnapshot{Epoch: 7, Revision: 1, Daemons: tt.daemons}))
			lines := catalogue.Lines()
			require.Equal(t, tt.wantSections, pickerSectionLabels(lines))
			require.Equal(t, tt.wantSessions, pickerSessionLabels(lines))
			_, hasHost := pickerHostLine(lines)
			require.Equal(t, tt.wantHostRow, hasHost, "a host status row is published only when it adds information")
			for label, actions := range tt.wantActions {
				line, ok := pickerLineByLabel(lines, label)
				require.True(t, ok, "session %q must be projected", label)
				require.True(t, line.Focusable, "a projected row is always a cursor destination")
				require.Equal(t, actions, line.Actions, "row %q actions", label)
			}
			for label, status := range tt.wantStatus {
				line, ok := pickerLineByLabel(lines, label)
				require.True(t, ok, "row %q must be projected", label)
				require.Equal(t, status, line.Status, "row %q badge", label)
			}
			for label, dim := range tt.wantDim {
				line, ok := pickerLineByLabel(lines, label)
				require.True(t, ok)
				require.Equal(t, dim, line.Dim, "row %q dim", label)
			}
		})
	}
}

func TestPickerCatalogueMonotonicUpdates(t *testing.T) {
	clock := newSupervisorTestClock()
	now := clock.Now()
	catalogue, _ := pickerTestCatalogue(t)

	newer := ports.BrokerSnapshot{Epoch: 3, Revision: 4, Daemons: []ports.BrokerDaemonObservation{
		pickerTestLocalObservation(now, pickerTestSession("alpha", 1, catalogue_Up)),
	}}
	require.True(t, catalogue.Apply(newer))
	before := catalogue.Lines()

	// A lower revision in the same epoch never supersedes and never regresses.
	older := ports.BrokerSnapshot{Epoch: 3, Revision: 2, Daemons: []ports.BrokerDaemonObservation{
		pickerTestLocalObservation(now, pickerTestSession("beta", 2, catalogue_Up)),
	}}
	require.False(t, catalogue.Apply(older))
	require.Equal(t, before, catalogue.Lines())
	require.Equal(t, ports.BrokerRevision(4), catalogue.Revision())

	// A new epoch is a full resynchronization and replaces every row.
	epoch := ports.BrokerSnapshot{Epoch: 4, Revision: 1, Daemons: []ports.BrokerDaemonObservation{
		pickerTestLocalObservation(now, pickerTestSession("gamma", 3, catalogue_Up)),
	}}
	require.True(t, catalogue.Apply(epoch))
	require.Equal(t, []string{"gamma"}, pickerSessionLabels(catalogue.Lines()))
}

// catalogue_Up/Broken keep the tables above short without importing the
// catalogue state into every literal.
const (
	catalogue_Up     = catalogue.RemoteCatalogSessionState("up")
	catalogue_Down   = catalogue.RemoteCatalogSessionState("down")
	catalogue_Broken = catalogue.RemoteCatalogSessionState("broken")
)

func TestPickerCatalogueSlowObservationKeepsNewerState(t *testing.T) {
	clock := newSupervisorTestClock()
	now := clock.Now()
	catalogue, _ := pickerTestCatalogue(t)

	fresh := pickerTestRemoteObservation("user@arch", 1, 1, now, pickerTestSession("alpha", 1, catalogue_Up))
	require.True(t, catalogue.Apply(ports.BrokerSnapshot{Epoch: 3, Revision: 2, Daemons: []ports.BrokerDaemonObservation{fresh}}))

	regressed := pickerTestRemoteObservation("user@arch", 1, 1, now.Add(-time.Minute), pickerTestSession("beta", 2, catalogue_Up))
	require.True(t, catalogue.Apply(ports.BrokerSnapshot{Epoch: 3, Revision: 3, Daemons: []ports.BrokerDaemonObservation{regressed}}), "the publication supersedes")

	require.Equal(t, []string{"alpha"}, pickerSessionLabels(catalogue.Lines()), "an older observed inventory never regresses a row")
}

func TestPickerCatalogueIdentityReplacement(t *testing.T) {
	clock := newSupervisorTestClock()
	now := clock.Now()
	catalogue, _ := pickerTestCatalogue(t)

	original := ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{
		pickerTestRemoteObservation("user@arch", 1, 1, now, pickerTestSession("alpha", 1, catalogue_Up)),
	}}
	require.True(t, catalogue.Apply(original))
	oldLine, ok := pickerLineByLabel(catalogue.Lines(), "alpha")
	require.True(t, ok)
	oldKey := oldLine.Key

	replaced := ports.BrokerSnapshot{Epoch: 3, Revision: 2, Daemons: []ports.BrokerDaemonObservation{
		pickerTestRemoteObservation("user@arch", 2, 2, now, pickerTestSession("alpha", 9, catalogue_Up)),
	}}
	require.True(t, catalogue.Apply(replaced))
	newLine, ok := pickerLineByLabel(catalogue.Lines(), "alpha")
	require.True(t, ok)
	require.NotEqual(t, oldKey, newLine.Key, "a replaced identity gets a new opaque key")

	// The old key is still recognized and refused as a replacement, not silently
	// re-bound to the new session.
	_, err := catalogue.Resolve(oldKey, pickerTestBase())
	require.Error(t, err)
	require.True(t, pickerCatalogueErrorIs(err, pickerCatalogueReplaced), "same endpoint with a new incarnation is a replacement")
}

// TestPickerCatalogueRegistrationGenerationChangeRequiresFreshSelection pins the
// complete-registration equality fence on an interactive selection: a
// registration whose generation advanced under the same endpoint and
// incarnation is a replacement, so the displayed row cannot silently re-bind
// to changed authority.
func TestPickerCatalogueRegistrationGenerationChangeRequiresFreshSelection(t *testing.T) {
	clock := newSupervisorTestClock()
	now := clock.Now()
	catalogue, _ := pickerTestCatalogue(t)

	require.True(t, catalogue.Apply(ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{
		pickerTestRemoteObservation("user@arch", 1, 1, now, pickerTestSession("alpha", 1, catalogue_Up)),
	}}))
	line, ok := pickerLineByLabel(catalogue.Lines(), "alpha")
	require.True(t, ok)

	advanced := pickerTestRemoteObservation("user@arch", 1, 1, now, pickerTestSession("alpha", 1, catalogue_Up))
	advanced.Registration.Generation = 2
	require.True(t, catalogue.Apply(ports.BrokerSnapshot{Epoch: 3, Revision: 2, Daemons: []ports.BrokerDaemonObservation{advanced}}))

	_, err := catalogue.Resolve(line.Key, pickerTestBase())
	require.Error(t, err)
	require.True(t, pickerCatalogueErrorIs(err, pickerCatalogueReplaced), "an advanced generation is a replacement: %v", err)
}

func TestPickerCatalogueStableRowOrder(t *testing.T) {
	clock := newSupervisorTestClock()
	now := clock.Now()
	catalogue, _ := pickerTestCatalogue(t)

	first := ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{
		pickerTestLocalObservation(now, pickerTestSession("alpha", 1, catalogue_Up)),
		pickerTestRemoteObservation("host-a", 1, 1, now, pickerTestSession("a-one", 2, catalogue_Up)),
		pickerTestRemoteObservation("host-b", 2, 2, now, pickerTestSession("b-one", 3, catalogue_Up)),
	}}
	require.True(t, catalogue.Apply(first))
	require.Equal(t, []string{"local", "host-a", "host-b"}, pickerSectionLabels(catalogue.Lines()))

	// The publication reorders the remote hosts and changes one unrelated
	// session; the first-seen order never reshuffles. The local daemon stays
	// first, as every valid broker publication requires.
	second := ports.BrokerSnapshot{Epoch: 3, Revision: 2, Daemons: []ports.BrokerDaemonObservation{
		pickerTestLocalObservation(now, pickerTestSession("alpha", 1, catalogue_Up)),
		pickerTestRemoteObservation("host-b", 2, 2, now, pickerTestSession("b-one", 3, catalogue_Up), pickerTestSession("b-two", 4, catalogue_Up)),
		pickerTestRemoteObservation("host-a", 1, 1, now, pickerTestSession("a-one", 2, catalogue_Up)),
	}}
	require.True(t, catalogue.Apply(second))
	require.Equal(t, []string{"local", "host-a", "host-b"}, pickerSectionLabels(catalogue.Lines()))
	require.Equal(t, []string{"alpha", "a-one", "b-one", "b-two"}, pickerSessionLabels(catalogue.Lines()))
}

func TestPickerCatalogueDeterministicKeys(t *testing.T) {
	clock := newSupervisorTestClock()
	now := clock.Now()
	daemons := []ports.BrokerDaemonObservation{
		pickerTestLocalObservation(now, pickerTestSession("alpha", 1, catalogue_Up)),
		pickerTestRemoteObservation("host-a", 1, 1, now, pickerTestSession("a-one", 2, catalogue_Up)),
	}
	first, _ := pickerTestCatalogue(t)
	second, _ := pickerTestCatalogue(t)
	require.True(t, first.Apply(ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: daemons}))
	require.True(t, second.Apply(ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: daemons}))
	require.Equal(t, first.Lines(), second.Lines())
}

func TestPickerCatalogueSelectionResolution(t *testing.T) {
	clock := newSupervisorTestClock()
	now := clock.Now()
	base := pickerTestBase()

	tests := []struct {
		name          string
		build         func(t *testing.T, catalogue *pickerCatalogue, clock *supervisorTestClock) string
		wantErr       pickerCatalogueErrorCode
		wantLocal     bool
		wantAdmission ports.BrokerStreamAdmission
	}{
		{
			name: "local exact attaches to the lifecycle",
			build: func(t *testing.T, catalogue *pickerCatalogue, _ *supervisorTestClock) string {
				require.True(t, catalogue.Apply(ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{
					pickerTestLocalObservation(now, pickerTestSession("alpha", 1, catalogue_Up)),
				}}))
				line, ok := pickerLineByLabel(catalogue.Lines(), "alpha")
				require.True(t, ok)
				return line.Key
			},
			wantLocal:     true,
			wantAdmission: ports.BrokerAdmissionExact,
		},
		{
			name: "remote exact carries the configured authority",
			build: func(t *testing.T, catalogue *pickerCatalogue, _ *supervisorTestClock) string {
				require.True(t, catalogue.Apply(ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{
					pickerTestRemoteObservation("user@arch", 1, 1, now, pickerTestSession("remote-a", 2, catalogue_Up)),
				}}))
				line, ok := pickerLineByLabel(catalogue.Lines(), "remote-a")
				require.True(t, ok)
				return line.Key
			},
			wantAdmission: ports.BrokerAdmissionExact,
		},
		{
			name: "gone host",
			build: func(t *testing.T, catalogue *pickerCatalogue, _ *supervisorTestClock) string {
				require.True(t, catalogue.Apply(ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{
					pickerTestRemoteObservation("user@arch", 1, 1, now, pickerTestSession("remote-a", 2, catalogue_Up)),
				}}))
				line, ok := pickerLineByLabel(catalogue.Lines(), "remote-a")
				require.True(t, ok)
				key := line.Key
				require.True(t, catalogue.Apply(ports.BrokerSnapshot{Epoch: 3, Revision: 2}))
				return key
			},
			wantErr: pickerCatalogueGone,
		},
		{
			name: "stale observation is still explicitly selectable",
			build: func(t *testing.T, catalogue *pickerCatalogue, clock *supervisorTestClock) string {
				require.True(t, catalogue.Apply(ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{
					pickerTestLocalObservation(now, pickerTestSession("alpha", 1, catalogue_Up)),
				}}))
				line, ok := pickerLineByLabel(catalogue.Lines(), "alpha")
				require.True(t, ok)
				advancePickerClock(clock, 2*pickerTestFreshness)
				return line.Key
			},
			wantLocal:     true,
			wantAdmission: ports.BrokerAdmissionExact,
		},
		{
			name: "incompatible host",
			build: func(t *testing.T, catalogue *pickerCatalogue, _ *supervisorTestClock) string {
				incompatible := pickerTestRemoteObservation("user@arch", 1, 1, now, pickerTestSession("remote-a", 2, catalogue_Up))
				incompatible.ProtocolVersion = protocol.Version + 1
				require.True(t, catalogue.Apply(ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{incompatible}}))
				line, ok := pickerLineByLabel(catalogue.Lines(), "remote-a")
				require.True(t, ok)
				return line.Key
			},
			wantErr: pickerCatalogueIncompatible,
		},
		{
			// Plan 003 B4: a remote host the broker observed failing refuses
			// instantly instead of a slow attempt that fails the same way.
			name: "unreachable remote host refuses instantly",
			build: func(t *testing.T, catalogue *pickerCatalogue, _ *supervisorTestClock) string {
				unavailable := pickerTestRemoteObservation("user@arch", 1, 1, now, pickerTestSession("remote-a", 2, catalogue_Up))
				unavailable.Availability = domain.RemoteAvailabilityUnreachable
				require.True(t, catalogue.Apply(ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{unavailable}}))
				line, ok := pickerLineByLabel(catalogue.Lines(), "remote-a")
				require.True(t, ok)
				return line.Key
			},
			wantErr: pickerCatalogueUnavailable,
		},
		{
			name: "stale reachable remote host stays attemptable",
			build: func(t *testing.T, catalogue *pickerCatalogue, _ *supervisorTestClock) string {
				stale := pickerTestRemoteObservation("user@arch", 1, 1, now.Add(-24*time.Hour), pickerTestSession("remote-a", 2, catalogue_Up))
				require.True(t, catalogue.Apply(ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{stale}}))
				line, ok := pickerLineByLabel(catalogue.Lines(), "remote-a")
				require.True(t, ok)
				return line.Key
			},
			wantAdmission: ports.BrokerAdmissionExact,
		},
		{
			name: "epoch change",
			build: func(t *testing.T, catalogue *pickerCatalogue, _ *supervisorTestClock) string {
				require.True(t, catalogue.Apply(ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{
					pickerTestLocalObservation(now, pickerTestSession("alpha", 1, catalogue_Up)),
				}}))
				line, ok := pickerLineByLabel(catalogue.Lines(), "alpha")
				require.True(t, ok)
				key := line.Key
				require.True(t, catalogue.Apply(ports.BrokerSnapshot{Epoch: 4, Revision: 1, Daemons: []ports.BrokerDaemonObservation{
					pickerTestLocalObservation(now, pickerTestSession("beta", 2, catalogue_Up)),
				}}))
				return key
			},
			wantErr: pickerCatalogueUnknown,
		},
		{
			name: "session lifecycle replaced",
			build: func(t *testing.T, catalogue *pickerCatalogue, _ *supervisorTestClock) string {
				require.True(t, catalogue.Apply(ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{
					pickerTestLocalObservation(now, pickerTestSession("alpha", 1, catalogue_Up)),
				}}))
				line, ok := pickerLineByLabel(catalogue.Lines(), "alpha")
				require.True(t, ok)
				key := line.Key
				require.True(t, catalogue.Apply(ports.BrokerSnapshot{Epoch: 3, Revision: 2, Daemons: []ports.BrokerDaemonObservation{
					pickerTestLocalObservation(now, pickerTestSession("alpha", 5, catalogue_Up)),
				}}))
				return key
			},
			wantErr: pickerCatalogueReplaced,
		},
		{
			name: "session gone",
			build: func(t *testing.T, catalogue *pickerCatalogue, _ *supervisorTestClock) string {
				require.True(t, catalogue.Apply(ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{
					pickerTestLocalObservation(now, pickerTestSession("alpha", 1, catalogue_Up), pickerTestSession("beta", 2, catalogue_Up)),
				}}))
				line, ok := pickerLineByLabel(catalogue.Lines(), "alpha")
				require.True(t, ok)
				key := line.Key
				require.True(t, catalogue.Apply(ports.BrokerSnapshot{Epoch: 3, Revision: 2, Daemons: []ports.BrokerDaemonObservation{
					pickerTestLocalObservation(now, pickerTestSession("beta", 2, catalogue_Up)),
				}}))
				return key
			},
			wantErr: pickerCatalogueGone,
		},
		{
			name: "unobserved host refuses as unavailable",
			build: func(t *testing.T, catalogue *pickerCatalogue, _ *supervisorTestClock) string {
				require.True(t, catalogue.Apply(ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{
					pickerTestUnobservedObservation(now, pickerTestSession("alpha", 1, catalogue_Up)),
				}}))
				line, ok := pickerLineByLabel(catalogue.Lines(), "alpha")
				require.True(t, ok)
				return line.Key
			},
			wantLocal:     true,
			wantAdmission: ports.BrokerAdmissionExact,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			catalogue, clock := pickerTestCatalogue(t)
			key := tt.build(t, catalogue, clock)
			request, err := catalogue.Resolve(key, base)
			if tt.wantErr != 0 {
				require.Error(t, err)
				require.True(t, pickerCatalogueErrorIs(err, tt.wantErr), "want %v, got %v", tt.wantErr, err)
				return
			}
			require.NoError(t, err)
			require.NoError(t, request.Validate(), "a resolved request always validates")
			require.Equal(t, ports.BrokerStreamAttachment, request.Purpose)
			require.Equal(t, tt.wantAdmission, request.Admission)
			require.Equal(t, tt.wantLocal, request.Local)
			require.Equal(t, ports.BrokerEpoch(3), request.Epoch)
			require.Equal(t, base.Connection, request.Connection)
			require.Equal(t, base.Stream, request.Stream)
		})
	}
}

func TestPickerCatalogueRemoteSelectionCarriesExactIdentity(t *testing.T) {
	clock := newSupervisorTestClock()
	now := clock.Now()
	catalogue, _ := pickerTestCatalogue(t)
	registration := pickerTestRegistration("user@arch", 1)
	lifecycle := pickerTestLifecycle(2)
	require.True(t, catalogue.Apply(ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{
		pickerTestRemoteObservation("user@arch", 1, 1, now, pickerTestSession("remote-a", 2, catalogue_Up)),
	}}))
	line, ok := pickerLineByLabel(catalogue.Lines(), "remote-a")
	require.True(t, ok)

	request, err := catalogue.Resolve(line.Key, pickerTestBase())
	require.NoError(t, err)
	require.Equal(t, "user@arch", request.Endpoint)
	require.Equal(t, registration, request.Registration)
	require.Equal(t, protocol.ExactSessionTarget{LifecycleID: lifecycle, SessionName: "remote-a"}, request.Target)
	require.Equal(t, pickerTestPolicy(), request.Policy)
}

func TestPickerCatalogueUnknownKeyRefused(t *testing.T) {
	catalogue, _ := pickerTestCatalogue(t)
	_, err := catalogue.Resolve("missing", pickerTestBase())
	require.Error(t, err)
	require.True(t, pickerCatalogueErrorIs(err, pickerCatalogueUnknown))
}

// TestPickerCatalogueEpochFenceOnCapturedRef pins the selection revision: a
// driver that captured a row's exact identity at one broker epoch may not
// resolve it after the broker process was replaced, even when the endpoint and
// session lifecycle happen to look the same.
func TestPickerCatalogueEpochFenceOnCapturedRef(t *testing.T) {
	clock := newSupervisorTestClock()
	now := clock.Now()
	catalogue, _ := pickerTestCatalogue(t)
	require.True(t, catalogue.Apply(ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{
		pickerTestLocalObservation(now, pickerTestSession("alpha", 1, catalogue_Up)),
	}}))
	line, ok := pickerLineByLabel(catalogue.Lines(), "alpha")
	require.True(t, ok)
	ref, ok := catalogue.Ref(line.Key)
	require.True(t, ok)

	// A new broker epoch resynchronizes the catalogue; the captured identity is
	// fenced even though its key is recomputed identically.
	require.True(t, catalogue.Apply(ports.BrokerSnapshot{Epoch: 4, Revision: 1, Daemons: []ports.BrokerDaemonObservation{
		pickerTestLocalObservation(now, pickerTestSession("alpha", 1, catalogue_Up)),
	}}))
	_, err := catalogue.ResolveRef(ref, pickerTestBase())
	require.Error(t, err)
	require.True(t, pickerCatalogueErrorIs(err, pickerCatalogueEpochStale))
}

func TestPickerCatalogueRetiredRefFenceIsBounded(t *testing.T) {
	catalogue, _ := pickerTestCatalogue(t)
	base := pickerTestBase()
	const inserted = pickerCatalogueMaxRetiredRefs + 8
	keys := make([]string, 0, inserted)
	for i := range inserted {
		name := fmt.Sprintf("s-%d", i)
		session := pickerTestSession(name, byte(i%200+1), catalogue_Up)
		require.True(t, catalogue.Apply(ports.BrokerSnapshot{Epoch: 3, Revision: ports.BrokerRevision(i + 1), Daemons: []ports.BrokerDaemonObservation{
			pickerTestLocalObservation(time.Unix(1000, 0), session),
		}}))
		line, ok := pickerLineByLabel(catalogue.Lines(), name)
		require.True(t, ok)
		keys = append(keys, line.Key)
	}
	// The fence holds the exact bound: only the newest retired keys survive.
	require.Len(t, catalogue.retired, pickerCatalogueMaxRetiredRefs)
	require.Len(t, catalogue.retiredOrder, pickerCatalogueMaxRetiredRefs)

	// A retained retired key still classifies as gone: its lifecycle left the
	// projection and no same-name replacement exists.
	retainedKey := keys[len(keys)-2]
	_, err := catalogue.Resolve(retainedKey, base)
	require.Error(t, err)
	require.True(t, pickerCatalogueErrorIs(err, pickerCatalogueGone), "a retained retired key must classify as gone: %v", err)

	// The oldest key was evicted from the bounded fence and is now unknown.
	evictedKey := keys[0]
	_, err = catalogue.Resolve(evictedKey, base)
	require.Error(t, err)
	require.True(t, pickerCatalogueErrorIs(err, pickerCatalogueUnknown), "an evicted retired key must classify as unknown: %v", err)
}

// TestPickerCatalogueRetiredRefFenceConcurrentApply exercises the retired-ref
// fence under -race: many goroutines fold distinct publications concurrently
// and the fence may never exceed its bound.
func TestPickerCatalogueRetiredRefFenceConcurrentApply(t *testing.T) {
	catalogue, _ := pickerTestCatalogue(t)
	const (
		workers   = 8
		perWorker = 64
	)
	var wait sync.WaitGroup
	for worker := range workers {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			for i := range perWorker {
				var lifecycle domain.SessionLifecycleID
				lifecycle[0] = byte(i + 1)
				lifecycle[1] = byte(worker + 1)
				session := pickerTestSessionWithLifecycle(fmt.Sprintf("w%d-%d", worker, i), lifecycle)
				catalogue.Apply(ports.BrokerSnapshot{Epoch: 3, Revision: ports.BrokerRevision(worker*perWorker + i + 1), Daemons: []ports.BrokerDaemonObservation{
					pickerTestLocalObservation(time.Unix(1000, 0), session),
				}})
			}
		}(worker)
	}
	wait.Wait()
	require.LessOrEqual(t, len(catalogue.retired), pickerCatalogueMaxRetiredRefs)
	require.LessOrEqual(t, len(catalogue.retiredOrder), pickerCatalogueMaxRetiredRefs)
}

// TestPickerCatalogueAvailabilityClassifiedBeforeCompatibility pins the
// authoritative ordering: availability first, then the reachable version
// branch. An unobserved (ProtocolVersion 0) or unreachable daemon is never a
// version mismatch.
func TestPickerCatalogueAvailabilityClassifiedBeforeCompatibility(t *testing.T) {
	now := time.Unix(1000, 0)
	reachable := func(mutate func(*ports.BrokerDaemonObservation)) ports.BrokerDaemonObservation {
		observation := pickerTestLocalObservation(now, pickerTestSession("alpha", 1, catalogue_Up))
		if mutate != nil {
			mutate(&observation)
		}
		return observation
	}
	unobserved := func(mutate func(*ports.BrokerDaemonObservation)) ports.BrokerDaemonObservation {
		observation := pickerTestUnobservedObservation(now, pickerTestSession("alpha", 1, catalogue_Up))
		if mutate != nil {
			mutate(&observation)
		}
		return observation
	}

	tests := []struct {
		name        string
		observation ports.BrokerDaemonObservation
		fresh       bool
		wantStatus  protocol.PickerLineStatus
		wantReason  string
	}{
		{name: "unobserved unknown is stale and refreshing", observation: unobserved(nil), fresh: false, wantStatus: protocol.PickerLineStatusStale, wantReason: domain.RemoteReasonRefreshing},
		{name: "unreachable is down", observation: unobserved(func(o *ports.BrokerDaemonObservation) { o.Availability = domain.RemoteAvailabilityUnreachable }), wantStatus: protocol.PickerLineStatusDown, wantReason: domain.RemoteReasonHostUnreachable},
		{name: "auth failed is down", observation: unobserved(func(o *ports.BrokerDaemonObservation) { o.Availability = domain.RemoteAvailabilityAuthFailed }), wantStatus: protocol.PickerLineStatusDown, wantReason: domain.RemoteReasonAuthFailure},
		{name: "invalid response is error", observation: unobserved(func(o *ports.BrokerDaemonObservation) { o.Availability = domain.RemoteAvailabilityInvalidResponse }), wantStatus: protocol.PickerLineStatusError, wantReason: domain.RemoteReasonMalformed},
		{name: "unreachable observed mismatch is down", observation: reachable(func(o *ports.BrokerDaemonObservation) {
			o.Availability = domain.RemoteAvailabilityUnreachable
			o.ProtocolVersion = protocol.Version + 1
		}), wantStatus: protocol.PickerLineStatusDown, wantReason: domain.RemoteReasonHostUnreachable},
		{name: "reachable mismatch is version", observation: reachable(func(o *ports.BrokerDaemonObservation) { o.ProtocolVersion = protocol.Version + 1 }), fresh: true, wantStatus: protocol.PickerLineStatusVersion, wantReason: domain.RemoteReasonVersionMismatch},
		{name: "reachable compatible fresh is up", observation: reachable(nil), fresh: true, wantStatus: protocol.PickerLineStatusUp, wantReason: ""},
		{name: "reachable compatible stale is stale", observation: reachable(nil), fresh: false, wantStatus: protocol.PickerLineStatusStale, wantReason: domain.RemoteReasonCatalogStale},
		{name: "reachable unobserved is stale and refreshing", observation: unobserved(func(o *ports.BrokerDaemonObservation) { o.Availability = domain.RemoteAvailabilityReachable }), fresh: true, wantStatus: protocol.PickerLineStatusStale, wantReason: domain.RemoteReasonRefreshing},
		{name: "legacy incompatible availability is version", observation: reachable(func(o *ports.BrokerDaemonObservation) { o.Availability = domain.RemoteAvailabilityIncompatible }), fresh: true, wantStatus: protocol.PickerLineStatusVersion, wantReason: domain.RemoteReasonVersionMismatch},
		{name: "reachable checking is up and refreshing", observation: reachable(func(o *ports.BrokerDaemonObservation) { o.Checking = true }), fresh: true, wantStatus: protocol.PickerLineStatusUp, wantReason: domain.RemoteReasonRefreshing},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.wantStatus, pickerObservationStatus(tt.observation, tt.fresh))
			require.Equal(t, tt.wantReason, pickerObservationReason(tt.observation, tt.fresh))
		})
	}
}

// TestPickerCatalogueResolutionAdmission pins the narrow refusal rules of the
// shared resolver: only a known incompatibility (a version mismatch or an
// availability classified as incompatible) is refused immediately. An
// unobserved, unreachable, auth-failed, invalid-response, or stale daemon stays
// explicitly attemptable, because only the destination revalidates the exact
// identity and only it may start a stopped target under the resolved policy.
func TestPickerCatalogueResolutionAdmission(t *testing.T) {
	now := time.Unix(1000, 0)
	tests := []struct {
		name    string
		daemon  ports.BrokerDaemonObservation
		wantErr pickerCatalogueErrorCode
	}{
		{
			name:   "unobserved unknown stays attemptable",
			daemon: pickerTestUnobservedObservation(now, pickerTestSession("alpha", 1, catalogue_Up)),
		},
		{
			name: "unreachable observed mismatch refuses incompatible",
			daemon: func() ports.BrokerDaemonObservation {
				o := pickerTestLocalObservation(now, pickerTestSession("alpha", 1, catalogue_Up))
				o.Availability = domain.RemoteAvailabilityUnreachable
				o.ProtocolVersion = protocol.Version + 1
				return o
			}(),
			wantErr: pickerCatalogueIncompatible,
		},
		{
			name: "auth failed stays attemptable",
			daemon: func() ports.BrokerDaemonObservation {
				o := pickerTestUnobservedObservation(now, pickerTestSession("alpha", 1, catalogue_Up))
				o.Availability = domain.RemoteAvailabilityAuthFailed
				return o
			}(),
		},
		{
			name: "invalid response stays attemptable",
			daemon: func() ports.BrokerDaemonObservation {
				o := pickerTestUnobservedObservation(now, pickerTestSession("alpha", 1, catalogue_Up))
				o.Availability = domain.RemoteAvailabilityInvalidResponse
				return o
			}(),
		},
		{
			name: "reachable unobserved stays attemptable",
			daemon: func() ports.BrokerDaemonObservation {
				o := pickerTestUnobservedObservation(now, pickerTestSession("alpha", 1, catalogue_Up))
				o.Availability = domain.RemoteAvailabilityReachable
				return o
			}(),
		},
		{
			name: "reachable mismatch refuses incompatible",
			daemon: func() ports.BrokerDaemonObservation {
				o := pickerTestLocalObservation(now, pickerTestSession("alpha", 1, catalogue_Up))
				o.ProtocolVersion = protocol.Version + 1
				return o
			}(),
			wantErr: pickerCatalogueIncompatible,
		},
		{
			name: "legacy incompatible availability refuses incompatible",
			daemon: func() ports.BrokerDaemonObservation {
				o := pickerTestLocalObservation(now, pickerTestSession("alpha", 1, catalogue_Up))
				o.Availability = domain.RemoteAvailabilityIncompatible
				return o
			}(),
			wantErr: pickerCatalogueIncompatible,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			catalogue, _ := pickerTestCatalogue(t)
			require.True(t, catalogue.Apply(ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{tt.daemon}}))
			line, ok := pickerLineByLabel(catalogue.Lines(), "alpha")
			require.True(t, ok)
			request, err := catalogue.Resolve(line.Key, pickerTestBase())
			if tt.wantErr != 0 {
				require.Error(t, err)
				require.True(t, pickerCatalogueErrorIs(err, tt.wantErr), "want %v, got %v", tt.wantErr, err)
				return
			}
			require.NoError(t, err)
			require.NoError(t, request.Validate())
			require.Equal(t, ports.BrokerDaemonStartIfNeeded, request.StartMode, "an explicit attempt keeps the start authorization")
		})
	}
}

// TestPickerCatalogueRejectsInvalidSnapshot pins the projection trust
// boundary: a snapshot that fails its own contract is refused and the previous
// projection is kept intact.
func TestPickerCatalogueRejectsInvalidSnapshot(t *testing.T) {
	catalogue, clock := pickerTestCatalogue(t)
	valid := pickerTestSnapshot(3, 1, clock.Now(), "alpha")
	require.True(t, catalogue.Apply(valid))
	validLines := catalogue.Lines()
	require.Equal(t, ports.BrokerRevision(1), catalogue.Revision())

	invalid := pickerTestSnapshot(3, 2, clock.Now(), "beta")
	invalid.Daemons[0].Availability = 0 // out of range: availability is never zero
	require.False(t, catalogue.Apply(invalid), "an invalid snapshot is refused")
	require.Equal(t, ports.BrokerRevision(1), catalogue.Revision(), "the previous projection is kept")
	require.Equal(t, validLines, catalogue.Lines())
}

// TestPickerCatalogueUnobservedDaemonProjects proves an unobserved daemon
// (Availability Unknown, ProtocolVersion 0, DisplayOrigin set) still projects a
// section and host row, classified as stale/refreshing rather than as a version
// mismatch.
func TestPickerCatalogueUnobservedDaemonProjects(t *testing.T) {
	catalogue, _ := pickerTestCatalogue(t)
	unobserved := pickerTestUnobservedObservation(time.Unix(1000, 0), pickerTestSession("alpha", 1, catalogue_Up))
	require.True(t, catalogue.Apply(ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{unobserved}}))

	lines := catalogue.Lines()
	require.Equal(t, []string{"local"}, pickerSectionLabels(lines))
	require.Equal(t, []string{"alpha"}, pickerSessionLabels(lines))
	_, hasHost := pickerHostLine(lines)
	require.False(t, hasHost, "the session rows already carry the host state")
	session, ok := pickerLineByLabel(lines, "alpha")
	require.True(t, ok)
	require.True(t, session.Focusable)
	require.Equal(t, protocol.PickerCanNavigate|protocol.PickerCanKill, session.Actions, "an unobserved daemon may still be attempted explicitly")
	require.True(t, session.Dim)
	require.Equal(t, domain.RemoteReasonRefreshing, session.StatusDetail, "an unobserved daemon refreshes, it is not a version mismatch")
}

// TestPickerCatalogueOriginLabelNeverShowsRawEndpoint proves display labels
// never fall back to a raw endpoint.
func TestPickerCatalogueOriginLabelNeverShowsRawEndpoint(t *testing.T) {
	raw := pickerTestRemoteObservation("user@arch", 1, 1, time.Unix(1000, 0))
	raw.DisplayOrigin = ""
	require.Equal(t, pickerCatalogueOriginFallback, pickerOriginLabel(raw))
	require.NotContains(t, pickerOriginLabel(raw), "user@arch")

	require.Equal(t, "local", pickerOriginLabel(pickerTestUnobservedObservation(time.Unix(1000, 0))))
	require.Equal(t, "user@arch", pickerOriginLabel(pickerTestRemoteObservation("user@arch", 1, 1, time.Unix(1000, 0))))
}
