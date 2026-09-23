package client

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// Closed initial-navigation union (Plan 001 P7, task 2). These tables pin the
// four variants locally and remotely, the zero Picker, the contradiction rules,
// the exact-registration equality fence, the one-shot consumption, and the
// "a reconnect never replays" contract.

func validInitialNavigationTarget() protocol.ExactSessionTarget {
	return protocol.ExactSessionTarget{LifecycleID: pickerTestLifecycle(2), SessionName: "alpha"}
}

func validInitialNavigationRemoteFence() ports.BrokerEndpointFence {
	return ports.BrokerEndpointFence{Registration: pickerTestRegistration("user@arch", 1)}
}

// TestInitialNavigationValidationClosedUnion pins the closed union's offline
// validation: the four variants and every contradictory field combination.
func TestInitialNavigationValidationClosedUnion(t *testing.T) {
	target := validInitialNavigationTarget()
	remote := validInitialNavigationRemoteFence()

	tests := []struct {
		name       string
		navigation InitialNavigation
		wantErr    bool
	}{
		{
			name:       "zero value is the safe picker",
			navigation: InitialNavigation{},
		},
		{
			name:       "picker with a local destination contradicts",
			navigation: InitialNavigation{Kind: InitialNavigationPicker, Destination: ports.BrokerEndpointFence{Local: true}},
			wantErr:    true,
		},
		{
			name:       "picker with an epoch contradicts",
			navigation: InitialNavigation{Kind: InitialNavigationPicker, Epoch: 3},
			wantErr:    true,
		},
		{
			name:       "picker with a name contradicts",
			navigation: InitialNavigation{Kind: InitialNavigationPicker, Name: "alpha"},
			wantErr:    true,
		},
		{
			name:       "picker with a target contradicts",
			navigation: InitialNavigation{Kind: InitialNavigationPicker, Target: target},
			wantErr:    true,
		},
		{
			name:       "local ephemeral creation may omit the epoch",
			navigation: InitialNavigation{Kind: InitialNavigationCreateEphemeral, Destination: ports.BrokerEndpointFence{Local: true}},
		},
		{
			name:       "local named creation may omit the epoch",
			navigation: InitialNavigation{Kind: InitialNavigationCreateNamed, Destination: ports.BrokerEndpointFence{Local: true}, Name: "fresh"},
		},
		{
			name:       "remote creation requires the resolving epoch",
			navigation: InitialNavigation{Kind: InitialNavigationCreateEphemeral, Destination: remote},
			wantErr:    true,
		},
		{
			name:       "remote creation with the epoch is valid",
			navigation: InitialNavigation{Kind: InitialNavigationCreateEphemeral, Epoch: 3, Destination: remote},
		},
		{
			name:       "ephemeral creation on a bare registration contradicts",
			navigation: InitialNavigation{Kind: InitialNavigationCreateEphemeral, Destination: ports.BrokerEndpointFence{}},
			wantErr:    true,
		},
		{
			name:       "creation with a target contradicts",
			navigation: InitialNavigation{Kind: InitialNavigationCreateEphemeral, Destination: ports.BrokerEndpointFence{Local: true}, Target: target},
			wantErr:    true,
		},
		{
			name:       "ephemeral creation with a name contradicts",
			navigation: InitialNavigation{Kind: InitialNavigationCreateEphemeral, Destination: ports.BrokerEndpointFence{Local: true}, Name: "alpha"},
			wantErr:    true,
		},
		{
			name:       "named creation with an invalid name contradicts",
			navigation: InitialNavigation{Kind: InitialNavigationCreateNamed, Destination: ports.BrokerEndpointFence{Local: true}, Name: "bad name"},
			wantErr:    true,
		},
		{
			name:       "named creation with a target contradicts",
			navigation: InitialNavigation{Kind: InitialNavigationCreateNamed, Destination: ports.BrokerEndpointFence{Local: true}, Name: "fresh", Target: target},
			wantErr:    true,
		},
		{
			name:       "local exact attach still requires an epoch",
			navigation: InitialNavigation{Kind: InitialNavigationAttachExact, Destination: ports.BrokerEndpointFence{Local: true}, Target: target},
			wantErr:    true,
		},
		{
			name:       "local exact attach with an epoch is valid",
			navigation: InitialNavigation{Kind: InitialNavigationAttachExact, Epoch: 3, Destination: ports.BrokerEndpointFence{Local: true}, Target: target},
		},
		{
			name:       "remote exact attach with an epoch is valid",
			navigation: InitialNavigation{Kind: InitialNavigationAttachExact, Epoch: 3, Destination: remote, Target: target},
		},
		{
			name:       "exact attach with a name contradicts",
			navigation: InitialNavigation{Kind: InitialNavigationAttachExact, Epoch: 3, Destination: ports.BrokerEndpointFence{Local: true}, Name: "alpha", Target: target},
			wantErr:    true,
		},
		{
			name:       "exact attach with an invalid target contradicts",
			navigation: InitialNavigation{Kind: InitialNavigationAttachExact, Epoch: 3, Destination: ports.BrokerEndpointFence{Local: true}, Target: protocol.ExactSessionTarget{SessionName: "alpha"}},
			wantErr:    true,
		},
		{
			name:       "unknown kind is refused",
			navigation: InitialNavigation{Kind: InitialNavigationKind(9), Destination: ports.BrokerEndpointFence{Local: true}},
			wantErr:    true,
		},
		{
			name:       "local destination carrying a registration contradicts",
			navigation: InitialNavigation{Kind: InitialNavigationCreateEphemeral, Destination: ports.BrokerEndpointFence{Local: true, Registration: pickerTestRegistration("user@arch", 1)}},
			wantErr:    true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.navigation.Validate()
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestInitialNavigationResolveVariants pins the exact request each variant
// produces, locally and remotely: purpose, admission, destination authority,
// policy provenance, and the exclusive name/target fields.
func TestInitialNavigationResolveVariants(t *testing.T) {
	now := time.Unix(1000, 0)
	target := validInitialNavigationTarget()
	remote := validInitialNavigationRemoteFence()

	tests := []struct {
		name          string
		navigation    InitialNavigation
		snapshot      ports.BrokerSnapshot
		wantErr       pickerCatalogueErrorCode
		wantLocal     bool
		wantAdmission ports.BrokerStreamAdmission
		wantName      string
		wantTarget    protocol.ExactSessionTarget
	}{
		{
			name:       "local ephemeral creation",
			navigation: InitialNavigation{Kind: InitialNavigationCreateEphemeral, Destination: ports.BrokerEndpointFence{Local: true}},
			snapshot:   localDaemonSnapshot(3, 1),
			wantLocal:  true, wantAdmission: ports.BrokerAdmissionCreateEphemeral,
		},
		{
			name:       "local named creation",
			navigation: InitialNavigation{Kind: InitialNavigationCreateNamed, Destination: ports.BrokerEndpointFence{Local: true}, Name: "fresh"},
			snapshot:   localDaemonSnapshot(3, 1),
			wantLocal:  true, wantAdmission: ports.BrokerAdmissionCreateNamed, wantName: "fresh",
		},
		{
			name:       "local exact attach",
			navigation: InitialNavigation{Kind: InitialNavigationAttachExact, Epoch: 3, Destination: ports.BrokerEndpointFence{Local: true}, Target: target},
			snapshot: ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{
				pickerTestLocalObservation(now, pickerTestSessionWithLifecycle("alpha", target.LifecycleID)),
			}},
			wantLocal: true, wantAdmission: ports.BrokerAdmissionExact, wantTarget: target,
		},
		{
			name:       "remote ephemeral creation",
			navigation: InitialNavigation{Kind: InitialNavigationCreateEphemeral, Epoch: 3, Destination: remote},
			snapshot: ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{
				pickerTestRemoteObservation("user@arch", 1, 1, now),
			}},
			wantAdmission: ports.BrokerAdmissionCreateEphemeral,
		},
		{
			name:       "remote named creation",
			navigation: InitialNavigation{Kind: InitialNavigationCreateNamed, Epoch: 3, Destination: remote, Name: "fresh"},
			snapshot: ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{
				pickerTestRemoteObservation("user@arch", 1, 1, now),
			}},
			wantAdmission: ports.BrokerAdmissionCreateNamed, wantName: "fresh",
		},
		{
			name:       "remote exact attach",
			navigation: InitialNavigation{Kind: InitialNavigationAttachExact, Epoch: 3, Destination: remote, Target: target},
			snapshot: ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{
				pickerTestRemoteObservation("user@arch", 1, 1, now, pickerTestSessionWithLifecycle("alpha", target.LifecycleID)),
			}},
			wantAdmission: ports.BrokerAdmissionExact, wantTarget: target,
		},
		{
			name:       "local creation on a stopped daemon stays attemptable",
			navigation: InitialNavigation{Kind: InitialNavigationCreateEphemeral, Destination: ports.BrokerEndpointFence{Local: true}},
			snapshot: ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{
				func() ports.BrokerDaemonObservation {
					observation := pickerTestUnobservedObservation(now)
					observation.Availability = domain.RemoteAvailabilityUnreachable
					return observation
				}(),
			}},
			wantLocal: true, wantAdmission: ports.BrokerAdmissionCreateEphemeral,
		},
		{
			name:       "local creation on a never-observed daemon stays attemptable",
			navigation: InitialNavigation{Kind: InitialNavigationCreateEphemeral, Destination: ports.BrokerEndpointFence{Local: true}},
			snapshot: ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{
				pickerTestUnobservedObservation(now),
			}},
			wantLocal: true, wantAdmission: ports.BrokerAdmissionCreateEphemeral,
		},
		{
			name:       "epoch change refuses the resolved intent",
			navigation: InitialNavigation{Kind: InitialNavigationAttachExact, Epoch: 3, Destination: ports.BrokerEndpointFence{Local: true}, Target: target},
			snapshot: ports.BrokerSnapshot{Epoch: 4, Revision: 1, Daemons: []ports.BrokerDaemonObservation{
				pickerTestLocalObservation(now, pickerTestSessionWithLifecycle("alpha", target.LifecycleID)),
			}},
			wantErr: pickerCatalogueEpochStale,
		},
		{
			name:       "re-added endpoint refuses the resolved intent",
			navigation: InitialNavigation{Kind: InitialNavigationCreateEphemeral, Epoch: 3, Destination: remote},
			snapshot: ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{
				pickerTestRemoteObservation("user@arch", 2, 2, now),
			}},
			wantErr: pickerCatalogueReplaced,
		},
		{
			name:       "advanced registration generation refuses the resolved intent",
			navigation: InitialNavigation{Kind: InitialNavigationCreateEphemeral, Epoch: 3, Destination: remote},
			snapshot: ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{
				func() ports.BrokerDaemonObservation {
					observation := pickerTestRemoteObservation("user@arch", 1, 1, now)
					observation.Registration.Generation = 2
					return observation
				}(),
			}},
			wantErr: pickerCatalogueReplaced,
		},
		{
			name:       "same-name new lifecycle refuses the exact attach",
			navigation: InitialNavigation{Kind: InitialNavigationAttachExact, Epoch: 3, Destination: ports.BrokerEndpointFence{Local: true}, Target: target},
			snapshot: ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{
				pickerTestLocalObservation(now, pickerTestSession("alpha", 5, catalogue_Up)),
			}},
			wantErr: pickerCatalogueReplaced,
		},
		{
			name:       "gone session refuses the exact attach",
			navigation: InitialNavigation{Kind: InitialNavigationAttachExact, Epoch: 3, Destination: ports.BrokerEndpointFence{Local: true}, Target: target},
			snapshot:   localDaemonSnapshot(3, 1),
			wantErr:    pickerCatalogueGone,
		},
		{
			name:       "known incompatibility refuses immediately",
			navigation: InitialNavigation{Kind: InitialNavigationCreateEphemeral, Destination: ports.BrokerEndpointFence{Local: true}},
			snapshot: ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{
				func() ports.BrokerDaemonObservation {
					observation := pickerTestLocalObservation(now)
					observation.ProtocolVersion = protocol.Version + 1
					return observation
				}(),
			}},
			wantErr: pickerCatalogueIncompatible,
		},
		{
			name:       "unavailable snapshot refuses",
			navigation: InitialNavigation{Kind: InitialNavigationCreateEphemeral, Destination: ports.BrokerEndpointFence{Local: true}},
			snapshot:   ports.BrokerSnapshot{},
			wantErr:    pickerCatalogueUnavailable,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request, err := tt.navigation.Resolve(tt.snapshot.Clone())
			if tt.wantErr != 0 {
				require.Error(t, err)
				require.True(t, pickerCatalogueErrorIs(err, tt.wantErr), "want %v, got %v", tt.wantErr, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, ports.BrokerStreamAttachment, request.Purpose)
			require.Equal(t, tt.wantLocal, request.Local)
			require.Equal(t, tt.wantAdmission, request.Admission)
			require.Equal(t, tt.wantName, request.Name)
			require.Equal(t, tt.wantTarget, request.Target)
			require.Equal(t, ports.BrokerEpoch(tt.snapshot.Epoch), request.Epoch)
			require.Equal(t, ports.BrokerDaemonStartIfNeeded, request.StartMode)
			// The gabarit never owns the live connection identity.
			require.True(t, request.Connection.IsZero())
			require.Zero(t, request.Stream)
		})
	}
}

// TestInitialNavigationPickerResolveIsRefused pins that resolving the picker
// directly is refused rather than returning a nil-but-usable request.
func TestInitialNavigationPickerResolveIsRefused(t *testing.T) {
	request, err := InitialNavigation{}.Resolve(localDaemonSnapshot(3, 1))
	require.Error(t, err)
	require.True(t, pickerCatalogueErrorIs(err, pickerCatalogueNoSelection))
	require.Equal(t, ports.BrokerOpenStreamRequest{}, request)
}

// TestInitialNavigationResolverIsConsultedAtMostOnce pins the one-shot
// consumption: the resolver runs exactly once for the whole run, on a
// defensive copy of the first committed publication, and a reconnect never
// replays the navigation.
func TestInitialNavigationResolverIsConsultedAtMostOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clock := newSupervisorTestClock()
	reader := newAttachTestReader()
	terminal := &attachTestTerminal{in: reader}
	picker := newAttachTestPicker()

	first := newSupervisorTestService(ports.BrokerConnectionID{1})
	first.publishSnapshot(localDaemonSnapshot(7, 1))
	second := newSupervisorTestService(ports.BrokerConnectionID{2})
	stream := newSessionTestStream()
	first.setOpenStream(func(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
		return stream, nil
	})
	connector := newSupervisorTestConnector(func(_ context.Context, call int) (ports.BrokerService, error) {
		if call == 1 {
			return first, nil
		}
		return second, nil
	})

	var calls atomic.Int64
	var resolvedEpoch atomic.Uint64
	var mutatedOnce atomic.Bool
	cfg := SupervisorConfig{
		Connector: connector,
		Terminal:  terminal,
		Clock:     clock,
		Jitter:    func() float64 { return 0 },
		Picker:    picker,
		ResolveInitialNavigation: func(snapshot ports.BrokerSnapshot) (InitialNavigation, error) {
			calls.Add(1)
			resolvedEpoch.Store(uint64(snapshot.Epoch))
			if !mutatedOnce.Swap(true) {
				// A defensive copy: mutating the argument must not affect the
				// supervisor's committed authority.
				snapshot.Epoch = 0
				snapshot.Daemons = nil
			}
			return InitialNavigation{Kind: InitialNavigationCreateEphemeral, Destination: ports.BrokerEndpointFence{Local: true}}, nil
		},
	}
	sup := mustSupervisor(t, cfg)
	done := make(chan error, 1)
	go func() { done <- sup.Run(ctx) }()

	require.Eventually(t, func() bool { return len(first.openedRequests()) == 1 }, 5*time.Second, time.Millisecond)
	require.Equal(t, int64(1), calls.Load(), "the resolver runs exactly once")
	// The resolver may not mutate the supervisor's authority: the epoch it saw
	// stays the committed one.
	require.Equal(t, ports.BrokerEpoch(7), ports.BrokerEpoch(resolvedEpoch.Load()))
	require.Equal(t, ports.BrokerEpoch(7), first.Snapshot().Epoch)
	require.Len(t, first.Snapshot().Daemons, 1)

	stream.deliver(protocol.ErrorMsg{Code: protocol.ErrInternal, Text: "creation failed"})
	require.Eventually(t, func() bool { return sup.State().Presentation == PresentPicker }, 5*time.Second, time.Millisecond)

	first.lose(ports.BrokerError{Code: ports.BrokerErrorUnavailable})
	require.Eventually(t, func() bool { return sup.State().Connectivity == ConnectivityRetryWait }, 5*time.Second, time.Millisecond)
	for connector.callCount() < 2 {
		clock.mu.Lock()
		var timer *supervisorTestTimer
		if len(clock.timers) != 0 {
			timer = clock.timers[len(clock.timers)-1]
		}
		clock.mu.Unlock()
		if timer != nil {
			timer.fire()
		}
	}
	second.publishSnapshot(localDaemonSnapshot(8, 1))
	require.Eventually(t, func() bool { return sup.State().Connectivity == ConnectivityReady }, 5*time.Second, time.Millisecond)
	require.Empty(t, second.openedRequests(), "a reconnect never replays the one-shot navigation")
	require.Equal(t, int64(1), calls.Load(), "the resolver is never re-run on the replacement generation")
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

// TestInitialNavigationPickerIntentOpensNothing pins that a composition that
// omits the intent, or sets the picker explicitly, opens no stream and leaves
// the picker running.
func TestInitialNavigationPickerIntentOpensNothing(t *testing.T) {
	for _, tc := range []struct {
		name       string
		configure  func(*SupervisorConfig)
		wantPicker bool
	}{
		{name: "omitted intent", configure: func(*SupervisorConfig) {}},
		{name: "explicit picker", configure: func(cfg *SupervisorConfig) { cfg.InitialNavigation = InitialNavigation{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			picker := newAttachTestPicker()
			harness := startAttachHarnessConfig(t, picker, tc.configure)
			require.Equal(t, PresentPicker, harness.sup.State().Presentation)
			require.Empty(t, harness.service.openedRequests(), "a picker intent opens no stream")
			require.Zero(t, picker.resolveCount())
		})
	}
}

// TestNewSupervisorRefusesContradictoryInitialConfiguration pins that a
// configuration carrying both an intent and a resolver, or an invalid intent,
// is refused at construction instead of at the first connection.
func TestNewSupervisorRefusesContradictoryInitialConfiguration(t *testing.T) {
	clock := newSupervisorTestClock()
	reader := newSupervisorTestReader()
	t.Cleanup(reader.unblock)
	terminal := newSupervisorTestTerminal(reader)
	connector := newSupervisorTestConnector(func(context.Context, int) (ports.BrokerService, error) {
		return nil, ports.BrokerError{Code: ports.BrokerErrorUnavailable}
	})

	base := func() SupervisorConfig {
		return SupervisorConfig{Connector: connector, Terminal: terminal, Clock: clock, Jitter: func() float64 { return 0 }, SessionEnvironment: SessionEnvironment{Provenance: SessionEnvironmentLocalCLI}}
	}

	both := base()
	both.InitialNavigation = InitialNavigation{Kind: InitialNavigationCreateEphemeral, Destination: ports.BrokerEndpointFence{Local: true}}
	both.ResolveInitialNavigation = func(ports.BrokerSnapshot) (InitialNavigation, error) { return InitialNavigation{}, nil }
	_, err := NewSupervisor(both)
	require.Error(t, err, "an intent and a resolver are mutually exclusive")

	invalid := base()
	invalid.InitialNavigation = InitialNavigation{Kind: InitialNavigationAttachExact, Destination: ports.BrokerEndpointFence{Local: true}}
	_, err = NewSupervisor(invalid)
	require.Error(t, err, "an exact attach without an epoch is refused at construction")

	valid := base()
	valid.InitialNavigation = InitialNavigation{Kind: InitialNavigationCreateEphemeral, Destination: ports.BrokerEndpointFence{Local: true}}
	_, err = NewSupervisor(valid)
	require.NoError(t, err)
}

// TestInitialNavigationResolverFailureIsSelectionUnavailable pins that a
// resolver that refuses localises to the selection-unavailable notice and never
// dials a destination or replays.
func TestInitialNavigationResolverFailureIsSelectionUnavailable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clock := newSupervisorTestClock()
	reader := newAttachTestReader()
	terminal := &attachTestTerminal{in: reader}
	picker := newAttachTestPicker()
	service := newSupervisorTestService(ports.BrokerConnectionID{1})
	service.publishSnapshot(localDaemonSnapshot(3, 1))
	opened := false
	service.setOpenStream(func(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
		opened = true
		return newSessionTestStream(), nil
	})
	notices := make(chan LifecycleNotice, 4)
	var calls atomic.Int64
	resolverErr := errors.New("no such target")
	sup := mustSupervisor(t, SupervisorConfig{
		Connector: newSupervisorTestConnector(func(context.Context, int) (ports.BrokerService, error) { return service, nil }),
		Terminal:  terminal, Clock: clock, Jitter: func() float64 { return 0 }, Picker: picker,
		NotifyLifecycle: func(notice LifecycleNotice) { notices <- notice },
		ResolveInitialNavigation: func(ports.BrokerSnapshot) (InitialNavigation, error) {
			calls.Add(1)
			return InitialNavigation{}, resolverErr
		},
	})
	done := make(chan error, 1)
	go func() { done <- sup.Run(ctx) }()
	require.Equal(t, LifecycleNoticeSelectionUnavailable, (<-notices).Kind)
	require.False(t, opened, "a refused resolver never dials a destination")
	require.Equal(t, int64(1), calls.Load())
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

// TestInitialNavigationWaitsForTargetObservation pins that a resolver whose
// target daemon is not observed yet keeps the intent armed: it resolves again
// on the next committed publication, and only the bounded observation budget
// turns the wait into a selection-unavailable refusal.
func TestInitialNavigationWaitsForTargetObservation(t *testing.T) {
	tests := []struct {
		name string
		// observedAt is the first revision the resolver can decide on; zero
		// means the target is never observed.
		observedAt ports.BrokerRevision
		wantOpen   bool
	}{
		{name: "a later publication resolves the armed intent", observedAt: 2, wantOpen: true},
		{name: "the observation budget refuses once", observedAt: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			clock := newSupervisorTestClock()
			reader := newAttachTestReader()
			terminal := &attachTestTerminal{in: reader}
			picker := newAttachTestPicker()
			service := newSupervisorTestService(ports.BrokerConnectionID{1})
			service.publishSnapshot(localDaemonSnapshot(3, 1))
			var opened atomic.Int64
			service.setOpenStream(func(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
				opened.Add(1)
				return newSessionTestStream(), nil
			})
			notices := make(chan LifecycleNotice, 4)
			var calls atomic.Int64
			sup := mustSupervisor(t, SupervisorConfig{
				Connector: newSupervisorTestConnector(func(context.Context, int) (ports.BrokerService, error) { return service, nil }),
				Terminal:  terminal, Clock: clock, Jitter: func() float64 { return 0 }, Picker: picker,
				NotifyLifecycle: func(notice LifecycleNotice) { notices <- notice },
				ResolveInitialNavigation: func(snapshot ports.BrokerSnapshot) (InitialNavigation, error) {
					calls.Add(1)
					if tt.observedAt == 0 || snapshot.Revision < tt.observedAt {
						return InitialNavigation{}, ErrInitialNavigationNotObserved
					}
					return InitialNavigation{Kind: InitialNavigationCreateEphemeral, Destination: ports.BrokerEndpointFence{Local: true}}, nil
				},
			})
			done := make(chan error, 1)
			go func() { done <- sup.Run(ctx) }()

			require.Eventually(t, func() bool { return calls.Load() == 1 }, 5*time.Second, time.Millisecond)
			var budget *supervisorTestTimer
			require.Eventually(t, func() bool {
				clock.mu.Lock()
				defer clock.mu.Unlock()
				for _, timer := range clock.timers {
					if timer.delay == initialNavigationObservationBudget {
						budget = timer
						return true
					}
				}
				return false
			}, 5*time.Second, time.Millisecond, "the wait is bounded by the observation budget")
			require.Zero(t, opened.Load(), "an unobserved target never dials")

			if tt.wantOpen {
				service.publishSnapshot(localDaemonSnapshot(3, 2))
				require.Eventually(t, func() bool { return opened.Load() == 1 }, 5*time.Second, time.Millisecond)
				require.Equal(t, int64(2), calls.Load(), "the armed intent resolves once per publication")
			} else {
				budget.fire()
				select {
				case notice := <-notices:
					require.Equal(t, LifecycleNoticeSelectionUnavailable, notice.Kind)
				case <-time.After(5 * time.Second):
					t.Fatal("the budget never refused the unobserved target")
				}
				require.Zero(t, opened.Load())
				service.publishSnapshot(localDaemonSnapshot(3, 2))
				require.Never(t, func() bool { return calls.Load() > 1 }, 100*time.Millisecond, 5*time.Millisecond, "a refused intent is never re-armed")
			}
			cancel()
			require.ErrorIs(t, <-done, context.Canceled)
		})
	}
}
