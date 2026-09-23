package broker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// localProbeFunc adapts a function to LocalProbe. LocalProbe is a use-case-local
// seam outside internal/ports, so no generated portsmocks mock exists for it.
type localProbeFunc func(context.Context) (ports.BrokerDaemonObservation, error)

func (f localProbeFunc) ProbeLocal(ctx context.Context) (ports.BrokerDaemonObservation, error) {
	return f(ctx)
}

// TestRegistryProbeDeadlineReleasesAttempt proves every observation attempt,
// local and remote, is bounded by the registry's ports.Clock deadline: a probe
// that never answers (whether or not it honors cancellation) is recorded as a
// typed timeout, its in-flight slot is released, and the next attempt is
// dispatched on the retry cadence. A probe that answers in time never reports a
// timeout once the deadline would have fired.
func TestRegistryProbeDeadlineReleasesAttempt(t *testing.T) {
	const probeTimeout = 3 * time.Second
	tests := []struct {
		name string
		// local selects the broker's own daemon instead of one remote host.
		local bool
		// honorsCancel: the hung probe returns once its context is cancelled;
		// otherwise it ignores cancellation until the test ends.
		honorsCancel bool
		// answers: the first attempt succeeds immediately instead of hanging.
		answers bool
	}{
		{name: "remote probe honoring cancellation times out", honorsCancel: true},
		{name: "remote probe ignoring cancellation still releases", honorsCancel: false},
		{name: "local probe honoring cancellation times out", local: true, honorsCancel: true},
		{name: "local probe ignoring cancellation still releases", local: true, honorsCancel: false},
		{name: "remote probe answering in time is not timed out", answers: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clock := newManualClock(time.Unix(100, 0))
			reg := registration(t, "user@host:22", 1)
			release := make(chan struct{})
			t.Cleanup(func() { close(release) })
			// calls carries each attempt's context to the test goroutine; the
			// probe worker only sends, it never asserts.
			calls := make(chan context.Context, 8)
			reachable := ports.BrokerDaemonObservation{
				Identity: "daemon", Incarnation: ports.BrokerDaemonIncarnation{7}, ProtocolVersion: protocol.Version,
				Availability: domain.RemoteAvailabilityReachable, InventoryKnown: true,
			}
			var attempts atomic.Int32
			observe := func(ctx context.Context) (ports.BrokerDaemonObservation, error) {
				first := attempts.Add(1) == 1
				calls <- ctx
				if tt.answers || !first {
					return reachable, nil
				}
				if tt.honorsCancel {
					<-ctx.Done()
					return ports.BrokerDaemonObservation{}, context.Cause(ctx)
				}
				<-release
				return ports.BrokerDaemonObservation{}, errors.New("late completion")
			}

			remote := portsmocks.NewMockBrokerHostProbe(t)
			var localProbe *LocalObservation
			if tt.local {
				localProbe = &LocalObservation{DisplayOrigin: "local", Policy: poolPolicy(), Probe: localProbeFunc(observe)}
			} else {
				remote.EXPECT().Probe(mock.Anything, reg).RunAndReturn(func(ctx context.Context, _ domain.RemoteRegistration) (ports.BrokerDaemonObservation, error) {
					return observe(ctx)
				})
			}
			registry, err := NewRegistryWithConfig(1, newTestStore(), remote, clock, nil, RegistryConfig{Local: localProbe, ProbeTimeout: probeTimeout})
			require.NoError(t, err)
			registry.jitter = identityJitter
			if !tt.local {
				require.NoError(t, registry.setHosts(hostRecords(reg)))
			}
			startRegistry(t, registry)

			host := func(predicate func(ports.BrokerDaemonObservation) bool) ports.BrokerDaemonObservation {
				if tt.local {
					return waitLocal(t, registry, predicate)
				}
				return waitHost(t, registry, reg.Endpoint, predicate)
			}
			receive := func() context.Context {
				t.Helper()
				select {
				case ctx := <-calls:
					return ctx
				case <-time.After(time.Second):
					t.Fatal("timed out waiting for a probe attempt")
					return nil
				}
			}

			firstCtx := receive()
			if tt.answers {
				host(func(o ports.BrokerDaemonObservation) bool {
					return !o.Checking && o.Availability == domain.RemoteAvailabilityReachable
				})
				// The deadline of a completed attempt is stopped: moving past it
				// never turns the observation into a timeout.
				clock.Advance(probeTimeout)
				observed := host(func(o ports.BrokerDaemonObservation) bool { return !o.Checking })
				require.Equal(t, domain.RemoteFailureNone, observed.LastFailure.Kind)
				require.Zero(t, observed.ConsecutiveFailures)
				return
			}

			host(func(o ports.BrokerDaemonObservation) bool { return o.Checking })
			clock.Advance(probeTimeout)
			failed := host(func(o ports.BrokerDaemonObservation) bool {
				return !o.Checking && o.ConsecutiveFailures == 1
			})
			require.Equal(t, domain.RemoteFailureTimeout, failed.LastFailure.Kind)
			require.ErrorIs(t, failed.LastFailure.Err, context.DeadlineExceeded)
			// The hung attempt's own context was cancelled by the deadline, so a
			// ctx-honoring dial or exchange unwinds instead of lingering.
			require.ErrorIs(t, context.Cause(firstCtx), errProbeTimeout)

			// The slot was released: the retry cadence dispatches a fresh
			// attempt, which succeeds. Advance repeatedly because the run loop
			// re-arms its timer asynchronously after publishing the failure.
			var second context.Context
			require.Eventually(t, func() bool {
				clock.Advance(defaultRetryBase)
				select {
				case second = <-calls:
					return true
				default:
					return false
				}
			}, 2*time.Second, time.Millisecond)
			require.NotNil(t, second)
			recovered := host(func(o ports.BrokerDaemonObservation) bool {
				return !o.Checking && o.Availability == domain.RemoteAvailabilityReachable
			})
			require.Zero(t, recovered.ConsecutiveFailures)
		})
	}
}
