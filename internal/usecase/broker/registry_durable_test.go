package broker

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/bnema/vev/internal/adapters/brokerstore"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/stretchr/testify/require"
)

// Exercise the real CAS and snapshot validator, not the observation-only fake.
// Each stage closes and reopens both owners, so membership and observations
// must come from disk, including the empty set and a fresh re-added identity.
func TestRegistryDurableReplaceHostsRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	first := ports.BrokerHostRecord{Registration: registration(t, "host", 1), Pinned: true, Policy: poolPolicy()}
	readded := first
	readded.Registration = registration(t, "host", 2)
	readded.Pinned, readded.Learned = false, true
	readded.Policy.Trust = "new-trust"
	var previous []ports.BrokerHostRecord
	for i, stage := range []struct {
		name    string
		records []ports.BrokerHostRecord
	}{
		{"add", []ports.BrokerHostRecord{first}}, {"remove", nil}, {"re-add", []ports.BrokerHostRecord{readded}}, {"restart", []ports.BrokerHostRecord{readded}},
	} {
		t.Run(stage.name, func(t *testing.T) {
			store, err := brokerstore.OpenOffline(brokerstore.Options{Dir: dir})
			require.NoError(t, err)
			defer store.Close()
			r, err := NewRegistry(ports.BrokerEpoch(i+1), store, newTestProbe(1), newManualClock(time.Unix(100, 0)), nil)
			require.NoError(t, err)
			defer r.settle()
			require.Equal(t, previous, r.authority.Hosts)
			require.Len(t, r.Snapshot().Hosts, len(previous))
			if len(previous) > 0 {
				require.True(t, r.Snapshot().Hosts[0].InventoryKnown, "advisory observation must also survive restart")
			}
			require.NoError(t, r.ReplaceHosts(stage.records))
			authority, err := store.LoadHosts()
			require.NoError(t, err)
			require.Equal(t, r.authority, authority)
			require.Equal(t, stage.records, authority.Hosts)
			require.Len(t, r.Snapshot().Hosts, len(stage.records))
			if len(stage.records) > 0 {
				reg := stage.records[0].Registration
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				r.inflight[reg.Endpoint] = &probeAttempt{token: 1, cancel: cancel}
				r.apply(probeResult{endpoint: reg.Endpoint, registration: reg, attempt: 1, at: r.clock.Now(), snapshot: ports.RemoteHostSnapshot{Availability: domain.RemoteAvailabilityReachable, InventoryKnown: true}})
				require.ErrorIs(t, ctx.Err(), context.Canceled)
			}
			r.settle() // flush before inspecting persisted projection
			snapshot, err := store.Load()
			require.NoError(t, err)
			require.Equal(t, durable(r.Snapshot()), snapshot)
			require.ErrorIs(t, r.ReplaceHosts(stage.records), ports.ErrBrokerRegistryClosed, "shutdown must not acknowledge mutations")
			previous = stage.records
		})
	}
}

func TestRegistryDurableConflictDoesNotMutateProjection(t *testing.T) {
	for _, mode := range []string{"stale revision", "policy without new generation"} {
		t.Run(mode, func(t *testing.T) {
			store, err := brokerstore.OpenOffline(brokerstore.Options{Dir: filepath.Join(t.TempDir(), "broker")})
			require.NoError(t, err)
			defer store.Close()
			record := ports.BrokerHostRecord{Registration: registration(t, "host", 1), Pinned: true, Policy: poolPolicy()}
			require.NoError(t, store.ReplaceHosts(1, []ports.BrokerHostRecord{record}))
			r, err := NewRegistry(1, store, newTestProbe(1), newManualClock(time.Unix(100, 0)), nil)
			require.NoError(t, err)
			defer r.settle()
			before := r.Snapshot()
			authority := r.authority
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			r.inflight[record.Registration.Endpoint] = &probeAttempt{token: 1, cancel: cancel}
			if mode == "stale revision" {
				// Simulate an illicit second mutator. Even identical local records must
				// CAS rather than returning success from a projection-only no-op.
				require.NoError(t, store.ReplaceHosts(authority.Revision, nil))
			} else {
				record.Policy.Trust = "changed-trust"
			}
			require.ErrorIs(t, r.ReplaceHosts([]ports.BrokerHostRecord{record}), ports.ErrBrokerHostConflict)
			require.Equal(t, before, r.Snapshot())
			require.Equal(t, authority, r.authority)
			require.NoError(t, ctx.Err(), "failed CAS must not cancel the current observation")
		})
	}
}

func TestRegistryReplaceHostsStoreFailureIsSynchronous(t *testing.T) {
	store := portsmocks.NewMockBrokerHostStore(t)
	store.EXPECT().Load().Return(ports.BrokerSnapshot{}, nil).Once()
	store.EXPECT().LoadHosts().Return(ports.BrokerHosts{Revision: 7}, nil).Once()
	record := ports.BrokerHostRecord{Registration: registration(t, "host", 1), Pinned: true, Policy: poolPolicy()}
	records := []ports.BrokerHostRecord{record}
	store.EXPECT().ReplaceHosts(uint64(7), records).Return(context.DeadlineExceeded).Once()
	r, err := NewRegistry(1, store, newTestProbe(1), newManualClock(time.Unix(100, 0)), nil)
	require.NoError(t, err)
	before := r.Snapshot()
	require.ErrorIs(t, r.ReplaceHosts(records), context.DeadlineExceeded)
	require.Equal(t, before, r.Snapshot())
	require.Equal(t, uint64(7), r.authority.Revision)
	require.Error(t, r.ReplaceHosts([]ports.BrokerHostRecord{{Registration: record.Registration}}))
	require.Equal(t, before, r.Snapshot())
}

// TestRegistryReplaceHostsAfterShutdownIsTyped pins the closed sentinel: once
// the run has settled, a membership mutation is refused with a typed error, so
// a caller can tell shutdown apart from a store failure without matching text.
func TestRegistryReplaceHostsAfterShutdownIsTyped(t *testing.T) {
	store := newTestStore()
	r := newTestRegistry(t, 1, store, newTestProbe(1), newManualClock(time.Unix(100, 0)))
	record := ports.BrokerHostRecord{Registration: registration(t, "host", 1), Pinned: true, Policy: poolPolicy()}
	require.NoError(t, r.setHosts([]domain.RemoteRegistration{record.Registration}))
	r.settle()

	before := r.Snapshot()
	authority := r.authority
	err := r.ReplaceHosts([]ports.BrokerHostRecord{record})
	require.ErrorIs(t, err, ports.ErrBrokerRegistryClosed)
	require.NotErrorIs(t, err, ports.ErrBrokerRevisionExhausted)
	require.Equal(t, before, r.Snapshot())
	require.Equal(t, authority, r.authority)
}

// Queued pre-CAS observations may be rejected as stale by the real store, but
// the newest post-CAS publication must always flush; churn cannot leave the
// registry permanently publishing membership that the store will reject.
func TestRegistryDurableChurnFlushesLatestMembership(t *testing.T) {
	store, err := brokerstore.OpenOffline(brokerstore.Options{Dir: filepath.Join(t.TempDir(), "broker")})
	require.NoError(t, err)
	defer store.Close()
	r, err := NewRegistry(1, store, newTestProbe(1), newManualClock(time.Unix(100, 0)), nil)
	require.NoError(t, err)
	defer r.settle()
	for i := byte(1); i <= 20; i++ {
		record := ports.BrokerHostRecord{Registration: registration(t, "host", i), Pinned: true, Policy: poolPolicy()}
		require.NoError(t, r.ReplaceHosts([]ports.BrokerHostRecord{record}))
		// A policy change is admitted only with a new registration generation.
		record.Registration.Generation++
		record.Policy.Trust = "replacement-trust"
		require.NoError(t, r.ReplaceHosts([]ports.BrokerHostRecord{record}))
		require.Equal(t, record.Registration, r.Snapshot().Hosts[0].Registration)
		if i < 20 {
			require.NoError(t, r.ReplaceHosts(nil))
		}
	}
	r.settle()
	snapshot, err := store.Load()
	require.NoError(t, err)
	require.Equal(t, durable(r.Snapshot()), snapshot)
	authority, err := store.LoadHosts()
	require.NoError(t, err)
	require.Equal(t, r.authority, authority)
}
