package brokerstore

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
	"github.com/stretchr/testify/require"
)

func testPolicy() ports.BrokerPolicy {
	return ports.BrokerPolicy{ProtocolVersion: 1, CatalogSchemaVersion: 1, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned, Transport: "quic", Trust: "known-hosts", Launch: "explicit", Isolation: "user"}
}

// observation builds a remote daemon observation that satisfies the durable
// shape: an exact registration, a derived display origin, and a reachable
// availability. Policy is intentionally left absent: the loader stamps it from
// membership, never from the durable snapshot.
func observation(endpoint string, registration domain.RemoteRegistration) ports.BrokerDaemonObservation {
	return ports.BrokerDaemonObservation{
		Endpoint:      endpoint,
		DisplayOrigin: domain.RemoteDisplayOrigin(endpoint),
		Registration:  registration,
		Availability:  domain.RemoteAvailabilityReachable,
	}
}

func testHostRecord() ports.BrokerHostRecord {
	reg := domain.RemoteRegistration{Endpoint: "user@arch", Incarnation: [16]byte{1}, Generation: 7}
	return ports.BrokerHostRecord{Registration: reg, Learned: true, Pinned: true, Policy: testPolicy(), Route: ports.BrokerRouteSpec{Kind: ports.BrokerRouteSSHQUIC, Target: reg.Endpoint, Argv: []string{"vev", "_broker-mux-quic-bootstrap", "--production"}}}
}

func options(t *testing.T) Options {
	t.Helper()
	return Options{Dir: filepath.Join(t.TempDir(), "broker"), InitialHosts: []ports.BrokerHostRecord{testHostRecord()}, InitialImportProvided: true}
}
func TestMigrationFaultRestart(t *testing.T) {
	for _, file := range []string{"recovery.json", "state.json"} {
		for _, point := range []string{"write", "sync", "rename", "dirsync"} {
			t.Run(file+point, func(t *testing.T) {
				o := options(t)
				fault := errors.New("power loss")
				o.Fault = func(p string) error {
					if p == file+":"+point {
						return fault
					}
					return nil
				}
				s, err := Open(o)
				require.ErrorIs(t, err, fault)
				require.Nil(t, s)
				var saved recovery
				raw, e := readBounded(filepath.Join(o.Dir, "recovery.json"))
				if e == nil {
					require.NoError(t, strict(raw, &saved))
				}
				o.Fault = nil
				s, err = Open(o)
				require.NoError(t, err)
				defer s.Close()
				hosts, err := s.LoadHosts()
				require.NoError(t, err)
				if e == nil {
					require.Equal(t, saved.State.Hosts, hosts)
				}
			})
		}
	}
}
func TestLockLifetime(t *testing.T) {
	if dir := os.Getenv("VEV_BROKERSTORE_LOCK_TEST"); dir != "" {
		s, err := Open(Options{Dir: dir})
		if s != nil {
			s.Close()
		}
		if !errors.Is(err, ErrLocked) {
			os.Exit(2)
		}
		return
	}
	o := options(t)
	s, err := Open(o)
	require.NoError(t, err)
	// The lifetime lock carries the legacy host writer's exact file name, so a
	// store opened beside a live hosts.json excludes that writer too.
	_, err = os.Stat(filepath.Join(o.Dir, "hosts.json.lock"))
	require.NoError(t, err)
	cmd := exec.Command(os.Args[0], "-test.run=^TestLockLifetime$")
	cmd.Env = append(os.Environ(), "VEV_BROKERSTORE_LOCK_TEST="+o.Dir)
	require.NoError(t, cmd.Run())
	// Assertions never run inside the competing goroutines: t.FailNow from a
	// non-test goroutine is invalid, so each worker records its outcome and the
	// test body checks them after the join.
	var wg sync.WaitGroup
	outcomes := make([]error, 8)
	for i := 0; i < len(outcomes); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			other, e := Open(o)
			if other != nil {
				_ = other.Close()
				outcomes[i] = fmt.Errorf("concurrent Open returned a store: %w", ports.ErrBrokerStoreLocked)
				return
			}
			if !errors.Is(e, ErrLocked) || !errors.Is(e, ports.ErrBrokerStoreLocked) {
				outcomes[i] = fmt.Errorf("concurrent Open error = %v, want ErrLocked", e)
			}
		}(i)
	}
	wg.Wait()
	for i, outcome := range outcomes {
		require.NoError(t, outcome, "worker %d", i)
	}
	require.NoError(t, s.Close())
	_, err = s.Load()
	require.Error(t, err)
	s, err = Open(o)
	require.NoError(t, err)
	require.NoError(t, s.Close())
}

// TestLockExcludesLegacyHostWriter holds the lock the way the legacy host
// writer does and proves the store refuses to own the same directory.
func TestLockExcludesLegacyHostWriter(t *testing.T) {
	o := options(t)
	s, err := Open(o)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	legacyLock, err := os.OpenFile(filepath.Join(o.Dir, "hosts.json.lock"), os.O_CREATE|os.O_RDWR, 0600)
	require.NoError(t, err)
	defer legacyLock.Close()
	require.NoError(t, syscall.Flock(int(legacyLock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB))

	_, err = Open(o)
	require.ErrorIs(t, err, ErrLocked)
	require.ErrorIs(t, err, ports.ErrBrokerStoreLocked)

	require.NoError(t, syscall.Flock(int(legacyLock.Fd()), syscall.LOCK_UN))
	s, err = Open(o)
	require.NoError(t, err)
	require.NoError(t, s.Close())
}
func TestStrictCorruption(t *testing.T) {
	for _, raw := range []string{`{"version":1,"version":1}`, `{"unknown":1}`, `{} {`, string([]byte{255}), strings.Repeat(" ", MaxFileBytes+1)} {
		var v state
		require.Error(t, strict([]byte(raw), &v))
	}
	o := options(t)
	s, err := Open(o)
	require.NoError(t, err)
	require.NoError(t, s.Close())
	require.NoError(t, os.WriteFile(filepath.Join(o.Dir, "state.json"), []byte(`{`), 0600))
	_, err = Open(o)
	require.Error(t, err)
}
func TestDurableStateSentinelMapping(t *testing.T) {
	o := options(t)
	s, err := Open(o)
	require.NoError(t, err)

	// A conflicting authority token is a host conflict, not a generic failure.
	require.ErrorIs(t, s.ReplaceHosts(0, nil), ErrStale)
	require.ErrorIs(t, s.ReplaceHosts(0, nil), ports.ErrBrokerHostConflict)

	// A locked, corrupt, or closed store is classified without importing the
	// adapter: callers retry, resolve state offline, or reopen respectively.
	require.ErrorIs(t, ErrLocked, ports.ErrBrokerStoreLocked)
	require.ErrorIs(t, ErrInvalidState, ports.ErrBrokerStoreInvalidState)

	require.NoError(t, s.Close())
	// Closing releases ownership without discarding the durable files.
	require.Error(t, s.ReplaceHosts(1, nil))
	require.NoError(t, os.WriteFile(filepath.Join(o.Dir, "state.json"), []byte(`{`), 0600))
	_, err = Open(o)
	require.ErrorIs(t, err, ErrInvalidState)
	require.ErrorIs(t, err, ports.ErrBrokerStoreInvalidState)
}

// TestCommittedStateAndRecoveryAreNeverFollowed replaces the committed state and
// the recovery record with symlinks to valid bytes: the store reads both with
// O_NOFOLLOW, so a symlink fails closed instead of redirecting a rename or
// silently adopting attacker-chosen state.
func TestCommittedStateAndRecoveryAreNeverFollowed(t *testing.T) {
	t.Run("state", func(t *testing.T) {
		o := options(t)
		s, err := Open(o)
		require.NoError(t, err)
		require.NoError(t, s.Close())

		state := filepath.Join(o.Dir, "state.json")
		raw, err := os.ReadFile(state)
		require.NoError(t, err)
		target := filepath.Join(o.Dir, "elsewhere.json")
		require.NoError(t, os.WriteFile(target, raw, 0600))
		require.NoError(t, os.Remove(state))
		require.NoError(t, os.Symlink(target, state))

		_, err = Open(o)
		require.ErrorIs(t, err, ErrInvalidState)
		require.ErrorIs(t, err, ports.ErrBrokerStoreInvalidState)
		after, err := os.ReadFile(target)
		require.NoError(t, err)
		require.Equal(t, raw, after)
	})
	t.Run("recovery", func(t *testing.T) {
		o := options(t)
		s, err := Open(o)
		require.NoError(t, err)
		require.NoError(t, s.Close())

		recoveryPath := filepath.Join(o.Dir, "recovery.json")
		raw, err := os.ReadFile(recoveryPath)
		require.NoError(t, err)
		target := filepath.Join(o.Dir, "elsewhere.json")
		require.NoError(t, os.WriteFile(target, raw, 0600))
		require.NoError(t, os.Remove(recoveryPath))
		require.NoError(t, os.Remove(filepath.Join(o.Dir, "state.json")))
		require.NoError(t, os.Symlink(target, recoveryPath))

		_, err = Open(o)
		require.ErrorIs(t, err, ErrInvalidState)
		require.ErrorIs(t, err, ports.ErrBrokerStoreInvalidState)
		_, err = os.Stat(filepath.Join(o.Dir, "state.json"))
		require.True(t, os.IsNotExist(err))
	})
}

func TestRevisionOverflowIsRefused(t *testing.T) {
	o := options(t)
	s, err := Open(o)
	require.NoError(t, err)
	defer s.Close()
	h, err := s.LoadHosts()
	require.NoError(t, err)
	for _, hosts := range [][]ports.BrokerHostRecord{h.Hosts, nil} {
		require.ErrorIs(t, s.ReplaceHosts(^uint64(0), hosts), ErrStale)
	}
	after, err := s.LoadHosts()
	require.NoError(t, err)
	require.Equal(t, h, after)
}

// TestAuthorityGenerationPolicyAndRemoveReAdd exercises the durable authority
// rules: policy changes need a fresh generation, generations never regress, a
// removed incarnation's observation never returns, and a re-add carries a new
// incarnation.
func TestAuthorityGenerationPolicyAndRemoveReAdd(t *testing.T) {
	o := options(t)
	s, err := Open(o)
	require.NoError(t, err)
	defer s.Close()
	h, err := s.LoadHosts()
	require.NoError(t, err)
	require.Len(t, h.Hosts, 1)
	base := h.Hosts[0]

	// A policy change with an unchanged registration identity is refused: an
	// already queued observation must never be accepted under changed trust.
	changed := append([]ports.BrokerHostRecord(nil), h.Hosts...)
	changed[0].Policy.Trust = "changed"
	require.ErrorIs(t, s.ReplaceHosts(h.Revision, changed), ErrStale)

	// Re-supplying identical membership is allowed and still advances the
	// authority revision.
	require.NoError(t, s.ReplaceHosts(h.Revision, h.Hosts))
	h2, err := s.LoadHosts()
	require.NoError(t, err)
	require.Equal(t, h.Revision+1, h2.Revision)

	// The same policy change with a bumped generation is accepted and durable.
	changed[0].Registration.Generation++
	require.NoError(t, s.ReplaceHosts(h2.Revision, changed))
	h3, err := s.LoadHosts()
	require.NoError(t, err)
	require.Equal(t, "changed", h3.Hosts[0].Policy.Trust)

	// A regressed generation on the same incarnation is refused.
	regressed := append([]ports.BrokerHostRecord(nil), h3.Hosts...)
	regressed[0].Registration.Generation = base.Registration.Generation
	require.ErrorIs(t, s.ReplaceHosts(h3.Revision, regressed), ErrStale)

	// Full removal drops the observation with the registration.
	require.NoError(t, s.ReplaceHosts(h3.Revision, nil))
	removed, err := s.LoadHosts()
	require.NoError(t, err)
	require.Empty(t, removed.Hosts)

	// A queued publication from the retired incarnation is refused even though
	// its revision is newer.
	require.ErrorIs(t, s.Store(ports.BrokerSnapshot{Epoch: 9, Revision: 1, Daemons: []ports.BrokerDaemonObservation{observation(base.Registration.Endpoint, base.Registration)}}), ErrStale)

	// Re-adding the endpoint needs a fresh incarnation; the retired
	// observation never comes back for it.
	var incarnation [16]byte
	incarnation[0] = 9
	registration, err := domain.NewRemoteRegistration(base.Registration.Endpoint, incarnation)
	require.NoError(t, err)
	reAdd := append([]ports.BrokerHostRecord(nil), base)
	reAdd[0].Registration = registration
	require.NoError(t, s.ReplaceHosts(removed.Revision, reAdd))
	readded, err := s.LoadHosts()
	require.NoError(t, err)
	require.True(t, readded.Hosts[0].Registration.Equal(registration))
	snapshot, err := s.Load()
	require.NoError(t, err)
	_, ok := snapshot.Find(base.Registration.Endpoint)
	require.False(t, ok, "a removed incarnation's observation must not survive a re-add")
}

// TestReplaceHostsRejectsCallerInputBeforeCAS proves malformed caller
// membership is rejected as a caller error before the compare-and-swap, never
// as ErrBrokerStoreInvalidState: the durable state is a victim, not the cause.
func TestReplaceHostsRejectsCallerInputBeforeCAS(t *testing.T) {
	o := options(t)
	s, err := Open(o)
	require.NoError(t, err)
	defer s.Close()
	h, err := s.LoadHosts()
	require.NoError(t, err)
	require.Len(t, h.Hosts, 1)
	base := h.Hosts[0]

	invalid := map[string][]ports.BrokerHostRecord{
		"zero registration": {{Pinned: true, Policy: base.Policy}},
		"zero policy":       {{Registration: base.Registration, Pinned: true}},
		"unanchored":        {{Registration: base.Registration, Policy: base.Policy}},
		"duplicate endpoint": {
			{Registration: base.Registration, Pinned: true, Policy: base.Policy},
			{Registration: base.Registration, Learned: true, Policy: base.Policy},
		},
	}
	for name, records := range invalid {
		t.Run(name, func(t *testing.T) {
			// The CAS token matches, so a CAS-first store would have accepted the
			// malformed set and failed later as invalid durable state.
			err := s.ReplaceHosts(h.Revision, records)
			require.Error(t, err)
			require.NotErrorIs(t, err, ErrStale)
			require.NotErrorIs(t, err, ErrInvalidState)
			require.NotErrorIs(t, err, ports.ErrBrokerStoreInvalidState)
		})
	}
	t.Run("over bound", func(t *testing.T) {
		records := make([]ports.BrokerHostRecord, 0, ports.BrokerMaxHosts+1)
		for index := 0; index <= ports.BrokerMaxHosts; index++ {
			registration, err := domain.NewRemoteRegistration(fmt.Sprintf("bound-%d.test", index), [16]byte{byte(index + 1)})
			require.NoError(t, err)
			records = append(records, ports.BrokerHostRecord{Registration: registration, Pinned: true, Policy: base.Policy})
		}
		err := s.ReplaceHosts(h.Revision, records)
		require.Error(t, err)
		require.NotErrorIs(t, err, ports.ErrBrokerStoreInvalidState)
	})

	// Rejected input never commits: the authority revision and membership are
	// exactly as before.
	after, err := s.LoadHosts()
	require.NoError(t, err)
	require.Equal(t, h, after)
}

// TestDurableSnapshotDropsProcessLocalTombstones proves a snapshot's
// process-local tombstone set is sanitized before persistence: storing a
// snapshot whose tombstone names a live host is accepted and reopened without
// Removed, instead of being rejected as durable-state corruption.
func TestDurableSnapshotDropsProcessLocalTombstones(t *testing.T) {
	o := options(t)
	s, err := Open(o)
	require.NoError(t, err)
	h, err := s.LoadHosts()
	require.NoError(t, err)
	require.Len(t, h.Hosts, 1)
	reg := h.Hosts[0].Registration

	snap, err := s.Load()
	require.NoError(t, err)
	snap.Epoch = 5
	snap.Revision = 7
	snap.Removed = []ports.BrokerHostTombstone{{Endpoint: reg.Endpoint, Registration: reg, RetiredRevision: 6}}
	require.NoError(t, s.Store(snap))

	stored, err := s.Load()
	require.NoError(t, err)
	require.Empty(t, stored.Removed)
	require.NoError(t, s.Close())

	s, err = Open(o)
	require.NoError(t, err)
	defer s.Close()
	reopened, err := s.Load()
	require.NoError(t, err)
	require.Empty(t, reopened.Removed)
}

// TestStoreZeroesTabAttentionOnPersist pins C3: a tab's Attention (bell) is
// transient observation state, not durable identity, so it is zeroed before
// every durable write. Without this, a bell recorded the instant before a
// broker restart would resurrect on reload even though nothing is still
// ringing. This ports the intent of main's
// internal/adapters/remote/catalog_cache_test.go:71, which pins the same rule
// for the legacy remote monitor's cache (there, dynamic tab fields including
// Attention never reach the stored JSON at all).
func TestStoreZeroesTabAttentionOnPersist(t *testing.T) {
	tests := []struct {
		name string
		tabs []catalogue.RemoteCatalogTab
	}{
		{name: "single tab with attention", tabs: []catalogue.RemoteCatalogTab{
			{ID: "t1", Index: 0, Attention: true},
		}},
		{name: "mixed attention across tabs", tabs: []catalogue.RemoteCatalogTab{
			{ID: "t1", Index: 0, Attention: true},
			{ID: "t2", Index: 1},
			{ID: "t3", Index: 2, Attention: true},
		}},
		{name: "no attention stays false", tabs: []catalogue.RemoteCatalogTab{
			{ID: "t1", Index: 0},
			{ID: "t2", Index: 1},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := options(t)
			s, err := Open(o)
			require.NoError(t, err)
			h, err := s.LoadHosts()
			require.NoError(t, err)
			require.Len(t, h.Hosts, 1)
			reg := h.Hosts[0].Registration

			snap, err := s.Load()
			require.NoError(t, err)
			snap.Epoch = 9
			snap.Revision = 1
			daemon := observation(reg.Endpoint, reg)
			daemon.InventoryKnown = true
			daemon.LastSuccess = time.Unix(1, 0)
			daemon.Sessions = []catalogue.RemoteCatalogSession{{
				LifecycleID: domain.SessionLifecycleID{1},
				Name:        "work",
				State:       catalogue.RemoteCatalogSessionUp,
				Tabs:        tt.tabs,
			}}
			snap.Daemons = []ports.BrokerDaemonObservation{daemon}
			require.NoError(t, s.Store(snap))

			stored, err := s.Load()
			require.NoError(t, err)
			require.Len(t, stored.Daemons, 1)
			require.Len(t, stored.Daemons[0].Sessions, 1)
			require.Len(t, stored.Daemons[0].Sessions[0].Tabs, len(tt.tabs))
			for _, tab := range stored.Daemons[0].Sessions[0].Tabs {
				require.False(t, tab.Attention, "tab %q must never persist a live bell", tab.ID)
			}
			require.NoError(t, s.Close())

			// The zeroing is durable, not just an in-memory view: a fresh Open
			// reading the same file back must never resurrect the bell either.
			s, err = Open(o)
			require.NoError(t, err)
			defer s.Close()
			reopened, err := s.Load()
			require.NoError(t, err)
			require.Len(t, reopened.Daemons, 1)
			require.Len(t, reopened.Daemons[0].Sessions, 1)
			for _, tab := range reopened.Daemons[0].Sessions[0].Tabs {
				require.False(t, tab.Attention)
			}
		})
	}
}

func TestFencingAndCommitFaults(t *testing.T) {
	for _, point := range []string{"write", "sync", "rename", "dirsync"} {
		t.Run(point, func(t *testing.T) {
			o := options(t)
			s, err := Open(o)
			require.NoError(t, err)
			h, err := s.LoadHosts()
			require.NoError(t, err)
			snap, err := s.Load()
			require.NoError(t, err)
			snap.Epoch = 20
			snap.Revision = 1
			require.NoError(t, s.Store(snap))
			require.ErrorIs(t, s.Store(snap), ErrStale)
			other := snap
			other.Epoch = 21
			require.ErrorIs(t, s.Store(other), ErrStale)
			require.ErrorIs(t, s.ReplaceHosts(0, nil), ErrStale)
			changed := append([]ports.BrokerHostRecord(nil), h.Hosts...)
			changed[0].Policy.Trust = "new"
			require.ErrorIs(t, s.ReplaceHosts(h.Revision, changed), ErrStale)
			s.fault = func(p string) error {
				if p == "state.json:"+point {
					return errors.New("disk fault")
				}
				return nil
			}
			require.Error(t, s.ReplaceHosts(h.Revision, nil))
			_, err = s.LoadHosts()
			require.Error(t, err)
			require.NoError(t, s.Close())
			s, err = Open(o)
			require.NoError(t, err)
			defer s.Close()
			h, err = s.LoadHosts()
			require.NoError(t, err)
			if point == "rename" || point == "dirsync" {
				require.Empty(t, h.Hosts)
				snap.Revision++
				require.NoError(t, s.Store(snap))
			} else {
				require.Len(t, h.Hosts, 1)
			}
		})
	}
	t.Run("generation and restart", func(t *testing.T) {
		o := options(t)
		s, err := Open(o)
		require.NoError(t, err)
		h, err := s.LoadHosts()
		require.NoError(t, err)
		snap, err := s.Load()
		require.NoError(t, err)
		h.Hosts[0].Registration.Generation++
		require.NoError(t, s.ReplaceHosts(h.Revision, h.Hosts))
		require.ErrorIs(t, s.Store(snap), ErrStale)
		require.NoError(t, s.Close())
		s, err = Open(o)
		require.NoError(t, err)
		defer s.Close()
		got, err := s.Load()
		require.NoError(t, err)
		require.Empty(t, got.Daemons)
		gotH, err := s.LoadHosts()
		require.NoError(t, err)
		require.Equal(t, h.Hosts, gotH.Hosts)
	})
}

// TestLocalDaemonObservationIsNeverDurable pins the durable boundary: a local
// daemon observation is process-local state and the store refuses it outright,
// so it can never become durable authority.
func TestLocalDaemonObservationIsNeverDurable(t *testing.T) {
	s, err := Open(options(t))
	require.NoError(t, err)
	defer s.Close()
	err = s.Store(ports.BrokerSnapshot{Epoch: 9, Revision: 1, Daemons: []ports.BrokerDaemonObservation{{
		Local:         true,
		DisplayOrigin: "local",
		Policy:        testPolicy(),
		Availability:  domain.RemoteAvailabilityReachable,
	}}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "never durable")
}
