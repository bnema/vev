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

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
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

func options(t *testing.T) Options {
	t.Helper()
	return Options{Dir: filepath.Join(t.TempDir(), "broker"), LegacyHosts: "testdata/hosts-v3.json", LegacyCache: "testdata/cache-v4.json", Policies: map[string]ports.BrokerPolicy{"user@arch": testPolicy()}}
}
func TestPriorFixtures(t *testing.T) {
	for h := 1; h <= 3; h++ {
		for c := 2; c <= 4; c++ {
			t.Run(fmt.Sprintf("hosts%d-cache%d", h, c), func(t *testing.T) {
				o := options(t)
				o.LegacyHosts = fmt.Sprintf("testdata/hosts-v%d.json", h)
				o.LegacyCache = fmt.Sprintf("testdata/cache-v%d.json", c)
				beforeH, err := os.ReadFile(o.LegacyHosts)
				require.NoError(t, err)
				beforeC, err := os.ReadFile(o.LegacyCache)
				require.NoError(t, err)
				s, err := OpenOffline(o)
				require.NoError(t, err)
				hosts, err := s.LoadHosts()
				require.NoError(t, err)
				require.Len(t, hosts.Hosts, 1)
				require.True(t, hosts.Hosts[0].Learned)
				require.Equal(t, h != 1, hosts.Hosts[0].Pinned)
				require.Equal(t, o.Policies["user@arch"], hosts.Hosts[0].Policy)
				if h == 3 {
					require.Equal(t, domain.RemoteGeneration(7), hosts.Hosts[0].Registration.Generation)
				}
				snap, err := s.Load()
				require.NoError(t, err)
				if h == 3 && c == 4 {
					require.Len(t, snap.Daemons, 1)
					require.Len(t, snap.Daemons[0].Sessions, 1)
					require.Equal(t, "t_work", snap.Daemons[0].Sessions[0].Tabs[0].ID)
				} else {
					require.Empty(t, snap.Daemons)
				}
				require.NoError(t, s.Close())
				o.LegacyHosts = "missing"
				o.LegacyCache = "missing"
				o.Policies = nil
				s, err = OpenOffline(o)
				require.NoError(t, err)
				defer s.Close()
				again, err := s.LoadHosts()
				require.NoError(t, err)
				require.Equal(t, hosts, again)
				raw, err := readBounded(filepath.Join(o.Dir, "recovery.json"))
				require.NoError(t, err)
				var r recovery
				require.NoError(t, strict(raw, &r))
				require.Equal(t, beforeH, r.Hosts)
				require.Equal(t, beforeC, r.Cache)
				for _, name := range []string{"state.json", "recovery.json", lockFileName} {
					st, err := os.Stat(filepath.Join(o.Dir, name))
					require.NoError(t, err)
					require.Equal(t, os.FileMode(0600), st.Mode().Perm())
				}
			})
		}
	}
}
func TestMigrationFaultRestart(t *testing.T) {
	for _, file := range []string{"recovery.json", "state.json"} {
		for _, point := range []string{"write", "sync", "rename", "dirsync"} {
			t.Run(file+point, func(t *testing.T) {
				o := options(t)
				o.LegacyHosts = "testdata/hosts-v2.json"
				fault := errors.New("power loss")
				o.Fault = func(p string) error {
					if p == file+":"+point {
						return fault
					}
					return nil
				}
				s, err := OpenOffline(o)
				require.ErrorIs(t, err, fault)
				require.Nil(t, s)
				var saved recovery
				raw, e := readBounded(filepath.Join(o.Dir, "recovery.json"))
				if e == nil {
					require.NoError(t, strict(raw, &saved))
				}
				o.Fault = nil
				s, err = OpenOffline(o)
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
		s, err := OpenOffline(Options{Dir: dir})
		if s != nil {
			s.Close()
		}
		if !errors.Is(err, ErrLocked) {
			os.Exit(2)
		}
		return
	}
	o := options(t)
	s, err := OpenOffline(o)
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
			other, e := OpenOffline(o)
			if other != nil {
				_ = other.Close()
				outcomes[i] = fmt.Errorf("concurrent OpenOffline returned a store: %w", ports.ErrBrokerStoreLocked)
				return
			}
			if !errors.Is(e, ErrLocked) || !errors.Is(e, ports.ErrBrokerStoreLocked) {
				outcomes[i] = fmt.Errorf("concurrent OpenOffline error = %v, want ErrLocked", e)
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
	s, err = OpenOffline(o)
	require.NoError(t, err)
	require.NoError(t, s.Close())
}

// TestLockExcludesLegacyHostWriter holds the lock the way the legacy host
// writer does and proves the store refuses to own the same directory.
func TestLockExcludesLegacyHostWriter(t *testing.T) {
	o := options(t)
	s, err := OpenOffline(o)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	legacyLock, err := os.OpenFile(filepath.Join(o.Dir, "hosts.json.lock"), os.O_CREATE|os.O_RDWR, 0600)
	require.NoError(t, err)
	defer legacyLock.Close()
	require.NoError(t, syscall.Flock(int(legacyLock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB))

	_, err = OpenOffline(o)
	require.ErrorIs(t, err, ErrLocked)
	require.ErrorIs(t, err, ports.ErrBrokerStoreLocked)

	require.NoError(t, syscall.Flock(int(legacyLock.Fd()), syscall.LOCK_UN))
	s, err = OpenOffline(o)
	require.NoError(t, err)
	require.NoError(t, s.Close())
}
func TestStrictCorruption(t *testing.T) {
	for _, raw := range []string{`{"version":1,"hosts":[]}`, `{"version":3,"hosts":[]}`} {
		var v any
		require.NoError(t, strict([]byte(raw), &v))
	}
	for name, raw := range map[string]string{"duplicate": `{"version":1,"version":1,"hosts":[]}`, "unknown": `{"version":1,"hosts":[],"extra":0}`, "trailing": `{"version":1,"hosts":[]} {}`, "missing": `{"version":2,"pinned":[]}`, "future": `{"version":99,"hosts":[]}`, "utf8": string([]byte{255}), "oversize": strings.Repeat(" ", MaxFileBytes+1), "empty": ""} {
		t.Run(name, func(t *testing.T) {
			o := options(t)
			o.LegacyHosts = filepath.Join(t.TempDir(), "hosts")
			require.NoError(t, os.WriteFile(o.LegacyHosts, []byte(raw), 0600))
			s, err := OpenOffline(o)
			require.Error(t, err)
			require.Nil(t, s)
			_, err = os.Stat(filepath.Join(o.Dir, "state.json"))
			require.True(t, os.IsNotExist(err))
		})
	}
	t.Run("no implicit policy", func(t *testing.T) { o := options(t); o.Policies = nil; _, err := OpenOffline(o); require.Error(t, err) })
	t.Run("state never falls back", func(t *testing.T) {
		o := options(t)
		s, err := OpenOffline(o)
		require.NoError(t, err)
		require.NoError(t, s.Close())
		require.NoError(t, os.WriteFile(filepath.Join(o.Dir, "state.json"), []byte(`{`), 0600))
		_, err = OpenOffline(o)
		require.Error(t, err)
	})
	t.Run("source symlink", func(t *testing.T) {
		o := options(t)
		p := filepath.Join(t.TempDir(), "link")
		require.NoError(t, os.Symlink(o.LegacyHosts, p))
		o.LegacyHosts = p
		_, err := OpenOffline(o)
		require.Error(t, err)
	})
}
func TestDurableStateSentinelMapping(t *testing.T) {
	o := options(t)
	s, err := OpenOffline(o)
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
	_, err = OpenOffline(o)
	require.ErrorIs(t, err, ErrInvalidState)
	require.ErrorIs(t, err, ports.ErrBrokerStoreInvalidState)
}

// TestNonRegularInputsAreRejected covers inputs that are neither regular files
// nor absent: a directory and a FIFO must both fail closed before decoding.
func TestNonRegularInputsAreRejected(t *testing.T) {
	for _, name := range []string{"directory", "fifo"} {
		t.Run(name, func(t *testing.T) {
			o := options(t)
			path := filepath.Join(t.TempDir(), name)
			if name == "directory" {
				require.NoError(t, os.Mkdir(path, 0700))
			} else {
				require.NoError(t, syscall.Mkfifo(path, 0600))
			}
			o.LegacyHosts = path
			s, err := OpenOffline(o)
			require.Error(t, err)
			require.Nil(t, s)
			_, err = os.Stat(filepath.Join(o.Dir, "state.json"))
			require.True(t, os.IsNotExist(err))
		})
	}
}

// TestCommittedStateAndRecoveryAreNeverFollowed replaces the committed state and
// the recovery record with symlinks to valid bytes: the store reads both with
// O_NOFOLLOW, so a symlink fails closed instead of redirecting a rename or
// silently adopting attacker-chosen state.
func TestCommittedStateAndRecoveryAreNeverFollowed(t *testing.T) {
	t.Run("state", func(t *testing.T) {
		o := options(t)
		s, err := OpenOffline(o)
		require.NoError(t, err)
		require.NoError(t, s.Close())

		state := filepath.Join(o.Dir, "state.json")
		raw, err := os.ReadFile(state)
		require.NoError(t, err)
		target := filepath.Join(o.Dir, "elsewhere.json")
		require.NoError(t, os.WriteFile(target, raw, 0600))
		require.NoError(t, os.Remove(state))
		require.NoError(t, os.Symlink(target, state))

		_, err = OpenOffline(o)
		require.ErrorIs(t, err, ErrInvalidState)
		require.ErrorIs(t, err, ports.ErrBrokerStoreInvalidState)
		after, err := os.ReadFile(target)
		require.NoError(t, err)
		require.Equal(t, raw, after)
	})
	t.Run("recovery", func(t *testing.T) {
		o := options(t)
		s, err := OpenOffline(o)
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

		_, err = OpenOffline(o)
		require.ErrorIs(t, err, ErrInvalidState)
		require.ErrorIs(t, err, ports.ErrBrokerStoreInvalidState)
		_, err = os.Stat(filepath.Join(o.Dir, "state.json"))
		require.True(t, os.IsNotExist(err))
	})
}

// TestManifestDistinguishesAbsentFromEmptySources pins the recovery manifest: an
// absent source and a supplied source that carries no records can produce the
// same membership, so presence, keyed by generic role label, is what makes the
// recovery record an exact description of the migration input.
func TestManifestDistinguishesAbsentFromEmptySources(t *testing.T) {
	readManifest := func(t *testing.T, dir string) (manifest, string) {
		t.Helper()
		raw, err := readBounded(filepath.Join(dir, "recovery.json"))
		require.NoError(t, err)
		var r recovery
		require.NoError(t, strict(raw, &r))
		return r.State.Manifest, digest(nil)
	}

	absent := options(t)
	absent.LegacyHosts = ""
	absent.LegacyCache = ""
	absent.Policies = nil
	absentStore, err := OpenOffline(absent)
	require.NoError(t, err)
	defer absentStore.Close()
	hosts, err := absentStore.LoadHosts()
	require.NoError(t, err)
	require.Empty(t, hosts.Hosts)
	absentManifest, emptyDigest := readManifest(t, absent.Dir)
	for _, label := range []manifestSourceLabel{manifestMembership, manifestObservations} {
		source, ok := absentManifest.source(label)
		require.True(t, ok, label)
		require.False(t, source.Present, label)
		require.Equal(t, emptyDigest, source.SHA256, label)
	}

	empty := options(t)
	empty.LegacyCache = ""
	empty.LegacyHosts = filepath.Join(t.TempDir(), "hosts")
	require.NoError(t, os.WriteFile(empty.LegacyHosts, []byte(`{"version":1,"hosts":[]}`), 0600))
	emptyStore, err := OpenOffline(empty)
	require.NoError(t, err)
	defer emptyStore.Close()
	stillEmpty, err := emptyStore.LoadHosts()
	require.NoError(t, err)
	// The same empty membership results, but the manifest records that the
	// membership source was supplied and what it contained.
	require.Equal(t, hosts, stillEmpty)
	emptyManifest, _ := readManifest(t, empty.Dir)
	membership, ok := emptyManifest.source(manifestMembership)
	require.True(t, ok)
	require.True(t, membership.Present)
	require.NotEqual(t, emptyDigest, membership.SHA256)
	observations, ok := emptyManifest.source(manifestObservations)
	require.True(t, ok)
	require.False(t, observations.Present)

	// A record that claims the source was absent while retaining its bytes is
	// rejected: presence is verified, not merely recorded.
	raw, err := readBounded(filepath.Join(empty.Dir, "recovery.json"))
	require.NoError(t, err)
	var r recovery
	require.NoError(t, strict(raw, &r))
	for i := range r.State.Manifest.Sources {
		if r.State.Manifest.Sources[i].Label == manifestMembership {
			r.State.Manifest.Sources[i].Present = false
		}
	}
	writer := Store{dir: empty.Dir}
	require.NoError(t, writer.write("recovery.json", r))
	require.NoError(t, os.Remove(filepath.Join(empty.Dir, "state.json")))
	require.NoError(t, emptyStore.Close())
	_, err = OpenOffline(Options{Dir: empty.Dir})
	require.ErrorIs(t, err, ErrInvalidState)
	require.ErrorContains(t, err, "absent membership source")
}

// TestRevisionOverflowIsRefused keeps the store from ever minting revision zero
// or accepting an exhausted authority token.
func TestRevisionOverflowIsRefused(t *testing.T) {
	o := options(t)
	s, err := OpenOffline(o)
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
	s, err := OpenOffline(o)
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
	s, err := OpenOffline(o)
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
	s, err := OpenOffline(o)
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

	s, err = OpenOffline(o)
	require.NoError(t, err)
	defer s.Close()
	reopened, err := s.Load()
	require.NoError(t, err)
	require.Empty(t, reopened.Removed)
}

func TestFencingAndCommitFaults(t *testing.T) {
	for _, point := range []string{"write", "sync", "rename", "dirsync"} {
		t.Run(point, func(t *testing.T) {
			o := options(t)
			s, err := OpenOffline(o)
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
			s, err = OpenOffline(o)
			require.NoError(t, err)
			defer s.Close()
			h, err = s.LoadHosts()
			require.NoError(t, err)
			if point == "rename" || point == "dirsync" {
				require.Empty(t, h.Hosts)
				snap.Revision++
				require.ErrorIs(t, s.Store(snap), ErrStale)
			} else {
				require.Len(t, h.Hosts, 1)
			}
		})
	}
	t.Run("generation and restart", func(t *testing.T) {
		o := options(t)
		s, err := OpenOffline(o)
		require.NoError(t, err)
		h, err := s.LoadHosts()
		require.NoError(t, err)
		snap, err := s.Load()
		require.NoError(t, err)
		h.Hosts[0].Registration.Generation++
		require.NoError(t, s.ReplaceHosts(h.Revision, h.Hosts))
		require.ErrorIs(t, s.Store(snap), ErrStale)
		require.NoError(t, s.Close())
		s, err = OpenOffline(o)
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
	s, err := OpenOffline(options(t))
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
