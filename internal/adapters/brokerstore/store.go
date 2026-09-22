// Package brokerstore provides broker persistence. Legacy sources are immutable inputs:
// the lifetime lock is the same hosts.json.lock the legacy host writer takes,
// so a live host writer is excluded, but the legacy catalog cache source has no
// lock at all and must be immutable or quiesced. No environment policy is
// inferred.
package brokerstore

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"syscall"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/safedir"
)

const MaxFileBytes = 16 << 20

// lockFileName is the lifetime lock of a durable broker store. The name is
// deliberately the one the legacy host writer takes beside hosts.json, so a
// store opened on a directory that writer also uses excludes it for the
// store's lifetime. The legacy catalog cache source is written without any
// lock, so that source must be immutable or quiesced before migration.
const lockFileName = "hosts.json.lock"

// ErrLocked reports that another owner already holds the lifetime lock.
var ErrLocked = fmt.Errorf("brokerstore: already owned: %w", ports.ErrBrokerStoreLocked)

// ErrStale reports that the caller's authority token no longer matches durable
// state, or that the supplied membership would regress or re-trust an existing
// registration without a fresh identity.
var ErrStale = fmt.Errorf("brokerstore: stale authority: %w", ports.ErrBrokerHostConflict)

// ErrInvalidState reports durable state that fails validation. The caller must
// resolve it offline from the recovery record; the store never replaces it.
var ErrInvalidState = fmt.Errorf("brokerstore: invalid state: %w", ports.ErrBrokerStoreInvalidState)

// invalid wraps one durable-state validation failure.
func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidState, fmt.Sprintf(format, args...))
}

// manifestSourceLabel names one immutable recovery input by its generic role:
// the membership source that seeds authority, and the observation source that
// only seeds advisory inventory. A label never names a legacy file or schema.
type manifestSourceLabel string

const (
	manifestMembership   manifestSourceLabel = "membership"
	manifestObservations manifestSourceLabel = "observations"
)

// manifestSource records one recovery input. Present distinguishes a source
// that was supplied but carried no records from a source that was never
// supplied: absence and emptiness can share a digest, so only presence makes
// the recovery record an exact description of the migration input.
type manifestSource struct {
	Label   manifestSourceLabel
	Present bool
	SHA256  string
}

type manifest struct {
	Version         int
	Sources         []manifestSource
	ImportVersion   int
	ImportCompleted bool
}

// source returns the manifest entry for one generic role label.
func (m manifest) source(label manifestSourceLabel) (manifestSource, bool) {
	for _, source := range m.Sources {
		if source.Label == label {
			return source, true
		}
	}
	return manifestSource{}, false
}

// Options names a private destination and immutable legacy inputs.
// Policies must explicitly cover every imported endpoint. Fault is a test seam
// called after each durable-write boundary; an error poisons this open handle.
type Options struct {
	Dir, LegacyHosts, LegacyCache string
	Policies                      map[string]ports.BrokerPolicy
	InitialHosts                  []ports.BrokerHostRecord
	InitialImportProvided         bool
	Fault                         func(string) error
}

type state struct {
	Manifest manifest
	Hosts    ports.BrokerHosts
	Snapshot ports.BrokerSnapshot
}
type recovery struct {
	State        state
	Hosts, Cache []byte
}
type Store struct {
	mu         sync.Mutex
	dir        string
	lock       *os.File
	state      state
	fault      func(string) error
	failed     bool
	writeEpoch ports.BrokerEpoch // first publication binds this lifetime handle
}

var _ ports.BrokerHostStore = (*Store)(nil)

// Open returns only after validated unified state is durable. The
// recovery record is written first and retained forever as the rollback copy;
// restart reuses its generated identities instead of migrating a second time.
// An existing corrupt state is an error, never silently replaced by a backup.
// Open acquires exclusive lifetime ownership, migrates immutable legacy input
// when needed, and returns the durable broker store.
func Open(o Options) (*Store, error) {
	if err := safedir.EnsurePrivate(o.Dir); err != nil {
		return nil, err
	}
	fd, err := syscall.Open(filepath.Join(o.Dir, lockFileName), syscall.O_CREAT|syscall.O_RDWR|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	lock := os.NewFile(uintptr(fd), lockFileName)
	info, err := lock.Stat()
	if err != nil {
		lock.Close()
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		lock.Close()
		return nil, invalid("unsafe owner lock")
	}
	if err = syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("%w: %v", ErrLocked, err)
	}
	s := &Store{dir: o.Dir, lock: lock, fault: o.Fault}
	ok := false
	defer func() {
		if !ok {
			s.Close()
		}
	}()
	raw, err := readBounded(filepath.Join(o.Dir, "state.json"))
	if err == nil {
		if err = strict(raw, &s.state); err != nil {
			return nil, invalid("state.json: %v", err)
		}
	} else if errors.Is(err, os.ErrNotExist) {
		var r recovery
		raw, err = readBounded(filepath.Join(o.Dir, "recovery.json"))
		if err == nil {
			if err = strict(raw, &r); err != nil {
				return nil, invalid("recovery.json: %v", err)
			}
			if err = r.verify(); err != nil {
				return nil, err
			}
		} else if errors.Is(err, os.ErrNotExist) {
			r, err = migrate(o)
			if err == nil {
				err = s.write("recovery.json", r)
			}
		} else {
			return nil, invalid("recovery.json: %v", err)
		}
		if err == nil {
			err = validate(r.State)
		}
		if err == nil {
			err = s.write("state.json", r.State)
		}
		s.state = r.State
	} else {
		return nil, invalid("state.json: %v", err)
	}
	if err != nil {
		return nil, err
	}
	upgraded, upgradeErr := ports.UpgradeBrokerHostRoutes(s.state.Hosts.Hosts)
	if upgradeErr != nil {
		return nil, upgradeErr
	}
	if !reflect.DeepEqual(upgraded, s.state.Hosts.Hosts) {
		next := s.state
		next.Hosts.Hosts = upgraded
		next.Manifest.ImportVersion = 1
		next.Manifest.ImportCompleted = true
		recovery := recovery{State: next}
		if err = s.write("recovery.json", recovery); err != nil {
			return nil, err
		}
		if err = s.commit(next); err != nil {
			return nil, err
		}
	} else if err = validate(s.state); err != nil {
		return nil, err
	}
	ok = true
	return s, nil
}

// verify reports whether the recovery record's retained bytes match their
// manifest entries, including each source's recorded presence: an absent
// source must retain no bytes, and a supplied source keeps its exact digest.
func (r recovery) verify() error {
	membership, ok := r.State.Manifest.source(manifestMembership)
	if !ok || digest(r.Hosts) != membership.SHA256 {
		return invalid("recovery digest mismatch")
	}
	observations, ok := r.State.Manifest.source(manifestObservations)
	if !ok || digest(r.Cache) != observations.SHA256 {
		return invalid("recovery digest mismatch")
	}
	for _, source := range []struct {
		label   manifestSourceLabel
		present bool
		bytes   int
	}{{manifestMembership, membership.Present, len(r.Hosts)}, {manifestObservations, observations.Present, len(r.Cache)}} {
		if !source.present && source.bytes != 0 {
			return invalid("recovery records an absent %s source with retained bytes", source.label)
		}
	}
	return nil
}
func digest(b []byte) string { sum := sha256.Sum256(b); return hex.EncodeToString(sum[:]) }
func (s *Store) ready() error {
	if s.failed {
		return ports.BrokerStoreOutcomeUnknownError{Err: errors.New("brokerstore: poisoned; reopen required")}
	}
	if s.lock == nil {
		return errors.New("brokerstore: closed")
	}
	return nil
}
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lock == nil {
		return nil
	}
	err := s.lock.Close()
	s.lock = nil
	return err
}
func (s *Store) LoadHosts() (ports.BrokerHosts, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ready(); err != nil {
		return ports.BrokerHosts{}, err
	}
	h := s.state.Hosts
	h.Hosts = ports.CloneBrokerHostRecords(h.Hosts)
	return h, nil
}
func (s *Store) Load() (ports.BrokerSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ready(); err != nil {
		return ports.BrokerSnapshot{}, err
	}
	return s.state.Snapshot.Clone(), nil
}
func (s *Store) ReplaceHosts(expected uint64, hosts []ports.BrokerHostRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ready(); err != nil {
		return err
	}
	// Validate caller membership before the CAS: malformed registration,
	// policy, anchoring, bound, or duplicate-endpoint input is a caller error,
	// not corrupt durable state, so it never surfaces as ErrInvalidState.
	if err := ports.ValidateBrokerHostRecords(hosts); err != nil {
		return err
	}
	if expected != s.state.Hosts.Revision || expected == ^uint64(0) {
		return ErrStale
	}
	next := s.state
	next.Hosts = ports.BrokerHosts{Revision: expected + 1, Hosts: ports.CloneBrokerHostRecords(hosts)}
	// Policy changes require a new registration generation as well; otherwise
	// an already queued observation could be accepted under changed trust.
	for _, h := range hosts {
		for _, old := range s.state.Hosts.Hosts {
			if h.Registration.Endpoint == old.Registration.Endpoint && h.Policy != old.Policy && h.Registration.Equal(old.Registration) {
				return ErrStale
			}
			if h.Registration.Incarnation == old.Registration.Incarnation && h.Registration.Endpoint == old.Registration.Endpoint && h.Registration.Generation < old.Registration.Generation {
				return ErrStale
			}
		}
	}
	next.Snapshot = filter(next.Snapshot, next.Hosts)
	return s.commit(next)
}
func (s *Store) Store(snapshot ports.BrokerSnapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ready(); err != nil {
		return err
	}
	// Removed tombstones and in-flight checking/failure detail are process-local
	// fencing and observation state; the durable snapshot never carries them, so
	// sanitize the caller's copy before validating the durable shape. Policy is
	// membership authority, not observation state, so the durable format omits
	// it too: membership is the single policy authority and the loader re-stamps
	// every restored observation from the matching record.
	snapshot = snapshot.Clone()
	snapshot.Removed = nil
	for i := range snapshot.Daemons {
		snapshot.Daemons[i].Checking = false
		snapshot.Daemons[i].LastFailure.Err = nil
		snapshot.Daemons[i].Policy = ports.BrokerPolicy{}
		// Attention is transient observation state, not durable identity: a
		// bell recorded the instant before a broker restart must never
		// resurrect on reload, so every tab's Attention is zeroed before the
		// durable shape is validated and written.
		for j := range snapshot.Daemons[i].Sessions {
			for k := range snapshot.Daemons[i].Sessions[j].Tabs {
				snapshot.Daemons[i].Sessions[j].Tabs[k].Attention = false
			}
		}
	}
	if err := validateDurableSnapshot(snapshot); err != nil {
		return err
	}
	if s.writeEpoch != 0 && s.writeEpoch != snapshot.Epoch {
		return ErrStale
	}
	old := s.state.Snapshot
	if old.Epoch == snapshot.Epoch && old.Revision >= snapshot.Revision {
		return ErrStale
	}
	// Reject rather than partially adopt a queued publication from retired hosts.
	for _, daemon := range snapshot.Daemons {
		found := false
		for _, a := range s.state.Hosts.Hosts {
			if daemon.Registration.Equal(a.Registration) {
				found = true
			}
		}
		if !found {
			return ErrStale
		}
	}
	next := s.state
	next.Snapshot = snapshot.Clone()
	if err := s.commit(next); err != nil {
		return err
	}
	s.writeEpoch = snapshot.Epoch
	return nil
}

func filter(snapshot ports.BrokerSnapshot, hosts ports.BrokerHosts) ports.BrokerSnapshot {
	out := snapshot.Clone()
	out.Daemons = nil
	out.Removed = nil
	for _, daemon := range snapshot.Daemons {
		for _, a := range hosts.Hosts {
			if daemon.Registration.Equal(a.Registration) {
				out.Daemons = append(out.Daemons, daemon.Clone())
			}
		}
	}
	return out
}
func validate(st state) error {
	if st.Manifest.Version != 1 {
		return invalid("unsupported manifest")
	}
	if st.Manifest.ImportVersion != 1 || !st.Manifest.ImportCompleted {
		return invalid("initial import is incomplete")
	}
	labels := make(map[manifestSourceLabel]bool, len(st.Manifest.Sources))
	for _, source := range st.Manifest.Sources {
		if labels[source.Label] {
			return invalid("duplicate manifest source")
		}
		labels[source.Label] = true
		b, err := hex.DecodeString(source.SHA256)
		if err != nil || len(b) != 32 {
			return invalid("invalid digest")
		}
	}
	for _, label := range []manifestSourceLabel{manifestMembership, manifestObservations} {
		if !labels[label] {
			return invalid("missing manifest source")
		}
	}
	if err := st.Hosts.Validate(); err != nil {
		return invalid("%v", err)
	}
	if st.Snapshot.Epoch == 0 {
		if st.Snapshot.Revision != 0 || len(st.Snapshot.Daemons) != 0 || len(st.Snapshot.Removed) != 0 {
			return invalid("invalid empty snapshot")
		}
		return nil
	}
	if err := validateDurableSnapshot(st.Snapshot); err != nil {
		return invalid("%v", err)
	}
	for _, daemon := range st.Snapshot.Daemons {
		found := false
		for _, a := range st.Hosts.Hosts {
			if daemon.Registration.Equal(a.Registration) {
				found = true
			}
		}
		if !found || daemon.Checking || daemon.LastFailure.Err != nil {
			return invalid("non-authoritative snapshot")
		}
	}
	return nil
}

// validateDurableSnapshot reports whether a snapshot satisfies the durable
// shape: an empty placeholder stays empty, and a populated snapshot carries
// only non-local daemon observations bound to an exact valid registration with
// a closed availability/failure range and a catalogue-valid inventory. Policy
// and display authority are deliberately excluded: the durable format omits
// policy (membership is the single policy authority and the loader re-stamps
// it) and display hints are derived presentation state, never durable
// authority. Transient checking and live failure causes are sanitized away by
// the callers before this rule runs.
func validateDurableSnapshot(snapshot ports.BrokerSnapshot) error {
	if snapshot.Epoch == 0 {
		if snapshot.Revision != 0 || len(snapshot.Daemons) != 0 || len(snapshot.Removed) != 0 {
			return errors.New("invalid empty snapshot")
		}
		return nil
	}
	if snapshot.Revision == 0 {
		return errors.New("snapshot has no revision")
	}
	if len(snapshot.Daemons) > ports.BrokerMaxDaemonsPerSnapshot {
		return errors.New("snapshot has too many daemons")
	}
	seen := make(map[string]struct{}, len(snapshot.Daemons))
	for _, daemon := range snapshot.Daemons {
		if err := validateDurableObservation(daemon); err != nil {
			return err
		}
		if _, duplicate := seen[daemon.Endpoint]; duplicate {
			return errors.New("snapshot has duplicate host")
		}
		seen[daemon.Endpoint] = struct{}{}
	}
	if len(snapshot.Removed) > ports.BrokerMaxTombstones {
		return errors.New("snapshot has too many tombstones")
	}
	for _, tombstone := range snapshot.Removed {
		if err := tombstone.Validate(); err != nil {
			return err
		}
		if _, live := seen[tombstone.Endpoint]; live {
			return errors.New("snapshot carries a live host as tombstone")
		}
	}
	return nil
}

// validateDurableObservation reports whether one daemon observation may be
// durable: it is remote-only (a local daemon observation is never durable), it
// carries an exact valid registration matching its endpoint, and it satisfies
// the ports durable projection rules shared with the registry.
func validateDurableObservation(daemon ports.BrokerDaemonObservation) error {
	if daemon.Local {
		return errors.New("local daemon observation is never durable")
	}
	if err := domain.ValidateRemoteHostTarget(daemon.Endpoint); err != nil {
		return err
	}
	if err := daemon.Registration.Validate(); err != nil {
		return err
	}
	if daemon.Endpoint != daemon.Registration.Endpoint {
		return errors.New("observation endpoint does not match registration")
	}
	if len(daemon.Sessions) > 0 && !daemon.InventoryKnown {
		return errors.New("observation carries sessions without known inventory")
	}
	return ports.ValidateDurableHostProjection(daemon)
}
func (s *Store) commit(next state) error {
	if err := validate(next); err != nil {
		return err
	}
	if err := s.write("state.json", next); err != nil {
		s.failed = true
		return err
	}
	s.state = next
	return nil
}
func (s *Store) boundary(name string) error {
	if s.fault != nil {
		return s.fault(name)
	}
	return nil
}
func (s *Store) write(name string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(data) > MaxFileBytes {
		return errors.New("brokerstore: file too large")
	}
	f, err := os.CreateTemp(s.dir, ".pending-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	// The staged state carries rollback inputs and registration identities, so
	// its permissions are set explicitly instead of relying on the umask.
	if err = f.Chmod(0o600); err != nil {
		return err
	}
	if _, err = f.Write(data); err != nil {
		return err
	}
	if err = s.boundary(name + ":write"); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = s.boundary(name + ":sync"); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), filepath.Join(s.dir, name)); err != nil {
		return err
	}
	unknown := func(err error) error {
		if err == nil {
			return nil
		}
		return ports.BrokerStoreOutcomeUnknownError{Err: err}
	}
	if err = s.boundary(name + ":rename"); err != nil {
		return unknown(err)
	}
	d, err := os.Open(s.dir)
	if err != nil {
		return unknown(err)
	}
	defer d.Close()
	if err = d.Sync(); err != nil {
		return unknown(err)
	}
	return unknown(s.boundary(name + ":dirsync"))
}
