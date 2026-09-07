package remote

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"unicode/utf8"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/safedir"
)

const (
	hostsFileName      = "hosts.json"
	hostsFileVersion   = 3
	hostsBackupSuffix  = ".v2.bak"
	legacyHostsVersion = 2
	legacyListVersion  = 1
)

// newRemoteRegistration creates a registration with a fresh random
// incarnation for a validated endpoint. Domain construction stays
// deterministic; only this adapter samples randomness.
func newRemoteRegistration(endpoint string) (domain.RemoteRegistration, error) {
	var incarnation [16]byte
	if _, err := rand.Read(incarnation[:]); err != nil {
		return domain.RemoteRegistration{}, fmt.Errorf("remote registration: %w", err)
	}
	return domain.NewRemoteRegistration(endpoint, incarnation)
}

// HostStorePath returns the canonical location of the remote host store in stateDir.
func HostStorePath(stateDir string) string { return filepath.Join(stateDir, hostsFileName) }

type hostsFile struct {
	Version int          `json:"version"`
	Pinned  []hostRecord `json:"pinned"`
	Learned []hostRecord `json:"learned"`
}

// hostRecord is the persisted registration for one endpoint: the exact
// configured identity plus a random incarnation and fencing generation.
// Incarnation is hex-encoded to stay debuggable in the JSON file.
type hostRecord struct {
	Endpoint    string `json:"endpoint"`
	Incarnation string `json:"incarnation"`
	Generation  uint64 `json:"generation"`
}

type legacyHostsFile struct {
	Version int      `json:"version"`
	Pinned  []string `json:"pinned"`
	Learned []string `json:"learned"`
}

type legacyListHostsFile struct {
	Version int      `json:"version"`
	Hosts   []string `json:"hosts"`
}

type hostState struct {
	pinned  []domain.RemoteRegistration
	learned map[string]domain.RemoteRegistration
}

type fileHostStore struct {
	path string
}

// NewFileHostStore returns a unified pinned/learned host store backed by path.
func NewFileHostStore(path string) ports.RemoteHostStore {
	return &fileHostStore{path: path}
}

var _ ports.RemoteHostStore = (*fileHostStore)(nil)

func (s *fileHostStore) Hosts() (pinned, learned []domain.RemoteRegistration, err error) {
	err = s.withLock(func() error {
		state, loadErr := s.loadMigratedLocked()
		if loadErr != nil {
			return loadErr
		}
		pinned = append([]domain.RemoteRegistration(nil), state.pinned...)
		learned = sortedRegistrations(state.learned)
		return nil
	})
	if err != nil {
		slog.Debug("remote host store hosts failed", "path", s.path, "err", err)
		return nil, nil, err
	}
	return pinned, learned, nil
}

func (s *fileHostStore) AddPinned(target string) error {
	if err := validateHostTarget(target); err != nil {
		return err
	}
	err := s.withLock(func() error {
		state, err := s.loadMigratedLocked()
		if err != nil {
			return err
		}
		for _, record := range state.pinned {
			if record.Endpoint == target {
				return nil
			}
		}
		// Pinning an existing registration retains its incarnation.
		if record, ok := state.learned[target]; ok {
			state.pinned = append(state.pinned, record)
		} else {
			record, err := newRemoteRegistration(target)
			if err != nil {
				return err
			}
			state.pinned = append(state.pinned, record)
		}
		return s.saveLocked(state)
	})
	if err != nil {
		slog.Debug("remote host store add pinned failed", "path", s.path, "target", target, "err", err)
	}
	return err
}

func (s *fileHostStore) RemovePinned(target string) error {
	if err := validateHostTarget(target); err != nil {
		return err
	}
	err := s.withLock(func() error {
		state, err := s.loadMigratedLocked()
		if err != nil {
			return err
		}
		next, changed := removeRegistration(state.pinned, target)
		if !changed {
			return nil
		}
		state.pinned = next
		return s.saveLocked(state)
	})
	if err != nil {
		slog.Debug("remote host store remove pinned failed", "path", s.path, "target", target, "err", err)
	}
	return err
}

func (s *fileHostStore) Remember(target string) error {
	if err := validateHostTarget(target); err != nil {
		return err
	}
	err := s.withLock(func() error {
		state, err := s.loadMigratedLocked()
		if err != nil {
			return err
		}
		if _, ok := state.learned[target]; ok {
			return nil
		}
		// Remembering an existing registration retains its incarnation.
		if record, ok := findRegistration(state.pinned, target); ok {
			state.learned[target] = record
		} else {
			record, err := newRemoteRegistration(target)
			if err != nil {
				return err
			}
			state.learned[target] = record
		}
		return s.saveLocked(state)
	})
	if err != nil {
		slog.Debug("remote host store remember failed", "path", s.path, "target", target, "err", err)
	}
	return err
}

func (s *fileHostStore) Forget(target string) error {
	if err := validateHostTarget(target); err != nil {
		return err
	}
	err := s.withLock(func() error {
		state, err := s.loadMigratedLocked()
		if err != nil {
			return err
		}
		if _, ok := state.learned[target]; !ok {
			return nil
		}
		delete(state.learned, target)
		return s.saveLocked(state)
	})
	if err != nil {
		slog.Debug("remote host store forget failed", "path", s.path, "target", target, "err", err)
	}
	return err
}

func (s *fileHostStore) Remove(target string) (deleted bool, err error) {
	if err := validateHostTarget(target); err != nil {
		return false, err
	}
	err = s.withLock(func() error {
		state, err := s.loadMigratedLocked()
		if err != nil {
			return err
		}
		nextPinned, pinnedChanged := removeRegistration(state.pinned, target)
		_, learnedPresent := state.learned[target]
		if !pinnedChanged && !learnedPresent {
			return nil
		}
		// Complete removal drops the incarnation: a later re-add creates
		// a new registration even though the endpoint string matches.
		state.pinned = nextPinned
		delete(state.learned, target)
		if err := s.saveLocked(state); err != nil {
			return err
		}
		deleted = true
		return nil
	})
	if err != nil {
		slog.Debug("remote host store remove failed", "path", s.path, "target", target, "err", err)
		return false, err
	}
	return deleted, nil
}

func (s *fileHostStore) withLock(fn func() error) error {
	if err := safedir.EnsurePrivate(filepath.Dir(s.path)); err != nil {
		return err
	}
	lock, err := os.OpenFile(s.path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = lock.Close() }()
	if err := lock.Chmod(0o600); err != nil {
		return err
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer func() { _ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) }()
	return fn()
}

// loadMigratedLocked loads the registry, migrating version 1 list files and
// version 2 string lists to version 3 registration records. Migration assigns
// one fresh random incarnation per endpoint, preserves pinned/learned
// ordering and membership, and persists a private pre-migration backup.
// Unsupported or corrupt versions fail closed without replacement.
func (s *fileHostStore) loadMigratedLocked() (hostState, error) {
	raw, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return hostState{
			pinned:  make([]domain.RemoteRegistration, 0),
			learned: make(map[string]domain.RemoteRegistration),
		}, nil
	}
	if err != nil {
		return hostState{}, err
	}
	if !utf8.Valid(raw) {
		return hostState{}, fmt.Errorf("remote host store: malformed hosts file: invalid UTF-8")
	}
	var header struct {
		Version int `json:"version"`
	}
	if err := json.Unmarshal(raw, &header); err != nil {
		return hostState{}, fmt.Errorf("remote host store: malformed hosts file: %w", err)
	}

	switch header.Version {
	case hostsFileVersion:
		return decodeHostsV3(raw)
	case legacyHostsVersion, legacyListVersion:
		state, err := decodeLegacyHosts(raw, header.Version)
		if err != nil {
			return hostState{}, err
		}
		if err := s.backupAndSaveMigratedLocked(raw, state); err != nil {
			return hostState{}, err
		}
		return state, nil
	default:
		return hostState{}, fmt.Errorf("remote host store: unsupported hosts file version %d", header.Version)
	}
}

// backupAndSaveMigratedLocked preserves the pre-migration bytes privately and
// atomically replaces the registry with version 3 records. Both steps run
// under the existing registry lock.
func (s *fileHostStore) backupAndSaveMigratedLocked(raw []byte, state hostState) error {
	if err := atomicReplacePrivateFile(s.path+hostsBackupSuffix, raw); err != nil {
		return err
	}
	return s.saveLocked(state)
}

func decodeHostsV3(raw []byte) (hostState, error) {
	var decoded hostsFile
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return hostState{}, fmt.Errorf("remote host store: malformed hosts file: %w", err)
	}
	if decoded.Pinned == nil {
		return hostState{}, fmt.Errorf("remote host store: malformed hosts file: missing pinned")
	}
	if decoded.Learned == nil {
		return hostState{}, fmt.Errorf("remote host store: malformed hosts file: missing learned")
	}
	pinned, err := validatePinnedRecords(decoded.Pinned)
	if err != nil {
		return hostState{}, err
	}
	learned, err := validateLearnedRecords(decoded.Learned)
	if err != nil {
		return hostState{}, err
	}
	for _, record := range pinned {
		if other, ok := learned[record.Endpoint]; ok && !other.Equal(record) {
			return hostState{}, fmt.Errorf("remote host store: malformed hosts file: conflicting identities for %q", record.Endpoint)
		}
	}
	return hostState{pinned: pinned, learned: learned}, nil
}

func decodeLegacyHosts(raw []byte, version int) (hostState, error) {
	if version == legacyListVersion {
		var legacy legacyListHostsFile
		if err := json.Unmarshal(raw, &legacy); err != nil {
			return hostState{}, fmt.Errorf("remote host store: malformed hosts file: %w", err)
		}
		if legacy.Hosts == nil {
			return hostState{}, fmt.Errorf("remote host store: malformed hosts file: missing hosts")
		}
		learned, err := migrateTargetsToRecords(legacy.Hosts, nil)
		if err != nil {
			return hostState{}, err
		}
		return hostState{pinned: []domain.RemoteRegistration{}, learned: learned}, nil
	}
	var legacy legacyHostsFile
	if err := json.Unmarshal(raw, &legacy); err != nil {
		return hostState{}, fmt.Errorf("remote host store: malformed hosts file: %w", err)
	}
	if legacy.Pinned == nil {
		return hostState{}, fmt.Errorf("remote host store: malformed hosts file: missing pinned")
	}
	if legacy.Learned == nil {
		return hostState{}, fmt.Errorf("remote host store: malformed hosts file: missing learned")
	}
	pinned, err := migrateOrderedTargetsToRecords(legacy.Pinned)
	if err != nil {
		return hostState{}, err
	}
	// One endpoint keeps one registration across memberships: learned
	// entries reuse the pinned incarnation instead of minting a second
	// identity that would surface on RemovePinned.
	reuse := make(map[string]domain.RemoteRegistration, len(pinned))
	for _, record := range pinned {
		reuse[record.Endpoint] = record
	}
	learned, err := migrateTargetsToRecords(legacy.Learned, reuse)
	if err != nil {
		return hostState{}, err
	}
	return hostState{pinned: pinned, learned: learned}, nil
}

func migrateOrderedTargetsToRecords(hosts []string) ([]domain.RemoteRegistration, error) {
	pinned := make([]domain.RemoteRegistration, 0, len(hosts))
	seen := make(map[string]struct{}, len(hosts))
	for _, host := range hosts {
		if err := validateHostTarget(host); err != nil {
			return nil, fmt.Errorf("remote host store: malformed hosts file: %w", err)
		}
		if _, dup := seen[host]; dup {
			return nil, fmt.Errorf("remote host store: malformed hosts file: duplicate pinned host %q", host)
		}
		seen[host] = struct{}{}
		record, err := newRemoteRegistration(host)
		if err != nil {
			return nil, err
		}
		pinned = append(pinned, record)
	}
	return pinned, nil
}

func migrateTargetsToRecords(hosts []string, reuse map[string]domain.RemoteRegistration) (map[string]domain.RemoteRegistration, error) {
	learned := make(map[string]domain.RemoteRegistration, len(hosts))
	for _, host := range hosts {
		if err := validateHostTarget(host); err != nil {
			return nil, fmt.Errorf("remote host store: malformed hosts file: %w", err)
		}
		if _, dup := learned[host]; dup {
			return nil, fmt.Errorf("remote host store: malformed hosts file: duplicate learned host %q", host)
		}
		if record, ok := reuse[host]; ok {
			learned[host] = record
			continue
		}
		record, err := newRemoteRegistration(host)
		if err != nil {
			return nil, err
		}
		learned[host] = record
	}
	return learned, nil
}

func (s *fileHostStore) saveLocked(state hostState) error {
	pinned := make([]hostRecord, 0, len(state.pinned))
	for _, record := range state.pinned {
		pinned = append(pinned, encodeRecord(record))
	}
	learned := make([]hostRecord, 0, len(state.learned))
	for _, record := range sortedRegistrations(state.learned) {
		learned = append(learned, encodeRecord(record))
	}
	payload, err := json.Marshal(hostsFile{
		Version: hostsFileVersion,
		Pinned:  pinned,
		Learned: learned,
	})
	if err != nil {
		return err
	}
	return atomicReplacePrivateFile(s.path, append(payload, '\n'))
}

func encodeRecord(record domain.RemoteRegistration) hostRecord {
	return hostRecord{
		Endpoint:    record.Endpoint,
		Incarnation: hex.EncodeToString(record.Incarnation[:]),
		Generation:  uint64(record.Generation),
	}
}

func decodeRecord(record hostRecord) (domain.RemoteRegistration, error) {
	if err := validateHostTarget(record.Endpoint); err != nil {
		return domain.RemoteRegistration{}, fmt.Errorf("remote host store: malformed hosts file: %w", err)
	}
	raw, err := hex.DecodeString(record.Incarnation)
	if err != nil || len(raw) != 16 {
		return domain.RemoteRegistration{}, fmt.Errorf("remote host store: malformed hosts file: invalid incarnation for %q", record.Endpoint)
	}
	var incarnation [16]byte
	copy(incarnation[:], raw)
	registration := domain.RemoteRegistration{Endpoint: record.Endpoint, Incarnation: incarnation, Generation: domain.RemoteGeneration(record.Generation)}
	if err := registration.Validate(); err != nil {
		return domain.RemoteRegistration{}, fmt.Errorf("remote host store: malformed hosts file: %w", err)
	}
	return registration, nil
}

func atomicReplacePrivateFile(path string, payload []byte) error {
	dir := filepath.Dir(path)
	if err := safedir.EnsurePrivate(dir); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".vev-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	committed := false
	defer func() {
		if !committed {
			_ = tmp.Close()
			_ = os.Remove(tmpName)
		}
	}()
	if err := tmp.Chmod(0o600); err != nil {
		return err
	}
	if _, err := tmp.Write(payload); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	committed = true
	return syncDir(dir)
}

func validatePinnedRecords(records []hostRecord) ([]domain.RemoteRegistration, error) {
	pinned := make([]domain.RemoteRegistration, 0, len(records))
	seen := make(map[string]struct{}, len(records))
	for _, record := range records {
		registration, err := decodeRecord(record)
		if err != nil {
			return nil, err
		}
		if _, dup := seen[registration.Endpoint]; dup {
			return nil, fmt.Errorf("remote host store: malformed hosts file: duplicate pinned host %q", registration.Endpoint)
		}
		seen[registration.Endpoint] = struct{}{}
		pinned = append(pinned, registration)
	}
	return pinned, nil
}

func validateLearnedRecords(records []hostRecord) (map[string]domain.RemoteRegistration, error) {
	learned := make(map[string]domain.RemoteRegistration, len(records))
	for _, record := range records {
		registration, err := decodeRecord(record)
		if err != nil {
			return nil, err
		}
		if _, dup := learned[registration.Endpoint]; dup {
			return nil, fmt.Errorf("remote host store: malformed hosts file: duplicate learned host %q", registration.Endpoint)
		}
		learned[registration.Endpoint] = registration
	}
	return learned, nil
}

func validateHostTarget(target string) error {
	if !utf8.ValidString(target) {
		return fmt.Errorf("remote host target is not valid UTF-8")
	}
	return domain.ValidateRemoteHostTarget(target)
}

func findRegistration(registrations []domain.RemoteRegistration, target string) (domain.RemoteRegistration, bool) {
	for _, record := range registrations {
		if record.Endpoint == target {
			return record, true
		}
	}
	return domain.RemoteRegistration{}, false
}

func removeRegistration(registrations []domain.RemoteRegistration, target string) ([]domain.RemoteRegistration, bool) {
	out := make([]domain.RemoteRegistration, 0, len(registrations))
	changed := false
	for _, record := range registrations {
		if record.Endpoint == target {
			changed = true
			continue
		}
		out = append(out, record)
	}
	return out, changed
}

func sortedRegistrations(registrations map[string]domain.RemoteRegistration) []domain.RemoteRegistration {
	out := make([]domain.RemoteRegistration, 0, len(registrations))
	for _, record := range registrations {
		out = append(out, record)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Endpoint < out[j].Endpoint })
	return out
}

func syncDir(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(file.Sync(), file.Close())
}
