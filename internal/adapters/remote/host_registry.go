package remote

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"syscall"
	"unicode/utf8"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/safedir"
)

const hostsFileVersion = 2

type hostsFile struct {
	Version int      `json:"version"`
	Pinned  []string `json:"pinned"`
	Learned []string `json:"learned"`
}

type legacyHostsFile struct {
	Version int      `json:"version"`
	Hosts   []string `json:"hosts"`
}

type hostState struct {
	pinned  []string
	learned map[string]struct{}
}

type fileHostStore struct {
	path string
}

// NewFileHostStore returns a unified pinned/learned host store backed by path.
func NewFileHostStore(path string) ports.RemoteHostStore {
	return &fileHostStore{path: path}
}

var _ ports.RemoteHostStore = (*fileHostStore)(nil)

func (s *fileHostStore) Hosts() (pinned, learned []string, err error) {
	err = s.withLock(func() error {
		state, loadErr := s.loadLocked()
		if loadErr != nil {
			return loadErr
		}
		pinned = append([]string(nil), state.pinned...)
		learned = sortedHosts(state.learned)
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
		state, err := s.loadLocked()
		if err != nil {
			return err
		}
		if slices.Contains(state.pinned, target) {
			return nil
		}
		state.pinned = append(state.pinned, target)
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
		state, err := s.loadLocked()
		if err != nil {
			return err
		}
		next, changed := removeTarget(state.pinned, target)
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
		state, err := s.loadLocked()
		if err != nil {
			return err
		}
		if _, ok := state.learned[target]; ok {
			return nil
		}
		state.learned[target] = struct{}{}
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
		state, err := s.loadLocked()
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
		state, err := s.loadLocked()
		if err != nil {
			return err
		}
		nextPinned, pinnedChanged := removeTarget(state.pinned, target)
		_, learnedPresent := state.learned[target]
		if !pinnedChanged && !learnedPresent {
			return nil
		}
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

func (s *fileHostStore) loadLocked() (hostState, error) {
	raw, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return hostState{
			pinned:  make([]string, 0),
			learned: make(map[string]struct{}),
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
	case 1:
		var legacy legacyHostsFile
		if err := json.Unmarshal(raw, &legacy); err != nil {
			return hostState{}, fmt.Errorf("remote host store: malformed hosts file: %w", err)
		}
		if legacy.Hosts == nil {
			return hostState{}, fmt.Errorf("remote host store: malformed hosts file: missing hosts")
		}
		learned, err := validateLearnedHosts(legacy.Hosts)
		if err != nil {
			return hostState{}, err
		}
		return hostState{pinned: []string{}, learned: learned}, nil
	case hostsFileVersion:
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
		pinned, err := validatePinnedHosts(decoded.Pinned)
		if err != nil {
			return hostState{}, err
		}
		learned, err := validateLearnedHosts(decoded.Learned)
		if err != nil {
			return hostState{}, err
		}
		return hostState{pinned: pinned, learned: learned}, nil
	default:
		return hostState{}, fmt.Errorf("remote host store: unsupported hosts file version %d", header.Version)
	}
}

func (s *fileHostStore) saveLocked(state hostState) error {
	pinned := append([]string{}, state.pinned...)
	if pinned == nil {
		pinned = []string{}
	}
	learned := sortedHosts(state.learned)
	if learned == nil {
		learned = []string{}
	}
	payload, err := json.Marshal(hostsFile{
		Version: hostsFileVersion,
		Pinned:  pinned,
		Learned: learned,
	})
	if err != nil {
		return err
	}
	payload = append(payload, '\n')

	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".hosts-*.tmp")
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
	if err := os.Rename(tmpName, s.path); err != nil {
		return err
	}
	committed = true
	return syncDir(dir)
}

func validatePinnedHosts(hosts []string) ([]string, error) {
	pinned := make([]string, 0, len(hosts))
	seen := make(map[string]struct{}, len(hosts))
	for _, host := range hosts {
		if err := validateHostTarget(host); err != nil {
			return nil, fmt.Errorf("remote host store: malformed hosts file: %w", err)
		}
		if _, dup := seen[host]; dup {
			return nil, fmt.Errorf("remote host store: malformed hosts file: duplicate pinned host %q", host)
		}
		seen[host] = struct{}{}
		pinned = append(pinned, host)
	}
	return pinned, nil
}

func validateLearnedHosts(hosts []string) (map[string]struct{}, error) {
	learned := make(map[string]struct{}, len(hosts))
	for _, host := range hosts {
		if err := validateHostTarget(host); err != nil {
			return nil, fmt.Errorf("remote host store: malformed hosts file: %w", err)
		}
		if _, dup := learned[host]; dup {
			return nil, fmt.Errorf("remote host store: malformed hosts file: duplicate learned host %q", host)
		}
		learned[host] = struct{}{}
	}
	return learned, nil
}

func validateHostTarget(target string) error {
	if !utf8.ValidString(target) {
		return fmt.Errorf("remote host target is not valid UTF-8")
	}
	return domain.ValidateRemoteHostTarget(target)
}

func removeTarget(hosts []string, target string) ([]string, bool) {
	out := make([]string, 0, len(hosts))
	changed := false
	for _, host := range hosts {
		if host == target {
			changed = true
			continue
		}
		out = append(out, host)
	}
	return out, changed
}

func sortedHosts(hosts map[string]struct{}) []string {
	out := make([]string, 0, len(hosts))
	for host := range hosts {
		out = append(out, host)
	}
	sort.Strings(out)
	return out
}

func syncDir(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(file.Sync(), file.Close())
}
