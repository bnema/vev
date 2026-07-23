package dgram

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/bnema/vev/pkg/safedir"
)

type ProxyRecord struct {
	Session string `json:"session"`
	PID     int    `json:"pid"`
	Port    int    `json:"port"`
	// KeyFingerprint is a non-secret ownership tag (SHA-256 of the AEAD key).
	// The raw key must never be persisted by vev.
	KeyFingerprint string    `json:"key_fingerprint"`
	Created        time.Time `json:"created"`
}

// KeyFingerprint derives the non-secret registry ownership tag.
func KeyFingerprint(key []byte) string {
	sum := sha256.Sum256(key)
	return base64.StdEncoding.EncodeToString(sum[:])
}

type ProxyRegistry struct{ dir string }

func NewProxyRegistry(dir string) *ProxyRegistry { return &ProxyRegistry{dir: dir} }

func (r *ProxyRegistry) Lookup(session string) (ProxyRecord, bool) {
	var rec ProxyRecord
	ok := false
	_ = r.withLock(func() error {
		var err error
		rec, err = r.read(session)
		if err != nil {
			return nil
		}
		if !processAlive(rec.PID) {
			_ = r.removeLocked(session)
			return nil
		}
		if rec.PID != os.Getpid() {
			return nil
		}
		ok = true
		return nil
	})
	return rec, ok
}

func (r *ProxyRegistry) Publish(rec ProxyRecord) error {
	if rec.Created.IsZero() {
		rec.Created = time.Now()
	}
	return r.withLock(func() error {
		b, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		tmp, err := os.CreateTemp(r.dir, ".udp-proxy-*.tmp")
		if err != nil {
			return err
		}
		tmpName := tmp.Name()
		defer func() { _ = os.Remove(tmpName) }()
		if _, err := tmp.Write(b); err != nil {
			_ = tmp.Close()
			return err
		}
		if err := tmp.Close(); err != nil {
			return err
		}
		return os.Rename(tmpName, r.path(rec.Session))
	})
}

func (r *ProxyRegistry) Remove(session string) error {
	return r.withLock(func() error { return r.removeLocked(session) })
}

func (r *ProxyRegistry) RemoveOwned(rec ProxyRecord) error {
	return r.withLock(func() error {
		cur, err := r.read(rec.Session)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if cur.PID != rec.PID || cur.Port != rec.Port || cur.KeyFingerprint != rec.KeyFingerprint {
			return nil
		}
		return r.removeLocked(rec.Session)
	})
}

func (r *ProxyRegistry) withLock(fn func() error) error {
	if err := safedir.EnsurePrivate(r.dir); err != nil {
		return err
	}
	lock, err := os.OpenFile(filepath.Join(r.dir, ".lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = lock.Close() }()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer func() { _ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) }()
	return fn()
}

func (r *ProxyRegistry) removeLocked(session string) error {
	err := os.Remove(r.path(session))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (r *ProxyRegistry) read(session string) (ProxyRecord, error) {
	b, err := os.ReadFile(r.path(session))
	if err != nil {
		return ProxyRecord{}, err
	}
	var rec ProxyRecord
	if err := json.Unmarshal(b, &rec); err != nil {
		return ProxyRecord{}, err
	}
	return rec, nil
}

func (r *ProxyRegistry) path(session string) string {
	if session == "" {
		return filepath.Join(r.dir, "udp-proxy-empty.json")
	}
	name := base64.RawURLEncoding.EncodeToString([]byte(session))
	return filepath.Join(r.dir, "udp-proxy-"+name+".json")
}

func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	p, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = p.Signal(syscall.Signal(0))
	return err == nil || errors.Is(err, syscall.EPERM)
}
