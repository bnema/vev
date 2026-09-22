// Package daemonidentity persists the stable authenticated identity of the
// per-user daemon. Only daemon-owned startup may create it; readers never do.
package daemonidentity

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/safedir"
)

const (
	DirName     = "daemon"
	FileName    = "identity"
	encodedSize = 32
)

var ErrInvalid = errors.New("daemonidentity: invalid identity file")

func Path(stateDir string) string { return filepath.Join(stateDir, DirName, FileName) }

// Load reads an existing identity without creating any filesystem object.
func Load(stateDir string) (ports.BrokerDaemonIdentity, error) {
	path := Path(stateDir)
	info, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		return "", ErrInvalid
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(st.Uid) != os.Getuid() {
		return "", ErrInvalid
	}
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, encodedSize+1))
	if err != nil || len(data) != encodedSize {
		return "", ErrInvalid
	}
	decoded := make([]byte, 16)
	if _, err := hex.Decode(decoded, data); err != nil {
		return "", ErrInvalid
	}
	id := ports.BrokerDaemonIdentity(string(data))
	if err := id.Validate(); err != nil {
		return "", ErrInvalid
	}
	return id, nil
}

// LoadOrCreate is daemon-owner-only creation. It writes a fresh random value
// through an owner-only temporary file and atomically publishes it.
func LoadOrCreate(stateDir string) (ports.BrokerDaemonIdentity, error) {
	id, err := Load(stateDir)
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	dir := filepath.Dir(Path(stateDir))
	if err := safedir.EnsurePrivate(dir); err != nil {
		return "", err
	}
	var raw [16]byte
	if _, err := io.ReadFull(rand.Reader, raw[:]); err != nil {
		return "", err
	}
	data := []byte(hex.EncodeToString(raw[:]))
	tmp, err := os.CreateTemp(dir, ".identity-")
	if err != nil {
		return "", err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err = tmp.Chmod(0o600); err == nil {
		_, err = tmp.Write(data)
	}
	if err == nil {
		err = tmp.Sync()
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return "", err
	}
	if err := os.Rename(tmpName, Path(stateDir)); err != nil {
		return "", err
	}
	d, err := os.Open(dir)
	if err == nil {
		err = d.Sync()
		_ = d.Close()
	}
	if err != nil {
		return "", err
	}
	return Load(stateDir)
}

func NewIncarnation() (ports.BrokerDaemonIncarnation, error) {
	var id ports.BrokerDaemonIncarnation
	_, err := io.ReadFull(rand.Reader, id[:])
	if err == nil && id.IsZero() {
		return NewIncarnation()
	}
	return id, err
}
