package snapshot

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// repositoryFD opens a repository-relative path one component at a time. Each
// directory is pinned by an O_NOFOLLOW descriptor before its child is opened,
// so a concurrent rename or symlink replacement cannot redirect an operation
// outside the repository.
func (r *Repository) repositoryFD(path string, finalFlags int, mode uint32) (*os.File, error) {
	rel, ok := r.repositoryRelative(path)
	if !ok {
		return nil, fmt.Errorf("snapshot path outside repository")
	}
	root, err := syscall.Open(r.dir, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	if rel == "." {
		if finalFlags&syscall.O_DIRECTORY == 0 {
			_ = syscall.Close(root)
			return nil, fmt.Errorf("invalid snapshot root operation")
		}
		return os.NewFile(uintptr(root), path), nil
	}
	parts := strings.Split(filepath.Clean(rel), string(filepath.Separator))
	fd := root
	for i, part := range parts {
		if part == "" || part == "." || part == ".." {
			_ = syscall.Close(fd)
			return nil, fmt.Errorf("invalid snapshot path")
		}
		flags := syscall.O_RDONLY | syscall.O_CLOEXEC | syscall.O_NOFOLLOW
		if i != len(parts)-1 {
			flags |= syscall.O_DIRECTORY
		} else {
			flags = finalFlags | syscall.O_CLOEXEC | syscall.O_NOFOLLOW
		}
		next, openErr := syscall.Openat(fd, part, flags, mode)
		_ = syscall.Close(fd)
		if openErr != nil {
			return nil, openErr
		}
		fd = next
	}
	f := os.NewFile(uintptr(fd), path)
	if f == nil {
		_ = syscall.Close(fd)
		return nil, fmt.Errorf("open snapshot path")
	}
	return f, nil
}

func (r *Repository) repositoryParent(path string) (int, string, error) {
	rel, ok := r.repositoryRelative(path)
	if !ok || rel == "." {
		return -1, "", fmt.Errorf("snapshot path outside repository")
	}
	parts := strings.Split(filepath.Clean(rel), string(filepath.Separator))
	if len(parts) == 0 || parts[len(parts)-1] == "." || parts[len(parts)-1] == ".." {
		return -1, "", fmt.Errorf("invalid snapshot path")
	}
	fd, err := syscall.Open(r.dir, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return -1, "", err
	}
	for _, part := range parts[:len(parts)-1] {
		if part == "" || part == "." || part == ".." {
			_ = syscall.Close(fd)
			return -1, "", fmt.Errorf("invalid snapshot path")
		}
		next, openErr := syscall.Openat(fd, part, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
		_ = syscall.Close(fd)
		if openErr != nil {
			return -1, "", openErr
		}
		fd = next
	}
	return fd, parts[len(parts)-1], nil
}

func (r *Repository) repositoryRelative(path string) (string, bool) {
	if path == r.dir {
		return ".", true
	}
	prefix := r.dir + string(filepath.Separator)
	if !strings.HasPrefix(path, prefix) {
		return "", false
	}
	rel := strings.TrimPrefix(path, prefix)
	if rel == "" || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == ".." || filepath.IsAbs(rel) {
		return "", false
	}
	return rel, true
}

func (r *Repository) openDirectory(path string) (*os.File, error) {
	return r.repositoryFD(path, syscall.O_RDONLY|syscall.O_DIRECTORY, 0)
}

func (r *Repository) stat(path string) (syscall.Stat_t, error) {
	file, err := r.repositoryFD(path, syscall.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return syscall.Stat_t{}, err
	}
	defer file.Close()
	var st syscall.Stat_t
	if err := syscall.Fstat(int(file.Fd()), &st); err != nil {
		return syscall.Stat_t{}, err
	}
	return st, nil
}

// createTempAt creates an exclusively owned temporary file in an already
// validated directory. os.CreateTemp(path) would resolve that path again.
func (r *Repository) createTempAt(dir string) (*os.File, error) {
	fd, err := r.openDirectory(dir)
	if err != nil {
		return nil, err
	}
	defer fd.Close()
	for i := 0; i < 100; i++ {
		var random [8]byte
		if _, err := rand.Read(random[:]); err != nil {
			return nil, err
		}
		name := ".tmp-" + hex.EncodeToString(random[:])
		tempFD, err := syscall.Openat(int(fd.Fd()), name, syscall.O_WRONLY|syscall.O_CREAT|syscall.O_EXCL|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0o600)
		if err == syscall.EEXIST {
			continue
		}
		if err != nil {
			return nil, err
		}
		return os.NewFile(uintptr(tempFD), filepath.Join(dir, name)), nil
	}
	return nil, fmt.Errorf("create snapshot temporary file: too many collisions")
}
