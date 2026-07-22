package snapshot

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
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
			err = fmt.Errorf("invalid snapshot root operation")
			joinCloseError(&err, "close snapshot root directory", r.closeDescriptor(root, "close snapshot root directory"))
			return nil, err
		}
		file := os.NewFile(uintptr(root), path)
		if file == nil {
			err = fmt.Errorf("open snapshot root")
			joinCloseError(&err, "close snapshot root directory", r.closeDescriptor(root, "close snapshot root directory"))
			return nil, err
		}
		return file, nil
	}
	parts := strings.Split(filepath.Clean(rel), string(filepath.Separator))
	fd := root
	for i, part := range parts {
		if part == "" || part == "." || part == ".." {
			err = fmt.Errorf("invalid snapshot path")
			joinCloseError(&err, "close snapshot path directory", r.closeDescriptor(fd, "close snapshot path directory"))
			return nil, err
		}
		flags := syscall.O_RDONLY | syscall.O_CLOEXEC | syscall.O_NOFOLLOW
		if i != len(parts)-1 {
			flags |= syscall.O_DIRECTORY
		} else {
			flags = finalFlags | syscall.O_CLOEXEC | syscall.O_NOFOLLOW
		}
		next, openErr := syscall.Openat(fd, part, flags, mode)
		closeErr := r.closeDescriptor(fd, "close snapshot path parent directory")
		if openErr != nil {
			err = openErr
			joinCloseError(&err, "close snapshot path parent directory", closeErr)
			return nil, err
		}
		if closeErr != nil {
			err = fmt.Errorf("close snapshot path parent directory: %w", closeErr)
			joinCloseError(&err, "close snapshot path child directory", r.closeDescriptor(next, "close snapshot path child directory"))
			return nil, err
		}
		fd = next
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		err = fmt.Errorf("open snapshot path")
		joinCloseError(&err, "close snapshot path descriptor", r.closeDescriptor(fd, "close snapshot path descriptor"))
		return nil, err
	}
	return file, nil
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
			err = fmt.Errorf("invalid snapshot path")
			joinCloseError(&err, "close snapshot parent directory", r.closeDescriptor(fd, "close snapshot parent directory"))
			return -1, "", err
		}
		next, openErr := syscall.Openat(fd, part, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
		closeErr := r.closeDescriptor(fd, "close snapshot parent directory")
		if openErr != nil {
			err = openErr
			joinCloseError(&err, "close snapshot parent directory", closeErr)
			return -1, "", err
		}
		if closeErr != nil {
			err = fmt.Errorf("close snapshot parent directory: %w", closeErr)
			joinCloseError(&err, "close snapshot child directory", r.closeDescriptor(next, "close snapshot child directory"))
			return -1, "", err
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

func (r *Repository) stat(path string) (st syscall.Stat_t, err error) {
	file, err := r.repositoryFD(path, syscall.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return syscall.Stat_t{}, err
	}
	defer func() {
		joinCloseError(&err, "close snapshot file", r.closeRepositoryFile(file, "close snapshot file"))
	}()
	if err := syscall.Fstat(int(file.Fd()), &st); err != nil {
		return syscall.Stat_t{}, err
	}
	return st, nil
}

// createTempAt creates an exclusively owned temporary file in an already
// validated directory. os.CreateTemp(path) would resolve that path again.
func (r *Repository) createTempAt(dir string) (temp *os.File, err error) {
	fd, err := r.openDirectory(dir)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := r.closeRepositoryFile(fd, "close snapshot temporary directory"); closeErr != nil {
			if temp != nil {
				joinCloseError(&err, "close snapshot temporary file after directory close failure", r.closeRepositoryFile(temp, "close snapshot temporary file after directory close failure"))
				temp = nil
			}
			joinCloseError(&err, "close snapshot temporary directory", closeErr)
		}
	}()
	for range 100 {
		var random [8]byte
		if _, err := rand.Read(random[:]); err != nil {
			return nil, err
		}
		name := ".tmp-" + hex.EncodeToString(random[:])
		tempFD, err := syscall.Openat(int(fd.Fd()), name, syscall.O_WRONLY|syscall.O_CREAT|syscall.O_EXCL|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0o600)
		if errors.Is(err, syscall.EEXIST) {
			continue
		}
		if err != nil {
			return nil, err
		}
		temp = os.NewFile(uintptr(tempFD), filepath.Join(dir, name))
		if temp == nil {
			err = fmt.Errorf("open snapshot temporary file")
			joinCloseError(&err, "close snapshot temporary descriptor", r.closeDescriptor(tempFD, "close snapshot temporary descriptor"))
			return nil, err
		}
		return temp, nil
	}
	return nil, fmt.Errorf("create snapshot temporary file: too many collisions")
}
