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

func (r *Repository) openRoot() (*os.Root, error) {
	// os.OpenRoot follows a final symlink, so validate that its pinned directory
	// is still the non-symlink directory at the configured path.
	root, err := os.OpenRoot(r.dir)
	if err != nil {
		return nil, err
	}
	if hook := r.hooks.afterOpenRoot; hook != nil {
		hook()
	}
	current, err := os.Lstat(r.dir)
	if err != nil {
		return nil, r.closeRootOnError(root, err)
	}
	if !current.IsDir() || current.Mode()&os.ModeSymlink != 0 {
		return nil, r.closeRootOnError(root, &os.PathError{Op: "open", Path: r.dir, Err: syscall.ELOOP})
	}
	pinned, err := root.Stat(".")
	if err != nil {
		return nil, r.closeRootOnError(root, err)
	}
	if !os.SameFile(current, pinned) {
		return nil, r.closeRootOnError(root, &os.PathError{Op: "open", Path: r.dir, Err: syscall.ESTALE})
	}
	if !privateDirectory(pinned) {
		return nil, r.closeRootOnError(root, fmt.Errorf("snapshot directory is not private"))
	}
	return root, nil
}

func privateDirectory(fi os.FileInfo) bool {
	st, ok := fi.Sys().(*syscall.Stat_t)
	return ok && fi.IsDir() && int(st.Uid) == os.Geteuid() && fi.Mode().Perm() == 0o700
}

func (r *Repository) closeRoot(root *os.Root) error {
	var injected error
	if hook := r.hooks.closeRoot; hook != nil {
		injected = hook()
	}
	return errors.Join(injected, root.Close())
}

func (r *Repository) closeRootOnError(root *os.Root, err error) error {
	return errors.Join(err, r.closeRoot(root))
}

// rejectFinalSymlink preserves the pre-os.Root guarantee that the final
// component is never a symlink. Root confinement is the hard boundary; this
// is defense-in-depth with a benign TOCTOU window inside a 0700 directory.
func rejectFinalSymlink(root *os.Root, rel string) error {
	fi, err := root.Lstat(rel)
	if err != nil {
		return err
	}
	if fi.Mode()&os.ModeSymlink != 0 {
		return &os.PathError{Op: "open", Path: rel, Err: syscall.ELOOP}
	}
	return nil
}

func (r *Repository) openDirectory(path string) (file *os.File, err error) {
	rel, ok := r.repositoryRelative(path)
	if !ok {
		return nil, fmt.Errorf("snapshot path outside repository")
	}
	root, err := r.openRoot()
	if err != nil {
		return nil, err
	}
	defer func() {
		closeErr := r.closeRoot(root)
		if closeErr != nil && file != nil {
			joinCloseError(&err, "close snapshot directory after root close failure", file.Close())
			file = nil
		}
		joinCloseError(&err, "close snapshot root", closeErr)
	}()
	if err := rejectFinalSymlink(root, rel); err != nil {
		return nil, err
	}
	return root.OpenFile(rel, syscall.O_RDONLY|syscall.O_DIRECTORY, 0)
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

func (r *Repository) stat(path string) (fi os.FileInfo, err error) {
	rel, ok := r.repositoryRelative(path)
	if !ok {
		return nil, fmt.Errorf("snapshot path outside repository")
	}
	root, err := r.openRoot()
	if err != nil {
		return nil, err
	}
	defer func() { joinCloseError(&err, "close snapshot root", r.closeRoot(root)) }()
	return root.Lstat(rel)
}

// createTempAt creates an exclusively owned temporary file in an already
// validated directory. os.CreateTemp(path) would resolve that path again.
func (r *Repository) createTempAt(dir string) (temp *os.File, err error) {
	rel, ok := r.repositoryRelative(dir)
	if !ok {
		return nil, fmt.Errorf("snapshot path outside repository")
	}
	root, err := r.openRoot()
	if err != nil {
		return nil, err
	}
	defer func() {
		joinCloseError(&err, "close snapshot root", r.closeRoot(root))
		if err != nil && temp != nil {
			joinCloseError(&err, "close snapshot temporary file after root close failure", temp.Close())
			temp = nil
		}
	}()
	for range 100 {
		var random [8]byte
		if _, err := rand.Read(random[:]); err != nil {
			return nil, err
		}
		name := ".tmp-" + hex.EncodeToString(random[:])
		temp, err = root.OpenFile(filepath.Join(rel, name), syscall.O_WRONLY|syscall.O_CREAT|syscall.O_EXCL, 0o600)
		if errors.Is(err, os.ErrExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		return temp, nil
	}
	return nil, fmt.Errorf("create snapshot temporary file: too many collisions")
}
