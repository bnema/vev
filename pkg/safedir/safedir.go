// Package safedir creates private directories and refuses to trust
// pre-existing ones that another user could control.
package safedir

import (
	"fmt"
	"os"
	"syscall"
)

// EnsurePrivate creates dir with mode 0700 if missing, then verifies it is a
// real directory (not a symlink) owned by the current user and has exact 0700
// permissions. It returns an error describing the violation otherwise.
func EnsurePrivate(dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	fi, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	if fi.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("safedir: %s is a symlink", dir)
	}
	if !fi.IsDir() {
		return fmt.Errorf("safedir: %s is not a directory", dir)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("safedir: cannot stat %s", dir)
	}
	if int(st.Uid) != os.Getuid() {
		return fmt.Errorf("safedir: %s is owned by uid %d, not current uid %d — refusing to use it", dir, st.Uid, os.Getuid())
	}
	if perm := fi.Mode().Perm(); perm != 0o700 {
		return fmt.Errorf("safedir: %s has permissions %04o, want 0700 — refusing to use it", dir, perm)
	}
	return nil
}
