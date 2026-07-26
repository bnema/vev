package snapshot

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/bnema/vev/internal/domain"
)

// DeleteIncarnation removes one incarnation-keyed snapshot namespace. It is
// retry-idempotent: an incarnation already absent is a successful deletion.
func (r *Repository) DeleteIncarnation(ctx context.Context, id domain.IncarnationID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key, err := incarnationKey(id)
	if err != nil {
		return err
	}
	lock := r.lockSession(key)
	defer r.unlockSession(lock)

	path := r.sessionPath(id)
	if err := r.removeIncarnationLocked(id); err != nil {
		return fmt.Errorf("remove snapshot incarnation %s: %w", id.String(), safeFilesystemError(err))
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	parent := filepath.Dir(path)
	if _, err := os.Lstat(parent); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	return r.syncDirectory(parent)
}

// removeIncarnationLocked removes an incarnation through a root-pinned path.
// The caller must hold the incarnation's session lock.
func (r *Repository) removeIncarnationLocked(id domain.IncarnationID) (err error) {
	root, err := r.openRoot()
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer func() { joinCloseError(&err, "close snapshot root", r.closeRoot(root)) }()
	return root.RemoveAll(filepath.Join(repositorySessionsDir, id.String()))
}
