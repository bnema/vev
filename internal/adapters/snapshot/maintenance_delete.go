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
	if err := os.RemoveAll(path); err != nil {
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
