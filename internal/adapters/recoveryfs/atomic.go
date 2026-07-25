package recoveryfs

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/bnema/vev/pkg/safedir"
)

func atomicWrite(path string, data []byte) (retErr error) {
	dir := filepath.Dir(path)
	if err := safedir.EnsurePrivate(dir); err != nil {
		return err
	}
	file, err := os.CreateTemp(dir, ".intent-*")
	if err != nil {
		return err
	}
	tmp := file.Name()
	defer func() {
		if err := os.Remove(tmp); err != nil && !errors.Is(err, os.ErrNotExist) {
			retErr = errors.Join(retErr, err)
		}
	}()
	if err := file.Chmod(0o600); err != nil {
		return errors.Join(err, file.Close())
	}
	if _, err := file.Write(data); err != nil {
		return errors.Join(err, file.Close())
	}
	if err := file.Sync(); err != nil {
		return errors.Join(err, file.Close())
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return syncDir(dir)
}

func syncDir(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(file.Sync(), file.Close())
}
