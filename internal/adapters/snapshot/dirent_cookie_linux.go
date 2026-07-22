//go:build linux

package snapshot

import (
	"errors"
	"syscall"
)

const maintenanceDirentBufferSize = 8192

// directoryCookie is d_off, the resumable getdents(2) cookie for Linux.
func directoryCookie(_ maintenanceDirectory, dirent *syscall.Dirent) (int64, error) {
	return dirent.Off, nil
}

func drainMaintenanceDirentBatch() bool { return false }

func directoryUnlinkRetry(err error, _ uint32) bool { return errors.Is(err, syscall.EISDIR) }
