//go:build darwin

package snapshot

import (
	"io"
	"syscall"
	"unsafe"
)

// syscall.ReadDirent on Darwin implements Getdirentries by storing the number
// of returned entries in the descriptor seek offset. Dirent.Seekoff is not
// that cursor. A max-sized record buffer lets each call be drained before the
// descriptor-maintained count is saved.
const maintenanceDirentBufferSize = int(unsafe.Sizeof(syscall.Dirent{}))

func directoryCookie(file maintenanceDirectory, _ *syscall.Dirent, remaining int) (int64, error) {
	end, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	return end - int64(remaining), nil
}

func drainMaintenanceDirentBatch() bool { return true }
