package snapshot

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"unsafe"
)

// maxMaintenanceCursors bounds all retained directory seek cookies, including
// cursors for attacker-controlled object shards. Maintenance processes only
// one shard at a time, so normal operation stays well below this ceiling.
const maxMaintenanceCursors = 8

var errMaintenanceCursorLimit = errors.New("snapshot maintenance cursor limit")

type maintenanceCursor struct {
	offset  int64
	pending []maintenanceDirEntry
	done    bool
}

type maintenanceDirEntry struct {
	name  string
	isDir bool
}

func (r *Repository) resetMaintenance() {
	for key := range r.maintenanceSessions {
		r.clearSessionMaintenance(key)
	}
	r.maintenanceCursors = nil
	r.maintenanceSessions = nil
	r.maintenanceQuarantine = nil
}

func maintenanceCursorID(dir, purpose string) string { return purpose + "\x00" + dir }

// readMaintenanceDir advances one named directory cursor. Its continuation is
// only a Linux seek cookie and at most one call's worth of pending names. The
// descriptor used to read a call is always closed before this method returns.
// Callers must return entries they could not process with
// requeueMaintenanceEntries before yielding for budget exhaustion.
func (r *Repository) readMaintenanceDir(dir string, n int, purpose string) ([]maintenanceDirEntry, bool, error) {
	id := maintenanceCursorID(dir, purpose)
	cursor := r.maintenanceCursors[id]
	if cursor != nil && len(cursor.pending) != 0 {
		entries := cursor.pending
		cursor.pending = nil
		if cursor.done {
			delete(r.maintenanceCursors, id)
		}
		return entries, cursor.done, nil
	}
	if cursor == nil {
		if len(r.maintenanceCursors) >= maxMaintenanceCursors {
			return nil, false, errMaintenanceCursorLimit
		}
		cursor = &maintenanceCursor{}
		r.maintenanceCursors[id] = cursor
	}

	entries, done, err := r.readMaintenanceDirent(dir, n, cursor)
	if err != nil {
		delete(r.maintenanceCursors, id)
		return nil, false, err
	}
	if done {
		delete(r.maintenanceCursors, id)
	}
	return entries, done, nil
}

// maintenanceDirectory is the small syscall surface needed to resume a
// bounded getdents cursor. It permits deterministic fault injection without
// retaining directory descriptors between maintenance passes.
type maintenanceDirectory interface {
	Seek(int64, int) (int64, error)
	ReadDirent([]byte) (int, error)
	Close() error
}

type osMaintenanceDirectory struct{ file *os.File }

func (d osMaintenanceDirectory) Seek(offset int64, whence int) (int64, error) {
	return syscall.Seek(int(d.file.Fd()), offset, whence)
}

func (d osMaintenanceDirectory) ReadDirent(buffer []byte) (int, error) {
	return syscall.ReadDirent(int(d.file.Fd()), buffer)
}

func (d osMaintenanceDirectory) Close() error { return d.file.Close() }

func (r *Repository) openMaintenanceDirectory(dir string) (maintenanceDirectory, error) {
	if open := r.hooks.openMaintenanceDirectory; open != nil {
		return open(dir)
	}
	file, err := r.openDirectory(dir)
	if err != nil {
		return nil, err
	}
	return osMaintenanceDirectory{file: file}, nil
}

// maintenanceDirectoryError adds stable operation context and removes any
// filesystem path while retaining the underlying cause for errors.Is.
func maintenanceDirectoryError(operation string, err error) error {
	return fmt.Errorf("%s: %w", operation, safeFilesystemError(err))
}

// readMaintenanceDirent uses the Linux getdents seek cookie from each record.
// syscall.Dirent's Off field is the d_off member of linux_dirent64 and
// syscall.Seek is lseek(2), as defined by Go's syscall Linux sources.
func (r *Repository) readMaintenanceDirent(dir string, limit int, cursor *maintenanceCursor) (entries []maintenanceDirEntry, done bool, err error) {
	file, err := r.openMaintenanceDirectory(dir)
	if err != nil {
		return nil, false, maintenanceDirectoryError("open maintenance directory", err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			closeErr = maintenanceDirectoryError("close maintenance directory", closeErr)
			if err != nil {
				err = errors.Join(err, closeErr)
			} else {
				entries = nil
				done = false
				err = closeErr
			}
		}
	}()
	if cursor.offset != 0 {
		if _, err := file.Seek(cursor.offset, io.SeekStart); err != nil {
			return nil, false, maintenanceDirectoryError("seek maintenance directory", err)
		}
	}

	entries = make([]maintenanceDirEntry, 0, limit)
	buffer := make([]byte, 8192)
	for len(entries) < limit {
		count, err := file.ReadDirent(buffer)
		if err != nil {
			return nil, false, maintenanceDirectoryError("read maintenance directory", err)
		}
		if count == 0 {
			return entries, true, nil
		}
		for data := buffer[:count]; len(data) != 0 && len(entries) < limit; {
			if len(data) < int(unsafe.Offsetof(syscall.Dirent{}.Name)) {
				return nil, false, syscall.EIO
			}
			record := (*syscall.Dirent)(unsafe.Pointer(&data[0]))
			reclen := int(record.Reclen)
			nameOffset := int(unsafe.Offsetof(syscall.Dirent{}.Name))
			if reclen < nameOffset || reclen > len(data) {
				return nil, false, syscall.EIO
			}
			data = data[reclen:]
			// Advance past every raw record, including dot and disappeared entries.
			cursor.offset = record.Off
			nameBytes := unsafe.Slice((*byte)(unsafe.Pointer(&record.Name[0])), reclen-nameOffset)
			if end := strings.IndexByte(string(nameBytes), 0); end >= 0 {
				nameBytes = nameBytes[:end]
			}
			name := string(nameBytes)
			if name == "." || name == ".." || name == "" {
				continue
			}
			stat, err := r.stat(filepath.Join(dir, name))
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				// A final symlink is not traversed and is treated as a non-directory;
				// deletion remains dirfd-relative and therefore unlinks only the link.
				if errors.Is(err, syscall.ELOOP) {
					entries = append(entries, maintenanceDirEntry{name: name})
					continue
				}
				return nil, false, maintenanceDirectoryError("stat maintenance directory entry", err)
			}
			entries = append(entries, maintenanceDirEntry{name: name, isDir: stat.Mode&syscall.S_IFMT == syscall.S_IFDIR})
		}
	}
	return entries, false, nil
}

func (r *Repository) requeueMaintenanceEntries(dir, purpose string, entries []maintenanceDirEntry) {
	if len(entries) == 0 {
		return
	}
	id := maintenanceCursorID(dir, purpose)
	cursor := r.maintenanceCursors[id]
	if cursor == nil {
		cursor = &maintenanceCursor{done: true}
		r.maintenanceCursors[id] = cursor
	}
	cursor.pending = append(entries, cursor.pending...)
}

func (r *Repository) removeTemps(ctx context.Context, dir string, budget *int, purpose string) (bool, error) {
	if *budget == 0 {
		return false, nil
	}
	entries, done, err := r.readMaintenanceDir(dir, maintenanceBatch, purpose)
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("read snapshot maintenance directory: %w", safeFilesystemError(err))
	}
	for i, entry := range entries {
		if *budget == 0 {
			r.requeueMaintenanceEntries(dir, purpose, entries[i:])
			return false, nil
		}
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if entry.isDir || !strings.HasPrefix(entry.name, ".tmp-") {
			continue
		}
		path := filepath.Join(dir, entry.name)
		if err := r.remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return false, fmt.Errorf("remove snapshot maintenance file %q: %w", maintenancePath(r.dir, path), safeFilesystemError(err))
		}
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if err := r.syncDirectory(dir); err != nil {
			return false, fmt.Errorf("sync snapshot maintenance directory for %q: %w", maintenancePath(r.dir, path), safeFilesystemError(err))
		}
		*budget--
	}
	return done, nil
}

// maintenancePath avoids exposing the repository root while retaining a
