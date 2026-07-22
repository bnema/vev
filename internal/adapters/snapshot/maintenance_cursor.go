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
		count := min(n, len(cursor.pending))
		entries := cursor.pending[:count]
		cursor.pending = cursor.pending[count:]
		done := cursor.done && len(cursor.pending) == 0
		if done {
			delete(r.maintenanceCursors, id)
		}
		return entries, done, nil
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

// readMaintenanceDirent saves a resumable directory cursor. Linux d_off lets
// a large getdents batch stop at limit. Darwin's syscall.ReadDirent instead
// stores an entry count in the descriptor offset, so every returned buffer
// must be drained before that descriptor-maintained count is saved.
func (r *Repository) readMaintenanceDirent(dir string, limit int, cursor *maintenanceCursor) (entries []maintenanceDirEntry, done bool, err error) {
	return r.readMaintenanceDirentWithDrain(dir, limit, cursor, drainMaintenanceDirentBatch(), directoryCookie)
}

// readMaintenanceDirentWithDrain contains the shared traversal logic. The
// explicit parameters permit platform-independent verification of Darwin's
// buffer-draining semantics with deterministic fake directories.
func (r *Repository) readMaintenanceDirentWithDrain(dir string, limit int, cursor *maintenanceCursor, drainBuffer bool, cookie func(maintenanceDirectory, *syscall.Dirent) (int64, error)) (entries []maintenanceDirEntry, done bool, err error) {
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
	buffer := make([]byte, maintenanceDirentBufferSize)
	for {
		count, err := file.ReadDirent(buffer)
		if err != nil {
			return nil, false, maintenanceDirectoryError("read maintenance directory", err)
		}
		if count < 0 || count > len(buffer) {
			return nil, false, syscall.EIO
		}
		if count == 0 {
			return entries, true, nil
		}
		for data := buffer[:count]; len(data) != 0 && (len(entries) < limit || drainBuffer); {
			record, nameBytes, reclen, decodeErr := decodeMaintenanceDirent(data)
			if decodeErr != nil {
				return nil, false, decodeErr
			}
			data = data[reclen:]
			// Advance past every raw record, including dot and disappeared entries.
			offset, cookieErr := cookie(file, record)
			if cookieErr != nil {
				return nil, false, maintenanceDirectoryError("tell maintenance directory", cookieErr)
			}
			cursor.offset = offset
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
		if len(entries) >= limit {
			if drainBuffer && len(entries) > limit {
				// Darwin has already advanced the descriptor past this entire
				// buffer, so retain only this final buffer's excess.
				cursor.pending = append(cursor.pending, entries[limit:]...)
			}
			return entries[:limit], false, nil
		}
	}
}

// decodeMaintenanceDirent copies a bounded raw record into aligned storage
// before reading its fields. syscall.ReadDirent writes to a byte buffer, whose
// start and record boundaries are not guaranteed to satisfy syscall.Dirent's
// alignment requirements on Darwin.
func decodeMaintenanceDirent(data []byte) (*syscall.Dirent, []byte, int, error) {
	nameOffset := int(unsafe.Offsetof(syscall.Dirent{}.Name))
	if len(data) < nameOffset {
		return nil, nil, 0, syscall.EIO
	}

	var record syscall.Dirent
	header := unsafe.Slice((*byte)(unsafe.Pointer(&record)), nameOffset)
	copy(header, data[:nameOffset])
	reclen := int(record.Reclen)
	if reclen < nameOffset || reclen > len(data) || reclen > int(unsafe.Sizeof(record)) {
		return nil, nil, 0, syscall.EIO
	}

	recordBytes := unsafe.Slice((*byte)(unsafe.Pointer(&record)), reclen)
	copy(recordBytes, data[:reclen])
	name := unsafe.Slice((*byte)(unsafe.Pointer(&record.Name[0])), reclen-nameOffset)
	return &record, name, reclen, nil
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
