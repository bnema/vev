package snapshot

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/bnema/vev/internal/ports"
)

const (
	legacyDeleteMarkerPrefix = ".legacy-import-receipt-"
	maxLegacyDeleteMarkers   = maxLegacySnapshotFiles
	maxLegacyDeleteNameBytes = 200
)

type legacyDeleteMarker struct {
	name   string
	digest [sha256.Size]byte
}

type legacyDirectory interface {
	ReadDir(int) ([]os.DirEntry, error)
	Close() error
}

func (r *Repository) openLegacyDirectory(path string) (legacyDirectory, error) {
	if r.hooks.openLegacyDirectory != nil {
		return r.hooks.openLegacyDirectory(path)
	}
	return r.openDirectory(path)
}

// readLegacyImportCandidate reports whether an untrusted root entry can be
// safely imported. Legacy scanning skips entries that disappear or fail secure
// descriptor validation so one hostile .snap file does not block other imports.
func (r *Repository) readLegacyImportCandidate(path string) ([]byte, bool) {
	data, err := r.readBounded(path)
	return data, err == nil
}

// LoadLegacy reads only pre-incremental root .snap files. It is deliberately
// isolated from repository reads so incremental directories are never traversed.
func (r *Repository) LoadLegacy(ctx context.Context) (out []ports.LegacySnapshot, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// One budget covers marker recovery and import. Every directory entry costs
	// work, including unrelated attacker-created names, so neither phase can be
	// made unbounded by a huge legacy root.
	budget := maxDirectoryTraversalEntries
	// A marker is an already-authorized deletion, not an import candidate. Finish
	// it before exposing any legacy blobs so process restart cannot republish one.
	if err := r.cleanupPendingLegacyDeletes(ctx, &budget); err != nil {
		return nil, err
	}
	f, err := r.openLegacyDirectory(r.dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read legacy snapshot directory: %w", safeFilesystemError(err))
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close legacy snapshot directory: %w", safeFilesystemError(closeErr)))
		}
	}()

	out = make([]ports.LegacySnapshot, 0, maxLegacySnapshotFiles)
	files := 0
	total := 0
	for {
		entries, readErr := f.ReadDir(maintenanceBatch)
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if budget == 0 {
				return nil, ErrDirectoryTraversalBudget
			}
			budget--
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".snap") {
				continue
			}
			files++
			if files > maxLegacySnapshotFiles {
				return nil, fmt.Errorf("legacy snapshot import has too many files (maximum %d)", maxLegacySnapshotFiles)
			}
			path := filepath.Join(r.dir, entry.Name())
			data, importable := r.readLegacyImportCandidate(path)
			if !importable {
				continue
			}
			if len(data) > maxLegacySnapshotBytes-total {
				return nil, fmt.Errorf("legacy snapshot import exceeds aggregate byte limit (%d bytes)", maxLegacySnapshotBytes)
			}
			total += len(data)
			name := strings.TrimSuffix(entry.Name(), ".snap")
			if strings.HasPrefix(name, "@") || !safeNameRE.MatchString(name) {
				name = ""
			}
			killed, tombstoneErr := r.tombstoned(name)
			if tombstoneErr != nil {
				return nil, fmt.Errorf("read killed session marker: %w", safeFilesystemError(tombstoneErr))
			}
			if killed {
				continue
			}
			out = append(out, ports.LegacySnapshot{Name: name, Data: data})
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, fmt.Errorf("read legacy snapshot directory: %w", safeFilesystemError(readErr))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// DeleteVerifiedLegacy records a hash-bound import receipt before deleting the
// source. LoadLegacy consumes surviving receipts after restart, but only when
// the source still has the exact bytes that were verified and published.
func (r *Repository) DeleteVerifiedLegacy(ctx context.Context, blob ports.LegacySnapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	filename := filenameForName(blob.Name)
	lock := r.lockSession("legacy-sync:" + filename)
	defer r.unlockSession(lock)
	if err := ctx.Err(); err != nil {
		return err
	}
	receipt := legacyDeleteMarker{name: blob.Name, digest: sha256.Sum256(blob.Data)}
	if err := r.authorizeLegacyDelete(receipt); err != nil {
		return err
	}
	return r.deleteAuthorizedLegacy(ctx, filename, receipt)
}

// DeleteLegacy is for deliberate session purges, which do not represent a
// verified import and therefore must not create an import receipt.
func (r *Repository) DeleteLegacy(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	filename := filenameForName(name)
	lock := r.lockSession("legacy-sync:" + filename)
	defer r.unlockSession(lock)
	return r.deleteLegacyFile(ctx, filename)
}

func legacyDeleteMarkerFilename(key string) string { return legacyDeleteMarkerPrefix + key }

func (r *Repository) authorizeLegacyDelete(receipt legacyDeleteMarker) error {
	if err := r.ensurePrivateDirectory(r.dir); err != nil {
		return fmt.Errorf("create legacy import receipt directory: %w", safeFilesystemError(err))
	}
	path := filepath.Join(r.dir, legacyDeleteMarkerFilename(sessionKey(receipt.name)))
	data := append(receipt.digest[:], receipt.name...)
	if err := r.writeImmutable(path, data, func(data []byte) error {
		if len(data) != sha256.Size+len(receipt.name) || string(data[sha256.Size:]) != receipt.name || !bytes.Equal(data[:sha256.Size], receipt.digest[:]) {
			return fmt.Errorf("legacy import receipt identity mismatch")
		}
		return nil
	}); err != nil {
		return fmt.Errorf("write legacy import receipt: %w", safeFilesystemError(err))
	}
	return nil
}

// deleteAuthorizedLegacy deletes only a source whose bytes match the durable
// receipt. A replaced source invalidates the receipt and remains importable.
func (r *Repository) deleteAuthorizedLegacy(ctx context.Context, filename string, receipt legacyDeleteMarker) error {
	if _, pending := r.pendingLegacySync.Load(filename); pending {
		if err := r.syncPendingLegacy(ctx, filename); err != nil {
			return err
		}
		return r.clearLegacyDeleteAuthorization(receipt.name)
	}
	path := filepath.Join(r.dir, filename)
	data, err := r.readBounded(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read legacy snapshot for deletion: %w", safeFilesystemError(err))
	}
	if err == nil && sha256.Sum256(data) != receipt.digest {
		return r.clearLegacyDeleteAuthorization(receipt.name)
	}
	return r.deleteLegacyFile(ctx, filename, receipt.name)
}

func (r *Repository) deleteLegacyFile(ctx context.Context, filename string, clearReceipt ...string) error {
	if _, pending := r.pendingLegacySync.Load(filename); pending {
		if err := r.syncPendingLegacy(ctx, filename); err != nil {
			return err
		}
	} else {
		path := filepath.Join(r.dir, filename)
		if err := r.remove(path); err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("delete legacy snapshot: %w", safeFilesystemError(err))
			}
			// A missing repository has no deletion to make durable. This preserves
			// idempotence for explicit purges before the snapshot root exists.
			if _, statErr := os.Lstat(r.dir); errors.Is(statErr, os.ErrNotExist) {
				return nil
			} else if statErr != nil {
				return fmt.Errorf("stat legacy snapshot directory: %w", safeFilesystemError(statErr))
			}
		}
		r.pendingLegacySync.Store(filename, struct{}{})
		if err := r.syncPendingLegacy(ctx, filename); err != nil {
			return err
		}
	}
	if len(clearReceipt) != 0 {
		return r.clearLegacyDeleteAuthorization(clearReceipt[0])
	}
	return nil
}

func (r *Repository) clearLegacyDeleteAuthorization(name string) error {
	path := filepath.Join(r.dir, legacyDeleteMarkerFilename(sessionKey(name)))
	if err := r.remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("clear legacy import receipt: %w", safeFilesystemError(err))
	}
	if err := r.syncDirectory(r.dir); err != nil {
		return fmt.Errorf("sync legacy import receipt directory: %w", safeFilesystemError(err))
	}
	return nil
}

func (r *Repository) cleanupPendingLegacyDeletes(ctx context.Context, budget *int) error {
	markers, err := r.pendingLegacyDeleteMarkers(ctx, budget)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, marker := range markers {
		filename := filenameForName(marker.name)
		lock := r.lockSession("legacy-sync:" + filename)
		err := r.deleteAuthorizedLegacy(ctx, filename, marker)
		r.unlockSession(lock)
		if err != nil {
			return fmt.Errorf("retry authorized legacy deletion: %w", err)
		}
	}
	return nil
}

func (r *Repository) pendingLegacyDeleteMarkers(ctx context.Context, budget *int) (markers []legacyDeleteMarker, err error) {
	f, err := r.openLegacyDirectory(r.dir)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close legacy deletion marker directory: %w", safeFilesystemError(closeErr)))
		}
	}()
	markers = make([]legacyDeleteMarker, 0, maxLegacyDeleteMarkers)
	for {
		entries, readErr := f.ReadDir(maintenanceBatch)
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if *budget == 0 {
				return nil, ErrDirectoryTraversalBudget
			}
			*budget--
			if entry.IsDir() || !strings.HasPrefix(entry.Name(), legacyDeleteMarkerPrefix) {
				continue
			}
			if len(markers) == maxLegacyDeleteMarkers {
				return nil, fmt.Errorf("legacy deletion retry has too many markers (maximum %d)", maxLegacyDeleteMarkers)
			}
			key := strings.TrimPrefix(entry.Name(), legacyDeleteMarkerPrefix)
			if !canonicalSessionKey(key) {
				return nil, fmt.Errorf("invalid legacy deletion marker")
			}
			path := filepath.Join(r.dir, entry.Name())
			data, dataErr := r.readBounded(path)
			if dataErr != nil || len(data) < sha256.Size || len(data)-sha256.Size > maxLegacyDeleteNameBytes {
				return nil, fmt.Errorf("invalid legacy import receipt")
			}
			name := string(data[sha256.Size:])
			if sessionKey(name) != key {
				return nil, fmt.Errorf("invalid legacy import receipt")
			}
			marker := legacyDeleteMarker{name: name}
			copy(marker.digest[:], data[:sha256.Size])
			markers = append(markers, marker)
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, fmt.Errorf("read legacy deletion markers: %w", safeFilesystemError(readErr))
		}
	}
	return markers, nil
}

// Tombstone durably marks name as logically killed before either snapshot
// source is deleted. Startup consults this marker before importing legacy or
// restoring incremental state, so a partial offline kill cannot resurrect it.
func (r *Repository) Tombstone(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key := sessionKey(name)
	lock := r.lockSession(key)
	defer r.unlockSession(lock)
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := r.ensurePrivateDirectory(r.dir); err != nil {
		return fmt.Errorf("create killed session marker directory: %w", safeFilesystemError(err))
	}
	path := filepath.Join(r.dir, tombstoneFilename(key))
	if err := r.writeImmutable(path, []byte(name), func(data []byte) error {
		if string(data) != name {
			return fmt.Errorf("killed session marker identity mismatch")
		}
		return nil
	}); err != nil {
		return fmt.Errorf("write killed session marker: %w", safeFilesystemError(err))
	}
	return nil
}

// DeleteTombstone removes a marker only after both sources have been durably
// deleted. A missing marker is idempotent for offline-kill retries.
func (r *Repository) DeleteTombstone(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key := sessionKey(name)
	lock := r.lockSession(key)
	defer r.unlockSession(lock)
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := r.remove(filepath.Join(r.dir, tombstoneFilename(key))); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("delete killed session marker: %w", safeFilesystemError(err))
	}
	if err := r.syncDirectory(r.dir); err != nil {
		return fmt.Errorf("sync killed session marker directory: %w", safeFilesystemError(err))
	}
	return nil
}

func tombstoneFilename(key string) string { return ".killed-" + key }

func (r *Repository) tombstoned(name string) (bool, error) {
	if name == "" {
		return false, nil
	}
	path := filepath.Join(r.dir, tombstoneFilename(sessionKey(name)))
	if hook := r.hooks.beforeTombstoneCheck; hook != nil {
		hook(path)
	}
	data, err := r.readBounded(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if string(data) != name {
		return false, fmt.Errorf("killed session marker identity mismatch")
	}
	return true, nil
}

func (r *Repository) syncPendingLegacy(ctx context.Context, filename string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := r.syncDirectory(r.dir); err != nil {
		return fmt.Errorf("sync legacy snapshot directory: %w", safeFilesystemError(err))
	}
	r.pendingLegacySync.Delete(filename)
	return nil
}
