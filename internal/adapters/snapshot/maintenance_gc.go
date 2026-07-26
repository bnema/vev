package snapshot

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	codec "github.com/bnema/vev/internal/usecase/snapshot"
)

// CollectGarbage applies the complete snapshot retention policy. The keep map
// must come from a catalogue that loaded and validated successfully: only then
// is an incarnation absent from keep known to be an orphan.
func (r *Repository) CollectGarbage(ctx context.Context, keep map[domain.IncarnationID]domain.CheckpointRef) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	sessions := filepath.Join(r.dir, repositorySessionsDir)
	entries, err := r.readGarbageCollectionDirectory(sessions)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read snapshot incarnations: %w", safeFilesystemError(err))
	}

	var collected error
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return errors.Join(collected, err)
		}
		if !entry.IsDir() {
			continue
		}
		var id domain.IncarnationID
		if err := id.UnmarshalText([]byte(entry.Name())); err != nil {
			continue
		}
		committed, known := keep[id]
		if !known {
			path := r.sessionPath(id)
			if err := os.RemoveAll(path); err != nil {
				collected = errors.Join(collected, fmt.Errorf("remove orphan snapshot incarnation %s: %w", id.String(), safeFilesystemError(err)))
				continue
			}
			r.log.Info("snapshot_garbage_collected", "incarnation", id.String(), "action", "remove-incarnation")
			continue
		}
		if err := r.pruneGenerations(ctx, id, committed); err != nil {
			collected = errors.Join(collected, err)
		}
	}
	if err := r.syncDirectory(sessions); err != nil {
		collected = errors.Join(collected, fmt.Errorf("sync snapshot sessions directory: %w", safeFilesystemError(err)))
	}
	return collected
}

// pruneGenerations keeps the committed generation and its immediate
// predecessor. Everything else, including a forward orphan newer than the
// catalogue commit, is removed before unreferenced objects are swept.
func (r *Repository) pruneGenerations(ctx context.Context, id domain.IncarnationID, committed domain.CheckpointRef) error {
	key, err := incarnationKey(id)
	if err != nil {
		return err
	}
	lock := r.lockSession(key)
	defer r.unlockSession(lock)

	generationsDir := filepath.Join(r.sessionPath(id), repositoryGenerations)
	entries, err := r.readGarbageCollectionDirectory(generationsDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read snapshot generations for %s: %w", id.String(), safeFilesystemError(err))
	}

	kept := make(map[uint64]struct{}, 2)
	if committed.Generation > 0 {
		kept[committed.Generation] = struct{}{}
	}
	if committed.Generation > 1 {
		kept[committed.Generation-1] = struct{}{}
	}
	references, err := r.referencesForGenerations(id, entries, kept, committed)
	if err != nil {
		return fmt.Errorf("mark snapshot objects for %s: %w", id.String(), err)
	}

	removed := 0
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		generation, canonical := parseGenerationFilename(entry.Name())
		if !canonical || entry.IsDir() {
			continue
		}
		if _, retain := kept[generation]; retain {
			continue
		}
		if err := r.remove(filepath.Join(generationsDir, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove snapshot generation %d for %s: %w", generation, id.String(), safeFilesystemError(err))
		}
		removed++
	}
	if removed > 0 {
		if err := r.syncDirectory(generationsDir); err != nil {
			return fmt.Errorf("sync snapshot generations for %s: %w", id.String(), safeFilesystemError(err))
		}
		r.log.Info("snapshot_garbage_collected", "incarnation", id.String(), "action", "remove-generations", "removed", removed)
	}
	if err := r.removeHeadForDiscardedGenerations(id, kept); err != nil {
		return err
	}
	return r.sweepUnreferencedObjects(ctx, id, references)
}

// removeHeadForDiscardedGenerations removes a repository HEAD only when its
// generation was removed by the retention policy. In particular, an
// incarnation with no committed checkpoint retains no generations, so its
// crash-published HEAD cannot make a retry conflict with an orphan.
func (r *Repository) removeHeadForDiscardedGenerations(id domain.IncarnationID, kept map[uint64]struct{}) error {
	removeHead := len(kept) == 0
	if !removeHead {
		generation, _, err := r.readHead(id)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read snapshot HEAD for %s: %w", id.String(), safeFilesystemError(err))
		}
		_, retained := kept[generation]
		removeHead = !retained
	}
	if !removeHead {
		return nil
	}
	if err := r.remove(r.headPath(id)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove snapshot HEAD for %s: %w", id.String(), safeFilesystemError(err))
	}
	if err := r.syncDirectory(r.sessionPath(id)); err != nil {
		return fmt.Errorf("sync snapshot incarnation for %s: %w", id.String(), safeFilesystemError(err))
	}
	return nil
}

func (r *Repository) referencesForGenerations(id domain.IncarnationID, entries []os.DirEntry, keep map[uint64]struct{}, committed domain.CheckpointRef) (map[ports.SnapshotDigest]struct{}, error) {
	references := make(map[ports.SnapshotDigest]struct{})
	committedFound := committed.Generation == 0
	for _, entry := range entries {
		generation, canonical := parseGenerationFilename(entry.Name())
		if !canonical || entry.IsDir() {
			continue
		}
		if _, retained := keep[generation]; !retained {
			continue
		}
		data, err := r.readBounded(r.manifestPath(id, generation))
		if err != nil {
			return nil, err
		}
		manifest, err := codec.UnmarshalManifest(data)
		if err != nil || manifest.IncarnationID != id || manifest.Generation != generation {
			return nil, fmt.Errorf("invalid retained manifest generation %d", generation)
		}
		if generation == committed.Generation {
			committedFound = true
			if sha256.Sum256(data) != committed.ManifestDigest {
				return nil, fmt.Errorf("committed manifest digest mismatch")
			}
		}
		refs := manifestRefs(manifest)
		if refs == nil || !withinGenerationBudget(len(data), refs) {
			return nil, fmt.Errorf("invalid retained manifest references generation %d", generation)
		}
		for digest := range refs {
			references[digest] = struct{}{}
		}
	}
	if !committedFound {
		return nil, fmt.Errorf("committed manifest generation %d is missing", committed.Generation)
	}
	return references, nil
}

type safePathError struct {
	op    string
	cause error
}

func (e safePathError) Error() string { return e.op + ": " + e.cause.Error() }
func (e safePathError) Unwrap() error { return e.cause }

// safeFilesystemError preserves error matching without exposing repository
// paths in terminal-facing errors.
func safeFilesystemError(err error) error {
	var linkError *os.LinkError
	if errors.As(err, &linkError) {
		return safePathError{op: linkError.Op, cause: linkError.Err}
	}
	var pathError *os.PathError
	if errors.As(err, &pathError) {
		return safePathError{op: pathError.Op, cause: pathError.Err}
	}
	return err
}

func (r *Repository) readGarbageCollectionDirectory(path string) (entries []os.DirEntry, err error) {
	directory, err := r.openDirectory(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := directory.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()
	return directory.ReadDir(-1)
}

func (r *Repository) sweepUnreferencedObjects(ctx context.Context, id domain.IncarnationID, references map[ports.SnapshotDigest]struct{}) error {
	objectsRoot := filepath.Join(r.sessionPath(id), repositoryObjectsDir)
	shards, err := r.readGarbageCollectionDirectory(objectsRoot)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read snapshot objects for %s: %w", id.String(), safeFilesystemError(err))
	}
	removed := 0
	for _, shard := range shards {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !shard.IsDir() || len(shard.Name()) != 2 {
			continue
		}
		dir := filepath.Join(objectsRoot, shard.Name())
		objects, err := r.readGarbageCollectionDirectory(dir)
		if err != nil {
			return fmt.Errorf("read snapshot object shard for %s: %w", id.String(), safeFilesystemError(err))
		}
		shardRemoved := false
		for _, object := range objects {
			digest, canonical := parseObjectDigest(object.Name())
			if !canonical || object.IsDir() {
				continue
			}
			if _, retained := references[digest]; retained {
				continue
			}
			if err := r.remove(filepath.Join(dir, object.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("remove snapshot object for %s: %w", id.String(), safeFilesystemError(err))
			}
			removed++
			shardRemoved = true
		}
		if shardRemoved {
			if err := r.syncDirectory(dir); err != nil {
				return fmt.Errorf("sync snapshot object shard for %s: %w", id.String(), safeFilesystemError(err))
			}
		}
	}
	if removed > 0 {
		r.log.Info("snapshot_garbage_collected", "incarnation", id.String(), "action", "remove-objects", "removed", removed)
	}
	return nil
}
