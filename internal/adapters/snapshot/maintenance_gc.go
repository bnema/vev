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
	codec "github.com/bnema/vev/internal/snapshotcodec"
)

// SnapshotIncarnations lists the canonical incarnation directories.
func (r *Repository) SnapshotIncarnations(ctx context.Context) ([]domain.IncarnationID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries, err := r.readGarbageCollectionDirectory(filepath.Join(r.dir, repositorySessionsDir))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read snapshot incarnations: %w", safeFilesystemError(err))
	}
	ids := make([]domain.IncarnationID, 0, len(entries))
	for _, entry := range entries {
		var id domain.IncarnationID
		if entry.IsDir() && id.UnmarshalText([]byte(entry.Name())) == nil {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

// CollectIncarnationGarbage applies the retention policy to one incarnation.
// keep must come from a catalogue that loaded and validated successfully: only
// then is a nil keep known to mark an orphan.
func (r *Repository) CollectIncarnationGarbage(ctx context.Context, id domain.IncarnationID, keep *domain.CheckpointRef) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key, err := incarnationKey(id)
	if err != nil {
		return err
	}
	lock := r.lockSession(key)
	defer r.unlockSession(lock)
	if keep == nil {
		if err := r.removeIncarnationLocked(id); err != nil {
			return fmt.Errorf("remove orphan snapshot incarnation %s: %w", id.String(), safeFilesystemError(err))
		}
		if err := r.syncDirectory(filepath.Join(r.dir, repositorySessionsDir)); err != nil {
			return fmt.Errorf("sync snapshot sessions directory: %w", safeFilesystemError(err))
		}
		r.log.Info("snapshot_garbage_collected", "incarnation", id.String(), "action", "remove-incarnation")
		return nil
	}
	return r.pruneGenerationsLocked(ctx, id, *keep)
}

// pruneGenerationsLocked keeps the committed generation and its immediate
// predecessor. Everything else, including a forward orphan newer than the
// catalogue commit, is removed before unreferenced objects are swept. The
// caller holds the incarnation's session lock.
func (r *Repository) pruneGenerationsLocked(ctx context.Context, id domain.IncarnationID, committed domain.CheckpointRef) error {
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
	// Reconcile HEAD before removing generations, so an interrupted pass never
	// leaves HEAD naming a removed manifest.
	if err := r.reconcileHeadForRetention(id, committed); err != nil {
		return err
	}

	// Generation removal runs to completion: it is bounded by the retained
	// window plus any backlog, and a half-pruned directory buys nothing.
	removed := 0
	for _, entry := range entries {
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
	return r.sweepUnreferencedObjects(ctx, id, references)
}

// reconcileHeadForRetention makes HEAD name the committed checkpoint. GC runs
// under the mutation fence, so no publication is in flight and any other HEAD
// (missing, malformed, a forward orphan, an older generation, or a digest
// mismatch) is damage: a live session's next publication must find its
// committed parent. Without a commit, no generation is retained and HEAD is
// removed so a crash-published HEAD cannot make a retry conflict with an orphan.
func (r *Repository) reconcileHeadForRetention(id domain.IncarnationID, committed domain.CheckpointRef) error {
	if committed.Generation > 0 {
		generation, digest, err := r.readHead(id)
		if err != nil && !errors.Is(err, os.ErrNotExist) && !errors.Is(err, ErrInvalidHEAD) {
			return fmt.Errorf("read snapshot HEAD for %s: %w", id.String(), safeFilesystemError(err))
		}
		if err == nil && generation == committed.Generation && digest == committed.ManifestDigest {
			return nil
		}
		// referencesForGenerations already validated the committed manifest digest.
		if err := r.writeMutable(r.headPath(id), marshalHead(committed.Generation, committed.ManifestDigest)); err != nil {
			return fmt.Errorf("rewind snapshot HEAD for %s: %w", id.String(), safeFilesystemError(err))
		}
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
	return readAndCloseDirectory(directory)
}

// readRootDirectory lists rel through an already pinned repository root, so a
// sweep over thousands of objects does not reopen and revalidate the root for
// every shard.
func readRootDirectory(root *os.Root, rel string) ([]os.DirEntry, error) {
	directory, err := openRootDirectory(root, rel)
	if err != nil {
		return nil, err
	}
	return readAndCloseDirectory(directory)
}

func readAndCloseDirectory(directory *os.File) (entries []os.DirEntry, err error) {
	defer func() {
		if closeErr := directory.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()
	return directory.ReadDir(-1)
}

func (r *Repository) sweepUnreferencedObjects(ctx context.Context, id domain.IncarnationID, references map[ports.SnapshotDigest]struct{}) (err error) {
	objectsRoot, ok := r.repositoryRelative(filepath.Join(r.sessionPath(id), repositoryObjectsDir))
	if !ok {
		return fmt.Errorf("snapshot path outside repository")
	}
	root, err := r.openRoot()
	if err != nil {
		return fmt.Errorf("open snapshot root for %s: %w", id.String(), safeFilesystemError(err))
	}
	defer func() { joinCloseError(&err, "close snapshot root", r.closeRoot(root)) }()
	shards, err := readRootDirectory(root, objectsRoot)
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
		objects, err := readRootDirectory(root, dir)
		if err != nil {
			return fmt.Errorf("read snapshot object shard for %s: %w", id.String(), safeFilesystemError(err))
		}
		// Object unlinks are deliberately not fsynced per shard. Objects are
		// immutable and content-addressed: an unlink lost to a crash only brings
		// back an unreferenced object, which the next pass collects again, and a
		// later publication revalidates any existing object before reuse.
		for _, object := range objects {
			digest, canonical := parseObjectDigest(object.Name())
			if !canonical || object.IsDir() {
				continue
			}
			if _, retained := references[digest]; retained {
				continue
			}
			if err := root.Remove(filepath.Join(dir, object.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("remove snapshot object for %s: %w", id.String(), safeFilesystemError(err))
			}
			removed++
		}
	}
	if removed > 0 {
		r.log.Info("snapshot_garbage_collected", "incarnation", id.String(), "action", "remove-objects", "removed", removed)
	}
	return nil
}
