package snapshot

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bnema/vev/internal/ports"
	codec "github.com/bnema/vev/internal/usecase/snapshot"
)

const maintenanceBatch = 64

// Delete makes a session unavailable by durably moving it out of the canonical
// namespace. Maintain reaps the private quarantine later; Delete never restores
// a quarantined directory.
func (r *Repository) Delete(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key := sessionKey(name)
	lock := r.sessionLock(key)
	lock.Lock()
	defer lock.Unlock()

	canonical := r.sessionPath(key)
	if _, err := os.Lstat(canonical); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("stat snapshot session %q: %w", key, safeFilesystemError(err))
	}
	quarantine := filepath.Join(filepath.Dir(canonical), ".deleting-"+key+"-"+fmt.Sprintf("%d", time.Now().UnixNano()))
	for attempt := 0; ; attempt++ {
		if _, err := os.Lstat(quarantine); errors.Is(err, os.ErrNotExist) {
			break
		} else if err != nil {
			return fmt.Errorf("stat deleting snapshot session %q: %w", key, safeFilesystemError(err))
		}
		quarantine = filepath.Join(filepath.Dir(canonical), ".deleting-"+key+"-"+fmt.Sprintf("%d-%d", time.Now().UnixNano(), attempt))
	}
	if err := r.rename(canonical, quarantine); err != nil {
		return fmt.Errorf("quarantine snapshot session %q: %w", key, safeFilesystemError(err))
	}
	if err := r.syncDirectory(filepath.Dir(canonical)); err != nil {
		return fmt.Errorf("sync deleted snapshot session directory %q: %w", key, safeFilesystemError(err))
	}
	return nil
}

// Maintain reaps a fixed number of stale entries. It serializes each session
// with publication and loading so objects written by an in-flight publication
// cannot be collected.
func (r *Repository) Maintain(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	budget := maintenanceBatch
	if err := r.removeTemps(ctx, r.dir, &budget); err != nil {
		return err
	}
	sessions := filepath.Join(r.dir, repositorySessionsDir)
	entries, err := os.ReadDir(sessions)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read snapshot maintenance sessions: %w", safeFilesystemError(err))
	}
	for _, entry := range entries {
		if budget == 0 {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		path := filepath.Join(sessions, entry.Name())
		if isQuarantine(entry.Name()) {
			if err := r.removeTree(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("remove snapshot quarantine %q: %w", maintenancePath(r.dir, path), safeFilesystemError(err))
			}
			if err := r.syncDirectory(sessions); err != nil {
				return fmt.Errorf("sync snapshot maintenance directory for %q: %w", maintenancePath(r.dir, path), safeFilesystemError(err))
			}
			budget--
			continue
		}
		if !entry.IsDir() || !canonicalSessionKey(entry.Name()) {
			continue
		}
		lock := r.sessionLock(entry.Name())
		lock.Lock()
		err := r.maintainSession(ctx, entry.Name(), &budget)
		lock.Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}

func isQuarantine(name string) bool { return strings.HasPrefix(name, ".deleting-") }

func (r *Repository) maintainSession(ctx context.Context, key string, budget *int) error {
	if err := r.removeTemps(ctx, filepath.Join(r.sessionPath(key), repositoryGenerations), budget); err != nil {
		return err
	}
	objectRoot := filepath.Join(r.sessionPath(key), repositoryObjectsDir)
	shards, err := os.ReadDir(objectRoot)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read snapshot object shards: %w", safeFilesystemError(err))
	}
	for _, shard := range shards {
		if *budget == 0 {
			return nil
		}
		if shard.IsDir() {
			if err := r.removeTemps(ctx, filepath.Join(objectRoot, shard.Name()), budget); err != nil {
				return err
			}
		}
	}

	generations, err := r.generationNumbers(key)
	if err != nil {
		return fmt.Errorf("read snapshot generations: %w", safeFilesystemError(err))
	}
	type manifestState struct {
		generation uint64
		refs       map[ports.SnapshotDigest]codec.ObjectRef
		complete   bool
	}
	states := make([]manifestState, 0, len(generations))
	complete := 0
	for _, generation := range generations {
		if err := ctx.Err(); err != nil {
			return err
		}
		state := manifestState{generation: generation}
		data, err := readBounded(r.manifestPath(key, generation))
		if err == nil {
			manifest, decodeErr := codec.UnmarshalManifest(data)
			if decodeErr == nil && manifest.Generation == generation && sessionKey(manifest.Name) == key {
				state.refs = manifestRefs(manifest)
				if state.refs != nil && withinGenerationBudget(len(data), state.refs) {
					_, loadErr := r.loadGeneration(ctx, manifest.Name, key, generation)
					state.complete = loadErr == nil
				}
			}
		}
		states = append(states, state)
	}

	retained := make(map[uint64]bool, 2)
	for _, state := range states { // generationNumbers is newest first.
		if state.complete && complete < 2 {
			retained[state.generation] = true
			complete++
		}
	}
	referenced := make(map[ports.SnapshotDigest]struct{})
	for _, state := range states {
		keep := retained[state.generation] || !state.complete
		if !keep && *budget > 0 {
			path := r.manifestPath(key, state.generation)
			if err := r.remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("remove snapshot manifest %q: %w", maintenancePath(r.dir, path), safeFilesystemError(err))
			}
			if err := r.syncDirectory(filepath.Dir(path)); err != nil {
				return fmt.Errorf("sync snapshot maintenance directory for %q: %w", maintenancePath(r.dir, path), safeFilesystemError(err))
			}
			*budget--
			continue
		}
		for digest := range state.refs { // incomplete manifests protect in-flight objects.
			referenced[digest] = struct{}{}
		}
	}
	return r.removeUnreferencedObjects(ctx, key, referenced, budget)
}

func (r *Repository) removeUnreferencedObjects(ctx context.Context, key string, referenced map[ports.SnapshotDigest]struct{}, budget *int) error {
	root := filepath.Join(r.sessionPath(key), repositoryObjectsDir)
	shards, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read snapshot objects: %w", safeFilesystemError(err))
	}
	for _, shard := range shards {
		if *budget == 0 {
			return nil
		}
		if !shard.IsDir() {
			continue
		}
		dir := filepath.Join(root, shard.Name())
		objects, err := os.ReadDir(dir)
		if err != nil {
			return fmt.Errorf("read snapshot object shard: %w", safeFilesystemError(err))
		}
		for _, object := range objects {
			if *budget == 0 {
				return nil
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			digest, ok := parseObjectDigest(object.Name())
			if !ok || object.IsDir() {
				continue
			}
			if _, used := referenced[digest]; used {
				continue
			}
			path := filepath.Join(dir, object.Name())
			if err := r.remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("remove snapshot object %q: %w", maintenancePath(r.dir, path), safeFilesystemError(err))
			}
			if err := r.syncDirectory(dir); err != nil {
				return fmt.Errorf("sync snapshot maintenance directory for %q: %w", maintenancePath(r.dir, path), safeFilesystemError(err))
			}
			*budget--
		}
	}
	return nil
}

func (r *Repository) removeTemps(ctx context.Context, dir string, budget *int) error {
	if *budget == 0 {
		return nil
	}
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read snapshot maintenance directory: %w", safeFilesystemError(err))
	}
	for _, entry := range entries {
		if *budget == 0 {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), ".tmp-") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		if err := r.remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove snapshot maintenance file %q: %w", maintenancePath(r.dir, path), safeFilesystemError(err))
		}
		if err := r.syncDirectory(dir); err != nil {
			return fmt.Errorf("sync snapshot maintenance directory for %q: %w", maintenancePath(r.dir, path), safeFilesystemError(err))
		}
		*budget--
	}
	return nil
}

func (r *Repository) removeTree(path string) error {
	if r.hooks.remove != nil {
		if err := r.hooks.remove(path); err != nil {
			return err
		}
	}
	return os.RemoveAll(path)
}

// maintenancePath avoids exposing the repository root while retaining a
// path-safe, actionable location in errors.
func maintenancePath(root, path string) string {
	rel, err := filepath.Rel(root, path)
	if err != nil || rel == "." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == ".." {
		return filepath.Base(path)
	}
	return rel
}

type safePathError struct {
	op    string
	cause error
}

func (e safePathError) Error() string { return e.op + ": " + e.cause.Error() }
func (e safePathError) Unwrap() error { return e.cause }

// safeFilesystemError preserves error matching without including a filesystem
// path from terminal-facing error text.
func safeFilesystemError(err error) error {
	var pathError *os.PathError
	if errors.As(err, &pathError) {
		return safePathError{op: pathError.Op, cause: pathError.Err}
	}
	return err
}
