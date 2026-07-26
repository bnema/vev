package snapshot

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/bnema/vev/internal/ports"
	codec "github.com/bnema/vev/internal/usecase/snapshot"
)

// Retired name-keyed repository behavior is compiled only for pre-incarnation
// tests. Production startup and daemon paths cannot call these methods.
func (r *Repository) List(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	budget := maxDirectoryTraversalEntries
	out := make([]string, 0)
	sessions := filepath.Join(r.dir, repositorySessionsDir)
	err := r.walkDirectory(ctx, sessions, &budget, func(entry os.DirEntry) error {
		if !entry.IsDir() || !canonicalSessionKey(entry.Name()) {
			return nil
		}
		generation, ok, err := r.loadNewestGeneration(ctx, entry.Name(), &budget)
		if err != nil || !ok {
			return err
		}
		out = append(out, generation.Name)
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read snapshot sessions: %w", err)
	}
	sort.Strings(out)
	return out, nil
}

func (r *Repository) Load(ctx context.Context, name string) (ports.SnapshotGeneration, error) {
	if err := ctx.Err(); err != nil {
		return ports.SnapshotGeneration{}, err
	}
	key, err := r.legacyRepositoryKey(name)
	if err != nil {
		return ports.SnapshotGeneration{}, err
	}
	lock := r.lockSession(key)
	defer r.unlockSession(lock)
	if err := ctx.Err(); err != nil {
		return ports.SnapshotGeneration{}, err
	}
	killed, err := r.tombstoned(name)
	if err != nil {
		return ports.SnapshotGeneration{}, fmt.Errorf("read killed session marker: %w", err)
	}
	if killed {
		return ports.SnapshotGeneration{}, fmt.Errorf("snapshot session %q is killed", name)
	}
	return r.load(ctx, name, key)
}

func (r *Repository) load(ctx context.Context, name, key string) (ports.SnapshotGeneration, error) {
	preferred, preferredDigest, err := r.readLegacyHead(key)
	if err != nil && !errors.Is(err, os.ErrNotExist) && !errors.Is(err, ErrInvalidHEAD) {
		return ports.SnapshotGeneration{}, fmt.Errorf("read snapshot HEAD: %w", err)
	}
	if preferred != 0 {
		got, err := r.loadGeneration(ctx, name, key, preferred)
		if err == nil && sha256.Sum256(got.Manifest) == preferredDigest {
			return got, nil
		}
	}

	budget := maxDirectoryTraversalEntries
	candidates, err := r.generationCandidates(ctx, key, preferred, &budget)
	if err != nil {
		return ports.SnapshotGeneration{}, err
	}
	var lastLoadErr error
	for _, generation := range candidates {
		got, err := r.loadGeneration(ctx, name, key, generation)
		if err != nil {
			lastLoadErr = err
			continue
		}
		return got, nil
	}
	if lastLoadErr != nil {
		return ports.SnapshotGeneration{}, fmt.Errorf("no complete snapshot generation for %q: %w", name, lastLoadErr)
	}
	return ports.SnapshotGeneration{}, fmt.Errorf("no complete snapshot generation for %q", name)
}

func (r *Repository) currentGeneration(ctx context.Context, name, key string) (uint64, []byte, error) {
	generation, digest, err := r.readLegacyHead(key)
	if err == nil {
		got, err := r.loadGeneration(ctx, name, key, generation)
		if err == nil && sha256.Sum256(got.Manifest) == digest {
			return generation, got.Manifest, nil
		}
	} else if !errors.Is(err, os.ErrNotExist) && !errors.Is(err, ErrInvalidHEAD) {
		return 0, nil, fmt.Errorf("read snapshot HEAD: %w", err)
	}
	budget := maxDirectoryTraversalEntries
	candidates, err := r.generationCandidates(ctx, key, 0, &budget)
	if err != nil {
		return 0, nil, err
	}
	for _, generation := range candidates {
		got, err := r.loadGeneration(ctx, name, key, generation)
		if err == nil {
			return generation, got.Manifest, nil
		}
	}
	return 0, nil, nil
}

func (r *Repository) loadNewestGeneration(ctx context.Context, key string, budget *int) (ports.SnapshotGeneration, bool, error) {
	candidates, err := r.generationCandidates(ctx, key, 0, budget)
	if err != nil {
		return ports.SnapshotGeneration{}, false, err
	}
	for _, generation := range candidates {
		data, err := r.readBounded(r.legacyManifestPath(key, generation))
		if err != nil {
			continue
		}
		manifest, err := codec.UnmarshalManifest(data)
		if err != nil || sessionKey(manifest.Name) != key {
			continue
		}
		killed, err := r.tombstoned(manifest.Name)
		if err != nil {
			return ports.SnapshotGeneration{}, false, fmt.Errorf("read killed session marker: %w", err)
		}
		if killed {
			return ports.SnapshotGeneration{}, false, nil
		}
		got, err := r.loadGeneration(ctx, manifest.Name, key, generation)
		if err == nil {
			return got, true, nil
		}
	}
	return ports.SnapshotGeneration{}, false, nil
}

func (r *Repository) generationCandidates(ctx context.Context, key string, exclude uint64, budget *int) ([]uint64, error) {
	candidates := make([]uint64, 0)
	err := r.walkDirectory(ctx, filepath.Join(r.legacySessionPath(key), repositoryGenerations), budget, func(entry os.DirEntry) error {
		if entry.IsDir() {
			return nil
		}
		generation, ok := parseGenerationFilename(entry.Name())
		if ok && generation != exclude {
			candidates = append(candidates, generation)
		}
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return candidates, nil
	}
	if err != nil {
		return nil, err
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i] > candidates[j] })
	return candidates, nil
}

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
