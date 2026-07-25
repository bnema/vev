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
	key := sessionKey(name)
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
	skipped := errors.Is(err, ErrInvalidHEAD)
	if err != nil && !errors.Is(err, os.ErrNotExist) && !errors.Is(err, ErrInvalidHEAD) {
		return ports.SnapshotGeneration{}, fmt.Errorf("read snapshot HEAD: %w", err)
	}
	if preferred != 0 {
		got, err := r.loadGeneration(ctx, name, key, preferred)
		if err == nil && sha256.Sum256(got.Manifest) == preferredDigest {
			return got, nil
		}
		skipped = true
	}

	budget := maxDirectoryTraversalEntries
	candidates, err := r.generationCandidates(ctx, key, preferred, &budget)
	if err != nil {
		return ports.SnapshotGeneration{}, err
	}
	for _, generation := range candidates {
		got, err := r.loadGeneration(ctx, name, key, generation)
		if err != nil {
			skipped = true
			continue
		}
		_ = skipped
		return got, nil
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

// loadNewestGeneration validates a single bounded candidate batch in strict
// descending generation order. It is used by List, whose name is discovered
// from each manifest rather than supplied by a caller.
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

// generationCandidates reads the generations directory once per load attempt.
// The shared traversal budget bounds both retained names and work; sorting the
// bounded batch preserves strict newest-to-oldest fallback without rescanning.
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
