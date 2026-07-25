package snapshot

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	codec "github.com/bnema/vev/internal/usecase/snapshot"
)

// Directory scans are deliberately finite. Snapshot directories are attacker
// controlled state, so callers get a retryable error instead of retaining an
// arbitrary number of names or spending an arbitrary amount of work.
const (
	directoryTraversalBatch      = 64
	maxDirectoryTraversalEntries = 4096
)

var (
	ErrDirectoryTraversalBudget = errors.New("snapshot directory traversal budget exceeded")
	ErrInvalidHEAD              = errors.New("invalid snapshot HEAD")
)

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
	preferred, preferredDigest, err := r.readHead(key)
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
	generation, digest, err := r.readHead(key)
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
		data, err := r.readBounded(r.manifestPath(key, generation))
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
	err := r.walkDirectory(ctx, filepath.Join(r.sessionPath(key), repositoryGenerations), budget, func(entry os.DirEntry) error {
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

// walkDirectory is the common finite directory traversal primitive for reads.
// File.ReadDir maintains a cursor on one descriptor and returns at most one
// small batch, so no full directory enumeration is retained in memory.
func (r *Repository) walkDirectory(ctx context.Context, dir string, budget *int, visit func(os.DirEntry) error) (err error) {
	if hook := r.hooks.beforeDirectoryRead; hook != nil {
		hook(dir)
	}
	file, err := r.openDirectory(dir)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := file.Close(); err == nil && closeErr != nil {
			err = closeErr
		}
	}()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		entries, readErr := file.ReadDir(directoryTraversalBatch)
		for _, entry := range entries {
			if *budget == 0 {
				return ErrDirectoryTraversalBudget
			}
			*budget--
			if err := visit(entry); err != nil {
				return err
			}
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

func (r *Repository) LoadCheckpoint(ctx context.Context, id domain.IncarnationID, name string, ref ports.CheckpointRef) (ports.SnapshotGeneration, error) {
	if err := ctx.Err(); err != nil {
		return ports.SnapshotGeneration{}, err
	}
	key, err := incarnationKey(id)
	if err != nil {
		return ports.SnapshotGeneration{}, err
	}
	lock := r.lockSession(key)
	defer r.unlockSession(lock)
	return r.loadCheckpointLocked(ctx, id, name, ref)
}

func (r *Repository) loadCheckpointLocked(ctx context.Context, id domain.IncarnationID, name string, ref ports.CheckpointRef) (ports.SnapshotGeneration, error) {
	if ref.Generation == 0 || ref.ManifestDigest == ([32]byte{}) {
		return ports.SnapshotGeneration{}, fmt.Errorf("invalid checkpoint reference")
	}
	data, err := r.readBounded(r.manifestPath(id, ref.Generation))
	if err != nil {
		return ports.SnapshotGeneration{}, err
	}
	if sha256.Sum256(data) != ref.ManifestDigest {
		return ports.SnapshotGeneration{}, fmt.Errorf("manifest digest mismatch")
	}
	manifest, err := codec.UnmarshalManifest(data)
	if err != nil || manifest.IncarnationID != id || manifest.Generation != ref.Generation {
		return ports.SnapshotGeneration{}, fmt.Errorf("invalid manifest")
	}
	refs := manifestRefs(manifest)
	if refs == nil || !withinGenerationBudget(len(data), refs) {
		return ports.SnapshotGeneration{}, fmt.Errorf("snapshot generation too large")
	}
	objects := make(map[ports.SnapshotDigest][]byte, len(refs))
	for digest, objectRef := range refs {
		if err := ctx.Err(); err != nil {
			return ports.SnapshotGeneration{}, err
		}
		object, err := r.readBounded(r.objectPath(id, digest))
		if err != nil {
			return ports.SnapshotGeneration{}, err
		}
		if sha256.Sum256(object) != digest || !validObject(object, objectRef) {
			return ports.SnapshotGeneration{}, fmt.Errorf("invalid object")
		}
		objects[digest] = object
	}
	return ports.SnapshotGeneration{IncarnationID: id, Name: name, Generation: ref.Generation, ParentCheckpoint: manifest.ParentCheckpoint, Manifest: data, Objects: objects}, nil
}

func (r *Repository) RepairHEAD(ctx context.Context, id domain.IncarnationID, ref ports.CheckpointRef) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key, err := incarnationKey(id)
	if err != nil {
		return err
	}
	lock := r.lockSession(key)
	defer r.unlockSession(lock)
	data, err := r.readBounded(r.manifestPath(id, ref.Generation))
	if err != nil {
		return fmt.Errorf("validate checkpoint before HEAD repair: %w", err)
	}
	if sha256.Sum256(data) != ref.ManifestDigest {
		return fmt.Errorf("validate checkpoint before HEAD repair: manifest digest mismatch")
	}
	manifest, err := codec.UnmarshalManifest(data)
	if err != nil || manifest.IncarnationID != id || manifest.Generation != ref.Generation {
		return fmt.Errorf("validate checkpoint before HEAD repair: invalid manifest")
	}
	if _, err := r.loadCheckpointLocked(ctx, id, manifest.Name, ref); err != nil {
		return err
	}
	return r.writeMutable(r.headPath(id), marshalHead(ref.Generation, ref.ManifestDigest))
}

func (r *Repository) loadGeneration(ctx context.Context, name, key string, generation uint64) (ports.SnapshotGeneration, error) {
	data, err := r.readBounded(r.manifestPath(key, generation))
	if err != nil {
		return ports.SnapshotGeneration{}, err
	}
	manifest, err := codec.UnmarshalManifest(data)
	if err != nil || manifest.Name != name || manifest.Generation != generation {
		return ports.SnapshotGeneration{}, fmt.Errorf("invalid manifest")
	}
	refs := manifestRefs(manifest)
	if refs == nil || !withinGenerationBudget(len(data), refs) {
		return ports.SnapshotGeneration{}, fmt.Errorf("snapshot generation too large")
	}
	objects := make(map[ports.SnapshotDigest][]byte, len(refs))
	for digest, ref := range refs {
		if err := ctx.Err(); err != nil {
			return ports.SnapshotGeneration{}, err
		}
		object, err := r.readBounded(r.objectPath(key, digest))
		if err != nil {
			return ports.SnapshotGeneration{}, err
		}
		if sha256.Sum256(object) != digest || !validObject(object, ref) {
			return ports.SnapshotGeneration{}, fmt.Errorf("invalid object")
		}
		objects[digest] = object
	}
	return ports.SnapshotGeneration{Name: manifest.Name, Generation: generation, Manifest: data, Objects: objects}, nil
}

func marshalHead(generation uint64, digest ports.SnapshotDigest) []byte {
	out := make([]byte, 4+8+sha256.Size)
	copy(out, "VEVH")
	binary.BigEndian.PutUint64(out[4:12], generation)
	copy(out[12:], digest[:])
	return out
}
func (r *Repository) readHead(key string) (uint64, ports.SnapshotDigest, error) {
	return r.readHeadWithRoot(nil, key)
}

func (r *Repository) readHeadWithRoot(root *os.Root, key string) (uint64, ports.SnapshotDigest, error) {
	path := r.headPath(key)
	if hook := r.hooks.beforeHeadRead; hook != nil {
		if err := hook(path); err != nil {
			return 0, ports.SnapshotDigest{}, err
		}
	}
	var data []byte
	var err error
	if root == nil {
		data, err = r.readBounded(path)
	} else {
		data, err = r.readBoundedRoot(root, path)
	}
	if err != nil {
		return 0, ports.SnapshotDigest{}, err
	}
	if len(data) != 4+8+sha256.Size || string(data[:4]) != "VEVH" {
		return 0, ports.SnapshotDigest{}, ErrInvalidHEAD
	}
	generation := binary.BigEndian.Uint64(data[4:12])
	var digest ports.SnapshotDigest
	copy(digest[:], data[12:])
	if generation == 0 {
		return 0, ports.SnapshotDigest{}, ErrInvalidHEAD
	}
	return generation, digest, nil
}
