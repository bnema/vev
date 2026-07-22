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

var ErrDirectoryTraversalBudget = errors.New("snapshot directory traversal budget exceeded")

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
	killed, err := r.tombstoned(name)
	if err != nil {
		return ports.SnapshotGeneration{}, fmt.Errorf("read killed session marker: %w", err)
	}
	if killed {
		return ports.SnapshotGeneration{}, fmt.Errorf("snapshot session %q is killed", name)
	}
	lock := r.lockSession(key)
	defer r.unlockSession(lock)
	if err := ctx.Err(); err != nil {
		return ports.SnapshotGeneration{}, err
	}
	return r.load(ctx, name, key)
}

func (r *Repository) load(ctx context.Context, name, key string) (ports.SnapshotGeneration, error) {
	preferred, preferredDigest, err := r.readHead(key)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		preferred = 0
	}
	skipped := false
	if preferred != 0 {
		got, err := r.loadGeneration(ctx, name, key, preferred)
		if err == nil && sha256.Sum256(got.Manifest) == preferredDigest {
			return got, nil
		}
		skipped = true
	}

	budget := maxDirectoryTraversalEntries
	before := uint64(0) // zero means no upper bound.
	for {
		generation, ok, err := r.nextGeneration(ctx, key, before, preferred, &budget)
		if err != nil {
			return ports.SnapshotGeneration{}, err
		}
		if !ok {
			break
		}
		before = generation
		got, err := r.loadGeneration(ctx, name, key, generation)
		if err != nil {
			skipped = true
			continue
		}
		got.Fallback = skipped || preferred != 0
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
	}
	budget := maxDirectoryTraversalEntries
	before := uint64(0)
	for {
		generation, ok, err := r.nextGeneration(ctx, key, before, 0, &budget)
		if err != nil || !ok {
			return 0, nil, err
		}
		before = generation
		got, err := r.loadGeneration(ctx, name, key, generation)
		if err == nil {
			return generation, got.Manifest, nil
		}
	}
}

// loadNewestGeneration validates candidates in descending generation order
// without materializing the generations directory. It is used by List, whose
// name is discovered from each manifest rather than supplied by a caller.
func (r *Repository) loadNewestGeneration(ctx context.Context, key string, budget *int) (ports.SnapshotGeneration, bool, error) {
	before := uint64(0)
	for {
		generation, ok, err := r.nextGeneration(ctx, key, before, 0, budget)
		if err != nil || !ok {
			return ports.SnapshotGeneration{}, false, err
		}
		before = generation
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
}

// nextGeneration finds one candidate in a complete, batched directory cursor
// pass. Repeating it with the returned number as before preserves strict
// newest-to-oldest fallback while work remains bounded by budget.
func (r *Repository) nextGeneration(ctx context.Context, key string, before, exclude uint64, budget *int) (uint64, bool, error) {
	var newest uint64
	err := r.walkDirectory(ctx, filepath.Join(r.sessionPath(key), repositoryGenerations), budget, func(entry os.DirEntry) error {
		if entry.IsDir() {
			return nil
		}
		generation, ok := parseGenerationFilename(entry.Name())
		if !ok || generation == exclude || (before != 0 && generation >= before) {
			return nil
		}
		if generation > newest {
			newest = generation
		}
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return newest, newest != 0, nil
}

// walkDirectory is the common finite directory traversal primitive for reads.
// File.ReadDir maintains a cursor on one descriptor and returns at most one
// small batch, so no full directory enumeration is retained in memory.
func (r *Repository) walkDirectory(ctx context.Context, dir string, budget *int, visit func(os.DirEntry) error) (err error) {
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
	data, err := r.readBounded(r.headPath(key))
	if err != nil {
		return 0, ports.SnapshotDigest{}, err
	}
	if len(data) != 4+8+sha256.Size || string(data[:4]) != "VEVH" {
		return 0, ports.SnapshotDigest{}, fmt.Errorf("invalid HEAD")
	}
	generation := binary.BigEndian.Uint64(data[4:12])
	var digest ports.SnapshotDigest
	copy(digest[:], data[12:])
	if generation == 0 {
		return 0, ports.SnapshotDigest{}, fmt.Errorf("invalid HEAD")
	}
	return generation, digest, nil
}
