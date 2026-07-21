package snapshot

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/bnema/vev/internal/ports"
	codec "github.com/bnema/vev/internal/usecase/snapshot"
)

func (r *Repository) List(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(filepath.Join(r.dir, repositorySessionsDir))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read snapshot sessions: %w", err)
	}
	out := make([]string, 0, len(entries))
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !entry.IsDir() || !canonicalSessionKey(entry.Name()) {
			continue
		}
		numbers, err := r.generationNumbers(entry.Name())
		if err != nil {
			continue
		}
		for _, number := range numbers {
			data, err := readBounded(r.manifestPath(entry.Name(), number))
			if err != nil {
				continue
			}
			manifest, err := codec.UnmarshalManifest(data)
			if err != nil || sessionKey(manifest.Name) != entry.Name() {
				continue
			}
			generation, err := r.loadGeneration(ctx, manifest.Name, entry.Name(), number)
			if err == nil {
				out = append(out, generation.Name)
				break
			}
		}
	}
	sort.Strings(out)
	return out, nil
}

func (r *Repository) Load(ctx context.Context, name string) (ports.SnapshotGeneration, error) {
	if err := ctx.Err(); err != nil {
		return ports.SnapshotGeneration{}, err
	}
	key := sessionKey(name)
	lock := r.sessionLock(key)
	lock.Lock()
	defer lock.Unlock()
	return r.load(ctx, name, key)
}

func (r *Repository) load(ctx context.Context, name, key string) (ports.SnapshotGeneration, error) {
	preferred, preferredDigest, err := r.readHead(key)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		preferred = 0
	}
	candidates, err := r.generationNumbers(key)
	if err != nil {
		return ports.SnapshotGeneration{}, err
	}
	if preferred != 0 {
		candidates = append([]uint64{preferred}, removeGeneration(candidates, preferred)...)
	}
	skipped := false
	for _, generation := range candidates {
		if err := ctx.Err(); err != nil {
			return ports.SnapshotGeneration{}, err
		}
		got, err := r.loadGeneration(ctx, name, key, generation)
		if err != nil || (generation == preferred && sha256.Sum256(got.Manifest) != preferredDigest) {
			skipped = true
			continue
		}
		got.Fallback = skipped || (preferred != 0 && generation != preferred)
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
	candidates, err := r.generationNumbers(key)
	if err != nil {
		return 0, nil, err
	}
	for _, candidate := range candidates {
		if got, err := r.loadGeneration(ctx, name, key, candidate); err == nil {
			return candidate, got.Manifest, nil
		}
	}
	return 0, nil, nil
}

func (r *Repository) loadGeneration(ctx context.Context, name, key string, generation uint64) (ports.SnapshotGeneration, error) {
	data, err := readBounded(r.manifestPath(key, generation))
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
		object, err := readBounded(r.objectPath(key, digest))
		if err != nil {
			return ports.SnapshotGeneration{}, err
		}
		if sha256.Sum256(object) != digest || !validObject(object, ref) {
			return ports.SnapshotGeneration{}, fmt.Errorf("invalid object")
		}
		// readBounded returns a fresh caller-owned buffer, so returning it directly
		// avoids a second allocation without retaining it in the repository.
		objects[digest] = object
	}
	return ports.SnapshotGeneration{Name: manifest.Name, Generation: generation, Manifest: data, Objects: objects}, nil
}

func (r *Repository) generationNumbers(key string) ([]uint64, error) {
	entries, err := os.ReadDir(filepath.Join(r.sessionPath(key), repositoryGenerations))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	numbers := make([]uint64, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if generation, ok := parseGenerationFilename(entry.Name()); ok {
			numbers = append(numbers, generation)
		}
	}
	sort.Slice(numbers, func(i, j int) bool { return numbers[i] > numbers[j] })
	return numbers, nil
}
func removeGeneration(numbers []uint64, generation uint64) []uint64 {
	out := numbers[:0]
	for _, n := range numbers {
		if n != generation {
			out = append(out, n)
		}
	}
	return out
}

func marshalHead(generation uint64, digest ports.SnapshotDigest) []byte {
	out := make([]byte, 4+8+sha256.Size)
	copy(out, "VEVH")
	binary.BigEndian.PutUint64(out[4:12], generation)
	copy(out[12:], digest[:])
	return out
}
func (r *Repository) readHead(key string) (uint64, ports.SnapshotDigest, error) {
	data, err := readBounded(r.headPath(key))
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
