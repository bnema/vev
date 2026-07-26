package snapshot

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

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
	generation, _, err := r.loadCheckpointLocked(ctx, id, name, ref)
	return generation, err
}

func (r *Repository) loadCheckpointLocked(ctx context.Context, id domain.IncarnationID, name string, ref ports.CheckpointRef) (ports.SnapshotGeneration, codec.Manifest, error) {
	if ref.Generation == 0 || ref.ManifestDigest == ([32]byte{}) {
		return ports.SnapshotGeneration{}, codec.Manifest{}, fmt.Errorf("invalid checkpoint reference")
	}
	data, err := r.readBounded(r.manifestPath(id, ref.Generation))
	if err != nil {
		return ports.SnapshotGeneration{}, codec.Manifest{}, err
	}
	if sha256.Sum256(data) != ref.ManifestDigest {
		return ports.SnapshotGeneration{}, codec.Manifest{}, fmt.Errorf("manifest digest mismatch")
	}
	manifest, err := codec.UnmarshalManifest(data)
	if err != nil || manifest.IncarnationID != id || manifest.Generation != ref.Generation {
		return ports.SnapshotGeneration{}, codec.Manifest{}, fmt.Errorf("invalid manifest")
	}
	refs := manifestRefs(manifest)
	if refs == nil || !withinGenerationBudget(len(data), refs) {
		return ports.SnapshotGeneration{}, codec.Manifest{}, fmt.Errorf("snapshot generation too large")
	}
	objects := make(map[ports.SnapshotDigest][]byte, len(refs))
	for digest, objectRef := range refs {
		if err := ctx.Err(); err != nil {
			return ports.SnapshotGeneration{}, codec.Manifest{}, err
		}
		object, err := r.readBounded(r.objectPath(id, digest))
		if err != nil {
			return ports.SnapshotGeneration{}, codec.Manifest{}, err
		}
		if sha256.Sum256(object) != digest || !validObject(object, objectRef) {
			return ports.SnapshotGeneration{}, codec.Manifest{}, fmt.Errorf("invalid object")
		}
		objects[digest] = object
	}
	return ports.SnapshotGeneration{IncarnationID: id, Name: name, Generation: ref.Generation, ParentCheckpoint: manifest.ParentCheckpoint, Manifest: data, Objects: objects}, manifest, nil
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
	_, manifest, err := r.loadCheckpointLocked(ctx, id, "", ref)
	if err != nil {
		return fmt.Errorf("validate checkpoint before HEAD repair: %w", err)
	}
	if manifest.Name == "" {
		return fmt.Errorf("validate checkpoint before HEAD repair: invalid manifest name")
	}
	return r.writeMutable(r.headPath(id), marshalHead(ref.Generation, ref.ManifestDigest))
}

func (r *Repository) loadGeneration(ctx context.Context, name, key string, generation uint64) (ports.SnapshotGeneration, error) {
	data, err := r.readBounded(r.legacyManifestPath(key, generation))
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
		object, err := r.readBounded(r.legacyObjectPath(key, digest))
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
func (r *Repository) readHead(id domain.IncarnationID) (uint64, ports.SnapshotDigest, error) {
	return r.readHeadWithRoot(nil, id)
}

func (r *Repository) readHeadWithRoot(root *os.Root, id domain.IncarnationID) (uint64, ports.SnapshotDigest, error) {
	return r.readHeadAt(root, r.headPath(id))
}

func (r *Repository) readHeadAt(root *os.Root, path string) (uint64, ports.SnapshotDigest, error) {
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

func (r *Repository) readLegacyHead(key string) (uint64, ports.SnapshotDigest, error) {
	return r.readHeadAt(nil, r.legacyHeadPath(key))
}
