package snapshot

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	codec "github.com/bnema/vev/internal/usecase/snapshot"
)

var ErrInvalidHEAD = errors.New("invalid snapshot HEAD")

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

func (r *Repository) ReconcileCheckpoint(ctx context.Context, id domain.IncarnationID, ref ports.CheckpointRef) error {
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
