package snapshot

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	codec "github.com/bnema/vev/internal/usecase/snapshot"
)

func (r *Repository) Publish(ctx context.Context, publication ports.SnapshotPublication) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key, err := incarnationKey(publication.IncarnationID)
	if err != nil {
		return err
	}
	lock := r.lockSession(key)
	defer r.unlockSession(lock)
	if err := ctx.Err(); err != nil {
		return err
	}

	refs, err := validatePublicationManifest(publication)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// A replay may find a forward orphan left after publication but before its
	// catalogue commit. Accept it only after the canonical checkpoint loader has
	// revalidated the pointer digest, manifest, and every referenced object.
	if unchanged, err := r.unchangedPublication(ctx, publication); err != nil {
		return err
	} else if unchanged {
		return nil
	}

	current, currentManifest, currentRefs, err := r.currentIncarnationPublication(ctx, publication)
	if err != nil {
		return err
	}
	if publication.Generation < current || publication.Generation > current+1 {
		return fmt.Errorf("snapshot generation %d, current %d: immutable conflict", publication.Generation, current)
	}
	if publication.Generation == current {
		if !bytes.Equal(currentManifest, publication.Manifest) {
			return fmt.Errorf("snapshot generation %d: immutable conflict", publication.Generation)
		}
		return nil
	}

	// Parent validation and all other publication checks above happen before
	// creating repository state. A rejected child must leave the authoritative
	// checkpoint and its storage byte-for-byte unchanged.
	if err := r.ensureSession(publication.IncarnationID); err != nil {
		return err
	}

	// Retained refs from the authoritative generation were verified when their
	// immutable files were installed. They are intentionally not re-read when a
	// producer supplies them again. Every other referenced object is checked
	// from disk once, or validated and installed from the supplied bytes.
	supplied, err := suppliedNecessaryObjects(refs, currentRefs, publication.Objects)
	if err != nil {
		return err
	}
	for digest, ref := range refs {
		if retainedObject(currentRefs, digest, ref) {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		path := r.objectPath(publication.IncarnationID, digest)
		exists, err := r.verifyObjectFile(path, digest, ref)
		if err != nil {
			return fmt.Errorf("verify object %x: %w", digest, err)
		}
		if exists {
			continue
		}
		objects := supplied[digest]
		if len(objects) == 0 {
			return fmt.Errorf("missing referenced object %x", digest)
		}
		object, err := r.validateSuppliedObject(objects, digest, ref)
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := r.writeImmutable(path, object.Data, func(existing []byte) error {
			if r.objectDigest(existing) != digest || !validObject(existing, ref) {
				return fmt.Errorf("existing immutable object is invalid")
			}
			return nil
		}); err != nil {
			return fmt.Errorf("write object: %w", err)
		}
	}

	manifestPath := r.manifestPath(publication.IncarnationID, publication.Generation)
	existing, exists, err := r.readOptionalBounded(manifestPath)
	if err != nil {
		return err
	}
	if exists {
		if !bytes.Equal(existing, publication.Manifest) {
			return fmt.Errorf("manifest generation %d: immutable conflict", publication.Generation)
		}
	} else {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := r.writeImmutable(manifestPath, publication.Manifest, func(existing []byte) error {
			if !bytes.Equal(existing, publication.Manifest) {
				return fmt.Errorf("manifest generation %d: immutable conflict", publication.Generation)
			}
			return nil
		}); err != nil {
			return fmt.Errorf("write manifest: %w", err)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := r.writeMutable(r.headPath(publication.IncarnationID), marshalHead(publication.Generation, sha256.Sum256(publication.Manifest))); err != nil {
		return fmt.Errorf("write HEAD: %w", err)
	}
	return nil
}

// unchangedPublication validates an existing forward orphan through the same
// loader used for recovery. A corrupt replay must not be accepted merely
// because its manifest bytes happen to match the requested publication.
func (r *Repository) unchangedPublication(ctx context.Context, publication ports.SnapshotPublication) (bool, error) {
	generation, digest, err := r.readHead(publication.IncarnationID)
	if err != nil || generation != publication.Generation {
		return false, nil
	}
	current, _, err := r.loadCheckpointLocked(ctx, publication.IncarnationID, publication.Name, ports.CheckpointRef{
		Generation:     generation,
		ManifestDigest: digest,
	})
	if err != nil {
		return false, fmt.Errorf("validate replayed checkpoint: %w", err)
	}
	return bytes.Equal(current.Manifest, publication.Manifest), nil
}

func (r *Repository) currentIncarnationPublication(ctx context.Context, publication ports.SnapshotPublication) (uint64, []byte, map[ports.SnapshotDigest]codec.ObjectRef, error) {
	generation, digest, err := r.readHead(publication.IncarnationID)
	if errors.Is(err, os.ErrNotExist) {
		if publication.Generation != 1 || publication.ParentCheckpoint != nil {
			return 0, nil, nil, fmt.Errorf("first snapshot generation must be 1 with no parent")
		}
		return 0, nil, nil, nil
	}
	if err != nil {
		return 0, nil, nil, fmt.Errorf("read snapshot HEAD: %w", err)
	}
	data, err := r.readBounded(r.manifestPath(publication.IncarnationID, generation))
	if err != nil {
		return 0, nil, nil, fmt.Errorf("validate current checkpoint: %w", err)
	}
	if sha256.Sum256(data) != digest {
		return 0, nil, nil, fmt.Errorf("validate current checkpoint: manifest digest mismatch")
	}
	manifest, err := codec.UnmarshalManifest(data)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("validate current manifest: %w", err)
	}
	if manifest.IncarnationID != publication.IncarnationID || manifest.Generation != generation {
		return 0, nil, nil, fmt.Errorf("current manifest identity mismatch")
	}
	// Every child checkpoint is bound to the exact authoritative HEAD.
	if publication.Generation == generation+1 {
		wantParent := &domain.CheckpointRef{Generation: generation, ManifestDigest: digest}
		if !checkpointRefEqual(publication.ParentCheckpoint, wantParent) {
			return 0, nil, nil, fmt.Errorf("publication parent does not match current checkpoint")
		}
	}
	refs := manifestRefs(manifest)
	if refs == nil || !withinGenerationBudget(len(data), refs) {
		return 0, nil, nil, fmt.Errorf("invalid current generation")
	}
	return generation, data, refs, ctx.Err()
}

func checkpointRefEqual(left, right *domain.CheckpointRef) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func validatePublication(p ports.SnapshotPublication) (map[ports.SnapshotDigest]codec.ObjectRef, map[ports.SnapshotDigest]ports.SnapshotObject, error) {
	refs, err := validatePublicationManifest(p)
	if err != nil {
		return nil, nil, err
	}
	supplied, err := suppliedNecessaryObjects(refs, nil, p.Objects)
	if err != nil {
		return nil, nil, err
	}
	objects := make(map[ports.SnapshotDigest]ports.SnapshotObject, len(supplied))
	for digest, entries := range supplied {
		object, err := validateSuppliedObject(entries, digest, refs[digest])
		if err != nil {
			return nil, nil, err
		}
		objects[digest] = object
	}
	return refs, objects, nil
}

func validatePublicationManifest(p ports.SnapshotPublication) (map[ports.SnapshotDigest]codec.ObjectRef, error) {
	return validateManifest(p.Manifest, p.IncarnationID, p.Name, p.Generation, p.ParentCheckpoint)
}

func validateManifest(data []byte, incarnationID domain.IncarnationID, name string, generation uint64, parent *domain.CheckpointRef) (map[ports.SnapshotDigest]codec.ObjectRef, error) {
	manifest, err := codec.UnmarshalManifest(data)
	if err != nil {
		return nil, fmt.Errorf("invalid manifest: %w", err)
	}
	if manifest.IncarnationID != incarnationID || manifest.Name != name || manifest.Generation != generation || generation == 0 || !checkpointRefEqual(manifest.ParentCheckpoint, parent) {
		return nil, fmt.Errorf("manifest identity, name, generation, or parent does not match publication")
	}
	refs := manifestRefs(manifest)
	if refs == nil {
		return nil, fmt.Errorf("conflicting manifest references")
	}
	if !withinGenerationBudget(len(data), refs) {
		return nil, fmt.Errorf("snapshot generation too large")
	}
	return refs, nil
}

// suppliedNecessaryObjects first rejects unreferenced input without inspecting
// its payload. Objects retained from currentRefs cannot affect publication and
// are left out so callers do not pay to hash or copy a full history again.
func suppliedNecessaryObjects(refs, currentRefs map[ports.SnapshotDigest]codec.ObjectRef, supplied []ports.SnapshotObject) (map[ports.SnapshotDigest][]ports.SnapshotObject, error) {
	out := make(map[ports.SnapshotDigest][]ports.SnapshotObject)
	for _, object := range supplied {
		if _, used := refs[object.Digest]; !used {
			return nil, fmt.Errorf("unreferenced object")
		}
		if retainedObject(currentRefs, object.Digest, refs[object.Digest]) {
			continue
		}
		out[object.Digest] = append(out[object.Digest], object)
	}
	return out, nil
}

func retainedObject(currentRefs map[ports.SnapshotDigest]codec.ObjectRef, digest ports.SnapshotDigest, ref codec.ObjectRef) bool {
	currentRef, ok := currentRefs[digest]
	return ok && currentRef == ref
}

func (r *Repository) validateSuppliedObject(objects []ports.SnapshotObject, digest ports.SnapshotDigest, ref codec.ObjectRef) (ports.SnapshotObject, error) {
	return validateSuppliedObject(objects, digest, ref)
}

func validateSuppliedObject(objects []ports.SnapshotObject, digest ports.SnapshotDigest, ref codec.ObjectRef) (ports.SnapshotObject, error) {
	object := objects[0]
	for _, candidate := range objects {
		actual := sha256.Sum256(candidate.Data)
		if actual != candidate.Digest || candidate.Digest != digest {
			return ports.SnapshotObject{}, fmt.Errorf("object digest mismatch")
		}
		if !bytes.Equal(candidate.Data, object.Data) {
			return ports.SnapshotObject{}, fmt.Errorf("conflicting object")
		}
	}
	kind, payload, err := codec.PreflightObject(object.Data)
	if err != nil || kind != ref.Kind || len(object.Data) != int(ref.Size) || len(payload) == 0 {
		return ports.SnapshotObject{}, fmt.Errorf("invalid object envelope")
	}
	return ports.SnapshotObject{Digest: digest, Data: append([]byte(nil), object.Data...)}, nil
}

func withinGenerationBudget(manifestSize int, refs map[ports.SnapshotDigest]codec.ObjectRef) bool {
	if manifestSize < 0 || manifestSize > maxRepositoryRead {
		return false
	}
	total := uint64(manifestSize)
	for _, ref := range refs {
		if uint64(ref.Size) > uint64(maxRepositoryRead)-total {
			return false
		}
		total += uint64(ref.Size)
	}
	return true
}

func manifestRefs(manifest codec.Manifest) map[ports.SnapshotDigest]codec.ObjectRef {
	refs := make(map[ports.SnapshotDigest]codec.ObjectRef)
	add := func(ref codec.ObjectRef) bool {
		if old, ok := refs[ref.Digest]; ok && old != ref {
			return false
		}
		refs[ref.Digest] = ref
		return true
	}
	for _, tab := range manifest.Tabs {
		for _, pane := range tab.Panes {
			for _, ref := range pane.Sealed {
				if !add(ref) {
					return nil
				}
			}
			if !add(pane.Tail) || !add(pane.Visible) {
				return nil
			}
		}
	}
	return refs
}

func validObject(data []byte, ref codec.ObjectRef) bool {
	kind, payload, err := codec.PreflightObject(data)
	return err == nil && kind == ref.Kind && len(payload) > 0 && len(data) == int(ref.Size)
}

func (r *Repository) objectDigest(data []byte) ports.SnapshotDigest {
	return sha256.Sum256(data)
}

func (r *Repository) verifyObjectFile(path string, digest ports.SnapshotDigest, ref codec.ObjectRef) (bool, error) {
	data, exists, err := r.readOptionalBounded(path)
	if err != nil || !exists {
		return exists, err
	}
	if r.objectDigest(data) != digest || !validObject(data, ref) {
		return true, fmt.Errorf("existing immutable object is invalid")
	}
	return true, nil
}
