package snapshot

import (
	"context"
	"crypto/sha256"
	"fmt"

	"github.com/bnema/vev/internal/ports"
	codec "github.com/bnema/vev/internal/usecase/snapshot"
)

func (r *Repository) Publish(ctx context.Context, publication ports.SnapshotPublication) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key := sessionKey(publication.Name)
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
	// A failed publication can still leave immutable blobs or a manifest behind.
	// Invalidate an in-progress GC mark before creating any publication storage.
	r.invalidateStorageEpoch(key)
	if err := r.ensureSession(key); err != nil {
		return err
	}

	current, currentManifest, currentRefs, err := r.currentPublication(ctx, publication.Name, key)
	if err != nil {
		return err
	}
	if publication.Generation < current || publication.Generation > current+1 {
		return fmt.Errorf("snapshot generation %d, current %d: immutable conflict", publication.Generation, current)
	}
	if publication.Generation == current {
		if !equalBytes(currentManifest, publication.Manifest) {
			return fmt.Errorf("snapshot generation %d: immutable conflict", publication.Generation)
		}
		return nil
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
		path := r.objectPath(key, digest)
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
		if r.hooks.beforeBlobWrite != nil {
			if err := r.hooks.beforeBlobWrite(path); err != nil {
				return err
			}
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

	manifestPath := r.manifestPath(key, publication.Generation)
	existing, exists, err := readOptionalBounded(manifestPath)
	if err != nil {
		return err
	}
	if exists {
		if !equalBytes(existing, publication.Manifest) {
			return fmt.Errorf("manifest generation %d: immutable conflict", publication.Generation)
		}
	} else {
		if r.hooks.beforeManifestWrite != nil {
			if err := r.hooks.beforeManifestWrite(manifestPath); err != nil {
				return err
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := r.writeImmutable(manifestPath, publication.Manifest, func(existing []byte) error {
			if !equalBytes(existing, publication.Manifest) {
				return fmt.Errorf("manifest generation %d: immutable conflict", publication.Generation)
			}
			return nil
		}); err != nil {
			return fmt.Errorf("write manifest: %w", err)
		}
	}
	if r.hooks.beforeHeadWrite != nil {
		if err := r.hooks.beforeHeadWrite(r.headPath(key)); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := r.writeMutable(r.headPath(key), marshalHead(publication.Generation, sha256.Sum256(publication.Manifest))); err != nil {
		return fmt.Errorf("write HEAD: %w", err)
	}
	return nil
}

// currentPublication takes the normal HEAD path without loading every object
// in the current generation. A damaged HEAD or manifest still uses the full
// fallback scan, because only Load's object validation can establish a safe
// replacement authoritative generation in that recovery case.
func (r *Repository) currentPublication(ctx context.Context, name, key string) (uint64, []byte, map[ports.SnapshotDigest]codec.ObjectRef, error) {
	generation, digest, err := r.readHead(key)
	if err == nil {
		data, readErr := readBounded(r.manifestPath(key, generation))
		if readErr == nil && sha256.Sum256(data) == digest {
			refs, validateErr := validateManifest(data, name, generation)
			if validateErr == nil {
				return generation, data, refs, nil
			}
		}
	}

	generation, data, err := r.currentGeneration(ctx, name, key)
	if err != nil || generation == 0 {
		return generation, data, nil, err
	}
	refs, err := validateManifest(data, name, generation)
	if err != nil {
		return 0, nil, nil, err
	}
	return generation, data, refs, nil
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
		object, err := validateSuppliedObject(entries, digest, refs[digest], nil)
		if err != nil {
			return nil, nil, err
		}
		objects[digest] = object
	}
	return refs, objects, nil
}

func validatePublicationManifest(p ports.SnapshotPublication) (map[ports.SnapshotDigest]codec.ObjectRef, error) {
	return validateManifest(p.Manifest, p.Name, p.Generation)
}

func validateManifest(data []byte, name string, generation uint64) (map[ports.SnapshotDigest]codec.ObjectRef, error) {
	manifest, err := codec.UnmarshalManifest(data)
	if err != nil {
		return nil, fmt.Errorf("invalid manifest: %w", err)
	}
	if manifest.Name != name || manifest.Generation != generation || generation == 0 {
		return nil, fmt.Errorf("manifest name or generation does not match publication")
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
	return validateSuppliedObject(objects, digest, ref, r)
}

func validateSuppliedObject(objects []ports.SnapshotObject, digest ports.SnapshotDigest, ref codec.ObjectRef, repository *Repository) (ports.SnapshotObject, error) {
	object := objects[0]
	for _, candidate := range objects {
		actual := sha256.Sum256(candidate.Data)
		if repository != nil {
			actual = repository.objectDigest(candidate.Data)
		}
		if actual != candidate.Digest || candidate.Digest != digest {
			return ports.SnapshotObject{}, fmt.Errorf("object digest mismatch")
		}
		if !equalBytes(candidate.Data, object.Data) {
			return ports.SnapshotObject{}, fmt.Errorf("conflicting object")
		}
	}
	kind, payload, err := codec.PreflightObject(object.Data)
	if err != nil || kind != ref.Kind || len(object.Data) != int(ref.Size) || len(payload) == 0 {
		return ports.SnapshotObject{}, fmt.Errorf("invalid object envelope")
	}
	if repository != nil && repository.hooks.beforeObjectCopy != nil {
		repository.hooks.beforeObjectCopy(object.Data)
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
	if r.hooks.beforeObjectHash != nil {
		r.hooks.beforeObjectHash(data)
	}
	return sha256.Sum256(data)
}

func (r *Repository) verifyObjectFile(path string, digest ports.SnapshotDigest, ref codec.ObjectRef) (bool, error) {
	if r.hooks.beforeObjectRead != nil {
		r.hooks.beforeObjectRead(path)
	}
	data, exists, err := readOptionalBounded(path)
	if err != nil || !exists {
		return exists, err
	}
	if r.objectDigest(data) != digest || !validObject(data, ref) {
		return true, fmt.Errorf("existing immutable object is invalid")
	}
	return true, nil
}
