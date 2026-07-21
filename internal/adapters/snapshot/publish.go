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
	lock := r.sessionLock(key)
	lock.Lock()
	defer lock.Unlock()

	refs, supplied, err := validatePublication(publication)
	if err != nil {
		return err
	}
	if err := r.ensureSession(key); err != nil {
		return err
	}

	current, currentManifest, err := r.currentGeneration(ctx, publication.Name, key)
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

	for digest, ref := range refs {
		if err := ctx.Err(); err != nil {
			return err
		}
		path := r.objectPath(key, digest)
		exists, err := verifyObjectFile(path, digest, ref)
		if err != nil {
			return fmt.Errorf("verify object %x: %w", digest, err)
		}
		if exists {
			continue
		}
		object, ok := supplied[digest]
		if !ok {
			return fmt.Errorf("missing referenced object %x", digest)
		}
		if r.hooks.beforeBlobWrite != nil {
			if err := r.hooks.beforeBlobWrite(path); err != nil {
				return err
			}
		}
		if err := r.writeImmutable(path, object.Data, func(existing []byte) error {
			if sha256.Sum256(existing) != digest || !validObject(existing, ref) {
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
	if err := r.writeMutable(r.headPath(key), marshalHead(publication.Generation, sha256.Sum256(publication.Manifest))); err != nil {
		return fmt.Errorf("write HEAD: %w", err)
	}
	return nil
}
func validatePublication(p ports.SnapshotPublication) (map[ports.SnapshotDigest]codec.ObjectRef, map[ports.SnapshotDigest]ports.SnapshotObject, error) {
	manifest, err := codec.UnmarshalManifest(p.Manifest)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid manifest: %w", err)
	}
	if manifest.Name != p.Name || manifest.Generation != p.Generation || p.Generation == 0 {
		return nil, nil, fmt.Errorf("manifest name or generation does not match publication")
	}
	refs := manifestRefs(manifest)
	if refs == nil {
		return nil, nil, fmt.Errorf("conflicting manifest references")
	}
	if !withinGenerationBudget(len(p.Manifest), refs) {
		return nil, nil, fmt.Errorf("snapshot generation too large")
	}
	objects := make(map[ports.SnapshotDigest]ports.SnapshotObject, len(p.Objects))
	for _, object := range p.Objects {
		if sha256.Sum256(object.Data) != object.Digest {
			return nil, nil, fmt.Errorf("object digest mismatch")
		}
		ref, used := refs[object.Digest]
		if !used {
			return nil, nil, fmt.Errorf("unreferenced object")
		}
		kind, payload, err := codec.PreflightObject(object.Data)
		if err != nil || kind != ref.Kind || len(object.Data) != int(ref.Size) || len(payload) == 0 {
			return nil, nil, fmt.Errorf("invalid object envelope")
		}
		if old, duplicate := objects[object.Digest]; duplicate && !equalBytes(old.Data, object.Data) {
			return nil, nil, fmt.Errorf("conflicting object")
		}
		objects[object.Digest] = ports.SnapshotObject{Digest: object.Digest, Data: append([]byte(nil), object.Data...)}
	}
	return refs, objects, nil
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

func verifyObjectFile(path string, digest ports.SnapshotDigest, ref codec.ObjectRef) (bool, error) {
	data, exists, err := readOptionalBounded(path)
	if err != nil || !exists {
		return exists, err
	}
	if sha256.Sum256(data) != digest || !validObject(data, ref) {
		return true, fmt.Errorf("existing immutable object is invalid")
	}
	return true, nil
}
