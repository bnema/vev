package snapshot

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	codec "github.com/bnema/vev/internal/usecase/snapshot"
)

var ErrLegacyTraversalBudgetExceeded = errors.New("snapshot: legacy presence traversal budget exceeded")

const (
	legacyPresenceEntryLimit     = 4096
	legacyIncrementalSessionsDir = "sessions"
	legacyManifestHeaderSize     = 16
)

type legacyManifestV1 struct {
	Name       string
	Generation uint64
	Objects    []legacyObjectV1
}

type legacyObjectV1 struct {
	Kind   uint8
	Digest ports.SnapshotDigest
	Size   uint64
}

func isLegacyIncrementalSessionNamespace(name string) bool { return canonicalSessionKey(name) }

func decodeManifestV1(data []byte) (legacyManifestV1, error) {
	manifest, _, err := upgradeManifestV1(data, domain.IncarnationID{1})
	if err != nil {
		return legacyManifestV1{}, err
	}
	seen := make(map[ports.SnapshotDigest]struct{})
	objects := make([]legacyObjectV1, 0)
	add := func(ref codec.ObjectRef) error {
		if _, ok := seen[ref.Digest]; ok {
			return errors.New("snapshot: duplicate legacy object digest")
		}
		seen[ref.Digest] = struct{}{}
		objects = append(objects, legacyObjectV1{Kind: uint8(ref.Kind), Digest: ref.Digest, Size: uint64(ref.Size)})
		return nil
	}
	for _, tab := range manifest.Tabs {
		for _, pane := range tab.Panes {
			for _, ref := range pane.Sealed {
				if err := add(ref); err != nil {
					return legacyManifestV1{}, err
				}
			}
			if err := add(pane.Tail); err != nil {
				return legacyManifestV1{}, err
			}
			if err := add(pane.Visible); err != nil {
				return legacyManifestV1{}, err
			}
		}
	}
	return legacyManifestV1{Name: manifest.Name, Generation: manifest.Generation, Objects: objects}, nil
}

func upgradeManifestV1(data []byte, id domain.IncarnationID) (codec.Manifest, []byte, error) {
	if len(data) < legacyManifestHeaderSize {
		return codec.Manifest{}, nil, errors.New("snapshot: short legacy manifest")
	}
	if string(data[:4]) != "VEVM" || binary.BigEndian.Uint16(data[4:6]) != 1 || binary.BigEndian.Uint16(data[6:8]) != 0 {
		return codec.Manifest{}, nil, errors.New("snapshot: invalid legacy manifest header")
	}
	n := binary.BigEndian.Uint32(data[8:12])
	if uint64(len(data)) != legacyManifestHeaderSize+uint64(n) {
		return codec.Manifest{}, nil, errors.New("snapshot: invalid legacy manifest length")
	}
	body := data[legacyManifestHeaderSize:]
	if crc32.ChecksumIEEE(body) != binary.BigEndian.Uint32(data[12:16]) || len(body) < 8 {
		return codec.Manifest{}, nil, errors.New("snapshot: invalid legacy manifest checksum")
	}
	upgradedBody := make([]byte, 0, len(body)+17)
	upgradedBody = append(upgradedBody, body[:8]...)
	upgradedBody = append(upgradedBody, id[:]...)
	upgradedBody = append(upgradedBody, 0)
	upgradedBody = append(upgradedBody, body[8:]...)
	upgraded := make([]byte, legacyManifestHeaderSize, legacyManifestHeaderSize+len(upgradedBody))
	copy(upgraded, "VEVM")
	binary.BigEndian.PutUint16(upgraded[4:6], codec.ManifestVersion)
	binary.BigEndian.PutUint32(upgraded[8:12], uint32(len(upgradedBody)))
	binary.BigEndian.PutUint32(upgraded[12:16], crc32.ChecksumIEEE(upgradedBody))
	upgraded = append(upgraded, upgradedBody...)
	manifest, err := codec.UnmarshalManifest(upgraded)
	if err != nil {
		return codec.Manifest{}, nil, fmt.Errorf("snapshot: decode legacy manifest: %w", err)
	}
	return manifest, upgraded, nil
}

func (r *Repository) HasLegacyState(ctx context.Context) (found bool, err error) {
	visited := 0
	visit := func(dir string, recognize func(os.DirEntry, os.FileInfo) bool) (found bool, err error) {
		f, err := r.openLegacyDirectory(dir)
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		defer func() {
			if closeErr := f.Close(); err == nil && closeErr != nil {
				err = closeErr
			}
		}()
		for {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			entries, readErr := f.ReadDir(directoryTraversalBatch)
			for _, entry := range entries {
				if visited >= legacyPresenceEntryLimit {
					return false, ErrLegacyTraversalBudgetExceeded
				}
				visited++
				info, infoErr := entry.Info()
				if infoErr != nil {
					return false, infoErr
				}
				if recognize(entry, info) {
					return true, nil
				}
			}
			if errors.Is(readErr, io.EOF) {
				return false, nil
			}
			if readErr != nil {
				return false, readErr
			}
		}
	}
	found, err = visit(r.dir, func(e os.DirEntry, info os.FileInfo) bool {
		return info.Mode().IsRegular() && isLegacyBlobName(e.Name())
	})
	if found || err != nil {
		return found, err
	}
	return visit(filepath.Join(r.dir, legacyIncrementalSessionsDir), func(e os.DirEntry, info os.FileInfo) bool {
		return info.IsDir() && isLegacyIncrementalSessionNamespace(e.Name())
	})
}

func isLegacyBlobName(name string) bool {
	if !strings.HasSuffix(name, ".snap") {
		return false
	}
	key := strings.TrimSuffix(name, ".snap")
	if safeNameRE.MatchString(key) && key != "." && key != ".." {
		return true
	}
	if len(key) != 41 || key[0] != '@' || strings.ToLower(key) != key {
		return false
	}
	_, err := hex.DecodeString(key[1:])
	return err == nil
}

func (r *Repository) ReadLegacyHEAD(ctx context.Context, name string) (domain.CheckpointRef, error) {
	if err := ctx.Err(); err != nil {
		return domain.CheckpointRef{}, err
	}
	key := sessionKey(name)
	generation, digest, err := r.readHead(key)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, ErrInvalidHEAD) {
			return domain.CheckpointRef{}, uncertainLegacyError("read HEAD", err)
		}
		return domain.CheckpointRef{}, fmt.Errorf("snapshot: read legacy HEAD: %w", err)
	}
	data, err := r.readBounded(r.manifestPath(key, generation))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return domain.CheckpointRef{}, uncertainLegacyError("read referenced manifest", err)
		}
		return domain.CheckpointRef{}, err
	}
	if sha256.Sum256(data) != digest {
		return domain.CheckpointRef{}, uncertainLegacyError("HEAD digest mismatch", nil)
	}
	legacy, err := decodeManifestV1(data)
	if err != nil {
		return domain.CheckpointRef{}, uncertainLegacyError("decode referenced manifest", err)
	}
	if legacy.Name != name || legacy.Generation != generation {
		return domain.CheckpointRef{}, uncertainLegacyError("manifest identity mismatch", nil)
	}
	return domain.CheckpointRef{Generation: generation, ManifestDigest: digest}, nil
}

func uncertainLegacyError(operation string, cause error) error {
	if cause == nil {
		return fmt.Errorf("%w: %s", ports.ErrLegacySnapshotUncertain, operation)
	}
	return fmt.Errorf("%w: %s: %v", ports.ErrLegacySnapshotUncertain, operation, cause)
}

func (r *Repository) MigrateV1Checkpoint(ctx context.Context, req ports.SnapshotMigrationRequest) (domain.CheckpointRef, error) {
	if err := ctx.Err(); err != nil {
		return domain.CheckpointRef{}, err
	}
	if req.IncarnationID == (domain.IncarnationID{}) || req.LegacyRef.Generation == 0 {
		return domain.CheckpointRef{}, errors.New("snapshot: invalid migration request")
	}
	legacyKey := sessionKey(req.LegacyName)
	data, err := r.readBounded(r.manifestPath(legacyKey, req.LegacyRef.Generation))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return domain.CheckpointRef{}, uncertainLegacyError("referenced manifest missing", err)
		}
		return domain.CheckpointRef{}, err
	}
	if sha256.Sum256(data) != req.LegacyRef.ManifestDigest {
		return domain.CheckpointRef{}, uncertainLegacyError("manifest digest mismatch", nil)
	}
	manifest, upgraded, err := upgradeManifestV1(data, req.IncarnationID)
	if err != nil {
		return domain.CheckpointRef{}, uncertainLegacyError("decode manifest", err)
	}
	decoded, err := decodeManifestV1(data)
	if err != nil {
		return domain.CheckpointRef{}, uncertainLegacyError("decode manifest objects", err)
	}
	if manifest.Name != req.LegacyName || manifest.Generation != req.LegacyRef.Generation {
		return domain.CheckpointRef{}, uncertainLegacyError("manifest identity mismatch", nil)
	}
	key, _ := incarnationKey(req.IncarnationID)
	lock := r.lockSession(key)
	defer r.unlockSession(lock)
	if err := r.ensureSession(key); err != nil {
		return domain.CheckpointRef{}, err
	}
	for _, ref := range decoded.Objects {
		if err := ctx.Err(); err != nil {
			return domain.CheckpointRef{}, err
		}
		object, err := r.readBounded(r.objectPath(legacyKey, ref.Digest))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return domain.CheckpointRef{}, uncertainLegacyError("referenced object missing", err)
			}
			return domain.CheckpointRef{}, err
		}
		if sha256.Sum256(object) != ref.Digest || uint64(len(object)) != ref.Size {
			return domain.CheckpointRef{}, uncertainLegacyError("invalid object", nil)
		}
		if err := r.writeImmutable(r.objectPath(req.IncarnationID, ref.Digest), object, func(existing []byte) error {
			if sha256.Sum256(existing) != ref.Digest {
				return uncertainLegacyError("migrated object conflict", nil)
			}
			return nil
		}); err != nil {
			return domain.CheckpointRef{}, err
		}
	}
	path := r.manifestPath(req.IncarnationID, manifest.Generation)
	if err := r.writeImmutable(path, upgraded, func(existing []byte) error {
		if sha256.Sum256(existing) != sha256.Sum256(upgraded) {
			return uncertainLegacyError("migrated manifest conflict", nil)
		}
		return nil
	}); err != nil {
		return domain.CheckpointRef{}, err
	}
	ref := domain.CheckpointRef{Generation: manifest.Generation, ManifestDigest: sha256.Sum256(upgraded)}
	if err := r.writeMutable(r.headPath(req.IncarnationID), marshalHead(ref.Generation, ref.ManifestDigest)); err != nil {
		return domain.CheckpointRef{}, err
	}
	return ref, nil
}

var _ ports.SnapshotMigration = (*Repository)(nil)
