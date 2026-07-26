package snapshot

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	codec "github.com/bnema/vev/internal/usecase/snapshot"
	"github.com/stretchr/testify/require"
)

func legacyManifestBytes(name string, generation uint64) []byte {
	body := binary.BigEndian.AppendUint64(nil, generation)
	body = binary.BigEndian.AppendUint16(body, uint16(len(name)))
	body = append(body, name...)
	body = binary.BigEndian.AppendUint64(body, 1)
	body = binary.BigEndian.AppendUint16(body, 0)
	body = binary.BigEndian.AppendUint16(body, 0)
	return legacyEnvelope(body)
}

func legacyBodyFromModern(modern []byte) []byte {
	body := append([]byte(nil), modern[legacyManifestHeaderSize:]...)
	const generationSize = 8
	modernOnly := generationSize + len(domain.IncarnationID{}) + 1 // incarnation and parent flag follow generation in v2.
	return append(body[:generationSize], body[modernOnly:]...)
}

func legacyEnvelope(body []byte) []byte {
	out := make([]byte, legacyManifestHeaderSize, legacyManifestHeaderSize+len(body))
	copy(out, "VEVM")
	binary.BigEndian.PutUint16(out[4:6], 1)
	binary.BigEndian.PutUint32(out[8:12], uint32(len(body)))
	binary.BigEndian.PutUint32(out[12:16], crc32.ChecksumIEEE(body))
	return append(out, body...)
}

func TestLegacyManifestV1(t *testing.T) {
	encoded := legacyManifestBytes("work", 7)
	got, err := decodeManifestV1(encoded)
	require.NoError(t, err)
	require.Equal(t, "work", got.Name)
	require.Equal(t, uint64(7), got.Generation)
	for size := range len(encoded) {
		_, err := decodeManifestV1(encoded[:size])
		require.Errorf(t, err, "prefix length %d", size)
	}
	_, err = decodeManifestV1(append(append([]byte(nil), encoded...), 0))
	require.Error(t, err)
}

func TestLegacyManifestV1DeduplicatesObjectsPreservingFirst(t *testing.T) {
	chunk, err := codec.MarshalObject(codec.HistoryChunk, []byte("chunk"))
	require.NoError(t, err)
	tail, err := codec.MarshalObject(codec.HistoryTail, []byte("tail"))
	require.NoError(t, err)
	visible, err := codec.MarshalObject(codec.Visible, []byte("visible"))
	require.NoError(t, err)
	chunkRef := codec.ObjectRef{Kind: codec.HistoryChunk, Digest: chunk.Digest, Size: uint32(len(chunk.Data))}
	manifest := codec.Manifest{Generation: 1, IncarnationID: domain.IncarnationID{1}, Name: "work", Tabs: []codec.ManifestTab{{Cols: 1, Rows: 1, Panes: []codec.ManifestPane{{ID: "p", Sealed: []codec.ObjectRef{chunkRef, chunkRef}, Tail: codec.ObjectRef{Kind: codec.HistoryTail, Digest: tail.Digest, Size: uint32(len(tail.Data))}, Visible: codec.ObjectRef{Kind: codec.Visible, Digest: visible.Digest, Size: uint32(len(visible.Data))}}}}}}
	modern, err := codec.MarshalManifest(manifest)
	require.NoError(t, err)
	legacy := legacyEnvelope(legacyBodyFromModern(modern))

	decoded, err := decodeManifestV1(legacy)
	require.NoError(t, err)
	require.Len(t, decoded.Objects, 3)
	require.Equal(t, chunk.Digest, decoded.Objects[0].Digest)
}

func TestUncertainLegacyErrorWrapsSentinelAndCause(t *testing.T) {
	cause := errors.New("legacy cause")
	err := uncertainLegacyError("read", cause)
	require.ErrorIs(t, err, ports.ErrLegacySnapshotUncertain)
	require.ErrorIs(t, err, cause)
}

func TestHasLegacyState(t *testing.T) {
	for _, tc := range []struct {
		name   string
		setup  func(string)
		want   bool
		budget bool
	}{
		{"absence", func(string) {}, false, false},
		{"top level blob", func(dir string) {
			require.NoError(t, os.WriteFile(filepath.Join(dir, "work.snap"), []byte("x"), 0o600))
		}, true, false},
		{"incremental child", func(dir string) { require.NoError(t, os.MkdirAll(filepath.Join(dir, "sessions", "work"), 0o700)) }, true, false},
		{"budget exhaustion", func(dir string) {
			for i := 0; i <= legacyPresenceEntryLimit; i++ {
				require.NoError(t, os.WriteFile(filepath.Join(dir, fmt.Sprintf("x-%04d", i)), nil, 0o600))
			}
		}, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := privateDir(t)
			require.NoError(t, NewRepository(dir).ensurePrivateDirectory(dir))
			tc.setup(dir)
			got, err := NewRepository(dir).HasLegacyState(context.Background())
			if tc.budget {
				require.ErrorIs(t, err, ErrLegacyTraversalBudgetExceeded)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.want, got)
			}
		})
	}
}

func TestMigrateV1CheckpointValidatesObjectEnvelopeAndKind(t *testing.T) {
	for _, tc := range []struct {
		name   string
		object func(t *testing.T) ports.SnapshotObject
	}{
		{"malformed envelope", func(t *testing.T) ports.SnapshotObject {
			object, err := codec.MarshalObject(codec.HistoryTail, []byte("payload"))
			require.NoError(t, err)
			object.Data[0] = 'X'
			object.Digest = sha256.Sum256(object.Data)
			return object
		}},
		{"kind mismatch", func(t *testing.T) ports.SnapshotObject {
			object, err := codec.MarshalObject(codec.Visible, []byte("wrong kind"))
			require.NoError(t, err)
			return object
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := privateDir(t)
			repo := NewRepository(dir)
			object := tc.object(t)
			visible, err := codec.MarshalObject(codec.Visible, []byte("visible"))
			require.NoError(t, err)
			manifest := codec.Manifest{Generation: 1, IncarnationID: domain.IncarnationID{1}, Name: "work", Tabs: []codec.ManifestTab{{Cols: 1, Rows: 1, Panes: []codec.ManifestPane{{ID: "p", Tail: codec.ObjectRef{Kind: codec.HistoryTail, Digest: object.Digest, Size: uint32(len(object.Data))}, Visible: codec.ObjectRef{Kind: codec.Visible, Digest: visible.Digest, Size: uint32(len(visible.Data))}}}}}}
			modern, err := codec.MarshalManifest(manifest)
			require.NoError(t, err)
			legacyManifest := legacyEnvelope(legacyBodyFromModern(modern))
			key := sessionKey("work")
			for _, stored := range []ports.SnapshotObject{object, visible} {
				require.NoError(t, os.MkdirAll(filepath.Dir(repo.legacyObjectPath(key, stored.Digest)), 0o700))
				require.NoError(t, os.WriteFile(repo.legacyObjectPath(key, stored.Digest), stored.Data, 0o600))
			}
			manifestPath := repo.legacyManifestPath(key, 1)
			require.NoError(t, os.MkdirAll(filepath.Dir(manifestPath), 0o700))
			require.NoError(t, os.WriteFile(manifestPath, legacyManifest, 0o600))
			legacyRef := domain.CheckpointRef{Generation: 1, ManifestDigest: sha256.Sum256(legacyManifest)}

			_, err = repo.MigrateV1Checkpoint(context.Background(), ports.SnapshotMigrationRequest{LegacyName: "work", IncarnationID: domain.IncarnationID{9}, LegacyRef: legacyRef})
			require.ErrorIs(t, err, ports.ErrLegacySnapshotUncertain)
			require.FileExists(t, manifestPath)
		})
	}
}

func TestMigrateV1CheckpointAcceptsLegacyNilParentBeyondGenerationOne(t *testing.T) {
	dir := privateDir(t)
	repo := NewRepository(dir)
	require.NoError(t, repo.ensurePrivateDirectory(dir))
	name := "work"
	key := sessionKey(name)
	generations := filepath.Join(dir, "sessions", key, "generations")
	require.NoError(t, os.MkdirAll(generations, 0o700))
	// Place the authoritative generation just beyond the bounded legacy scan.
	highestGeneration := maxDirectoryTraversalEntries + 1
	for i := 1; i < highestGeneration; i++ {
		require.NoError(t, os.WriteFile(filepath.Join(generations, generationFilename(uint64(i))), []byte("stale"), 0o600))
	}
	legacy := legacyManifestBytes(name, uint64(highestGeneration))
	legacyDigest := sha256.Sum256(legacy)
	require.NoError(t, os.WriteFile(filepath.Join(generations, generationFilename(uint64(highestGeneration))), legacy, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "sessions", key, "HEAD"), marshalHead(uint64(highestGeneration), legacyDigest), 0o600))
	ref, err := repo.ReadLegacyHEAD(context.Background(), name)
	require.NoError(t, err)
	require.Equal(t, uint64(highestGeneration), ref.Generation)
	id := domain.IncarnationID{1}
	migrated, err := repo.MigrateV1Checkpoint(context.Background(), ports.SnapshotMigrationRequest{LegacyName: name, IncarnationID: id, LegacyRef: ref})
	require.NoError(t, err)
	got, err := repo.LoadCheckpoint(context.Background(), id, name, migrated)
	require.NoError(t, err)
	manifest, err := codec.UnmarshalManifest(got.Manifest)
	require.NoError(t, err)
	require.Equal(t, id, manifest.IncarnationID)
	require.Nil(t, manifest.ParentCheckpoint)
	for i := 1; i < highestGeneration; i++ {
		require.FileExists(t, filepath.Join(generations, generationFilename(uint64(i))))
	}
}
