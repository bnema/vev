package snapshot

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
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
	out := make([]byte, 16, 16+len(body))
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
	for _, invalid := range [][]byte{encoded[:5], append(append([]byte(nil), encoded...), 0)} {
		_, err := decodeManifestV1(invalid)
		require.Error(t, err)
	}
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
			dir := t.TempDir()
			require.NoError(t, os.Chmod(dir, 0o700))
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

func TestMigrationOver4096(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o700))
	repo := NewRepository(dir)
	name := "work"
	key := sessionKey(name)
	generations := filepath.Join(dir, "sessions", key, "generations")
	require.NoError(t, os.MkdirAll(generations, 0o700))
	for i := 1; i <= 5000; i++ {
		require.NoError(t, os.WriteFile(filepath.Join(generations, generationFilename(uint64(i))), []byte("stale"), 0o600))
	}
	legacy := legacyManifestBytes(name, 5001)
	legacyDigest := sha256.Sum256(legacy)
	require.NoError(t, os.WriteFile(filepath.Join(generations, generationFilename(5001)), legacy, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "sessions", key, "HEAD"), marshalHead(5001, legacyDigest), 0o600))
	ref, err := repo.ReadLegacyHEAD(context.Background(), name)
	require.NoError(t, err)
	require.Equal(t, uint64(5001), ref.Generation)
	id := domain.IncarnationID{1}
	migrated, err := repo.MigrateV1Checkpoint(context.Background(), ports.SnapshotMigrationRequest{LegacyName: name, IncarnationID: id, LegacyRef: ref})
	require.NoError(t, err)
	got, err := repo.LoadCheckpoint(context.Background(), id, name, migrated)
	require.NoError(t, err)
	manifest, err := codec.UnmarshalManifest(got.Manifest)
	require.NoError(t, err)
	require.Equal(t, id, manifest.IncarnationID)
	require.Nil(t, manifest.ParentCheckpoint)
	for i := 1; i <= 5000; i++ {
		require.FileExists(t, filepath.Join(generations, generationFilename(uint64(i))))
	}
}
