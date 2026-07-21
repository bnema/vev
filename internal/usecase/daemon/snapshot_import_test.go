package daemon

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
	snapcodec "github.com/bnema/vev/internal/usecase/snapshot"
)

type snapshotAcceptanceLegacySource struct {
	blobs       []ports.LegacySnapshot
	loadCalls   int
	deleteCalls []string
	deleteErr   error
}

func (s *snapshotAcceptanceLegacySource) LoadLegacy(ctx context.Context) ([]ports.LegacySnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.loadCalls++
	out := make([]ports.LegacySnapshot, len(s.blobs))
	for i, blob := range s.blobs {
		out[i] = ports.LegacySnapshot{Name: blob.Name, Data: append([]byte(nil), blob.Data...)}
	}
	return out, nil
}

func (s *snapshotAcceptanceLegacySource) DeleteLegacy(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.deleteCalls = append(s.deleteCalls, name)
	if s.deleteErr != nil {
		return s.deleteErr
	}
	for i, blob := range s.blobs {
		if blob.Name == name {
			s.blobs = append(s.blobs[:i], s.blobs[i+1:]...)
			break
		}
	}
	return nil
}

func legacyAcceptanceBlob(t *testing.T, snapshot snapcodec.Session) ports.LegacySnapshot {
	t.Helper()
	data, err := snapcodec.Marshal(snapshot)
	require.NoError(t, err)
	return ports.LegacySnapshot{Name: snapshot.Name, Data: data}
}

func TestImportLegacyPublishesCompleteGenerationBeforeDeletingSource(t *testing.T) {
	snapshot := restoreAcceptanceSession(t, "legacy")
	repository := &snapshotAcceptanceRepository{generations: make(map[string]ports.SnapshotGeneration)}
	legacy := &snapshotAcceptanceLegacySource{blobs: []ports.LegacySnapshot{legacyAcceptanceBlob(t, snapshot)}}
	d := newTestDaemon(t, nil, stubClock{})
	WithSnapshotRepository(repository, legacy)(d)
	expected, verifyErr := legacyPublication(snapshot)
	require.NoError(t, verifyErr)
	_, verifyErr = snapcodec.Unmarshal(legacy.blobs[0].Data)
	require.NoError(t, verifyErr)
	require.NoError(t, verifyLegacyImportGeneration(acceptanceGeneration(t, snapshot, 1), expected))

	d.importLegacySnapshots(context.Background())

	require.Len(t, repository.publishes, 1)
	publication := repository.publishes[0]
	require.Equal(t, snapshot.Name, publication.Name)
	require.EqualValues(t, 1, publication.Generation)
	manifest, err := snapcodec.UnmarshalManifest(publication.Manifest)
	require.NoError(t, err)
	require.Equal(t, uint64(1), manifest.Generation)
	require.Len(t, manifest.Tabs[0].Panes[0].Sealed, 2)
	for _, object := range publication.Objects {
		kind, _, err := snapcodec.UnmarshalObject(object.Data)
		require.NoError(t, err)
		require.Contains(t, []snapcodec.ObjectKind{snapcodec.HistoryChunk, snapcodec.HistoryTail, snapcodec.Visible}, kind)
		require.Equal(t, object.Digest, snapcodec.ManifestDigest(object.Data))
	}
	require.Equal(t, []string{snapshot.Name}, legacy.deleteCalls)
	require.Empty(t, legacy.blobs)
}

func TestImportLegacyExistingIncrementalSkipsLegacy(t *testing.T) {
	snapshot := restoreAcceptanceSession(t, "already-incremental")
	repository := &snapshotAcceptanceRepository{names: []string{snapshot.Name}, generations: make(map[string]ports.SnapshotGeneration)}
	legacy := &snapshotAcceptanceLegacySource{blobs: []ports.LegacySnapshot{legacyAcceptanceBlob(t, snapshot)}}
	d := newTestDaemon(t, nil, stubClock{})
	WithSnapshotRepository(repository, legacy)(d)

	d.importLegacySnapshots(context.Background())

	require.Empty(t, repository.publishes)
	require.Empty(t, legacy.deleteCalls)
	require.Len(t, legacy.blobs, 1)
}

func TestImportLegacyRetainsSourceUntilExactReloadVerification(t *testing.T) {
	snapshot := restoreAcceptanceSession(t, "retain")
	for _, test := range []struct {
		name    string
		publish error
		load    error
		mutate  func(ports.SnapshotGeneration, ports.SnapshotPublication) ports.SnapshotGeneration
	}{
		{"publish failure", errors.New("publish failed"), nil, nil},
		{"reload failure", nil, errors.New("reload failed"), nil},
		{"manifest metadata mismatch", nil, nil, func(g ports.SnapshotGeneration, _ ports.SnapshotPublication) ports.SnapshotGeneration {
			g.Generation++
			return g
		}},
		{"object bytes mismatch", nil, nil, func(g ports.SnapshotGeneration, _ ports.SnapshotPublication) ports.SnapshotGeneration {
			for digest, data := range g.Objects {
				data[len(data)-1] ^= 1
				g.Objects[digest] = data
				break
			}
			return g
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			repository := &snapshotAcceptanceRepository{generations: make(map[string]ports.SnapshotGeneration), publishErr: test.publish, loadErr: test.load, loadMutate: test.mutate}
			legacy := &snapshotAcceptanceLegacySource{blobs: []ports.LegacySnapshot{legacyAcceptanceBlob(t, snapshot)}}
			d := newTestDaemon(t, nil, stubClock{})
			WithSnapshotRepository(repository, legacy)(d)
			d.importLegacySnapshots(context.Background())
			require.Empty(t, legacy.deleteCalls)
			require.Len(t, legacy.blobs, 1)
		})
	}
}

func TestImportLegacyDeleteFailureRetriesWithoutRepublishing(t *testing.T) {
	snapshot := restoreAcceptanceSession(t, "retry-delete")
	repository := &snapshotAcceptanceRepository{generations: make(map[string]ports.SnapshotGeneration)}
	legacy := &snapshotAcceptanceLegacySource{blobs: []ports.LegacySnapshot{legacyAcceptanceBlob(t, snapshot)}, deleteErr: errors.New("sync failed")}
	d := newTestDaemon(t, nil, stubClock{})
	WithSnapshotRepository(repository, legacy)(d)

	d.importLegacySnapshots(context.Background())
	legacy.deleteErr = nil
	d.importLegacySnapshots(context.Background())

	require.Len(t, repository.publishes, 1, "a retry must only retry source deletion")
	require.Equal(t, []string{snapshot.Name, snapshot.Name}, legacy.deleteCalls)
	require.Empty(t, legacy.blobs)
}

func TestImportLegacyCancellationAndPerSessionContinuation(t *testing.T) {
	bad := restoreAcceptanceSession(t, "bad")
	good := restoreAcceptanceSession(t, "good")
	badBlob := legacyAcceptanceBlob(t, bad)
	badBlob.Data = []byte("not-a-v3-snapshot")
	repository := &snapshotAcceptanceRepository{generations: make(map[string]ports.SnapshotGeneration)}
	legacy := &snapshotAcceptanceLegacySource{blobs: []ports.LegacySnapshot{badBlob, legacyAcceptanceBlob(t, good)}}
	d := newTestDaemon(t, nil, stubClock{})
	WithSnapshotRepository(repository, legacy)(d)

	d.importLegacySnapshots(context.Background())
	require.Len(t, repository.publishes, 1, "a corrupt session must not block later sessions")
	require.Equal(t, []string{good.Name}, legacy.deleteCalls)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	d.importLegacySnapshots(cancelled)
	require.Equal(t, 1, legacy.loadCalls, "a canceled import must not touch the legacy source")
}
