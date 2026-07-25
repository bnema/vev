package persist

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/kv"
	"github.com/stretchr/testify/require"
)

type migrationStub struct {
	legacy      bool
	probeErr    error
	heads       map[string]domain.CheckpointRef
	migrateErr  map[string]error
	assignments map[string]domain.IncarnationID
	sources     map[string]string
}

func (s *migrationStub) HasLegacyState(context.Context) (bool, error) { return s.legacy, s.probeErr }
func (s *migrationStub) ReadLegacyHEAD(_ context.Context, name string) (domain.CheckpointRef, error) {
	if source := s.sources[name]; source != "" {
		if _, err := os.Stat(source); err != nil {
			return domain.CheckpointRef{}, err
		}
	}
	ref, ok := s.heads[name]
	if !ok {
		return domain.CheckpointRef{}, errors.New("uncertain legacy checkpoint")
	}
	return ref, nil
}
func (s *migrationStub) MigrateV1Checkpoint(_ context.Context, req ports.SnapshotMigrationRequest) (domain.CheckpointRef, error) {
	if source := s.sources[req.LegacyName]; source != "" {
		if _, err := os.Stat(source); err != nil {
			return domain.CheckpointRef{}, err
		}
	}
	if err := s.migrateErr[req.LegacyName]; err != nil {
		return domain.CheckpointRef{}, err
	}
	if s.assignments == nil {
		s.assignments = map[string]domain.IncarnationID{}
	}
	s.assignments[req.LegacyName] = req.IncarnationID
	ref := req.LegacyRef
	ref.ManifestDigest = sha256.Sum256(append(ref.ManifestDigest[:], req.IncarnationID[:]...))
	return ref, nil
}

func encodeLegacyRecordV0(r legacyCatalogueRecordV0) []byte {
	out := binary.BigEndian.AppendUint32(nil, uint32(len(r.Cwd)))
	out = append(out, r.Cwd...)
	out = binary.BigEndian.AppendUint64(out, uint64(r.CreatedAt))
	out = binary.BigEndian.AppendUint64(out, uint64(r.UpdatedAt))
	out = binary.BigEndian.AppendUint64(out, r.LastUsedSeq)
	out = binary.BigEndian.AppendUint32(out, uint32(len(r.TabNames)))
	for _, tab := range r.TabNames {
		out = binary.BigEndian.AppendUint32(out, uint32(len(tab)))
		out = append(out, tab...)
	}
	return out
}

func writeLegacyCatalogue(t *testing.T, dir string, records ...legacyCatalogueRecordV0) string {
	t.Helper()
	require.NoError(t, os.Chmod(dir, 0o700))
	require.NoError(t, os.MkdirAll(dir, 0o700))
	store, err := kv.Open(StorePath(dir))
	require.NoError(t, err)
	for _, record := range records {
		require.NoError(t, store.Set([]byte(record.Name), encodeLegacyRecordV0(record)))
	}
	require.NoError(t, store.Sync())
	require.NoError(t, store.Close())
	return StorePath(dir)
}

func TestMigrationExistingState(t *testing.T) {
	dir := t.TempDir()
	legacy := writeLegacyCatalogue(t, dir,
		legacyCatalogueRecordV0{Name: "work", Cwd: "/tmp", CreatedAt: 1, UpdatedAt: 2, TabNames: []string{"one"}},
		legacyCatalogueRecordV0{Name: "uncertain", Cwd: "/tmp", CreatedAt: 3, UpdatedAt: 4},
	)
	ref := domain.CheckpointRef{Generation: 7, ManifestDigest: [32]byte{1}}
	migration := &migrationStub{heads: map[string]domain.CheckpointRef{"work": ref, "uncertain": ref}, migrateErr: map[string]error{"uncertain": errors.Join(ports.ErrLegacySnapshotUncertain, errors.New("invalid legacy manifest"))}}
	result, err := OpenOrMigrate(context.Background(), OpenDeps{StateDir: dir, Random: &countingReader{}, SnapshotMigration: migration})
	require.NoError(t, err)
	require.True(t, result.Migrated)
	require.False(t, result.NewInstall)
	require.FileExists(t, filepath.Join(dir, "migration", "legacy-catalogue.backup"))
	require.FileExists(t, legacy)
	require.Len(t, result.Records, 2)
	require.Equal(t, domain.RecoveryHealthy, recordByName(t, result.Records, "work").RecoveryState)
	require.Equal(t, domain.RecoveryDegraded, recordByName(t, result.Records, "uncertain").RecoveryState)
	require.NoError(t, result.Catalogue.Close())
	reopened, err := OpenOrMigrate(context.Background(), OpenDeps{StateDir: dir, Random: io.LimitReader(&countingReader{}, 0), SnapshotMigration: migration})
	require.NoError(t, err)
	require.False(t, reopened.Migrated, "completed migration is one-shot")
	require.NoError(t, reopened.Catalogue.Close())
}

func TestMigrationOver4096(t *testing.T) {
	dir := t.TempDir()
	writeLegacyCatalogue(t, dir, legacyCatalogueRecordV0{Name: "large", Cwd: "/tmp"})
	migration := &migrationStub{heads: map[string]domain.CheckpointRef{"large": {Generation: 5001, ManifestDigest: [32]byte{1}}}, migrateErr: map[string]error{}}
	result, err := OpenOrMigrate(context.Background(), OpenDeps{StateDir: dir, Random: &countingReader{}, SnapshotMigration: migration})
	require.NoError(t, err)
	require.NoError(t, result.Catalogue.Close())
	require.Equal(t, uint64(5001), result.Records[0].Committed.Generation)
}

func TestMigrationResumeMatrix(t *testing.T) {
	type crashPoint struct {
		boundary   string
		occurrence int
		record     string
	}
	points := []crashPoint{
		{boundary: "backup-sync", occurrence: 1},
		{boundary: "intent-sync", occurrence: 1},
		{boundary: "identity-sync", occurrence: 1, record: "alpha"},
		{boundary: "identity-sync", occurrence: 2, record: "uncertain"},
		{boundary: "identity-sync", occurrence: 3, record: "work"},
		{boundary: "head-validation", occurrence: 1, record: "alpha"},
		{boundary: "head-validation", occurrence: 2, record: "uncertain"},
		{boundary: "head-validation", occurrence: 3, record: "work"},
		{boundary: "catalogue-sync", occurrence: 1},
		{boundary: "receipt-sync", occurrence: 1},
		{boundary: "complete-sync", occurrence: 1},
	}
	for _, point := range points {
		name := point.boundary + "-" + strconv.Itoa(point.occurrence)
		if point.record != "" {
			name += "-" + point.record
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			legacyCatalogue := writeLegacyCatalogue(t, dir,
				legacyCatalogueRecordV0{Name: "alpha", Cwd: "/tmp"},
				legacyCatalogueRecordV0{Name: "uncertain", Cwd: "/tmp"},
				legacyCatalogueRecordV0{Name: "work", Cwd: "/tmp"},
			)
			ref := domain.CheckpointRef{Generation: 1, ManifestDigest: [32]byte{1}}
			heads := map[string]domain.CheckpointRef{"alpha": ref, "uncertain": ref, "work": ref}
			migrationErrors := map[string]error{"uncertain": errors.Join(ports.ErrLegacySnapshotUncertain, errors.New("corrupt legacy manifest"))}
			legacySnapshotDir := filepath.Join(dir, "legacy-snapshots")
			require.NoError(t, os.Mkdir(legacySnapshotDir, 0o700))
			legacySnapshotSources := make(map[string]string, 3)
			for _, recordName := range []string{"alpha", "uncertain", "work"} {
				path := filepath.Join(legacySnapshotDir, recordName+".snap")
				require.NoError(t, os.WriteFile(path, []byte("legacy source"), 0o600))
				legacySnapshotSources[recordName] = path
			}
			stub := &migrationStub{heads: heads, migrateErr: migrationErrors, sources: legacySnapshotSources}
			seen := 0
			_, err := OpenOrMigrate(context.Background(), OpenDeps{StateDir: dir, Random: &countingReader{}, SnapshotMigration: stub, Fault: func(got string) error {
				if got != point.boundary {
					return nil
				}
				seen++
				if seen == point.occurrence {
					return errors.New("simulated crash")
				}
				return nil
			}})
			require.Error(t, err)
			require.Equal(t, point.occurrence, seen)

			before := map[string]domain.IncarnationID{}
			intentPath := filepath.Join(dir, migrationDirName, migrationIntentName)
			if journal, journalErr := readMigrationRecord(intentPath); journalErr == nil {
				for recordName, id := range journal.Assignments {
					before[recordName] = id
				}
			}
			restarted := &migrationStub{heads: heads, migrateErr: migrationErrors, sources: legacySnapshotSources}
			result, err := OpenOrMigrate(context.Background(), OpenDeps{StateDir: dir, Random: &countingReader{next: 64}, SnapshotMigration: restarted})
			require.NoError(t, err)
			completed, err := readMigrationRecord(intentPath)
			require.NoError(t, err)
			require.Len(t, completed.Assignments, 3)
			for recordName, id := range before {
				require.Equal(t, id, completed.Assignments[recordName], "assigned identities must survive restart")
			}
			for _, recordName := range []string{"alpha", "uncertain", "work"} {
				require.Equal(t, completed.Assignments[recordName], recordByName(t, result.Records, recordName).IncarnationID)
				require.FileExists(t, legacySnapshotSources[recordName], "legacy snapshot source must remain retained")
			}
			uncertain := recordByName(t, result.Records, "uncertain")
			require.Equal(t, domain.RecoveryDegraded, uncertain.RecoveryState)
			require.Contains(t, uncertain.DegradedReason, "corrupt legacy manifest")
			require.NoError(t, result.Catalogue.Close())

			reopened, err := OpenOrMigrate(context.Background(), OpenDeps{StateDir: dir, Random: io.LimitReader(&countingReader{}, 0), SnapshotMigration: restarted})
			require.NoError(t, err)
			for _, recordName := range []string{"alpha", "uncertain", "work"} {
				require.Equal(t, completed.Assignments[recordName], recordByName(t, reopened.Records, recordName).IncarnationID)
			}
			require.Equal(t, domain.RecoveryDegraded, recordByName(t, reopened.Records, "uncertain").RecoveryState)
			require.NoError(t, reopened.Catalogue.Close())
			require.FileExists(t, legacyCatalogue)
			require.FileExists(t, filepath.Join(dir, migrationDirName, migrationBackupName))
			require.FileExists(t, filepath.Join(dir, migrationDirName, migrationCompleteName))
		})
	}
}

func TestMigrationPreservesUncertain(t *testing.T) {
	dir := t.TempDir()
	legacy := writeLegacyCatalogue(t, dir, legacyCatalogueRecordV0{Name: "broken", Cwd: "/tmp"})
	ref := domain.CheckpointRef{Generation: 1, ManifestDigest: [32]byte{1}}
	result, err := OpenOrMigrate(context.Background(), OpenDeps{StateDir: dir, Random: &countingReader{}, SnapshotMigration: &migrationStub{heads: map[string]domain.CheckpointRef{"broken": ref}, migrateErr: map[string]error{"broken": errors.Join(ports.ErrLegacySnapshotUncertain, errors.New("invalid legacy manifest"))}}})
	require.NoError(t, err)
	require.NoError(t, result.Catalogue.Close())
	require.FileExists(t, legacy)
	require.Equal(t, domain.RecoveryDegraded, result.Records[0].RecoveryState)
	require.NotEmpty(t, result.Records[0].DegradedReason)

	failClosedDir := t.TempDir()
	writeLegacyCatalogue(t, failClosedDir, legacyCatalogueRecordV0{Name: "work", Cwd: "/tmp"})
	_, err = OpenOrMigrate(context.Background(), OpenDeps{StateDir: failClosedDir, Random: &countingReader{}, SnapshotMigration: &migrationStub{heads: map[string]domain.CheckpointRef{"work": ref}, migrateErr: map[string]error{"work": errors.New("repository unavailable")}}})
	require.ErrorContains(t, err, "repository unavailable")
	data, replayErr := kv.Replay(StorePath(failClosedDir))
	require.NoError(t, replayErr)
	_, legacyFormat, classifyErr := classifyCatalogue(data)
	require.NoError(t, classifyErr)
	require.True(t, legacyFormat, "migration-port failure must not install a new catalogue")
}

func TestOpenOrMigrateLegacyProbe(t *testing.T) {
	probeErr := errors.New("probe uncertain")
	for _, tc := range []struct {
		name    string
		stub    *migrationStub
		wantNew bool
		wantErr error
	}{
		{"proven absent", &migrationStub{}, true, nil},
		{"legacy blob", &migrationStub{legacy: true}, false, nil},
		{"incremental namespace", &migrationStub{legacy: true}, false, nil},
		{"probe error", &migrationStub{probeErr: probeErr}, false, probeErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.Chmod(dir, 0o700))
			result, err := OpenOrMigrate(context.Background(), OpenDeps{StateDir: dir, Random: &countingReader{}, SnapshotMigration: tc.stub})
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else if tc.stub.legacy {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.wantNew, result.NewInstall)
				require.NoError(t, result.Catalogue.Close())
				if tc.wantNew {
					reopened, reopenErr := OpenOrMigrate(context.Background(), OpenDeps{StateDir: dir, Random: &countingReader{}, SnapshotMigration: tc.stub})
					require.NoError(t, reopenErr)
					require.False(t, reopened.NewInstall)
					require.False(t, reopened.Migrated)
					require.NoError(t, reopened.Catalogue.Close())
				}
			}
			if tc.stub.legacy || tc.wantErr != nil {
				_, statErr := os.Stat(StorePath(dir))
				require.ErrorIs(t, statErr, os.ErrNotExist)
			}
		})
	}
}

type countingReader struct{ next byte }

func (r *countingReader) Read(p []byte) (int, error) {
	for i := range p {
		r.next++
		if r.next == 0 {
			r.next = 1
		}
		p[i] = r.next
	}
	return len(p), nil
}
func recordByName(t *testing.T, records []domain.CatalogueRecord, name string) domain.CatalogueRecord {
	t.Helper()
	for _, r := range records {
		if r.Name == name {
			return r
		}
	}
	t.Fatalf("record %q not found", name)
	return domain.CatalogueRecord{}
}
