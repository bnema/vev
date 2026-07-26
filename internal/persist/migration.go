package persist

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/kv"
	"github.com/bnema/vev/pkg/safedir"
)

type MigrationPhase uint8

const (
	MigrationBackedUp MigrationPhase = iota + 1
	MigrationAssigned
	MigrationValidated
	MigrationInstalled
	MigrationComplete
)

type MigrationRecord struct {
	Phase       MigrationPhase
	Assignments map[string]domain.IncarnationID
	Validated   map[string]*domain.CheckpointRef
	Degraded    map[string]string
}

type OpenDeps struct {
	StateDir          string
	Random            io.Reader
	SnapshotMigration ports.SnapshotMigration
	// Fault is a test-only crash seam called after named durable boundaries.
	Fault func(string) error
}

type OpenResult struct {
	Catalogue  *Persister
	Records    []domain.CatalogueRecord
	NewInstall bool
	Migrated   bool
}

type legacyCatalogueRecordV0 struct {
	Name        string
	Cwd         string
	CreatedAt   int64
	UpdatedAt   int64
	LastUsedSeq uint64
	TabNames    []string
}

const (
	migrationDirName              = "migration"
	migrationIntentName           = "durable-session-v1.intent"
	migrationCompleteName         = "durable-session-v1.complete"
	migrationBackupName           = "legacy-catalogue.backup"
	catalogueFormatName           = "durable-session-v1.format"
	migrationRecordMagic          = "VEVJ"
	migrationRecordVersion uint16 = 1
)

func OpenOrMigrate(ctx context.Context, deps OpenDeps) (OpenResult, error) {
	if err := ctx.Err(); err != nil {
		return OpenResult{}, err
	}
	if deps.StateDir == "" || deps.Random == nil || deps.SnapshotMigration == nil {
		return OpenResult{}, errors.New("persist: incomplete open or migrate dependencies")
	}
	path := StorePath(deps.StateDir)
	present, err := catalogueCandidatesPresent(path)
	if err != nil {
		return OpenResult{}, err
	}
	migrationDir := filepath.Join(deps.StateDir, migrationDirName)
	intentPath := filepath.Join(migrationDir, migrationIntentName)
	if !present {
		if _, err := os.Stat(intentPath); err == nil {
			return resumeLegacyMigration(ctx, deps)
		} else if !errors.Is(err, os.ErrNotExist) {
			return OpenResult{}, err
		}
		legacy, err := deps.SnapshotMigration.HasLegacyState(ctx)
		if err != nil {
			return OpenResult{}, err
		}
		if legacy {
			return OpenResult{}, errors.New("persist: legacy snapshot state exists without a migratable catalogue")
		}
		if err := atomicMigrationWrite(filepath.Join(migrationDir, catalogueFormatName), []byte("v1\n")); err != nil {
			return OpenResult{}, err
		}
		catalogue, records, err := openCurrentCatalogue(deps.StateDir, true)
		if err != nil {
			return OpenResult{}, err
		}
		return OpenResult{Catalogue: catalogue, Records: records, NewInstall: true}, nil
	}
	// Opening and closing first applies fixed-path WAL recovery before format detection.
	store, err := OpenStore(path)
	if err != nil {
		return OpenResult{}, err
	}
	if err := store.Close(); err != nil {
		return OpenResult{}, err
	}
	if complete, err := completedMigration(migrationDir, intentPath); err != nil {
		return OpenResult{}, err
	} else if complete {
		data, err := kv.Replay(path)
		if err != nil {
			return OpenResult{}, err
		}
		current, legacy, err := classifyCatalogue(data)
		if err != nil || !current || legacy {
			return OpenResult{}, errors.Join(errors.New("persist: completed migration has invalid current catalogue"), err)
		}
		catalogue, records, err := openCurrentCatalogue(deps.StateDir, false)
		return OpenResult{Catalogue: catalogue, Records: records}, err
	}
	if _, err := os.Stat(intentPath); err == nil {
		return resumeLegacyMigration(ctx, deps)
	} else if !errors.Is(err, os.ErrNotExist) {
		return OpenResult{}, err
	}
	data, err := kv.Replay(path)
	if err != nil {
		return OpenResult{}, err
	}
	current, legacy, err := classifyCatalogue(data)
	if err != nil {
		if len(data) != 0 || !catalogueFormatProven(migrationDir) {
			return OpenResult{}, err
		}
		current = true
	}
	if current {
		if err := atomicMigrationWrite(filepath.Join(migrationDir, catalogueFormatName), []byte("v1\n")); err != nil {
			return OpenResult{}, err
		}
		catalogue, records, err := openCurrentCatalogue(deps.StateDir, false)
		return OpenResult{Catalogue: catalogue, Records: records}, err
	}
	if !legacy {
		return OpenResult{}, errors.New("persist: ambiguous catalogue format")
	}
	return startLegacyMigration(ctx, deps)
}

func catalogueFormatProven(dir string) bool {
	data, err := os.ReadFile(filepath.Join(dir, catalogueFormatName))
	return err == nil && string(data) == "v1\n"
}

func completedMigration(dir, intentPath string) (bool, error) {
	completePath := filepath.Join(dir, migrationCompleteName)
	data, err := os.ReadFile(completePath)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if string(data) != "complete\n" {
		return false, errors.New("persist: invalid migration completion marker")
	}
	record, err := readMigrationRecord(intentPath)
	if err != nil {
		return false, err
	}
	if record.Phase != MigrationComplete {
		return false, errors.New("persist: completion marker precedes completed intent")
	}
	return true, nil
}

func catalogueCandidatesPresent(path string) (bool, error) {
	present := false
	for _, candidate := range []string{path, path + ".next", path + ".prev"} {
		_, err := os.Stat(candidate)
		if err == nil {
			present = true
		} else if !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
	}
	return present, nil
}
func classifyCatalogue(data map[string][]byte) (current, legacy bool, err error) {
	if len(data) == 0 {
		return false, false, errors.New("persist: empty existing catalogue has unknown provenance")
	}
	for name, value := range data {
		if len(value) >= 6 && string(value[:4]) == string(catalogueMagic[:]) {
			if _, e := decodeRecordValue(name, value); e != nil {
				return false, false, e
			}
			current = true
		} else {
			if _, e := decodeLegacyRecordV0(name, value); e != nil {
				return false, false, e
			}
			legacy = true
		}
	}
	if current && legacy {
		return false, false, errors.New("persist: mixed catalogue formats")
	}
	return current, legacy, nil
}

func startLegacyMigration(ctx context.Context, deps OpenDeps) (OpenResult, error) {
	migrationDir := filepath.Join(deps.StateDir, migrationDirName)
	if err := safedir.EnsurePrivate(migrationDir); err != nil {
		return OpenResult{}, err
	}
	backup := filepath.Join(migrationDir, migrationBackupName)
	source, err := os.ReadFile(StorePath(deps.StateDir))
	if err != nil {
		return OpenResult{}, err
	}
	if err := atomicMigrationWrite(backup, source); err != nil {
		return OpenResult{}, err
	}
	if err := migrationBoundary(deps, "backup-sync"); err != nil {
		return OpenResult{}, err
	}
	record := MigrationRecord{Phase: MigrationBackedUp, Assignments: map[string]domain.IncarnationID{}, Validated: map[string]*domain.CheckpointRef{}, Degraded: map[string]string{}}
	if err := writeMigrationRecord(filepath.Join(migrationDir, migrationIntentName), record); err != nil {
		return OpenResult{}, err
	}
	if err := migrationBoundary(deps, "intent-sync"); err != nil {
		return OpenResult{}, err
	}
	return resumeLegacyMigration(ctx, deps)
}

func resumeLegacyMigration(ctx context.Context, deps OpenDeps) (OpenResult, error) {
	migrationDir := filepath.Join(deps.StateDir, migrationDirName)
	intent := filepath.Join(migrationDir, migrationIntentName)
	record, err := readMigrationRecord(intent)
	if err != nil {
		return OpenResult{}, err
	}
	backup := filepath.Join(migrationDir, migrationBackupName)
	legacyData, err := kv.Replay(backup)
	if err != nil {
		return OpenResult{}, fmt.Errorf("persist: replay legacy backup: %w", err)
	}
	legacyRecords, err := decodeLegacyMap(legacyData)
	if err != nil {
		return OpenResult{}, err
	}
	if record.Phase < MigrationAssigned {
		for _, legacy := range legacyRecords {
			if _, ok := record.Assignments[legacy.Name]; ok {
				continue
			}
			id, err := domain.NewIncarnationID(deps.Random)
			if err != nil {
				return OpenResult{}, err
			}
			record.Assignments[legacy.Name] = id
			if err := writeMigrationRecord(intent, record); err != nil {
				return OpenResult{}, err
			}
			if err := migrationBoundary(deps, "identity-sync"); err != nil {
				return OpenResult{}, err
			}
		}
		record.Phase = MigrationAssigned
		if err := writeMigrationRecord(intent, record); err != nil {
			return OpenResult{}, err
		}
	}
	if record.Phase < MigrationValidated {
		for _, legacy := range legacyRecords {
			if _, ok := record.Validated[legacy.Name]; ok {
				continue
			}
			if _, ok := record.Degraded[legacy.Name]; ok {
				continue
			}
			ref, err := deps.SnapshotMigration.ReadLegacyHEAD(ctx, legacy.Name)
			if err == nil {
				var migrated domain.CheckpointRef
				migrated, err = deps.SnapshotMigration.MigrateV1Checkpoint(ctx, ports.SnapshotMigrationRequest{LegacyName: legacy.Name, IncarnationID: record.Assignments[legacy.Name], LegacyRef: ref})
				if err == nil {
					record.Validated[legacy.Name] = &migrated
				}
			}
			if err != nil {
				if !errors.Is(err, ports.ErrLegacySnapshotUncertain) {
					return OpenResult{}, fmt.Errorf("persist: migrate legacy snapshot %q: %w", legacy.Name, err)
				}
				if ref.Generation != 0 && ref.ManifestDigest != ([32]byte{}) {
					legacyRef := ref
					record.Validated[legacy.Name] = &legacyRef
				}
				record.Degraded[legacy.Name] = err.Error()
			}
			if err := writeMigrationRecord(intent, record); err != nil {
				return OpenResult{}, err
			}
			if err := migrationBoundary(deps, "head-validation"); err != nil {
				return OpenResult{}, err
			}
		}
		record.Phase = MigrationValidated
		if err := writeMigrationRecord(intent, record); err != nil {
			return OpenResult{}, err
		}
	}
	records := make([]domain.CatalogueRecord, 0, len(legacyRecords))
	for _, legacy := range legacyRecords {
		next := domain.CatalogueRecord{Name: legacy.Name, IncarnationID: record.Assignments[legacy.Name], Cwd: legacy.Cwd, CreatedAt: legacy.CreatedAt, UpdatedAt: legacy.UpdatedAt, LastUsedSeq: legacy.LastUsedSeq, TabNames: append([]string(nil), legacy.TabNames...)}
		if ref := record.Validated[legacy.Name]; ref != nil {
			copyRef := *ref
			next.Committed = &copyRef
		}
		if reason := record.Degraded[legacy.Name]; reason != "" {
			next.RecoveryState = domain.RecoveryDegraded
			next.DegradedReason = reason
		} else {
			next.RecoveryState = domain.RecoveryHealthy
		}
		records = append(records, next)
	}
	if record.Phase < MigrationInstalled {
		if err := installMigratedCatalogue(deps.StateDir, records); err != nil {
			return OpenResult{}, err
		}
		if err := migrationBoundary(deps, "catalogue-sync"); err != nil {
			return OpenResult{}, err
		}
		record.Phase = MigrationInstalled
		if err := writeMigrationRecord(intent, record); err != nil {
			return OpenResult{}, err
		}
		if err := migrationBoundary(deps, "receipt-sync"); err != nil {
			return OpenResult{}, err
		}
	}
	catalogue, opened, err := openCurrentCatalogue(deps.StateDir, false)
	if err != nil {
		return OpenResult{}, err
	}
	if !catalogueRecordsEqual(records, opened) {
		_ = catalogue.Close()
		return OpenResult{}, errors.New("persist: installed catalogue validation mismatch")
	}
	if record.Phase < MigrationComplete {
		record.Phase = MigrationComplete
		if err := writeMigrationRecord(intent, record); err != nil {
			_ = catalogue.Close()
			return OpenResult{}, err
		}
		if err := atomicMigrationWrite(filepath.Join(migrationDir, migrationCompleteName), []byte("complete\n")); err != nil {
			_ = catalogue.Close()
			return OpenResult{}, err
		}
		if err := migrationBoundary(deps, "complete-sync"); err != nil {
			_ = catalogue.Close()
			return OpenResult{}, err
		}
	}
	return OpenResult{Catalogue: catalogue, Records: opened, Migrated: true}, nil
}

func migrationBoundary(deps OpenDeps, name string) error {
	if deps.Fault != nil {
		return deps.Fault(name)
	}
	return nil
}

func installMigratedCatalogue(stateDir string, records []domain.CatalogueRecord) error {
	stage := filepath.Join(stateDir, migrationDirName, "install")
	_ = os.RemoveAll(stage)
	if err := safedir.EnsurePrivate(stage); err != nil {
		return err
	}
	p, _, err := openCurrentCatalogue(stage, true)
	if err != nil {
		return err
	}
	changes := make(map[string]*domain.CatalogueRecord, len(records))
	for i := range records {
		r := records[i]
		changes[r.Name] = &r
	}
	if len(changes) > 0 {
		if err := p.Apply(changes); err != nil {
			_ = p.Close()
			return err
		}
	}
	if err := p.Close(); err != nil {
		return err
	}
	source := StorePath(stage)
	target := StorePath(stateDir)
	if err := os.Rename(source, target); err != nil {
		return err
	}
	if err := syncMigrationDir(stateDir); err != nil {
		return err
	}
	return atomicMigrationWrite(filepath.Join(stateDir, migrationDirName, catalogueFormatName), []byte("v1\n"))
}

func decodeLegacyMap(data map[string][]byte) ([]legacyCatalogueRecordV0, error) {
	out := make([]legacyCatalogueRecordV0, 0, len(data))
	for name, value := range data {
		record, err := decodeLegacyRecordV0(name, value)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}
func decodeLegacyRecordV0(name string, value []byte) (legacyCatalogueRecordV0, error) {
	if err := domain.ValidateSessionName(name); err != nil {
		return legacyCatalogueRecordV0{}, err
	}
	r := valueReader{data: value}
	cwd, ok := r.str()
	if !ok {
		return legacyCatalogueRecordV0{}, errMalformedRecord
	}
	created, ok := r.u64()
	if !ok {
		return legacyCatalogueRecordV0{}, errMalformedRecord
	}
	updated, ok := r.u64()
	if !ok {
		return legacyCatalogueRecordV0{}, errMalformedRecord
	}
	record := legacyCatalogueRecordV0{Name: name, Cwd: cwd, CreatedAt: int64(created), UpdatedAt: int64(updated)}
	if r.remaining() == 0 {
		return record, nil
	}
	if record.LastUsedSeq, ok = r.u64(); !ok {
		return legacyCatalogueRecordV0{}, errMalformedRecord
	}
	if r.remaining() == 0 {
		return record, nil
	}
	count, ok := r.u32()
	if !ok || uint64(count) > uint64(r.remaining())/4 {
		return legacyCatalogueRecordV0{}, errMalformedRecord
	}
	for range count {
		tab, ok := r.str()
		if !ok {
			return legacyCatalogueRecordV0{}, errMalformedRecord
		}
		record.TabNames = append(record.TabNames, tab)
	}
	if r.remaining() != 0 {
		return legacyCatalogueRecordV0{}, errMalformedRecord
	}
	return record, nil
}

func writeMigrationRecord(path string, record MigrationRecord) error {
	return atomicMigrationWrite(path, encodeMigrationRecord(record))
}
func encodeMigrationRecord(record MigrationRecord) []byte {
	out := append([]byte(migrationRecordMagic), byte(migrationRecordVersion>>8), byte(migrationRecordVersion), byte(record.Phase))
	names := make([]string, 0, len(record.Assignments))
	for name := range record.Assignments {
		names = append(names, name)
	}
	sort.Strings(names)
	out = binary.BigEndian.AppendUint32(out, uint32(len(names)))
	for _, name := range names {
		out = appendMigrationString(out, name)
		id := record.Assignments[name]
		out = append(out, id[:]...)
		if ref := record.Validated[name]; ref != nil {
			out = append(out, 1)
			out = binary.BigEndian.AppendUint64(out, ref.Generation)
			out = append(out, ref.ManifestDigest[:]...)
		} else {
			out = append(out, 0)
		}
		out = appendMigrationString(out, record.Degraded[name])
	}
	return out
}
func readMigrationRecord(path string) (MigrationRecord, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return MigrationRecord{}, err
	}
	if len(data) < 11 || string(data[:4]) != migrationRecordMagic || binary.BigEndian.Uint16(data[4:6]) != migrationRecordVersion {
		return MigrationRecord{}, errors.New("persist: invalid migration journal")
	}
	record := MigrationRecord{Phase: MigrationPhase(data[6]), Assignments: map[string]domain.IncarnationID{}, Validated: map[string]*domain.CheckpointRef{}, Degraded: map[string]string{}}
	r := valueReader{data: data[7:]}
	count, ok := r.u32()
	if !ok {
		return MigrationRecord{}, errMalformedRecord
	}
	for range count {
		name, ok := r.str()
		if !ok {
			return MigrationRecord{}, errMalformedRecord
		}
		idBytes, ok := r.take(16)
		if !ok {
			return MigrationRecord{}, errMalformedRecord
		}
		var id domain.IncarnationID
		copy(id[:], idBytes)
		if _, exists := record.Assignments[name]; exists || id == (domain.IncarnationID{}) {
			return MigrationRecord{}, errMalformedRecord
		}
		record.Assignments[name] = id
		present, ok := r.byte()
		if !ok {
			return MigrationRecord{}, errMalformedRecord
		}
		if present == 1 {
			generation, ok := r.u64()
			if !ok {
				return MigrationRecord{}, errMalformedRecord
			}
			digest, ok := r.take(32)
			if !ok {
				return MigrationRecord{}, errMalformedRecord
			}
			ref := &domain.CheckpointRef{Generation: generation}
			copy(ref.ManifestDigest[:], digest)
			record.Validated[name] = ref
		} else if present != 0 {
			return MigrationRecord{}, errMalformedRecord
		}
		reason, ok := r.str()
		if !ok {
			return MigrationRecord{}, errMalformedRecord
		}
		if reason != "" {
			record.Degraded[name] = reason
		}
	}
	if r.remaining() != 0 || record.Phase < MigrationBackedUp || record.Phase > MigrationComplete {
		return MigrationRecord{}, errMalformedRecord
	}
	return record, nil
}
func appendMigrationString(out []byte, value string) []byte {
	out = binary.BigEndian.AppendUint32(out, uint32(len(value)))
	return append(out, value...)
}
func atomicMigrationWrite(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := safedir.EnsurePrivate(dir); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".migration-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	return syncMigrationDir(dir)
}
func syncMigrationDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
func catalogueRecordsEqual(a, b []domain.CatalogueRecord) bool {
	if len(a) != len(b) {
		return false
	}
	sort.Slice(a, func(i, j int) bool { return a[i].Name < a[j].Name })
	sort.Slice(b, func(i, j int) bool { return b[i].Name < b[j].Name })
	for i := range a {
		if a[i].Name != b[i].Name || a[i].IncarnationID != b[i].IncarnationID || a[i].RecoveryState != b[i].RecoveryState {
			return false
		}
	}
	return true
}
