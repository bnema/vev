package daemon

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	recoveryusecase "github.com/bnema/vev/internal/usecase/recovery"
	snapcodec "github.com/bnema/vev/internal/usecase/snapshot"
)

type durableRecoveryCatalogue struct {
	ports.Catalogue
	mu      sync.Mutex
	records map[string]domain.CatalogueRecord
}

func newDurableRecoveryCatalogue(records []domain.CatalogueRecord) *durableRecoveryCatalogue {
	catalogue := &durableRecoveryCatalogue{records: make(map[string]domain.CatalogueRecord, len(records))}
	for _, record := range records {
		catalogue.records[record.Name] = record
	}
	return catalogue
}

func (c *durableRecoveryCatalogue) Records() []domain.CatalogueRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	records := make([]domain.CatalogueRecord, 0, len(c.records))
	for _, record := range c.records {
		records = append(records, record)
	}
	return records
}

func (c *durableRecoveryCatalogue) Record(name string) (domain.CatalogueRecord, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	record, ok := c.records[name]
	return record, ok
}

func (c *durableRecoveryCatalogue) Replace(name string, record domain.CatalogueRecord) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if name != record.Name {
		return errors.New("catalogue key mismatch")
	}
	c.records[name] = record
	return nil
}

func (c *durableRecoveryCatalogue) Close() error { return nil }

type durableRecoveryRepository struct {
	ports.SnapshotRepository
	mu          sync.Mutex
	generations map[string]ports.SnapshotGeneration
	errors      map[string]error
	loads       map[string]int
	repairs     map[string]domain.CheckpointRef
}

func (r *durableRecoveryRepository) LoadCheckpoint(_ context.Context, id domain.IncarnationID, name string, ref ports.CheckpointRef) (ports.SnapshotGeneration, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.loads[name]++
	if err := r.errors[name]; err != nil {
		return ports.SnapshotGeneration{}, err
	}
	generation, ok := r.generations[name]
	if !ok || generation.Generation != ref.Generation {
		return ports.SnapshotGeneration{}, errors.New("checkpoint unavailable")
	}
	return generation, nil
}

func (r *durableRecoveryRepository) RepairHEAD(_ context.Context, id domain.IncarnationID, ref ports.CheckpointRef) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.repairs[id.String()] = ref
	return nil
}

func durableRecoveryRecord(index int) domain.CatalogueRecord {
	id := domain.IncarnationID{0: byte(index>>8 + 1), 1: byte(index + 1)}
	ref := domain.CheckpointRef{Generation: uint64(index + 1), ManifestDigest: [32]byte{byte(index%255 + 1)}}
	return domain.CatalogueRecord{
		Name:          fmt.Sprintf("session-%04d", index),
		IncarnationID: id,
		Cwd:           "/tmp",
		CreatedAt:     int64(index + 1),
		UpdatedAt:     int64(index + 1),
		RecoveryState: domain.RecoveryHealthy,
		Committed:     &ref,
	}
}

func validGeneration(t testing.TB, record domain.CatalogueRecord) ports.SnapshotGeneration {
	t.Helper()
	manifest, err := snapcodec.MarshalManifest(snapcodec.Manifest{
		Generation:    record.Committed.Generation,
		IncarnationID: record.IncarnationID,
		Name:          record.Name,
		CreatedAt:     uint64(record.CreatedAt),
	})
	require.NoError(t, err)
	return ports.SnapshotGeneration{
		IncarnationID: record.IncarnationID,
		Name:          record.Name,
		Generation:    record.Committed.Generation,
		Manifest:      manifest,
		Objects:       map[ports.SnapshotDigest][]byte{},
	}
}

func newDurableRecoveryDaemon(t testing.TB, records []domain.CatalogueRecord, repository ports.SnapshotRepository) (*Daemon, *durableRecoveryCatalogue) {
	t.Helper()
	catalogue := newDurableRecoveryCatalogue(records)
	coordinator := recoveryusecase.NewCoordinator(catalogue, repository, nil, nil)
	d := New(nil, stubClock{}, slog.New(slog.NewTextHandler(io.Discard, nil)), WithCatalogue(catalogue, records), WithSnapshotRepository(repository, nil), WithCheckpointCoordinator(coordinator))
	d.serveCtx, d.serveCancel = context.WithCancel(context.Background())
	t.Cleanup(d.serveCancel)
	return d, catalogue
}

func TestFinalCheckpointFailurePreservesCatalogue(t *testing.T) {
	priorRef := domain.CheckpointRef{Generation: 7, ManifestDigest: [32]byte{7}}
	prior := domain.CatalogueRecord{
		Name:          "work",
		IncarnationID: domain.IncarnationID{1},
		Cwd:           "/work",
		CreatedAt:     42,
		RecoveryState: domain.RecoveryHealthy,
		Committed:     &priorRef,
	}
	catalogue := newDurableRecoveryCatalogue([]domain.CatalogueRecord{prior})
	repository := &durableRecoveryRepository{}
	coordinator := recoveryusecase.NewCoordinator(catalogue, repository, nil, nil)
	d := New(nil, stubClock{}, slog.New(slog.NewTextHandler(io.Discard, nil)), WithCatalogue(catalogue, []domain.CatalogueRecord{prior}), WithSnapshotRepository(repository, nil), WithCheckpointCoordinator(coordinator))
	startSnapshotEncodeWorker(t, d)

	sess := newSnapshotTestSession(t, prior.Name, false, prior.Cwd)
	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	sess.snapshotMu.Lock()
	sess.snapshotPublishedGeneration = priorRef.Generation
	sess.snapshotPublishedCheckpoint = &priorRef
	sess.snapshotGeneration = priorRef.Generation + 1
	sess.snapshotPublicationContext = expired
	sess.snapDirty.Store(true)
	sess.snapshotMu.Unlock()

	require.True(t, d.scheduleFinalSnapshot(sess))
	awaitSnapshotIdle(t, sess)
	got, ok := catalogue.Record(prior.Name)
	require.True(t, ok)
	require.Equal(t, prior, got, "a timed-out final checkpoint must not change authoritative bytes")
	require.Equal(t, priorRef, *sess.snapshotPublishedCheckpoint)
}

func TestCatalogueRestoreIndependent(t *testing.T) {
	emptyDaemon, emptyCatalogue := newDurableRecoveryDaemon(t, nil, &durableRecoveryRepository{})
	emptyDone := make(chan struct{})
	go func() {
		emptyDaemon.restoreCatalogue(context.Background(), emptyCatalogue.Records())
		close(emptyDone)
	}()
	select {
	case <-emptyDone:
	case <-time.After(time.Second):
		t.Fatal("empty catalogue restoration blocked")
	}

	healthy := durableRecoveryRecord(0)
	broken := durableRecoveryRecord(1)
	repository := &durableRecoveryRepository{
		generations: map[string]ports.SnapshotGeneration{healthy.Name: validGeneration(t, healthy)},
		errors:      map[string]error{broken.Name: ports.ErrBudgetExhausted},
		loads:       make(map[string]int),
		repairs:     make(map[string]domain.CheckpointRef),
	}
	d, catalogue := newDurableRecoveryDaemon(t, []domain.CatalogueRecord{healthy, broken}, repository)

	d.restoreCatalogue(context.Background(), catalogue.Records())

	require.Equal(t, runtimeHealthy, d.stopped[healthy.Name].state)
	require.Equal(t, runtimeDegraded, d.stopped[broken.Name].state)
	for _, name := range []string{healthy.Name, broken.Name} {
		select {
		case <-d.stopped[name].restoreDone:
		default:
			t.Fatalf("restore completion for %q was not closed", name)
		}
	}
	degraded, ok := catalogue.Record(broken.Name)
	require.True(t, ok)
	require.Equal(t, domain.RecoveryDegraded, degraded.RecoveryState)
	require.Equal(t, 1, repository.loads[healthy.Name])
	require.Equal(t, 1, repository.loads[broken.Name])
	require.Empty(t, d.sessions, "a corrupt record must never open a replacement runtime")

	fallback := durableRecoveryRecord(2)
	fallbackRef := domain.CheckpointRef{Generation: fallback.Committed.Generation - 1}
	fallbackSnapshotRecord := fallback
	fallbackSnapshotRecord.Committed = &fallbackRef
	fallbackGeneration := validGeneration(t, fallbackSnapshotRecord)
	fallbackRef.ManifestDigest = sha256.Sum256(fallbackGeneration.Manifest)
	fallback.Fallbacks[0] = &fallbackRef
	fallbackRepository := &durableRecoveryRepository{
		generations: map[string]ports.SnapshotGeneration{fallback.Name: fallbackGeneration},
		errors:      make(map[string]error),
		loads:       make(map[string]int),
		repairs:     make(map[string]domain.CheckpointRef),
	}
	fallbackDaemon, fallbackCatalogue := newDurableRecoveryDaemon(t, []domain.CatalogueRecord{fallback}, fallbackRepository)
	fallbackDaemon.restoreCatalogue(context.Background(), fallbackCatalogue.Records())
	promoted, ok := fallbackCatalogue.Record(fallback.Name)
	require.True(t, ok)
	require.Equal(t, fallbackRef, *promoted.Committed)
	require.Equal(t, runtimeHealthy, fallbackDaemon.stopped[fallback.Name].state)
	require.Equal(t, 3, fallbackRepository.loads[fallback.Name])
	require.Equal(t, fallbackRef, fallbackRepository.repairs[fallback.IncarnationID.String()])
}

func TestCatalogueRestoreAggregateOver4096(t *testing.T) {
	const count = 4200
	records := make([]domain.CatalogueRecord, count)
	repository := &durableRecoveryRepository{generations: make(map[string]ports.SnapshotGeneration, count), errors: make(map[string]error), loads: make(map[string]int), repairs: make(map[string]domain.CheckpointRef)}
	for i := range records {
		records[i] = durableRecoveryRecord(i)
		repository.generations[records[i].Name] = validGeneration(t, records[i])
	}
	d, catalogue := newDurableRecoveryDaemon(t, records, repository)

	d.restoreCatalogue(context.Background(), catalogue.Records())

	require.Len(t, d.stopped, count)
	for _, record := range records {
		require.Equal(t, runtimeHealthy, d.stopped[record.Name].state)
		require.Equal(t, 1, repository.loads[record.Name])
	}
}

func TestCatalogueRestoreSingleSessionOver4096(t *testing.T) {
	record := durableRecoveryRecord(0)
	repository := &durableRecoveryRepository{generations: map[string]ports.SnapshotGeneration{record.Name: validGeneration(t, record)}, errors: make(map[string]error), loads: make(map[string]int), repairs: make(map[string]domain.CheckpointRef)}
	d, catalogue := newDurableRecoveryDaemon(t, []domain.CatalogueRecord{record}, repository)

	// The repository may contain any number of stale entries; restoration has no
	// discovery API and therefore performs exactly one direct catalogue lookup.
	d.restoreCatalogue(context.Background(), catalogue.Records())

	require.Equal(t, runtimeHealthy, d.stopped[record.Name].state)
	require.Equal(t, 1, repository.loads[record.Name])
}

func TestCatalogueRestoreIncarnationMismatch(t *testing.T) {
	record := durableRecoveryRecord(0)
	generation := validGeneration(t, record)
	generation.IncarnationID = domain.IncarnationID{99}
	repository := &durableRecoveryRepository{generations: map[string]ports.SnapshotGeneration{record.Name: generation}, errors: make(map[string]error), loads: make(map[string]int), repairs: make(map[string]domain.CheckpointRef)}
	d, catalogue := newDurableRecoveryDaemon(t, []domain.CatalogueRecord{record}, repository)

	d.restoreCatalogue(context.Background(), catalogue.Records())

	require.Equal(t, runtimeDegraded, d.stopped[record.Name].state)
	got, _ := catalogue.Record(record.Name)
	require.Equal(t, domain.RecoveryDegraded, got.RecoveryState)
}

func TestCatalogueRestoreDegradedVisible(t *testing.T) {
	record := durableRecoveryRecord(0)
	record.RecoveryState = domain.RecoveryDegraded
	record.DegradedReason = "checkpoint validation failed"
	repository := &durableRecoveryRepository{generations: make(map[string]ports.SnapshotGeneration), errors: make(map[string]error), loads: make(map[string]int), repairs: make(map[string]domain.CheckpointRef)}
	d, catalogue := newDurableRecoveryDaemon(t, []domain.CatalogueRecord{record}, repository)

	d.restoreCatalogue(context.Background(), catalogue.Records())

	require.Equal(t, runtimeDegraded, d.stopped[record.Name].state)
	require.Zero(t, repository.loads[record.Name])
}
