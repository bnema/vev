package daemon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	recoveryusecase "github.com/bnema/vev/internal/usecase/recovery"
	snapcodec "github.com/bnema/vev/internal/usecase/snapshot"
)

type durableRecoveryCatalogue struct {
	ports.Catalogue
	mu              sync.Mutex
	records         map[string]domain.CatalogueRecord
	metadataUpdates []domain.CatalogueMetadataUpdate
	replaceErr      error
}

type recordErrorCatalogue struct {
	*durableRecoveryCatalogue
	err error
}

func (c recordErrorCatalogue) Record(string) (domain.CatalogueRecord, bool, error) {
	return domain.CatalogueRecord{}, false, c.err
}

func newDurableRecoveryCatalogue(records []domain.CatalogueRecord) *durableRecoveryCatalogue {
	catalogue := &durableRecoveryCatalogue{records: make(map[string]domain.CatalogueRecord, len(records))}
	for _, record := range records {
		catalogue.records[record.Name] = record
	}
	return catalogue
}

func (c *durableRecoveryCatalogue) Records() ([]domain.CatalogueRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	records := make([]domain.CatalogueRecord, 0, len(c.records))
	for _, record := range c.records {
		records = append(records, record)
	}
	return records, nil
}

func (c *durableRecoveryCatalogue) Record(name string) (domain.CatalogueRecord, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	record, ok := c.records[name]
	return record, ok, nil
}

func mustDurableRecords(t *testing.T, c *durableRecoveryCatalogue) []domain.CatalogueRecord {
	t.Helper()
	records, err := c.Records()
	require.NoError(t, err)
	return records
}

func (c *durableRecoveryCatalogue) Create(record domain.CatalogueRecord) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.records[record.Name]; exists {
		return errors.New("catalogue record already exists")
	}
	c.records[record.Name] = record
	return nil
}

func (c *durableRecoveryCatalogue) Delete(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.records, name)
	return nil
}

func (c *durableRecoveryCatalogue) Replace(name string, record domain.CatalogueRecord) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.replaceErr != nil {
		return c.replaceErr
	}
	if name != record.Name {
		return errors.New("catalogue key mismatch")
	}
	c.records[name] = record
	return nil
}

func (c *durableRecoveryCatalogue) UpdateMetadata(update domain.CatalogueMetadataUpdate) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	record, ok := c.records[update.Name]
	if !ok {
		return errors.New("catalogue record not found")
	}
	if update.IncarnationID == (domain.IncarnationID{}) || record.IncarnationID != update.IncarnationID {
		return errors.New("catalogue incarnation changed")
	}
	if update.Cwd != nil {
		record.Cwd = *update.Cwd
	}
	if update.UpdatedAt != nil {
		record.UpdatedAt = *update.UpdatedAt
	}
	if update.LastUsedSeq != nil {
		record.LastUsedSeq = *update.LastUsedSeq
	}
	if update.TabNames != nil {
		record.TabNames = append([]string(nil), (*update.TabNames)...)
	}
	c.records[update.Name] = record
	c.metadataUpdates = append(c.metadataUpdates, update)
	return nil
}

func (c *durableRecoveryCatalogue) MetadataUpdates() []domain.CatalogueMetadataUpdate {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]domain.CatalogueMetadataUpdate(nil), c.metadataUpdates...)
}

func (c *durableRecoveryCatalogue) Sync() error  { return nil }
func (c *durableRecoveryCatalogue) Close() error { return nil }

type durableRecoveryRepository struct {
	ports.SnapshotRepository
	mu          sync.Mutex
	generations map[string]ports.SnapshotGeneration
	errors      map[string]error
	loads       map[string]int
	repairs     map[string]domain.CheckpointRef
	deleted     []domain.IncarnationID
	onLoad      func()
	repairErr   error
	deleteErr   error
	repairCalls int
}

func (r *durableRecoveryRepository) LoadCheckpoint(_ context.Context, id domain.IncarnationID, name string, ref ports.CheckpointRef) (ports.SnapshotGeneration, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.loads == nil {
		r.loads = make(map[string]int)
	}
	r.loads[name]++
	if r.onLoad != nil {
		r.onLoad()
	}
	if err := r.errors[name]; err != nil {
		return ports.SnapshotGeneration{}, err
	}
	generation, ok := r.generations[name]
	if !ok || generation.Generation != ref.Generation {
		return ports.SnapshotGeneration{}, errors.New("checkpoint unavailable")
	}
	return generation, nil
}

func (r *durableRecoveryRepository) ReconcileCheckpoint(_ context.Context, id domain.IncarnationID, ref ports.CheckpointRef) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.repairCalls++
	if r.repairErr != nil {
		return r.repairErr
	}
	if r.repairs == nil {
		r.repairs = make(map[string]domain.CheckpointRef)
	}
	r.repairs[id.String()] = ref
	return nil
}

func (r *durableRecoveryRepository) DeleteIncarnation(_ context.Context, id domain.IncarnationID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deleted = append(r.deleted, id)
	return r.deleteErr
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
	coordinator := recoveryusecase.NewCoordinator(catalogue, repository, bytes.NewReader(bytes.Repeat([]byte{0xa5}, 16)))
	d := New(nil, stubClock{}, slog.New(slog.NewTextHandler(io.Discard, nil)), WithCatalogue(catalogue, records), WithSnapshotRepository(repository), WithRecoveryCoordinator(coordinator))
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
		Committed:     &priorRef,
	}
	catalogue := newDurableRecoveryCatalogue([]domain.CatalogueRecord{prior})
	repository := &durableRecoveryRepository{}
	coordinator := recoveryusecase.NewCoordinator(catalogue, repository, nil)
	d := New(nil, stubClock{}, slog.New(slog.NewTextHandler(io.Discard, nil)), WithCatalogue(catalogue, []domain.CatalogueRecord{prior}), WithSnapshotRepository(repository), WithRecoveryCoordinator(coordinator))
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
	got, ok, _ := catalogue.Record(prior.Name)
	require.True(t, ok)
	require.Equal(t, prior, got, "a timed-out final checkpoint must not change authoritative bytes")
	sess.snapshotMu.Lock()
	published := sess.snapshotPublishedCheckpoint
	sess.snapshotMu.Unlock()
	require.Equal(t, priorRef, *published)
}

func TestCatalogueRestoreIndependent(t *testing.T) {
	t.Run("empty catalogue restoration", func(t *testing.T) {
		emptyDaemon, emptyCatalogue := newDurableRecoveryDaemon(t, nil, &durableRecoveryRepository{})
		emptyDone := make(chan struct{})
		go func() {
			emptyDaemon.restoreCatalogue(context.Background(), mustDurableRecords(t, emptyCatalogue))
			close(emptyDone)
		}()
		select {
		case <-emptyDone:
		case <-time.After(time.Second):
			t.Fatal("empty catalogue restoration blocked")
		}
	})

	t.Run("healthy and broken restoration", func(t *testing.T) {
		healthy := durableRecoveryRecord(0)
		broken := durableRecoveryRecord(1)
		repository := &durableRecoveryRepository{
			generations: map[string]ports.SnapshotGeneration{healthy.Name: validGeneration(t, healthy)},
			errors:      map[string]error{broken.Name: errors.New("corrupt checkpoint")},
			loads:       make(map[string]int),
			repairs:     make(map[string]domain.CheckpointRef),
		}
		d, catalogue := newDurableRecoveryDaemon(t, []domain.CatalogueRecord{healthy, broken}, repository)

		d.restoreCatalogue(context.Background(), mustDurableRecords(t, catalogue))

		d.mu.Lock()
		healthyEntry := d.stopped[healthy.Name]
		brokenEntry := d.stopped[broken.Name]
		d.mu.Unlock()
		require.Equal(t, ports.SessionStopped, healthyEntry.state)
		require.Equal(t, ports.SessionBroken, brokenEntry.state)
		for name, done := range map[string]<-chan struct{}{healthy.Name: healthyEntry.restoreDone, broken.Name: brokenEntry.restoreDone} {
			select {
			case <-done:
			default:
				t.Fatalf("restore completion for %q was not closed", name)
			}
		}
		brokenRecord, ok, _ := catalogue.Record(broken.Name)
		require.True(t, ok)
		require.Equal(t, "checkpoint validation failed", brokenRecord.DegradedReason)
		require.Equal(t, 1, repository.loads[healthy.Name])
		require.Equal(t, 1, repository.loads[broken.Name])
		require.Empty(t, d.sessions, "a corrupt record must never open a replacement runtime")
	})

}

func TestCatalogueRestoreResetsOnlyIncompatibleCheckpoint(t *testing.T) {
	incompatible := durableRecoveryRecord(0)
	healthy := durableRecoveryRecord(1)
	repository := &durableRecoveryRepository{
		generations: map[string]ports.SnapshotGeneration{healthy.Name: validGeneration(t, healthy)},
		errors:      map[string]error{incompatible.Name: fmt.Errorf("load envelope: %w", snapcodec.ErrBadVersion)},
		loads:       make(map[string]int),
		repairs:     make(map[string]domain.CheckpointRef),
	}
	d, catalogue := newDurableRecoveryDaemon(t, []domain.CatalogueRecord{incompatible, healthy}, repository)

	d.restoreCatalogue(context.Background(), mustDurableRecords(t, catalogue))

	fresh, ok, err := catalogue.Record(incompatible.Name)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, domain.IncarnationID(bytes.Repeat([]byte{0xa5}, 16)), fresh.IncarnationID)
	require.Nil(t, fresh.Committed)
	require.Empty(t, fresh.DegradedReason)
	require.Empty(t, fresh.TabNames)
	require.Equal(t, []domain.IncarnationID{incompatible.IncarnationID}, repository.deleted)
	require.Equal(t, ports.SessionStopped, d.stopped[incompatible.Name].state)
	require.Equal(t, fresh, d.stopped[incompatible.Name].record)

	restoredSibling, ok, err := catalogue.Record(healthy.Name)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, healthy, restoredSibling)
	require.Equal(t, ports.SessionStopped, d.stopped[healthy.Name].state)
	require.Equal(t, 1, repository.loads[healthy.Name])
}

func TestIncompatibleCheckpointDeleteFailurePublishesFreshStoppedAuthority(t *testing.T) {
	record := durableRecoveryRecord(0)
	deleteErr := errors.New("delete old incarnation")
	repository := &durableRecoveryRepository{
		errors:    map[string]error{record.Name: snapcodec.ErrBadVersion},
		loads:     make(map[string]int),
		repairs:   make(map[string]domain.CheckpointRef),
		deleteErr: deleteErr,
	}
	d, catalogue := newDurableRecoveryDaemon(t, []domain.CatalogueRecord{record}, repository)
	d.mu.Lock()
	restoreDone := d.stopped[record.Name].restoreDone
	d.mu.Unlock()

	restoreErr := d.restoreRecord(context.Background(), record)
	require.NoError(t, restoreErr, "post-commit cleanup failure must not fail the authority transition")

	fresh, ok, err := catalogue.Record(record.Name)
	require.NoError(t, err)
	require.True(t, ok)
	require.NotEqual(t, record.IncarnationID, fresh.IncarnationID)
	require.Nil(t, fresh.Committed)
	require.Empty(t, fresh.DegradedReason)
	d.mu.Lock()
	entry := d.stopped[record.Name]
	d.mu.Unlock()
	require.Equal(t, ports.SessionStopped, entry.state)
	require.Equal(t, fresh, entry.record)
	require.Equal(t, restoreDone, entry.restoreDone)
	select {
	case <-entry.restoreDone:
		t.Fatal("direct restore closed its coordinator-owned completion barrier")
	default:
	}

	d.finishRecordRestore(record, restoreErr, restoreDone)
	d.mu.Lock()
	entry = d.stopped[record.Name]
	d.mu.Unlock()
	require.Equal(t, ports.SessionStopped, entry.state)
	require.Equal(t, fresh, entry.record)
	select {
	case <-entry.restoreDone:
	default:
		t.Fatal("restore completion was not signaled")
	}
	require.Equal(t, []domain.IncarnationID{record.IncarnationID}, repository.deleted)
}

func TestIncompatibleCheckpointPreCommitFailureReturnsError(t *testing.T) {
	record := durableRecoveryRecord(0)
	cause := errors.New("catalogue replace failed")
	repository := &durableRecoveryRepository{
		errors:  map[string]error{record.Name: snapcodec.ErrBadVersion},
		loads:   make(map[string]int),
		repairs: make(map[string]domain.CheckpointRef),
	}
	d, catalogue := newDurableRecoveryDaemon(t, []domain.CatalogueRecord{record}, repository)
	catalogue.replaceErr = cause

	err := d.restoreRecord(context.Background(), record)

	require.ErrorIs(t, err, cause)
	persisted, ok, recordErr := catalogue.Record(record.Name)
	require.NoError(t, recordErr)
	require.True(t, ok)
	require.Equal(t, record, persisted)
	require.Empty(t, repository.deleted)
}

func TestRestoreLoadFailuresRetainCheckpointWithoutDeletion(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "corruption", err: errors.New("corrupt checkpoint")},
		{name: "generic", err: errors.New("load failed")},
		{name: "io", err: &os.PathError{Op: "open", Path: "/snapshot", Err: syscall.EACCES}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := durableRecoveryRecord(0)
			repository := &durableRecoveryRepository{
				errors:  map[string]error{record.Name: tt.err},
				loads:   make(map[string]int),
				repairs: make(map[string]domain.CheckpointRef),
			}
			d, catalogue := newDurableRecoveryDaemon(t, []domain.CatalogueRecord{record}, repository)

			d.restoreCatalogue(context.Background(), mustDurableRecords(t, catalogue))

			persisted, ok, err := catalogue.Record(record.Name)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, record.IncarnationID, persisted.IncarnationID)
			require.Equal(t, record.Committed, persisted.Committed)
			require.Equal(t, "checkpoint validation failed", persisted.DegradedReason)
			require.Equal(t, ports.SessionBroken, d.stopped[record.Name].state)
			require.Empty(t, repository.deleted)
		})
	}
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

	d.restoreCatalogue(context.Background(), mustDurableRecords(t, catalogue))

	require.Len(t, d.stopped, count)
	for _, record := range records {
		require.Equal(t, ports.SessionStopped, d.stopped[record.Name].state)
		require.Equal(t, 1, repository.loads[record.Name])
	}
}

func TestCatalogueRestoreSingleSessionOver4096(t *testing.T) {
	record := durableRecoveryRecord(0)
	repository := &durableRecoveryRepository{generations: map[string]ports.SnapshotGeneration{record.Name: validGeneration(t, record)}, errors: make(map[string]error), loads: make(map[string]int), repairs: make(map[string]domain.CheckpointRef)}
	d, catalogue := newDurableRecoveryDaemon(t, []domain.CatalogueRecord{record}, repository)

	// The repository may contain any number of stale entries; restoration has no
	// discovery API and therefore performs exactly one direct catalogue lookup.
	d.restoreCatalogue(context.Background(), mustDurableRecords(t, catalogue))

	require.Equal(t, ports.SessionStopped, d.stopped[record.Name].state)
	require.Equal(t, 1, repository.loads[record.Name])
}

func TestCatalogueRestoreIncarnationMismatch(t *testing.T) {
	record := durableRecoveryRecord(0)
	generation := validGeneration(t, record)
	generation.IncarnationID = domain.IncarnationID{99}
	repository := &durableRecoveryRepository{generations: map[string]ports.SnapshotGeneration{record.Name: generation}, errors: make(map[string]error), loads: make(map[string]int), repairs: make(map[string]domain.CheckpointRef)}
	d, catalogue := newDurableRecoveryDaemon(t, []domain.CatalogueRecord{record}, repository)

	d.restoreCatalogue(context.Background(), mustDurableRecords(t, catalogue))

	require.Equal(t, ports.SessionBroken, d.stopped[record.Name].state)
	got, _, _ := catalogue.Record(record.Name)
	require.Equal(t, "checkpoint validation failed", got.DegradedReason)
}

type cancellationRecoveryRepository struct {
	ports.SnapshotRepository
	started chan struct{}
	once    sync.Once
}

func (r *cancellationRecoveryRepository) LoadCheckpoint(ctx context.Context, _ domain.IncarnationID, _ string, _ ports.CheckpointRef) (ports.SnapshotGeneration, error) {
	r.once.Do(func() { close(r.started) })
	<-ctx.Done()
	return ports.SnapshotGeneration{}, ctx.Err()
}

type recoveryCountingPTYFactory struct {
	calls atomic.Int32
}

func (f *recoveryCountingPTYFactory) Open(context.Context, string, []string, []string, string, domain.Size) (ports.PTY, error) {
	f.calls.Add(1)
	return nil, errors.New("unexpected PTY open")
}

func TestSessionRecoveryUsesIncomingCommandContext(t *testing.T) {
	type contextKey struct{}
	key := contextKey{}
	ctx := context.WithValue(t.Context(), key, "request")
	wantErr := errors.New("stop after context capture")
	record := durableRecoveryRecord(0)
	record.DegradedReason = "checkpoint validation failed"
	catalogue := portsmocks.NewMockCatalogue(t)
	repository := portsmocks.NewMockSnapshotRepository(t)
	catalogue.EXPECT().Record(record.Name).Return(record, true, nil).Once()
	catalogue.EXPECT().Replace(record.Name, mock.Anything).Return(nil).Once()
	seen := false
	repository.EXPECT().DeleteIncarnation(mock.Anything, record.IncarnationID).
		Run(func(got context.Context, _ domain.IncarnationID) { seen = got.Value(key) == "request" }).
		Return(wantErr).Once()

	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	d.catalogue = catalogue
	d.recovery = recoveryusecase.NewCoordinator(catalogue, repository, bytes.NewReader(bytes.Repeat([]byte{9}, 16)))

	_, err := (controlExec{ctx: ctx, d: d, recoveryName: record.Name}).SessionRecovery("discard")
	require.ErrorIs(t, err, wantErr)
	require.True(t, seen)
}

type temporaryRestoreError struct{}

func (temporaryRestoreError) Error() string   { return "temporary restore failure" }
func (temporaryRestoreError) Temporary() bool { return true }

type badVersionLookalikeError struct{}

func (badVersionLookalikeError) Error() string        { return "bad version lookalike" }
func (badVersionLookalikeError) Is(target error) bool { return target == snapcodec.ErrBadVersion }

type cyclicUnwrapError struct{}

func (*cyclicUnwrapError) Error() string { return "cyclic unwrap" }
func (e *cyclicUnwrapError) Unwrap() error {
	return e
}

func TestUnambiguousBadVersionErrorClassification(t *testing.T) {
	tests := []struct {
		name         string
		err          error
		incompatible bool
	}{
		{name: "nil"},
		{name: "exact sentinel", err: snapcodec.ErrBadVersion, incompatible: true},
		{name: "single cause chain", err: fmt.Errorf("load checkpoint: %w", fmt.Errorf("decode manifest: %w", snapcodec.ErrBadVersion)), incompatible: true},
		{name: "single entry multi cause", err: errors.Join(snapcodec.ErrBadVersion)},
		{name: "joined retryable branch", err: errors.Join(snapcodec.ErrBadVersion, syscall.ENOSPC)},
		{name: "custom Is lookalike", err: badVersionLookalikeError{}},
		{name: "wrapped custom Is lookalike", err: fmt.Errorf("load checkpoint: %w", badVersionLookalikeError{})},
		{name: "chain ending elsewhere", err: fmt.Errorf("load checkpoint: %w", errors.New("corrupt checkpoint"))},
		{name: "cyclic custom chain", err: &cyclicUnwrapError{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.incompatible, isUnambiguousBadVersionError(tt.err))
		})
	}
}

func TestRetryableRestoreLoadErrorClassification(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		retryable bool
	}{
		{name: "missing path", err: fmt.Errorf("wrapped: %w", &os.PathError{Op: "open", Path: "/snapshot", Err: syscall.ENOENT})},
		{name: "permission denied", err: fmt.Errorf("wrapped: %w", &os.PathError{Op: "open", Path: "/snapshot", Err: syscall.EACCES})},
		{name: "invalid path operation", err: fmt.Errorf("wrapped: %w", &os.PathError{Op: "open", Path: "/snapshot", Err: syscall.EINVAL})},
		{name: "temporary path failure", err: fmt.Errorf("wrapped: %w", &os.PathError{Op: "open", Path: "/snapshot", Err: syscall.EAGAIN}), retryable: true},
		{name: "resource exhausted path", err: fmt.Errorf("wrapped: %w", &os.PathError{Op: "open", Path: "/snapshot", Err: syscall.ENOSPC}), retryable: true},
		{name: "temporary interface", err: fmt.Errorf("wrapped: %w", temporaryRestoreError{}), retryable: true},
		{name: "corrupt data", err: errors.New("corrupt checkpoint")},
		{name: "context canceled", err: context.Canceled},
		{name: "context deadline exceeded", err: context.DeadlineExceeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.retryable, isRetryableRestoreLoadError(tt.err))
		})
	}
}

func TestFinishRecordRestoreDoesNotDegradeStaleCatalogueAuthority(t *testing.T) {
	for _, tt := range []struct {
		name  string
		setup func(*Daemon, *durableRecoveryCatalogue, domain.CatalogueRecord)
	}{
		{
			name: "missing record",
			setup: func(_ *Daemon, c *durableRecoveryCatalogue, record domain.CatalogueRecord) {
				c.mu.Lock()
				delete(c.records, record.Name)
				c.mu.Unlock()
			},
		},
		{
			name: "replacement incarnation",
			setup: func(_ *Daemon, c *durableRecoveryCatalogue, record domain.CatalogueRecord) {
				record.IncarnationID = domain.IncarnationID{99}
				c.mu.Lock()
				c.records[record.Name] = record
				c.mu.Unlock()
			},
		},
		{
			name: "catalogue read error",
			setup: func(d *Daemon, c *durableRecoveryCatalogue, _ domain.CatalogueRecord) {
				d.catalogue = recordErrorCatalogue{durableRecoveryCatalogue: c, err: errors.New("read failed")}
				d.recovery = recoveryusecase.NewCoordinator(d.catalogue, nil, nil)
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			record := durableRecoveryRecord(0)
			d, catalogue := newDurableRecoveryDaemon(t, []domain.CatalogueRecord{record}, &durableRecoveryRepository{})
			tt.setup(d, catalogue, record)
			d.mu.Lock()
			before := d.stopped[record.Name]
			d.mu.Unlock()

			d.finishRecordRestore(record, errors.New("invalid checkpoint"), before.restoreDone)

			d.mu.Lock()
			after := d.stopped[record.Name]
			d.mu.Unlock()
			require.Equal(t, before.record, after.record)
			require.Equal(t, before.state, after.state)
		})
	}
}

func TestPersistAndRegisterRestoredSessionRegistrationFailurePreservesStoppedEntry(t *testing.T) {
	record := durableRecoveryRecord(0)
	d, _ := newDurableRecoveryDaemon(t, []domain.CatalogueRecord{record}, &durableRecoveryRepository{})
	d.mu.Lock()
	before := d.stopped[record.Name]
	collisionID := domain.SessionID(fmt.Sprintf("sess-%d", d.nextID))
	d.sessions[collisionID] = &session{sessionCore: sessionCore{id: collisionID, name: "collision"}}
	d.mu.Unlock()
	sess := newSnapshotTestSession(t, record.Name, false, record.Cwd)
	sess.incarnation = record.IncarnationID

	registered, err := d.persistAndRegisterRestoredSession(t.Context(), sess)

	require.ErrorContains(t, err, "registry rejected")
	require.False(t, registered)
	d.mu.Lock()
	after := d.stopped[record.Name]
	d.mu.Unlock()
	require.Equal(t, before, after, "failed runtime publication must preserve the exact stopped authority and barrier")
	require.Equal(t, before.restoreDone, after.restoreDone)
}

func TestCreateNamedSessionRegistrationFailureRollsBackDurableState(t *testing.T) {
	for _, tc := range []struct {
		name     string
		records  []domain.CatalogueRecord
		sessName string
		cwd      string
	}{
		{name: "fresh create", sessName: "fresh", cwd: "/fresh"},
		{name: "stopped metadata update", records: []domain.CatalogueRecord{durableRecoveryRecord(0)}, sessName: durableRecoveryRecord(0).Name, cwd: "/resumed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pty, release := newBlockingPTY(t)
			defer release()
			catalogue := newDurableRecoveryCatalogue(tc.records)
			coordinator := recoveryusecase.NewCoordinator(catalogue, nil, bytes.NewReader(bytes.Repeat([]byte{0x6a}, 16)))
			d := newTestDaemon(t, newFactory(t, pty), stubClock{})
			WithCatalogue(catalogue, tc.records)(d)
			WithRecoveryCoordinator(coordinator)(d)

			d.mu.Lock()
			for _, record := range tc.records {
				d.stopped[record.Name] = stoppedSession{
					name: record.Name, cwd: record.Cwd, createdAt: record.CreatedAt,
					incarnation: record.IncarnationID, lastUsedSeq: record.LastUsedSeq,
					tabNames: append([]string(nil), record.TabNames...), record: record,
					state: ports.SessionStopped, restoreDone: make(chan struct{}),
				}
			}
			beforeStopped, hadStopped := d.stopped[tc.sessName]
			collisionID := domain.SessionID(fmt.Sprintf("sess-%d", d.nextID))
			d.sessions[collisionID] = &session{sessionCore: sessionCore{id: collisionID, name: "collision"}}
			d.mu.Unlock()
			beforeRecords := mustDurableRecords(t, catalogue)

			_, err := createSessionForTest(d, tc.sessName, false, tc.cwd, defaultSize, terminalEnv{}, d.baseEnv)

			require.ErrorContains(t, err, "registry rejected")
			require.ElementsMatch(t, beforeRecords, mustDurableRecords(t, catalogue), "failed runtime publication must leave no created record or metadata mutation")
			d.mu.Lock()
			afterStopped, stillStopped := d.stopped[tc.sessName]
			d.mu.Unlock()
			require.Equal(t, hadStopped, stillStopped)
			if hadStopped {
				require.Equal(t, beforeStopped, afterStopped, "resume publication failure must restore the exact stopped state")
				require.Equal(t, beforeStopped.restoreDone, afterStopped.restoreDone)
			}
		})
	}
}

func TestPersistAndRegisterRestoredSessionRejectsReplacementIncarnation(t *testing.T) {
	record := durableRecoveryRecord(0)
	d, catalogue := newDurableRecoveryDaemon(t, []domain.CatalogueRecord{record}, &durableRecoveryRepository{})
	replacement := record
	replacement.IncarnationID = domain.IncarnationID{99}
	require.NoError(t, catalogue.Replace(record.Name, replacement))
	sess := newSnapshotTestSession(t, record.Name, false, record.Cwd)
	sess.incarnation = record.IncarnationID

	registered, err := d.persistAndRegisterRestoredSession(t.Context(), sess)
	require.ErrorContains(t, err, "incarnation")
	require.False(t, registered)
	require.Empty(t, d.sessions)
}

func TestSessionRecoveryCommand(t *testing.T) {
	degraded := durableRecoveryRecord(2)
	degraded.DegradedReason = "checkpoint validation failed"
	fresh := degraded
	fresh.IncarnationID = domain.IncarnationID{9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9}
	fresh.Committed = nil
	fresh.DegradedReason = ""
	catalogue := portsmocks.NewMockCatalogue(t)
	repository := portsmocks.NewMockSnapshotRepository(t)
	catalogue.EXPECT().Record(degraded.Name).Return(degraded, true, nil).Once()
	catalogue.EXPECT().Replace(degraded.Name, fresh).Return(nil).Once()
	repository.EXPECT().DeleteIncarnation(mock.Anything, degraded.IncarnationID).Return(nil).Once()
	catalogue.EXPECT().Record(degraded.Name).Return(fresh, true, nil).Once()
	coordinator := recoveryusecase.NewCoordinator(catalogue, repository, bytes.NewReader(bytes.Repeat([]byte{9}, 16)))
	d := New(nil, stubClock{}, slog.New(slog.NewTextHandler(io.Discard, nil)),
		WithCatalogue(catalogue, []domain.CatalogueRecord{degraded}),
		WithSnapshotRepository(repository), WithRecoveryCoordinator(coordinator))

	_, err := (controlExec{d: d, recoveryName: degraded.Name}).SessionRecovery("discard")
	require.NoError(t, err)
	require.Equal(t, ports.SessionStopped, d.stopped[degraded.Name].state)
	require.Equal(t, fresh, d.stopped[degraded.Name].record)
}

func TestListShowsDegraded(t *testing.T) {
	fresh := durableRecoveryRecord(0)
	fresh.Committed = nil
	restoring := durableRecoveryRecord(1)
	degraded := durableRecoveryRecord(2)
	degraded.DegradedReason = "checkpoint validation failed"
	d, _ := newDurableRecoveryDaemon(t, []domain.CatalogueRecord{fresh, restoring, degraded}, &durableRecoveryRepository{})

	listed := listSessions(t, d)
	require.Equal(t, []ports.SessionInfo{
		{Name: fresh.Name, State: ports.SessionStopped},
		{Name: restoring.Name, State: ports.SessionStopped},
		{Name: degraded.Name, State: ports.SessionBroken},
	}, listed.Sessions)
}

func TestListPurgingDominatesRestoringState(t *testing.T) {
	record := durableRecoveryRecord(0)
	d, _ := newDurableRecoveryDaemon(t, []domain.CatalogueRecord{record}, &durableRecoveryRepository{})
	d.mu.Lock()
	entry := d.stopped[record.Name]
	entry.purging = true
	entry.state = ports.SessionStopped
	d.stopped[record.Name] = entry
	d.mu.Unlock()

	listed := listSessions(t, d)
	require.Equal(t, []ports.SessionInfo{{Name: record.Name, State: ports.SessionBroken}}, listed.Sessions)
}

func TestAttachWaitsForRestore(t *testing.T) {
	record := durableRecoveryRecord(0)
	factory := &recoveryCountingPTYFactory{}
	d := New(factory, stubClock{}, slog.New(slog.NewTextHandler(io.Discard, nil)), WithCatalogue(newDurableRecoveryCatalogue([]domain.CatalogueRecord{record}), []domain.CatalogueRecord{record}))
	d.serveCtx, d.serveCancel = context.WithCancel(context.Background())
	defer d.serveCancel()

	result := make(chan error, 1)
	go func() {
		_, _, err := d.route(ports.Hello{Version: ports.ProtocolVersion, Intent: ports.IntentAttach, Name: record.Name, Size: domain.Size{Cols: 80, Rows: 24}}, nil)
		result <- err
	}()

	select {
	case err := <-result:
		t.Fatalf("attach returned before target restoration completed: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	require.Zero(t, factory.calls.Load())

	d.setStoppedRecovery(record, ports.SessionBroken)
	d.mu.Lock()
	closeRuntimeRestoreDoneLocked(d.stopped[record.Name].restoreDone)
	d.mu.Unlock()
	var protocolError *protoErr
	require.ErrorAs(t, <-result, &protocolError)
	require.Equal(t, ports.ErrInternal, protocolError.code)
	require.Zero(t, factory.calls.Load())
}

func TestAttachBarrierSurvivesLiveRegistryPublication(t *testing.T) {
	record := durableRecoveryRecord(0)
	d, _ := newDurableRecoveryDaemon(t, []domain.CatalogueRecord{record}, &durableRecoveryRepository{})
	d.mu.Lock()
	done := d.stopped[record.Name].restoreDone
	d.mu.Unlock()
	sess := &session{sessionCore: sessionCore{name: record.Name, incarnation: record.IncarnationID}}

	registered, err := d.persistAndRegisterRestoredSession(t.Context(), sess)
	require.NoError(t, err)
	require.True(t, registered)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, d.waitForTargetRestore(cancelled, record.Name), context.Canceled,
		"an attach racing after live publication must still observe the restore barrier")

	d.finishRecordRestore(record, nil, done)
	require.NoError(t, d.waitForTargetRestore(context.Background(), record.Name),
		"attach must proceed after successful restoration completes")
}

func TestAttachRestoreWaitHonorsContextCancellation(t *testing.T) {
	record := durableRecoveryRecord(0)
	d, _ := newDurableRecoveryDaemon(t, []domain.CatalogueRecord{record}, &durableRecoveryRepository{})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- d.waitForTargetRestore(ctx, record.Name) }()

	select {
	case err := <-result:
		t.Fatalf("restore wait returned before cancellation: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	cancel()
	require.ErrorIs(t, <-result, context.Canceled)
}

func TestRestoreCancellationTransitionsBeforeAttachCompletion(t *testing.T) {
	record := durableRecoveryRecord(0)
	repository := &cancellationRecoveryRepository{started: make(chan struct{})}
	d, catalogue := newDurableRecoveryDaemon(t, []domain.CatalogueRecord{record}, repository)
	restoreCtx, cancelRestore := context.WithCancel(context.Background())
	restored := make(chan struct{})
	go func() {
		d.restoreCatalogue(restoreCtx, mustDurableRecords(t, catalogue))
		close(restored)
	}()
	<-repository.started

	attached := make(chan error, 1)
	go func() {
		_, _, err := d.route(ports.Hello{Version: ports.ProtocolVersion, Intent: ports.IntentAttach, Name: record.Name}, nil)
		attached <- err
	}()
	cancelRestore()

	var protocolError *protoErr
	require.ErrorAs(t, <-attached, &protocolError)
	require.Equal(t, ports.ErrInternal, protocolError.code)
	<-restored
	entry := d.stopped[record.Name]
	require.Equal(t, ports.SessionBroken, entry.state)
	require.Equal(t, "restore interrupted", entry.record.DegradedReason)
	select {
	case <-entry.restoreDone:
	default:
		t.Fatal("restore completion was not signaled")
	}
	committed, _, _ := catalogue.Record(record.Name)
	require.Equal(t, entry.record, committed)
}

func TestWaitForTargetRestoreRejectsClosedRestoringChannel(t *testing.T) {
	record := durableRecoveryRecord(0)
	d, _ := newDurableRecoveryDaemon(t, []domain.CatalogueRecord{record}, &durableRecoveryRepository{})
	d.mu.Lock()
	closeRuntimeRestoreDoneLocked(d.stopped[record.Name].restoreDone)
	d.mu.Unlock()

	result := make(chan error, 1)
	go func() { result <- d.waitForTargetRestore(context.Background(), record.Name) }()

	select {
	case err := <-result:
		var protocolError *protoErr
		require.ErrorAs(t, err, &protocolError)
		require.Equal(t, ports.ErrInternal, protocolError.code)
	case <-time.After(time.Second):
		t.Fatal("waiter spun on a closed restore channel")
	}
}

// A healthy catalogue record whose runtime registration was skipped is not
// degraded durable state, and must not send the operator to durable recovery.
func TestWaitForTargetRestoreDistinguishesHealthyRuntime(t *testing.T) {
	record := durableRecoveryRecord(0)
	tests := []struct {
		name    string
		state   ports.SessionState
		message string
	}{
		{name: "healthy record without runtime", state: ports.SessionStopped, message: "session was not restored into this daemon: " + record.Name},
		{name: "broken durable state", state: ports.SessionBroken, message: "session durable state is broken: " + record.Name},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, _ := newDurableRecoveryDaemon(t, []domain.CatalogueRecord{record}, &durableRecoveryRepository{})
			d.setStoppedRecovery(record, tt.state)
			d.mu.Lock()
			closeRuntimeRestoreDoneLocked(d.stopped[record.Name].restoreDone)
			d.mu.Unlock()

			var protocolError *protoErr
			require.ErrorAs(t, d.waitForTargetRestore(context.Background(), record.Name), &protocolError)
			require.Equal(t, ports.ErrInternal, protocolError.code)
			require.Equal(t, tt.message, protocolError.Error())
		})
	}
}

func TestAttachRejectsDegraded(t *testing.T) {
	record := durableRecoveryRecord(0)
	record.DegradedReason = "checkpoint validation failed"
	factory := &recoveryCountingPTYFactory{}
	d := New(factory, stubClock{}, slog.New(slog.NewTextHandler(io.Discard, nil)), WithCatalogue(newDurableRecoveryCatalogue([]domain.CatalogueRecord{record}), []domain.CatalogueRecord{record}))
	d.serveCtx, d.serveCancel = context.WithCancel(context.Background())
	defer d.serveCancel()

	_, _, err := d.route(ports.Hello{Version: ports.ProtocolVersion, Intent: ports.IntentAttach, Name: record.Name}, nil)
	var protocolError *protoErr
	require.ErrorAs(t, err, &protocolError)
	require.Equal(t, ports.ErrInternal, protocolError.code)
	require.Zero(t, factory.calls.Load())
}

func TestRestoreUnavailableReleasesPerRecordWaiters(t *testing.T) {
	record := durableRecoveryRecord(0)
	d, _ := newDurableRecoveryDaemon(t, []domain.CatalogueRecord{record}, nil)
	d.closeRestoreDone()

	d.mu.Lock()
	entry := d.stopped[record.Name]
	d.mu.Unlock()
	require.Equal(t, ports.SessionBroken, entry.state)
	select {
	case <-entry.restoreDone:
	default:
		t.Fatal("per-record restore waiter remained blocked when repository was unavailable")
	}
}

func TestCatalogueRestoreDegradedVisible(t *testing.T) {
	record := durableRecoveryRecord(0)
	record.DegradedReason = "checkpoint validation failed"
	repository := &durableRecoveryRepository{generations: make(map[string]ports.SnapshotGeneration), errors: make(map[string]error), loads: make(map[string]int), repairs: make(map[string]domain.CheckpointRef)}
	d, catalogue := newDurableRecoveryDaemon(t, []domain.CatalogueRecord{record}, repository)

	d.restoreCatalogue(context.Background(), mustDurableRecords(t, catalogue))

	require.Equal(t, ports.SessionBroken, d.stopped[record.Name].state)
	require.Zero(t, repository.loads[record.Name])
}
