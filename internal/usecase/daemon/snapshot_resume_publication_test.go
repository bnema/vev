package daemon

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

type snapshotLifecycleRepository struct {
	mu          sync.Mutex
	current     map[domain.IncarnationID]ports.CheckpointRef
	generations map[domain.IncarnationID]map[ports.CheckpointRef]ports.SnapshotGeneration
	attempts    []ports.SnapshotPublication
	errors      []error
}

func (r *snapshotLifecycleRepository) Publish(ctx context.Context, publication ports.SnapshotPublication) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	err := r.publish(ctx, publication)
	r.attempts = append(r.attempts, publication)
	r.errors = append(r.errors, err)
	return err
}

func (r *snapshotLifecycleRepository) publish(ctx context.Context, publication ports.SnapshotPublication) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if publication.IncarnationID == (domain.IncarnationID{}) || publication.Name == "" || publication.Generation == 0 {
		return fmt.Errorf("invalid snapshot publication identity")
	}

	current, exists := r.current[publication.IncarnationID]
	if !exists {
		if publication.Generation != 1 || publication.ParentCheckpoint != nil {
			return fmt.Errorf("first snapshot generation must be 1 with no parent")
		}
	} else {
		generation := r.generations[publication.IncarnationID][current]
		if generation.Name != publication.Name {
			return fmt.Errorf("snapshot name does not match current checkpoint")
		}
		if publication.Generation == current.Generation && bytes.Equal(publication.Manifest, generation.Manifest) {
			return nil
		}
		if publication.Generation != current.Generation+1 || publication.ParentCheckpoint == nil || *publication.ParentCheckpoint != current {
			return fmt.Errorf("snapshot generation or parent does not follow current checkpoint")
		}
	}

	ref := ports.CheckpointRef{Generation: publication.Generation, ManifestDigest: sha256.Sum256(publication.Manifest)}
	objects := make(map[ports.SnapshotDigest][]byte, len(publication.Objects))
	for _, object := range publication.Objects {
		objects[object.Digest] = append([]byte(nil), object.Data...)
	}
	generation := ports.SnapshotGeneration{
		IncarnationID:    publication.IncarnationID,
		Name:             publication.Name,
		Generation:       publication.Generation,
		ParentCheckpoint: cloneCheckpointRef(publication.ParentCheckpoint),
		Manifest:         append([]byte(nil), publication.Manifest...),
		Objects:          objects,
	}
	if r.generations[publication.IncarnationID] == nil {
		r.generations[publication.IncarnationID] = make(map[ports.CheckpointRef]ports.SnapshotGeneration)
	}
	r.generations[publication.IncarnationID][ref] = generation
	r.current[publication.IncarnationID] = ref
	return nil
}

func (r *snapshotLifecycleRepository) LoadCheckpoint(ctx context.Context, id domain.IncarnationID, name string, ref ports.CheckpointRef) (ports.SnapshotGeneration, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return ports.SnapshotGeneration{}, err
	}
	generation, ok := r.generations[id][ref]
	if !ok || generation.Name != name {
		return ports.SnapshotGeneration{}, fmt.Errorf("checkpoint not found")
	}
	return generation, nil
}

func (r *snapshotLifecycleRepository) ReconcileCheckpoint(ctx context.Context, id domain.IncarnationID, ref ports.CheckpointRef) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, ok := r.generations[id][ref]; !ok {
		return fmt.Errorf("checkpoint not found")
	}
	r.current[id] = ref
	return nil
}

func (r *snapshotLifecycleRepository) DeleteIncarnation(ctx context.Context, id domain.IncarnationID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	delete(r.current, id)
	delete(r.generations, id)
	return nil
}

func (r *snapshotLifecycleRepository) CollectGarbage(ctx context.Context, _ map[domain.IncarnationID]domain.CheckpointRef) error {
	return ctx.Err()
}

func (r *snapshotLifecycleRepository) results() ([]ports.SnapshotPublication, []error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]ports.SnapshotPublication(nil), r.attempts...), append([]error(nil), r.errors...)
}

func newSnapshotLifecycleRepository(testing.TB) *snapshotLifecycleRepository {
	return &snapshotLifecycleRepository{
		current:     make(map[domain.IncarnationID]ports.CheckpointRef),
		generations: make(map[domain.IncarnationID]map[ports.CheckpointRef]ports.SnapshotGeneration),
	}
}

func cloneCheckpointRef(ref *domain.CheckpointRef) *domain.CheckpointRef {
	if ref == nil {
		return nil
	}
	clone := *ref
	return &clone
}

func TestNamedSessionPublishesNextCheckpointAfterClientSnatch(t *testing.T) {
	pty, release := newBlockingPTY(t)
	repository := newSnapshotLifecycleRepository(t)
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})
	WithSnapshotRepository(repository)(d)
	store, _ := newMockStore(t)
	WithStore(t, store)(d)
	startSnapshotEncodeWorker(t, d)
	t.Cleanup(func() {
		d.snapsEnabled = false
		release()
		d.sessWg.Wait()
	})

	firstTransport := &closeTrackingTransport{}
	sess, firstClient, err := d.route(snapshotLifecycleHello(ports.IntentNew, "work"), firstTransport)
	require.NoError(t, err)

	testAttachmentTab(sess).focusedPane().screen.Write([]byte("first checkpoint"))
	markSnapshotDirty(sess)
	require.True(t, d.scheduleSnapshot(sess))
	awaitSnapshotIdle(t, sess)
	d.snapshotNoticeMu.Lock()
	firstFailure := d.snapshotActiveFailureSignature
	d.snapshotNoticeMu.Unlock()
	attempts, publicationErrors := repository.results()
	require.Len(t, attempts, 1)
	require.NoError(t, publicationErrors[0])
	require.Empty(t, firstFailure)
	require.False(t, sess.snapDirty.Load())
	firstRecord := snapshotLifecycleRecord(t, d, sess.name)
	require.NotNil(t, firstRecord.Committed)
	require.Equal(t, uint64(1), firstRecord.Committed.Generation)

	secondTransport := &closeTrackingTransport{}
	snatchedSession, secondClient, err := d.route(snapshotLifecycleHello(ports.IntentAttach, "work"), secondTransport)
	require.NoError(t, err)
	require.Same(t, sess, snatchedSession, "client snatch must retain the live session")
	require.NotSame(t, firstClient, secondClient, "client B must displace client A")

	testAttachmentTab(sess).focusedPane().screen.Write([]byte("second checkpoint"))
	markSnapshotDirty(sess)
	require.True(t, d.scheduleFinalSnapshot(sess))
	awaitSnapshotIdle(t, sess)
	attempts, publicationErrors = repository.results()
	require.Len(t, attempts, 2)
	require.NoError(t, publicationErrors[1])
	require.False(t, sess.snapDirty.Load())

	secondRecord := snapshotLifecycleRecord(t, d, sess.name)
	require.NotNil(t, secondRecord.Committed)
	require.Equal(t, firstRecord.Committed.Generation+1, secondRecord.Committed.Generation,
		"client snatch must not reset or conflict with the repository generation")
	generation, err := repository.LoadCheckpoint(context.Background(), sess.incarnation, sess.name, *secondRecord.Committed)
	require.NoError(t, err)
	require.Equal(t, firstRecord.Committed, generation.ParentCheckpoint,
		"the post-snatch checkpoint must retain the committed parent")
}

func TestStoppedNamedSessionResumePublishesNextCheckpointInSameDaemon(t *testing.T) {
	keeperPTY, releaseKeeper := newBlockingPTY(t)
	firstPTY, releaseFirst := newBlockingPTY(t)
	resumedPTY, releaseResumed := newBlockingPTY(t)
	repository := newSnapshotLifecycleRepository(t)
	d := newTestDaemon(t, newFactorySeq(t, keeperPTY, firstPTY, resumedPTY), stubClock{})
	WithSnapshotRepository(repository)(d)
	store, _ := newMockStore(t)
	WithStore(t, store)(d)
	startSnapshotEncodeWorker(t, d)
	t.Cleanup(func() {
		d.snapsEnabled = false
		releaseKeeper()
		releaseFirst()
		releaseResumed()
		d.sessWg.Wait()
	})

	_, _, err := d.route(snapshotLifecycleHello(ports.IntentEphemeral, ""), &closeTrackingTransport{})
	require.NoError(t, err, "keeper session setup failed")
	sess, _, err := d.route(snapshotLifecycleHello(ports.IntentNew, "work"), &closeTrackingTransport{})
	require.NoError(t, err)

	testAttachmentTab(sess).focusedPane().screen.Write([]byte("first checkpoint"))
	markSnapshotDirty(sess)
	require.True(t, d.scheduleSnapshot(sess))
	awaitSnapshotClean(t, sess)
	firstRecord := snapshotLifecycleRecord(t, d, sess.name)
	require.NotNil(t, firstRecord.Committed)
	require.Equal(t, uint64(1), firstRecord.Committed.Generation)

	require.NoError(t, d.killSession(sess, ports.ReasonSessionKilled, false))
	d.mu.Lock()
	stopped, retained := d.stopped[sess.name]
	d.mu.Unlock()
	require.True(t, retained, "non-purged named session must remain stopped")
	require.False(t, stopped.purging)
	closedRecord := snapshotLifecycleRecord(t, d, sess.name)
	require.NotNil(t, closedRecord.Committed)
	require.Equal(t, firstRecord.Committed.Generation+1, closedRecord.Committed.Generation,
		"terminal checkpoint must be authoritative before same-daemon resume")

	resumed, _, err := d.route(snapshotLifecycleHello(ports.IntentAttach, sess.name), &closeTrackingTransport{})
	require.NoError(t, err)
	require.NotSame(t, sess, resumed, "stopped-session attach must create a new live runtime")
	require.Equal(t, sess.incarnation, resumed.incarnation, "resume must retain durable incarnation authority")

	testAttachmentTab(resumed).focusedPane().screen.Write([]byte("checkpoint after resume"))
	markSnapshotDirty(resumed)
	require.True(t, d.scheduleSnapshot(resumed))
	awaitSnapshotIdle(t, resumed)

	resumedRecord := snapshotLifecycleRecord(t, d, resumed.name)
	require.NotNil(t, resumedRecord.Committed)
	attempts, publicationErrors := repository.results()
	require.Len(t, attempts, 3, "the resumed checkpoint must reach the repository after the initial and terminal checkpoints")
	require.NoError(t, publicationErrors[2])
	require.False(t, resumed.snapDirty.Load(), "the published resumed checkpoint must clear dirty state")
	d.snapshotNoticeMu.Lock()
	failure := d.snapshotActiveFailureSignature
	d.snapshotNoticeMu.Unlock()
	require.Empty(t, failure, "successful resumed publication must not retain a failure signature")
	require.Equal(t, closedRecord.Committed.Generation+1, resumedRecord.Committed.Generation,
		"same-daemon resume must publish exactly committed generation+1")
	generation, err := repository.LoadCheckpoint(context.Background(), resumed.incarnation, resumed.name, *resumedRecord.Committed)
	require.NoError(t, err)
	require.Equal(t, closedRecord.Committed, generation.ParentCheckpoint,
		"same-daemon resume must use the committed terminal checkpoint as parent")
}

func snapshotLifecycleHello(intent uint8, name string) ports.Hello {
	return ports.Hello{
		Version: ports.ProtocolVersion,
		Intent:  intent,
		Name:    name,
		Cwd:     "/tmp",
		Size:    domain.Size{Cols: 80, Rows: 24},
	}
}

func snapshotLifecycleRecord(t testing.TB, d *Daemon, name string) domain.CatalogueRecord {
	t.Helper()
	record, ok, err := d.catalogue.Record(name)
	require.NoError(t, err)
	require.True(t, ok, "catalogue record %q not found", name)
	return record
}
