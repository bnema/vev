package snapshot

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/safedir"
)

func TestReadMaintenanceDirentPreservesCursorAndCloseFailuresWithoutPaths(t *testing.T) {
	privatePath := filepath.Join(t.TempDir(), "private-maintenance-directory")
	closeCause := errors.New("injected close failure")
	seekCause := errors.New("injected seek failure")
	readCause := errors.New("injected read failure")

	for _, tc := range []struct {
		name      string
		offset    int64
		seekErr   error
		readErr   error
		wantCause []error
		wantOps   []string
	}{
		{
			name:      "close only",
			wantCause: []error{closeCause},
			wantOps:   []string{"close maintenance directory"},
		},
		{
			name:      "seek and close",
			offset:    1,
			seekErr:   &os.PathError{Op: "seek", Path: privatePath, Err: seekCause},
			wantCause: []error{seekCause, closeCause},
			wantOps:   []string{"seek maintenance directory", "close maintenance directory"},
		},
		{
			name:      "read and close",
			readErr:   &os.PathError{Op: "read", Path: privatePath, Err: readCause},
			wantCause: []error{readCause, closeCause},
			wantOps:   []string{"read maintenance directory", "close maintenance directory"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := NewRepository(privateDir(t))
			repo.hooks.openMaintenanceDirectory = func(string) (maintenanceDirectory, error) {
				return fakeMaintenanceDirectory{
					seekErr:  tc.seekErr,
					readErr:  tc.readErr,
					closeErr: &os.PathError{Op: "close", Path: privatePath, Err: closeCause},
				}, nil
			}

			_, _, err := repo.readMaintenanceDirent(privatePath, 1, &maintenanceCursor{offset: tc.offset})
			if err == nil {
				t.Fatal("readMaintenanceDirent error = nil, want failure")
			}
			for _, cause := range tc.wantCause {
				if !errors.Is(err, cause) {
					t.Errorf("errors.Is(%v, %v) = false, want true", err, cause)
				}
			}
			for _, operation := range tc.wantOps {
				if !strings.Contains(err.Error(), operation) {
					t.Errorf("error = %q, want stable operation context %q", err, operation)
				}
			}
			if strings.Contains(err.Error(), privatePath) {
				t.Errorf("error leaks private path: %q", err)
			}
		})
	}
}

func TestReadMaintenanceDirentRejectsRecordShorterThanNameOffset(t *testing.T) {
	repo := NewRepository(privateDir(t))
	nameOffset := int(unsafe.Offsetof(syscall.Dirent{}.Name))
	recordData := make([]byte, nameOffset)
	reclenOffset := unsafe.Offsetof(syscall.Dirent{}.Reclen)
	binary.NativeEndian.PutUint16(recordData[reclenOffset:], 1)

	repo.hooks.openMaintenanceDirectory = func(string) (maintenanceDirectory, error) {
		return fakeMaintenanceDirectory{data: recordData}, nil
	}
	_, _, err := repo.readMaintenanceDirent("unused", 1, &maintenanceCursor{})
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("readMaintenanceDirent error = %v, want EIO", err)
	}
}

func TestDecodeMaintenanceDirentBoundsRecords(t *testing.T) {
	nameOffset := int(unsafe.Offsetof(syscall.Dirent{}.Name))
	reclenOffset := unsafe.Offsetof(syscall.Dirent{}.Reclen)
	valid := make([]byte, nameOffset+len("entry")+1)
	binary.NativeEndian.PutUint16(valid[reclenOffset:], uint16(len(valid)))
	copy(valid[nameOffset:], "entry")

	tests := []struct {
		name     string
		data     []byte
		wantName string
		wantErr  bool
	}{
		{name: "truncated header", data: make([]byte, nameOffset-1), wantErr: true},
		{name: "short record", data: append([]byte(nil), valid...), wantErr: true},
		{name: "truncated record", data: valid[:nameOffset], wantErr: true},
		{name: "record exceeds aligned storage", data: make([]byte, int(unsafe.Sizeof(syscall.Dirent{}))+1), wantErr: true},
		{name: "valid", data: valid, wantName: "entry"},
	}
	binary.NativeEndian.PutUint16(tests[1].data[reclenOffset:], uint16(nameOffset-1))
	binary.NativeEndian.PutUint16(tests[2].data[reclenOffset:], uint16(len(valid)))
	binary.NativeEndian.PutUint16(tests[3].data[reclenOffset:], uint16(len(tests[3].data)))

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			record, name, reclen, err := decodeMaintenanceDirent(tc.data)
			if tc.wantErr {
				if !errors.Is(err, syscall.EIO) {
					t.Fatalf("decodeMaintenanceDirent() error = %v, want EIO", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if record == nil {
				t.Fatal("decodeMaintenanceDirent() record = nil")
			}
			if reclen != len(valid) {
				t.Fatalf("decodeMaintenanceDirent() reclen = %d, want %d", reclen, len(valid))
			}
			if got := string(name[:len(tc.wantName)]); got != tc.wantName {
				t.Fatalf("decodeMaintenanceDirent() name = %q, want %q", got, tc.wantName)
			}
		})
	}
}

type fakeMaintenanceDirectory struct {
	data       []byte
	seekOffset int64
	seekErr    error
	readErr    error
	closeErr   error
}

func (d fakeMaintenanceDirectory) Seek(int64, int) (int64, error) { return d.seekOffset, d.seekErr }
func (d fakeMaintenanceDirectory) ReadDirent(buffer []byte) (int, error) {
	if d.readErr != nil {
		return 0, d.readErr
	}
	return copy(buffer, d.data), nil
}
func (d fakeMaintenanceDirectory) Close() error { return d.closeErr }

func TestReadMaintenanceDirentDrainsDarwinBatchesWithoutStarvation(t *testing.T) {
	dir := privateDir(t)
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	batches := [][]string{
		{"one", "two"},
		{"three", "four", "five", "six"},
	}
	for _, batch := range batches {
		for _, name := range batch {
			if err := os.WriteFile(filepath.Join(dir, name), nil, 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}

	file := &fakeMultiBatchMaintenanceDirectory{batches: [][]byte{
		maintenanceDirentBatch(batches[0]...),
		maintenanceDirentBatch(batches[1]...),
	}}
	repo := NewRepository(dir)
	repo.hooks.openMaintenanceDirectory = func(string) (maintenanceDirectory, error) { return file, nil }
	cursor := &maintenanceCursor{}

	entries, done, err := repo.readMaintenanceDirentWithDrain(dir, 4, cursor, true, maintenanceTestDirectoryCookie)
	if err != nil {
		t.Fatal(err)
	}
	if done {
		t.Fatal("readMaintenanceDirentWithDrain() done = true, want false before EOF")
	}
	if got, want := maintenanceEntryNames(entries), []string{"one", "two", "three", "four"}; !slices.Equal(got, want) {
		t.Fatalf("first entries = %v, want %v", got, want)
	}
	if got, want := maintenanceEntryNames(cursor.pending), []string{"five", "six"}; !slices.Equal(got, want) {
		t.Fatalf("pending entries = %v, want exactly final-buffer excess %v", got, want)
	}
	if got := file.reads; got != 2 {
		t.Fatalf("ReadDirent calls = %d, want 2 to fill the requested batch", got)
	}

	id := maintenanceCursorID(dir, "test")
	repo.maintenanceCursors = map[string]*maintenanceCursor{id: cursor}
	entries, done, err = repo.readMaintenanceDir(dir, 4, "test")
	if err != nil {
		t.Fatal(err)
	}
	if done {
		t.Fatal("pending batch done = true, want false before EOF")
	}
	if got, want := maintenanceEntryNames(entries), []string{"five", "six"}; !slices.Equal(got, want) {
		t.Fatalf("pending entries = %v, want %v", got, want)
	}
	if got := file.reads; got != 2 {
		t.Fatalf("ReadDirent calls while delivering pending entries = %d, want 2", got)
	}

	entries, done, err = repo.readMaintenanceDir(dir, 4, "test")
	if err != nil {
		t.Fatal(err)
	}
	if !done {
		t.Fatal("EOF batch done = false, want true")
	}
	if len(entries) != 0 {
		t.Fatalf("EOF entries = %v, want none", entries)
	}
	if _, ok := repo.maintenanceCursors[id]; ok {
		t.Fatal("EOF cursor retained after pending entries were delivered")
	}
}

func maintenanceEntryNames(entries []maintenanceDirEntry) []string {
	names := make([]string, len(entries))
	for i, entry := range entries {
		names[i] = entry.name
	}
	return names
}

func maintenanceDirentBatch(names ...string) []byte {
	nameOffset := int(unsafe.Offsetof(syscall.Dirent{}.Name))
	reclenOffset := unsafe.Offsetof(syscall.Dirent{}.Reclen)
	var data []byte
	for _, name := range names {
		record := make([]byte, nameOffset+len(name)+1)
		binary.NativeEndian.PutUint16(record[reclenOffset:], uint16(len(record)))
		copy(record[nameOffset:], name)
		data = append(data, record...)
	}
	return data
}

type fakeMultiBatchMaintenanceDirectory struct {
	batches [][]byte
	index   int
	reads   int
}

func (d *fakeMultiBatchMaintenanceDirectory) Seek(offset int64, whence int) (int64, error) {
	if whence == io.SeekCurrent {
		return int64(d.index), nil
	}
	d.index = int(offset)
	return offset, nil
}

func (d *fakeMultiBatchMaintenanceDirectory) ReadDirent(buffer []byte) (int, error) {
	d.reads++
	if d.index == len(d.batches) {
		return 0, nil
	}
	batch := d.batches[d.index]
	d.index++
	return copy(buffer, batch), nil
}

func (d *fakeMultiBatchMaintenanceDirectory) Close() error { return nil }

func maintenanceTestDirectoryCookie(file maintenanceDirectory, _ *syscall.Dirent, _ int) (int64, error) {
	return file.Seek(0, io.SeekCurrent)
}

func TestRepositoryDeleteQuarantinesCanonicalSessionBeforeCleanup(t *testing.T) {
	repo := NewRepository(privateDir(t))
	if err := repo.Publish(context.Background(), repositoryPublication(t, "named", 1, []byte("state"))); err != nil {
		t.Fatal(err)
	}
	if err := repo.Delete(context.Background(), "named"); err != nil {
		t.Fatal(err)
	}

	canonical := repo.legacySessionPath(legacyIncarnationID("named").String())
	if _, err := os.Lstat(canonical); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canonical session after Delete = %v, want not exist", err)
	}
	entries, err := os.ReadDir(filepath.Join(repo.dir, repositorySessionsDir))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || !isQuarantine(entries[0].Name()) || !entries[0].IsDir() {
		t.Fatalf("session entries after Delete = %#v, want one quarantine", entries)
	}
	if err := repo.Maintain(context.Background()); err != nil {
		t.Fatal(err)
	}
	entries, err = os.ReadDir(filepath.Join(repo.dir, repositorySessionsDir))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("session entries after Maintain = %#v, want none", entries)
	}
}

func TestRepositoryDeleteReturnsRootSyncFailureAfterQuarantine(t *testing.T) {
	repo := NewRepository(privateDir(t))
	if err := repo.Publish(context.Background(), repositoryPublication(t, "named", 1, []byte("state"))); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("sessions sync")
	repo.hooks.syncDirectory = func(string) error { return injected }
	if err := repo.Delete(context.Background(), "named"); !errors.Is(err, injected) {
		t.Fatalf("Delete error = %v, want root sync failure", err)
	}
	if _, err := os.Lstat(repo.legacySessionPath(legacyIncarnationID("named").String())); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canonical session after failed sync = %v, want not exist", err)
	}
}

func TestRepositoryDeleteWaitsForPublication(t *testing.T) {
	repo := NewRepository(privateDir(t))
	if err := repo.Publish(context.Background(), repositoryPublication(t, "named", 1, []byte("first"))); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	repo.hooks.beforeHeadWrite = func(string) error {
		close(entered)
		<-release
		return nil
	}
	publishDone := make(chan error, 1)
	go func() {
		publishDone <- repo.Publish(context.Background(), repositoryPublicationAfter(t, repo, "named", 2, []byte("second")))
	}()
	<-entered
	deleteReached := make(chan struct{})
	deleteRelease := make(chan struct{})
	repo.hooks.beforeSessionLock = waitAtSessionLock(deleteReached, deleteRelease)
	deleteDone := make(chan error, 1)
	go func() { deleteDone <- repo.Delete(context.Background(), "named") }()
	<-deleteReached
	// Delete is parked at the session-lock boundary while Publish owns it.
	close(deleteRelease)
	close(release)
	if err := <-publishDone; err != nil {
		t.Fatal(err)
	}
	if err := <-deleteDone; err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(repo.legacySessionPath(legacyIncarnationID("named").String())); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canonical session after concurrent Delete = %v, want not exist", err)
	}
}

func TestRepositoryMaintainRetainsNewestTwoCompleteGenerations(t *testing.T) {
	repo := NewRepository(privateDir(t))
	for generation := uint64(1); generation <= 3; generation++ {
		if err := repo.Publish(context.Background(), repositoryPublicationAfter(t, repo, "named", generation, []byte{byte(generation)})); err != nil {
			t.Fatal(err)
		}
	}
	key := legacyIncarnationID("named").String()
	for pass := range maintenanceBatch {
		if err := repo.Maintain(context.Background()); err != nil {
			t.Fatal(err)
		}
		_, err := os.Lstat(repo.legacyManifestPath(key, 1))
		if errors.Is(err, os.ErrNotExist) {
			break
		}
		if err != nil {
			t.Fatalf("old manifest during Maintain = %v", err)
		}
		if pass == maintenanceBatch-1 {
			t.Fatal("old manifest was not eventually collected")
		}
	}
	for _, generation := range []uint64{2, 3} {
		if _, err := os.Lstat(repo.legacyManifestPath(key, generation)); err != nil {
			t.Fatalf("retained manifest %d: %v", generation, err)
		}
	}
	if _, err := os.Lstat(repo.legacyManifestPath(key, 1)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old manifest after Maintain = %v, want not exist", err)
	}
	for _, generation := range []uint64{2, 3} {
		if _, err := loadGenerationCheckpoint(context.Background(), repo, legacyIncarnationID("named"), "named", generation); err != nil {
			t.Fatalf("LoadCheckpoint after collecting generation %d: %v", generation, err)
		}
	}
}

func TestRepositoryMaintainPreservesIncompleteManifestReferences(t *testing.T) {
	repo := NewRepository(privateDir(t))
	for generation := uint64(1); generation <= 3; generation++ {
		if err := repo.Publish(context.Background(), repositoryPublicationAfter(t, repo, "named", generation, []byte{byte(generation)})); err != nil {
			t.Fatal(err)
		}
	}
	key := legacyIncarnationID("named").String()
	manifest := repositoryPublication(t, "named", 3, []byte{3})
	if err := os.Remove(repo.legacyObjectPath(key, manifest.Objects[1].Digest)); err != nil {
		t.Fatal(err)
	}
	if err := repo.Maintain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(repo.legacyManifestPath(key, 3)); err != nil {
		t.Fatalf("incomplete manifest removed: %v", err)
	}
	if _, err := os.Lstat(repo.legacyObjectPath(key, manifest.Objects[0].Digest)); err != nil {
		t.Fatalf("object referenced by incomplete manifest removed: %v", err)
	}
}

func TestRepositoryCancellationAfterSessionLockWaitPreventsMutations(t *testing.T) {
	t.Run("publish", func(t *testing.T) {
		repo := NewRepository(privateDir(t))
		key := legacyIncarnationID("named").String()
		lock := repo.lockSession(key)
		ctx, cancel := context.WithCancel(context.Background())
		reached := make(chan struct{})
		release := make(chan struct{})
		repo.hooks.beforeSessionLock = waitAtSessionLock(reached, release)
		done := make(chan error, 1)
		go func() { done <- repo.Publish(ctx, repositoryPublication(t, "named", 1, []byte("state"))) }()
		<-reached
		cancel()
		close(release)
		repo.unlockSession(lock)
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("Publish error = %v, want canceled", err)
		}
		if _, err := os.Lstat(repo.dir); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("Publish mutated canceled repository: %v", err)
		}
	})
	t.Run("load and delete", func(t *testing.T) {
		repo := NewRepository(privateDir(t))
		publication := repositoryPublication(t, "named", 1, []byte("state"))
		if err := repo.Publish(context.Background(), publication); err != nil {
			t.Fatal(err)
		}
		key := publication.IncarnationID.String()
		for _, operation := range []struct {
			name string
			run  func(context.Context) error
		}{
			{"load", func(ctx context.Context) error { _, err := loadPublication(ctx, repo, publication); return err }},
			{"delete", func(ctx context.Context) error { return repo.Delete(ctx, "named") }},
		} {
			t.Run(operation.name, func(t *testing.T) {
				lock := repo.lockSession(key)
				ctx, cancel := context.WithCancel(context.Background())
				reached := make(chan struct{})
				release := make(chan struct{})
				repo.hooks.beforeSessionLock = waitAtSessionLock(reached, release)
				done := make(chan error, 1)
				go func() { done <- operation.run(ctx) }()
				<-reached
				cancel()
				close(release)
				repo.unlockSession(lock)
				if err := <-done; !errors.Is(err, context.Canceled) {
					t.Fatalf("%s error = %v, want canceled", operation.name, err)
				}
				repo.hooks.beforeSessionLock = nil
				if _, err := loadPublication(context.Background(), repo, publication); err != nil {
					t.Fatalf("%s mutated session: %v", operation.name, err)
				}
			})
		}
	})
	t.Run("maintain", func(t *testing.T) {
		repo := NewRepository(privateDir(t))
		publication := repositoryPublication(t, "named", 1, []byte("state"))
		if err := repo.Publish(context.Background(), publication); err != nil {
			t.Fatal(err)
		}
		lock := repo.lockSession(publication.IncarnationID.String())
		ctx, cancel := context.WithCancel(context.Background())
		reached := make(chan struct{})
		release := make(chan struct{})
		repo.hooks.beforeSessionLock = waitAtSessionLock(reached, release)
		done := make(chan error, 1)
		go func() { done <- repo.Maintain(ctx) }()
		<-reached
		cancel()
		close(release)
		repo.unlockSession(lock)
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("Maintain error = %v, want canceled", err)
		}
		repo.hooks.beforeSessionLock = nil
		if _, err := loadPublication(context.Background(), repo, publication); err != nil {
			t.Fatalf("Maintain mutated session: %v", err)
		}
	})
}

func waitAtSessionLock(reached chan<- struct{}, release <-chan struct{}) func(string) {
	return func(string) {
		close(reached)
		<-release
	}
}

func TestRepositoryDeleteRetriesPendingQuarantineSyncWithoutDeletingRecreatedSession(t *testing.T) {
	repo := NewRepository(privateDir(t))
	if err := repo.Publish(context.Background(), repositoryPublication(t, "named", 1, []byte("old"))); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("sessions sync")
	repo.hooks.syncDirectory = func(string) error { return injected }
	if err := repo.Delete(context.Background(), "named"); !errors.Is(err, injected) {
		t.Fatalf("Delete error = %v, want sync failure", err)
	}
	repo.hooks.syncDirectory = nil
	recreated := repositoryPublication(t, "named", 1, []byte("new"))
	if err := repo.Publish(context.Background(), recreated); err != nil {
		t.Fatal(err)
	}
	if err := repo.Delete(context.Background(), "named"); err != nil {
		t.Fatalf("retry Delete: %v", err)
	}
	if _, err := loadPublication(context.Background(), repo, recreated); err != nil {
		t.Fatalf("recreated session was deleted: %v", err)
	}
}

func TestRepositoryMaintainResumesNestedQuarantineWithinBatch(t *testing.T) {
	repo := NewRepository(privateDir(t))
	quarantine := filepath.Join(repo.dir, repositorySessionsDir, ".deleting-named-test")
	for i := range maintenanceBatch + 1 {
		path := filepath.Join(quarantine, "nested", string(rune('a'+i)))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("stale"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := repo.Maintain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(quarantine); errors.Is(err, os.ErrNotExist) {
		t.Fatalf("quarantine after one bounded call = %v, want remaining tree", err)
	}
	for range 12 {
		if err := repo.Maintain(context.Background()); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Lstat(quarantine); errors.Is(err, os.ErrNotExist) {
			return
		}
	}
	if _, err := os.Lstat(quarantine); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("quarantine after resumed maintenance = %v, want removed", err)
	}
}

func TestRepositoryMaintainClosesContinuationDirectories(t *testing.T) {
	repo := NewRepository(privateDir(t))
	sessions := filepath.Join(repo.dir, repositorySessionsDir)
	for i := range maintenanceBatch + 1 {
		generations := filepath.Join(sessions, fmt.Sprintf("named-%03d", i), repositoryGenerations)
		if err := os.MkdirAll(generations, 0o700); err != nil {
			t.Fatal(err)
		}
		for j := range maintenanceBatch + 1 {
			if err := os.WriteFile(filepath.Join(generations, fmt.Sprintf("unclassified-%03d", j)), []byte("state"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	before := openDescriptorCount(t)
	if err := repo.Maintain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := openDescriptorCount(t); got > before+4 {
		t.Fatalf("open descriptors after bounded maintenance = %d, want at most %d", got, before+4)
	}
}

func TestRepositoryMaintainBoundsQueuedShardNames(t *testing.T) {
	repo := NewRepository(privateDir(t))
	key := legacyIncarnationID("named").String()
	root := filepath.Join(repo.legacySessionPath(key), repositoryObjectsDir)
	for i := range maintenanceBatch * 2 {
		if err := os.MkdirAll(filepath.Join(root, fmt.Sprintf("%02x", i)), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	repo.maintenanceCursors = make(map[string]*maintenanceCursor)
	state := &sessionMaintenance{marked: make(map[uint64]manifestMaintenance), markDone: true}
	for range maintenanceBatch + 2 {
		budget := maintenanceBatch
		if err := repo.sweepSession(context.Background(), key, state, &budget); err != nil {
			t.Fatal(err)
		}
		if state.sweepShard != "" && len(repo.maintenanceCursors) > maxMaintenanceCursors {
			t.Fatalf("retained maintenance cursors = %d, want at most %d", len(repo.maintenanceCursors), maxMaintenanceCursors)
		}
	}
}

func TestRepositoryMaintainResumesCursorAfterDirectoryMutation(t *testing.T) {
	repo := NewRepository(privateDir(t))
	if err := os.Mkdir(repo.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	for i := range maintenanceBatch + 1 {
		if err := os.WriteFile(filepath.Join(repo.dir, fmt.Sprintf("live-%03d", i)), []byte("live"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := repo.Maintain(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Mutating entries ahead of the saved cookie must not reset traversal to
	// the prefix or lose a stale entry appended after that cookie.
	if err := os.Remove(filepath.Join(repo.dir, "live-000")); err != nil {
		t.Fatal(err)
	}
	stale := filepath.Join(repo.dir, ".tmp-appended")
	if err := os.WriteFile(stale, []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	for range 4 {
		if err := repo.Maintain(context.Background()); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Lstat(stale); errors.Is(err, os.ErrNotExist) {
			return
		}
	}
	t.Fatal("stale entry appended after cursor mutation was not eventually removed")
}

func TestRepositoryMaintainDoesNotStarveLaterTemporaryEntries(t *testing.T) {
	repo := NewRepository(privateDir(t))
	if err := os.Mkdir(repo.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	// The live entries precede the stale entries in directory insertion order.
	// A fresh ReadDir from the start on every call would never reach the latter.
	for i := range maintenanceBatch + 1 {
		if err := os.WriteFile(filepath.Join(repo.dir, "live-"+string(rune('a'+i))), []byte("live"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for i := range maintenanceBatch + 1 {
		if err := os.WriteFile(filepath.Join(repo.dir, ".tmp-later-"+string(rune('a'+i))), []byte("stale"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for range 4 {
		if err := repo.Maintain(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := os.ReadDir(repo.dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".tmp-") {
			t.Fatalf("stale entry %q was starved", entry.Name())
		}
	}
}

func TestRepositoryMaintainMarksLargeGenerationSetBeforeSweeping(t *testing.T) {
	repo := NewRepository(privateDir(t))
	var newest ports.SnapshotPublication
	for generation := uint64(1); generation <= maintenanceBatch+2; generation++ {
		publication := repositoryPublicationAfter(t, repo, "named", generation, fmt.Appendf(nil, "state-%d", generation))
		if err := repo.Publish(context.Background(), publication); err != nil {
			t.Fatal(err)
		}
		newest = publication
	}
	key := legacyIncarnationID("named").String()
	stale := sha256.Sum256([]byte("stale object"))
	stalePath := repo.legacyObjectPath(key, stale)
	if err := os.MkdirAll(filepath.Dir(stalePath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stalePath, []byte("stale object"), 0o600); err != nil {
		t.Fatal(err)
	}

	// The first call has classified only one manifest batch. It must not sweep
	// even an otherwise stale object, nor a blob owned by a later manifest.
	if err := repo.Maintain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(stalePath); err != nil {
		t.Fatalf("stale object swept before complete mark pass: %v", err)
	}
	for _, object := range newest.Objects {
		if _, err := os.Lstat(repo.legacyObjectPath(key, object.Digest)); err != nil {
			t.Fatalf("object referenced by not-yet-classified manifest removed: %v", err)
		}
	}

	for i := range 600 {
		if err := repo.Maintain(context.Background()); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Lstat(stalePath); errors.Is(err, os.ErrNotExist) {
			break
		}
		if i == 599 {
			t.Fatal("stale object was not eventually swept")
		}
	}
	for _, generation := range []uint64{maintenanceBatch + 1, maintenanceBatch + 2} {
		if _, err := os.Lstat(repo.legacyManifestPath(key, generation)); err != nil {
			t.Fatalf("newest complete manifest %d removed: %v", generation, err)
		}
	}
}

func TestRepositoryMaintainUsesFixedRemovalBatch(t *testing.T) {
	repo := NewRepository(privateDir(t))
	if err := os.Mkdir(repo.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	for i := range maintenanceBatch + 1 {
		if err := os.WriteFile(filepath.Join(repo.dir, ".tmp-"+string(rune('a'+i))), []byte("stale"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := repo.Maintain(context.Background()); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(repo.dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("remaining stale entries = %d, want 1 after one batch", len(entries))
	}
}

func TestRepositoryMaintainQueuesFetchedSessionsPastWorkBudget(t *testing.T) {
	repo := NewRepository(privateDir(t))
	if err := os.MkdirAll(filepath.Join(repo.dir, repositorySessionsDir), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo.dir, ".tmp-consume-budget"), []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}

	removed := make(map[string]int)
	for i := range maintenanceBatch + 1 {
		path := filepath.Join(repo.dir, repositorySessionsDir, fmt.Sprintf(".deleting-%03d", i))
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	repo.hooks.remove = func(path string) error {
		if strings.HasPrefix(filepath.Base(path), ".deleting-") {
			removed[path]++
		}
		return nil
	}

	sessions := filepath.Join(repo.dir, repositorySessionsDir)
	f, err := os.Open(sessions)
	if err != nil {
		t.Fatal(err)
	}
	fetched, err := f.ReadDir(maintenanceBatch)
	if err != nil {
		t.Fatal(err)
	}
	unread, err := f.ReadDir(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	queued := fetched[len(fetched)-1].Name()
	for i := range maintenanceBatch + 4 {
		if err := repo.Maintain(context.Background()); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Lstat(filepath.Join(sessions, queued)); errors.Is(err, os.ErrNotExist) {
			break
		} else if i == maintenanceBatch+3 {
			t.Fatalf("fetched but unprocessed session %q = %v, want eventually removed", queued, err)
		}
	}
	if _, err := os.Lstat(filepath.Join(sessions, unread[0].Name())); err != nil {
		t.Fatalf("unread session %q = %v, want queued entry to run first", unread[0].Name(), err)
	}

	for range maintenanceBatch + 4 {
		if err := repo.Maintain(context.Background()); err != nil {
			t.Fatal(err)
		}
		entries, err := os.ReadDir(sessions)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) == 0 {
			break
		}
	}
	entries, err := os.ReadDir(sessions)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("queued sessions after resumed maintenance = %d, want none", len(entries))
	}
	if len(removed) != maintenanceBatch+1 {
		t.Fatalf("removed sessions = %d, want %d", len(removed), maintenanceBatch+1)
	}
	for path, calls := range removed {
		if calls != 1 {
			t.Fatalf("remove calls for %q = %d, want one", path, calls)
		}
	}
}

func TestRepositoryMaintainClassifiesUnpublishedGenerationsBeforeSweep(t *testing.T) {
	for _, head := range []struct {
		name string
		set  func(t *testing.T, repo *Repository, key string)
	}{
		{
			name: "missing",
			set: func(t *testing.T, repo *Repository, key string) {
				t.Helper()
				if err := os.Remove(repo.legacyHeadPath(key)); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "corrupt",
			set: func(t *testing.T, repo *Repository, key string) {
				t.Helper()
				if err := os.WriteFile(repo.legacyHeadPath(key), []byte("corrupt"), 0o600); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(head.name, func(t *testing.T) {
			repo := NewRepository(privateDir(t))
			key := legacyIncarnationID("named").String()
			publications := make([]ports.SnapshotPublication, 0, maintenanceBatch+2)
			for generation := uint64(1); generation <= maintenanceBatch+2; generation++ {
				publication := repositoryPublicationAfter(t, repo, "named", generation, fmt.Appendf(nil, "state-%d", generation))
				if err := repo.Publish(context.Background(), publication); err != nil {
					t.Fatal(err)
				}
				publications = append(publications, publication)
			}
			head.set(t, repo, key)
			stale := sha256.Sum256([]byte("unpublished stale object"))
			stalePath := repo.legacyObjectPath(key, stale)
			if err := os.MkdirAll(filepath.Dir(stalePath), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(stalePath, []byte("unpublished stale object"), 0o600); err != nil {
				t.Fatal(err)
			}

			if err := repo.Maintain(context.Background()); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Lstat(stalePath); err != nil {
				t.Fatalf("stale object swept before full manifest classification: %v", err)
			}
			for _, publication := range publications {
				if _, err := os.Lstat(repo.legacyObjectPath(key, publication.Objects[0].Digest)); err != nil {
					t.Fatalf("object for unpublished generation %d removed early: %v", publication.Generation, err)
				}
			}

			for i := range 600 {
				if err := repo.Maintain(context.Background()); err != nil {
					t.Fatal(err)
				}
				if _, err := os.Lstat(stalePath); errors.Is(err, os.ErrNotExist) {
					break
				} else if i == 599 {
					t.Fatal("stale object was not swept after complete classification")
				}
			}
			for _, publication := range publications {
				if _, err := os.Lstat(repo.legacyObjectPath(key, publication.Objects[0].Digest)); err != nil {
					t.Fatalf("object for unpublished generation %d removed: %v", publication.Generation, err)
				}
			}
		})
	}
}

func TestRepositoryMaintainRestartsMarkAfterFailedPublication(t *testing.T) {
	repo := NewRepository(privateDir(t))
	key := legacyIncarnationID("named").String()
	first := publicationWithTailShard(t, "named", 1, "ff")
	stalePaths := make([]string, 0, maintenanceBatch+1)
	for i := range maintenanceBatch + 1 {
		digest := digestInShard(t, "00", i)
		path := repo.legacyObjectPath(key, digest)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("stale"), 0o600); err != nil {
			t.Fatal(err)
		}
		stalePaths = append(stalePaths, path)
	}
	if err := repo.Publish(context.Background(), first); err != nil {
		t.Fatal(err)
	}

	// This completes marking but exhausts the sweep budget in the stale shard,
	// leaving the next sweep batch pending.
	if err := repo.Maintain(context.Background()); err != nil {
		t.Fatal(err)
	}
	staleRemaining := false
	for _, path := range stalePaths {
		if _, err := os.Lstat(path); err == nil {
			staleRemaining = true
			break
		}
	}
	if !staleRemaining {
		t.Fatal("sweep did not leave a pending stale object")
	}

	second := publicationWithCurrentParent(t, repo, publicationWithTailShard(t, "named", 2, "ff"))
	injected := errors.New("before HEAD")
	repo.hooks.beforeHeadWrite = func(string) error { return injected }
	if err := repo.Publish(context.Background(), second); !errors.Is(err, injected) {
		t.Fatalf("Publish error = %v, want injected failure", err)
	}
	repo.hooks.beforeHeadWrite = nil

	for i := range 20 {
		if err := repo.Maintain(context.Background()); err != nil {
			t.Fatal(err)
		}
		for _, object := range second.Objects {
			if _, err := os.Lstat(repo.legacyObjectPath(key, object.Digest)); err != nil {
				t.Fatalf("object referenced by failed publication removed on pass %d: %v", i, err)
			}
		}
		staleRemaining = false
		for _, path := range stalePaths {
			if _, err := os.Lstat(path); err == nil {
				staleRemaining = true
				break
			}
		}
	}
	if staleRemaining {
		t.Fatal("stale object was not eventually swept")
	}
}

func publicationWithTailShard(t *testing.T, name string, generation uint64, shard string) ports.SnapshotPublication {
	t.Helper()
	for i := range 4096 {
		publication := repositoryPublication(t, name, generation, fmt.Appendf(nil, "state-%d-%d", generation, i))
		if fmt.Sprintf("%02x", publication.Objects[0].Digest[0]) == shard {
			return publication
		}
	}
	t.Fatalf("did not find publication tail in shard %q", shard)
	return ports.SnapshotPublication{}
}

func digestInShard(t *testing.T, shard string, n int) ports.SnapshotDigest {
	t.Helper()
	for i := range 4096 {
		digest := sha256.Sum256(fmt.Appendf(nil, "stale-%d-%d", n, i))
		if fmt.Sprintf("%02x", digest[0]) == shard {
			return digest
		}
	}
	t.Fatalf("did not find stale digest in shard %q", shard)
	return ports.SnapshotDigest{}
}

func TestRetentionSlots(t *testing.T) {
	for _, tc := range []struct {
		name string
		refs []uint64
		want []uint64
	}{
		{name: "first checkpoint", refs: []uint64{1}, want: []uint64{1}},
		{name: "second checkpoint", refs: []uint64{2, 1}, want: []uint64{2, 1}},
		{name: "committed and two predecessors", refs: []uint64{4, 3, 2}, want: []uint64{4, 3, 2}},
		{name: "fallback hole", refs: []uint64{4, 2}, want: []uint64{4, 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := NewRepository(privateDir(t))
			publications := publishMaintenanceGenerations(t, repo, "work", 4)
			plan := ports.RetentionPlan{IncarnationID: publications[0].IncarnationID}
			for _, generation := range tc.refs {
				plan.Keep = append(plan.Keep, checkpointRefForPublication(publications[generation-1]))
			}
			done, err := repo.MaintainSession(context.Background(), plan, ports.MaintenanceBudget{Entries: 64, Bytes: 8 << 20})
			if err != nil {
				t.Fatal(err)
			}
			if !done {
				t.Fatal("MaintainSession() done = false with sufficient budget")
			}
			if got := remainingMaintenanceGenerations(t, repo, plan.IncarnationID); !slices.Equal(got, tc.want) {
				t.Fatalf("remaining generations = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestUnresolvedDataPinned(t *testing.T) {
	repo := NewRepository(privateDir(t))
	publications := publishMaintenanceGenerations(t, repo, "unresolved", 4)
	done, err := repo.MaintainSession(context.Background(), ports.RetentionPlan{IncarnationID: publications[0].IncarnationID, PinAll: true}, ports.MaintenanceBudget{})
	if err != nil {
		t.Fatal(err)
	}
	if !done {
		t.Fatal("pinned maintenance did not complete without traversing")
	}
	if got := remainingMaintenanceGenerations(t, repo, publications[0].IncarnationID); !slices.Equal(got, []uint64{4, 3, 2, 1}) {
		t.Fatalf("pinned generations = %v", got)
	}
}

func TestPinAllDoesNotWaitForSameSessionRetention(t *testing.T) {
	repo := NewRepository(privateDir(t))
	publications := publishMaintenanceGenerations(t, repo, "pin-active", 2)
	publication := publications[0]
	plan := ports.RetentionPlan{IncarnationID: publication.IncarnationID, Keep: []ports.CheckpointRef{checkpointRefForPublication(publication)}}

	entered := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce sync.Once
	repo.hooks.beforeMaintenancePayloadRead = func(string) {
		enteredOnce.Do(func() { close(entered) })
		<-release
	}
	activeDone := make(chan error, 1)
	go func() {
		_, err := repo.MaintainSession(context.Background(), plan, ports.MaintenanceBudget{Entries: 64, Bytes: 8 << 20})
		activeDone <- err
	}()
	<-entered

	pinDone := make(chan error, 1)
	go func() {
		_, err := repo.MaintainSession(context.Background(), ports.RetentionPlan{IncarnationID: publication.IncarnationID, PinAll: true}, ports.MaintenanceBudget{})
		pinDone <- err
	}()
	select {
	case err := <-pinDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(remainingTestTime(t) / 2):
		close(release)
		t.Fatal("PinAll waited for active retention on the same incarnation")
	}
	close(release)
	if err := <-activeDone; err != nil {
		t.Fatal(err)
	}
	if got := remainingMaintenanceGenerations(t, repo, publication.IncarnationID); !slices.Equal(got, []uint64{2, 1}) {
		t.Fatalf("PinAll allowed active retention to delete generations: %v", got)
	}
}

func TestMaintainSessionRejectsZeroBudget(t *testing.T) {
	repo := NewRepository(privateDir(t))
	id := domain.IncarnationID{1}
	done, err := repo.MaintainSession(context.Background(), ports.RetentionPlan{IncarnationID: id}, ports.MaintenanceBudget{})
	if done || !errors.Is(err, ErrMaintenanceBudgetTooSmall) {
		t.Fatalf("zero budget: done=%v err=%v", done, err)
	}
}

func TestProcessRetentionEntriesSyncsRemovalBatchOnce(t *testing.T) {
	repo := NewRepository(privateDir(t))
	dir := filepath.Join(repo.dir, "batch")
	if err := safedir.EnsurePrivate(dir); err != nil {
		t.Fatal(err)
	}
	entries := []maintenanceDirEntry{{name: generationFilename(1), cookie: 1}, {name: generationFilename(2), cookie: 2}}
	for _, entry := range entries {
		if err := os.WriteFile(filepath.Join(dir, entry.name), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	syncs := 0
	repo.hooks.syncDirectory = func(got string) error {
		if got == dir {
			syncs++
		}
		return nil
	}
	state := &retentionMaintenance{cursors: make(map[string]*maintenanceCursor)}
	budget := ports.MaintenanceBudget{Entries: 2, Bytes: 2}
	admitted, err := repo.processRetentionEntries(context.Background(), state, dir, "batch", entries, &budget, false, retentionEntryPolicy{kind: retentionManifest})
	if err != nil || !admitted {
		t.Fatalf("batch processing: admitted=%v err=%v", admitted, err)
	}
	if syncs != 1 {
		t.Fatalf("directory syncs = %d, want 1", syncs)
	}
}

func TestRequeueRetentionEntriesDoesNotAliasInput(t *testing.T) {
	repo := NewRepository(privateDir(t))
	state := &retentionMaintenance{cursors: make(map[string]*maintenanceCursor)}
	entries := []maintenanceDirEntry{{name: "one", cookie: 1}}
	repo.requeueRetentionEntries(state, "/dir", "purpose", entries)
	entries[0].name = "mutated"
	pending := state.cursors[maintenanceCursorID("/dir", "purpose")].pending
	if pending[0].name != "one" {
		t.Fatalf("queued entry aliased caller input: %q", pending[0].name)
	}
}

func TestMaintenanceBudgetAdmitsPayloadsBeforeRead(t *testing.T) {
	repo := NewRepository(privateDir(t))
	publications := publishMaintenanceGenerations(t, repo, "budget", 1)
	plan := ports.RetentionPlan{IncarnationID: publications[0].IncarnationID, Keep: []ports.CheckpointRef{checkpointRefForPublication(publications[0])}}

	var reads []string
	repo.hooks.beforeMaintenancePayloadRead = func(path string) { reads = append(reads, path) }
	done, err := repo.MaintainSession(context.Background(), plan, ports.MaintenanceBudget{Entries: 1, Bytes: 1})
	if !errors.Is(err, ErrMaintenanceBudgetTooSmall) {
		t.Fatalf("undersized manifest budget error = %v, want ErrMaintenanceBudgetTooSmall", err)
	}
	if done || len(reads) != 0 {
		t.Fatalf("undersized manifest budget: done=%v payload reads=%v", done, reads)
	}

	manifestPath := repo.manifestPath(plan.IncarnationID, 1)
	info, err := repo.stat(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	reads = nil
	done, err = repo.MaintainSession(context.Background(), plan, ports.MaintenanceBudget{Entries: 1, Bytes: uint64(info.Size())})
	if !errors.Is(err, ErrMaintenanceBudgetTooSmall) {
		t.Fatalf("manifest-only budget error = %v, want ErrMaintenanceBudgetTooSmall", err)
	}
	if done {
		t.Fatal("manifest-only budget unexpectedly admitted the generation objects")
	}
	if len(reads) != 1 || reads[0] != manifestPath {
		t.Fatalf("payload reads = %v, want only admitted manifest %q", reads, manifestPath)
	}
}

func TestMaintainSessionDistinguishesInitialOversizeFromResumableExhaustion(t *testing.T) {
	repo := NewRepository(privateDir(t))
	publications := publishMaintenanceGenerations(t, repo, "budget-semantics", 2)
	plan := ports.RetentionPlan{IncarnationID: publications[0].IncarnationID}
	firstInfo, err := os.Stat(repo.manifestPath(plan.IncarnationID, 1))
	if err != nil {
		t.Fatal(err)
	}
	secondInfo, err := os.Stat(repo.manifestPath(plan.IncarnationID, 2))
	if err != nil {
		t.Fatal(err)
	}
	firstSize, secondSize := uint64(firstInfo.Size()), uint64(secondInfo.Size())

	done, err := repo.MaintainSession(context.Background(), plan, ports.MaintenanceBudget{Entries: 2, Bytes: min(firstSize, secondSize) - 1})
	if done || !errors.Is(err, ErrMaintenanceBudgetTooSmall) {
		t.Fatalf("initial oversized item: done=%v err=%v", done, err)
	}

	done, err = repo.MaintainSession(context.Background(), plan, ports.MaintenanceBudget{Entries: 2, Bytes: firstSize + secondSize - 1})
	if err != nil || done {
		t.Fatalf("mid-pass exhaustion must be resumable: done=%v err=%v", done, err)
	}
	done, err = repo.MaintainSession(context.Background(), plan, ports.MaintenanceBudget{Entries: 64, Bytes: 8 << 20})
	if err != nil || !done {
		t.Fatalf("resumed maintenance: done=%v err=%v", done, err)
	}
}

func TestMaintainSessionYieldsOnEmptyNonDoneObjectShardBatch(t *testing.T) {
	repo := NewRepository(privateDir(t))
	publication := publishMaintenanceGenerations(t, repo, "empty-shard-batch", 1)[0]
	plan := ports.RetentionPlan{IncarnationID: publication.IncarnationID}
	key := publication.IncarnationID.String()
	token := retentionToken(plan)
	root := filepath.Join(repo.sessionPath(plan.IncarnationID), repositoryObjectsDir)
	purpose := "retention-shards:" + key + ":" + token
	state := &retentionMaintenance{
		token:         token,
		cursors:       map[string]*maintenanceCursor{maintenanceCursorID(root, purpose): {pending: []maintenanceDirEntry{}}},
		references:    make(map[ports.SnapshotDigest]struct{}),
		validated:     true,
		manifestsDone: true,
	}
	repo.retentionSessions[key] = state

	done, err := repo.MaintainSession(context.Background(), plan, ports.MaintenanceBudget{Entries: 1, Bytes: 1024})
	if err != nil || done {
		t.Fatalf("empty non-done shard batch: done=%v err=%v", done, err)
	}
	if state.objectRootDone {
		t.Fatal("empty non-done shard batch advanced object-root completion")
	}
}

func TestMaintainSessionDoesNotHoldMaintenanceLockDuringPayloadIO(t *testing.T) {
	repo := NewRepository(privateDir(t))
	active := publishMaintenanceGenerations(t, repo, "active-lock", 1)
	other := publishMaintenanceGenerations(t, repo, "other-lock", 1)
	plan := ports.RetentionPlan{IncarnationID: active[0].IncarnationID, Keep: []ports.CheckpointRef{checkpointRefForPublication(active[0])}}

	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	repo.hooks.beforeMaintenancePayloadRead = func(string) {
		once.Do(func() { close(entered) })
		<-release
	}
	activeDone := make(chan error, 1)
	go func() {
		_, err := repo.MaintainSession(context.Background(), plan, ports.MaintenanceBudget{Entries: 64, Bytes: 8 << 20})
		activeDone <- err
	}()
	<-entered

	pinDone := make(chan error, 1)
	go func() {
		_, err := repo.MaintainSession(context.Background(), ports.RetentionPlan{IncarnationID: other[0].IncarnationID, PinAll: true}, ports.MaintenanceBudget{Entries: 1, Bytes: 1})
		pinDone <- err
	}()
	select {
	case err := <-pinDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(remainingTestTime(t) / 2):
		close(release)
		t.Fatal("unrelated PinAll was blocked by retention payload I/O")
	}
	close(release)
	if err := <-activeDone; err != nil {
		t.Fatal(err)
	}
}

func remainingTestTime(t *testing.T) time.Duration {
	t.Helper()
	if deadline, ok := t.Deadline(); ok {
		return time.Until(deadline)
	}
	return 30 * time.Second
}

func TestPerSessionBudgetIsolation(t *testing.T) {
	repo := NewRepository(privateDir(t))
	large := publishMaintenanceGenerations(t, repo, "large", 4)
	small := publishMaintenanceGenerations(t, repo, "small", 2)
	largePlan := ports.RetentionPlan{IncarnationID: large[0].IncarnationID, Keep: []ports.CheckpointRef{checkpointRefForPublication(large[3])}}
	smallPlan := ports.RetentionPlan{IncarnationID: small[0].IncarnationID, Keep: []ports.CheckpointRef{checkpointRefForPublication(small[1])}}

	done, err := repo.MaintainSession(context.Background(), largePlan, ports.MaintenanceBudget{Entries: 1, Bytes: 1})
	if !errors.Is(err, ErrMaintenanceBudgetTooSmall) {
		t.Fatalf("large session error = %v, want ErrMaintenanceBudgetTooSmall", err)
	}
	if done {
		t.Fatal("large session unexpectedly exhausted no budget")
	}
	done, err = repo.MaintainSession(context.Background(), smallPlan, ports.MaintenanceBudget{Entries: 64, Bytes: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if !done {
		t.Fatal("small session did not progress after unrelated exhaustion")
	}
	if got := remainingMaintenanceGenerations(t, repo, small[0].IncarnationID); !slices.Equal(got, []uint64{2}) {
		t.Fatalf("small generations = %v, want [2]", got)
	}
}

func publishMaintenanceGenerations(t *testing.T, repo *Repository, name string, count uint64) []ports.SnapshotPublication {
	t.Helper()
	publications := make([]ports.SnapshotPublication, 0, count)
	for generation := uint64(1); generation <= count; generation++ {
		publication := repositoryPublicationAfter(t, repo, name, generation, []byte{byte(generation)})
		if err := repo.Publish(context.Background(), publication); err != nil {
			t.Fatal(err)
		}
		publications = append(publications, publication)
	}
	return publications
}

func checkpointRefForPublication(publication ports.SnapshotPublication) domain.CheckpointRef {
	return domain.CheckpointRef{Generation: publication.Generation, ManifestDigest: sha256.Sum256(publication.Manifest)}
}

func remainingMaintenanceGenerations(t *testing.T, repo *Repository, id domain.IncarnationID) []uint64 {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(repo.sessionPath(id), repositoryGenerations))
	if err != nil {
		t.Fatal(err)
	}
	generations := make([]uint64, 0, len(entries))
	for _, entry := range entries {
		if generation, ok := parseGenerationFilename(entry.Name()); ok {
			generations = append(generations, generation)
		}
	}
	slices.SortFunc(generations, func(a, b uint64) int { return cmp.Compare(b, a) })
	return generations
}
