package remote

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	appports "github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

type blockingCache struct {
	release chan struct{}
	done    chan struct{}
	mu      sync.Mutex
	stores  [][]catalogue.RemoteCatalogCacheEntry
}

func newBlockingCache(t *testing.T) *blockingCache {
	t.Helper()
	cache := &blockingCache{release: make(chan struct{}), done: make(chan struct{})}
	t.Cleanup(func() {
		select {
		case <-cache.done:
		default:
			close(cache.done)
		}
	})
	return cache
}

func (c *blockingCache) Load() ([]catalogue.RemoteCatalogCacheEntry, error) { return nil, nil }

func (c *blockingCache) Store(entries []catalogue.RemoteCatalogCacheEntry) error {
	c.mu.Lock()
	c.stores = append(c.stores, entries)
	c.mu.Unlock()
	select {
	case <-c.release:
		return nil
	case <-c.done:
		return errors.New("test teardown releases blocked store")
	}
}

type instantCatalog struct {
	mu       sync.Mutex
	calls    []string
	deadline []time.Duration
}

func (c *instantCatalog) List(ctx context.Context, target string) (catalogue.RemoteCatalog, error) {
	deadline, _ := ctx.Deadline()
	c.mu.Lock()
	c.calls = append(c.calls, target)
	c.deadline = append(c.deadline, time.Until(deadline))
	c.mu.Unlock()
	return catalogue.RemoteCatalog{Sessions: []catalogue.RemoteCatalogSession{{Name: "work"}}}, nil
}

type hangingCatalog struct{ started chan struct{} }

func (c *hangingCatalog) List(ctx context.Context, target string) (catalogue.RemoteCatalog, error) {
	close(c.started)
	<-ctx.Done()
	return catalogue.RemoteCatalog{}, ctx.Err()
}

type recordStore struct {
	pinned  []domain.RemoteRegistration
	learned []domain.RemoteRegistration
	err     error
}

func testRecord(endpoint string, incarnation byte) domain.RemoteRegistration {
	return domain.RemoteRegistration{Endpoint: endpoint, Incarnation: [16]byte{incarnation}, Generation: 1}
}

func (s *recordStore) Hosts() ([]domain.RemoteRegistration, []domain.RemoteRegistration, error) {
	return s.pinned, s.learned, s.err
}
func (*recordStore) AddPinned(string) error    { return nil }
func (*recordStore) RemovePinned(string) error { return nil }
func (*recordStore) Remember(string) error     { return nil }
func (*recordStore) Forget(string) error       { return nil }
func (*recordStore) Remove(string) (bool, error) {
	return false, nil
}

func startRuntime(t *testing.T, runtime *Runtime) (chan<- appports.RemoteJob, <-chan appports.RemoteJobResult, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	jobs := make(chan appports.RemoteJob, 16)
	results := make(chan appports.RemoteJobResult, 16)
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx, jobs, results) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("runtime did not stop")
		}
	})
	return jobs, results, cancel
}

func receiveResult(t *testing.T, ch <-chan appports.RemoteJobResult, what string) appports.RemoteJobResult {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		return appports.RemoteJobResult{}
	}
}

func TestRuntimeBlockedWriterDoesNotStallOtherLanes(t *testing.T) {
	t.Parallel()

	cache := &blockingCache{release: make(chan struct{})}
	catalog := &instantCatalog{}
	store := &recordStore{
		pinned:  []domain.RemoteRegistration{testRecord("user@arch", 1)},
		learned: []domain.RemoteRegistration{testRecord("user@mule", 2)},
	}
	runtime := NewRuntime(store, catalog, cache, nil)
	jobs, results, _ := startRuntime(t, runtime)
	// Release the blocked writer on any early failure so the runtime
	// cleanup's bounded join never wedges behind this test's Fatalf.
	t.Cleanup(func() {
		select {
		case <-cache.release:
		default:
			close(cache.release)
		}
	})

	// Occupy the single cache writer, then prove registry reads and all
	// four observation lanes make progress around the blocked write.
	jobs <- appports.RemoteJob{Kind: appports.RemoteJobCacheStore, Entries: []catalogue.RemoteCatalogCacheEntry{{Host: "user@arch"}}}
	jobs <- appports.RemoteJob{Kind: appports.RemoteJobRegistryRead}
	for _, endpoint := range []string{"a", "b", "c", "d"} {
		jobs <- appports.RemoteJob{Kind: appports.RemoteJobCatalogObserve, Endpoint: endpoint}
	}

	registry := appports.RemoteJobResult{}
	observes := 0
	// Lanes run independently: the registry read and the four
	// observations complete in arrival order, not admission order.
	for i := 0; i < 5; i++ {
		result := receiveResult(t, results, "registry or observe")
		switch result.Kind {
		case appports.RemoteJobRegistryRead:
			if registry.Kind != 0 {
				t.Fatalf("duplicate registry result = %+v", result)
			}
			registry = result
		case appports.RemoteJobCatalogObserve:
			if result.Err != nil || result.FailureKind != domain.RemoteFailureNone {
				t.Fatalf("observe result = %+v", result)
			}
			if len(result.Catalog.Sessions) != 1 {
				t.Fatalf("observe catalog = %+v", result.Catalog)
			}
			observes++
		default:
			t.Fatalf("unexpected result kind = %+v", result)
		}
	}
	if registry.Err != nil {
		t.Fatalf("registry result = %+v", registry)
	}
	if len(registry.Registrations) != 2 || registry.Registrations[0].Endpoint != "user@arch" || registry.Registrations[1].Endpoint != "user@mule" {
		t.Fatalf("registrations = %+v, want pinned-then-learned order", registry.Registrations)
	}
	if observes != 4 {
		t.Fatalf("observes = %d, want 4", observes)
	}

	// A second store while the writer is occupied queues behind it; a third
	// concurrent store cannot enter the lane and retires with a lane-busy
	// failure instead of wedging dispatch. The service never issues either:
	// it keeps one active write plus one replaceable pending snapshot.
	second := appports.RemoteJob{Kind: appports.RemoteJobCacheStore, Entries: []catalogue.RemoteCatalogCacheEntry{{Host: "second"}}}
	third := appports.RemoteJob{Kind: appports.RemoteJobCacheStore, Entries: []catalogue.RemoteCatalogCacheEntry{{Host: "third"}}}
	jobs <- second
	jobs <- third
	duplicate := receiveResult(t, results, "duplicate store")
	if duplicate.Kind != appports.RemoteJobCacheStore || !errors.Is(duplicate.Err, errRemoteLaneBusy) {
		t.Fatalf("duplicate store result = %+v, want lane-busy failure", duplicate)
	}

	close(cache.release)
	firstStored := receiveResult(t, results, "cache store")
	if firstStored.Kind != appports.RemoteJobCacheStore || firstStored.Err != nil {
		t.Fatalf("store result = %+v", firstStored)
	}
	secondStored := receiveResult(t, results, "queued cache store")
	if secondStored.Kind != appports.RemoteJobCacheStore || secondStored.Err != nil {
		t.Fatalf("queued store result = %+v", secondStored)
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if len(cache.stores) != 2 || len(cache.stores[0]) != 1 || cache.stores[0][0].Host != "user@arch" ||
		len(cache.stores[1]) != 1 || cache.stores[1][0].Host != "second" {
		t.Fatalf("stored payloads = %+v, want serialized first then second", cache.stores)
	}
}

func TestRuntimeObserveHonorsDeadlineAndCancel(t *testing.T) {
	t.Parallel()

	catalog := &instantCatalog{}
	runtime := NewRuntime(&recordStore{}, catalog, &blockingCache{release: make(chan struct{})}, nil)
	jobs, results, _ := startRuntime(t, runtime)

	jobs <- appports.RemoteJob{Kind: appports.RemoteJobCatalogObserve, Endpoint: "user@arch", Deadline: 5 * time.Second}
	result := receiveResult(t, results, "bounded observe")
	if result.Err != nil {
		t.Fatalf("observe result = %+v", result)
	}
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	if len(catalog.calls) != 1 || catalog.calls[0] != "user@arch" {
		t.Fatalf("catalog calls = %v", catalog.calls)
	}
	if catalog.deadline[0] <= 0 || catalog.deadline[0] > 5*time.Second {
		t.Fatalf("observe ctx deadline in %v, want within (0, 5s]", catalog.deadline[0])
	}
}

func TestRuntimeObserveCancelReapsWorker(t *testing.T) {
	t.Parallel()

	catalog := &hangingCatalog{started: make(chan struct{})}
	runtime := NewRuntime(&recordStore{}, catalog, &blockingCache{release: make(chan struct{})}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	jobs := make(chan appports.RemoteJob, 4)
	results := make(chan appports.RemoteJobResult, 4)
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx, jobs, results) }()

	jobs <- appports.RemoteJob{Kind: appports.RemoteJobCatalogObserve, Endpoint: "user@arch", Deadline: time.Minute}
	select {
	case <-catalog.started:
	case <-time.After(5 * time.Second):
		t.Fatal("observation did not start")
	}
	cancel()
	// Delivery is abandoned once the runtime context is cancelled, so a
	// cancelled observation either reports context.Canceled or never
	// reports at all; either way the runtime itself must stop. Note done
	// is buffered depth one: once observed stopped, do not wait on it
	// again.
	stopped := false
	select {
	case result := <-results:
		if !errors.Is(result.Err, context.Canceled) {
			t.Fatalf("cancelled observe err = %v, want context.Canceled", result.Err)
		}
	case <-done:
		stopped = true
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled observation neither completed nor stopped the runtime")
	}
	if !stopped {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("runtime did not stop after cancel")
		}
	}
}

func TestRuntimeJobsCloseReapsWorkers(t *testing.T) {
	t.Parallel()

	// Closing the job channel with a live parent context must still
	// reap every worker: the runtime owns cancellation explicitly
	// instead of hanging behind idle lanes.
	runtime := NewRuntime(&recordStore{}, &instantCatalog{}, &blockingCache{release: make(chan struct{})}, nil)
	jobs := make(chan appports.RemoteJob, 4)
	results := make(chan appports.RemoteJobResult, 4)
	done := make(chan error, 1)
	go func() { done <- runtime.Run(context.Background(), jobs, results) }()

	close(jobs)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runtime did not stop after jobs closed with live context")
	}
}

func TestRuntimeCancelledCompletionDoesNotWedge(t *testing.T) {
	t.Parallel()

	runtime := NewRuntime(&recordStore{}, &instantCatalog{}, &blockingCache{release: make(chan struct{})}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	jobs := make(chan appports.RemoteJob, 4)
	// Unbuffered, unconsumed results: completion must abandon delivery on
	// cancellation instead of wedging shutdown.
	results := make(chan appports.RemoteJobResult)
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx, jobs, results) }()

	jobs <- appports.RemoteJob{Kind: appports.RemoteJobRegistryRead}
	// Let the worker finish and park in completion delivery with no
	// consumer on the unbuffered results channel.
	time.Sleep(200 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("runtime wedged on unread result")
	}
}

func TestCombineRegistrations(t *testing.T) {
	t.Parallel()

	archPinned := testRecord("user@arch", 1)
	archLearned := testRecord("user@arch", 9)
	mule := testRecord("user@mule", 2)
	got := combineRegistrations(
		[]domain.RemoteRegistration{archPinned},
		[]domain.RemoteRegistration{mule, archLearned},
	)
	if len(got) != 2 || got[0] != archPinned || got[1] != mule {
		t.Fatalf("combine = %+v, want pinned-then-learned-only", got)
	}
	if len(combineRegistrations(nil, nil)) != 0 {
		t.Fatal("empty combine must stay empty")
	}
}
