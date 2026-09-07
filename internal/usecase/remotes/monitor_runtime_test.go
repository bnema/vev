package remotes

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/catalogue"

	remoteadapter "github.com/bnema/vev/internal/adapters/remote"
)

// gateCatalog is a scripted RemoteCatalogClient: observations block until the
// test answers or the observation context ends.
type gateCatalog struct {
	mu      sync.Mutex
	calls   []string
	request chan gateRequest
}

type gateRequest struct {
	ctx    context.Context
	target string
	reply  chan gateReply
}

type gateReply struct {
	catalog catalogue.RemoteCatalog
	err     error
}

func newGateCatalog() *gateCatalog {
	return &gateCatalog{request: make(chan gateRequest)}
}

func (c *gateCatalog) List(ctx context.Context, target string) (catalogue.RemoteCatalog, error) {
	c.mu.Lock()
	c.calls = append(c.calls, target)
	c.mu.Unlock()
	reply := make(chan gateReply, 1)
	select {
	case c.request <- gateRequest{ctx: ctx, target: target, reply: reply}:
	case <-ctx.Done():
		return catalogue.RemoteCatalog{}, ctx.Err()
	}
	select {
	case answer := <-reply:
		return answer.catalog, answer.err
	case <-ctx.Done():
		return catalogue.RemoteCatalog{}, ctx.Err()
	}
}

func (c *gateCatalog) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.calls)
}

func receiveGateRequest(t *testing.T, catalog *gateCatalog, what string) gateRequest {
	t.Helper()
	select {
	case request := <-catalog.request:
		return request
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		return gateRequest{}
	}
}

// waitSnapshot polls the monitor publication until the desired state holds.
// Publications may coalesce, so integration sync keys on state, not wakes.
func waitSnapshot(t *testing.T, monitor *Monitor, what string, want func(ports.RemoteDirectorySnapshot) bool) ports.RemoteDirectorySnapshot {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		snapshot := monitor.Snapshot()
		if want(snapshot) {
			return snapshot
		}
		select {
		case <-time.After(20 * time.Millisecond):
		case <-deadline:
			t.Fatalf("timed out waiting for %s: %+v", what, snapshot)
			return ports.RemoteDirectorySnapshot{}
		}
	}
}

// TestMonitorRuntimeIntegration drives the real registry, cache and runtime
// adapters through the Monitor policy loop: bootstrap from disk, fair
// observation across hosts, successful persistence, backward-clock safety
// and cooperative cancellation with no stranded work.
func TestMonitorRuntimeIntegration(t *testing.T) {
	// The state directory must not exist yet so safedir creates it private.
	stateDir := filepath.Join(t.TempDir(), "vev")
	store := remoteadapter.NewFileHostStore(remoteadapter.HostStorePath(stateDir))
	if err := store.AddPinned("user@arch"); err != nil {
		t.Fatalf("AddPinned: %v", err)
	}
	if err := store.Remember("user@mule"); err != nil {
		t.Fatalf("Remember: %v", err)
	}
	pinned, learned, err := store.Hosts()
	if err != nil || len(pinned) != 1 || len(learned) != 1 {
		t.Fatalf("Hosts() = %+v, %+v, %v", pinned, learned, err)
	}
	archRegistration, muleRegistration := pinned[0], learned[0]

	cache := remoteadapter.NewFileCatalogCache(remoteadapter.CatalogCachePath(stateDir))
	now := time.Unix(2_000, 0)
	if err := cache.Store([]catalogue.RemoteCatalogCacheEntry{
		{
			Host: "user@arch", FetchedAt: now, Incarnation: archRegistration.Incarnation,
			Sessions: []catalogue.RemoteCatalogSession{{LifecycleID: domain.SessionLifecycleID{1}, Name: "cached", State: catalogue.RemoteCatalogSessionUp, Tabs: []catalogue.RemoteCatalogTab{{ID: "t-cached"}}}},
		},
		{
			Host: "user@mule", FetchedAt: now, Incarnation: [16]byte{9},
			Sessions: []catalogue.RemoteCatalogSession{{LifecycleID: domain.SessionLifecycleID{2}, Name: "ghost", State: catalogue.RemoteCatalogSessionDown, Tabs: []catalogue.RemoteCatalogTab{{ID: "t-ghost"}}}},
		},
	}); err != nil {
		t.Fatalf("Store: %v", err)
	}

	gate := newGateCatalog()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	clock := newFakeClock(now)
	runtime := remoteadapter.NewRuntime(store, gate, cache, log)
	monitor := NewMonitor(runtime, clock, log)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- monitor.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatalf("monitor did not stop")
		}
	})
	sub := monitor.Subscribe()
	defer sub.Close()

	// Both hosts are admitted fairly: one observation each, in rank order.
	first := receiveGateRequest(t, gate, "first observe")
	second := receiveGateRequest(t, gate, "second observe")
	observed := map[string]gateRequest{first.target: first, second.target: second}
	if len(observed) != 2 {
		t.Fatalf("observed targets = %v, want both hosts", observed)
	}
	deadline, ok := first.ctx.Deadline()
	if !ok || time.Until(deadline) <= 0 || time.Until(deadline) > 10*time.Second {
		t.Fatalf("observe ctx deadline in %v, want bounded 10s observation", time.Until(deadline))
	}

	// Matching cache seeds arch; the mismatched mule entry never seeds.
	// Registry and cache complete independently and their publications
	// may coalesce, so synchronize on snapshot state, not wake counts.
	snapshot := waitSnapshot(t, monitor, "seeded bootstrap", func(snapshot ports.RemoteDirectorySnapshot) bool {
		if !snapshot.Initialized {
			return false
		}
		arch, ok := snapshot.Find("user@arch")
		if !ok || !arch.InventoryKnown {
			return false
		}
		_, ok = snapshot.Find("user@mule")
		return ok
	})
	arch, ok := snapshot.Find("user@arch")
	if !ok || !arch.InventoryKnown || len(arch.Sessions) != 1 || arch.Sessions[0].Name != "cached" {
		t.Fatalf("arch = %+v, want seeded cached inventory", arch)
	}
	mule, ok := snapshot.Find("user@mule")
	if !ok || mule.InventoryKnown {
		t.Fatalf("mule = %+v, want no mismatched seeding", mule)
	}
	if mule.Registration != muleRegistration {
		t.Fatalf("mule registration = %+v, want %+v", mule.Registration, muleRegistration)
	}
	if arch.Rank != 0 || mule.Rank != 1 {
		t.Fatalf("ranks = %d, %d, want pinned-then-learned 0, 1", arch.Rank, mule.Rank)
	}

	// Successful observations publish reachability and persist bound
	// inventories through the real cache file.
	observed["user@arch"].reply <- gateReply{catalog: catalogue.RemoteCatalog{Sessions: []catalogue.RemoteCatalogSession{
		{LifecycleID: domain.SessionLifecycleID{1}, Name: "work", State: catalogue.RemoteCatalogSessionUp, Tabs: []catalogue.RemoteCatalogTab{{ID: "t-work"}}},
	}}}
	observed["user@mule"].reply <- gateReply{err: errMonitorTestFailure}
	snapshot = waitSnapshot(t, monitor, "observations", func(snapshot ports.RemoteDirectorySnapshot) bool {
		arch, ok := snapshot.Find("user@arch")
		if !ok || arch.Availability != domain.RemoteAvailabilityReachable || len(arch.Sessions) != 1 || arch.Sessions[0].Name != "work" {
			return false
		}
		mule, ok := snapshot.Find("user@mule")
		return ok && mule.Availability == domain.RemoteAvailabilityUnreachable && mule.ConsecutiveFailures == 1
	})
	arch, _ = snapshot.Find("user@arch")
	mule, _ = snapshot.Find("user@mule")

	var persisted []catalogue.RemoteCatalogCacheEntry
	persistDeadline := time.After(10 * time.Second)
	for {
		var err error
		persisted, err = cache.Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if len(persisted) == 1 && persisted[0].Host == "user@arch" && len(persisted[0].Sessions) == 1 && persisted[0].Sessions[0].Name == "work" {
			break
		}
		select {
		case <-time.After(20 * time.Millisecond):
		case <-persistDeadline:
			t.Fatalf("persisted = %+v, want bound arch inventory only", persisted)
		}
	}
	if persisted[0].Incarnation != archRegistration.Incarnation {
		t.Fatalf("persisted incarnation = %x, want %x", persisted[0].Incarnation, archRegistration.Incarnation)
	}

	// A backward clock advance schedules no new work and changes nothing.
	calls := gate.callCount()
	clock.Advance(-time.Hour)
	select {
	case request := <-gate.request:
		t.Fatalf("backward clock admitted observe for %q", request.target)
	case <-time.After(200 * time.Millisecond):
	}
	if gate.callCount() != calls {
		t.Fatal("backward clock caused new observations")
	}

	// Cooperative cancellation strands nothing: cancel here and let the
	// cleanup below assert that Run returns (single owner of done).
	cancel()
}
