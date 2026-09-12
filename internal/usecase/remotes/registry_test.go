package remotes

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
)

type registryTestDialer struct{ name string }

func (d registryTestDialer) Dial(context.Context) (ports.ClientConnection, error) {
	return nil, errors.New("registry test dialer is never dialed")
}

var _ ports.ClientDialer = registryTestDialer{}

// registryTestFactory resolves endpoints from its tables and records every
// call, so a test can prove the registry resolved an endpoint at most once.
type registryTestFactory struct {
	mu      sync.Mutex
	calls   []string
	dialers map[string]ports.ClientDialer
	envs    map[string][]string
	errs    map[string]error
}

func newRegistryTestFactory() *registryTestFactory {
	return &registryTestFactory{
		dialers: map[string]ports.ClientDialer{},
		envs:    map[string][]string{},
		errs:    map[string]error{},
	}
}

func (f *registryTestFactory) ResolveEndpoint(_ context.Context, endpoint string) (ports.RemoteEndpointBinding, error) {
	f.mu.Lock()
	f.calls = append(f.calls, endpoint)
	err := f.errs[endpoint]
	dialer := f.dialers[endpoint]
	env := f.envs[endpoint]
	f.mu.Unlock()
	if err != nil {
		return ports.RemoteEndpointBinding{}, err
	}
	return ports.RemoteEndpointBinding{Dialer: dialer, Environment: env}, nil
}

func (f *registryTestFactory) callCount(endpoint string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	count := 0
	for _, call := range f.calls {
		if call == endpoint {
			count++
		}
	}
	return count
}

// registryTestDirectory is an injectable discovery projection.
type registryTestDirectory struct {
	snapshot   ports.RemoteDirectorySnapshot
	sub        *registryTestSubscription
	reconciled []string
	changed    int
	mu         sync.Mutex
}

func (d *registryTestDirectory) Snapshot() ports.RemoteDirectorySnapshot { return d.snapshot }

func (d *registryTestDirectory) Subscribe() ports.RemoteDirectorySubscription {
	if d.sub == nil {
		d.sub = &registryTestSubscription{changed: make(chan struct{}, 1)}
	}
	return d.sub
}

func (d *registryTestDirectory) RequestReconcile(endpoint string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.reconciled = append(d.reconciled, endpoint)
}

func (d *registryTestDirectory) RegistryChanged() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.changed++
}

type registryTestSubscription struct{ changed chan struct{} }

func (s *registryTestSubscription) Changed() <-chan struct{} { return s.changed }
func (s *registryTestSubscription) Close()                   {}

func TestHostRegistryReusesEndpointBindings(t *testing.T) {
	factory := newRegistryTestFactory()
	dialA := registryTestDialer{name: "A"}
	dialB := registryTestDialer{name: "B"}
	factory.dialers["a"] = dialA
	factory.dialers["b"] = dialB
	registry := NewHostRegistry(factory)

	first, err := registry.ResolveEndpoint(context.Background(), "a")
	require.NoError(t, err)
	second, err := registry.ResolveEndpoint(context.Background(), "b")
	require.NoError(t, err)
	again, err := registry.ResolveEndpoint(context.Background(), "a")
	require.NoError(t, err)

	require.Equal(t, dialA, first.Dialer)
	require.Equal(t, dialB, second.Dialer)
	require.Equal(t, dialA, again.Dialer, "returning to an endpoint must reuse its binding")
	require.Equal(t, 1, factory.callCount("a"))
	require.Equal(t, 1, factory.callCount("b"))
}

func TestHostRegistryReturnsDefensiveBindings(t *testing.T) {
	factory := newRegistryTestFactory()
	factory.dialers["a"] = registryTestDialer{name: "A"}
	factory.envs["a"] = []string{"FOO=1"}
	factory.dialers["b"] = registryTestDialer{name: "B"}
	factory.envs["b"] = []string{}
	registry := NewHostRegistry(factory)

	first, err := registry.ResolveEndpoint(context.Background(), "a")
	require.NoError(t, err)
	first.Environment[0] = "FOO=mutated"
	again, err := registry.ResolveEndpoint(context.Background(), "a")
	require.NoError(t, err)
	require.Equal(t, []string{"FOO=1"}, again.Environment, "a caller must not mutate the cached binding")

	empty, err := registry.ResolveEndpoint(context.Background(), "b")
	require.NoError(t, err)
	require.NotNil(t, empty.Environment, "an explicitly empty environment stays empty, not nil")

	factory.dialers["c"] = registryTestDialer{name: "C"}
	absent, err := registry.ResolveEndpoint(context.Background(), "c")
	require.NoError(t, err)
	require.Nil(t, absent.Environment, "an endpoint without an environment keeps it nil")
}

func TestHostRegistryDoesNotCacheResolutionFailures(t *testing.T) {
	factory := newRegistryTestFactory()
	want := errors.New("launch policy refused this endpoint")
	factory.errs["a"] = want
	factory.dialers["a"] = registryTestDialer{name: "A"}
	registry := NewHostRegistry(factory)

	_, err := registry.ResolveEndpoint(context.Background(), "a")
	require.ErrorIs(t, err, want, "the factory error is the only authority")

	delete(factory.errs, "a")
	binding, err := registry.ResolveEndpoint(context.Background(), "a")
	require.NoError(t, err)
	require.Equal(t, registryTestDialer{name: "A"}, binding.Dialer)
	require.Equal(t, 2, factory.callCount("a"), "a failed resolution must be retried, never cached")
}

func TestHostRegistryRejectsEmptyEndpointAndMissingFactory(t *testing.T) {
	registry := NewHostRegistry(newRegistryTestFactory())
	_, err := registry.ResolveEndpoint(context.Background(), "")
	require.ErrorIs(t, err, ErrInvalidEndpoint)

	var nilRegistry *HostRegistry
	_, err = nilRegistry.ResolveEndpoint(context.Background(), "a")
	require.ErrorIs(t, err, ErrInvalidEndpoint)

	withoutFactory := NewHostRegistry(nil)
	_, err = withoutFactory.ResolveEndpoint(context.Background(), "a")
	require.ErrorIs(t, err, ErrInvalidEndpoint)

	withoutDialer := newRegistryTestFactory()
	registry = NewHostRegistry(withoutDialer)
	_, err = registry.ResolveEndpoint(context.Background(), "a")
	require.ErrorIs(t, err, ErrInvalidEndpoint)
}

func TestHostRegistryConcurrentResolvesShareOneBinding(t *testing.T) {
	factory := newRegistryTestFactory()
	factory.dialers["a"] = registryTestDialer{name: "A"}
	registry := NewHostRegistry(factory)

	const workers = 16
	bindings := make([]ports.RemoteEndpointBinding, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			binding, err := registry.ResolveEndpoint(context.Background(), "a")
			require.NoError(t, err)
			bindings[index] = binding
		}(i)
	}
	wg.Wait()

	for _, binding := range bindings {
		require.Equal(t, registryTestDialer{name: "A"}, binding.Dialer)
	}
	require.Positive(t, factory.callCount("a"))
}
