package remotes

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/bnema/vev/internal/ports"
)

// This file owns the runner-scoped client host registry: the endpoint bindings
// one client reuses across handoffs, plus the discovery projection the same
// client subscribes to. It sits in front of the remote monitor, so a runner
// starts and joins exactly one discovery loop no matter how many serving
// daemons, sessions, or picker interactions it passes through.

// ErrInvalidEndpoint reports a request the registry cannot resolve because its
// endpoint identity is empty. It is never a transport or policy failure.
var ErrInvalidEndpoint = errors.New("vev: invalid remote endpoint")

// HostRegistry resolves endpoints into reusable client carriages and forwards
// the discovery projection. One registry belongs to one runner: its bindings
// are valid only for that runner's frozen launch policy, and Run owns the
// discovery loop until the runner exits.
type HostRegistry struct {
	factory   ports.RemoteEndpointFactory
	directory ports.RemoteDirectory
	run       func(context.Context) error
	log       *slog.Logger

	mu       sync.Mutex
	bindings map[string]ports.RemoteEndpointBinding
}

// NewHostRegistry wires a registry from the endpoint factory and the discovery
// directory it projects, plus the loop it drives. Construction performs no I/O
// and starts nothing.
func NewHostRegistry(factory ports.RemoteEndpointFactory, directory ports.RemoteDirectory, run func(context.Context) error, log *slog.Logger) *HostRegistry {
	if log == nil {
		log = slog.Default()
	}
	return &HostRegistry{
		factory: factory, directory: directory, run: run, log: log,
		bindings: make(map[string]ports.RemoteEndpointBinding),
	}
}

// ResolveEndpoint returns the endpoint's binding, resolving it once and reusing
// it for every later handoff to that endpoint. A failed resolution is never
// cached, so a later attempt retries through the same single authority instead
// of falling back to another resolver.
func (r *HostRegistry) ResolveEndpoint(ctx context.Context, endpoint string) (ports.RemoteEndpointBinding, error) {
	if r == nil {
		return ports.RemoteEndpointBinding{}, ErrInvalidEndpoint
	}
	if endpoint == "" {
		return ports.RemoteEndpointBinding{}, ErrInvalidEndpoint
	}
	r.mu.Lock()
	cached, ok := r.bindings[endpoint]
	r.mu.Unlock()
	if ok {
		return bindingCopy(cached), nil
	}
	if r.factory == nil {
		return ports.RemoteEndpointBinding{}, ErrInvalidEndpoint
	}
	resolved, err := r.factory.ResolveEndpoint(ctx, endpoint)
	if err != nil {
		return ports.RemoteEndpointBinding{}, err
	}
	if resolved.Dialer == nil {
		return ports.RemoteEndpointBinding{}, ErrInvalidEndpoint
	}
	// The binding is stored and handed out defensively: a caller that mutates
	// the environment it received must not change the next handoff's.
	r.mu.Lock()
	if existing, ok := r.bindings[endpoint]; ok {
		// A concurrent resolve won the race; both are equivalent by policy.
		resolved = existing
	} else {
		r.bindings[endpoint] = bindingCopy(resolved)
	}
	r.mu.Unlock()
	return bindingCopy(resolved), nil
}

// Snapshot returns the newest discovery publication.
func (r *HostRegistry) Snapshot() ports.RemoteDirectorySnapshot {
	if r == nil || r.directory == nil {
		return ports.RemoteDirectorySnapshot{}
	}
	return r.directory.Snapshot()
}

// Subscribe wakes the caller when a newer discovery publication exists.
func (r *HostRegistry) Subscribe() ports.RemoteDirectorySubscription {
	if r == nil || r.directory == nil {
		return noDirectorySubscription{}
	}
	return r.directory.Subscribe()
}

// RequestReconcile asks the discovery loop to re-verify one endpoint.
func (r *HostRegistry) RequestReconcile(endpoint string) {
	if r == nil || r.directory == nil {
		return
	}
	r.directory.RequestReconcile(endpoint)
}

// RegistryChanged tells the discovery loop that host registrations changed.
func (r *HostRegistry) RegistryChanged() {
	if r == nil || r.directory == nil {
		return
	}
	r.directory.RegistryChanged()
}

// Run drives the discovery loop until ctx ends. One runner calls it once, so
// every handoff and picker interaction shares the same observation lifetime.
func (r *HostRegistry) Run(ctx context.Context) error {
	if r == nil || r.run == nil {
		return nil
	}
	return r.run(ctx)
}

// bindingCopy returns a binding whose environment the caller owns. A nil
// environment stays nil: an endpoint with no explicit environment is not the
// same as one configured with an empty environment.
func bindingCopy(binding ports.RemoteEndpointBinding) ports.RemoteEndpointBinding {
	if binding.Environment == nil {
		return ports.RemoteEndpointBinding{Dialer: binding.Dialer}
	}
	return ports.RemoteEndpointBinding{
		Dialer:      binding.Dialer,
		Environment: append([]string{}, binding.Environment...),
	}
}

// noDirectorySubscription is the inert subscription of a registry without a
// directory: it never wakes and closes without effect.
type noDirectorySubscription struct{}

func (noDirectorySubscription) Changed() <-chan struct{} { return nil }
func (noDirectorySubscription) Close()                   {}
