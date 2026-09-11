package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	remoteadapter "github.com/bnema/vev/internal/adapters/remote"
	"github.com/bnema/vev/internal/adapters/sessionwire"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/platform"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/remotes"
)

// This file composes the launching client's own view of the remote hosts it
// can reach: the endpoint bindings it reuses across handoffs and the discovery
// directory it projects. The daemon keeps its own composition (the discovery
// daemon option); the two never share a writer, so a client monitor cannot
// truncate the daemon's durable catalogue snapshot.

// clientEndpointFactory resolves one endpoint into the carriage the client
// reuses for every session on that endpoint. Transport mode, launch
// configuration, and endpoint environment are frozen here by the composition
// root; the client only receives the resulting typed dialer.
type clientEndpointFactory struct {
	factory remoteDialerForTarget
	mode    remoteadapter.TransportMode
	// modeErr is the transport-mode validation failure, reported for every
	// endpoint so an invalid configured mode fails a handoff instead of
	// silently selecting another carriage.
	modeErr     error
	environment func(string) []string
	allowed     map[string]struct{}
	// restricted reports whether an allowlist was configured at all. A
	// configured allowlist is authoritative even when it lists nothing.
	restricted bool
	log        *slog.Logger
}

var _ ports.RemoteEndpointFactory = clientEndpointFactory{}

func (f clientEndpointFactory) ResolveEndpoint(ctx context.Context, endpoint string) (ports.RemoteEndpointBinding, error) {
	if err := ctx.Err(); err != nil {
		return ports.RemoteEndpointBinding{}, err
	}
	if err := domain.ValidateRemoteHostTarget(endpoint); err != nil {
		return ports.RemoteEndpointBinding{}, fmt.Errorf("vev: invalid remote endpoint: %w", err)
	}
	if f.modeErr != nil {
		return ports.RemoteEndpointBinding{}, f.modeErr
	}
	if f.restricted && !allowlistedRemoteEndpoint(f.allowed, endpoint) {
		return ports.RemoteEndpointBinding{}, fmt.Errorf("vev: remote endpoint %q is not allowed", endpoint)
	}
	if f.factory == nil {
		return ports.RemoteEndpointBinding{}, errors.New("vev: remote dialer factory unavailable")
	}
	// The carriage is endpoint-scoped: the session name only labels logs, so a
	// binding resolved for one session serves every later session on it.
	dialer, err := f.factory(endpoint, "", f.mode, f.log)
	if err != nil {
		return ports.RemoteEndpointBinding{}, err
	}
	if dialer == nil {
		return ports.RemoteEndpointBinding{}, errors.New("vev: remote dialer factory returned no dialer")
	}
	return ports.RemoteEndpointBinding{
		Dialer:      sessionwire.NewClientDialer(dialer),
		Environment: clientEnvironment(f.environment, endpoint),
	}, nil
}

// newClientHostRegistry composes the runner-scoped client host registry: the
// endpoint factory above, the discovery monitor over the client's own store,
// catalogue client, and runner-local cache, and the loop the runner joins.
func newClientHostRegistry(deps runAttachDeps, mode remoteadapter.TransportMode, modeErr error, clk ports.Clock, log *slog.Logger) (ports.ClientHostRegistry, error) {
	allowed, restricted, err := remoteLaunchAllowlistFromEnv()
	if err != nil {
		return nil, err
	}
	stateDir := deps.stateDir
	if stateDir == nil {
		stateDir = platform.StateDir
	}
	store := deps.hostStore
	if store == nil {
		store = ports.RemoteHostStore(remoteadapter.NewFileHostStore(remoteadapter.HostStorePath(stateDir())))
	}
	catalog := ports.RemoteCatalogClient(newRemoteCatalogClient())
	// The client monitor seeds from the durable catalogue snapshot and keeps
	// every later write in memory: the daemon remains its only writer.
	cache := ports.RemoteCatalogCache(remoteadapter.NewSeededCatalogCache(
		remoteadapter.NewFileCatalogCache(remoteadapter.CatalogCachePath(stateDir())),
	))
	if restricted {
		store = allowlistedRemoteHostStore{delegate: store, allowed: allowed}
		catalog = allowlistedRemoteCatalogClient{delegate: catalog, allowed: allowed}
	}
	runtime := remoteadapter.NewRuntime(store, catalog, cache, log)
	monitor := remotes.NewMonitor(runtime, clk, log)
	factory := clientEndpointFactory{
		factory: deps.remoteDialerFactory, mode: mode, modeErr: modeErr, environment: deps.remoteEnvironment,
		allowed: allowed, restricted: restricted, log: log,
	}
	return remotes.NewHostRegistry(factory, monitor, monitor.Run, log), nil
}
