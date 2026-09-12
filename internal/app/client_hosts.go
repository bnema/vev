package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	remoteadapter "github.com/bnema/vev/internal/adapters/remote"
	"github.com/bnema/vev/internal/adapters/sessionwire"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/remotes"
)

// This file composes the endpoint bindings a launching client reuses across
// handoffs. Remote discovery and catalogue publication remain daemon-owned.

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
	modeErr error
	// allowlistErr is the launch-allowlist validation failure, reported for
	// every endpoint so a malformed configured allowlist fails remote
	// resolution instead of admitting an unvetted endpoint. It is deliberately
	// not fatal to construction: a local attach never resolves an endpoint and
	// must keep working with a malformed allowlist in the environment.
	allowlistErr error
	environment  func(string) []string
	allowed      map[string]struct{}
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
	if f.allowlistErr != nil {
		return ports.RemoteEndpointBinding{}, f.allowlistErr
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

// newClientHostRegistry composes the runner-scoped endpoint cache. A malformed
// launch allowlist is stored on the factory and deferred to ResolveEndpoint, so
// it can only fail a remote target's resolution, never construction or a local
// attach. Without a configured allowlist every endpoint is admitted.
func newClientHostRegistry(deps runAttachDeps, mode remoteadapter.TransportMode, modeErr error, log *slog.Logger) ports.ClientHostRegistry {
	allowed, restricted, allowlistErr := remoteLaunchAllowlistFromEnv()
	factory := clientEndpointFactory{
		factory: deps.remoteDialerFactory, mode: mode, modeErr: modeErr, allowlistErr: allowlistErr,
		environment: deps.remoteEnvironment, allowed: allowed, restricted: restricted, log: log,
	}
	return remotes.NewHostRegistry(factory)
}
