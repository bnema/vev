package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/bnema/vev/internal/adapters/brokerconfig"
	"github.com/bnema/vev/internal/adapters/daemonmux"
	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/ports"
)

// Broker-owned daemon start.
//
// The broker is the only component that decides whether a target daemon may be
// started. Its resolved dial target already carries the closed start
// authorization (ports.BrokerDaemonStartMode) together with the policy and the
// endpoint fence, and this file is the single place where that authorization
// reaches transport mechanics:
//
//   - BrokerDaemonExistingOnly dials the private daemonmux carriage and nothing
//     else. It never probes the lifecycle lock, never takes the spawn lock,
//     never launches a process, and never dials a session: an existing-only
//     acquisition is exactly one carriage dial.
//   - BrokerDaemonStartIfNeeded dials the same carriage first. A successful dial
//     is the whole result — an already running daemon is never restarted. Only
//     when the dial proves the daemon is absent does the start proceed, and only
//     when the resolved policy explicitly authorizes launching. The start itself
//     is the existing election in spawn.go: lifecycle availability, one elected
//     spawner under the shared spawn lock, then a bounded redial of the same
//     carriage under the same budget.
//
// The result handed to the pool is always daemonmux: no session transport is
// ever produced here, so no caller can fall back to the direct session dial the
// broker replaced. Readiness is proven by the carriage the pool then
// authenticates with the daemonmux physical preamble, never by a session dial,
// a session control stream, or a Hello.
//
// Refusals are explicit and typed. A daemon that is absent may only be started
// under an authorizing policy; a carriage that exists but refuses the peer, the
// path, or the credentials is never "fixed" by spawning a second daemon, and a
// start that leaves the carriage still undialable is reported rather than
// retried with a session dial. Identity, incarnation, policy, and version are
// verified by the daemonmux preamble after this function returns, so those
// failures can never trigger a spawn either.

// brokerLaunchAuthority is the broker-owned policy launch token that explicitly
// authorizes starting a stopped target daemon. The ports policy carries the
// token opaquely — it never interprets it — so the component that owns "may
// this daemon be started?" is the composition. The broker's own production local
// daemon authority names this token; an operator-provisioned registration that
// names something else, and every sandbox-private token, fails closed and never
// spawns.
const brokerLaunchAuthority = "explicit"

// brokerDaemonStarter owns the three facts a daemon start needs: the private
// daemonmux carriage dial, the detached daemon launcher, and the exact carriage
// the launched daemon will publish. All three are seams so tests exercise the
// real election, policy, and refusal rules without binding a socket or
// re-executing a daemon.
type brokerDaemonStarter struct {
	dial    func(ctx context.Context, carriage string) (daemonmux.RawFramedTransport, error)
	spawn   spawnFunc
	backoff backoffConfig
	// published returns the private daemonmux carriage the spawned daemon binds.
	// Nil means the production endpoint of this process.
	published func() string
}

// localDaemonStarter is the production starter: the real private Unix daemonmux
// carriage, the real double-fork daemon launcher, and the exact endpoint that
// launcher publishes. It is the broker process' starter and a remote mux
// helper's starter alike, because both start the daemon published on their own
// machine by re-executing their own binary. A test replaces it with scripted
// mechanics and restores it immediately; it is package state for the same reason
// defaultBackoff is, and no test mutates it in parallel.
var localDaemonStarter = brokerDaemonStarter{dial: dialLocalDaemonCarriage, spawn: realSpawn}

// effectiveBackoff returns the starter's explicit backoff, or the production
// default read at call time, so a test that narrows defaultBackoff still governs
// the daemon start.
func (s brokerDaemonStarter) effectiveBackoff() backoffConfig {
	if s.backoff.total > 0 {
		return s.backoff
	}
	return defaultBackoff
}

// effectivePublished returns the private daemonmux carriage the spawned daemon
// binds: the starter's explicit endpoint, or this process' own production
// daemon endpoint. The daemon launcher re-executes this binary, which always
// binds daemonmux.SocketPath(ipc.SocketDir()), so that is the only carriage a
// start can ever produce.
func (s brokerDaemonStarter) effectivePublished() string {
	if s.published != nil {
		return s.published()
	}
	return daemonmux.SocketPath(ipc.SocketDir())
}

// dialLocalDaemonCarriage dials exactly the private daemonmux carriage at
// carriage, verifying that the connected peer is the current effective user. It
// is deliberately the mux carriage only: the daemon's public session socket is
// never dialed here.
func dialLocalDaemonCarriage(ctx context.Context, carriage string) (daemonmux.RawFramedTransport, error) {
	return ipc.DialMuxContext(ctx, carriage)
}

// dialBrokerLocalDaemon dials the daemon published on this machine for one
// resolved local target. carriage is the private daemonmux carriage of the
// broker-owned local route: that route is its only source, and this function
// never derives, guesses, or accepts a carriage of its own.
//
// A registered endpoint is refused even when it is provisioned on a local Unix
// carriage: only the broker-owned local authority names the daemon this
// process can start, so a configured host can never be spawned by mistake.
func dialBrokerLocalDaemon(ctx context.Context, carriage string, target ports.BrokerDialTarget) (daemonmux.RawFramedTransport, error) {
	if !target.Fence.Local {
		return nil, localDaemonStartRefused("the target is not the broker-owned local daemon")
	}
	return dialBrokerDaemonCarriage(ctx, carriage, target)
}

// dialBrokerDaemonCarriage is the shared carriage dial and start. The target's
// start authorization decides the whole behavior. ExistingOnly performs exactly
// one carriage dial. StartIfNeeded performs the same dial first, returns the
// daemon it found, and only starts a daemon when the dial proves absence and the
// resolved policy authorizes launching; otherwise it reports the refusal without
// touching any process.
//
// A start is only ever possible for the one daemon this process can actually
// produce: the launcher re-executes this binary, which binds its private
// daemonmux carriage at daemonmux.SocketPath(ipc.SocketDir()) and holds its
// lifecycle and spawn locks in that same runtime directory. A carriage that is
// anything else is dialed and, if absent, reported — never replaced by a daemon
// this process did not start.
func dialBrokerDaemonCarriage(ctx context.Context, carriage string, target ports.BrokerDialTarget) (daemonmux.RawFramedTransport, error) {
	if err := target.Validate(); err != nil {
		return nil, ports.BrokerError{Code: ports.BrokerErrorIncompatible, Cause: err}
	}
	if err := validateDaemonCarriage(carriage); err != nil {
		return nil, err
	}
	starter := localDaemonStarter
	if starter.dial == nil || starter.spawn == nil {
		return nil, ports.BrokerError{Code: ports.BrokerErrorIncompatible, Cause: errors.New("vev: daemon starter is incomplete")}
	}
	raw, err := starter.dial(ctx, carriage)
	if err == nil {
		return raw, nil
	}
	if target.StartMode == ports.BrokerDaemonExistingOnly {
		// An existing-only acquisition is one dial: no lifecycle probe, no spawn
		// lock, no process, and no fallback of any kind.
		if daemonCarriageAbsence(err) {
			return nil, ports.BrokerError{Code: ports.BrokerErrorNoDaemon, Cause: err}
		}
		return nil, err
	}
	// A refusal that spawning could never repair is reported unchanged: a
	// cancelled attempt, a foreign path, a rejected peer, or a permission
	// failure is not daemon absence.
	if !daemonCarriageAbsence(err) {
		return nil, err
	}
	if err := validateDaemonCarriageStart(carriage, starter); err != nil {
		return nil, err
	}
	if err := daemonStartRefusal(target); err != nil {
		return nil, err
	}
	published := starter.effectivePublished()
	return ensureMuxDaemon(ctx, filepath.Dir(published), carriage, starter.dial, starter.spawn, starter.effectiveBackoff())
}

// validateDaemonCarriage refuses a carriage that cannot be a provisioned private
// daemonmux endpoint: it must be an absolute, cleaned path. The daemon launcher
// re-executes this binary to start one specific daemon, so a relative, unclean,
// or empty path can never trigger a spawn.
func validateDaemonCarriage(carriage string) error {
	if carriage == "" {
		return ports.BrokerError{Code: ports.BrokerErrorIncompatible, Cause: errors.New("vev: daemon carriage path is empty")}
	}
	if !filepath.IsAbs(carriage) || filepath.Clean(carriage) != carriage {
		return ports.BrokerError{
			Code:  ports.BrokerErrorIncompatible,
			Cause: fmt.Errorf("vev: daemon carriage %q is not an absolute cleaned path", carriage),
		}
	}
	return nil
}

// validateDaemonCarriageStart refuses starting a daemon for any carriage other
// than the one the launched daemon will actually publish. The launcher can only
// ever produce the daemon bound to that exact private endpoint, so starting one
// for a foreign carriage would spawn a process the caller could never reach.
func validateDaemonCarriageStart(carriage string, starter brokerDaemonStarter) error {
	if want := starter.effectivePublished(); carriage != want {
		return ports.BrokerError{
			Code:  ports.BrokerErrorIncompatible,
			Cause: fmt.Errorf("vev: refusing to start a daemon for carriage %q: this process publishes %q", carriage, want),
		}
	}
	return nil
}

// daemonStartRefusal refuses a start-if-needed acquisition that must never reach
// the spawn mechanics. The condition is checked before the election, so a
// refused start creates no lock and launches no process.
//
// The mode is an authorization, never an obligation, and it can only narrow what
// the resolved policy already permits. A policy that does not name the broker's
// launch authority therefore refuses the start outright, which is exactly how a
// provisioned configuration that forbids launching stays authoritative.
func daemonStartRefusal(target ports.BrokerDialTarget) error {
	if target.StartMode != ports.BrokerDaemonStartIfNeeded {
		return localDaemonStartRefused("the acquisition does not authorize starting a daemon")
	}
	if target.Policy.Launch != brokerLaunchAuthority {
		return localDaemonStartRefused("the resolved policy does not authorize launching")
	}
	return nil
}

// localDaemonStartRefused reports a refusal to start a daemon as a typed broker
// policy conflict. The reason is local diagnostic authority; it is never a
// message the daemon produced.
func localDaemonStartRefused(reason string) error {
	return ports.BrokerError{
		Code:  ports.BrokerErrorConflictingPolicy,
		Cause: fmt.Errorf("vev: refusing to start the daemon: %s", reason),
	}
}

// daemonCarriageAbsence reports whether a failed carriage dial means the daemon
// is simply not there, so starting one is the only remaining option. It is
// deliberately fail-closed: a cancelled attempt, an invalid or foreign path, a
// rejected peer, a permission failure, and every unclassified error report
// false, so only proven absence can authorize a spawn.
func daemonCarriageAbsence(err error) bool {
	switch {
	case err == nil:
		return false
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return false
	case errors.Is(err, ipc.ErrMuxPath), errors.Is(err, ipc.ErrMuxForeignPath),
		errors.Is(err, ipc.ErrMuxPeerRejected), errors.Is(err, ipc.ErrMuxUnsupported):
		return false
	case errors.Is(err, os.ErrPermission):
		return false
	case errors.Is(err, os.ErrNotExist), errors.Is(err, syscall.ENOENT),
		errors.Is(err, syscall.ECONNREFUSED), errors.Is(err, syscall.EWOULDBLOCK):
		return true
	default:
		return false
	}
}

// brokerDaemonStartArgvFlag renders the closed start authorization as the exact
// helper argument value. An unknown mode is refused, never defaulted: a helper
// that cannot read its authorization must not run with a permissive one.
func brokerDaemonStartArgvFlag(mode ports.BrokerDaemonStartMode) (string, error) {
	switch mode {
	case ports.BrokerDaemonExistingOnly:
		return "existing-only", nil
	case ports.BrokerDaemonStartIfNeeded:
		return "if-needed", nil
	default:
		return "", fmt.Errorf("vev: invalid broker daemon start mode %s", mode)
	}
}

// parseBrokerDaemonStartArgvFlag is the strict inverse of
// brokerDaemonStartArgvFlag. An unknown, empty, or differently spelled value is
// refused; a helper never maps an unrecognized word onto a start authorization.
func parseBrokerDaemonStartArgvFlag(value string) (ports.BrokerDaemonStartMode, error) {
	switch value {
	case "existing-only":
		return ports.BrokerDaemonExistingOnly, nil
	case "if-needed":
		return ports.BrokerDaemonStartIfNeeded, nil
	default:
		return 0, fmt.Errorf("vev: unknown daemon start mode %q (want existing-only or if-needed)", value)
	}
}

// helperCarriageTarget resolves the dial target for the private daemonmux
// carriage a remote-side mux helper bridges to, strictly from that helper's own
// broker-owned configuration. A helper owns no request and no client authority:
// the target is the one provisioned entry whose route is exactly the carriage
// being bridged, so the helper can only ever narrow what its own configuration
// already permits.
//
// The provisioned local binding wins when it names the carriage, because the
// local binding is the broker's own daemon authority. Otherwise the single
// registration that provisions this carriage supplies the fence, identity, and
// policy. A configuration that provisions neither yields no target, and a
// start-if-needed request is then refused rather than widened.
func helperCarriageTarget(config *brokerconfig.Config, route brokerconfig.Route) (ports.BrokerDialTarget, error) {
	if binding, ok := config.LocalBinding(); ok && binding.Route.Address() == route.Address() {
		return ports.BrokerDialTarget{
			Fence:            ports.BrokerEndpointFence{Local: true},
			Address:          binding.Route.Address(),
			Policy:           binding.Policy,
			ExpectedIdentity: ports.BrokerExpectedIdentity{Identity: binding.Identity, Bound: binding.Identity != ""},
		}, nil
	}
	for _, endpoint := range config.Endpoints() {
		registration, ok := config.Registration(endpoint)
		if !ok || registration.Route.Address() != route.Address() {
			continue
		}
		return ports.BrokerDialTarget{
			Fence:            ports.BrokerEndpointFence{Registration: registration.Registration},
			Address:          registration.Route.Address(),
			Policy:           registration.Policy,
			ExpectedIdentity: ports.BrokerExpectedIdentity{Identity: registration.Identity, Bound: registration.Identity != ""},
		}, nil
	}
	return ports.BrokerDialTarget{}, localDaemonStartRefused("the helper configuration provisions no daemon authority for its mux carriage")
}

// dialMuxHelperCarriage dials the private daemonmux carriage one remote-side mux
// helper bridges to, under the start authorization the broker propagated. The
// helper applies the same local election the broker does, from its own
// broker-owned configuration, and it never starts a broker, an observer, or a
// recursive helper: an existing-only mode is one carriage dial, and a
// start-if-needed mode is the shared daemon start gated by the helper's own
// provisioned policy and identity.
func dialMuxHelperCarriage(ctx context.Context, config *brokerconfig.Config, mode ports.BrokerDaemonStartMode) (daemonmux.RawFramedTransport, error) {
	if config == nil {
		return nil, errors.New("vev: broker mux helper requires a configuration")
	}
	if err := mode.Validate(); err != nil {
		return nil, err
	}
	route, err := config.LocalMuxRoute()
	if err != nil {
		return nil, err
	}
	target, err := helperCarriageTarget(config, route)
	if err != nil {
		return nil, err
	}
	target.StartMode = mode
	if err := target.Validate(); err != nil {
		return nil, ports.BrokerError{Code: ports.BrokerErrorIncompatible, Cause: err}
	}
	// Only a policy that names the launch token may start the daemon. A carriage
	// provisioned under a non-launching policy refuses the start outright. The
	// existing-only mode never reaches this check: dialBrokerDaemonCarriage
	// returns its single dial's result immediately.
	if mode == ports.BrokerDaemonStartIfNeeded {
		if err := daemonStartRefusal(target); err != nil {
			return nil, err
		}
	}
	return dialBrokerDaemonCarriage(ctx, route.Path(), target)
}
