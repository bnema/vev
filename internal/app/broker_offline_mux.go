package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/bnema/vev/internal/adapters/brokerconfig"
	"github.com/bnema/vev/internal/adapters/daemonmux"
	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/adapters/quic"
	"github.com/bnema/vev/internal/adapters/sshstdio"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/wire"
)

// Broker mux remote helper composition (Plan 001 P3.4 slice E).
//
// The offline broker reaches a remote daemon's private daemonmux carriage
// through one of three explicitly provisioned routes: a local Unix mux socket,
// an SSH stdio child that runs a hidden mux helper, or an SSH-authenticated
// QUIC bootstrap that pins a freshly minted ephemeral certificate. This file
// owns both halves.
//
// The broker side is a transport-selecting daemonmux.EndpointConnector. In
// production it rechecks remote targets against committed registry records;
// sandbox fixtures retain their immutable configured routes and trust inputs. It
// never authenticates the carriage itself: the daemonmux physical preamble
// verifies the daemon identity, incarnation, and policy the resolver already
// pinned. One caller-supplied setup context bounds bootstrap, pinned dial, and
// the daemonmux handshake together; a successful physical connection detaches
// from that context and is owned by the pool.
//
// The remote side is three hidden helpers, excluded from public help:
//
//   - `_broker-mux-stdio --offline-root ABS` bridges its own stdio to the
//     single provisioned private Unix daemonmux carriage.
//   - `_broker-mux-quic-bootstrap --offline-root ABS` starts one detached
//     `_broker-mux-quic-proxy`, forwards its single readiness line, and exits.
//   - `_broker-mux-quic-proxy --offline-root ABS` mints one ephemeral QUIC
//     server, admits exactly one authenticated carriage, and bridges it to the
//     same private Unix daemonmux carriage.
//
// Each helper also accepts --production instead of --offline-root. That scope
// derives the private carriage from this user's production runtime. Helpers
// never start a broker or observer. Only an explicit if-needed authorization
// permits the shared daemon starter; existing-only observation never spawns.
// The daemon on the other side owns its identity and incarnation.
// The one-time QUIC token and nonce are minted per physical connection, travel
// only inside the SSH channel and the encrypted stream, and never reach an
// address, a log, or an error. SSH host-key verification, noninteractive
// authentication, and the absence of a remote PTY are preserved: the helpers
// only add explicit trust inputs that narrow verification, never disable it.

const (
	// brokerMuxStdioCommand is the hidden remote SSH stdio mux bridge.
	brokerMuxStdioCommand = "_broker-mux-stdio"
	// brokerMuxQUICBootstrapCommand is the hidden remote QUIC mux bootstrap.
	brokerMuxQUICBootstrapCommand = "_broker-mux-quic-bootstrap"
	// brokerMuxQUICProxyCommand is the hidden detached remote QUIC mux proxy.
	brokerMuxQUICProxyCommand = "_broker-mux-quic-proxy"
)

// brokerMuxSetupTimeout bounds each side's bootstrap readiness wait. On the
// broker side it caps the wait for the local ssh bootstrap child's readiness
// line together with that child's kill/reap cleanup; on the remote side it caps
// the bootstrap command's wait for the detached proxy's readiness line. It is an
// additional backstop only: an earlier caller context deadline always stays
// authoritative, because context.WithTimeout keeps the sooner deadline, so the
// independent bound can never extend a shorter caller budget.
const brokerMuxSetupTimeout = 30 * time.Second

// brokerMuxBootstrapReadyTimeout is the effective broker-side bootstrap
// readiness budget. Production always uses brokerMuxSetupTimeout; a test
// narrows it so a stalled helper that never prints readiness is exercised
// deterministically without waiting out the production bound.
var brokerMuxBootstrapReadyTimeout = brokerMuxSetupTimeout

// brokerMuxQUICDialTimeout bounds the pinned QUIC dial. The setup context is
// still authoritative; this narrows the dial itself.
const brokerMuxQUICDialTimeout = 15 * time.Second

// brokerMuxWaitDelay bounds how long a bootstrap Wait blocks on a captured-
// stderr copy goroutine after the direct child exits. A remote helper that
// leaves a grandchild holding the inherited stderr pipe can therefore never pin
// setup shutdown past this delay; the direct child is still killed and reaped.
const brokerMuxWaitDelay = 2 * time.Second

// brokerDaemonStartArg is the closed helper flag that carries one daemon-start
// authorization across the SSH process boundary. It is added by the broker to
// the provisioned remote argv and parsed strictly by the helper; the value is
// brokerDaemonStartArgvFlag's exact spelling, so the two sides can never drift.
const brokerDaemonStartArg = "--daemon-start"

// brokerMuxOptions is the parsed hidden mux helper invocation.
type brokerMuxOptions struct {
	production  bool
	offlineRoot string
	// startMode is the daemon-start authorization the broker propagated. An
	// absent flag is the safe default: the helper observes only what is already
	// running and never starts a daemon, a broker, or a recursive helper.
	startMode ports.BrokerDaemonStartMode
}

// parseBrokerMuxArgs strictly parses `<hidden> (--offline-root ABS | --production)
// [--daemon-start existing-only|if-needed]`. Unknown or duplicate flags, a
// missing or unknown value, and positionals are refused, exactly like the
// foreground sandbox entry point. An absent --daemon-start defaults to
// existing-only, and an unrecognized value is never mapped onto a permissive
// authorization.
func parseBrokerMuxArgs(name string, kind cmdKind, args []string) (command, error) {
	options := brokerMuxOptions{startMode: ports.BrokerDaemonExistingOnly}
	rootSet, startSet := false, false
	for index := 0; index < len(args); {
		switch args[index] {
		case "--production":
			if options.production || rootSet {
				return command{}, usagef("`%s` received duplicate or conflicting scope", name)
			}
			options.production = true
			index++
		case "--offline-root":
			if options.production {
				return command{}, usagef("`%s` received conflicting scope", name)
			}
			if rootSet {
				return command{}, usagef("`%s` received duplicate --offline-root", name)
			}
			if index+1 >= len(args) || args[index+1] == "" {
				return command{}, usagef("`--offline-root` requires a path")
			}
			options.offlineRoot = args[index+1]
			rootSet = true
			index += 2
		case brokerDaemonStartArg:
			if startSet {
				return command{}, usagef("`%s` received duplicate %s", name, brokerDaemonStartArg)
			}
			if index+1 >= len(args) {
				return command{}, usagef("`%s` requires a value", brokerDaemonStartArg)
			}
			mode, err := parseBrokerDaemonStartArgvFlag(args[index+1])
			if err != nil {
				return command{}, usagef("`%s`: %s", name, err)
			}
			options.startMode = mode
			startSet = true
			index += 2
		default:
			if strings.HasPrefix(args[index], "-") {
				return command{}, usagef("unknown flag %q for `%s`", args[index], name)
			}
			return command{}, usagef("`%s` does not accept positional arguments", name)
		}
	}
	if !rootSet && !options.production {
		return command{}, usagef("`%s` requires --offline-root or --production", name)
	}
	return command{kind: kind, brokerMux: options}, nil
}

// loadMuxHelperConfig validates one offline root and loads its strict sandbox
// configuration. A helper never creates sandbox directories: the root and the
// config marker must already exist, so a helper can never provision state.
func loadMuxHelperConfig(offlineRoot string) (*brokerconfig.Config, error) {
	layout, err := offlineLayout(offlineRoot)
	if err != nil {
		return nil, err
	}
	return brokerconfig.Load(layout)
}

// brokerMuxConnector builds the transport-selecting daemonmux endpoint
// connector over one immutable configuration. The dial function receives the
// whole resolved target the pool will authenticate against and refuses any
// address the configuration did not produce, so a request can never select a
// transport, an address, or a start authorization of its own. It re-checks that
// the route's provisioned authority still matches the resolved policy and fence
// before dialing, so the transport is only ever reached under the authority the
// resolver published.
func brokerMuxConnector(config *brokerconfig.Config, log *slog.Logger) (*daemonmux.EndpointConnector, error) {
	if config == nil {
		return nil, errors.New("vev: broker mux connector requires a configuration")
	}
	dial := func(ctx context.Context, target ports.BrokerDialTarget) (daemonmux.RawFramedTransport, error) {
		route, ok := config.RouteByAddress(target.Address)
		if !ok {
			return nil, fmt.Errorf("vev: broker mux: unknown route address")
		}
		if err := brokerRouteAuthorityMatches(config, target); err != nil {
			return nil, err
		}
		return dialBrokerRoute(ctx, route, target, log)
	}
	return daemonmux.NewEndpointConnector(dial, daemonmux.DefaultMuxCeilings())
}

// brokerDynamicMuxConnector keeps the configured local carriage, but remote
// targets must still match committed registry authority at dial time.
func brokerDynamicMuxConnector(config *brokerconfig.Config, routes *brokerRoutes, log *slog.Logger) (*daemonmux.EndpointConnector, error) {
	return daemonmux.NewEndpointConnector(func(ctx context.Context, target ports.BrokerDialTarget) (daemonmux.RawFramedTransport, error) {
		if !target.Fence.Local {
			route, err := routes.remoteRoute(ctx, target)
			if err != nil {
				return nil, err
			}
			return dialBrokerRoute(ctx, route, target, log)
		}
		if err := brokerRouteAuthorityMatches(config, target); err != nil {
			return nil, err
		}
		route, ok := config.RouteByAddress(target.Address)
		if !ok {
			return nil, errors.New("vev: unknown local route")
		}
		return dialBrokerRoute(ctx, route, target, log)
	}, daemonmux.DefaultMuxCeilings())
}

// brokerRouteAuthorityMatches re-verifies that the route the opaque address
// selected is still the one the resolved target's authority names: the same
// fence, and a policy the provisioned entry accepts. The pool and the daemonmux
// preamble already check this; re-checking here keeps the dial decision and the
// authentication decision on one authority instead of two that could drift.
func brokerRouteAuthorityMatches(config *brokerconfig.Config, target ports.BrokerDialTarget) error {
	if target.Fence.Local {
		binding, ok := config.LocalBinding()
		if !ok || binding.Route.Address() != target.Address || !binding.Policy.Compatible(target.Policy) {
			return ports.BrokerError{Code: ports.BrokerErrorConflictingPolicy, Cause: errors.New("vev: broker mux: local route authority does not match the resolved target")}
		}
		return nil
	}
	for _, endpoint := range config.Endpoints() {
		registration, ok := config.Registration(endpoint)
		if !ok || registration.Route.Address() != target.Address {
			continue
		}
		if registration.Registration.Equal(target.Fence.Registration) && registration.Policy.Compatible(target.Policy) {
			return nil
		}
	}
	return ports.BrokerError{Code: ports.BrokerErrorConflictingPolicy, Cause: errors.New("vev: broker mux: route authority does not match the resolved target")}
}

// dialBrokerRoute dials one provisioned route under one resolved target as an
// already-authenticated raw framed carriage. Authentication, identity, and
// policy remain the daemonmux connector's concern; this function selects the
// transport and forwards the whole target, so a mux stdio/QUIC helper receives
// the start authorization before it decides anything about the remote daemon.
//
// A Unix route is dialed directly by this process. The configuration's own
// local binding route is the machine daemon the broker may start, so it goes
// through the broker-owned local daemon start; every other Unix route is a
// provisioned endpoint carriage that the broker only ever dials.
func dialBrokerRoute(ctx context.Context, route brokerconfig.Route, target ports.BrokerDialTarget, log *slog.Logger) (daemonmux.RawFramedTransport, error) {
	if !target.Fence.Local && log != nil {
		// One line per remote physical bootstrap: warm reuse is verified by
		// the absence of this event, not by timing.
		log.Info("broker_remote_dial", "endpoint", target.Fence.Registration.Endpoint, "route", string(route.Kind()))
	}
	switch route.Kind() {
	case brokerconfig.RouteUnix:
		if target.Fence.Local {
			// The broker-owned local fence is the machine daemon this process may
			// start; it goes through the shared daemon start so the authorization
			// reaches the election. Every other Unix carriage is only dialed.
			return dialBrokerLocalDaemon(ctx, route.Path(), target)
		}
		return ipc.DialMuxContext(ctx, route.Path())
	case brokerconfig.RouteSSHStdio:
		spec, err := sshMuxCommandSpec(route, target.StartMode)
		if err != nil {
			return nil, err
		}
		return sshstdio.DialMuxContext(ctx, spec, log)
	case brokerconfig.RouteSSHQUIC:
		return dialBrokerQUICRoute(ctx, route, target.StartMode, log)
	default:
		return nil, fmt.Errorf("vev: broker mux: unsupported route kind %q", route.Kind())
	}
}

// sshMuxCommandSpec builds the local ssh argv for one provisioned route and one
// start authorization. It reuses the mux carriage's own command construction
// (the option terminator, the POSIX-quoted remote argv, and `-T` for no remote
// PTY), splices in only the explicit trust inputs the operator provisioned, and
// appends the closed `--daemon-start existing-only|if-needed` word so the remote
// helper reads the authorization the broker propagated instead of defaulting.
// It never disables host-key verification, never forces a non-interactive auth
// bypass, and never requests a PTY.
func sshMuxCommandSpec(route brokerconfig.Route, mode ports.BrokerDaemonStartMode) (sshstdio.CommandSpec, error) {
	flag, err := brokerDaemonStartArgvFlag(mode)
	if err != nil {
		return sshstdio.CommandSpec{}, err
	}
	argv := route.Argv()
	for _, word := range argv {
		if word == brokerDaemonStartArg {
			return sshstdio.CommandSpec{}, fmt.Errorf("vev: broker mux route provisions its own %s", brokerDaemonStartArg)
		}
	}
	argv = append(argv, brokerDaemonStartArg, flag)
	spec := sshstdio.BuildCommandForMux(route.Target(), argv...)
	options := sshTrustOptions(route)
	if len(options) == 0 || len(spec.Args) < 2 {
		return spec, nil
	}
	// BuildCommandForMux returns ["-T", "--", target, remote]; insert options
	// after `-T` so they stay ssh options and the option terminator still
	// protects the target.
	args := make([]string, 0, len(spec.Args)+len(options))
	args = append(args, spec.Args[0])
	args = append(args, options...)
	args = append(args, spec.Args[1:]...)
	return sshstdio.CommandSpec{Path: spec.Path, Args: args}, nil
}

// sshTrustOptions renders the provisioned trust inputs as ssh options. A
// connect timeout only narrows the attempt; a known-hosts file names the
// verification source. So the option carries a single bounded, whitespace-free
// value and can never inject a second option.
func sshTrustOptions(route brokerconfig.Route) []string {
	var options []string
	if timeout := route.ConnectTimeout(); timeout > 0 {
		seconds := int(timeout / time.Second)
		if seconds < 1 {
			seconds = 1
		}
		options = append(options, "-o", fmt.Sprintf("ConnectTimeout=%d", seconds))
	}
	if knownHosts := route.KnownHostsFile(); knownHosts != "" {
		options = append(options, "-o", "UserKnownHostsFile="+knownHosts)
	}
	return options
}

// dialBrokerQUICRoute runs one SSH-authenticated QUIC bootstrap and returns the
// pinned, authenticated raw bounded carriage. The local ssh child starts the
// remote `_broker-mux-quic-bootstrap`, prints exactly one readiness line on
// stdout, and exits; the detached remote proxy serves one carriage. A refused
// pin, token, or nonce closes the carriage and hands nothing out. Captured
// stderr is sanitized and never part of the returned error.
//
// The caller's setup context bounds the exchange, and brokerMuxSetupTimeout is
// an independent backstop: an earlier caller deadline stays authoritative,
// while a bootstrap that never prints readiness is still killed and reaped
// within the bound and can never pin pool setup. The captured stderr sink is
// read only after that kill and reap, and the sink is itself synchronized, so
// the os/exec copy goroutine can never race the diagnostic.
func dialBrokerQUICRoute(ctx context.Context, route brokerconfig.Route, mode ports.BrokerDaemonStartMode, log *slog.Logger) (wire.BoundedTransport, error) {
	spec, err := sshMuxCommandSpec(route, mode)
	if err != nil {
		return nil, err
	}
	stderr := sshstdio.NewDiagnosticSink(0)
	cmd := exec.CommandContext(ctx, spec.Path, spec.Args...)
	cmd.Stderr = stderr
	// A captured stderr is a pipe with a copy goroutine; WaitDelay ensures a
	// grandchild that inherits the write end cannot block Wait past the delay
	// once the direct child is gone.
	cmd.WaitDelay = brokerMuxWaitDelay
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, brokerQUICUnavailable("bootstrap stdout", err)
	}
	if err := cmd.Start(); err != nil {
		_ = stdout.Close()
		return nil, brokerQUICUnavailable("start bootstrap", err)
	}
	// reapBootstrap kills the local bootstrap and starts a bounded join, exactly
	// once. Every error path reaps before it reads the captured stderr sink; the
	// sink is safe to read whether or not that bounded join has finished, because
	// it synchronizes its own reads and writes. The remote proxy is detached
	// in its own session, so killing the local ssh child never ends it.
	reaped := false
	reapBootstrap := func() {
		if reaped {
			return
		}
		reaped = true
		reapBootstrapProcess(cmd)
	}
	defer func() {
		reapBootstrap()
		_ = stdout.Close()
	}()

	// The readiness wait is bounded independently of the caller context, so a
	// bootstrap that never prints readiness cannot pin pool setup. An earlier
	// caller deadline still wins: WithTimeout keeps the sooner deadline.
	readyCtx, cancelReadiness := context.WithTimeout(ctx, brokerMuxBootstrapReadyTimeout)
	readiness, err := quic.ReadReadiness(readyCtx, stdout)
	cancelReadiness()
	if err != nil {
		reapBootstrap()
		logBrokerBootstrapStderr(log, stderr)
		return nil, brokerQUICUnavailable("read bootstrap readiness", err)
	}
	// The bootstrap channel served its purpose: the detached remote proxy owns
	// the carriage lifetime. Reap the short-lived local child without killing
	// it, so SIGKILL never races the proxy detach. A bounded I/O drain that the
	// child's exit outlived (ErrWaitDelay) is not a bootstrap failure.
	if waitErr := cmd.Wait(); waitErr != nil && !errors.Is(waitErr, exec.ErrWaitDelay) {
		reaped = true
		logBrokerBootstrapStderr(log, stderr)
		return nil, brokerQUICUnavailable("reap bootstrap", waitErr)
	}
	reaped = true

	// Compose the dial address from the configured ssh target host plus the
	// host-independent readiness port: a remote loopback address is never
	// dialed. The one-time token and nonce travel only inside the encrypted
	// stream after the pinned handshake.
	addr, err := brokerQUICPeerAddr(route.Target(), readiness.Port)
	if err != nil {
		return nil, brokerQUICUnavailable("resolve QUIC peer", err)
	}
	transport, err := quic.DialMuxContext(ctx, addr, readiness, quic.Config{}, brokerMuxQUICDialTimeout)
	if err != nil {
		return nil, brokerQUICUnavailable("dial bootstrap QUIC", err)
	}
	return transport, nil
}

// brokerQUICPeerAddr composes the dial address from the ssh target host and the
// bootstrap-published port, stripping a user prefix, an explicit port, and
// bracketed IPv6 literals to the bare host.
func brokerQUICPeerAddr(target string, port int) (string, error) {
	if port <= 0 || port > 65535 {
		return "", fmt.Errorf("invalid bootstrap port %d", port)
	}
	host := target
	if at := strings.LastIndexByte(host, '@'); at >= 0 {
		host = host[at+1:]
	}
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.TrimPrefix(strings.TrimSuffix(host, "]"), "[")
	if host == "" {
		return "", errors.New("empty bootstrap host")
	}
	return net.JoinHostPort(host, strconv.Itoa(port)), nil
}

// logBrokerBootstrapStderr records a bounded, sanitized bootstrap diagnostic.
// The readiness secret is never on stderr, and no raw remote text is surfaced.
// The join performed by the caller is bounded, so the os/exec stderr copy
// goroutine may still be writing when this reads the sink; the diagnostic is
// nonetheless consistent because DiagnosticSink synchronizes its own reads and
// writes and returns a bounded, sanitized snapshot rather than relying on the
// join having quiesced the sink.
func logBrokerBootstrapStderr(log *slog.Logger, sink *sshstdio.DiagnosticSink) {
	if log == nil || sink == nil {
		return
	}
	diagnostic := strings.TrimSpace(sink.String())
	if diagnostic == "" {
		return
	}
	log.Warn("broker_mux_bootstrap_stderr", "diagnostic", diagnostic)
}

// reapBootstrapProcess kills one bootstrap child and starts a bounded join of
// its wait. The join is bounded by brokerMuxSetupTimeout so a helper that
// leaves a grandchild holding the captured stderr pipe cannot pin cleanup, and
// exec.Cmd.WaitDelay narrows the wait much further once the direct child is
// gone. When the bound expires the wait goroutine may still be draining the
// captured-stderr copy, so a caller that reads the sink afterwards relies on
// the sink's own synchronization, not on the join having finished. The direct
// child is always killed, so no bootstrap process is ever leaked.
func reapBootstrapProcess(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Kill()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = cmd.Wait()
	}()
	timer := time.NewTimer(brokerMuxSetupTimeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
	}
}

// brokerQUICUnavailable wraps one bootstrap or pinned-dial failure. The
// readiness record is deliberately absent: a returned error never carries the
// one-time token, the nonce, or remote stderr.
func brokerQUICUnavailable(action string, err error) error {
	if err == nil {
		err = errors.New("unavailable")
	}
	return fmt.Errorf("vev: broker mux QUIC unavailable: %s: %w", action, err)
}

// runBrokerMuxStdioCommand bridges the process' own stdio to the single
// provisioned private Unix daemonmux carriage, under the daemon-start
// authorization the broker propagated.
func runBrokerMuxStdioCommand(ctx context.Context, options brokerMuxOptions) error {
	raw, err := dialBrokerMuxHelper(ctx, options)
	if err != nil {
		return fmt.Errorf("vev: broker mux stdio: dial daemonmux carriage: %w", err)
	}
	bounded, ok := raw.(wire.BoundedTransport)
	if !ok {
		_ = raw.Close()
		return errors.New("vev: broker mux stdio: carriage is not a bounded transport")
	}
	return bridgeMuxTransports(ctx, sshstdio.NewStdioTransport(), bounded)
}

// runBrokerMuxQUICBootstrapCommand starts one detached `_broker-mux-quic-proxy`,
// forwards its single readiness line on this process' stdout, and exits. The
// proxy owns the carriage lifetime in its own session and survives this short-
// lived bootstrap, exactly like the ordinary QUIC bootstrap.
func runBrokerMuxQUICBootstrapCommand(ctx context.Context, options brokerMuxOptions) error {
	// Validate the sandbox before spawning anything, so a bad root or config
	// fails without a readiness line and without a child.
	if !options.production {
		if _, err := loadMuxHelperConfig(options.offlineRoot); err != nil {
			return err
		}
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		return err
	}
	defer func() { _ = reader.Close() }()
	devNull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		_ = writer.Close()
		return err
	}
	defer func() { _ = devNull.Close() }()

	args := []string{brokerMuxQUICProxyCommand}
	if options.production {
		args = append(args, "--production")
	} else {
		args = append(args, "--offline-root", options.offlineRoot)
	}
	flag, err := brokerDaemonStartArgvFlag(options.startMode)
	if err != nil {
		_ = writer.Close()
		return err
	}
	args = append(args, brokerDaemonStartArg, flag)
	cmd := exec.CommandContext(ctx, exe, args...)
	// The proxy writes its readiness through the pipe and its diagnostics
	// through stderr; stdio is otherwise detached so the SSH bootstrap channel
	// can close while the detached proxy owns the carriage.
	cmd.Stdin = devNull
	cmd.Stdout = writer
	cmd.Stderr = devNull
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		_ = writer.Close()
		return err
	}
	_ = writer.Close()

	readyCtx, cancel := context.WithTimeout(ctx, brokerMuxSetupTimeout)
	defer cancel()
	readiness, err := quic.ReadReadiness(readyCtx, reader)
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return fmt.Errorf("vev: broker mux bootstrap readiness: %w", err)
	}
	line, err := quic.EncodeReadiness(readiness)
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return err
	}
	if _, err := fmt.Fprintf(os.Stdout, "%s\n", line); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return err
	}
	// Release the detached proxy: it is in its own session and outlives this
	// bootstrap. Reaping it here would block until the carriage ends.
	return cmd.Process.Release()
}

// runBrokerMuxQUICProxyCommand mints one ephemeral QUIC server, admits exactly
// one authenticated carriage, and bridges it to the single provisioned private
// Unix daemonmux carriage. It exits after its carriage ends, on credential
// expiry, or on setup failure, so it can never outlive its advertised
// credential or serve a second physical connection.
func runBrokerMuxQUICProxyCommand(ctx context.Context, options brokerMuxOptions) error {
	server, readiness, err := quic.NewServer()
	if err != nil {
		return err
	}
	defer func() { _ = server.Close() }()
	line, err := quic.EncodeReadiness(readiness)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(os.Stdout, "%s\n", line); err != nil {
		return err
	}
	transport, err := server.AcceptMux(ctx)
	if err != nil {
		return err
	}
	// The proxy owns its process: QUIC's orderly Close only FINs the stream and
	// defers the connection close, so returning (and exiting) right away would
	// drop both and leave the broker's preamble read parked until the QUIC idle
	// timeout. Waiting for the bounded teardown delivers the FIN, so a refused
	// existing-only dial (no daemon running) fails the broker's attempt at once.
	defer func() {
		_ = transport.Close()
		_ = waitGracefulTeardown(transport)
	}()
	raw, err := dialBrokerMuxHelper(ctx, options)
	if err != nil {
		return fmt.Errorf("vev: broker mux quic proxy: dial daemonmux carriage: %w", err)
	}
	bounded, ok := raw.(wire.BoundedTransport)
	if !ok {
		_ = raw.Close()
		return errors.New("vev: broker mux quic proxy: carriage is not a bounded transport")
	}
	return bridgeMuxTransports(ctx, transport, bounded)
}

// dialBrokerMuxHelper is a remote-side broker carriage helper, not a client
// daemon dial. Production authority is derived locally, never from SSH argv.
func dialBrokerMuxHelper(ctx context.Context, options brokerMuxOptions) (daemonmux.RawFramedTransport, error) {
	if !options.production {
		config, err := loadMuxHelperConfig(options.offlineRoot)
		if err != nil {
			return nil, err
		}
		return dialMuxHelperCarriage(ctx, config, options.startMode)
	}
	route, err := brokerconfig.RouteFromSpec(ports.BrokerRouteSpec{Kind: ports.BrokerRouteUnix, Path: daemonmux.SocketPath(ipc.SocketDir())})
	if err != nil {
		return nil, err
	}
	target := ports.BrokerDialTarget{Fence: ports.BrokerEndpointFence{Local: true}, Policy: localDaemonPolicy(), Address: route.Address(), StartMode: options.startMode}
	if err := target.Validate(); err != nil {
		return nil, err
	}
	return dialBrokerLocalDaemon(ctx, route.Path(), target)
}

// bridgeMuxTransports copies raw frames both ways until either carriage ends or
// ctx is done, then closes both. An orderly end of either direction (EOF) is a
// normal exit for a bridge owner that merely forwards bytes.
func bridgeMuxTransports(ctx context.Context, a, b wire.BoundedTransport) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()
	errCh := make(chan error, 2)
	go func() { errCh <- relayMux(ctx, a, b) }()
	go func() { errCh <- relayMux(ctx, b, a) }()
	select {
	case err := <-errCh:
		return orderlyRelay(err)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// relayMux copies one direction of raw bounded frames.
func relayMux(ctx context.Context, src, dst wire.BoundedTransport) error {
	for {
		envelope, err := src.RecvBounded(wire.AbsoluteEnvelopeLimit)
		if err != nil {
			return err
		}
		if err := dst.Send(envelope); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
}

// orderlyRelay treats EOF as a clean bridge end and passes every other outcome
// through unchanged.
func orderlyRelay(err error) error {
	if err == nil || errors.Is(err, io.EOF) {
		return nil
	}
	return err
}
