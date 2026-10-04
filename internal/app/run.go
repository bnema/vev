// Package app wires vev's runtime together via dependency injection. It is
// the single place where usecases (client, daemon) are connected to concrete
// adapters (ipc transport/listener, terminal); usecases themselves never
// import adapters.
package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"time"

	"github.com/bnema/vev/internal/adapters/clock"
	"github.com/bnema/vev/internal/adapters/config"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/logging"
	"github.com/bnema/vev/internal/platform"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/client"
)

// Build metadata. Defaults describe a plain `go build`; releases overwrite
// them via -ldflags "-X github.com/bnema/vev/internal/app.version=..." (and
// .commit / .date) — see .goreleaser.yaml.
var (
	version = "0.1.0-dev"
	commit  = "none"
	date    = "unknown"
)

// versionLine renders the --version output.
func versionLine() string {
	return fmt.Sprintf("vev %s (commit %s, built %s)", version, commit, date)
}

// Run is the entry point invoked by main. It parses args into a command and
// dispatches it.
func Run(args []string) error {
	if err := platform.ActivateDevelopmentEnvironment(); err != nil {
		return err
	}
	cmd, err := parseArgs(args)
	if err != nil {
		return err
	}
	return dispatch(context.Background(), cmd)
}

// dispatch runs the parsed command.
func dispatch(ctx context.Context, cmd command) error {
	switch cmd.kind {
	case kindHelp:
		fmt.Println(usageText)
		return nil
	case kindVersion:
		fmt.Println(versionLine())
		return nil
	case kindWebRenew:
		return renewWebToken(ctx)
	case kindWebDaemon:
		return launchWebDaemon(ctx, cmd.web)
	case kindWebServe:
		return runWebDaemon(ctx, cmd.web)
	case kindDaemon:
		return runDaemon()
	case kindDaemonLauncher:
		return runDaemonLauncher()
	case kindBrokerMuxStdio:
		return runBrokerMuxStdioCommand(ctx, cmd.brokerMux)
	case kindBrokerMuxQUICBootstrap:
		return runBrokerMuxQUICBootstrapCommand(ctx, cmd.brokerMux)
	case kindBrokerMuxQUICProxy:
		return runBrokerMuxQUICProxyCommand(ctx, cmd.brokerMux)
	case kindProductionBrokerServe:
		return runBrokerServeCommand(ctx, cmd.brokerServe)
	case kindProductionBrokerLauncher:
		return runProductionBrokerLauncherCommand(ctx)
	case kindBrokerReady:
		return runBrokerReady(ctx, cmd.brokerReady, os.Stdout)
	case kindDaemonStop:
		return requestDaemonStop(ctx)
	case kindUIRemoteCleanup:
		return runUIRemoteCleanup(ctx)
	case kindUIDriver:
		return runUIDriver(ctx, cmd.uiDriver)
	case kindList:
		return runList(ctx, cmd)
	case kindHost:
		return runHostCommand(ctx, cmd, defaultRemoteHostDeps())
	case kindKill:
		if cmd.killAll {
			return runKillAll(ctx, os.Stdout)
		}
		return runKill(ctx, cmd.name, cmd.killSessions)
	case kindCmd:
		return runCmd(ctx, cmd.cmd)
	case kindEnv:
		return runEnv(ctx, cmd.env)
	case kindAttach:
		return runAttachWithOptions(ctx, cmd.intent, cmd.name, cmd.remoteTarget, cmd.ui)
	default:
		return usagef("unhandled command")
	}
}

func configureLogging(component logging.Component, rotateAtRuntime bool) (*slog.Logger, io.Closer, error) {
	return logging.Setup(logging.Config{
		Dir:             platform.StateDir(),
		Component:       component,
		Level:           logging.EnvLevel(),
		MaxBytes:        logging.DefaultMaxBytes,
		RotateAtRuntime: rotateAtRuntime,
	})
}

func logConfigWarnings(log *slog.Logger, warnings []domain.Warning) {
	for _, warning := range warnings {
		if warning.Line > 0 {
			log.Warn("config warning", "line", warning.Line, "msg", warning.Msg)
			continue
		}
		log.Warn("config warning", "msg", warning.Msg)
	}
}

func loadConfigOrDefaults(log *slog.Logger, path string) domain.Config {
	cfg, warnings, err := config.Load(path)
	if err != nil {
		if log != nil {
			log.Warn("loading config failed; using defaults", "path", path, "err", err)
		}
		cfg = domain.Defaults()
	}
	if log != nil {
		logConfigWarnings(log, warnings)
	}
	return cfg
}

const (
	uiRemoteCleanupCommand = "_ui-cleanup"
	// daemonStopCommand is the hidden, environment-scoped graceful stop of
	// this environment's daemon, for harnesses and scripts. Users run
	// `kill --all` instead.
	daemonStopCommand = "_daemon-stop"
)

const daemonStopTimeout = 2 * time.Second

var errDaemonNotRunning = errors.New("no daemon running")

func runUIRemoteCleanup(ctx context.Context) error {
	root := os.Getenv("VEV_ENV_ROOT")
	if !filepath.IsAbs(root) || filepath.Clean(root) == string(filepath.Separator) {
		return errors.New("remote UI cleanup requires a private absolute launch root")
	}
	if runtimeDir := os.Getenv("XDG_RUNTIME_DIR"); runtimeDir == "" || runtimeDir != filepath.Join(root, "runtime") {
		return errors.New("remote UI cleanup requires the launch runtime directory")
	}
	if err := requestDaemonStop(ctx); err != nil && !errors.Is(err, errDaemonNotRunning) {
		return err
	}
	return nil
}

// connectDaemonStopBroker is the broker connection seam for requestDaemonStop.
var connectDaemonStopBroker = connectExistingBroker

func requestDaemonStop(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, daemonStopTimeout)
	defer cancel()
	service, err := connectDaemonStopBroker(ctx)
	if err != nil {
		if errors.Is(err, errBrokerAbsent) {
			return stopDaemonWithoutBroker(ctx)
		}
		return unreachableBrokerError(err)
	}
	defer func() { _ = service.Close() }()
	operations, err := client.NewBrokerOperations(service, clock.New())
	if err != nil {
		return err
	}
	result, err := operations.StopDaemon(ctx, localBrokerOperationRoute(service.Snapshot()))
	if err != nil {
		return err
	}
	return brokerKillResultError(result)
}

func unreachableBrokerError(err error) error {
	return &exitCoded{code: 3, err: fmt.Errorf("%w: %v", errDaemonUnreachable, err)}
}

// stopDaemonWithoutBroker asks an existing local daemon to stop directly. A
// daemon routinely outlives the idle broker, so "no broker" never means "no
// daemon".
func stopDaemonWithoutBroker(ctx context.Context) error {
	reply, err := exchangeLocalDaemon(ctx, protocol.Kill{RequestID: 1, Scope: protocol.KillDaemon})
	if errors.Is(err, errNoLocalDaemon) {
		return errDaemonNotRunning
	}
	if err != nil {
		return fmt.Errorf("stop local daemon: %w", err)
	}
	result, ok := reply.(protocol.KillResult)
	if !ok {
		return fmt.Errorf("unexpected daemon stop reply %T", reply)
	}
	return brokerKillResultError(result)
}
