package app

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	snapshotadapter "github.com/bnema/vev/internal/adapters/snapshot"

	"github.com/bnema/vev/internal/adapters/clock"
	"github.com/bnema/vev/internal/adapters/config"
	"github.com/bnema/vev/internal/adapters/daemonidentity"
	"github.com/bnema/vev/internal/adapters/daemonmux"
	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/adapters/lifecycle"
	"github.com/bnema/vev/internal/adapters/noticefile"
	"github.com/bnema/vev/internal/adapters/pty"
	"github.com/bnema/vev/internal/adapters/sessionwire"
	"github.com/bnema/vev/internal/adapters/shellcmd"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/logging"
	"github.com/bnema/vev/internal/persist"
	"github.com/bnema/vev/internal/platform"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/bnema/vev/internal/usecase/daemon"
	"github.com/bnema/vev/internal/usecase/recovery"
	"github.com/bnema/vev/pkg/safedir"
)

// Daemon composition seams replaced by tests.
var (
	openCatalogue = persist.OpenOrCreate
	listenDaemon  = func(dir string, observer ports.SerializedRuntimeObserver) (wire.Listener, error) {
		return ipc.Listen(dir, ipc.WithRuntimeObserver(observer))
	}
)

func snapshotDir() string {
	return filepath.Join(platform.StateDir(), "snapshots")
}

func developmentTempDirOption(ensurePrivate func(string) error) (daemon.Option, error) {
	dir, active := platform.DevelopmentEnvironmentTempDir()
	if !active {
		return nil, nil
	}
	if err := ensurePrivate(dir); err != nil {
		return nil, fmt.Errorf("vev: secure development temp directory: %w", err)
	}
	return daemon.WithTempDir(dir), nil
}

func pprofAddrIsLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil || host == "" {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

type ownedDaemonStart func(context.Context) error

type lifecycleOwnership interface {
	Release() error
}

type lifecycleStartupDeps struct {
	ensurePrivate func(string) error
	acquire       func(context.Context, string, time.Duration) (lifecycleOwnership, error)
	log           *slog.Logger
}

const lifecycleAcquireRetry = 25 * time.Millisecond

func lifecycleStartupDependencies(log *slog.Logger) lifecycleStartupDeps {
	return lifecycleStartupDeps{
		ensurePrivate: safedir.EnsurePrivate,
		acquire: func(ctx context.Context, runtimeDir string, retry time.Duration) (lifecycleOwnership, error) {
			return lifecycle.Acquire(ctx, runtimeDir, retry)
		},
		log: log,
	}
}

func runWithLifecycleOwner(ctx context.Context, runtimeDir, stateDir string, start ownedDaemonStart) error {
	return runWithLifecycleOwnerDeps(ctx, runtimeDir, stateDir, start, lifecycleStartupDependencies(nil))
}

func runWithLifecycleOwnerDeps(ctx context.Context, runtimeDir, stateDir string, start ownedDaemonStart, deps lifecycleStartupDeps) (retErr error) {
	log := deps.log
	if log == nil {
		log = slog.Default()
	}
	if err := deps.ensurePrivate(runtimeDir); err != nil {
		return fmt.Errorf("vev: secure runtime directory: %w", err)
	}
	if err := deps.ensurePrivate(stateDir); err != nil {
		return fmt.Errorf("vev: secure state directory: %w", err)
	}
	lockPath := lifecycle.Path(runtimeDir)
	log.Info("lifecycle_owner_wait", "path", lockPath)
	owner, err := deps.acquire(ctx, runtimeDir, lifecycleAcquireRetry)
	if err != nil {
		log.Warn("lifecycle_owner_wait_failed", "path", lockPath, "reason_code", "acquire-failed")
		return fmt.Errorf("vev: acquire lifecycle ownership: %w", err)
	}
	log.Info("lifecycle_owner_acquired", "path", lockPath)
	defer func() {
		releaseErr := owner.Release()
		if releaseErr != nil {
			log.Error("lifecycle_owner_release_failed", "path", lockPath, "reason_code", "release-failed")
		} else {
			log.Info("lifecycle_owner_released", "path", lockPath)
		}
		retErr = errors.Join(retErr, releaseErr)
	}()
	if start == nil {
		return errors.New("vev: nil owned daemon startup")
	}
	return start(ctx)
}

// runDaemon runs the daemon in the foreground (the hidden --daemon path,
// entered by an auto-spawned child) while holding exclusive lifecycle
// ownership through complete teardown.
func runDaemon() (retErr error) {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	log, logCloser, err := configureLogging(logging.Daemon, true)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, logCloser.Close()) }()
	return runWithLifecycleOwnerDeps(ctx, ipc.SocketDir(), platform.StateDir(), func(ctx context.Context) error {
		return runDaemonOwnedWithLogger(ctx, log)
	}, lifecycleStartupDependencies(log))
}

func logCatalogueRecovery(log *slog.Logger, records []domain.CatalogueRecord, recoveryMode string) {
	log.Info("catalogue_validated", "records", len(records), "recovery", recoveryMode)
}

func logStartupRecoveryCounts(log *slog.Logger, records []domain.CatalogueRecord, restoring int) {
	healthy, fresh, broken := 0, 0, 0
	for _, record := range records {
		switch {
		case record.DegradedReason != "":
			broken++
		case record.Committed == nil:
			fresh++
		default:
			healthy++
		}
	}
	log.Info("daemon_startup_complete", "healthy", healthy, "fresh", fresh, "restoring", restoring, "broken", broken)
}

func runDaemonOwned(ctx context.Context) (retErr error) {
	log, logCloser, err := configureLogging(logging.Daemon, true)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, logCloser.Close()) }()
	return runDaemonOwnedWithLogger(ctx, log)
}

func runDaemonOwnedWithLogger(ctx context.Context, log *slog.Logger) (retErr error) {
	clk := clock.New()
	observer, observerCloser, err := newPerformanceTrace(clk)
	if err != nil {
		return fmt.Errorf("vev: performance trace: %w", err)
	}
	if observerCloser != nil {
		defer func() { retErr = errors.Join(retErr, observerCloser.Close()) }()
	}
	if addr := os.Getenv("VEV_PPROF_ADDR"); addr != "" {
		if !pprofAddrIsLoopback(addr) {
			log.Warn("pprof bound to non-loopback address; /debug/pprof is unauthenticated", "addr", addr)
		}
		go func() {
			if err := http.ListenAndServe(addr, nil); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("pprof server exited", "err", err)
			}
		}()
		log.Info("pprof enabled", "addr", addr)
	}

	var daemonOpts []daemon.Option
	developmentTempOpt, err := developmentTempDirOption(safedir.EnsurePrivate)
	if err != nil {
		return err
	}
	if developmentTempOpt != nil {
		daemonOpts = append(daemonOpts, developmentTempOpt)
	}
	if observer != nil {
		daemonOpts = append(daemonOpts, daemon.WithRuntimeObserver(observer))
	}
	configPath := platform.ConfigPath()
	cfg := loadConfigOrDefaults(log, configPath)
	daemonOpts = append(daemonOpts, daemon.WithConfig(cfg))
	daemonOpts = append(daemonOpts, daemon.WithBarScriptCommandRunner(shellcmd.New()))
	daemonOpts = append(daemonOpts, daemon.WithProcessInspector(platform.NewProcessInspector()), daemon.WithDirOrHome(platform.DirOrHome))
	snapshotRepository := snapshotadapter.NewRepositoryWithLogger(snapshotDir(), log)
	daemonOpts = append(daemonOpts, daemon.WithSnapshotRepository(snapshotRepository))
	noticeStore := noticefile.New(platform.StateDir())
	daemonOpts = append(daemonOpts, daemon.WithNoticeStore(noticeStore))
	stateDir := platform.StateDir()
	storePath := persist.StorePath(stateDir)
	opened, err := openCatalogue(stateDir)
	if err != nil {
		log.Error("catalogue_validation_failed", "path", storePath, "reason_code", "open-failed")
		if errors.Is(err, persist.ErrCatalogueUnreadable) {
			return unreadableCatalogueError(stateDir)
		}
		return fmt.Errorf("vev: open durable session state %s: %w", storePath, err)
	}
	recoveryMode := "current"
	if opened.NewInstall {
		recoveryMode = "new-install"
	}
	coordinator := recovery.NewCoordinator(opened.Catalogue, snapshotRepository, rand.Reader)
	if opened.Migration.Performed {
		recoveryMode = "catalogue-migrated"
		log.Info("catalogue_migrated", "source_formats", opened.Migration.SourceFormats, "target_format", opened.Migration.TargetFormat, "records", opened.Migration.RecordCount, "backup", opened.Migration.BackupPath)
	}
	logCatalogueRecovery(log, opened.Records, recoveryMode)
	daemonOpts = append(daemonOpts, daemon.WithRecoveryCoordinator(coordinator))
	daemonOpts = append(daemonOpts, daemon.WithSnapshotGarbageCollection())
	log.Info("session persistence enabled", "path", storePath)
	daemonOpts = append(daemonOpts, daemon.WithCatalogue(opened.Catalogue, opened.Records))

	// Construct the catalogue-backed expected-session registry before socket
	// publication. Phase 3 snapshot restoration remains asynchronous in Serve.
	identity, err := daemonidentity.LoadOrCreate(stateDir)
	if err != nil {
		return fmt.Errorf("vev: establish daemon identity: %w", err)
	}
	incarnation, err := daemonidentity.NewIncarnation()
	if err != nil {
		return fmt.Errorf("vev: establish daemon incarnation: %w", err)
	}
	binding, err := daemonmux.NewServerBindings(identity, incarnation, []daemonmux.ServerPolicyAdmission{
		{Policy: localDaemonPolicy(), Origin: ports.SessionOriginLocal},
		{Policy: remoteBrokerPolicy(hostTransportQUIC), Origin: ports.SessionOriginRemote},
		{Policy: remoteBrokerPolicy(hostTransportSSH), Origin: ports.SessionOriginRemote},
	})
	if err != nil {
		return fmt.Errorf("vev: establish daemon mux authority: %w", err)
	}
	aggregate := daemonmux.NewAggregateListener()
	muxSupervisor, err := daemonmux.NewServerSupervisor(aggregate, binding, daemonmux.DefaultMuxCeilings(), 0)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, muxSupervisor.Close()) }()
	muxListener, err := ipc.ListenMux(daemonmux.SocketPath(ipc.SocketDir()))
	if err != nil {
		return fmt.Errorf("vev: listen daemon mux: %w", err)
	}
	defer func() { retErr = errors.Join(retErr, muxListener.Close()) }()
	go func() {
		for {
			raw, acceptErr := muxListener.Accept()
			if acceptErr != nil {
				return
			}
			go func() { _ = muxSupervisor.Adopt(ctx, raw) }()
		}
	}()

	// The catalogue-backed registry exists before the socket is published.
	// Snapshot GC runs in the background once restoration finishes, so it never
	// delays socket publication.
	d := daemon.New(pty.NewFactory(), clk, log, daemonOpts...)
	ln, err := listenDaemon(ipc.SocketDir(), observer)
	if err != nil {
		closeErr := opened.Catalogue.Close()
		log.Error("daemon listen failed", "socket_dir", ipc.SocketDir(), "err", err)
		return errors.Join(fmt.Errorf("vev: daemon listen: %w", err), closeErr)
	}
	defer func() { _ = ln.Close() }()
	log.Info("daemon starting", "socket", ln.Addr())

	watchCtx, stopWatch := context.WithCancel(ctx)
	defer stopWatch()
	go func() {
		if err := config.Watch(watchCtx, clk, configPath, func(cfg domain.Config, warnings []domain.Warning) {
			logConfigWarnings(log, warnings)
			d.ApplyConfig(cfg)
		}); err != nil && !errors.Is(err, context.Canceled) {
			log.Warn("config watcher stopped", "path", configPath, "err", err)
		}
	}()
	if err := aggregate.Register(sessionwire.NewServerListener(ln)); err != nil {
		return fmt.Errorf("vev: register local session listener: %w", err)
	}
	if err := d.Serve(ctx, aggregate); err != nil {
		log.Error("daemon exited", "err", err)
		return err
	}
	log.Info("daemon exited cleanly")
	return nil
}
