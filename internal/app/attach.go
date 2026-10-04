package app

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/bnema/vev/internal/adapters/clock"
	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/adapters/term"
	"github.com/bnema/vev/internal/adapters/uidriver"
	"github.com/bnema/vev/internal/adapters/uiterm"
	"github.com/bnema/vev/internal/logging"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/client"
)

// runAttach dials (auto-spawning the daemon if needed) and runs the client
// attach loop. Logging goes to the shared file: the client must never write
// to the console while the terminal is raw.
func runAttach(ctx context.Context, intent uint8, name, remoteTarget string) (retErr error) {
	if os.Getenv("VEV") != "" {
		if remoteTarget == "" && intent == protocol.IntentNew {
			return createDetachedTerminalSession(ctx, name)
		}
		return errors.New("sessions should be nested with care; unset VEV to force")
	}
	ctx, stop := clientSignalContext(ctx)
	defer stop()
	log, logCloser, err := configureLogging(logging.Client, false)
	if err != nil {
		return err
	}
	defer func() { _ = logCloser.Close() }()
	clk := clock.New()
	observer, observerCloser, err := newPerformanceTrace(clk)
	if err != nil {
		return fmt.Errorf("performance trace: %w", err)
	}
	if observerCloser != nil {
		defer func() { retErr = errors.Join(retErr, observerCloser.Close()) }()
	}
	if observer != nil {
		observer.ObserveRuntime(ports.NewRuntimeMark("client", ports.RuntimeTransportDiagnostic, 0, true))
	}
	terminal := terminalForAttach()
	intent, preconnected, err := resolveAttachCreationIntent(ctx, intent, name, remoteTarget, terminal)
	if err != nil {
		return err
	}
	navigation, resolver, err := terminalBrokerNavigation(intent, name, remoteTarget)
	if err != nil {
		if preconnected != nil {
			_ = preconnected.Close()
		}
		return err
	}
	callbacks := terminalBrokerCallbacks()
	return runBrokerClient(ctx, brokerClientConfig{
		Logger: log, Connector: withPreconnected(newProductionBrokerConnector(), preconnected), Terminal: terminal, Clock: clk,
		InitialNavigation: navigation, ResolveInitialNavigation: resolver,
		AttachmentEnvironment: outerTerminalAttachmentEnvironment(log), SessionEnvironment: terminalSessionEnvironment(),
		OnState: callbacks.OnState, OnLifecycle: callbacks.OnLifecycle, OnFailure: callbacks.OnFailure,
	})
}

// runAttachWithOptions is the composition path for ordinary interactive
// clients. Observation is opt-in; the default path remains term.New().
//
// The observed path composes the physical terminal, one VT mirror, the UI
// service over that mirror, and the same broker connector and initial-navigation
// translation the ordinary terminal attach uses, then delegates the whole run to
// the shared runBrokerClient. It owns no daemon dialer, no remote carriage, and
// no session: mirroring observes bytes at the serialized terminal writer and the
// broker owns every transport.
func runAttachWithOptions(ctx context.Context, intent uint8, name, remoteTarget string, options interactiveUIOptions) (retErr error) {
	if !options.enabled() {
		return runAttach(ctx, intent, name, remoteTarget)
	}
	if options.control {
		options.observe = true
	}
	ctx, stop := clientSignalContext(ctx)
	defer stop()
	log, logCloser, err := configureLogging(logging.Client, false)
	if err != nil {
		return err
	}
	defer func() { _ = logCloser.Close() }()
	clk := clock.New()
	// The process trace is still created and joined here so an operator's trace
	// file keeps its exact lifecycle, including a close failure joined into the
	// run's result. Client-side transport spans are no longer emitted: the
	// broker owns carriage, so the observed composition keeps no dialer seam.
	_, observerCloser, err := newPerformanceTrace(clk)
	if err != nil {
		return fmt.Errorf("performance trace: %w", err)
	}
	if observerCloser != nil {
		defer func() { retErr = errors.Join(retErr, observerCloser.Close()) }()
	}
	physical := term.NewWithFilesAndObservation(os.Stdin, os.Stdout, nil)
	geometry, err := physical.Geometry()
	if err != nil {
		return fmt.Errorf("reading terminal geometry: %w", err)
	}
	mirror, err := uiterm.NewMirror(ctx, geometry, "")
	if err != nil {
		return fmt.Errorf("create UI mirror: %w", err)
	}
	defer mirror.Close()
	physical = term.NewWithFilesAndObservation(os.Stdin, os.Stdout, mirror)
	ui := client.NewUI(mirror, clk)
	var endpoint *uidriver.UnixEndpoint
	if options.observe {
		server := uidriver.New(ui, clk)
		socketPath := options.socket
		if socketPath == "" {
			socketPath = uidriver.DefaultSocketPath(ipc.SocketDir(), ui.Handle())
		}
		endpoint, err = uidriver.ListenUnix(socketPath, server, func() uidriver.Ready {
			ready := uidriver.Ready{Attachment: ui.Handle(), Control: options.control, Status: ports.UIStatusPicker}
			if snapshot, snapshotErr := ui.Capture(ui.Handle()); snapshotErr == nil {
				ready = uidriverReady(ui, snapshot, options.control)
			}
			return ready
		})
		if err != nil {
			return err
		}
		fmt.Fprintln(os.Stderr, socketPath)
		defer func() { _ = endpoint.Close() }()
	}
	terminal := observedTerminal{Terminal: physical, UIOutputTransaction: mirror}
	intent, preconnected, err := resolveAttachCreationIntent(ctx, intent, name, remoteTarget, terminal)
	if err != nil {
		return err
	}
	navigation, resolver, err := terminalBrokerNavigation(intent, name, remoteTarget)
	if err != nil {
		if preconnected != nil {
			_ = preconnected.Close()
		}
		return err
	}
	sessionEnv := terminalSessionEnvironment()
	attachmentEnv := configuredTerminalAttachmentEnvironment(log)
	attachmentEnv.Cwd = sessionEnv.Cwd
	return runBrokerClient(ctx, brokerClientConfig{
		Logger:                   log,
		Connector:                withPreconnected(newProductionBrokerConnector(), preconnected),
		Terminal:                 terminal,
		Clock:                    clk,
		UI:                       ui,
		InitialNavigation:        navigation,
		ResolveInitialNavigation: resolver,
		AttachmentEnvironment:    attachmentEnv,
		SessionEnvironment:       sessionEnv,
	})
}
