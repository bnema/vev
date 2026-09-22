package app

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/bnema/vev/internal/adapters/clock"
	"github.com/bnema/vev/internal/adapters/webterm"
	"github.com/bnema/vev/internal/logging"
	"github.com/bnema/vev/internal/ports"
)

const webStartupTimeout = 10 * time.Second

func webReachable(ctx context.Context, client *http.Client, access webAccess, expected webterm.Settings) bool {
	if access.Settings != expected {
		return false
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+access.Settings.ProbeAddress()+"/health", nil)
	if err != nil {
		return false
	}
	request.Host = access.Settings.Host()
	request.AddCookie(&http.Cookie{Name: "vev-web-session", Value: access.Token})
	response, err := client.Do(request)
	if err != nil {
		return false
	}
	defer response.Body.Close()
	return response.StatusCode == http.StatusNoContent
}

func printWebLink(access webAccess) {
	fmt.Printf("vev web terminal: %s/#token=%s\nKeep this access link private.\n", access.Settings.Origin, access.Token)
}

func renewWebToken(ctx context.Context) error {
	token, err := webControlRequest(ctx, true)
	if err != nil {
		return fmt.Errorf("vev: renewing web token: %w", err)
	}
	printWebLink(token)
	return nil
}

func launchWebDaemon(ctx context.Context, options webOptions) error {
	settings, warnings, err := loadWebSettings(options)
	if err != nil {
		return err
	}
	for _, warning := range warnings {
		if warning.Line > 0 {
			fmt.Fprintf(os.Stderr, "vev: config warning (line %d): %s\n", warning.Line, warning.Msg)
			continue
		}
		fmt.Fprintf(os.Stderr, "vev: config warning: %s\n", warning.Msg)
	}
	// One shared client for every readiness probe. Proxy stays disabled so
	// probes only ever contact the local listener.
	client := &http.Client{Timeout: time.Second, Transport: &http.Transport{Proxy: nil}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer client.CloseIdleConnections()
	if token, err := webControlRequest(ctx, false); err == nil {
		if token.Settings != settings {
			return errors.New("vev: web gateway is running with different settings; stop and restart it to apply web.listen/web.origin")
		}
		readyCtx, cancel := context.WithTimeout(ctx, webStartupTimeout)
		defer cancel()
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for !webReachable(readyCtx, client, token, settings) {
			select {
			case <-readyCtx.Done():
				return fmt.Errorf("vev: existing web gateway not ready: %w", readyCtx.Err())
			case <-ticker.C:
			}
		}
		printWebLink(token)
		return nil
	}
	listener, err := listenWeb(settings.Listen)
	if err != nil {
		return fmt.Errorf("vev: web port unavailable: %w", err)
	}
	defer listener.Close()
	inherited, ok := listener.(*net.TCPListener)
	if !ok {
		return fmt.Errorf("vev: web listener is not TCP")
	}
	inheritedFile, err := inherited.File()
	if err != nil {
		return err
	}
	defer inheritedFile.Close()
	executable, err := selfExePath()
	if err != nil {
		return err
	}
	null, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer null.Close()
	child := exec.Command(executable, "--web-serve", "--web-listen", settings.Listen, "--web-origin", settings.Origin)
	// Browser capabilities do not depend on the launcher's TTY.
	child.Env = append(withoutPerformanceTraceEnv(os.Environ()), "TERM=xterm-256color", "COLORTERM=truecolor")
	child.Stdin, child.Stdout, child.Stderr = null, null, null
	child.ExtraFiles = []*os.File{inheritedFile}
	child.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := child.Start(); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- child.Wait() }()
	readyCtx, cancel := context.WithTimeout(ctx, webStartupTimeout)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		token, err := webControlRequest(readyCtx, false)
		if err == nil && webReachable(readyCtx, client, token, settings) {
			printWebLink(token)
			return nil
		}
		select {
		case err := <-done:
			if err != nil {
				return fmt.Errorf("vev: web process exited before readiness: %w", err)
			}
			return errors.New("vev: web process exited before readiness")
		case <-readyCtx.Done():
			_ = child.Process.Kill()
			<-done
			return fmt.Errorf("vev: web startup: %w", readyCtx.Err())
		case <-ticker.C:
		}
	}
}

// webTerminalConnector is the composition-root seam of one browser attachment's
// broker connector. Production composes the lazy per-user connector, which
// connect-or-spawns the broker on demand inside Connect; a test substitutes a
// connector over its own private sandbox.
var webTerminalConnector = newProductionBrokerConnector

// webTerminal is the terminal of one browser attachment: the adapter's virtual
// terminal plus the optional UI output transaction the shared supervisor
// expects. The browser gateway has no UI observation service — the adapter
// renders from its own VT mirror and change signal — so the transaction is the
// honest "observation channel unavailable" shape: begin/end bracket the write
// with no published snapshot, and PublishContext reports ports.ErrUIUnavailable,
// which the supervisor's foreground deliberately tolerates so the frame is
// still written and flushed. This keeps the browser run on the same code path
// as every other frontend without inventing a second renderer or a parallel
// presentation owner.
type webTerminal struct {
	*webterm.Terminal
}

var _ ports.UIOutputTransaction = webTerminal{}

func (webTerminal) BeginOutput(ports.UIContext) {}

// EndOutput is a no-op: no publication was staged, so nothing is committed or
// rolled back.
func (webTerminal) EndOutput(bool) {}

// PublishContext reports the unavailable observation channel. The supervisor
// tolerates it and still writes the frame; a picker paint does the same.
func (webTerminal) PublishContext(ports.UIContext) error { return ports.ErrUIUnavailable }

// runWebTerminalClient composes exactly one browser attachment over the shared
// broker client. Every authenticated WebSocket owns one call, and therefore one
// client.Picker, one client.Supervisor, one broker service, and one session
// stream; the virtual webterm.Terminal is simply that run's terminal, so the
// picker, connecting, and attached presentations and their bounded failures are
// the same ones the ordinary terminal path presents.
//
// It dials no daemon, selects no transport, and keeps no host registry, remote
// factory, launch configuration, or attachment cache: the broker owns every
// carriage, and the no-argument product intent is expressed as the closed
// initial-navigation union, exactly like the sandbox harness. Cancelling the
// run's context ends only this supervisor and its logical stream — the broker,
// its daemons, and every other tab stay alive.
func runWebTerminalClient(ctx context.Context, terminal *webterm.Terminal) error {
	callbacks := terminalBrokerCallbacks()
	sessionEnv := terminalSessionEnvironment()
	attachmentEnv := terminalAttachmentEnvironment()
	attachmentEnv.Cwd = sessionEnv.Cwd
	return runBrokerClient(ctx, brokerClientConfig{
		Connector:             webTerminalConnector(),
		Terminal:              webTerminal{Terminal: terminal},
		Clock:                 clock.New(),
		InitialNavigation:     localEphemeralNavigation(),
		AttachmentEnvironment: attachmentEnv,
		SessionEnvironment:    sessionEnv,
		OnState:               callbacks.OnState,
		OnFailure:             callbacks.OnFailure,
		OnLifecycle:           callbacks.OnLifecycle,
	})
}

// Preserve the requested address family, including IPv4 wildcard listeners.
func listenWeb(address string) (net.Listener, error) {
	addr, err := netip.ParseAddrPort(address)
	if err != nil {
		return nil, err
	}
	network := "tcp6"
	if addr.Addr().Is4() {
		network = "tcp4"
	}
	return net.Listen(network, address)
}

func runWebDaemon(parent context.Context, options webOptions) error {
	// The launcher passes resolved settings; do not reload a changing config.
	if options.listen == "" || options.origin == "" {
		return errors.New("vev: web server requires its launcher settings")
	}
	settings, err := webterm.ParseSettings(options.listen, options.origin)
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(parent, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	inherited := os.NewFile(3, "web-listener")
	if inherited == nil {
		return errors.New("vev: web server requires its launcher")
	}
	listener, err := net.FileListener(inherited)
	_ = inherited.Close()
	if err != nil {
		return fmt.Errorf("vev: inherited web listener: %w", err)
	}
	defer listener.Close()
	if listener.Addr().String() != settings.Listen {
		return errors.New("vev: inherited web listener does not match web.listen")
	}
	token, err := webterm.NewToken()
	if err != nil {
		return err
	}
	log, closer, err := configureLogging(logging.Client, false)
	if err != nil {
		return err
	}
	defer closer.Close()
	handler, err := webterm.NewServer(ctx, settings, token, func(ctx context.Context, terminal *webterm.Terminal) error {
		err := runWebTerminalClient(ctx, terminal)
		if err != nil && ctx.Err() == nil {
			log.Warn("web attachment ended", "error", strings.TrimSpace(err.Error()))
		}
		return err
	})
	if err != nil {
		return err
	}
	control, err := startWebControl(ctx, handler, settings)
	if err != nil {
		return err
	}
	defer control.Close()
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-ctx.Done()
		shutdownCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = server.Shutdown(shutdownCtx)
	}()
	err = server.Serve(listener)
	cancel()
	<-done
	handler.Wait()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
