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
	"github.com/bnema/vev/internal/platform"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
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
		clk := clock.New()
		deps := runAttachDeps{
			terminal: func() ports.Terminal { return terminal }, clock: func() ports.Clock { return clk },
			disableCapabilityProbe: true, selectedRemoteTransport: os.Getenv(envRemoteTransport),
			createDetached: createDetachedLocalSession, stateDir: platform.StateDir,
		}
		err := runAttachWithDeps(ctx, protocol.IntentEphemeral, "", "", "", log, deps)
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
