package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bnema/vev/internal/adapters/dgram"
	remoteadapter "github.com/bnema/vev/internal/adapters/remote"
	"github.com/bnema/vev/internal/adapters/sshstdio"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

const (
	defaultTarget = "test@remote"
	sessionName   = "harness"
)

var requestID atomic.Uint64

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "remote picker harness: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("remote picker harness: PASS")
}

func run() error {
	target := os.Getenv("VEV_HARNESS_TARGET")
	if target == "" {
		target = defaultTarget
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	catalog := remoteadapter.NewCatalogClient()
	preview := remoteadapter.NewPreviewClient()

	stdio, err := openTransport(ctx, ports.RemoteTransportStdio, target)
	if err != nil {
		return fmt.Errorf("open stdio transport: %w", err)
	}
	if err := attachAndCheck(ctx, stdio, ports.Hello{
		Intent:    ports.IntentNew,
		Name:      sessionName,
		Env:       localEnvironment(),
		Cwd:       "/tmp/local-cwd",
		TermEnv:   "xterm-256color",
		TrueColor: true,
	}); err != nil {
		_ = stdio.Close()
		return fmt.Errorf("stdio live attach: %w", err)
	}
	if err := sendInputAndAwait(ctx, stdio, "printf 'VEV_REMOTE_HARNESS_ENV=%s:%s:%s:%s\\n' \"$VEV_TEST_ENV\" \"$XDG_RUNTIME_DIR\" \"$WAYLAND_DISPLAY\" \"$VEV\"", "local:/run/local:wayland-local:"); err != nil {
		_ = stdio.Close()
		return fmt.Errorf("direct client environment: %w", err)
	}

	live, err := waitForSession(ctx, catalog, target, sessionName, "running")
	if err != nil {
		_ = stdio.Close()
		return fmt.Errorf("live catalog: %w", err)
	}
	liveTarget, err := targetForSession(target, live, false)
	if err != nil {
		_ = stdio.Close()
		return fmt.Errorf("live target: %w", err)
	}
	if _, err := preview.Preview(ctx, liveTarget, 40, 8); err != nil {
		_ = stdio.Close()
		return fmt.Errorf("live preview: %w", err)
	}

	if err := sendCommand(ctx, stdio, "new-tab"); err != nil {
		_ = stdio.Close()
		return fmt.Errorf("create second tab: %w", err)
	}
	liveWithTabs, err := waitForSessionWithTabCount(ctx, catalog, target, sessionName, "running", 2)
	if err != nil {
		_ = stdio.Close()
		return fmt.Errorf("multi-tab catalog: %w", err)
	}
	removedTargets := make([]domain.RemoteSessionTarget, 0, len(ports.CatalogTabs(liveWithTabs)))
	for _, tab := range ports.CatalogTabs(liveWithTabs) {
		candidate := liveWithTabs
		candidate.ActiveTabID = tab.ID
		removed, targetErr := targetForSession(target, candidate, false)
		if targetErr != nil {
			_ = stdio.Close()
			return fmt.Errorf("removed-tab target: %w", targetErr)
		}
		removedTargets = append(removedTargets, removed)
	}
	if err := sendCommand(ctx, stdio, "close-tab"); err != nil {
		_ = stdio.Close()
		return fmt.Errorf("remove tab: %w", err)
	}
	if _, err := waitForSessionWithTabCount(ctx, catalog, target, sessionName, "running", 1); err != nil {
		_ = stdio.Close()
		return fmt.Errorf("removed-tab catalog: %w", err)
	}
	removed := false
	for _, removedTarget := range removedTargets {
		if _, previewErr := preview.Preview(ctx, removedTarget, 40, 8); previewErr != nil {
			removed = true
			break
		}
	}
	if !removed {
		_ = stdio.Close()
		return errors.New("removed tab unexpectedly produced a preview")
	}

	if err := detach(ctx, stdio); err != nil {
		_ = stdio.Close()
		return fmt.Errorf("stop session: %w", err)
	}
	_ = stdio.Close()
	if err := runRemoteCommand(ctx, target, "vev", "kill", "--daemon"); err != nil {
		return fmt.Errorf("stop remote daemon: %w", err)
	}
	if err := runRemoteCommand(ctx, target, "vev", "--daemon-launcher"); err != nil {
		return fmt.Errorf("restart remote daemon: %w", err)
	}

	resumedCatalog, err := waitForSession(ctx, catalog, target, sessionName, "running")
	if err != nil {
		return fmt.Errorf("resumed catalog: %w", err)
	}
	resumedTarget, err := targetForSession(target, resumedCatalog, false)
	if err != nil {
		return fmt.Errorf("resumed target: %w", err)
	}
	resumed, err := openTransport(ctx, ports.RemoteTransportStdio, target)
	if err != nil {
		return fmt.Errorf("open restart stdio transport: %w", err)
	}
	if err := attachAndCheck(ctx, resumed, ports.Hello{
		Intent:            ports.IntentResume,
		Name:              sessionName,
		Env:               localEnvironment(),
		Cwd:               "/tmp/local-cwd",
		TermEnv:           "xterm-256color",
		TrueColor:         true,
		RemoteTarget:      &resumedTarget,
		EnvironmentPolicy: ports.EnvironmentPolicyDaemonOwned,
	}); err != nil {
		_ = resumed.Close()
		return fmt.Errorf("exact restart resume: %w", err)
	}
	if _, err := preview.Preview(ctx, resumedTarget, 40, 8); err != nil {
		_ = resumed.Close()
		return fmt.Errorf("restart preview: %w", err)
	}
	if err := touchRemoteFlag(ctx, target, "/tmp/vev-slow-preview"); err != nil {
		_ = resumed.Close()
		return fmt.Errorf("enable slow preview: %w", err)
	}
	slowCtx, slowCancel := context.WithTimeout(ctx, 150*time.Millisecond)
	_, slowErr := preview.Preview(slowCtx, resumedTarget, 40, 8)
	slowCancel()
	if slowErr == nil {
		_ = resumed.Close()
		return errors.New("slow preview did not honor cancellation")
	}
	_ = touchRemoteFlag(ctx, target, "/tmp/vev-slow-preview-off")

	if err := detach(ctx, resumed); err != nil {
		_ = resumed.Close()
		return fmt.Errorf("stop resumed session: %w", err)
	}
	_ = resumed.Close()

	udp, err := openTransport(ctx, ports.RemoteTransportUDP, target)
	if err != nil {
		return fmt.Errorf("open UDP transport: %w", err)
	}
	if err := attachAndCheck(ctx, udp, ports.Hello{
		Intent:    ports.IntentNew,
		Name:      "udp-harness",
		Env:       localEnvironment(),
		Cwd:       "/tmp/local-cwd",
		TermEnv:   "xterm-256color",
		TrueColor: true,
	}); err != nil {
		_ = udp.Close()
		return fmt.Errorf("UDP attach: %w", err)
	}
	if err := detach(ctx, udp); err != nil {
		_ = udp.Close()
		return fmt.Errorf("UDP detach: %w", err)
	}
	_ = udp.Close()

	if err := touchRemoteFlag(ctx, target, "/tmp/vev-bad-catalog"); err != nil {
		return fmt.Errorf("enable mismatched catalog: %w", err)
	}
	if _, err := catalog.List(ctx, target); err == nil {
		return errors.New("version-mismatched catalog unexpectedly decoded")
	}
	_ = touchRemoteFlag(ctx, target, "/tmp/vev-bad-catalog-off")

	if _, err := catalog.List(ctx, "test@unreachable.invalid"); err == nil {
		return errors.New("unreachable catalog unexpectedly succeeded")
	}

	if err := runRemoteCommand(ctx, target, "vev", "kill", sessionName); err != nil {
		return fmt.Errorf("destroy session for stale-target test: %w", err)
	}
	if err := waitForMissingSession(ctx, catalog, target, sessionName); err != nil {
		return fmt.Errorf("wait for destroyed session: %w", err)
	}
	stale, err := openTransport(ctx, ports.RemoteTransportStdio, target)
	if err != nil {
		return fmt.Errorf("open stale-target transport: %w", err)
	}
	if err := expectNoSuchTarget(ctx, stale, resumedTarget); err != nil {
		_ = stale.Close()
		return fmt.Errorf("stale target fence: %w", err)
	}
	_ = stale.Close()

	return nil
}

func localEnvironment() []string {
	return []string{
		"VEV_TEST_ENV=local",
		"XDG_RUNTIME_DIR=/run/local",
		"WAYLAND_DISPLAY=wayland-local",
		"TERM=xterm-256color",
		"COLORTERM=truecolor",
		"TERM_PROGRAM=local-harness",
	}
}

func openTransport(ctx context.Context, mode ports.RemoteTransportMode, target string) (ports.Transport, error) {
	switch mode {
	case ports.RemoteTransportStdio:
		return sshstdio.DialContext(ctx, target, "", slog.New(slog.DiscardHandler))
	case ports.RemoteTransportUDP:
		dialer := dgram.NewRemoteDialerWithLogger(target, "", slog.New(slog.DiscardHandler))
		dialer.BootstrapTimeout = 8 * time.Second
		dialer.ProbeTimeout = 3 * time.Second
		return dialer.Dial(ctx)
	default:
		return nil, fmt.Errorf("unsupported transport %q", mode)
	}
}

func attachAndCheck(ctx context.Context, tr ports.Transport, hello ports.Hello) error {
	hello.Version = ports.ProtocolVersion
	hello.Size = domain.Size{Cols: 80, Rows: 24}
	if hello.EnvironmentPolicy == 0 {
		hello.EnvironmentPolicy = ports.EnvironmentPolicyClientOwned
	}
	payload := ports.MarshalHello(hello)
	if err := tr.Send(ports.Frame{Type: ports.MsgHello, Payload: payload}); err != nil {
		return err
	}
	frame, err := receiveWithTimeout(ctx, tr)
	if err != nil {
		return err
	}
	if frame.Type == ports.MsgError {
		message, decodeErr := ports.UnmarshalErrorMsg(frame.Payload)
		if decodeErr != nil {
			return decodeErr
		}
		return fmt.Errorf("remote error code %d", message.Code)
	}
	if frame.Type != ports.MsgWelcome {
		return fmt.Errorf("welcome frame type %d", frame.Type)
	}
	if err := tr.Send(ports.Frame{Type: ports.MsgTheme, Payload: ports.MarshalTheme(ports.Theme{TrueColor: hello.TrueColor, SchemeKnown: true})}); err != nil {
		return fmt.Errorf("send initial theme: %w", err)
	}
	return nil
}

func sendInputAndAwait(ctx context.Context, tr ports.Transport, command, want string) error {
	if err := tr.Send(ports.Frame{Type: ports.MsgInput, Payload: ports.MarshalInput(ports.Input{Data: []byte(command + "\n")})}); err != nil {
		return err
	}
	var received strings.Builder
	var outputFrames int
	var outputBytes int
	deadline, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for {
		frame, err := receiveWithTimeout(deadline, tr)
		if err != nil {
			return fmt.Errorf("waiting for marker after %d output frames/%d bytes: %w", outputFrames, outputBytes, err)
		}
		if frame.Type != ports.MsgOutput {
			continue
		}
		output, err := ports.UnmarshalOutput(frame.Payload)
		if err != nil {
			return err
		}
		outputFrames++
		outputBytes += len(output.Data)
		if output.Epoch != 0 && output.New != 0 {
			ackPayload, err := ports.MarshalAck(ports.Ack{Epoch: output.Epoch, State: output.New})
			if err != nil {
				return fmt.Errorf("encode output ack: %w", err)
			}
			if err := tr.Send(ports.Frame{Type: ports.MsgAck, Payload: ackPayload}); err != nil {
				return fmt.Errorf("ack output: %w", err)
			}
		}
		received.Write(output.Data)
		if strings.Contains(received.String(), want) {
			return nil
		}
		if received.Len() > 64<<10 {
			text := received.String()
			received.Reset()
			received.WriteString(text[len(text)-16<<10:])
		}
	}
}

func sendCommand(ctx context.Context, tr ports.Transport, slug string) error {
	id := requestID.Add(1)
	request := ports.CommandRequest{
		Version:   ports.ProtocolVersion,
		RequestID: id,
		Attached:  true,
		Slug:      slug,
	}
	payload, err := ports.MarshalCommandRequest(request)
	if err != nil {
		return err
	}
	if err := tr.Send(ports.Frame{Type: ports.MsgCommand, Payload: payload}); err != nil {
		return err
	}
	deadline, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for {
		frame, err := receiveWithTimeout(deadline, tr)
		if err != nil {
			return err
		}
		if frame.Type != ports.MsgCommandResult {
			continue
		}
		result, err := ports.UnmarshalCommandResult(frame.Payload)
		if err != nil {
			return err
		}
		if !result.OK {
			return fmt.Errorf("remote command failed (code %d): %s", result.Code, result.Text)
		}
		return nil
	}
}

func detach(ctx context.Context, tr ports.Transport) error {
	if err := tr.Send(ports.Frame{Type: ports.MsgDetach, Payload: ports.MarshalDetach(ports.Detach{})}); err != nil {
		return err
	}
	deadline, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for {
		frame, err := receiveWithTimeout(deadline, tr)
		if err != nil {
			return err
		}
		if frame.Type == ports.MsgDetached {
			return nil
		}
	}
}

func expectNoSuchTarget(ctx context.Context, tr ports.Transport, target domain.RemoteSessionTarget) error {
	target.Endpoint = targetEndpoint(target.Endpoint)
	target.DisplayOrigin = "remote"
	target.SessionName = sessionName
	if err := target.Validate(); err != nil {
		return err
	}
	hello := ports.Hello{
		Version:           ports.ProtocolVersion,
		Intent:            ports.IntentResume,
		Name:              sessionName,
		Size:              domain.Size{Cols: 80, Rows: 24},
		TermEnv:           "xterm-256color",
		TrueColor:         true,
		Env:               localEnvironment(),
		Cwd:               "/tmp/local-cwd",
		RemoteTarget:      &target,
		EnvironmentPolicy: ports.EnvironmentPolicyDaemonOwned,
	}
	payload := ports.MarshalHello(hello)
	if err := tr.Send(ports.Frame{Type: ports.MsgHello, Payload: payload}); err != nil {
		return err
	}
	frame, err := receiveWithTimeout(ctx, tr)
	if err != nil {
		return err
	}
	if frame.Type != ports.MsgError {
		return fmt.Errorf("stale target returned frame type %d", frame.Type)
	}
	message, err := ports.UnmarshalErrorMsg(frame.Payload)
	if err != nil {
		return err
	}
	if message.Code != ports.ErrNoSuchTarget {
		return fmt.Errorf("stale target returned error code %d", message.Code)
	}
	return nil
}

func targetForSession(endpoint string, session ports.RemoteCatalogSession, stopped bool) (domain.RemoteSessionTarget, error) {
	tabs := ports.CatalogTabs(session)
	if len(tabs) == 0 {
		return domain.RemoteSessionTarget{}, errors.New("catalog session has no tabs")
	}
	tab := tabs[0]
	if !stopped && session.ActiveTabID != "" {
		for _, candidate := range tabs {
			if candidate.ID == session.ActiveTabID {
				tab = candidate
				break
			}
		}
	}
	target := domain.RemoteSessionTarget{
		Endpoint:      endpoint,
		DisplayOrigin: "remote",
		LifecycleID:   session.LifecycleID,
		SessionName:   session.Name,
		Stopped:       stopped,
	}
	if stopped {
		target.StoppedTab = domain.NewStableTabSelector(domain.TabStableID(tab.ID))
	} else {
		target.LiveTabID = domain.TabStableID(tab.ID)
	}
	if err := target.Validate(); err != nil {
		return domain.RemoteSessionTarget{}, err
	}
	return target, nil
}

func targetEndpoint(endpoint string) string {
	if endpoint == "" {
		return defaultTarget
	}
	return endpoint
}

func waitForSession(ctx context.Context, catalog ports.RemoteCatalogClient, endpoint, name, state string) (ports.RemoteCatalogSession, error) {
	return waitForSessionWithTabCount(ctx, catalog, endpoint, name, state, 0)
}

func waitForSessionWithTabCount(ctx context.Context, catalog ports.RemoteCatalogClient, endpoint, name, state string, tabs int) (ports.RemoteCatalogSession, error) {
	deadline, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for {
		result, err := catalog.List(deadline, endpoint)
		if err == nil {
			for _, session := range result.Sessions {
				if session.Name == name && session.State == state && (tabs == 0 || len(ports.CatalogTabs(session)) == tabs) {
					return session, nil
				}
			}
		}
		select {
		case <-deadline.Done():
			if err != nil {
				return ports.RemoteCatalogSession{}, err
			}
			return ports.RemoteCatalogSession{}, fmt.Errorf("session %q did not reach state %q", name, state)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func waitForMissingSession(ctx context.Context, catalog ports.RemoteCatalogClient, endpoint, name string) error {
	deadline, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for {
		result, err := catalog.List(deadline, endpoint)
		if err == nil {
			found := false
			for _, session := range result.Sessions {
				found = found || session.Name == name
			}
			if !found {
				return nil
			}
		}
		select {
		case <-deadline.Done():
			return errors.New("destroyed session remained in catalog")
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func touchRemoteFlag(ctx context.Context, target, path string) error {
	if strings.HasSuffix(path, "-off") {
		path = strings.TrimSuffix(path, "-off")
		return runRemoteCommand(ctx, target, "rm", "-f", path)
	}
	return runRemoteCommand(ctx, target, "touch", path)
}

func runRemoteCommand(ctx context.Context, target string, command ...string) error {
	spec := sshstdio.BuildCommandForRemoteCommand(target, command...)
	cmd := exec.CommandContext(ctx, spec.Path, spec.Args...)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	return cmd.Run()
}

func receiveWithTimeout(ctx context.Context, tr ports.Transport) (ports.Frame, error) {
	result := make(chan struct {
		frame ports.Frame
		err   error
	}, 1)
	go func() {
		frame, err := tr.Recv()
		result <- struct {
			frame ports.Frame
			err   error
		}{frame: frame, err: err}
	}()
	select {
	case <-ctx.Done():
		return ports.Frame{}, ctx.Err()
	case outcome := <-result:
		return outcome.frame, outcome.err
	}
}
