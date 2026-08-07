package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bnema/vev/internal/adapters/dgram"
	"github.com/bnema/vev/internal/adapters/ipc"
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

func run() (runErr error) {
	target := os.Getenv("VEV_HARNESS_TARGET")
	if target == "" {
		target = defaultTarget
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	artifact := newHarnessArtifact(os.Getenv("VEV_HARNESS_ARTIFACT_DIR"))
	if artifact != nil {
		ctx = withHarnessArtifact(ctx, artifact)
		defer func() {
			if err := artifact.write(runErr == nil); err != nil {
				runErr = errors.Join(runErr, err)
			}
		}()
	}

	catalog := remoteadapter.NewCatalogClient()
	preview := remoteadapter.NewPreviewClient()
	if err := runLocalPickerUnitedPhase(ctx, target, catalog, preview); err != nil {
		return err
	}
	if err := runLiveStdioPhase(ctx, target, catalog, preview); err != nil {
		return err
	}
	resumedTarget, err := runRestartResumePhase(ctx, target, catalog, preview)
	if err != nil {
		return err
	}
	if err := runUDPPhase(ctx, target); err != nil {
		return err
	}
	if err := runCatalogFailurePhase(ctx, target, catalog); err != nil {
		return err
	}
	if _, err := catalog.List(ctx, "test@unreachable.invalid"); err == nil {
		return errors.New("unreachable catalog unexpectedly succeeded")
	}
	return runStaleTargetPhase(ctx, target, catalog, resumedTarget)
}

func runLiveStdioPhase(ctx context.Context, target string, catalog ports.RemoteCatalogClient, preview ports.RemotePreviewClient) error {
	stdio, err := openTransport(ctx, ports.RemoteTransportStdio, target)
	if err != nil {
		return fmt.Errorf("open stdio transport: %w", err)
	}
	defer func() { _ = stdio.Close() }()
	if err := attachAndCheck(ctx, stdio, ports.Hello{
		Intent: ports.IntentNew, Name: sessionName, Env: localEnvironment(), Cwd: "/tmp/local-cwd",
		TermEnv: "xterm-256color", TrueColor: true,
	}); err != nil {
		return fmt.Errorf("stdio live attach: %w", err)
	}
	if err := sendInputAndAwait(ctx, stdio, "printf 'VEV_REMOTE_HARNESS_ENV=%s:%s:%s:%s\\n' \"$VEV_TEST_ENV\" \"$XDG_RUNTIME_DIR\" \"$WAYLAND_DISPLAY\" \"$VEV\"", "local:/run/local:wayland-local:"); err != nil {
		return fmt.Errorf("direct client environment: %w", err)
	}
	live, err := waitForSession(ctx, catalog, target, sessionName, "running")
	if err != nil {
		return fmt.Errorf("live catalog: %w", err)
	}
	liveTarget, err := targetForSession(target, live, false)
	if err != nil {
		return fmt.Errorf("live target: %w", err)
	}
	if _, err := preview.Preview(ctx, liveTarget, 40, 8); err != nil {
		return fmt.Errorf("live preview: %w", err)
	}
	if err := sendCommand(ctx, stdio, "new-tab"); err != nil {
		return fmt.Errorf("create second tab: %w", err)
	}
	liveWithTabs, err := waitForSessionWithTabCount(ctx, catalog, target, sessionName, "running", 2)
	if err != nil {
		return fmt.Errorf("multi-tab catalog: %w", err)
	}
	beforeTabs := ports.CatalogTabs(liveWithTabs)
	if err := sendCommand(ctx, stdio, "close-tab"); err != nil {
		return fmt.Errorf("remove tab: %w", err)
	}
	after, err := waitForSessionWithTabCount(ctx, catalog, target, sessionName, "running", 1)
	if err != nil {
		return fmt.Errorf("removed-tab catalog: %w", err)
	}
	removedID, ok := removedCatalogTabID(beforeTabs, ports.CatalogTabs(after))
	if !ok {
		return errors.New("removed-tab catalog did not identify exactly one removed tab")
	}
	removedSession := liveWithTabs
	removedSession.ActiveTabID = removedID
	removedTarget, err := targetForSession(target, removedSession, false)
	if err != nil {
		return fmt.Errorf("removed-tab target: %w", err)
	}
	if _, err := preview.Preview(ctx, removedTarget, 40, 8); err == nil {
		return errors.New("removed tab unexpectedly produced a preview")
	}
	survivingTarget, err := targetForSession(target, after, false)
	if err != nil {
		return fmt.Errorf("surviving-tab target: %w", err)
	}
	if _, err := preview.Preview(ctx, survivingTarget, 40, 8); err != nil {
		return fmt.Errorf("surviving tab preview: %w", err)
	}
	if err := detach(ctx, stdio); err != nil {
		return fmt.Errorf("stop session: %w", err)
	}
	return nil
}

// runLocalPickerUnitedPhase drives the actual local picker against the SSH
// daemon container. It validates the seam that individual catalog, preview,
// and attach tests cannot: unified rows before and after selection, remote
// content publication on the same local client transport, local chrome
// composition, and continued input forwarding.
func runLocalPickerUnitedPhase(ctx context.Context, target string, catalog ports.RemoteCatalogClient, preview ports.RemotePreviewClient) (err error) {
	const (
		localSession  = "local-picker"
		remoteSession = "picker"
		previewMarker = "VEV_REMOTE_PICKER_ACTIVE"
		inputMarker   = "VEV_REMOTE_PICKER_INPUT_OK"
	)

	remote, err := openTransport(ctx, ports.RemoteTransportStdio, target)
	if err != nil {
		return fmt.Errorf("open remote picker source: %w", err)
	}
	remote.probe.configure("remote-picker-source")
	ownsRemoteSession := false
	defer func() {
		_ = remote.Close()
		if !ownsRemoteSession {
			return
		}
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if cleanupErr := runRemoteCommand(cleanup, target, "vev", "kill", remoteSession); cleanupErr != nil {
			err = errors.Join(err, fmt.Errorf("destroy remote picker session: %w", cleanupErr))
		}
	}()
	if err := attachAndCheck(ctx, remote, ports.Hello{
		Intent: ports.IntentNew, Name: remoteSession, Env: localEnvironment(), Cwd: "/tmp/remote-cwd",
		TermEnv: "xterm-256color", TrueColor: true,
	}); err != nil {
		return fmt.Errorf("attach remote picker source: %w", err)
	}
	ownsRemoteSession = true
	if err := sendCommand(ctx, remote, "new-tab"); err != nil {
		return fmt.Errorf("create remote picker tab: %w", err)
	}
	if err := sendCommand(ctx, remote, "next-tab"); err != nil {
		return fmt.Errorf("focus remote picker tab: %w", err)
	}
	if err := sendInputAndAwait(ctx, remote, "printf '"+previewMarker+"\\n'", previewMarker); err != nil {
		return fmt.Errorf("seed remote picker preview: %w", err)
	}
	remoteCatalog, err := waitForSessionWithTabCount(ctx, catalog, target, remoteSession, "running", 2)
	if err != nil {
		return fmt.Errorf("remote picker catalog: %w", err)
	}
	if remoteCatalog.ActiveTabID == "" {
		return errors.New("remote picker catalog omitted active tab")
	}
	tabs := ports.CatalogTabs(remoteCatalog)
	if len(tabs) != 2 {
		return errors.New("remote picker catalog did not retain both tabs")
	}
	selectedTarget, err := targetForSession(target, remoteCatalog, false)
	if err != nil {
		return fmt.Errorf("active remote picker target: %w", err)
	}
	if selectedTarget.LiveTabID != domain.TabStableID(remoteCatalog.ActiveTabID) {
		return errors.New("remote picker catalog active tab did not resolve to the selected target")
	}
	activeTabIndex := -1
	for i, tab := range tabs {
		if domain.TabStableID(tab.ID) == selectedTarget.LiveTabID {
			activeTabIndex = i
			break
		}
	}
	if activeTabIndex < 0 {
		return errors.New("remote picker catalog active tab was absent from tab list")
	}
	selectedPreview, err := preview.Preview(ctx, selectedTarget, 55, 22)
	if err != nil {
		return fmt.Errorf("explicit remote tab preview: %w", err)
	}
	if !remotePreviewContains(selectedPreview, previewMarker) {
		return errors.New("explicit remote tab preview omitted marker")
	}

	if err := runLocalCommand(ctx, "vev", "host", "add", target); err != nil {
		return fmt.Errorf("register remote picker host: %w", err)
	}
	localDaemon, err := startLocalDaemon(ctx)
	if err != nil {
		return fmt.Errorf("start local picker daemon: %w", err)
	}
	defer stopLocalDaemon(localDaemon)

	local, err := openLocalTransport(ctx)
	if err != nil {
		return fmt.Errorf("open local picker transport: %w", err)
	}
	// This is the one client-equivalent connection for the proof. Remote
	// selection must replace its daemon-side owner without sending an endpoint
	// or MsgAttachTarget to this transport.
	local.probe.configure("local-picker")
	defer func() { _ = local.Close() }()
	if err := attachAndCheck(ctx, local, ports.Hello{
		Intent: ports.IntentNew, Name: localSession, Env: localEnvironment(), Cwd: "/tmp/local-cwd",
		TermEnv: "xterm-256color", TrueColor: true,
	}); err != nil {
		return fmt.Errorf("attach local picker session: %w", err)
	}
	if err := sendRawInput(local, "\x1b "); err != nil {
		return fmt.Errorf("open command palette: %w", err)
	}
	if err := awaitOutputContains(ctx, local, "Commands"); err != nil {
		return fmt.Errorf("await command palette: %w", err)
	}
	if err := sendRawInput(local, "SSP\r"); err != nil {
		return fmt.Errorf("open session picker: %w", err)
	}
	if err := awaitOutputContains(ctx, local, remoteSession+"@remote"); err != nil {
		return fmt.Errorf("await remote picker catalog row: %w", err)
	}
	beforeRows := capturePickerRows(local.probe)
	if err := assertUnifiedPickerRows(beforeRows, localSession, remoteSession); err != nil {
		return fmt.Errorf("capture unified picker rows before remote selection: %w", err)
	}
	// The local selected tab is first and the remote session header is not
	// selectable, so one down reaches the first remote tab. Derive the rest
	// from the catalog's stable active tab identity instead of assuming an
	// ordinal target.
	if err := sendRawInput(local, strings.Repeat("j", activeTabIndex+1)); err != nil {
		return fmt.Errorf("select catalog active remote tab: %w", err)
	}
	if err := awaitRemotePickerPreview(ctx, local, previewMarker); err != nil {
		return fmt.Errorf("remote picker preview: %w", err)
	}
	if err := sendRawInput(local, "\r"); err != nil {
		return fmt.Errorf("commit remote picker target: %w", err)
	}
	if err := awaitOutputContains(ctx, local, remoteSession+" at remote"); err != nil {
		return fmt.Errorf("publish remote content on local transport: %w", err)
	}
	if !local.probe.contains(previewMarker) {
		return errors.New("published remote content omitted preview marker")
	}
	if err := assertNoDirectHandoff(local); err != nil {
		return err
	}
	if err := assertNoRemoteChrome(local.probe, remoteSession); err != nil {
		return fmt.Errorf("remote content chrome: %w", err)
	}
	if err := sendInputAndAwait(ctx, local, "printf '"+inputMarker+"'", inputMarker); err != nil {
		return fmt.Errorf("continued input on local transport: %w", err)
	}
	if err := assertNoDirectHandoff(local); err != nil {
		return err
	}

	// Reopen the picker through the same local connection while its owner is a
	// remote view. The navigation shell must remain local and retain both sides
	// of the unified session list.
	if err := sendRawInput(local, "\x1b "); err != nil {
		return fmt.Errorf("reopen command palette from remote content: %w", err)
	}
	if err := awaitOutputContains(ctx, local, "Commands"); err != nil {
		return fmt.Errorf("await reopened command palette: %w", err)
	}
	if err := sendRawInput(local, "SSP\r"); err != nil {
		return fmt.Errorf("reopen session picker from remote content: %w", err)
	}
	if err := awaitOutputContains(ctx, local, remoteSession+"@remote"); err != nil {
		return fmt.Errorf("await reopened remote picker row: %w", err)
	}
	afterRows := capturePickerRows(local.probe)
	if err := assertUnifiedPickerRows(afterRows, localSession, remoteSession); err != nil {
		return fmt.Errorf("capture unified picker rows after remote selection: %w", err)
	}
	if err := assertNoDirectHandoff(local); err != nil {
		return err
	}
	if err := sendRawInput(local, "\x1b"); err != nil {
		return fmt.Errorf("close reopened session picker: %w", err)
	}
	return nil
}

func runRestartResumePhase(ctx context.Context, target string, catalog ports.RemoteCatalogClient, preview ports.RemotePreviewClient) (domain.RemoteSessionTarget, error) {
	if err := runRemoteCommand(ctx, target, "vev", "kill", "--daemon"); err != nil {
		return domain.RemoteSessionTarget{}, fmt.Errorf("stop remote daemon: %w", err)
	}
	if err := runRemoteCommand(ctx, target, "vev", "--daemon-launcher"); err != nil {
		return domain.RemoteSessionTarget{}, fmt.Errorf("restart remote daemon: %w", err)
	}
	resumedCatalog, err := waitForSession(ctx, catalog, target, sessionName, "running")
	if err != nil {
		return domain.RemoteSessionTarget{}, fmt.Errorf("resumed catalog: %w", err)
	}
	resumedTarget, err := targetForSession(target, resumedCatalog, false)
	if err != nil {
		return domain.RemoteSessionTarget{}, fmt.Errorf("resumed target: %w", err)
	}
	resumed, err := openTransport(ctx, ports.RemoteTransportStdio, target)
	if err != nil {
		return domain.RemoteSessionTarget{}, fmt.Errorf("open restart stdio transport: %w", err)
	}
	defer func() { _ = resumed.Close() }()
	if err := attachAndCheck(ctx, resumed, ports.Hello{
		Intent: ports.IntentResume, Name: sessionName, Env: localEnvironment(), Cwd: "/tmp/local-cwd",
		TermEnv: "xterm-256color", TrueColor: true, RemoteTarget: &resumedTarget,
		EnvironmentPolicy: ports.EnvironmentPolicyDaemonOwned,
	}); err != nil {
		return domain.RemoteSessionTarget{}, fmt.Errorf("exact restart resume: %w", err)
	}
	if _, err := preview.Preview(ctx, resumedTarget, 40, 8); err != nil {
		return domain.RemoteSessionTarget{}, fmt.Errorf("restart preview: %w", err)
	}
	if err := touchRemoteFlag(ctx, target, "/tmp/vev-slow-preview"); err != nil {
		return domain.RemoteSessionTarget{}, fmt.Errorf("enable slow preview: %w", err)
	}
	slowCtx, slowCancel := context.WithTimeout(ctx, 150*time.Millisecond)
	_, slowErr := preview.Preview(slowCtx, resumedTarget, 40, 8)
	slowCancel()
	if err := touchRemoteFlag(ctx, target, "/tmp/vev-slow-preview-off"); err != nil {
		return domain.RemoteSessionTarget{}, fmt.Errorf("disable slow preview: %w", err)
	}
	if slowErr == nil {
		return domain.RemoteSessionTarget{}, errors.New("slow preview did not honor cancellation")
	}
	if err := detach(ctx, resumed); err != nil {
		return domain.RemoteSessionTarget{}, fmt.Errorf("stop resumed session: %w", err)
	}
	return resumedTarget, nil
}

func runUDPPhase(ctx context.Context, target string) error {
	udp, err := openTransport(ctx, ports.RemoteTransportUDP, target)
	if err != nil {
		return fmt.Errorf("open UDP transport: %w", err)
	}
	defer func() { _ = udp.Close() }()
	if err := attachAndCheck(ctx, udp, ports.Hello{
		Intent: ports.IntentNew, Name: "udp-harness", Env: localEnvironment(), Cwd: "/tmp/local-cwd",
		TermEnv: "xterm-256color", TrueColor: true,
	}); err != nil {
		return fmt.Errorf("UDP attach: %w", err)
	}
	if err := detach(ctx, udp); err != nil {
		return fmt.Errorf("UDP detach: %w", err)
	}
	return nil
}

func runCatalogFailurePhase(ctx context.Context, target string, catalog ports.RemoteCatalogClient) error {
	if err := touchRemoteFlag(ctx, target, "/tmp/vev-bad-catalog"); err != nil {
		return fmt.Errorf("enable mismatched catalog: %w", err)
	}
	if _, err := catalog.List(ctx, target); err == nil {
		return errors.New("version-mismatched catalog unexpectedly decoded")
	}
	if err := touchRemoteFlag(ctx, target, "/tmp/vev-bad-catalog-off"); err != nil {
		return fmt.Errorf("disable mismatched catalog: %w", err)
	}
	return nil
}

func runStaleTargetPhase(ctx context.Context, target string, catalog ports.RemoteCatalogClient, staleTarget domain.RemoteSessionTarget) error {
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
	defer func() { _ = stale.Close() }()
	if err := expectNoSuchTarget(ctx, stale, staleTarget); err != nil {
		return fmt.Errorf("stale target fence: %w", err)
	}
	return nil
}

func removedCatalogTabID(before, after []ports.RemoteCatalogTab) (string, bool) {
	remaining := make(map[string]struct{}, len(after))
	for _, tab := range after {
		remaining[tab.ID] = struct{}{}
	}
	var removed string
	for _, tab := range before {
		if _, exists := remaining[tab.ID]; exists {
			continue
		}
		if removed != "" {
			return "", false
		}
		removed = tab.ID
	}
	return removed, removed != ""
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

type harnessFrame struct {
	frame ports.Frame
	err   error
}

type harnessTransport struct {
	ports.Transport
	probe     *visualProbe
	frames    chan harnessFrame
	queued    []harnessFrame
	stop      chan struct{}
	closeOnce sync.Once
	closeErr  error
}

func newHarnessTransport(transport ports.Transport) *harnessTransport {
	h := &harnessTransport{
		Transport: transport,
		probe:     newVisualProbe(domain.Size{Cols: defaultProbeCols, Rows: defaultProbeRows}),
		frames:    make(chan harnessFrame, 16),
		stop:      make(chan struct{}),
	}
	go h.receiveLoop()
	return h
}

func (h *harnessTransport) receiveLoop() {
	for {
		frame, err := h.Transport.Recv()
		select {
		case h.frames <- harnessFrame{frame: frame, err: err}:
		case <-h.stop:
			return
		}
		if err != nil {
			return
		}
	}
}

func (h *harnessTransport) Close() error {
	h.closeOnce.Do(func() {
		close(h.stop)
		h.closeErr = h.Transport.Close()
	})
	return h.closeErr
}

func openTransport(ctx context.Context, mode ports.RemoteTransportMode, target string) (*harnessTransport, error) {
	var transport ports.Transport
	var err error
	switch mode {
	case ports.RemoteTransportStdio:
		transport, err = sshstdio.DialContext(ctx, target, "", slog.New(slog.DiscardHandler))
	case ports.RemoteTransportUDP:
		dialer := dgram.NewRemoteDialerWithLogger(target, "", slog.New(slog.DiscardHandler))
		dialer.BootstrapTimeout = 8 * time.Second
		dialer.ProbeTimeout = 3 * time.Second
		transport, err = dialer.Dial(ctx)
	default:
		return nil, fmt.Errorf("unsupported transport %q", mode)
	}
	if err != nil {
		return nil, err
	}
	h := newHarnessTransport(transport)
	h.probe.configure(string(mode))
	if artifact := harnessArtifactFromContext(ctx); artifact != nil {
		artifact.registerProbe(h.probe)
	}
	return h, nil
}

const commandStderrLimit = 4 << 10

type boundedStderr struct {
	bytes.Buffer
	truncated bool
}

func (b *boundedStderr) Write(p []byte) (int, error) {
	written := len(p)
	remaining := commandStderrLimit - b.Len()
	if remaining <= 0 {
		b.truncated = b.truncated || written != 0
		return written, nil
	}
	if len(p) > remaining {
		p = p[:remaining]
		b.truncated = true
	}
	_, _ = b.Buffer.Write(p)
	return written, nil
}

func (b *boundedStderr) String() string {
	stderr := b.Buffer.String()
	if b.truncated {
		stderr += "\n[stderr truncated]"
	}
	return stderr
}

func commandErrorWithStderr(err error, stderr string) error {
	if stderr = strings.TrimSpace(stderr); stderr == "" {
		return err
	}
	return fmt.Errorf("%w: stderr: %s", err, stderr)
}

func startLocalDaemon(ctx context.Context) (*exec.Cmd, error) {
	cmd := exec.CommandContext(ctx, "vev", "--daemon")
	stderr := &boundedStderr{}
	cmd.Stdout = io.Discard
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		return nil, err
	}

	deadline, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for {
		transport, err := ipc.DialContext(deadline, ipc.SocketDir())
		if err == nil {
			_ = transport.Close()
			return cmd, nil
		}
		select {
		case <-deadline.Done():
			stopLocalProcess(cmd)
			return nil, commandErrorWithStderr(deadline.Err(), stderr.String())
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func stopLocalDaemon(cmd *exec.Cmd) {
	cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = runLocalCommand(cleanup, "vev", "kill", "--daemon")
	stopLocalProcess(cmd)
}

func stopLocalProcess(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
}

func runLocalCommand(ctx context.Context, name string, args ...string) error {
	return runCapturedCommand(ctx, name, args...)
}

func runCapturedCommand(ctx context.Context, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	stderr := &boundedStderr{}
	cmd.Stdout = io.Discard
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		return commandErrorWithStderr(err, stderr.String())
	}
	return nil
}

func openLocalTransport(ctx context.Context) (*harnessTransport, error) {
	transport, err := ipc.DialContext(ctx, ipc.SocketDir())
	if err != nil {
		return nil, err
	}
	h := newHarnessTransport(transport)
	h.probe.configure("local")
	if artifact := harnessArtifactFromContext(ctx); artifact != nil {
		artifact.registerProbe(h.probe)
	}
	return h, nil
}

func attachAndCheck(ctx context.Context, tr *harnessTransport, hello ports.Hello) error {
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
	tr.probe.recordControl(probeEventWelcome)
	if err := tr.Send(ports.Frame{Type: ports.MsgTheme, Payload: ports.MarshalTheme(ports.Theme{TrueColor: hello.TrueColor, SchemeKnown: true})}); err != nil {
		return fmt.Errorf("send initial theme: %w", err)
	}
	return nil
}

func sendRawInput(tr *harnessTransport, data string) error {
	return tr.Send(ports.Frame{Type: ports.MsgInput, Payload: ports.MarshalInput(ports.Input{Data: []byte(data)})})
}

func processOutputFrame(tr *harnessTransport, frame ports.Frame) error {
	output, err := ports.UnmarshalOutput(frame.Payload)
	if err != nil {
		return err
	}
	result := tr.probe.apply(output)
	if !result.Accepted {
		if !tr.probe.resetPending {
			if err := tr.Send(ports.Frame{Type: ports.MsgOutputResetRequest, Payload: ports.MarshalOutputResetRequest(ports.OutputResetRequest{})}); err != nil {
				return err
			}
			tr.probe.resetPending = true
			tr.probe.recordControl(probeEventOutputReset)
		}
		return nil
	}
	if !result.StateBearing {
		return nil
	}
	payload, err := ports.MarshalAck(result.Ack)
	if err != nil {
		return err
	}
	if err := tr.Send(ports.Frame{Type: ports.MsgAck, Payload: payload}); err != nil {
		return err
	}
	tr.probe.resetPending = false
	tr.probe.markAcked(result)
	return nil
}

func awaitOutputContains(ctx context.Context, tr *harnessTransport, want string) error {
	deadline, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	visible := tr.probe.contains(want)
	if visible {
		return nil
	}
	for {
		frame, err := receiveWithTimeout(deadline, tr)
		if err != nil {
			return err
		}
		if frame.Type != ports.MsgOutput {
			continue
		}
		if err := processOutputFrame(tr, frame); err != nil {
			return err
		}
		nowVisible := tr.probe.contains(want)
		if !visible && nowVisible {
			return nil
		}
		visible = nowVisible
	}
}

func awaitRemotePickerPreview(ctx context.Context, tr *harnessTransport, marker string) error {
	deadline, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	sawLoading := false
	markerVisible := tr.probe.contains(marker)
	for {
		frame, err := receiveWithTimeout(deadline, tr)
		if err != nil {
			return fmt.Errorf("no rendered remote preview after %d output frames (loading=%t): %w", tr.probe.outputFrames, sawLoading, err)
		}
		if frame.Type != ports.MsgOutput {
			continue
		}
		if err := processOutputFrame(tr, frame); err != nil {
			return err
		}
		text := tr.probe.text()
		sawLoading = sawLoading || strings.Contains(text, "loading remote preview")
		if strings.Contains(text, "remote preview unavailable") {
			return errors.New("remote preview unavailable")
		}
		nowMarkerVisible := strings.Contains(text, marker)
		if !markerVisible && nowMarkerVisible {
			return nil
		}
		markerVisible = nowMarkerVisible
	}
}

func remotePreviewContains(preview ports.RemotePreview, want string) bool {
	var text strings.Builder
	for _, cell := range preview.Cells {
		if !cell.Continuation {
			text.WriteRune(cell.Rune)
		}
	}
	return strings.Contains(text.String(), want)
}

func sendInputAndAwait(ctx context.Context, tr *harnessTransport, command, want string) error {
	if err := tr.Send(ports.Frame{Type: ports.MsgInput, Payload: ports.MarshalInput(ports.Input{Data: []byte(command + "\n")})}); err != nil {
		return err
	}
	deadline, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	visible := tr.probe.contains(want)
	for {
		frame, err := receiveWithTimeout(deadline, tr)
		if err != nil {
			return fmt.Errorf("waiting for marker after %d output frames/%d bytes: %w", tr.probe.outputFrames, tr.probe.outputBytes, err)
		}
		if frame.Type != ports.MsgOutput {
			continue
		}
		if err := processOutputFrame(tr, frame); err != nil {
			return fmt.Errorf("process output: %w", err)
		}
		nowVisible := tr.probe.contains(want)
		if !visible && nowVisible {
			return nil
		}
		visible = nowVisible
	}
}

func sendCommand(ctx context.Context, tr *harnessTransport, slug string) error {
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
		if frame.Type == ports.MsgOutput {
			if err := processOutputFrame(tr, frame); err != nil {
				return err
			}
			continue
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

func detach(ctx context.Context, tr *harnessTransport) error {
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

func expectNoSuchTarget(ctx context.Context, tr *harnessTransport, target domain.RemoteSessionTarget) error {
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
	return runCapturedCommand(ctx, spec.Path, spec.Args...)
}

func assertNoDirectHandoff(tr *harnessTransport) error {
	if tr == nil || tr.probe == nil {
		return errors.New("local picker transport has no visual probe")
	}
	// Inspect frames already queued by the receiver, retaining non-handoff
	// frames for the next receiveWithTimeout call.
	for {
		select {
		case outcome := <-tr.frames:
			if outcome.frame.Type == ports.MsgAttachTarget {
				tr.probe.recordIncoming(outcome.frame)
			} else {
				tr.queued = append(tr.queued, outcome)
			}
		default:
			if tr.probe.unexpectedHandoffs != 0 {
				return fmt.Errorf("picker emitted %d direct MsgAttachTarget handoff frame(s)", tr.probe.unexpectedHandoffs)
			}
			return nil
		}
	}
}

func receiveWithTimeout(ctx context.Context, tr *harnessTransport) (ports.Frame, error) {
	if len(tr.queued) != 0 {
		outcome := tr.queued[0]
		tr.queued = tr.queued[1:]
		if outcome.err == nil {
			tr.probe.recordIncoming(outcome.frame)
		}
		return outcome.frame, outcome.err
	}
	select {
	case <-ctx.Done():
		return ports.Frame{}, ctx.Err()
	case outcome := <-tr.frames:
		if outcome.err == nil {
			tr.probe.recordIncoming(outcome.frame)
		}
		return outcome.frame, outcome.err
	}
}
