//go:build linux

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/bnema/vev/pkg/safedir"
)

type discardTerminalOutput struct{}

func (discardTerminalOutput) Write(p []byte) (int, error) { return len(p), nil }
func (discardTerminalOutput) Sync() error                 { return nil }
func (discardTerminalOutput) Close() error                { return nil }

type fakeFDPTY struct {
	*fakePTY
	fd uintptr
}

func (p *fakeFDPTY) Fd() uintptr { return p.fd }

func TestCLILauncherSetsCanonicalClientGeometryBeforeStart(t *testing.T) {
	original := setPTYWinsize
	t.Cleanup(func() { setPTYWinsize = original })

	for _, tt := range []struct {
		name    string
		setSize func(started string, fd int, cols, rows uint16) error
		wantErr bool
	}{
		{
			name: "starts after canonical geometry",
			setSize: func(started string, _ int, cols, rows uint16) error {
				if _, err := os.Stat(started); !errors.Is(err, os.ErrNotExist) {
					return fmt.Errorf("client started before geometry was set: %w", err)
				}
				if cols != 120 || rows != 40 {
					return fmt.Errorf("geometry = %dx%d, want 120x40", cols, rows)
				}
				return nil
			},
		},
		{
			name: "winsize failure closes PTY before returning",
			setSize: func(started string, fd int, _, _ uint16) error {
				if _, err := os.Stat(started); !errors.Is(err, os.ErrNotExist) {
					return fmt.Errorf("client started after winsize failure: %w", err)
				}
				return errTestWinsize
			},
			wantErr: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			started := filepath.Join(t.TempDir(), "started")
			t.Setenv("STARTED", started)
			bin := filepath.Join(t.TempDir(), "client")
			if err := os.WriteFile(bin, []byte("#!/bin/sh\ntouch \"$STARTED\"\n"), 0o700); err != nil {
				t.Fatal(err)
			}
			runDir := filepath.Join(t.TempDir(), "run")
			if err := safedir.EnsurePrivate(runDir); err != nil {
				t.Fatal(err)
			}
			setFD := -1
			setPTYWinsize = func(fd int, cols, rows uint16) error {
				setFD = fd
				return tt.setSize(started, fd, cols, rows)
			}
			launcher := &cliLauncher{bin: bin}
			t.Cleanup(func() {
				if err := launcher.releaseRuntime(runDir); err != nil {
					t.Error(err)
				}
			})
			p, err := launcher.Launch(processMapping{Role: "client", TracePath: filepath.Join(runDir, "client.jsonl")}, roleCommand{Args: []string{"test"}})
			if tt.wantErr {
				if !errors.Is(err, errTestWinsize) || p != nil {
					t.Fatalf("Launch() = (%T, %v), want winsize error", p, err)
				}
				if setFD < 0 {
					t.Fatal("winsize setter was not called")
				}
				if closeErr := syscall.Close(setFD); !errors.Is(closeErr, syscall.EBADF) {
					t.Fatalf("winsize failure left PTY fd %d open: close error %v", setFD, closeErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if setFD < 0 {
				t.Fatal("winsize setter was not called before successful launch")
			}
			if err := p.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

var errTestWinsize = errors.New("test winsize failure")

func TestCLIProcessResize(t *testing.T) {
	original := setPTYWinsize
	t.Cleanup(func() { setPTYWinsize = original })

	var gotFD int
	var gotCols, gotRows uint16
	setPTYWinsize = func(fd int, cols, rows uint16) error {
		gotFD, gotCols, gotRows = fd, cols, rows
		return nil
	}
	p := &cliProcess{pty: &fakeFDPTY{fakePTY: &fakePTY{}, fd: 42}}
	if err := p.Resize(119, 40); err != nil {
		t.Fatal(err)
	}
	if gotFD != 42 || gotCols != 119 || gotRows != 40 {
		t.Fatalf("winsize setter got fd=%d cols=%d rows=%d", gotFD, gotCols, gotRows)
	}

	if err := (&cliProcess{pty: &fakePTY{}}).Resize(119, 40); err == nil || !strings.Contains(err.Error(), "file descriptor") {
		t.Fatalf("Resize without FD error = %v", err)
	}
}

func TestCLIProcessCloseDrainsQueuedTerminalOutputBeforeClientExit(t *testing.T) {
	harnessPTY, clientPTY := net.Pipe()
	t.Cleanup(func() { _ = clientPTY.Close() })
	waitErr := make(chan error, 1)
	secondRead := make(chan struct{})
	p := &cliProcess{
		pty:          harnessPTY,
		output:       discardTerminalOutput{},
		chunks:       make(chan []byte, 1),
		done:         make(chan struct{}),
		waitErr:      waitErr,
		waitTimeout:  func() <-chan time.Time { return time.After(time.Second) },
		forceCleanup: func() { waitErr <- nil },
	}
	go p.copyTerminal()
	go func() {
		_, _ = clientPTY.Write([]byte("first"))
		_, _ = clientPTY.Write([]byte("second"))
		close(secondRead)
	}()
	select {
	case <-secondRead:
	case <-time.After(time.Second):
		t.Fatal("terminal reader did not queue output")
	}
	closed := make(chan error, 1)
	go func() { closed <- p.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not drain queued terminal output before waiting for client exit")
	}
}

func TestCLIProcessCloseKeepsTerminalDrainActiveUntilClientExit(t *testing.T) {
	pty, err := os.CreateTemp(t.TempDir(), "pty")
	if err != nil {
		t.Fatal(err)
	}
	waiting := make(chan struct{})
	deadline := make(chan time.Time)
	waitErr := make(chan error)
	done := make(chan struct{})
	closed := make(chan error, 1)
	var waitingOnce sync.Once
	p := &cliProcess{
		pty:     pty,
		done:    done,
		waitErr: waitErr,
		waitTimeout: func() <-chan time.Time {
			waitingOnce.Do(func() { close(waiting) })
			return deadline
		},
	}
	go func() { closed <- p.Close() }()

	select {
	case <-waiting:
	case <-time.After(time.Second):
		t.Fatal("Close did not begin waiting for graceful client exit")
	}
	select {
	case <-done:
		t.Fatal("terminal drain stopped before the client exited")
	default:
	}
	waitErr <- nil
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not finish after the client exited")
	}
	select {
	case <-done:
	default:
		t.Fatal("terminal drain remained active after client exit")
	}
}

func TestCLIProcessCloseIsBoundedWhenPublicRoleDoesNotExit(t *testing.T) {
	deadline := make(chan time.Time)
	close(deadline)
	var term, kill int
	p := &cliProcess{
		waitErr:      make(chan error),
		waitTimeout:  func() <-chan time.Time { return deadline },
		forceCleanup: func() { term++ },
		forceKill:    func() { kill++ },
	}
	if err := p.Close(); err == nil {
		t.Fatal("stuck public role close succeeded")
	}
	if term != 1 || kill != 1 {
		t.Fatalf("TERM/KILL calls = %d/%d, want 1/1", term, kill)
	}
}

func TestCLIProcessCloseEscalatesHungClientAfterGracefulDrainTimeout(t *testing.T) {
	deadline := make(chan time.Time)
	close(deadline)
	var graceful, term, kill int
	p := &cliProcess{
		pty:              &orderedPTY{close: func() {}},
		waitErr:          make(chan error),
		waitTimeout:      func() <-chan time.Time { return deadline },
		gracefulShutdown: func() { graceful++ },
		forceCleanup:     func() { term++ },
		forceKill:        func() { kill++ },
	}
	if err := p.Close(); err == nil {
		t.Fatal("hung client close succeeded")
	}
	if graceful != 1 || term != 1 || kill != 1 {
		t.Fatalf("graceful/TERM/KILL calls = %d/%d/%d, want 1/1/1", graceful, term, kill)
	}
}

func TestCLIProcessCloseEscalationOutcome(t *testing.T) {
	tests := []struct {
		name        string
		reapsAt     string // "term" or "kill": which rung's callback reaps the process
		waitTimeout func() func() <-chan time.Time
		wantErrSub  string // "" means Close must return nil
		wantTerm    int
		wantKill    int
	}{
		{
			// A role reaped during the SIGTERM wait stopped gracefully: no SIGKILL,
			// and Close must not report an error for a stage that can still flush a
			// trace. The deadline channel never fires, so only the SIGTERM callback
			// reaping the process can end the wait.
			name:    "SIGTERM stop is graceful",
			reapsAt: "term",
			waitTimeout: func() func() <-chan time.Time {
				never := make(chan time.Time)
				return func() <-chan time.Time { return never }
			},
			wantErrSub: "",
			wantTerm:   1,
			wantKill:   0,
		},
		{
			// A role that only dies to SIGKILL may have truncated its trace, so
			// Close must surface a named escalation error rather than a silent
			// success. One ready deadline releases the SIGTERM wait so the ladder
			// escalates to SIGKILL, whose callback then reaps the process.
			name:    "errors after kill escalation",
			reapsAt: "kill",
			waitTimeout: func() func() <-chan time.Time {
				deadline := make(chan time.Time, 1)
				deadline <- time.Now()
				return func() <-chan time.Time { return deadline }
			},
			wantErrSub: "killed",
			wantTerm:   1,
			wantKill:   1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			waitErr := make(chan error, 1)
			var term, kill int
			p := &cliProcess{
				waitErr:     waitErr,
				waitTimeout: tt.waitTimeout(),
				forceCleanup: func() {
					term++
					if tt.reapsAt == "term" {
						waitErr <- nil
					}
				},
				forceKill: func() {
					kill++
					if tt.reapsAt == "kill" {
						waitErr <- nil
					}
				},
			}
			err := p.Close()
			if tt.wantErrSub == "" {
				if err != nil {
					t.Fatalf("Close() error = %v, want nil", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tt.wantErrSub) {
				t.Fatalf("Close() error = %v, want an error containing %q", err, tt.wantErrSub)
			}
			if term != tt.wantTerm || kill != tt.wantKill {
				t.Fatalf("TERM/KILL calls = %d/%d, want %d/%d", term, kill, tt.wantTerm, tt.wantKill)
			}
		})
	}
}

func TestCLIProcessWaitReadyRequiresDaemonSocket(t *testing.T) {
	path := filepath.Join(t.TempDir(), "daemon.sock")
	var lc net.ListenConfig
	listener, err := lc.Listen(context.Background(), "unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := listener.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := (&cliProcess{readyPath: path, waitErr: make(chan error, 1)}).WaitReady(); err != nil {
		t.Fatal(err)
	}
}

func TestCLILauncherIsolatesRoleXDGEnvironmentAcrossRepetitions(t *testing.T) {
	captureDir := t.TempDir()
	t.Setenv("CAPTURE_DIR", captureDir)
	t.Setenv("XDG_RUNTIME_DIR", filepath.Join(t.TempDir(), "inherited-runtime"))
	t.Setenv("XDG_STATE_HOME", filepath.Join(t.TempDir(), "inherited-state"))

	bin := filepath.Join(t.TempDir(), "capture-env")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\nenv > \"$CAPTURE_DIR/$VEV_PERF_PROCESS_ID\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	launcher := &cliLauncher{bin: bin}
	for run := 1; run <= 3; run++ {
		runDir := filepath.Join(t.TempDir(), fmt.Sprintf("run-%d", run))
		if err := safedir.EnsurePrivate(runDir); err != nil {
			t.Fatal(err)
		}
		roles := []string{"daemon", "client"}
		for _, role := range roles {
			id := fmt.Sprintf("run-%d-%s", run, role)
			p, err := launcher.Launch(processMapping{
				Role: role, TracePath: filepath.Join(runDir, role+".jsonl"), ProcessID: id,
			}, roleCommand{Args: []string{"test"}})
			if err != nil {
				t.Fatalf("run %d %s launch: %v", run, role, err)
			}
			// The fixture exits on its own. Synchronize with that exit instead of
			// letting Close race its SIGTERM against the environment capture.
			proc, ok := p.(*cliProcess)
			if !ok {
				t.Fatalf("run %d %s process type = %T, want *cliProcess", run, role, p)
			}
			exitErr := <-proc.waitErr
			proc.waitErr <- exitErr
			runtimeDir := launcher.runtimes[runDir]
			if err := p.Close(); err != nil {
				t.Fatalf("run %d %s close: %v", run, role, err)
			}

			env, err := os.ReadFile(filepath.Join(captureDir, id))
			if err != nil {
				t.Fatalf("run %d %s capture: %v", run, role, err)
			}
			for _, want := range []string{
				"XDG_RUNTIME_DIR=" + runtimeDir,
				"XDG_STATE_HOME=" + filepath.Join(runDir, "state"),
			} {
				name := strings.SplitN(want, "=", 2)[0]
				if count := bytes.Count(env, []byte(name+"=")); count != 1 {
					t.Fatalf("run %d %s %s entries = %d, want exactly one; env:\n%s", run, role, name, count, env)
				}
				if !bytes.Contains(env, []byte(want+"\n")) {
					t.Fatalf("run %d %s missing %q; env:\n%s", run, role, want, env)
				}
			}
		}
		if err := launcher.releaseRuntime(runDir); err != nil {
			t.Fatalf("run %d release runtime: %v", run, err)
		}
	}
}

func TestPreparePeerReleasesOwnedRuntimeOnPreparationFailure(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	runDir := filepath.Join(t.TempDir(), "run")
	if err := safedir.EnsurePrivate(runDir); err != nil {
		t.Fatal(err)
	}
	launcher := &cliLauncher{}
	_, err := launcher.preparePeer(processMapping{
		Role:      "ssh_stdio_peer",
		TracePath: filepath.Join(runDir, "peer.jsonl"),
	}, roleCommand{Args: []string{"invalid"}})
	if err == nil || !strings.Contains(err.Error(), "unsupported public peer command") {
		t.Fatalf("preparePeer error = %v", err)
	}
	if runtimeDir := launcher.runtimes[runDir]; runtimeDir != "" {
		t.Fatalf("failed peer preparation leaked runtime directory %q", runtimeDir)
	}
	if paths, globErr := filepath.Glob(filepath.Join(os.TempDir(), "vev-harness-runtime-*")); globErr != nil || len(paths) != 0 {
		t.Fatalf("failed peer preparation left runtime directories %q: %v", paths, globErr)
	}
}

func TestCLIProcessCloseUsesPublicDaemonShutdown(t *testing.T) {
	wait := make(chan error, 1)
	wait <- nil
	var shutdown int
	p := &cliProcess{waitErr: wait, shutdown: func() error { shutdown++; return nil }}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if shutdown != 0 {
		t.Fatalf("shutdown calls = %d, want 0 for an exited daemon", shutdown)
	}

	wait = make(chan error, 1)
	p = &cliProcess{waitErr: wait, shutdown: func() error { shutdown++; wait <- nil; return nil }}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if shutdown != 1 {
		t.Fatalf("shutdown calls = %d, want 1", shutdown)
	}
}

func TestHarnessCanonicalLocalSmokeRealRoles(t *testing.T) {
	if testing.Short() {
		t.Skip("public CLI smoke")
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(t.TempDir(), "vev")
	build := exec.Command("go", "build", "-o", bin, "./")
	build.Dir = root
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build public CLI: %v\n%s", err, output)
	}
	m, err := readManifest(filepath.Join(root, "testdata", "perf", "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := validateManifest(m); err != nil {
		t.Fatal(err)
	}
	var local scenario
	for _, s := range m.Scenarios {
		if s.Topology == "1x4" && s.Workload == "idle" && s.Transport == "local" {
			local = s
			break
		}
	}
	if local.ID == "" {
		t.Fatal("canonical local scenario missing")
	}
	dir := t.TempDir()
	raw, err := os.OpenFile(filepath.Join(dir, "raw.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	closeTestFile(t, raw)
	h := defaultHarness()
	h.clock = &fakeClock{} // injected duration keeps the public 30s contract bounded in test
	h.launcher = &cliLauncher{bin: bin}
	result, err := h.runOne(options{vevBin: bin, out: dir, duration: minimumDuration}, m, local, 1, raw)
	if err != nil {
		t.Fatal(err)
	}
	if result.Samples < minimumInIntervalEventSamples || len(result.Processes) != 2 {
		t.Fatalf("canonical local smoke result=%+v", result)
	}
}

func TestTraceEnvironmentMatchesProcessMapping(t *testing.T) {
	mapping := processMapping{TracePath: "/tmp/trace.jsonl", ProcessID: "p-01", Scenario: "1x4-idle-local", Run: 3}
	got := strings.Join(traceEnvironment(mapping), "\n")
	for _, want := range []string{"VEV_PERF_TRACE=/tmp/trace.jsonl", "VEV_PERF_PROCESS_ID=p-01", "VEV_PERF_SCENARIO=1x4-idle-local", "VEV_PERF_RUN=3"} {
		if !strings.Contains(got, want) {
			t.Errorf("trace environment missing %q: %s", want, got)
		}
	}
}

func TestCLIProcessCloseStopsShellBeforeProcessGroupCleanup(t *testing.T) {
	var order []string
	waitErr := make(chan error, 1)
	p := &cliProcess{
		pty:     &orderedPTY{close: func() { order = append(order, "pty_closed") }},
		output:  &orderedOutput{close: func() { order = append(order, "output_closed") }},
		waitErr: waitErr,
		gracefulShutdown: func() {
			order = append(order, "shell_exit")
			// The client closes and waits for its transport descendant before it
			// reports its own exit to the launcher.
			order = append(order, "descendant_receive_end")
			waitErr <- nil
		},
		forceCleanup: func() {
			t.Fatal("normal shutdown sent SIGTERM to the traced descendant process group")
		},
	}

	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if !equalStrings(order, []string{"shell_exit", "descendant_receive_end", "pty_closed", "output_closed"}) {
		t.Fatalf("cleanup order=%q", order)
	}
}

func TestCLIProcessWarmupWaitsForDelayedTerminalReadiness(t *testing.T) {
	allowWrite := make(chan struct{})
	pty, output := &stagedPTY{allow: allowWrite, writes: make(chan []byte, 1)}, &fakeOutput{}
	p := &cliProcess{pty: pty, output: output, chunks: make(chan []byte)}
	input := workloadInput(scenario{ID: "s"}, 1, "warmup")
	done := make(chan error, 1)
	go func() { done <- p.Warmup(input) }()

	// This is public client output from the initial application prompt/state,
	// emitted only after the client has entered raw mode. A premature PTY write
	// blocks on allowWrite and therefore cannot consume this delayed readiness.
	go func() {
		p.chunks <- []byte("shell prompt$ ")
		close(allowWrite)
	}()
	select {
	case got := <-pty.writes:
		if !bytes.Equal(got, input) {
			t.Fatalf("warmup write=%q want %q", got, input)
		}
	case <-time.After(time.Second):
		t.Fatal("warmup did not wait for and then proceed from client readiness")
	}
	go func() { p.chunks <- append([]byte("application "), inputMarker(input)...); close(p.chunks) }()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if output.syncs != 1 {
		t.Fatalf("readiness output was not flushed: syncs=%d", output.syncs)
	}
}

func TestCLIProcessRejectsPTYLocalEchoAsFlushEvidence(t *testing.T) {
	pty, output := &fakePTY{}, &fakeOutput{}
	p := &cliProcess{pty: pty, output: output, chunks: make(chan []byte, 1)}
	input := workloadInput(scenario{ID: "s"}, 1, "measured-1")
	p.chunks <- newPTYLocalEcho(input).expected
	close(p.chunks)
	flushed := false
	if err := p.Measure(input, func() error { return nil }, func() error { flushed = true; return nil }); err == nil {
		t.Fatal("local PTY echo satisfied terminal flush boundary")
	}
	if flushed || output.syncs != 0 {
		t.Fatalf("local echo stamped a flush: flushed=%t syncs=%d", flushed, output.syncs)
	}
}

func TestCLIProcessPairsApplicationOutputWithSuccessfulFlush(t *testing.T) {
	pty, output := &fakePTY{}, &fakeOutput{}
	p := &cliProcess{pty: pty, output: output, chunks: make(chan []byte, 1)}
	input := workloadInput(scenario{ID: "s"}, 1, "measured-1")
	echo := newPTYLocalEcho(input).expected
	p.chunks <- append(append([]byte(nil), echo...), append([]byte("application "), inputMarker(input)...)...)
	order := []string{}
	if err := p.Measure(input, func() error { order = append(order, "injected"); return nil }, func() error { order = append(order, "flushed"); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(pty.writes) != 1 || output.syncs != 1 || !equalStrings(order, []string{"injected", "flushed"}) {
		t.Fatalf("writes=%q syncs=%d boundary order=%q", pty.writes, output.syncs, order)
	}
}

func TestHarnessUsesPublicRoleCommandsAndPTYWorkloads(t *testing.T) {
	for _, tc := range []struct {
		role string
		want []string
	}{
		{"daemon", []string{"--daemon"}},
		{"client", []string{"new", "perf-s-001"}},
		{"ssh_stdio_peer", []string{"_stdio", "perf-s-001"}},
		{"udp_peer", []string{"_udp-proxy", "perf-s-001"}},
	} {
		t.Run(tc.role, func(t *testing.T) {
			got := roleArgs(scenario{ID: "s", Transport: "local"}, processMapping{Role: tc.role, Run: 1})
			if !equalStrings(got.Args, tc.want) {
				t.Fatalf("args=%q want %q", got.Args, tc.want)
			}
		})
	}
	input := string(workloadInput(scenario{ID: "s", Workload: "interactive_flood"}, 1, "measured-1"))
	if string(inputMarker([]byte(input))) != "__VEV_HARNESS_s_r1_measured-1__" || !bytes.Contains([]byte(input), []byte("while [ $i -lt 128 ]")) || !strings.HasSuffix(input, "printf '__VEV_HARNESS_s_r1_measured-1__\\n'\n") {
		t.Fatalf("workload is not real PTY shell input with an observable marker: %q", input)
	}
}
