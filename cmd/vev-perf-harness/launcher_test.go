package main

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

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

func TestCLIProcessWaitReadyRequiresDaemonSocket(t *testing.T) {
	path := filepath.Join(t.TempDir(), "daemon.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
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
		if err := os.MkdirAll(runDir, 0o700); err != nil {
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
	build := exec.Command("/usr/local/go/bin/go", "build", "-o", bin, "./")
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
	defer raw.Close()
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

func TestCLIProcessCloseGracefulDetachCompletesSpanBeforeForcedCleanup(t *testing.T) {
	var order []string
	waitErr := make(chan error, 1)
	timeout := make(chan time.Time)
	p := &cliProcess{
		pty: &orderedPTY{
			write: func(input []byte) {
				if string(input) != "exit\n" {
					t.Fatalf("graceful exit input=%q", input)
				}
				order = append(order, "exit_written")
				// The graceful client exit has closed its transport and emitted
				// its adapter receive-end mark before cmd.Wait completes.
				order = append(order, "adapter_receive_end")
				waitErr <- nil
			},
			close: func() { order = append(order, "pty_closed") },
		},
		output:      &orderedOutput{close: func() { order = append(order, "output_closed") }},
		waitErr:     waitErr,
		waitTimeout: func() <-chan time.Time { return timeout },
		forceCleanup: func() {
			order = append(order, "forced_process_group_cleanup")
		},
	}

	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if !equalStrings(order, []string{"exit_written", "adapter_receive_end", "pty_closed", "output_closed"}) {
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
