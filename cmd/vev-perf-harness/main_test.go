package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

type fakeClock struct{ tick int64 }

func (c *fakeClock) Now() int64        { c.tick += 10; return c.tick }
func (*fakeClock) Sleep(time.Duration) {}

type fakeUDPNetem struct {
	port   int
	closed bool
}

func (n *fakeUDPNetem) Port() int    { return n.port }
func (n *fakeUDPNetem) Close() error { n.closed = true; return nil }

type fakeProcess struct {
	fail       bool
	measureErr error
	closeErr   error
	warmups    [][]byte
	measures   [][]byte
	closed     bool
}

func (p *fakeProcess) Warmup(input []byte) error {
	p.warmups = append(p.warmups, append([]byte(nil), input...))
	return nil
}
func (p *fakeProcess) Measure(input []byte, injected, flush func() error) error {
	p.measures = append(p.measures, append([]byte(nil), input...))
	if p.measureErr != nil {
		return p.measureErr
	}
	if p.fail {
		return errors.New("measure failure")
	}
	if err := injected(); err != nil {
		return err
	}
	return flush()
}
func (p *fakeProcess) Close() error { p.closed = true; return p.closeErr }

type fakeLauncher struct {
	mappings        []processMapping
	args            [][]string
	commands        []roleCommand
	process         []*fakeProcess
	measureErr      map[string]error
	closeErr        map[string]error
	manifestPresent bool
}

func (l *fakeLauncher) Launch(m processMapping, command roleCommand) (launchedProcess, error) {
	l.mappings = append(l.mappings, m)
	l.args = append(l.args, append([]string(nil), command.Args...))
	l.commands = append(l.commands, command)
	_, e := os.Stat(filepath.Join(filepath.Dir(m.TracePath), "manifest.json"))
	l.manifestPresent = e == nil
	p := &fakeProcess{measureErr: l.measureErr[m.Role], closeErr: l.closeErr[m.Role]}
	l.process = append(l.process, p)
	return p, nil
}

func TestHarnessFlagsRejectShortMeasurements(t *testing.T) {
	for _, args := range [][]string{
		{"--vev-bin", "vev", "--manifest", "m", "--out", "o", "--duration", "29s", "--repetitions", "10"},
		{"--vev-bin", "vev", "--manifest", "m", "--out", "o", "--duration", "30s", "--repetitions", "9"},
		{"--manifest", "m", "--out", "o", "--duration", "30s", "--repetitions", "10"},
	} {
		if _, err := parseOptions(args); err == nil {
			t.Fatalf("parseOptions(%v) succeeded", args)
		}
	}
	if _, err := parseOptions([]string{"--vev-bin", "vev", "--manifest", "m", "--out", "o", "--duration", "30s", "--repetitions", "10"}); err == nil {
		t.Fatal("missing --warmup accepted")
	}
	if _, err := parseOptions([]string{"--vev-bin", "vev", "--manifest", "m", "--out", "o", "--warmup", "1s", "--duration", "30s", "--repetitions", "10"}); err != nil {
		t.Fatal(err)
	}
}

func TestHarnessManifestCoversCanonicalMatrix(t *testing.T) {
	m, err := readManifest(filepath.Join("..", "..", "testdata", "perf", "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := validateManifest(m); err != nil {
		t.Fatal(err)
	}
	if len(m.Scenarios) != 4*9*7 {
		t.Fatalf("scenarios=%d", len(m.Scenarios))
	}
	m.Scenarios = m.Scenarios[:len(m.Scenarios)-1]
	if err := validateManifest(m); err == nil {
		t.Fatal("missing combination accepted")
	}
}

func TestHarnessCreatesExclusiveTraceManifestAndEvidence(t *testing.T) {
	dir := t.TempDir()
	raw, err := os.OpenFile(filepath.Join(dir, "raw-harness.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	launcher := &fakeLauncher{}
	h := defaultHarness()
	h.clock = &fakeClock{}
	h.launcher = launcher
	o := options{vevBin: "ignored", out: dir, warmup: time.Second, duration: minimumDuration, repetitions: minimumRepetitions}
	r, err := h.runOne(o, manifest{}, scenario{ID: "4x4-local", Roles: []string{"daemon", "client"}}, 1, raw)
	if err != nil {
		t.Fatal(err)
	}
	if !launcher.manifestPresent {
		t.Fatal("launched before complete run manifest")
	}
	if len(r.EndToEnd) != 1 || r.Samples != 1 {
		t.Fatalf("samples=%+v", r)
	}
	seen := map[string]bool{}
	for _, m := range r.Processes {
		if m.ProcessID == "" || m.ClockDomain != m.ProcessID || m.TracePath == "" || seen[m.TracePath] {
			t.Fatalf("bad mapping %+v", m)
		}
		seen[m.TracePath] = true
		if _, err := os.Stat(m.TracePath); err != nil {
			t.Fatal(err)
		}
	}
	b, err := os.ReadFile(filepath.Join(dir, "4x4-local-run-001", "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var got runManifest
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Processes) != 2 {
		t.Fatalf("manifest=%+v", got)
	}
	if len(launcher.process) != 2 || !launcher.process[0].closed || len(launcher.process[0].warmups) != 0 || len(launcher.process[1].warmups) != 1 || len(launcher.process[1].measures) != 1 {
		t.Fatalf("process lifecycle not recorded: %+v", launcher.process)
	}
}

type fakePTY struct{ writes [][]byte }

func (p *fakePTY) Read([]byte) (int, error) { return 0, os.ErrClosed }
func (p *fakePTY) Write(b []byte) (int, error) {
	p.writes = append(p.writes, append([]byte(nil), b...))
	return len(b), nil
}
func (*fakePTY) Close() error { return nil }

type stagedPTY struct {
	allow  <-chan struct{}
	writes chan []byte
}

func (*stagedPTY) Read([]byte) (int, error) { return 0, os.ErrClosed }
func (p *stagedPTY) Write(b []byte) (int, error) {
	if p.allow != nil {
		<-p.allow
	}
	p.writes <- append([]byte(nil), b...)
	return len(b), nil
}
func (*stagedPTY) Close() error { return nil }

type fakeOutput struct {
	bytes.Buffer
	syncs int
}

func (o *fakeOutput) Sync() error { o.syncs++; return nil }
func (*fakeOutput) Close() error  { return nil }

type orderedPTY struct{ close func() }

func (*orderedPTY) Read([]byte) (int, error)    { return 0, os.ErrClosed }
func (*orderedPTY) Write(b []byte) (int, error) { return len(b), nil }
func (p *orderedPTY) Close() error {
	p.close()
	return nil
}

type orderedOutput struct{ close func() }

func (*orderedOutput) Write(b []byte) (int, error) { return len(b), nil }
func (*orderedOutput) Sync() error                 { return nil }
func (o *orderedOutput) Close() error {
	o.close()
	return nil
}

func TestCLIProcessCloseGracefulDetachCompletesSpanBeforeForcedCleanup(t *testing.T) {
	var order []string
	waitErr := make(chan error, 1)
	timeout := make(chan time.Time)
	p := &cliProcess{
		pty: &orderedPTY{close: func() {
			order = append(order, "pty_closed")
			// The graceful client detach has closed its transport and emitted its
			// adapter receive-end mark before cmd.Wait reports completion.
			order = append(order, "adapter_receive_end")
			waitErr <- nil
		}},
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
	if !equalStrings(order, []string{"pty_closed", "adapter_receive_end", "output_closed"}) {
		t.Fatalf("cleanup order=%q", order)
	}
}

func TestHarnessDoesNotInheritNestedSession(t *testing.T) {
	env := withoutEnv([]string{"VEV=session=old", "PATH=/bin", "VEV_PERF_TRACE=old"}, "VEV")
	if !equalStrings(env, []string{"PATH=/bin", "VEV_PERF_TRACE=old"}) {
		t.Fatalf("environment=%q", env)
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

func TestHarnessRoutesEveryRemoteFixtureThroughItsDeclaredPeer(t *testing.T) {
	cases := []struct {
		name      string
		transport transport
		peer      string
		wantRTT   int
		wantLoss  int
	}{
		{"ssh", transport{ID: "ssh_stdio", Kind: "ssh_stdio"}, "ssh_stdio_peer", 0, 0},
		{"udp baseline", transport{ID: "udp_baseline", Kind: "udp"}, "udp_peer", 0, 0},
		{"udp 25ms", transport{ID: "udp_25ms", Kind: "udp", RTTMS: 25}, "udp_peer", 25, 0},
		{"udp 100ms", transport{ID: "udp_100ms", Kind: "udp", RTTMS: 100}, "udp_peer", 100, 0},
		{"udp loss zero", transport{ID: "udp_loss_0pct", Kind: "udp", LossPercent: 0}, "udp_peer", 0, 0},
		{"udp loss", transport{ID: "udp_loss_1pct", Kind: "udp", LossPercent: 1}, "udp_peer", 0, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := scenario{ID: "route", Transport: tc.transport.ID}
			client := routeRoleArgs(s, processMapping{Role: "client", Run: 2}, tc.transport)
			peer := routeRoleArgs(s, processMapping{Role: tc.peer, Run: 2}, tc.transport)
			if !equalStrings(client.Args, []string{"attach", "harness@127.0.0.1:perf-route-002"}) {
				t.Fatalf("client did not use public remote attach: %q", client.Args)
			}
			if tc.peer == "ssh_stdio_peer" && !equalStrings(peer.Args, []string{"_stdio", "perf-route-002"}) {
				t.Fatalf("ssh peer command=%q", peer.Args)
			}
			if tc.peer == "udp_peer" && !equalStrings(peer.Args, []string{"_udp-proxy", "perf-route-002"}) {
				t.Fatalf("udp peer command=%q", peer.Args)
			}
			if peer.Transport.RTTMS != tc.wantRTT || peer.Transport.LossPercent != tc.wantLoss {
				t.Fatalf("peer lost manifest network settings: %+v", peer.Transport)
			}
		})
	}
}

func TestHarnessFakeRunnerRoutesClientToPeerAndCleansEveryRole(t *testing.T) {
	for _, tc := range []struct {
		name, transport, peer, peerCommand string
	}{
		{"ssh", "ssh_stdio", "ssh_stdio_peer", "_stdio"},
		{"udp baseline", "udp_baseline", "udp_peer", "_udp-proxy"},
		{"udp 25ms", "udp_25ms", "udp_peer", "_udp-proxy"},
		{"udp 100ms", "udp_100ms", "udp_peer", "_udp-proxy"},
		{"udp loss zero", "udp_loss_0pct", "udp_peer", "_udp-proxy"},
		{"udp loss", "udp_loss_1pct", "udp_peer", "_udp-proxy"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			raw, err := os.OpenFile(filepath.Join(dir, "raw.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			defer raw.Close()
			l := &fakeLauncher{}
			h := defaultHarness()
			h.clock, h.launcher = &fakeClock{}, l
			s := scenario{ID: "routed", Transport: tc.transport, Roles: []string{"daemon", "client", tc.peer}}
			if _, err := h.runOne(options{out: dir, warmup: time.Second, duration: minimumDuration, repetitions: minimumRepetitions}, manifest{}, s, 1, raw); err != nil {
				t.Fatal(err)
			}
			if len(l.mappings) != 3 || l.mappings[1].Role != tc.peer || l.mappings[2].Role != "client" {
				t.Fatalf("dependency launch order=%+v", l.mappings)
			}
			if l.commands[1].Args[0] != tc.peerCommand || l.commands[2].Args[0] != "attach" {
				t.Fatalf("commands do not connect client through declared peer: %+v", l.commands)
			}
			for i, p := range l.process {
				if !p.closed {
					t.Fatalf("role %s was not cleaned up", l.mappings[i].Role)
				}
			}
		})
	}
}

func TestCLITransportSeamOwnsExclusivePeerTraceAndCleanup(t *testing.T) {
	fixtures := []struct {
		name string
		tr   transport
		rtt  time.Duration
		loss int
	}{
		{"baseline", transport{ID: "udp_baseline", Kind: "udp"}, 0, 0},
		{"25ms", transport{ID: "udp_25ms", Kind: "udp", RTTMS: 25}, 25 * time.Millisecond, 0},
		{"100ms", transport{ID: "udp_100ms", Kind: "udp", RTTMS: 100}, 100 * time.Millisecond, 0},
		{"loss0", transport{ID: "udp_loss_0pct", Kind: "udp"}, 0, 0},
		{"loss1", transport{ID: "udp_loss_1pct", Kind: "udp", LossPercent: 1}, 0, 1},
	}
	for _, tc := range fixtures {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.Chmod(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			var configs []udpNetemConfig
			netem := &fakeUDPNetem{port: 45678}
			l := &cliLauncher{bin: "/bin/true", netemFactory: func(c udpNetemConfig) (udpNetem, error) {
				configs = append(configs, c)
				return netem, nil
			}}
			m := processMapping{ProcessID: "udp-peer", TracePath: filepath.Join(dir, "udp.jsonl"), Role: "udp_peer"}
			if err := os.WriteFile(m.TracePath, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			p, err := l.preparePeer(m, roleCommand{Args: []string{"_udp-proxy", "work"}, Transport: tc.tr})
			if err != nil {
				t.Fatal(err)
			}
			if len(configs) != 1 || configs[0].RTT != tc.rtt || configs[0].LossPercent != tc.loss || configs[0].TargetPath != filepath.Join(dir, "udp-peer.target") {
				t.Fatalf("fixture did not reach netem seam: %+v", configs)
			}
			shim, err := os.ReadFile(filepath.Join(dir, "ssh"))
			if err != nil {
				t.Fatal(err)
			}
			text := string(shim)
			for _, want := range []string{"_udp-proxy", m.TracePath, m.ProcessID, "udp-peer.target", "VEV-UDP %s %s\\n' 45678", "udp-peer.pid"} {
				if !strings.Contains(text, want) {
					t.Errorf("seam does not retain %q:\n%s", want, text)
				}
			}
			if strings.Contains(text, "VEV_PERF_UDP_") {
				t.Fatalf("ignored vev UDP environment was used instead of netem: %s", text)
			}
			// A nonexistent pid is already-cleaned-up; Close must still close the
			// harness-owned emulator.
			if err := p.Close(); err != nil {
				t.Fatal(err)
			}
			if !netem.closed {
				t.Fatal("harness netem was not cleaned up")
			}
		})
	}
}

func TestUDPNetemExecutesRTTAndLossFixtures(t *testing.T) {
	fixtures := []struct {
		name string
		rtt  time.Duration
		loss int
		sent int
		want int
	}{
		{"25ms RTT", 25 * time.Millisecond, 0, 1, 1},
		{"100ms RTT", 100 * time.Millisecond, 0, 1, 1},
		{"zero loss", 0, 0, 10, 10},
		{"one percent loss", 0, 1, 100, 99},
	}
	for _, tc := range fixtures {
		t.Run(tc.name, func(t *testing.T) {
			target, err := net.ListenPacket("udp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer target.Close()
			path := filepath.Join(t.TempDir(), "target")
			if err := os.WriteFile(path, []byte("VEV-UDP "+strconv.Itoa(target.LocalAddr().(*net.UDPAddr).Port)+" key\\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			netem, err := newUDPNetem(udpNetemConfig{RTT: tc.rtt, LossPercent: tc.loss, TargetPath: path})
			if err != nil {
				t.Fatal(err)
			}
			defer netem.Close()
			client, err := net.ListenPacket("udp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			started := time.Now()
			for i := 0; i < tc.sent; i++ {
				if _, err := client.WriteTo([]byte{byte(i)}, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: netem.Port()}); err != nil {
					t.Fatal(err)
				}
			}
			if err := target.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
				t.Fatal(err)
			}
			got := 0
			buf := make([]byte, 32)
			for got < tc.want {
				if _, _, err := target.ReadFrom(buf); err != nil {
					t.Fatalf("received %d packets, want %d: %v", got, tc.want, err)
				}
				got++
			}
			if tc.rtt > 0 {
				if elapsed := time.Since(started); elapsed < tc.rtt/2 {
					t.Fatalf("one-way netem delay=%s, want at least %s", elapsed, tc.rtt/2)
				}
			}
			// Keep the deadline short: extra carriage would prove the exact 1%%
			// loss cycle was not enforced.
			if err := target.SetReadDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
				t.Fatal(err)
			}
			if _, _, err := target.ReadFrom(buf); tc.want != tc.sent && err == nil {
				t.Fatalf("received more than %d packets with %d%% loss", tc.want, tc.loss)
			}
		})
	}
}

func TestHarnessRejectsInvalidBoundaryPairs(t *testing.T) {
	for _, marks := range [][]harnessMark{
		{{Sequence: 1, Kind: "terminal_flushed", Tick: 1, Valid: true}},
		{{Sequence: 1, Kind: "input_injected", Tick: 2, Valid: true}, {Sequence: 1, Kind: "terminal_flushed", Tick: 1, Valid: true}},
		{{Sequence: 1, Kind: "input_injected", Tick: 1, Valid: true}},
		{{Sequence: 1, Kind: "input_injected", Tick: 1, Valid: true}, {Sequence: 1, Kind: "input_injected", Tick: 2, Valid: true}},
	} {
		if _, err := pairedSamples(marks); err == nil {
			t.Fatalf("invalid marks accepted: %+v", marks)
		}
	}
	got, err := pairedSamples([]harnessMark{{Sequence: 9, Kind: "input_injected", Tick: 1, Valid: true}, {Sequence: 9, Kind: "terminal_flushed", Tick: 5, Valid: true}})
	if err != nil || len(got) != 1 || got[0] != 4 {
		t.Fatalf("got %v %v", got, err)
	}
}

func TestHarnessPreservesWorkloadAndCleanupErrors(t *testing.T) {
	dir := t.TempDir()
	raw, err := os.OpenFile(filepath.Join(dir, "raw.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	primary := errors.New("primary workload failure")
	cleanup := errors.New("daemon cleanup failure")
	launcher := &fakeLauncher{
		measureErr: map[string]error{"client": primary},
		closeErr:   map[string]error{"daemon": cleanup},
	}
	h := defaultHarness()
	h.clock, h.launcher = &fakeClock{}, launcher
	_, err = h.runOne(options{out: dir, warmup: time.Second, duration: minimumDuration, repetitions: minimumRepetitions}, manifest{}, scenario{ID: "errors", Roles: []string{"daemon", "client"}}, 1, raw)
	if !errors.Is(err, primary) || !errors.Is(err, cleanup) {
		t.Fatalf("workload and cleanup errors were not both preserved: %v", err)
	}
	if got, want := err.Error(), primary.Error()+"\n"+cleanup.Error(); got != want {
		t.Fatalf("error ordering=%q want %q", got, want)
	}
	for i, p := range launcher.process {
		if !p.closed {
			t.Fatalf("role %s was not closed", launcher.mappings[i].Role)
		}
	}
}

func TestHarnessCleansRunDirectoryOnProcessFailure(t *testing.T) {
	dir := t.TempDir()
	raw, err := os.OpenFile(filepath.Join(dir, "raw-harness.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	launcher := &fakeLauncher{}
	h := defaultHarness()
	h.clock = &fakeClock{}
	// The client is the second role and fails after the daemon has started.
	h.launcher = &failingLauncher{fakeLauncher: launcher}
	o := options{vevBin: "ignored", out: dir, warmup: time.Second, duration: minimumDuration, repetitions: minimumRepetitions}
	if _, err := h.runOne(o, manifest{}, scenario{ID: "cleanup", Roles: []string{"daemon", "client"}}, 1, raw); err == nil {
		t.Fatal("failed process accepted")
	}
	if _, err := os.Stat(filepath.Join(dir, "cleanup-run-001")); !os.IsNotExist(err) {
		t.Fatalf("failed run directory remains: %v", err)
	}
}

type failingLauncher struct{ *fakeLauncher }

func (l *failingLauncher) Launch(m processMapping, command roleCommand) (launchedProcess, error) {
	p, err := l.fakeLauncher.Launch(m, command)
	if err != nil {
		return nil, err
	}
	if m.Role == "client" {
		p.(*fakeProcess).fail = true
	}
	return p, nil
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestHarnessRawRecordsAreDeterministicJSONL(t *testing.T) {
	var b bytes.Buffer
	for _, m := range []harnessMark{{Scenario: "s", Run: 1, Sequence: 1, Kind: "input_injected", Tick: 1, Valid: true}, {Scenario: "s", Run: 1, Sequence: 1, Kind: "terminal_flushed", Tick: 2, Valid: true}} {
		b.WriteString(mustJSON(m))
		b.WriteByte('\n')
	}
	path := filepath.Join(t.TempDir(), "raw.jsonl")
	if err := os.WriteFile(path, b.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := readJSONL(path); err != nil || len(got) != 2 {
		t.Fatalf("records=%d err=%v", len(got), err)
	}
}

type traceLauncher struct{ fakeLauncher }

func (l *traceLauncher) Launch(m processMapping, command roleCommand) (launchedProcess, error) {
	p, err := l.fakeLauncher.Launch(m, command)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(m.TracePath, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	for i, pair := range spanPairs {
		for n, kind := range []string{pair.start, pair.end} {
			b, _ := json.Marshal(traceRecord{ProcessID: m.ProcessID, Component: m.Role, Scenario: m.Scenario, Run: uint64(m.Run), Sequence: uint64(i + 1), RequestID: 1, Epoch: 1, Kind: kind, Tick: int64(i*20 + n*5)})
			if _, err := f.Write(append(b, '\n')); err != nil {
				return nil, err
			}
		}
	}
	return p, nil
}

type closeTraceProcess struct {
	*fakeProcess
	traceEnd func() error
}

func (p *closeTraceProcess) Close() error {
	p.closed = true
	return errors.Join(p.closeErr, p.traceEnd())
}

type closeTraceLauncher struct{ fakeLauncher }

func (l *closeTraceLauncher) Launch(m processMapping, command roleCommand) (launchedProcess, error) {
	p, err := l.fakeLauncher.Launch(m, command)
	if err != nil {
		return nil, err
	}
	start := traceRecord{ProcessID: m.ProcessID, Component: m.Role, Scenario: m.Scenario, Run: uint64(m.Run), Sequence: 1, RequestID: 1, Epoch: 1, Kind: "adapter_receive_start", Tick: 10}
	if err := appendTraceRecord(m.TracePath, start); err != nil {
		return nil, err
	}
	return &closeTraceProcess{fakeProcess: p.(*fakeProcess), traceEnd: func() error {
		return appendTraceRecord(m.TracePath, traceRecord{ProcessID: m.ProcessID, Component: m.Role, Scenario: m.Scenario, Run: uint64(m.Run), Sequence: 1, RequestID: 1, Epoch: 1, Kind: "adapter_receive_end", Tick: 20})
	}}, nil
}

func appendTraceRecord(path string, record traceRecord) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	b, err := json.Marshal(record)
	if err != nil {
		return err
	}
	_, err = f.Write(append(b, '\n'))
	return err
}

func TestHarnessClosesRolesBeforeMergingReceiveSpans(t *testing.T) {
	dir := t.TempDir()
	raw, err := os.OpenFile(filepath.Join(dir, "raw.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	launcher := &closeTraceLauncher{}
	h := defaultHarness()
	h.clock, h.launcher = &fakeClock{}, launcher
	result, err := h.runOne(options{out: dir, warmup: time.Second, duration: minimumDuration, repetitions: minimumRepetitions}, manifest{}, scenario{ID: "receive-cleanup", Roles: []string{"daemon", "client"}}, 1, raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Spans) != 2 {
		t.Fatalf("post-cleanup receive spans=%+v", result.Spans)
	}
	for _, s := range result.Spans {
		if s.Name != "adapter_receive_duration" || len(s.Samples) != 1 || s.Samples[0] != 10 {
			t.Fatalf("unmatched or wrong receive span: %+v", s)
		}
	}
	for i, p := range launcher.process {
		if !p.closed {
			t.Fatalf("role %s was not closed before trace merge", launcher.mappings[i].Role)
		}
	}
}

func TestHarnessWritesRequiredEvidenceWithSufficientSpans(t *testing.T) {
	base, err := readManifest(filepath.Join("..", "..", "testdata", "perf", "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	for i := range base.Scenarios {
		if i > 0 {
			base.Scenarios[i].InapplicableReason = "fixture limits this test to one public topology"
			base.Scenarios[i].Roles = nil
		}
	}
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "manifest.json")
	if err := os.WriteFile(manifestPath, []byte(mustJSON(base)), 0o600); err != nil {
		t.Fatal(err)
	}
	l := &traceLauncher{}
	h := defaultHarness()
	h.clock = &fakeClock{}
	h.launcher = l
	h.gitSHA = func() string { return "test-sha" }
	if err := run([]string{"--vev-bin", "ignored", "--manifest", manifestPath, "--out", filepath.Join(dir, "out"), "--warmup", "1s", "--duration", "30s", "--repetitions", "10"}, h); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"raw-harness.jsonl", "runs.json", "summary.json"} {
		if _, err := os.Stat(filepath.Join(dir, "out", name)); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	var got summary
	b, err := os.ReadFile(filepath.Join(dir, "out", "summary.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got.EndToEnd.Count != 10 || got.Repetitions != 10 || len(got.Spans) == 0 {
		t.Fatalf("summary=%+v", got)
	}
	for _, s := range got.Spans {
		if s.Distribution.Count < 10 {
			t.Fatalf("insufficient span summary: %+v", s)
		}
	}
}
func TestHarnessRejectsCrossProcessAndBadTraceSpans(t *testing.T) {
	write := func(t *testing.T, records ...traceRecord) processMapping {
		t.Helper()
		path := filepath.Join(t.TempDir(), "trace.jsonl")
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range records {
			b, _ := json.Marshal(r)
			_, _ = f.Write(append(b, '\n'))
		}
		_ = f.Close()
		return processMapping{ProcessID: "one", ClockDomain: "one", TracePath: path, Scenario: "s", Run: 1}
	}
	base := func(kind string, tick int64) traceRecord {
		return traceRecord{ProcessID: "one", Component: "daemon", Scenario: "s", Run: 1, Sequence: 1, RequestID: 1, Epoch: 1, Kind: kind, Tick: tick}
	}
	for _, records := range [][]traceRecord{
		{base("diff_end", 1)},
		{base("diff_start", 2), base("diff_end", 1)},
		{base("diff_start", 1)},
		{func() traceRecord { r := base("diff_start", 1); r.ProcessID = "other"; return r }()},
		{func() traceRecord { r := base("diff_start", 1); r.Scenario = "other-scenario"; return r }()},
		{func() traceRecord { r := base("diff_start", 1); r.Run = 2; return r }()},
	} {
		if _, err := mergeProcessTraces([]processMapping{write(t, records...)}); err == nil {
			t.Fatalf("invalid trace accepted: %+v", records)
		}
	}
	m := write(t, base("diff_start", 1), base("diff_end", 3))
	spans, err := mergeProcessTraces([]processMapping{m})
	if err != nil || len(spans) != 1 || spans[0].Samples[0] != 2 {
		t.Fatalf("spans=%+v err=%v", spans, err)
	}
}
