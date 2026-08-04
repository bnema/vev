//go:build linux

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestClientTracePairsAfterPTYHangup drives the real teardown escalation: a
// live client is severed by closing its controlling PTY master (the harness
// hangup stage) with no graceful "exit\n". A client that dies from the default
// SIGHUP disposition truncates its receive pump's in-flight span, so its trace
// merge fails with an unpaired adapter_receive_start. A client that survives the
// hangup closes its transport and serializes the matching receive-end first, so
// the merge pairs.
func TestClientTracePairsAfterPTYHangup(t *testing.T) {
	if testing.Short() {
		t.Skip("public CLI hangup lifecycle")
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	// Keep the run directory (and thus the Unix socket path) short: MkdirTemp in
	// the system temp root stays well under the platform socket-path limit.
	runDir, err := os.MkdirTemp("", "vev-hangup-")
	if err != nil {
		t.Fatal(err)
	}
	removeTestTree(t, runDir)
	bin := filepath.Join(runDir, "vev")
	build := exec.Command("go", "build", "-o", bin, "./")
	build.Dir = root
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build public CLI: %v\n%s", err, output)
	}

	s := scenario{ID: "hangup-local", Transport: "local", Workload: "idle", Roles: []string{"daemon", "client"}}
	selected, err := manifestTransport(manifest{}, s.Transport)
	if err != nil {
		t.Fatal(err)
	}

	maps := make([]processMapping, 0, len(s.Roles))
	for i, role := range s.Roles {
		pid := fmt.Sprintf("%s-r%03d-%s-%02d", safeName(s.ID), 1, safeName(role), i+1)
		path := filepath.Join(runDir, pid+".jsonl")
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		maps = append(maps, processMapping{ProcessID: pid, ClockDomain: pid, TracePath: path, Role: role, Scenario: s.ID, Run: 1, Identity: "harness-assigned:" + pid})
	}

	launcher := &cliLauncher{bin: bin}
	var daemonProc launchedProcess
	var client *cliProcess
	var clientMapping processMapping
	for _, pm := range launchOrder(maps) {
		p, err := launcher.Launch(pm, routeRoleArgs(s, pm, selected))
		if err != nil {
			t.Fatalf("launch %s: %v", pm.Role, err)
		}
		switch pm.Role {
		case "daemon":
			daemonProc = p
			ready, ok := p.(interface{ WaitReady() error })
			if !ok {
				t.Fatal("daemon role does not expose WaitReady")
			}
			if err := ready.WaitReady(); err != nil {
				t.Fatalf("daemon readiness: %v", err)
			}
		case "client":
			cp, ok := p.(*cliProcess)
			if !ok {
				t.Fatalf("client role is %T, want *cliProcess", p)
			}
			client, clientMapping = cp, pm
		}
	}
	if client == nil || daemonProc == nil {
		t.Fatal("did not launch both roles")
	}
	// Close the daemon last, on every exit path, so a failed assertion cannot
	// leak a foreground daemon or its runtime directory.
	defer func() {
		if err := daemonProc.Close(); err != nil {
			t.Errorf("close daemon role: %v", err)
		}
	}()
	// Close the client on every exit path so a failed assertion cannot leak the
	// process. Close is idempotent (sync.Once): the explicit call below, once
	// reached, is the one whose result is asserted; this defer is a backstop.
	defer func() {
		if err := client.Close(); err != nil {
			t.Errorf("close client role: %v", err)
		}
	}()

	// Wait until the client is attached with a receive pump blocked mid-frame:
	// its trace then holds one more adapter_receive_start than end. That in-flight
	// span is exactly what an ungraceful death truncates.
	if err := waitForInflightReceive(clientMapping.TracePath, 10*time.Second); err != nil {
		t.Fatalf("client never reached a blocked receive: %v", err)
	}

	// Sever the controlling terminal with no graceful exit. This is the harness
	// hangup escalation stage, delivering SIGHUP to the client session leader.
	if err := client.pty.Close(); err != nil {
		t.Fatalf("close client PTY master: %v", err)
	}
	// Close runs the bounded escalation ladder; the hangup above already severed
	// the PTY, so this should reap gracefully without reaching SIGTERM/SIGKILL.
	if err := client.Close(); err != nil {
		t.Fatalf("client did not exit cleanly after PTY hangup: %v", err)
	}

	// The severed client's trace must pair. On the unfixed client this fails with
	// "missing process-local span pair" for the truncated adapter_receive span.
	if _, err := mergeProcessTraces([]processMapping{clientMapping}); err != nil {
		t.Fatalf("client trace did not pair after PTY hangup: %v", err)
	}
}

// TestClientGracefulShutdownDrainsTracedStdioDescendant proves the launcher's
// normal client stop exits the attached shell before sending any signal. The
// client then closes and waits for its ssh transport, giving the traced _stdio
// descendant time to serialize its blocked receive's end mark and exit before
// the client is reaped.
func TestClientGracefulShutdownDrainsTracedStdioDescendant(t *testing.T) {
	if testing.Short() {
		t.Skip("public CLI stdio descendant lifecycle")
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	runDir, err := os.MkdirTemp("", "vev-stdio-drain-")
	if err != nil {
		t.Fatal(err)
	}
	removeTestTree(t, runDir)
	bin := filepath.Join(runDir, "vev")
	build := exec.Command("go", "build", "-o", bin, "./")
	build.Dir = root
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build public CLI: %v\n%s", err, output)
	}

	s := scenario{ID: "stdio-drain", Roles: []string{"daemon", "ssh_stdio_peer", "client"}}
	maps := make([]processMapping, 0, len(s.Roles))
	for i, role := range s.Roles {
		id := fmt.Sprintf("%s-r001-%s-%02d", safeName(s.ID), safeName(role), i+1)
		path := filepath.Join(runDir, id+".jsonl")
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		maps = append(maps, processMapping{ProcessID: id, ClockDomain: id, TracePath: path, Role: role, Scenario: s.ID, Run: 1, Identity: "harness-assigned:" + id})
	}

	launcher := &cliLauncher{bin: bin}
	var daemon, peer launchedProcess
	var client *cliProcess
	var peerMapping processMapping
	for _, pm := range launchOrder(maps) {
		var command roleCommand
		switch pm.Role {
		case "daemon":
			command = roleCommand{Args: []string{"--daemon"}}
		case "ssh_stdio_peer":
			// The transport-neutral _stdio mode requests an ephemeral session.
			command = roleCommand{Args: []string{"_stdio"}, Transport: transport{ID: "ssh_stdio", Kind: "ssh_stdio"}}
		case "client":
			command = roleCommand{Args: []string{"attach", "harness@127.0.0.1"}}
		}
		process, err := launcher.Launch(pm, command)
		if err != nil {
			t.Fatalf("launch %s: %v", pm.Role, err)
		}
		switch pm.Role {
		case "daemon":
			daemon = process
			readyProcess, ok := process.(interface{ WaitReady() error })
			if !ok {
				t.Fatalf("daemon process type %T does not support readiness", process)
			}
			if err := readyProcess.WaitReady(); err != nil {
				t.Fatalf("daemon readiness: %v", err)
			}
		case "ssh_stdio_peer":
			peer, peerMapping = process, pm
		case "client":
			var ok bool
			client, ok = process.(*cliProcess)
			if !ok {
				t.Fatalf("client process type = %T, want *cliProcess", process)
			}
		}
	}
	if daemon == nil || peer == nil || client == nil {
		t.Fatal("did not launch complete stdio process tree")
	}
	defer func() {
		if err := peer.Close(); err != nil {
			t.Errorf("close peer role: %v", err)
		}
		if err := daemon.Close(); err != nil {
			t.Errorf("close daemon role: %v", err)
		}
	}()
	defer func() {
		if err := client.Close(); err != nil {
			t.Errorf("close client role: %v", err)
		}
	}()

	if err := waitForInflightReceive(peerMapping.TracePath, 10*time.Second); err != nil {
		t.Fatalf("stdio descendant never reached blocked receive: %v", err)
	}
	processGroup := client.cmd.Process.Pid
	if err := client.Close(); err != nil {
		t.Fatalf("graceful client close: %v", err)
	}
	if _, err := mergeProcessTraces([]processMapping{peerMapping}); err != nil {
		t.Fatalf("stdio descendant trace did not drain: %v", err)
	}
	if err := waitForProcessGroupGone(processGroup, 10*time.Second); err != nil {
		t.Fatal(err)
	}
}

func waitForProcessGroupGone(pgid int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		err := syscall.Kill(-pgid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("probe process group %d: %w", pgid, err)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("process group %d remained after client shutdown", pgid)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitForInflightReceive polls a process trace until it holds an unpaired
// adapter_receive_start (more starts than ends). It tolerates a partially
// written trailing line while the traced process appends concurrently.
func waitForInflightReceive(path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		starts, ends := 0, 0
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			var r traceRecord
			if json.Unmarshal([]byte(line), &r) != nil {
				continue // a concurrently appended, not-yet-complete final line
			}
			switch r.Kind {
			case "adapter_receive_start":
				starts++
			case "adapter_receive_end":
				ends++
			}
		}
		if starts > ends {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("adapter_receive starts=%d ends=%d, want an in-flight start", starts, ends)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
