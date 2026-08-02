package sshstdio

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
)

func TestBuildCommandUsesExecArgs(t *testing.T) {
	// BuildCommand deliberately starts `ssh -- target 'vev' '_stdio' [session]`.
	// Terminal color capability is carried in vev's Hello message, not in ssh env.
	tests := []struct {
		name    string
		target  string
		session string
		want    []string
	}{
		{name: "no session", target: "user@example.com", want: []string{"--", "user@example.com", "'vev' '_stdio'"}},
		{name: "session shell metacharacters are quoted in remote command", target: "user@example.com", session: "work; rm -rf /", want: []string{"--", "user@example.com", "'vev' '_stdio' 'work; rm -rf /'"}},
		{name: "single quote in session is posix escaped", target: "user@example.com", session: "it's fine", want: []string{"--", "user@example.com", "'vev' '_stdio' 'it'\\''s fine'"}},
		{name: "target kept as single local arg after option terminator", target: "user@host; touch /tmp/pwn", session: "work", want: []string{"--", "user@host; touch /tmp/pwn", "'vev' '_stdio' 'work'"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := BuildCommand(tt.target, tt.session)
			if got.Path != "ssh" {
				t.Fatalf("Path = %q, want ssh", got.Path)
			}
			if len(got.Args) != len(tt.want) {
				t.Fatalf("Args len = %d, want %d (%q)", len(got.Args), len(tt.want), got.Args)
			}
			for i := range tt.want {
				if got.Args[i] != tt.want[i] {
					t.Fatalf("Args[%d] = %q, want %q (all args %q)", i, got.Args[i], tt.want[i], got.Args)
				}
			}
		})
	}
}

func TestBuildCommandForModeUsesCanonicalSSHArgs(t *testing.T) {
	got := BuildCommandForMode("user@example.com", "_udp-bootstrap", "work")
	want := []string{"--", "user@example.com", "'vev' '_udp-bootstrap' 'work'"}
	if got.Path != "ssh" {
		t.Fatalf("Path = %q, want ssh", got.Path)
	}
	for i := range want {
		if got.Args[i] != want[i] {
			t.Fatalf("Args[%d] = %q, want %q (all args %q)", i, got.Args[i], want[i], got.Args)
		}
	}
}

func TestBuildCommandForRemoteCommandQuotesEveryWord(t *testing.T) {
	tests := []struct {
		name    string
		target  string
		command []string
		want    []string
	}{
		{
			name:    "catalog command",
			target:  "arch",
			command: []string{"vev", "cmd", "remote-catalog", "--json"},
			want:    []string{"--", "arch", "'vev' 'cmd' 'remote-catalog' '--json'"},
		},
		{
			name:    "metacharacters in remote words are quoted",
			target:  "user@host; touch /tmp/pwn",
			command: []string{"vev", "cmd", "remote-catalog; rm -rf /", "--json"},
			want:    []string{"--", "user@host; touch /tmp/pwn", "'vev' 'cmd' 'remote-catalog; rm -rf /' '--json'"},
		},
		{
			name:    "single quotes are posix escaped",
			target:  "arch",
			command: []string{"it's", "fine"},
			want:    []string{"--", "arch", "'it'\\''s' 'fine'"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := BuildCommandForRemoteCommand(tt.target, tt.command...)
			if got.Path != "ssh" {
				t.Fatalf("Path = %q, want ssh", got.Path)
			}
			if len(got.Args) != len(tt.want) {
				t.Fatalf("Args len = %d, want %d (%q)", len(got.Args), len(tt.want), got.Args)
			}
			for i := range tt.want {
				if got.Args[i] != tt.want[i] {
					t.Fatalf("Args[%d] = %q, want %q (all args %q)", i, got.Args[i], tt.want[i], got.Args)
				}
			}
		})
	}
}

func TestTransportRoundTripAndVersionMismatchFrame(t *testing.T) {
	clientRead, serverWrite := io.Pipe()
	serverRead, clientWrite := io.Pipe()
	client := NewTransport(clientRead, clientWrite, func() error {
		_ = clientRead.Close()
		return clientWrite.Close()
	})
	server := NewTransport(serverRead, serverWrite, func() error {
		_ = serverRead.Close()
		return serverWrite.Close()
	})
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()

	hello := ports.Hello{Version: ports.ProtocolVersion + 1, Intent: ports.IntentAttach, Name: "work"}
	go func() {
		f, err := server.Recv()
		if err != nil {
			t.Errorf("server Recv: %v", err)
			return
		}
		got, err := ports.UnmarshalHello(f.Payload)
		if err != nil {
			t.Errorf("UnmarshalHello: %v", err)
			return
		}
		if got.Version == ports.ProtocolVersion {
			t.Errorf("test did not send a mismatched version")
		}
		_ = server.Send(ports.Frame{Type: ports.MsgError, Payload: ports.MarshalErrorMsg(ports.ErrorMsg{Code: ports.ErrVersionMismatch, Text: "protocol version mismatch"})})
	}()

	if err := client.Send(ports.Frame{Type: ports.MsgHello, Payload: ports.MarshalHello(hello)}); err != nil {
		t.Fatalf("client Send: %v", err)
	}
	reply, err := client.Recv()
	if err != nil {
		t.Fatalf("client Recv: %v", err)
	}
	if reply.Type != ports.MsgError {
		t.Fatalf("reply type = %d, want MsgError", reply.Type)
	}
	em, err := ports.UnmarshalErrorMsg(reply.Payload)
	if err != nil {
		t.Fatalf("UnmarshalErrorMsg: %v", err)
	}
	if em.Code != ports.ErrVersionMismatch {
		t.Fatalf("error code = %d, want ErrVersionMismatch", em.Code)
	}
}

func TestTransportRejectsZeroLengthFrame(t *testing.T) {
	tr := NewTransport(strings.NewReader("\x00\x00\x00\x00"), io.Discard, nil)
	_, err := tr.Recv()
	if !errors.Is(err, ErrZeroLengthFrame) {
		t.Fatalf("Recv error = %v, want ErrZeroLengthFrame", err)
	}
}

func TestTransportUsesCanonicalFrameMaximum(t *testing.T) {
	boundaryPayload := make([]byte, ports.MaxFrameLen-1)
	boundaryWire := &bytes.Buffer{}
	boundarySend := NewTransport(nil, boundaryWire, nil)
	if err := boundarySend.Send(ports.Frame{Type: ports.MsgOutput, Payload: boundaryPayload}); err != nil {
		t.Fatalf("boundary Send error = %v", err)
	}
	if got := binary.BigEndian.Uint32(boundaryWire.Bytes()[:frameHeaderLen]); got != ports.MaxFrameLen {
		t.Fatalf("boundary frame length = %d, want %d", got, ports.MaxFrameLen)
	}
	boundaryRecv := NewTransport(bytes.NewReader(boundaryWire.Bytes()), io.Discard, nil)
	boundaryFrame, err := boundaryRecv.Recv()
	if err != nil {
		t.Fatalf("boundary Recv error = %v", err)
	}
	if boundaryFrame.Type != ports.MsgOutput || len(boundaryFrame.Payload) != len(boundaryPayload) {
		t.Fatalf("boundary frame = type %d, payload %d bytes; want type %d, payload %d bytes", boundaryFrame.Type, len(boundaryFrame.Payload), ports.MsgOutput, len(boundaryPayload))
	}
	if !bytes.Equal(boundaryFrame.Payload, boundaryPayload) {
		t.Fatal("boundary payload was corrupted")
	}

	send := NewTransport(nil, io.Discard, nil)
	err = send.Send(ports.Frame{Type: ports.MsgOutput, Payload: make([]byte, ports.MaxFrameLen)})
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("Send error = %v, want ErrFrameTooLarge", err)
	}

	var header [frameHeaderLen]byte
	binary.BigEndian.PutUint32(header[:], ports.MaxFrameLen+1)
	recv := NewTransport(bytes.NewReader(header[:]), io.Discard, nil)
	_, err = recv.Recv()
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("Recv error = %v, want ErrFrameTooLarge", err)
	}
}

func TestDialContextCanceledBeforeStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := DialContext(ctx, "example.com", "work")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("DialContext error = %v, want context.Canceled", err)
	}
}

func TestRecvMapsUnexpectedEOFDuringHeaderToSSHExit(t *testing.T) {
	sshErr := errors.New("ssh exited with status 255")
	tr := newTransport(strings.NewReader("\x00"), io.Discard, nil, func() error { return sshErr })

	_, err := tr.Recv()
	if !errors.Is(err, sshErr) {
		t.Fatalf("Recv error = %v, want %v", err, sshErr)
	}
}

func TestRecvReportsSSHExitWhenProcessClosesBeforeFrame(t *testing.T) {
	cmd := exec.CommandContext(context.Background(), "sh", "-c", "echo connection refused >&2; exit 255")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("StdinPipe: %v", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("StdoutPipe: %v", err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	waiter := newProcessWaiter(cmd, stdin, &stderr, time.Second, nil, "user@example.com", "work")
	tr := newTransport(stdout, stdin, waiter.close, waiter.eofErr)

	_, err = tr.Recv()
	if err == nil {
		t.Fatal("Recv error = nil, want ssh exit error")
	}
	for _, want := range []string{"sshstdio: ssh exited:", "connection refused"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("Recv error = %q, want substring %q", err, want)
		}
	}
}

func TestProcessCloserLogsNonCleanExitStderr(t *testing.T) {
	cmd := exec.Command("sh", "-c", "echo remote failure >&2; exit 7")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("StdinPipe: %v", err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	var logBuf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logBuf, nil))
	err = newProcessCloser(cmd, stdin, &stderr, time.Second, log, "user@example.com", "work")()
	if err == nil {
		t.Fatal("Close error = nil, want non-clean ssh exit")
	}
	entry := logBuf.String()
	for _, want := range []string{"ssh exited non-cleanly", "remote failure", "user@example.com", "work"} {
		if !strings.Contains(entry, want) {
			t.Fatalf("log entry = %q, want substring %q", entry, want)
		}
	}
	if strings.Contains(entry, "'vev' '_stdio'") || strings.Contains(entry, "-- user@example.com") {
		t.Fatalf("log entry includes generated command line: %q", entry)
	}
}

func TestProcessCloser(t *testing.T) {
	tests := []struct {
		name      string
		cmd       *exec.Cmd
		timeout   time.Duration
		wantErrs  []string
		wantBound bool
	}{
		{
			name:      "wedged process is killed after timeout and reaped",
			cmd:       exec.Command("sleep", "30"),
			timeout:   50 * time.Millisecond,
			wantErrs:  []string{"sshstdio: ssh exited:"},
			wantBound: true,
		},
		{
			name:     "stderr from failing shell command is included in error",
			cmd:      exec.Command("sh", "-c", "echo kaboom >&2; exit 7"),
			timeout:  time.Second,
			wantErrs: []string{"sshstdio: ssh exited:", "kaboom"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stdin, err := tt.cmd.StdinPipe()
			if err != nil {
				t.Fatalf("StdinPipe: %v", err)
			}
			var stderr bytes.Buffer
			tt.cmd.Stderr = &stderr
			if err := tt.cmd.Start(); err != nil {
				t.Fatalf("Start: %v", err)
			}

			started := time.Now()
			err = newProcessCloser(tt.cmd, stdin, &stderr, tt.timeout, nil, "", "")()
			elapsed := time.Since(started)

			if err == nil {
				t.Fatalf("Close error = nil, want error containing %q", tt.wantErrs)
			}
			for _, want := range tt.wantErrs {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("Close error = %q, want substring %q", err.Error(), want)
				}
			}
			if tt.wantBound && elapsed > time.Second {
				t.Fatalf("Close took %s, want bounded below 1s", elapsed)
			}
			if tt.cmd.ProcessState == nil {
				t.Fatalf("ProcessState is nil, want child reaped")
			}
		})
	}
}
