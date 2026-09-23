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
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/adapters/sessionwire"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

// frameHeaderLen is the 4-byte big-endian length prefix; envelopes carry no
// legacy type byte.
const frameHeaderLen = 4

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

func TestCloseInterruptsBlockedSend(t *testing.T) {
	writer := &blockedWriter{started: make(chan struct{}), released: make(chan struct{})}
	transport := NewTransport(nil, writer, nil)

	sendDone := make(chan error, 1)
	go func() { sendDone <- transport.Send(wire.Envelope{Payload: []byte("blocked")}) }()
	<-writer.started
	if err := transport.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := <-sendDone; err == nil {
		t.Fatal("Send() error = nil after Close()")
	}
}

type blockedWriter struct {
	started  chan struct{}
	released chan struct{}
	once     sync.Once
}

func (w *blockedWriter) Write([]byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	<-w.released
	return 0, io.ErrClosedPipe
}

func (w *blockedWriter) Close() error {
	select {
	case <-w.released:
	default:
		close(w.released)
	}
	return nil
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

	hello := protocol.Hello{Version: protocol.Version + 1, Intent: protocol.IntentAttach, Name: "work", Size: domain.Size{Cols: 80, Rows: 24}}
	go func() {
		f, err := server.Recv()
		if err != nil {
			t.Errorf("server Recv: %v", err)
			return
		}
		decoded, err := sessionwire.DecodeClientEnvelope(f.Payload)
		if err != nil {
			t.Errorf("DecodeClientEnvelope: %v", err)
			return
		}
		got, ok := decoded.(protocol.Hello)
		if !ok {
			t.Errorf("decoded client message = %T, want protocol.Hello", decoded)
			return
		}
		if got.Version == protocol.Version {
			t.Errorf("test did not send a mismatched version")
		}
		errPayload, err := sessionwire.EncodeServerMessage(protocol.ErrorMsg{Code: protocol.ErrVersionMismatch, Text: "protocol version mismatch"})
		if err != nil {
			t.Errorf("EncodeServerMessage: %v", err)
			return
		}
		_ = server.Send(wire.Envelope{Payload: errPayload})
	}()

	helloPayload, err := sessionwire.EncodeClientMessage(hello)
	if err != nil {
		t.Fatalf("EncodeClientMessage: %v", err)
	}
	if err := client.Send(wire.Envelope{Payload: helloPayload}); err != nil {
		t.Fatalf("client Send: %v", err)
	}
	reply, err := client.Recv()
	if err != nil {
		t.Fatalf("client Recv: %v", err)
	}
	decodedReply, err := sessionwire.DecodeServerEnvelope(reply.Payload)
	if err != nil {
		t.Fatalf("DecodeServerEnvelope: %v", err)
	}
	em, ok := decodedReply.(protocol.ErrorMsg)
	if !ok {
		t.Fatalf("decoded server message = %T, want protocol.ErrorMsg", decodedReply)
	}
	if em.Code != protocol.ErrVersionMismatch {
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
	boundaryPayload := make([]byte, wire.AbsoluteEnvelopeLimit-1)
	boundaryWire := &bytes.Buffer{}
	boundarySend := NewTransport(nil, boundaryWire, nil)
	if err := boundarySend.Send(wire.Envelope{Payload: boundaryPayload}); err != nil {
		t.Fatalf("boundary Send error = %v", err)
	}
	if got := binary.BigEndian.Uint32(boundaryWire.Bytes()[:frameHeaderLen]); got != uint32(len(boundaryPayload)) {
		t.Fatalf("boundary envelope length = %d, want %d", got, len(boundaryPayload))
	}
	boundaryRecv := NewTransport(bytes.NewReader(boundaryWire.Bytes()), io.Discard, nil)
	boundaryFrame, err := boundaryRecv.Recv()
	if err != nil {
		t.Fatalf("boundary Recv error = %v", err)
	}
	if len(boundaryFrame.Payload) != len(boundaryPayload) {
		t.Fatalf("boundary envelope = payload %d bytes; want %d bytes", len(boundaryFrame.Payload), len(boundaryPayload))
	}
	if !bytes.Equal(boundaryFrame.Payload, boundaryPayload) {
		t.Fatal("boundary payload was corrupted")
	}

	send := NewTransport(nil, io.Discard, nil)
	err = send.Send(wire.Envelope{Payload: make([]byte, wire.AbsoluteEnvelopeLimit+1)})
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("Send error = %v, want ErrFrameTooLarge", err)
	}

	var header [frameHeaderLen]byte
	binary.BigEndian.PutUint32(header[:], wire.AbsoluteEnvelopeLimit+1)
	recv := NewTransport(bytes.NewReader(header[:]), io.Discard, nil)
	_, err = recv.Recv()
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("Recv error = %v, want ErrFrameTooLarge", err)
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

// TestTransportRecvBoundedEnforcesCallerLimit proves the SSH stdio transport
// exposes the shared bounded receive: an over-limit length prefix is refused
// before the body is read, a zero limit refuses a non-empty frame, and an
// in-limit frame round-trips byte for byte.
func TestTransportRecvBoundedEnforcesCallerLimit(t *testing.T) {
	var header [frameHeaderLen]byte

	t.Run("over-limit prefix refused before body", func(t *testing.T) {
		binary.BigEndian.PutUint32(header[:], 8)
		recv := NewTransport(bytes.NewReader(header[:]), io.Discard, nil).(wire.BoundedTransport)
		if _, err := recv.RecvBounded(4); !errors.Is(err, ErrFrameTooLarge) {
			t.Fatalf("RecvBounded error = %v, want ErrFrameTooLarge", err)
		}
	})

	t.Run("zero limit refuses non-empty frame", func(t *testing.T) {
		binary.BigEndian.PutUint32(header[:], 1)
		recv := NewTransport(bytes.NewReader(header[:]), io.Discard, nil).(wire.BoundedTransport)
		if _, err := recv.RecvBounded(0); !errors.Is(err, ErrFrameTooLarge) {
			t.Fatalf("RecvBounded error = %v, want ErrFrameTooLarge", err)
		}
	})

	t.Run("in-limit frame round trips", func(t *testing.T) {
		payload := []byte("bounded ssh envelope")
		var wireBuf bytes.Buffer
		binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
		wireBuf.Write(header[:])
		wireBuf.Write(payload)
		recv := NewTransport(bytes.NewReader(wireBuf.Bytes()), io.Discard, nil).(wire.BoundedTransport)
		envelope, err := recv.RecvBounded(uint64(len(payload)))
		if err != nil {
			t.Fatalf("RecvBounded error = %v", err)
		}
		if !bytes.Equal(envelope.Payload, payload) {
			t.Fatalf("RecvBounded payload = %q, want %q", envelope.Payload, payload)
		}
	})
}
