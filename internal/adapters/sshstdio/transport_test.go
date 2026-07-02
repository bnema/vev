package sshstdio

import (
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/bnema/vev/internal/ports"
)

func TestBuildCommandUsesExecArgs(t *testing.T) {
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
