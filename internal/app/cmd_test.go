package app

import (
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/bnema/vev/internal/ports"
)

func TestParseCmdArgs(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		want    cmdInvocation
		wantErr bool
	}{
		{"bare cmd is help", []string{"cmd"}, cmdInvocation{help: true}, false},
		{"cmd help", []string{"cmd", "--help"}, cmdInvocation{help: true}, false},
		{"slug help", []string{"cmd", "split-right", "--help"}, cmdInvocation{slug: "split-right", help: true}, false},
		{"simple", []string{"cmd", "split-right"}, cmdInvocation{slug: "split-right"}, false},
		{"session flag", []string{"cmd", "-s", "dev", "new-tab"}, cmdInvocation{slug: "new-tab", session: "dev"}, false},
		{"json flag", []string{"cmd", "list-panes", "--json"}, cmdInvocation{slug: "list-panes", jsonOut: true}, false},
		{"self flag", []string{"cmd", "--self", "split-right"}, cmdInvocation{slug: "split-right", self: true}, false},
		{"self and session conflict", []string{"cmd", "-s", "other", "--self", "split-right"}, cmdInvocation{}, true},
		{"passthrough args", []string{"cmd", "toast", "-l", "warn", "hello"}, cmdInvocation{slug: "toast", args: []string{"-l", "warn", "hello"}}, false},
		{"unknown slug", []string{"cmd", "frobnicate"}, cmdInvocation{}, true},
		{"non-scriptable slug", []string{"cmd", "detach"}, cmdInvocation{}, true},
		{"session missing value", []string{"cmd", "-s"}, cmdInvocation{}, true},
		{"unknown leading flag", []string{"cmd", "--wat"}, cmdInvocation{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseArgs(tt.args)
			if tt.wantErr {
				var usage *usageError
				if !errors.As(err, &usage) {
					t.Fatalf("parseArgs(%q) error = %v, want usage error", tt.args, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseArgs(%q): %v", tt.args, err)
			}
			if got.kind != kindCmd || !reflect.DeepEqual(got.cmd, tt.want) {
				t.Fatalf("parseArgs(%q) = kind %v, cmd %#v, want %#v", tt.args, got.kind, got.cmd, tt.want)
			}
		})
	}
}

func TestExitCodes(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{"success", nil, 0},
		{"command", errors.New("command failed"), 1},
		{"usage", &usageError{msg: "bad args"}, 2},
		{"unreachable", &exitCoded{code: 3, err: errDaemonUnreachable}, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ExitCode(tt.err); got != tt.want {
				t.Fatalf("ExitCode(%v) = %d, want %d", tt.err, got, tt.want)
			}
		})
	}
}

func TestCmdHelpUsesRegistryWithoutDialing(t *testing.T) {
	called := false
	out := new(strings.Builder)
	err := runCmdWithDeps(context.Background(), cmdInvocation{help: true}, cmdDeps{
		stdout: out,
		getenv: func(string) string { return "" },
		dial: func(context.Context, string) (ports.Transport, error) {
			called = true
			return nil, errors.New("must not dial")
		},
	})
	if err != nil {
		t.Fatalf("runCmdWithDeps(help): %v", err)
	}
	if called {
		t.Fatal("help dialed daemon")
	}
	for _, want := range []string{"usage: vev cmd", "split-right", "list-panes"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("help %q missing %q", out.String(), want)
		}
	}
	if strings.Contains(out.String(), "session-picker") {
		t.Fatalf("help leaked non-scriptable command: %q", out.String())
	}
}

func TestRunCmdBuildsOneShotTargetedRequest(t *testing.T) {
	transport := &cmdTestTransport{recv: ports.Frame{Type: ports.MsgCommandResult, Payload: ports.MarshalCommandResult(ports.CommandResult{OK: true, Output: "done"})}}
	out := new(strings.Builder)
	err := runCmdWithDeps(context.Background(), cmdInvocation{slug: "split-right", self: true}, cmdDeps{
		stdout: out,
		getenv: func(string) string { return "session=old,tab=t_abc,pane=p_def" },
		dial:   func(context.Context, string) (ports.Transport, error) { return transport, nil },
	})
	if err != nil {
		t.Fatalf("runCmdWithDeps: %v", err)
	}
	if len(transport.sent) != 1 || transport.recvCalls != 1 || transport.closeCalls != 1 {
		t.Fatalf("transport calls: sent=%d recv=%d close=%d", len(transport.sent), transport.recvCalls, transport.closeCalls)
	}
	req, err := ports.UnmarshalCommandRequest(transport.sent[0].Payload)
	if err != nil {
		t.Fatalf("decode request: %v", err)
	}
	if transport.sent[0].Type != ports.MsgCommand || req.Slug != "split-right" || req.TargetSession != "old" || req.TargetTab != "t_abc" || req.TargetPane != "p_def" {
		t.Fatalf("request = type %d %+v", transport.sent[0].Type, req)
	}
	if out.String() != "done\n" {
		t.Fatalf("output = %q", out.String())
	}
}

func TestRunCmdExplicitSessionDoesNotUseEnvIDsWithoutSelf(t *testing.T) {
	transport := &cmdTestTransport{recv: ports.Frame{Type: ports.MsgCommandResult, Payload: ports.MarshalCommandResult(ports.CommandResult{OK: true})}}
	err := runCmdWithDeps(context.Background(), cmdInvocation{slug: "new-tab", session: "current"}, cmdDeps{
		stdout: io.Discard,
		getenv: func(string) string { return "session=old,tab=t_abc,pane=p_def" },
		dial:   func(context.Context, string) (ports.Transport, error) { return transport, nil },
	})
	if err != nil {
		t.Fatalf("runCmdWithDeps: %v", err)
	}
	req, err := ports.UnmarshalCommandRequest(transport.sent[0].Payload)
	if err != nil {
		t.Fatalf("decode request: %v", err)
	}
	if req.TargetSession != "current" || req.TargetTab != "" || req.TargetPane != "" {
		t.Fatalf("explicit targeting request = %+v", req)
	}
}

func TestRunCmdDoesNotAutostartAndClassifiesDialFailure(t *testing.T) {
	dials := 0
	dialErr := errors.New("unreachable")
	err := runCmdWithDeps(context.Background(), cmdInvocation{slug: "split-right"}, cmdDeps{
		stdout: io.Discard,
		getenv: func(string) string { return "" },
		dial: func(context.Context, string) (ports.Transport, error) {
			dials++
			return nil, dialErr
		},
	})
	if ExitCode(err) != 3 || dials != 1 {
		t.Fatalf("error=%v code=%d dials=%d, want unreachable/3 and one dial", err, ExitCode(err), dials)
	}
	if !errors.Is(err, dialErr) {
		t.Fatalf("error=%v does not wrap dial error", err)
	}
}

func TestRunCmdDaemonErrorHasExitOne(t *testing.T) {
	transport := &cmdTestTransport{recv: ports.Frame{Type: ports.MsgCommandResult, Payload: ports.MarshalCommandResult(ports.CommandResult{Code: ports.ErrNoSuchTarget, Text: "no live sessions"})}}
	err := runCmdWithDeps(context.Background(), cmdInvocation{slug: "split-right"}, cmdDeps{
		stdout: io.Discard,
		getenv: func(string) string { return "" },
		dial:   func(context.Context, string) (ports.Transport, error) { return transport, nil },
	})
	if err == nil || ExitCode(err) != 1 || !strings.Contains(err.Error(), "no live sessions") {
		t.Fatalf("error=%v code=%d", err, ExitCode(err))
	}
}

type cmdTestTransport struct {
	sent       []ports.Frame
	recv       ports.Frame
	recvErr    error
	recvCalls  int
	closeCalls int
}

func (t *cmdTestTransport) Send(frame ports.Frame) error {
	t.sent = append(t.sent, frame)
	return nil
}

func (t *cmdTestTransport) Recv() (ports.Frame, error) {
	t.recvCalls++
	return t.recv, t.recvErr
}

func (t *cmdTestTransport) Close() error {
	t.closeCalls++
	return nil
}
