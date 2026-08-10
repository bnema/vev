package app

import (
	"context"
	"errors"
	"io"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/usecase/daemon"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
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
		{"resize one-shot", []string{"cmd", "grow-pane-width"}, cmdInvocation{slug: "grow-pane-width"}, false},
		{"consume or expel pane left", []string{"cmd", "consume-or-expel-pane-left"}, cmdInvocation{slug: "consume-or-expel-pane-left"}, false},
		{"consume or expel pane right", []string{"cmd", "consume-or-expel-pane-right"}, cmdInvocation{slug: "consume-or-expel-pane-right"}, false},
		{"resize modal is not scriptable", []string{"cmd", "resize-pane"}, cmdInvocation{}, true},
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
	for _, want := range []string{"usage: vev cmd", "split-right", "consume-or-expel-pane-left", "consume-or-expel-pane-right", "grow-pane-width", "shrink-pane-width", "grow-pane-height", "shrink-pane-height", "equalize-panes", "list-panes"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("help %q missing %q", out.String(), want)
		}
	}
	for _, hidden := range []string{"session-picker", "resize-pane"} {
		if strings.Contains(out.String(), hidden) {
			t.Fatalf("help leaked non-scriptable command %q: %q", hidden, out.String())
		}
	}
}

func TestMoveCommandHelpUsesExactUsages(t *testing.T) {
	for _, tt := range []struct {
		slug, usage, desc string
	}{
		{"move-pane", "move-pane <destination-session> <destination-tab-id>", "Move the focused pane to another live tab"},
		{"move-tab", "move-tab <destination-session>", "Move the active tab to another live session"},
	} {
		t.Run(tt.slug, func(t *testing.T) {
			got := cmdHelp(cmdInvocation{slug: tt.slug})
			want := "usage: vev cmd [-s <session>] [--self] " + tt.usage + "\n\n" + tt.desc
			if got != want {
				t.Fatalf("cmdHelp() = %q, want %q", got, want)
			}
		})
	}
}

func TestMoveCmdPreservesPositionalArguments(t *testing.T) {
	for _, tt := range []struct {
		slug string
		args []string
	}{
		{slug: "move-pane", args: []string{"work", "t_dest"}},
		{slug: "move-tab", args: []string{"work"}},
	} {
		t.Run(tt.slug, func(t *testing.T) {
			parsed, err := parseArgs(append([]string{"cmd", tt.slug}, tt.args...))
			require.NoError(t, err)
			transport := &cmdTestTransport{recv: ports.Frame{Type: ports.MsgCommandResult, Payload: ports.MarshalCommandResult(ports.CommandResult{OK: true})}}
			require.NoError(t, runCmdWithDeps(context.Background(), parsed.cmd, cmdDeps{
				stdout: io.Discard,
				getenv: func(string) string { return "" },
				dial:   func(context.Context, string) (ports.Transport, error) { return transport, nil },
			}))
			require.Len(t, transport.sent, 1)
			request, err := ports.UnmarshalCommandRequest(transport.sent[0].Payload)
			require.NoError(t, err)
			require.Equal(t, tt.slug, request.Slug)
			require.Equal(t, tt.args, request.Args)
			require.Equal(t, ports.ProtocolVersion, request.Version)
		})
	}
}

func TestRemoteCatalogCommandEnsuresDaemon(t *testing.T) {
	transport := &cmdTestTransport{recv: ports.Frame{Type: ports.MsgCommandResult, Payload: ports.MarshalCommandResult(ports.CommandResult{OK: true})}}
	ensureCalls := 0
	err := runCmdWithDeps(context.Background(), cmdInvocation{slug: "remote-catalog", jsonOut: true}, cmdDeps{
		stdout: io.Discard,
		getenv: func(string) string { return "" },
		dial: func(context.Context, string) (ports.Transport, error) {
			t.Fatal("remote catalog used non-starting daemon dial")
			return nil, errors.New("unexpected dial")
		},
		ensure: func(context.Context, string) (ports.Transport, error) {
			ensureCalls++
			return transport, nil
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, ensureCalls)
}

func TestMoveCmdInvalidArgumentResultExitsTwo(t *testing.T) {
	for _, invocation := range []cmdInvocation{
		{slug: "move-pane"},
		{slug: "move-pane", args: []string{"work", "t_dest", "extra"}},
		{slug: "move-tab"},
		{slug: "move-tab", args: []string{"work", "extra"}},
	} {
		t.Run(invocation.slug+"/"+strconv.Itoa(len(invocation.args)), func(t *testing.T) {
			transport := &cmdTestTransport{recv: ports.Frame{Type: ports.MsgCommandResult, Payload: ports.MarshalCommandResult(ports.CommandResult{
				Code: ports.ErrInvalidCommandArgs, Text: "invalid command arguments",
			})}}
			err := runCmdWithDeps(context.Background(), invocation, cmdDeps{
				stdout: io.Discard,
				getenv: func(string) string { return "" },
				dial:   func(context.Context, string) (ports.Transport, error) { return transport, nil },
			})
			if ExitCode(err) != 2 {
				t.Fatalf("ExitCode(%v) = %d, want 2", err, ExitCode(err))
			}
		})
	}
}

func TestTargetPaneCmdsParseAndUseSelfTarget(t *testing.T) {
	for _, slug := range []string{
		"grow-pane-width", "shrink-pane-width", "grow-pane-height", "shrink-pane-height", "equalize-panes",
		"consume-or-expel-pane-left", "consume-or-expel-pane-right",
	} {
		t.Run(slug, func(t *testing.T) {
			invocation, err := parseArgs([]string{"cmd", "--self", slug})
			require.NoError(t, err)
			transport := &cmdTestTransport{recv: ports.Frame{Type: ports.MsgCommandResult, Payload: ports.MarshalCommandResult(ports.CommandResult{OK: true})}}
			require.NoError(t, runCmdWithDeps(context.Background(), invocation.cmd, cmdDeps{
				stdout: io.Discard,
				getenv: func(string) string { return "session=work,tab=t_abc,pane=p_def" },
				dial:   func(context.Context, string) (ports.Transport, error) { return transport, nil },
			}))
			require.Len(t, transport.sent, 1)
			request, err := ports.UnmarshalCommandRequest(transport.sent[0].Payload)
			require.NoError(t, err)
			require.Equal(t, slug, request.Slug)
			require.True(t, request.Self)
			require.Equal(t, "work", request.TargetSession)
			require.Equal(t, "t_abc", request.TargetTab)
			require.Equal(t, "p_def", request.TargetPane)
		})
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
	if transport.sent[0].Type != ports.MsgCommand || req.Slug != "split-right" || !req.Self || req.TargetSession != "old" || req.TargetTab != "t_abc" || req.TargetPane != "p_def" {
		t.Fatalf("request = type %d %+v", transport.sent[0].Type, req)
	}
	if out.String() != "done\n" {
		t.Fatalf("output = %q", out.String())
	}
}

func TestRunCmdVEVWithoutSelfUsesIDsOnlyAsSessionLocator(t *testing.T) {
	transport := &cmdTestTransport{recv: ports.Frame{Type: ports.MsgCommandResult, Payload: ports.MarshalCommandResult(ports.CommandResult{OK: true})}}
	err := runCmdWithDeps(context.Background(), cmdInvocation{slug: "split-right"}, cmdDeps{
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
	if req.Self || req.TargetSession != "old" || req.TargetTab != "t_abc" || req.TargetPane != "p_def" {
		t.Fatalf("VEV locator request = %+v", req)
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

func TestRunCmdRejectsTooManyArgumentsBeforeDialing(t *testing.T) {
	dialed := false
	err := runCmdWithDeps(context.Background(), cmdInvocation{
		slug: "toast",
		args: make([]string, math.MaxUint16+1),
	}, cmdDeps{
		stdout: io.Discard,
		getenv: func(string) string { return "" },
		dial: func(context.Context, string) (ports.Transport, error) {
			dialed = true
			return nil, errors.New("must not dial")
		},
	})

	if ExitCode(err) != 2 {
		t.Fatalf("error=%v code=%d, want usage error/2", err, ExitCode(err))
	}
	if dialed {
		t.Fatal("oversized command dialed daemon")
	}
}

func TestRunCmdTimesOutPendingRequest(t *testing.T) {
	timerCh := make(chan time.Time, 1)
	timer := portsmocks.NewMockTimer(t)
	timer.EXPECT().C().Return(timerCh).Once()
	timer.EXPECT().Stop().Return(true).Once()
	delayCh := make(chan time.Duration, 1)
	clock := portsmocks.NewMockClock(t)
	clock.EXPECT().NewTimer(mock.Anything).Run(func(delay time.Duration) {
		delayCh <- delay
	}).Return(timer).Once()
	recvStarted := make(chan struct{})
	recvGate := make(chan struct{})
	transport := &cmdTestTransport{recvStarted: recvStarted, recvGate: recvGate}
	done := make(chan error, 1)
	go func() {
		done <- runCmdWithDeps(context.Background(), cmdInvocation{slug: "split-right"}, cmdDeps{
			stdout: io.Discard,
			getenv: func(string) string { return "" },
			dial:   func(context.Context, string) (ports.Transport, error) { return transport, nil },
			clock:  clock,
		})
	}()
	<-recvStarted
	if delay := <-delayCh; delay != daemon.CommandRequestTimeout {
		t.Fatalf("command timer delay = %s, want %s", delay, daemon.CommandRequestTimeout)
	}
	timerCh <- time.Time{}
	err := <-done
	close(recvGate)
	if ExitCode(err) != 3 || !errors.Is(err, daemon.ErrCommandRequestTimeout) {
		t.Fatalf("timeout error=%v code=%d", err, ExitCode(err))
	}
	request, decodeErr := ports.UnmarshalCommandRequest(transport.sent[0].Payload)
	if decodeErr != nil || request.RequestID == 0 {
		t.Fatalf("request=%+v decode=%v, want non-zero request ID", request, decodeErr)
	}
}

func TestRunCmdZeroRequestIDReplyReturnsDaemonError(t *testing.T) {
	const want = "daemon error"
	transport := &cmdTestTransport{recv: ports.Frame{Type: ports.MsgCommandResult, Payload: ports.MarshalCommandResult(ports.CommandResult{
		Code: ports.ErrInternal, Text: want,
	})}}
	err := runCmdWithDeps(context.Background(), cmdInvocation{slug: "split-right"}, cmdDeps{
		stdout: io.Discard,
		getenv: func(string) string { return "" },
		dial:   func(context.Context, string) (ports.Transport, error) { return transport, nil },
	})
	if err == nil || err.Error() != want {
		t.Fatalf("error = %v, want daemon error %q", err, want)
	}
	if errors.Is(err, daemon.ErrCommandRequestTimeout) {
		t.Fatalf("error = %v, want daemon error instead of timeout", err)
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

func TestRunCmdClassifiesDaemonCommandErrors(t *testing.T) {
	tests := []struct {
		name     string
		result   ports.CommandResult
		wantCode int
	}{
		{
			name:     "invalid command arguments are usage errors",
			result:   ports.CommandResult{Code: ports.ErrInvalidCommandArgs, Text: "usage: toast [-l level] <message>"},
			wantCode: 2,
		},
		{
			name:     "missing runtime target is a command failure",
			result:   ports.CommandResult{Code: ports.ErrNoSuchTarget, Text: "no live sessions"},
			wantCode: 1,
		},
		{
			name:     "resize not in split remains an exit one failure",
			result:   ports.CommandResult{Code: ports.ErrNoSuchTarget, Text: "pane is not in a split"},
			wantCode: 1,
		},
		{
			name:     "resize minimum remains an exit one failure",
			result:   ports.CommandResult{Code: ports.ErrNoSuchTarget, Text: "pane cannot be resized further"},
			wantCode: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := &cmdTestTransport{recv: ports.Frame{Type: ports.MsgCommandResult, Payload: ports.MarshalCommandResult(tt.result)}}
			err := runCmdWithDeps(context.Background(), cmdInvocation{slug: "split-right"}, cmdDeps{
				stdout: io.Discard,
				getenv: func(string) string { return "" },
				dial:   func(context.Context, string) (ports.Transport, error) { return transport, nil },
			})
			if err == nil || ExitCode(err) != tt.wantCode || err.Error() != tt.result.Text {
				t.Fatalf("error=%v code=%d, want message %q and code %d", err, ExitCode(err), tt.result.Text, tt.wantCode)
			}
		})
	}
}

type cmdTestTransport struct {
	sent        []ports.Frame
	recv        ports.Frame
	recvErr     error
	recvCalls   int
	closeCalls  int
	recvStarted chan struct{}
	recvGate    <-chan struct{}
}

func (t *cmdTestTransport) Send(frame ports.Frame) error {
	t.sent = append(t.sent, frame)
	return nil
}

func (t *cmdTestTransport) Recv() (ports.Frame, error) {
	t.recvCalls++
	if t.recvStarted != nil {
		close(t.recvStarted)
	}
	if t.recvGate != nil {
		<-t.recvGate
	}
	return t.recv, t.recvErr
}

func (t *cmdTestTransport) Close() error {
	t.closeCalls++
	return nil
}
