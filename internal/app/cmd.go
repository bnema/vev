package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/ports"
	commandusecase "github.com/bnema/vev/internal/usecase/command"
)

type cmdInvocation struct {
	slug    string
	args    []string
	session string
	self    bool
	jsonOut bool
	help    bool
}

type exitCoded struct {
	code int
	err  error
}

func (e *exitCoded) Error() string { return e.err.Error() }
func (e *exitCoded) Unwrap() error { return e.err }

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	if coded, ok := errors.AsType[*exitCoded](err); ok {
		return coded.code
	}
	if _, ok := errors.AsType[*usageError](err); ok {
		return 2
	}
	return 1
}

// ExitCode maps application outcomes to the CLI contract.
func ExitCode(err error) int { return exitCode(err) }

func parseCmdArgs(args []string) (cmdInvocation, error) {
	invocation := cmdInvocation{}
	index := 0
	for index < len(args) && invocation.slug == "" {
		switch args[index] {
		case "--help", "-h":
			invocation.help = true
			index++
		case "-s":
			if index+1 >= len(args) || args[index+1] == "" {
				return cmdInvocation{}, usagef("`cmd -s` requires a session name")
			}
			invocation.session = args[index+1]
			index += 2
		case "--self":
			invocation.self = true
			index++
		case "--json":
			invocation.jsonOut = true
			index++
		default:
			if strings.HasPrefix(args[index], "-") {
				return cmdInvocation{}, usagef("unknown flag %q for `cmd`", args[index])
			}
			invocation.slug = args[index]
			index++
		}
	}
	if invocation.slug == "" {
		invocation.help = true
		return invocation, nil
	}
	if invocation.self && invocation.session != "" {
		return cmdInvocation{}, usagef("`cmd --self` cannot be combined with `-s`")
	}
	cmd, ok := commandusecase.BySlug(invocation.slug)
	if !ok || !cmd.Scriptable {
		return cmdInvocation{}, usagef("unknown command %q; see `vev cmd --help`", invocation.slug)
	}
	for _, arg := range args[index:] {
		switch arg {
		case "--help", "-h":
			invocation.help = true
			return invocation, nil
		case "--json":
			invocation.jsonOut = true
		default:
			invocation.args = append(invocation.args, arg)
		}
	}
	return invocation, nil
}

func cmdHelp(invocation cmdInvocation) string {
	if invocation.slug != "" {
		cmd, _ := commandusecase.BySlug(invocation.slug)
		return fmt.Sprintf("usage: vev cmd [-s <session>] [--self] %s\n\n%s", cmd.Usage, cmd.Desc)
	}
	var out strings.Builder
	out.WriteString("usage: vev cmd [-s <session>] [--self] <command> [args]\n\ncommands:\n")
	writer := tabwriter.NewWriter(&out, 0, 4, 2, ' ', 0)
	commands := commandusecase.Registry()
	sort.Slice(commands, func(i, j int) bool { return commands[i].Slug < commands[j].Slug })
	for _, cmd := range commands {
		if cmd.Scriptable {
			_, _ = fmt.Fprintf(writer, "  %s\t%s\n", cmd.Usage, cmd.Desc)
		}
	}
	_ = writer.Flush()
	return out.String()
}

var errDaemonUnreachable = errors.New("daemon not running")

type cmdDeps struct {
	stdout io.Writer
	getenv func(string) string
	dial   func(context.Context, string) (ports.Transport, error)
}

func runCmd(ctx context.Context, invocation cmdInvocation) error {
	return runCmdWithDeps(ctx, invocation, cmdDeps{stdout: os.Stdout, getenv: os.Getenv, dial: realDial})
}

func runCmdWithDeps(ctx context.Context, invocation cmdInvocation, deps cmdDeps) error {
	if invocation.help {
		_, err := fmt.Fprintln(deps.stdout, cmdHelp(invocation))
		return err
	}
	request := ports.CommandRequest{
		Version:       ports.ProtocolVersion,
		Self:          invocation.self,
		Slug:          invocation.slug,
		Args:          invocation.args,
		TargetSession: invocation.session,
		JSON:          invocation.jsonOut,
	}
	if identity, ok := parseVEVEnv(deps.getenv("VEV")); ok {
		if request.TargetSession == "" {
			request.TargetSession = identity.session
		}
		if invocation.self || invocation.session == "" {
			request.TargetTab = identity.tab
			request.TargetPane = identity.pane
		}
	} else if invocation.self {
		return usagef("--self requires running inside a vev pane")
	}

	transport, err := deps.dial(ctx, ipc.SocketDir())
	if err != nil {
		return &exitCoded{code: 3, err: fmt.Errorf("%w: %w", errDaemonUnreachable, err)}
	}
	defer func() { _ = transport.Close() }()

	frame := ports.Frame{Type: ports.MsgCommand, Payload: ports.MarshalCommandRequest(request)}
	if err := transport.Send(frame); err != nil {
		return &exitCoded{code: 3, err: fmt.Errorf("sending command: %w", err)}
	}
	reply, err := transport.Recv()
	if err != nil {
		return &exitCoded{code: 3, err: fmt.Errorf("reading command reply: %w", err)}
	}
	if reply.Type != ports.MsgCommandResult {
		return fmt.Errorf("unexpected command reply type %d", reply.Type)
	}
	result, err := ports.UnmarshalCommandResult(reply.Payload)
	if err != nil {
		return fmt.Errorf("decoding command reply: %w", err)
	}
	if !result.OK {
		return errors.New(result.Text)
	}
	if result.Output != "" {
		if _, err := io.WriteString(deps.stdout, result.Output); err != nil {
			return err
		}
		if !strings.HasSuffix(result.Output, "\n") {
			_, err = fmt.Fprintln(deps.stdout)
			return err
		}
	}
	return nil
}
