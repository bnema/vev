package app

import (
	"fmt"
	"strings"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// cmdKind identifies which sub-command the CLI parsed.
type cmdKind int

const (
	kindAttach cmdKind = iota // ephemeral/new/attach — distinguished by intent
	kindList
	kindHost
	kindKill
	kindCmd
	kindEnv
	kindDaemon
	kindDaemonLauncher
	kindUIDriver
	kindUIRemoteCleanup
	kindDaemonStop
	kindWebDaemon
	kindWebRenew
	kindWebServe
	kindBrokerMuxStdio
	kindBrokerMuxQUICBootstrap
	kindBrokerMuxQUICProxy
	kindProductionBrokerServe
	kindProductionBrokerLauncher
	kindBrokerReady
	kindHelp
	kindVersion
)

// command is the parsed CLI invocation: what to do, plus the attach intent
// and session name where relevant.
type command struct {
	kind         cmdKind
	intent       uint8
	name         string
	remoteTarget string
	listHost     string
	listAll      bool
	hostAction   string
	hostTarget   string
	// hostTransport is the `host add` carriage: hostTransportQUIC or hostTransportSSH.
	hostTransport string
	killAll       bool
	killSessions  bool
	cmd           cmdInvocation
	env           envInvocation
	brokerServe   brokerServeOptions
	brokerMux     brokerMuxOptions
	brokerReady   brokerReadyOptions
	uiDriver      uiDriverOptions
	ui            interactiveUIOptions
	web           webOptions
}

// usageError is a user-facing argument error; the app prints it (with usage)
// rather than a stack of wrapped internals.
type usageError struct{ msg string }

func (e *usageError) Error() string { return e.msg }

func usagef(format string, args ...any) error {
	return &usageError{msg: fmt.Sprintf(format, args...)}
}

const usageText = `vev — a terminal multiplexer

usage:
  vev                 attach to (or create) an ephemeral session
  vev new <name>      create and attach to a named session
  vev attach <name>   attach to an existing session (alias: a)
  vev attach user@host[:session]
                      attach through SSH to a remote vev daemon
  vev ls              list local sessions
  vev ls <host>       list sessions on a known remote host
  vev ls --all        list local and remote sessions
  vev host add [--transport quic|ssh] <host>
                      add a pinned remote host (default transport: quic)
  vev host rm <host>  remove a pinned remote host
  vev host list       list known remote hosts
  vev kill <name>     kill a session
  vev kill --sessions kill all sessions (vev keeps running)
  vev kill --all      stop everything: every vev window, the broker, and the
                      daemon (named sessions come back on the next start)
  vev cmd <command>   run a control command (vev cmd --help)
  vev env <fish|sh>   print shell code that refreshes desktop variables
  vev --ui-observe    expose passive observation for this interactive client
                      (optional: --ui-socket PATH)
  vev --ui-control    expose observation and input control for this client
                      (optional: --ui-socket PATH)
  vev --web-daemon    start the private web terminal or print its current link
  vev --web-renew-token  revoke web access and print a fresh link
  Web options: --web-listen IP:PORT --web-origin http(s)://HOST[:PORT]
  vev --help          show this help
  vev --version       show version`

// parseArgs turns the raw argv tail into a command. It is deliberately a
// pure function so the full dispatch table can be unit-tested without any
// I/O.
func parseArgs(args []string) (command, error) {
	if len(args) > 0 && args[0] == "--ui-driver" {
		options, err := parseUIDriverArgs(args[1:])
		if err != nil {
			return command{}, err
		}
		return command{kind: kindUIDriver, uiDriver: options}, nil
	}
	ui, args, err := parseLeadingUIFlags(args)
	if err != nil {
		return command{}, err
	}
	if len(args) == 0 {
		return command{kind: kindAttach, intent: protocol.IntentEphemeral, ui: ui}, nil
	}
	switch args[0] {
	case "new":
		return parseNewArgs(args[1:], ui)
	case "attach", "a":
		return parseAttachArgs(args[1:], ui)
	}
	if ui.enabled() {
		return command{}, usagef("UI flags are only valid for an attach command")
	}
	return parseSubcommand(args)
}

// parseLeadingUIFlags consumes the interactive UI flags placed before the
// subcommand and returns the remaining arguments.
func parseLeadingUIFlags(args []string) (interactiveUIOptions, []string, error) {
	end := 0
	for end < len(args) {
		switch args[end] {
		case "--ui-observe", "--ui-control":
			end++
		case "--ui-socket":
			if end+1 >= len(args) || args[end+1] == "" {
				return interactiveUIOptions{}, nil, usagef("`--ui-socket` requires a path")
			}
			end += 2
		default:
			ui, err := parseInteractiveUIFlags(args[:end], interactiveUIOptions{})
			return ui, args[end:], err
		}
	}
	ui, err := parseInteractiveUIFlags(args, interactiveUIOptions{})
	return ui, nil, err
}

// parseSubcommand parses every subcommand that does not accept UI flags.
func parseSubcommand(args []string) (command, error) {
	switch args[0] {
	case "--web-daemon", "--web-serve":
		return parseWebArgs(args)
	case "--web-renew-token":
		return parseNoArgs(args, kindWebRenew)
	case "--daemon":
		return command{kind: kindDaemon}, nil
	case "--daemon-launcher":
		return command{kind: kindDaemonLauncher}, nil
	case productionBrokerServeCommand:
		return parseProductionBrokerServeArgs(args[1:])
	case productionBrokerLauncherCommand:
		return parseProductionBrokerLauncherArgs(args[1:])
	case brokerReadyCommand:
		options, err := parseBrokerReadyArgs(args[1:])
		return command{kind: kindBrokerReady, brokerReady: options}, err
	case ports.BrokerMuxStdioCommand:
		return parseBrokerMuxArgs(ports.BrokerMuxStdioCommand, kindBrokerMuxStdio, args[1:])
	case ports.BrokerMuxQUICBootstrapCommand:
		return parseBrokerMuxArgs(ports.BrokerMuxQUICBootstrapCommand, kindBrokerMuxQUICBootstrap, args[1:])
	case brokerMuxQUICProxyCommand:
		return parseBrokerMuxArgs(brokerMuxQUICProxyCommand, kindBrokerMuxQUICProxy, args[1:])
	case uiRemoteCleanupCommand:
		return parseNoArgs(args, kindUIRemoteCleanup)
	case daemonStopCommand:
		return parseNoArgs(args, kindDaemonStop)
	case "ls", "list":
		return parseListArgs(args[1:])
	case "host":
		return parseHostArgs(args[1:])
	case "env":
		invocation, err := parseEnvArgs(args[1:])
		if err != nil {
			return command{}, err
		}
		return command{kind: kindEnv, env: invocation}, nil
	case "cmd":
		invocation, err := parseCmdArgs(args[1:])
		if err != nil {
			return command{}, err
		}
		return command{kind: kindCmd, cmd: invocation}, nil
	case "kill":
		return parseKillArgs(args[1:])
	case "-h", "--help", "help":
		return command{kind: kindHelp}, nil
	case "--version", "version":
		return command{kind: kindVersion}, nil
	default:
		return command{}, usagef("unknown command %q", args[0])
	}
}

// parseNoArgs accepts a flag-style subcommand that takes no arguments.
func parseNoArgs(args []string, kind cmdKind) (command, error) {
	if len(args) != 1 {
		return command{}, usagef("`%s` does not accept arguments", args[0])
	}
	return command{kind: kind}, nil
}

// parseNewArgs parses `new <name> [user@host] [UI flags]`.
func parseNewArgs(args []string, ui interactiveUIOptions) (command, error) {
	if len(args) == 0 || args[0] == "" {
		return command{}, usagef("`new` requires a session name")
	}
	name, args := args[0], args[1:]
	var remoteTarget string
	if len(args) > 0 && !strings.HasPrefix(args[0], "--") {
		remoteTarget, args = args[0], args[1:]
		if err := domain.ValidateRemoteHostTarget(remoteTarget); err != nil {
			return command{}, err
		}
	}
	ui, err := parseInteractiveUIFlags(args, ui)
	if err != nil {
		return command{}, err
	}
	if err := domain.ValidateSessionName(name); err != nil {
		return command{}, err
	}
	return command{kind: kindAttach, intent: protocol.IntentNew, name: name, remoteTarget: remoteTarget, ui: ui}, nil
}

// parseAttachArgs parses `attach <name|user@host[:session]> [UI flags]`.
func parseAttachArgs(args []string, ui interactiveUIOptions) (command, error) {
	if len(args) == 0 || args[0] == "" {
		return command{}, usagef("`attach` requires a session name")
	}
	ui, err := parseInteractiveUIFlags(args[1:], ui)
	if err != nil {
		return command{}, err
	}
	cmd := command{kind: kindAttach, intent: protocol.IntentAttach, name: args[0], ui: ui}
	target, session, ok := parseRemoteAttachTarget(args[0])
	if !ok {
		return cmd, nil
	}
	if err := domain.ValidateRemoteHostTarget(target); err != nil {
		return command{}, err
	}
	if session == "" {
		cmd.intent = protocol.IntentEphemeral
	} else if err := domain.ValidateSessionName(session); err != nil {
		return command{}, err
	}
	cmd.remoteTarget = target
	cmd.name = session
	return cmd, nil
}

// parseKillArgs parses `kill <name>`, `kill -- <name>`, `kill --sessions`, and
// `kill --all`.
func parseKillArgs(args []string) (command, error) {
	if len(args) == 0 || args[0] == "" {
		return command{}, usagef("`kill` requires a session name, --sessions, or --all")
	}
	if args[0] == "--" {
		if len(args) != 2 || args[1] == "" {
			return command{}, usagef("`kill --` requires a session name")
		}
		return command{kind: kindKill, name: args[1]}, nil
	}
	if len(args) > 1 {
		return command{}, usagef("`kill` accepts exactly one session name, --sessions, or --all")
	}
	switch args[0] {
	case "--sessions":
		return command{kind: kindKill, killSessions: true}, nil
	case "--all":
		return command{kind: kindKill, killAll: true}, nil
	}
	if strings.HasPrefix(args[0], "-") {
		return command{}, usagef("unknown flag %q for `kill`; use `kill -- NAME` for a dashed session name", args[0])
	}
	return command{kind: kindKill, name: args[0]}, nil
}

func parseListArgs(args []string) (command, error) {
	if len(args) == 0 {
		return command{kind: kindList}, nil
	}
	if len(args) > 1 {
		return command{}, usagef("`ls` accepts at most one host or --all")
	}
	switch args[0] {
	case "--all":
		return command{kind: kindList, listAll: true}, nil
	default:
		if strings.HasPrefix(args[0], "-") {
			return command{}, usagef("unknown flag %q for `ls`", args[0])
		}
		if err := domain.ValidateRemoteHostTarget(args[0]); err != nil {
			return command{}, err
		}
		return command{kind: kindList, listHost: args[0]}, nil
	}
}

func parseHostArgs(args []string) (command, error) {
	if len(args) == 0 {
		return command{}, usagef("`host` requires add, rm, or list")
	}
	switch args[0] {
	case hostActionAdd:
		return parseHostAddArgs(args[1:])
	case hostActionRm:
		if len(args) < 2 || args[1] == "" {
			return command{}, usagef("`host rm` requires a host target")
		}
		if strings.HasPrefix(args[1], "-") {
			return command{}, usagef("unknown flag %q for `host rm`", args[1])
		}
		if len(args) > 2 {
			return command{}, usagef("`host rm` accepts exactly one host target")
		}
		if err := domain.ValidateRemoteHostTarget(args[1]); err != nil {
			return command{}, err
		}
		return command{kind: kindHost, hostAction: hostActionRm, hostTarget: args[1]}, nil
	case hostActionList:
		if len(args) > 1 {
			if strings.HasPrefix(args[1], "-") {
				return command{}, usagef("unknown flag %q for `host list`", args[1])
			}
			return command{}, usagef("`host list` accepts no arguments")
		}
		return command{kind: kindHost, hostAction: hostActionList}, nil
	default:
		return command{}, usagef("unknown host action %q", args[0])
	}
}

// parseHostAddArgs parses `host add [--transport quic|ssh] <host>`.
func parseHostAddArgs(args []string) (command, error) {
	transport, target := "", ""
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "--transport" || strings.HasPrefix(arg, "--transport="):
			if transport != "" {
				return command{}, usagef("`host add` accepts --transport once")
			}
			value, hasValue := strings.CutPrefix(arg, "--transport=")
			if !hasValue {
				if i+1 >= len(args) {
					return command{}, usagef("`--transport` requires a value (%s or %s)", hostTransportQUIC, hostTransportSSH)
				}
				i++
				value = args[i]
			}
			switch value {
			case hostTransportQUIC, hostTransportSSH:
				transport = value
			default:
				return command{}, usagef("invalid transport %q (want %q or %q)", value, hostTransportQUIC, hostTransportSSH)
			}
		case strings.HasPrefix(arg, "-"):
			return command{}, usagef("unknown flag %q for `host add`", arg)
		case target != "":
			return command{}, usagef("`host add` accepts exactly one host target")
		default:
			target = arg
		}
	}
	if target == "" {
		return command{}, usagef("`host add` requires a host target")
	}
	if err := domain.ValidateRemoteHostTarget(target); err != nil {
		return command{}, err
	}
	if transport == "" {
		transport = hostTransportQUIC
	}
	return command{kind: kindHost, hostAction: hostActionAdd, hostTarget: target, hostTransport: transport}, nil
}
