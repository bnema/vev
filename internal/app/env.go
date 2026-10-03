package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
)

const envHelp = `usage: vev env <fish|sh> [-s <session>]

Print shell code that applies the session's current desktop and login
variables (WAYLAND_DISPLAY, DBUS_SESSION_BUS_ADDRESS, SSH_AUTH_SOCK, ...) to a
running shell. Inside a pane the current session is used; outside a pane,
-s <session> is required.

  fish   vev env fish | source
  sh     eval "$(vev env sh)"   (bash, zsh, and other POSIX shells)

For the same values as data, use vev cmd env [--json].`

// envInvocation is the shell-integration front end for the `env` control
// command. It only renders shell code; `vev cmd env` owns the data API.
type envInvocation struct {
	shell   string
	session string
	help    bool
}

func parseEnvArgs(args []string) (envInvocation, error) {
	var invocation envInvocation
	for index := 0; index < len(args); index++ {
		switch arg := args[index]; arg {
		case "--help", "-h":
			invocation.help = true
		case "-s":
			if index+1 >= len(args) || args[index+1] == "" {
				return envInvocation{}, usagef("`env -s` requires a session name")
			}
			invocation.session = args[index+1]
			index++
		case "fish", "sh":
			if invocation.shell != "" {
				return envInvocation{}, usagef("`env` takes one shell: fish or sh")
			}
			invocation.shell = arg
		default:
			if strings.HasPrefix(arg, "-") {
				return envInvocation{}, usagef("unknown flag %q for `env`", arg)
			}
			return envInvocation{}, usagef("unsupported shell %q for `env`; use fish or sh", arg)
		}
	}
	if invocation.shell == "" && !invocation.help {
		return envInvocation{}, usagef("`env` requires a shell: fish or sh")
	}
	return invocation, nil
}

func runEnv(ctx context.Context, invocation envInvocation) error {
	return runEnvWithDeps(ctx, invocation, productionCmdDeps())
}

func runEnvWithDeps(ctx context.Context, invocation envInvocation, deps cmdDeps) error {
	if invocation.help {
		_, err := fmt.Fprintln(deps.stdout, envHelp)
		return err
	}
	// The output is evaluated by a shell; never guess which session it follows.
	if _, inPane := parseVEVEnv(deps.getenv("VEV")); !inPane && invocation.session == "" {
		return usagef("`env` requires running inside a vev pane or -s <session>")
	}
	var captured strings.Builder
	dataDeps := deps
	dataDeps.stdout = &captured
	if err := runCmdWithDeps(ctx, cmdInvocation{slug: "env", session: invocation.session, jsonOut: true}, dataDeps); err != nil {
		return err
	}
	return writeShellEnvironment(deps.stdout, invocation.shell, captured.String())
}

// writeShellEnvironment converts the `env --json` result into code that sets
// or unsets each variable in the given shell. Nothing is written on error.
func writeShellEnvironment(out io.Writer, shell, encoded string) error {
	var values map[string]*string
	if err := json.Unmarshal([]byte(encoded), &values); err != nil {
		return fmt.Errorf("decoding session environment: %w", err)
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		if !isEnvironmentName(key) {
			return fmt.Errorf("invalid session environment name %q", key)
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var code strings.Builder
	for _, key := range keys {
		value := values[key]
		switch {
		case shell == "fish" && value == nil:
			// -g: without a global, a bare `set -e` erases the user's
			// universal variable for every fish session.
			code.WriteString("set -e -g " + key + "\n")
		case shell == "fish":
			code.WriteString("set -gx " + key + " " + fishQuote(*value) + "\n")
		case value == nil:
			code.WriteString("unset " + key + "\n")
		default:
			code.WriteString("export " + key + "=" + shQuote(*value) + "\n")
		}
	}
	_, err := io.WriteString(out, code.String())
	return err
}

func isEnvironmentName(name string) bool {
	if name == "" || name[0] >= '0' && name[0] <= '9' {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if c != '_' && (c < 'A' || c > 'Z') && (c < 'a' || c > 'z') && (c < '0' || c > '9') {
			return false
		}
	}
	return true
}

func shQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}

func fishQuote(value string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(value) + "'"
}
