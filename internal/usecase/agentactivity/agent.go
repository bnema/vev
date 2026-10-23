package agentactivity

import "strings"

// Agent is an agent CLI whose screen this package knows how to classify.
type Agent uint8

const (
	// None is any process that is not a known agent CLI.
	None Agent = iota
	Claude
	Codex
	OpenCode
	Zut
)

// String returns the lowercase agent name used in logs.
func (a Agent) String() string {
	switch a {
	case Claude:
		return "claude"
	case Codex:
		return "codex"
	case OpenCode:
		return "opencode"
	case Zut:
		return "zut"
	default:
		return "none"
	}
}

// maxUnwrapDepth bounds nested wrappers such as `env bash -c "npx codex"`.
const maxUnwrapDepth = 4

// Identify maps the argv of a foreground process group leader to an agent.
// Runtime launchers (node, bun, deno), package runners (npx, pnpm, yarn, npm,
// bunx), env, and `sh -c` wrappers are unwrapped; a `sh -c` script resolves
// to its last simple command. comm, the kernel's 15-byte process name, is
// used only when argv is unavailable.
func Identify(argv []string, comm string) Agent {
	if len(argv) == 0 {
		return agentForName(normalizeName(comm))
	}
	return identifyArgv(argv, 0)
}

func identifyArgv(argv []string, depth int) Agent {
	if len(argv) == 0 || depth > maxUnwrapDepth {
		return None
	}
	name := commandName(argv[0])
	rest := argv[1:]
	switch name {
	case "node", "nodejs", "bun", "deno":
		i := firstOperand(rest, name == "bun" || name == "deno")
		if i < 0 {
			return None
		}
		// npx, pnpm, yarn, and npm are themselves node scripts:
		// `node /usr/bin/npx -y @openai/codex`.
		if packageRunner(normalizeName(rest[i])) {
			return identifyArgv(rest[i:], depth+1)
		}
		return agentForScript(rest[i])
	case "npx", "npx-cli", "bunx", "pnpx":
		return agentForPackage(rest)
	case "pnpm", "yarn":
		if len(rest) > 0 && rest[0] == "dlx" {
			return agentForPackage(rest[1:])
		}
		return None
	case "npm", "npm-cli":
		if len(rest) > 0 && (rest[0] == "exec" || rest[0] == "x") {
			return agentForPackage(rest[1:])
		}
		return None
	case "uv", "direnv":
		if len(rest) > 0 && (rest[0] == "run" || rest[0] == "exec") {
			return identifyArgv(skipWrapperArgs(rest[1:], name), depth+1)
		}
		return None
	case "mise":
		if len(rest) > 0 && (rest[0] == "x" || rest[0] == "exec") {
			return identifyArgv(skipWrapperArgs(rest[1:], name), depth+1)
		}
		return None
	case "env", "exec", "nohup", "command", "nice", "timeout", "sudo":
		return identifyArgv(skipWrapperArgs(rest, name), depth+1)
	case "sh", "bash", "zsh", "fish", "dash", "ksh":
		script, ok := shellCommand(rest)
		if !ok {
			return None
		}
		words := strings.Fields(lastSimpleCommand(script))
		return identifyArgv(skipAssignments(words), depth+1)
	}
	return agentForName(name)
}

// commandName returns the program name of an argv[0] path. A version-only
// basename (`~/.local/share/claude/versions/2.1.308`) resolves to the nearest
// meaningful parent directory.
func commandName(path string) string {
	for path != "" {
		path = strings.TrimRight(path, "/")
		base := path
		if i := strings.LastIndexByte(path, '/'); i >= 0 {
			base = path[i+1:]
			path = path[:i]
		} else {
			path = ""
		}
		if base == "versions" || versionLike(base) {
			continue
		}
		return normalizeName(base)
	}
	return ""
}

// agentForScript resolves a script run by a JS runtime. A generic entry
// point (`.../@anthropic-ai/claude-code/cli.js`) is identified by its
// package directory.
func agentForScript(script string) Agent {
	for script != "" {
		script = strings.TrimRight(script, "/")
		base := script
		if i := strings.LastIndexByte(script, '/'); i >= 0 {
			base = script[i+1:]
			script = script[:i]
		} else {
			script = ""
		}
		if a := agentForName(normalizeName(base)); a != None {
			return a
		}
		switch normalizeName(base) {
		case "cli", "index", "main", "bin", "dist", "build", "lib", "src":
			continue
		}
		return None
	}
	return None
}

// normalizeName lowercases a program or package name and strips its
// directory, npm scope/version, script extension, and `-<version>` suffix.
func normalizeName(s string) string {
	if i := strings.LastIndexByte(s, '/'); i >= 0 {
		s = s[i+1:]
	}
	if i := strings.LastIndexByte(s, '@'); i > 0 {
		s = s[:i]
	}
	for _, ext := range [...]string{".js", ".mjs", ".cjs", ".ts", ".exe"} {
		if strings.HasSuffix(s, ext) {
			s = s[:len(s)-len(ext)]
			break
		}
	}
	if i := strings.LastIndexByte(s, '-'); i > 0 && versionLike(s[i+1:]) {
		s = s[:i]
	}
	return strings.ToLower(s)
}

// versionLike reports whether s looks like `2.1.308`, `v2`, or `0.163.0`.
func versionLike(s string) bool {
	s = strings.TrimPrefix(s, "v")
	if s == "" || s[0] < '0' || s[0] > '9' {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && c != '.' {
			return false
		}
	}
	return true
}

func agentForName(name string) Agent {
	switch name {
	case "claude", "claude-code":
		return Claude
	case "codex":
		return Codex
	case "opencode", "opencode-ai":
		return OpenCode
	case "zut":
		return Zut
	default:
		return None
	}
}

// agentForPackage resolves the package run by a package runner. The npm
// packages `@anthropic-ai/claude-code`, `@openai/codex`, and `opencode-ai`
// normalize to agent names.
func agentForPackage(args []string) Agent {
	i := firstOperand(args, false)
	if i < 0 {
		return None
	}
	return agentForName(normalizeName(args[i]))
}

// packageRunner reports whether a node script name is a package runner whose
// own arguments name the package to run.
func packageRunner(name string) bool {
	switch name {
	case "npx", "npx-cli", "pnpm", "pnpx", "yarn", "npm", "npm-cli":
		return true
	}
	return false
}

// runtimeValueFlags take their value as the next argument (`node -r x`).
func runtimeValueFlag(arg string) bool {
	switch arg {
	case "-r", "--require", "--import", "-e", "--eval", "--loader", "--experimental-loader", "-p", "--print":
		return true
	}
	return false
}

// firstOperand returns the index of the first argument that is not a flag or
// the value of a known value flag, or -1. Runtimes with subcommands (`bun run
// x`, `deno run x`) skip the subcommand.
func firstOperand(args []string, skipRun bool) int {
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "" || arg[0] == '-' {
			if runtimeValueFlag(arg) {
				i++
			}
			continue
		}
		if skipRun && (arg == "run" || arg == "x") {
			continue
		}
		return i
	}
	return -1
}

// skipWrapperArgs drops the leading options of wrapper and the values of its
// value options, then the command words skipAssignments drops. timeout takes
// a leading duration operand and direnv exec a directory.
func skipWrapperArgs(args []string, wrapper string) []string {
	for len(args) > 0 {
		arg := args[0]
		if arg == "--" {
			args = args[1:]
			break
		}
		if arg == "" || arg[0] != '-' {
			break
		}
		args = args[1:]
		if len(args) > 0 && wrapperValueFlag(wrapper, arg) {
			args = args[1:]
		}
	}
	if (wrapper == "timeout" || wrapper == "direnv") && len(args) > 0 {
		args = args[1:]
	}
	return skipAssignments(args)
}

// wrapperValueFlag reports whether flag of wrapper takes the next argument.
func wrapperValueFlag(wrapper, flag string) bool {
	switch wrapper {
	case "nice":
		return flag == "-n" || flag == "--adjustment"
	case "timeout":
		return flag == "-s" || flag == "--signal" || flag == "-k" || flag == "--kill-after"
	case "sudo":
		switch flag {
		case "-u", "--user", "-g", "--group", "-C", "--close-from", "-D", "--chdir", "-h", "--host", "-p", "--prompt", "-R", "--chroot", "-T", "--command-timeout":
			return true
		}
	case "exec":
		return flag == "-a"
	case "env":
		return flag == "-u" || flag == "--unset" || flag == "-C" || flag == "--chdir" || flag == "-S" || flag == "--split-string"
	case "uv":
		switch flag {
		case "--with", "--python", "-p", "--project", "--directory", "--package", "--env-file", "--extra", "--group", "--index", "--from":
			return true
		}
	}
	return false
}

// skipAssignments drops leading flags, `NAME=value` assignments, and the
// exec, command, nohup, and env words with their own options.
func skipAssignments(args []string) []string {
	for len(args) > 0 {
		arg := args[0]
		eq := strings.IndexByte(arg, '=')
		slash := strings.IndexByte(arg, '/')
		switch {
		case arg == "exec", arg == "command", arg == "nohup", arg == "env":
			return skipWrapperArgs(args[1:], arg)
		case arg != "" && arg[0] == '-':
		case eq > 0 && (slash < 0 || eq < slash):
		default:
			return args
		}
		args = args[1:]
	}
	return args
}

// shellCommand returns the script of `sh -c script` (also `-lc`, `-ic`,
// `--command`). Options end at the first operand: in `bash ./run.sh -c x`,
// -c is an argument of the script.
func shellCommand(args []string) (string, bool) {
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "" || (arg[0] != '-' && arg[0] != '+') || arg == "--" {
			return "", false
		}
		if arg == "--command" || (arg[0] == '-' && len(arg) > 1 && arg[1] != '-' && strings.IndexByte(arg, 'c') > 0) {
			if i+1 < len(args) {
				return args[i+1], true
			}
			return "", false
		}
		if shellValueFlag(arg) {
			i++
		}
	}
	return "", false
}

// shellValueFlag reports whether a shell option takes the next argument:
// `-o pipefail`, `+O extglob`, `--rcfile x`, fish `-C init`.
func shellValueFlag(arg string) bool {
	switch arg {
	case "-o", "+o", "-O", "+O", "-C", "--init-command", "--rcfile", "--init-file":
		return true
	}
	return false
}

// lastSimpleCommand returns the last non-empty command of a shell script
// split on `&&`, `||`, `;`, and newlines: the one left in the foreground in
// `cd x && claude`. Quotes are not parsed.
func lastSimpleCommand(script string) string {
	last := script
	start := 0
	for i := 0; i < len(script); i++ {
		sep := 0
		switch c := script[i]; {
		case c == ';' || c == '\n':
			sep = 1
		case (c == '&' || c == '|') && i+1 < len(script) && script[i+1] == c:
			sep = 2
		}
		if sep == 0 {
			continue
		}
		if cmd := script[start:i]; strings.TrimSpace(cmd) != "" {
			last = cmd
		}
		i += sep - 1
		start = i + 1
	}
	if cmd := script[start:]; strings.TrimSpace(cmd) != "" {
		last = cmd
	}
	return last
}
