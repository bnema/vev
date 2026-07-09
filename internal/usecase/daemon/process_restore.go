package daemon

import (
	"path/filepath"
	"strings"
	"unicode"

	snapcodec "github.com/bnema/vev/internal/usecase/snapshot"
)

const (
	processStrategyGeneric  = "generic"
	processStrategyClaude   = "claude"
	processStrategyCodex    = "codex"
	processStrategyOpenCode = "opencode"
	processStrategyPi       = "pi"
)

type processRestoreDecision struct {
	Command string
	Restore bool
	Reason  string
}

func detectProcessStrategy(argv []string) string {
	if len(argv) == 0 {
		return processStrategyGeneric
	}
	switch filepath.Base(argv[0]) {
	case "claude":
		return processStrategyClaude
	case "codex":
		return processStrategyCodex
	case "opencode":
		return processStrategyOpenCode
	case "pi":
		return processStrategyPi
	default:
		return processStrategyGeneric
	}
}

func extractAgentSessionID(strategy string, argv []string) string {
	for i, arg := range argv {
		if !agentSessionFlag(strategy, arg) || i+1 >= len(argv) {
			continue
		}
		candidate := argv[i+1]
		if candidate != "" && !strings.HasPrefix(candidate, "-") && !containsControl(candidate) {
			return candidate
		}
	}
	return ""
}

func agentSessionFlag(strategy, arg string) bool {
	switch strategy {
	case processStrategyClaude:
		return arg == "--session-id" || arg == "--resume" || arg == "-r"
	case processStrategyOpenCode:
		return arg == "-s" || arg == "--session"
	case processStrategyCodex:
		return arg == "resume"
	case processStrategyPi:
		return arg == "--resume" || arg == "-r"
	default:
		return false
	}
}

func planProcessRestore(proc *snapcodec.Process, allow map[string]struct{}) processRestoreDecision {
	if proc == nil || len(proc.Argv) == 0 || proc.Argv[0] == "" {
		return processRestoreDecision{Reason: "missing_process"}
	}
	strategy := normalizeProcessStrategy(proc.Strategy, proc.Argv)
	if _, ok := allow[processAllowlistKey(strategy, proc.Argv)]; !ok {
		return processRestoreDecision{Reason: "not_allowlisted"}
	}

	id := proc.Opts.AgentSessionID
	if containsControl(id) {
		return processRestoreDecision{Reason: "invalid_agent_session_id"}
	}
	switch strategy {
	case processStrategyPi:
		if id != "" {
			return processRestoreDecision{Command: shellQuoteArgvMust([]string{"pi", "--resume", id}), Restore: true}
		}
		return processRestoreDecision{Command: "pi --continue", Restore: true}
	case processStrategyClaude:
		if id != "" {
			return processRestoreDecision{Command: shellQuoteArgvMust([]string{"claude", "--resume", id}), Restore: true}
		}
		return processRestoreDecision{Command: "claude --continue", Restore: true}
	case processStrategyOpenCode:
		if id != "" {
			return processRestoreDecision{Command: shellQuoteArgvMust([]string{"opencode", "--session", id}), Restore: true}
		}
		return processRestoreDecision{Command: "opencode --continue", Restore: true}
	case processStrategyCodex:
		if id != "" {
			return processRestoreDecision{Command: shellQuoteArgvMust([]string{"codex", "resume", id}), Restore: true}
		}
		return processRestoreDecision{Command: "codex resume --last", Restore: true}
	}

	command, ok := shellQuoteArgv(proc.Argv)
	if !ok {
		return processRestoreDecision{Reason: "empty_command"}
	}
	return processRestoreDecision{Command: command, Restore: true}
}

func normalizeProcessStrategy(strategy string, argv []string) string {
	switch strategy {
	case processStrategyClaude, processStrategyCodex, processStrategyOpenCode, processStrategyPi:
		return strategy
	case "", processStrategyGeneric:
		return detectProcessStrategy(argv)
	default:
		return processStrategyGeneric
	}
}

func processAllowlistKey(strategy string, argv []string) string {
	switch strategy {
	case processStrategyClaude, processStrategyCodex, processStrategyOpenCode, processStrategyPi:
		return strategy
	default:
		return filepath.Base(argv[0])
	}
}

func containsControl(s string) bool {
	return strings.IndexFunc(s, unicode.IsControl) >= 0
}

func shellQuoteArgvMust(argv []string) string {
	command, _ := shellQuoteArgv(argv)
	return command
}

func shellQuoteArgv(argv []string) (string, bool) {
	if len(argv) == 0 {
		return "", false
	}
	quoted := make([]string, 0, len(argv))
	for _, arg := range argv {
		quoted = append(quoted, shellQuote(arg))
	}
	return strings.Join(quoted, " "), true
}

func shellQuote(s string) string {
	if s == "" {
		return "''"
	}
	if strings.IndexFunc(s, func(r rune) bool {
		return r != '-' && r != '_' && r != '.' && r != '/' && r != ':' && r != '+' && r != '=' && !unicode.IsLetter(r) && !unicode.IsDigit(r)
	}) == -1 {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}
