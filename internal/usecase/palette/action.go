package palette

import (
	"strings"
	"unicode"

	"github.com/bnema/vev/internal/usecase/command"
)

// Action is the exact command token and its unmodified, whitespace-delimited
// arguments. It is deliberately independent of palette state.
type Action struct {
	Command command.Command
	Args    []string
}

// ParseAction recognizes an exact effective command result token. It accepts
// outer and repeated whitespace but never treats a prefix or concatenated token
// as a command. Commands with optional arguments accept zero or more arguments.
func ParseAction(results []Result, input string) (Action, bool) {
	fields := strings.Fields(input)
	if len(fields) == 0 {
		return Action{}, false
	}
	for _, result := range results {
		cmd, ok := result.Command()
		if !ok || !strings.EqualFold(fields[0], cmd.Code) {
			continue
		}
		args := append([]string(nil), fields[1:]...)
		switch cmd.Arguments {
		case command.ArgumentsNone:
			if len(args) != 0 {
				return Action{}, false
			}
		case command.ArgumentsRequired:
			if len(args) == 0 {
				return Action{}, false
			}
		}
		return Action{Command: cmd, Args: args}, true
	}
	return Action{}, false
}

// ExactCommandResult returns a command whose code exactly matches input.
func ExactCommandResult(results []Result, input string) (Result, bool) {
	if strings.Fields(input) == nil || strings.ContainsAny(strings.TrimSpace(input), " \t\n\r") {
		return Result{}, false
	}
	for _, result := range results {
		cmd, ok := result.Command()
		if ok && strings.EqualFold(strings.TrimSpace(input), cmd.Code) {
			return result, true
		}
	}
	return Result{}, false
}

// ArgumentCommand returns the exact argument-capable command whose token is
// being typed, including the whitespace-before-argument state.
func ArgumentCommand(results []Result, input string) (command.Command, bool) {
	_, cmd, ok := argumentResult(results, input)
	return cmd, ok
}

// Preview returns the pure preview text for an exact argument-capable command.
// It never consults mutable daemon state.
func Preview(cmd command.Command, input string) string {
	if cmd.Preview == nil {
		return ""
	}
	fields := strings.Fields(input)
	hasArgument := len(fields) > 1 || strings.TrimRightFunc(input, unicode.IsSpace) != input
	return cmd.Preview(append([]string(nil), fields[1:]...), hasArgument)
}

// argumentResult finds the exact argument-taking command and its source row in
// one pass so callers can reuse the immutable result without another scan.
func argumentResult(results []Result, input string) (Result, command.Command, bool) {
	fields := strings.Fields(input)
	if len(fields) == 0 {
		return Result{}, command.Command{}, false
	}
	for _, result := range results {
		cmd, ok := result.Command()
		if ok && cmd.Arguments != command.ArgumentsNone && strings.EqualFold(fields[0], cmd.Code) {
			return result, cmd, true
		}
	}
	return Result{}, command.Command{}, false
}
