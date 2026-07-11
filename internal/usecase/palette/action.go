package palette

import (
	"strings"

	"github.com/bnema/vev/internal/usecase/command"
)

// Action is the exact command token and its unmodified, whitespace-delimited
// arguments. It is deliberately independent of palette state.
type Action struct {
	Command command.Command
	Args    []string
}

// ParseAction recognizes an exact effective command token. It accepts outer
// and repeated whitespace but never treats a prefix or concatenated token as
// a command. Static commands reject arguments.
func ParseAction(commands []command.Command, input string) (Action, bool) {
	fields := strings.Fields(input)
	if len(fields) == 0 {
		return Action{}, false
	}
	for _, cmd := range commands {
		if !strings.EqualFold(fields[0], cmd.Code) {
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

// ArgumentCommand returns the exact argument-taking command whose token is
// being typed, including the whitespace-before-argument state.
func ArgumentCommand(commands []command.Command, input string) (command.Command, bool) {
	fields := strings.Fields(input)
	if len(fields) == 0 {
		return command.Command{}, false
	}
	for _, cmd := range commands {
		if cmd.Arguments == command.ArgumentsRequired && strings.EqualFold(fields[0], cmd.Code) {
			return cmd, true
		}
	}
	return command.Command{}, false
}
