package main

import (
	"fmt"
	"os"

	"bnema.dev/vev/internal/app"
)

func main() {
	if err := app.Run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, cliError(err))
		os.Exit(app.ExitCode(err))
	}
}

// cliError adds the sole user-facing CLI prefix; internal errors never carry
// it.
func cliError(err error) string {
	return "vev: " + err.Error()
}
