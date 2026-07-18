package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/bnema/vev/internal/app"
)

func main() {
	if err := app.Run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, cliError(err))
		os.Exit(1)
	}
}

// cliError adds the sole user-facing CLI prefix. Errors may already include
// it when they originate from a runtime component, so remove any such prefix
// before rendering.
func cliError(err error) string {
	message := err.Error()
	for strings.HasPrefix(message, "vev:") {
		message = strings.TrimSpace(strings.TrimPrefix(message, "vev:"))
	}
	return "vev: " + message
}
