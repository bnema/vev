package main

import (
	"fmt"
	"os"

	"github.com/bnema/vev/internal/app"
)

func main() {
	if err := app.Run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "vev:", err)
		os.Exit(1)
	}
}
