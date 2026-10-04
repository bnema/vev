package main

import (
	"errors"
	"fmt"
	"testing"

	"github.com/bnema/vev/internal/app"
)

func TestCLIError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "usage error",
			err:  runError([]string{"unknown"}),
			want: `vev: unknown command "unknown"`,
		},
		{
			name: "domain validation error",
			err:  runError([]string{"new", "invalid name"}),
			want: "vev: invalid session name: must match [A-Za-z0-9][A-Za-z0-9._-]{0,63}",
		},
		{
			name: "wrapped runtime error",
			err:  fmt.Errorf("daemon listen: %w", errors.New("address already in use")),
			want: "vev: daemon listen: address already in use",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := cliError(tt.err); got != tt.want {
				t.Errorf("cliError(%v) = %q, want %q", tt.err, got, tt.want)
			}
		})
	}
}

func runError(args []string) error {
	return app.Run(args)
}
