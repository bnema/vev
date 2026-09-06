//go:build linux

package pty_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestOpenSelectsPaneTerminal(t *testing.T) {
	for _, tt := range []struct {
		name string
		exit string
		want string
	}{
		{name: "direct", exit: "0", want: "xterm-direct"},
		{name: "fallback", exit: "1", want: "xterm-256color"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			bin := t.TempDir()
			t.Setenv("PATH", bin)
			require.NoError(t, os.WriteFile(filepath.Join(bin, "infocmp"), []byte("#!/bin/sh\nexit "+tt.exit+"\n"), 0o700))
			env := []string{"TERM=xterm-256color", "COLORTERM=truecolor", "TERM_PROGRAM=vev"}
			p, err := newFactory().Open(t.Context(), "/bin/sh", []string{"-c", `printf '%s|%s|%s' "$TERM" "$COLORTERM" "$TERM_PROGRAM"`}, env, t.TempDir(), domain.Geometry{})
			require.NoError(t, err)
			t.Cleanup(func() { _ = p.Close() })
			require.Equal(t, tt.want+"|truecolor|vev", string(readAll(t, p, 5*time.Second)))
		})
	}
}
