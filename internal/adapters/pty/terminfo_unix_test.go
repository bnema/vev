//go:build linux || darwin

package pty

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestPaneEnvironment(t *testing.T) {
	for _, tt := range []struct {
		name    string
		helper  string
		want    string
		symlink bool
	}{
		{name: "available", helper: "exit 0", want: "xterm-direct"},
		{name: "symlinked working directory", helper: "exit 0", want: "xterm-direct", symlink: true},
		{name: "missing entry", helper: "exit 1", want: "xterm-256color"},
		{name: "missing helper", want: "xterm-256color"},
		{name: "hung helper", helper: "exec /bin/sleep 60", want: "xterm-256color"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			bin := t.TempDir()
			dir := t.TempDir()
			if tt.symlink {
				link := filepath.Join(t.TempDir(), "pane-cwd")
				require.NoError(t, os.Symlink(dir, link))
				dir = link
			}
			t.Setenv("PATH", bin)
			if tt.helper != "" {
				// The helper must use the pane's lookup environment and working
				// directory, but must not be resolved through the pane's PATH.
				// Compare directory identity: shells can resolve symlinked temp
				// paths (including macOS's /var) when initializing PWD.
				script := "#!/bin/sh\n" +
					"[ \"$1\" = -x ] && [ \"$2\" = xterm-direct ] && [ \"$#\" = 2 ] || exit 2\n" +
					"[ \"$HOME\" = /pane-home ] && [ \"$TERMINFO\" = relative-db ] && [ \"$TERMINFO_DIRS\" = /pane-db: ] || exit 3\n" +
					"[ . -ef \"$EXPECTED_DIR\" ] || exit 4\n" + tt.helper + "\n"
				require.NoError(t, os.WriteFile(filepath.Join(bin, "infocmp"), []byte(script), 0o700))
			}
			env := []string{"TERM=outer", "KEEP=a=b", "TERM=duplicate", "COLORTERM=truecolor", "TERM_PROGRAM=vev", "HOME=/pane-home", "TERMINFO=relative-db", "TERMINFO_DIRS=/pane-db:", "PATH=/not-the-daemon-path", "EXPECTED_DIR=" + dir}
			before := slices.Clone(env)
			got := paneEnvironment(t.Context(), env, dir)
			require.Equal(t, before, env)
			want := append(slices.Clone(env[1:2]), env[3:]...)
			want = append(want, "TERM="+tt.want)
			require.Equal(t, want, got)
		})
	}
}

func TestPaneEnvironmentNilInherits(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	t.Setenv("TERM", "outer")
	t.Setenv("PANE_ENV_TEST", "inherited")
	env := paneEnvironment(t.Context(), nil, t.TempDir())
	require.Contains(t, env, "PANE_ENV_TEST=inherited")
	require.Contains(t, env, "TERM=xterm-256color")
	require.NotContains(t, env, "TERM=outer")
}

func TestOpenCancelledDuringTerminfoProbe(t *testing.T) {
	bin := t.TempDir()
	t.Setenv("PATH", bin)
	require.NoError(t, os.WriteFile(filepath.Join(bin, "infocmp"), []byte("#!/bin/sh\nexec /bin/sleep 60\n"), 0o700))
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	p, err := NewFactory().Open(ctx, "/bin/sh", []string{"-c", "exit 0"}, nil, t.TempDir(), domain.Geometry{})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, p)
}
