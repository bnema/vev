package daemon

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

func TestBarScriptSanitizeOutput(t *testing.T) {
	tests := []struct {
		name  string
		raw   string
		limit int
		want  string
	}{
		{name: "first line only", raw: "one\ntwo\n", limit: barScriptOutputLimit, want: "one"},
		{name: "ansi stripped", raw: "\x1b[31mred\x1b[0m", limit: barScriptOutputLimit, want: "red"},
		{name: "controls stripped", raw: "a\x00b\x07c", limit: barScriptOutputLimit, want: "abc"},
		{name: "utf8 and nerd font preserved", raw: "󰍛 14:32 ↑3", limit: barScriptOutputLimit, want: "󰍛 14:32 ↑3"},
		{name: "display capped", raw: strings.Repeat("a", barScriptDisplayLimit+10), limit: barScriptOutputLimit, want: strings.Repeat("a", barScriptDisplayLimit)},
		{name: "capture capped before first line", raw: strings.Repeat("a", 8) + "\nignored", limit: 4, want: "aaaa"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sanitizeBarScriptOutput([]byte(tt.raw), tt.limit); got != tt.want {
				t.Fatalf("sanitizeBarScriptOutput() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestBarScriptContextEnv(t *testing.T) {
	base := []string{
		"PATH=/bin",
		"VEV_ANCHOR=old",
		"VEV_SESSION=old",
		"VEV_TAB=old",
		"VEV_PANE=old",
		"VEV_PANE_CWD=/old",
		"VEV_COLS=1",
	}
	ctx := barScriptContext{Anchor: "top-right", Session: "work", Tab: "2", Pane: "pane-3", PaneCWD: "/repo", Cols: 120}
	got := ctx.env(base)

	want := map[string]string{
		"VEV_ANCHOR":   "top-right",
		"VEV_SESSION":  "work",
		"VEV_TAB":      "2",
		"VEV_PANE":     "pane-3",
		"VEV_PANE_CWD": "/repo",
		"VEV_COLS":     "120",
	}
	seen := map[string]int{}
	for _, entry := range got {
		key, val, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		if _, isVEV := want[key]; isVEV {
			seen[key]++
			if val != want[key] {
				t.Fatalf("%s = %q, want %q in env %v", key, val, want[key], got)
			}
		}
	}
	for key := range want {
		if seen[key] != 1 {
			t.Fatalf("%s appears %d times, want 1 in env %v", key, seen[key], got)
		}
	}
}

func TestBarScriptRunner(t *testing.T) {
	runner := barScriptRunner{timeout: 50 * time.Millisecond, baseEnv: os.Environ()}
	got, err := runner.run(context.Background(), "", barScriptContext{})
	if err != nil || got != "" {
		t.Fatalf("empty command = %q, %v; want empty nil", got, err)
	}

	got, err = runner.run(context.Background(), "printf 'ok\\nignored'", barScriptContext{})
	if err != nil {
		t.Fatalf("run command: %v", err)
	}
	if got != "ok" {
		t.Fatalf("run command = %q, want ok", got)
	}

	got, err = runner.run(context.Background(), "sleep 1; printf late", barScriptContext{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout err = %v, want DeadlineExceeded", err)
	}
	if got != "" {
		t.Fatalf("timeout output = %q, want empty", got)
	}
}

func TestBarScriptRunnerBoundsStdoutBeforeSanitize(t *testing.T) {
	runner := barScriptRunner{timeout: time.Second, baseEnv: os.Environ()}
	got, err := runner.run(context.Background(), "yes a | head -c 4096", barScriptContext{})
	if err != nil {
		t.Fatalf("run bounded output: %v", err)
	}
	if len(got) > barScriptDisplayLimit {
		t.Fatalf("output length = %d, want <= %d", len(got), barScriptDisplayLimit)
	}
}
