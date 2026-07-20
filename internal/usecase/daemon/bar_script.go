package daemon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/bnema/vev/internal/ports"
)

const (
	barScriptOutputLimit  = 1024
	barScriptStderrLimit  = 512
	barScriptDisplayLimit = 256
	barScriptTimeout      = time.Second
)

type barScriptContext struct {
	Anchor  string
	Session string
	Tab     string
	Pane    string
	PaneCWD string
	Cols    int
}

func (c barScriptContext) env(base []string) []string {
	vars := map[string]string{
		"VEV_ANCHOR":   c.Anchor,
		"VEV_SESSION":  c.Session,
		"VEV_TAB":      c.Tab,
		"VEV_PANE":     c.Pane,
		"VEV_PANE_CWD": c.PaneCWD,
		"VEV_COLS":     strconv.Itoa(c.Cols),
	}
	out := make([]string, 0, len(base)+len(vars))
	for _, entry := range base {
		key, _, ok := strings.Cut(entry, "=")
		if ok {
			if _, replace := vars[key]; replace {
				continue
			}
		}
		out = append(out, entry)
	}
	for _, key := range []string{"VEV_ANCHOR", "VEV_SESSION", "VEV_TAB", "VEV_PANE", "VEV_PANE_CWD", "VEV_COLS"} {
		out = append(out, key+"="+vars[key])
	}
	return out
}

type barScriptRunner struct {
	runner  ports.ShellCommandRunner
	timeout time.Duration
}

func (r barScriptRunner) run(ctx context.Context, command string, env []string, scriptCtx barScriptContext) (string, error) {
	if strings.TrimSpace(command) == "" {
		return "", nil
	}
	if r.runner == nil {
		return "", errors.New("bar script command runner is nil")
	}
	timeout := r.timeout
	if timeout <= 0 {
		timeout = barScriptTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	res, err := r.runner.Run(ctx, ports.CommandSpec{
		Command:     command,
		Env:         scriptCtx.env(env),
		Timeout:     timeout,
		StdoutLimit: barScriptOutputLimit,
		StderrLimit: barScriptStderrLimit,
	})
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return "", context.DeadlineExceeded
	}
	text := sanitizeBarScriptOutput(res.Stdout, barScriptOutputLimit)
	if err != nil {
		return text, &barScriptError{
			exitCode: res.ExitCode,
			stderr:   sanitizeBarScriptOutput(res.Stderr, barScriptStderrLimit),
			err:      err,
		}
	}
	return text, nil
}

func sanitizeBarScriptOutput(raw []byte, limit int) string {
	if limit <= 0 || limit > len(raw) {
		limit = len(raw)
	}
	raw = raw[:limit]
	if i := bytes.IndexAny(raw, "\r\n"); i >= 0 {
		raw = raw[:i]
	}
	text := strings.ToValidUTF8(string(raw), "")
	text = stripANSIAndControls(text)
	if len(text) <= barScriptDisplayLimit {
		return text
	}
	return trimUTF8Bytes(text, barScriptDisplayLimit)
}

func stripANSIAndControls(s string) string {
	var out strings.Builder
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		switch {
		case r == '\x1b':
			i = skipEscapeSequence(s, i+size)
			continue
		case r == 0x9b:
			i = skipCSI(s, i+size)
			continue
		case r == 0x90 || r == 0x98 || r == 0x9d || r == 0x9e || r == 0x9f:
			i = skipStringControl(s, i+size)
			continue
		case r < 0x20 || r == 0x7f || (r >= 0x80 && r <= 0x9f):
			i += size
			continue
		}
		out.WriteRune(r)
		i += size
	}
	return out.String()
}

func skipEscapeSequence(s string, i int) int {
	if i >= len(s) {
		return i
	}
	switch s[i] {
	case '[':
		return skipCSI(s, i+1)
	case ']', 'P', '^', '_', 'X':
		return skipStringControl(s, i+1)
	}
	for i < len(s) && s[i] >= 0x20 && s[i] <= 0x2f {
		i++
	}
	if i < len(s) && s[i] >= 0x30 && s[i] <= 0x7e {
		return i + 1
	}
	return i
}

func skipCSI(s string, i int) int {
	for i < len(s) {
		c := s[i]
		i++
		if c >= 0x40 && c <= 0x7e {
			break
		}
	}
	return i
}

func skipStringControl(s string, i int) int {
	for i < len(s) {
		if s[i] == '\a' {
			return i + 1
		}
		if s[i] == '\x1b' && i+1 < len(s) && s[i+1] == '\\' {
			return i + 2
		}
		_, size := utf8.DecodeRuneInString(s[i:])
		i += size
	}
	return i
}

func trimUTF8Bytes(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	for limit > 0 && !utf8.ValidString(s[:limit]) {
		limit--
	}
	return s[:limit]
}

// barScriptError carries the diagnostic detail needed to explain why a bar
// script failed: the process exit code and whatever it wrote to stderr.
type barScriptError struct {
	exitCode int
	stderr   string
	err      error
}

func (e *barScriptError) Error() string {
	if e.stderr != "" {
		return fmt.Sprintf("exit %d: %s", e.exitCode, e.stderr)
	}
	return fmt.Sprintf("exit %d", e.exitCode)
}

func (e *barScriptError) Unwrap() error { return e.err }

// pathFromEnv returns the PATH entry's value, or "" when unset.
func pathFromEnv(env []string) string {
	for _, entry := range env {
		if name, value, ok := environmentEntry(entry); ok && name == "PATH" {
			return value
		}
	}
	return ""
}
