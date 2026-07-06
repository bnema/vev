package daemon

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	barScriptOutputLimit  = 1024
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
	timeout time.Duration
	baseEnv []string
}

func (r barScriptRunner) run(ctx context.Context, command string, scriptCtx barScriptContext) (string, error) {
	if strings.TrimSpace(command) == "" {
		return "", nil
	}
	timeout := r.timeout
	if timeout <= 0 {
		timeout = barScriptTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "sh", "-c", command)
	cmd.Env = scriptCtx.env(r.baseEnv)
	var stdout boundedBuffer
	stdout.limit = barScriptOutputLimit
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return "", context.DeadlineExceeded
	}
	text := sanitizeBarScriptOutput(stdout.Bytes(), barScriptOutputLimit)
	if err != nil {
		return text, err
	}
	return text, nil
}

type boundedBuffer struct {
	buf   bytes.Buffer
	limit int
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	if b.limit <= 0 {
		return len(p), nil
	}
	remaining := b.limit - b.buf.Len()
	if remaining > 0 {
		if len(p) < remaining {
			remaining = len(p)
		}
		_, _ = b.buf.Write(p[:remaining])
	}
	return len(p), nil
}

func (b *boundedBuffer) Bytes() []byte { return b.buf.Bytes() }

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
		if r == '\x1b' {
			i += size
			if i < len(s) && s[i] == '[' {
				i++
				for i < len(s) {
					c := s[i]
					i++
					if c >= 0x40 && c <= 0x7e {
						break
					}
				}
			}
			continue
		}
		if r < 0x20 || r == 0x7f {
			i += size
			continue
		}
		out.WriteRune(r)
		i += size
	}
	return out.String()
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

var _ io.Writer = (*boundedBuffer)(nil)
