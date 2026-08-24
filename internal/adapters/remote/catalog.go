package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/bnema/vev/internal/adapters/sshstdio"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

const (
	maxCatalogBytes           = 1 << 20
	maxCatalogDiagnosticBytes = 4 << 10
	catalogCommandTimeout     = 30 * time.Second
	catalogCommandWaitDelay   = 500 * time.Millisecond
)

var (
	errCatalogDecode   = errors.New("remote catalog: invalid JSON")
	errCatalogTrailing = errors.New("remote catalog: trailing bytes after JSON")
	errCatalogRequired = errors.New("remote catalog: missing required fields")
	errCatalogSSH      = errors.New("remote catalog: ssh command failed")
	errCatalogTooLarge = errors.New("remote catalog: output exceeds size limit")
)

// CatalogClient fetches remote session catalogs over SSH.
type CatalogClient struct {
	command func(ctx context.Context, name string, args ...string) *exec.Cmd
	// timeout bounds each List when > 0; otherwise catalogCommandTimeout is used.
	timeout time.Duration
}

var _ ports.RemoteCatalogClient = (*CatalogClient)(nil)

// NewCatalogClient returns a RemoteCatalogClient that shells out to ssh.
func NewCatalogClient() ports.RemoteCatalogClient {
	return &CatalogClient{command: exec.CommandContext}
}

func (c *CatalogClient) listTimeout() time.Duration {
	if c != nil && c.timeout > 0 {
		return c.timeout
	}
	return catalogCommandTimeout
}

// List runs `ssh -- <target> 'vev' 'cmd' 'remote-catalog' '--json'` and decodes
// exactly one versioned catalog envelope from stdout. List always applies a
// bounded command timeout derived from ctx so a caller with no deadline cannot
// hang indefinitely, while still honoring caller cancellation.
func (c *CatalogClient) List(ctx context.Context, target string) (ports.RemoteCatalog, error) {
	if err := ctx.Err(); err != nil {
		return ports.RemoteCatalog{}, err
	}
	if err := domain.ValidateRemoteHostTarget(target); err != nil {
		slog.Debug("remote catalog rejected target", "target", target, "err", err)
		return ports.RemoteCatalog{}, err
	}

	runCtx, cancel := context.WithTimeout(ctx, c.listTimeout())
	defer cancel()

	command := c.command
	if command == nil {
		command = exec.CommandContext
	}
	spec := sshstdio.BuildCommandForRemoteCommand(target, "vev", "cmd", "remote-catalog", "--json")
	cmd := command(runCtx, spec.Path, spec.Args...)
	stdout := boundedBuffer{limit: maxCatalogBytes}
	stderr := boundedBuffer{limit: maxCatalogDiagnosticBytes}
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.WaitDelay = catalogCommandWaitDelay
	err := cmd.Run()
	if ctxErr := runCtx.Err(); ctxErr != nil {
		return ports.RemoteCatalog{}, ctxErr
	}
	if err != nil {
		if stdout.overflow || stderr.overflow {
			slog.Debug("remote catalog output too large", "target", target, "stdout_limit", maxCatalogBytes, "stderr_limit", maxCatalogDiagnosticBytes)
			return ports.RemoteCatalog{}, errCatalogTooLarge
		}
		stderrText := sanitizeCatalogDiagnostic(string(stderr.Bytes()))
		slog.Debug("remote catalog ssh failed", "target", target, "err", err, "stderr", stderrText)
		if stderrText != "" {
			return ports.RemoteCatalog{}, fmt.Errorf("%w: %w: %s", errCatalogSSH, err, stderrText)
		}
		return ports.RemoteCatalog{}, fmt.Errorf("%w: %w", errCatalogSSH, err)
	}
	if stdout.overflow || stderr.overflow {
		slog.Debug("remote catalog output too large", "target", target, "stdout_limit", maxCatalogBytes, "stderr_limit", maxCatalogDiagnosticBytes)
		return ports.RemoteCatalog{}, errCatalogTooLarge
	}

	catalog, err := decodeRemoteCatalog(stdout.Bytes())
	if err != nil {
		slog.Debug("remote catalog decode failed", "target", target, "err", err)
		return ports.RemoteCatalog{}, err
	}
	return catalog, nil
}

type catalogEnvelope struct {
	ProtocolVersion *uint16                       `json:"protocol_version"`
	SchemaVersion   *uint16                       `json:"schema_version"`
	Sessions        *[]ports.RemoteCatalogSession `json:"sessions"`
}

func decodeRemoteCatalog(raw []byte) (ports.RemoteCatalog, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	var envelope catalogEnvelope
	if err := dec.Decode(&envelope); err != nil {
		return ports.RemoteCatalog{}, fmt.Errorf("%w: %w", errCatalogDecode, err)
	}
	if envelope.ProtocolVersion == nil || envelope.Sessions == nil {
		return ports.RemoteCatalog{}, errCatalogRequired
	}
	if envelope.SchemaVersion == nil {
		return ports.RemoteCatalog{}, &ports.RemoteCatalogVersionMismatchError{
			Got:  0,
			Want: ports.RemoteCatalogSchemaVersion,
			Kind: "catalog",
		}
	}
	if err := rejectTrailingJSON(dec); err != nil {
		return ports.RemoteCatalog{}, err
	}

	schemaVersion := *envelope.SchemaVersion
	if *envelope.ProtocolVersion != ports.ProtocolVersion {
		return ports.RemoteCatalog{}, &ports.RemoteCatalogVersionMismatchError{
			Got:  *envelope.ProtocolVersion,
			Want: ports.ProtocolVersion,
			Kind: "protocol",
		}
	}
	if schemaVersion != ports.RemoteCatalogSchemaVersion {
		return ports.RemoteCatalog{}, &ports.RemoteCatalogVersionMismatchError{
			Got:  schemaVersion,
			Want: ports.RemoteCatalogSchemaVersion,
			Kind: "catalog",
		}
	}

	sessions := *envelope.Sessions
	if sessions == nil {
		sessions = []ports.RemoteCatalogSession{}
	}
	catalog := ports.RemoteCatalog{
		ProtocolVersion: *envelope.ProtocolVersion,
		SchemaVersion:   schemaVersion,
		Sessions:        sessions,
	}
	if err := ports.ValidateRemoteCatalog(catalog); err != nil {
		return ports.RemoteCatalog{}, err
	}
	return catalog, nil
}

func rejectTrailingJSON(dec *json.Decoder) error {
	var extra json.RawMessage
	err := dec.Decode(&extra)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return errCatalogTrailing
	}
	return fmt.Errorf("%w: %w", errCatalogTrailing, err)
}

// sanitizeCatalogDiagnostic strips terminal escapes and control bytes while
// preserving useful printable diagnostic text for errors and logs.
func sanitizeCatalogDiagnostic(raw string) string {
	var b strings.Builder
	b.Grow(len(raw))
	for i := 0; i < len(raw); {
		if raw[i] == 0x1b {
			i = skipTerminalEscape(raw, i+1)
			continue
		}
		r, size := utf8.DecodeRuneInString(raw[i:])
		if r == utf8.RuneError && size == 1 {
			i++
			continue
		}
		if unicode.IsPrint(r) || r == '\t' || r == '\n' {
			b.WriteRune(r)
		}
		i += size
	}
	return strings.TrimSpace(b.String())
}

func skipTerminalEscape(s string, i int) int {
	if i >= len(s) {
		return i
	}
	switch s[i] {
	case '[': // CSI: ESC [ ... final byte in 0x40-0x7E
		i++
		for i < len(s) {
			ch := s[i]
			i++
			if ch >= 0x40 && ch <= 0x7e {
				break
			}
		}
		return i
	case ']': // OSC: ESC ] ... BEL or ST (ESC \)
		i++
		for i < len(s) {
			if s[i] == 0x07 {
				return i + 1
			}
			if s[i] == 0x1b && i+1 < len(s) && s[i+1] == '\\' {
				return i + 2
			}
			i++
		}
		return i
	default:
		// Two-byte Fe escape or unknown: drop ESC and the next byte.
		return i + 1
	}
}

// boundedBuffer captures stream output up to an explicit byte limit and records overflow.
type boundedBuffer struct {
	buf      bytes.Buffer
	limit    int
	overflow bool
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	if b.overflow {
		return len(p), nil
	}
	remaining := b.limit - b.buf.Len()
	if remaining <= 0 {
		b.overflow = true
		return len(p), nil
	}
	if len(p) <= remaining {
		return b.buf.Write(p)
	}
	n, err := b.buf.Write(p[:remaining])
	b.overflow = true
	if err != nil {
		return n, err
	}
	return len(p), nil
}

func (b *boundedBuffer) Bytes() []byte {
	return b.buf.Bytes()
}
