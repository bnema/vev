package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/bnema/vev/internal/adapters/sshstdio"
	"github.com/bnema/vev/internal/domain"
	appports "github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

const (
	maxCatalogBytes           = 1 << 20
	maxCatalogDiagnosticBytes = 4 << 10
	catalogCommandTimeout     = 10 * time.Second
	catalogSSHConnectTimeout  = 5 * time.Second
	catalogCommandWaitDelay   = 500 * time.Millisecond
)

var (
	errCatalogDecode   = errors.New("remote catalog: invalid JSON")
	errCatalogTrailing = errors.New("remote catalog: trailing bytes after JSON")
	errCatalogRequired = errors.New("remote catalog: missing required fields")
	errCatalogSSH      = errors.New("remote catalog: ssh command failed")
	errCatalogTooLarge = errors.New("remote catalog: output exceeds size limit")
)

// sshClientTrustMarkers are OpenSSH-client-originated diagnostics for host-key
// trust failures. They are only consulted when the ssh client itself exited
// 255, which separates client-side failures from remote command output.
var sshClientTrustMarkers = []string{
	"Host key verification failed.",
	"REMOTE HOST IDENTIFICATION HAS CHANGED",
	"Host key for ",
	"No host key is known ",
	"No ECDSA host key is known ",
}

// sshClientAuthMarkers are OpenSSH-client diagnostics for explicit
// authentication failures. Unlike host-key text, these phrases can also be
// emitted by a remote command that exits 255 (ssh propagates the remote
// status), so they only classify when ssh-client framing proves client
// provenance: an `ssh:` line prefix or a `host:` label prefix on the same
// line. Bare matches stay generic transport errors.
var sshClientAuthMarkers = []string{
	"Permission denied ",
	"Permission denied (",
	"Too many authentication failures",
	"Authentication failed",
}

// sshClientTimeoutMarkers are OpenSSH-client diagnostics for bounded
// connect/command timeouts. As with authentication text, they require
// ssh-client framing; unframed matches stay generic transport errors.
var sshClientTimeoutMarkers = []string{
	"Connection timed out",
	"timed out",
	"Timeout, server not responding",
}

// ClassifyCatalogError maps an observation failure to its sanitized failure
// kind without leaking raw stderr, keys, endpoints or terminal contents.
// Unrecognized SSH errors stay generic transport errors: authentication is
// never inferred from arbitrary remote stderr, only from explicit ssh-client
// diagnostics on ssh client exit 255.
func ClassifyCatalogError(err error) domain.RemoteFailureKind {
	if err == nil {
		return domain.RemoteFailureNone
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return domain.RemoteFailureTimeout
	}
	var mismatch *catalogue.RemoteCatalogVersionMismatchError
	if errors.As(err, &mismatch) {
		return domain.RemoteFailureIncompatible
	}
	if errors.Is(err, errCatalogTooLarge) ||
		errors.Is(err, errCatalogDecode) ||
		errors.Is(err, errCatalogTrailing) ||
		errors.Is(err, errCatalogRequired) ||
		errors.Is(err, catalogue.ErrInvalidRemoteCatalog) {
		return domain.RemoteFailureInvalidResponse
	}
	if errors.Is(err, errCatalogSSH) && sshClientExit(err) {
		text := err.Error()
		for _, marker := range sshClientTrustMarkers {
			if strings.Contains(text, marker) {
				return domain.RemoteFailureTrust
			}
		}
		for _, marker := range sshClientAuthMarkers {
			if hasClientFramedMarker(text, marker) {
				return domain.RemoteFailureAuthentication
			}
		}
		for _, marker := range sshClientTimeoutMarkers {
			if hasClientFramedMarker(text, marker) {
				return domain.RemoteFailureTimeout
			}
		}
	}
	if timeoutError(err) {
		return domain.RemoteFailureTimeout
	}
	return domain.RemoteFailureTransport
}

// hasClientFramedMarker reports whether marker appears with ssh-client
// framing: on a line starting with `ssh:`, or after a `host:` label prefix
// on the same line (the `destination: message` form ssh uses for its own
// errors). Only the stderr tail past the client's exit status is examined
// so the wrapper's own colons never count as framing. A remote command
// exiting 255 can emit the same phrases without framing, so unframed
// matches prove nothing and stay transport errors.
func hasClientFramedMarker(text, marker string) bool {
	diag := text
	if _, after, ok := strings.Cut(text, "exit status 255: "); ok {
		diag = after
	}
	for _, line := range strings.Split(diag, "\n") {
		if !strings.Contains(line, marker) {
			continue
		}
		if strings.HasPrefix(line, "ssh:") {
			return true
		}
		if label, _, ok := strings.Cut(line, ": "); ok && !strings.ContainsAny(label, " \t:") {
			return true
		}
	}
	return false
}

// sshClientExit reports whether the ssh client itself failed (exit 255),
// separating client-side transport/auth/trust failures from remote command
// output carried in the same wrapped error.
func sshClientExit(err error) bool {
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		return false
	}
	return exit.ExitCode() == 255
}

func timeoutError(err error) bool {
	var timeout interface{ Timeout() bool }
	if errors.As(err, &timeout) && timeout.Timeout() {
		return true
	}
	return errors.Is(err, os.ErrDeadlineExceeded)
}

// CatalogClient fetches remote session catalogs over SSH.
type CatalogClient struct {
	command func(ctx context.Context, name string, args ...string) *exec.Cmd
	// timeout bounds each List when > 0; otherwise catalogCommandTimeout is used.
	timeout time.Duration
}

var _ appports.RemoteCatalogClient = (*CatalogClient)(nil)

// NewCatalogClient returns a RemoteCatalogClient that shells out to ssh.
func NewCatalogClient() appports.RemoteCatalogClient {
	return &CatalogClient{command: exec.CommandContext}
}

func (c *CatalogClient) listTimeout() time.Duration {
	if c != nil && c.timeout > 0 {
		return c.timeout
	}
	return catalogCommandTimeout
}

// List runs a non-interactive `ssh` observation of `vev cmd remote-catalog
// --json` and decodes exactly one versioned catalog envelope from stdout.
// The observation never prompts, allocates a TTY, mutates trust, or executes
// host strings as shell fragments. List always applies a bounded command
// timeout derived from ctx so a caller with no deadline cannot hang
// indefinitely, while still honoring caller cancellation; owned SSH
// processes are killed and reaped on cancellation with a bounded wait.
func (c *CatalogClient) List(ctx context.Context, target string) (catalogue.RemoteCatalog, error) {
	if err := ctx.Err(); err != nil {
		return catalogue.RemoteCatalog{}, err
	}
	if err := domain.ValidateRemoteHostTarget(target); err != nil {
		slog.Debug("remote catalog rejected target", "target", target, "err", err)
		return catalogue.RemoteCatalog{}, err
	}

	runCtx, cancel := context.WithTimeout(ctx, c.listTimeout())
	defer cancel()

	command := c.command
	if command == nil {
		command = exec.CommandContext
	}
	spec := sshstdio.BuildCommandForObservation(target, catalogSSHConnectTimeout, "vev", "cmd", "remote-catalog", "--json")
	cmd := command(runCtx, spec.Path, spec.Args...)
	// Stdin stays detached: exec leaves a nil Stdin on the null device, and
	// batch mode forbids prompts even if output were ever attached to a TTY.
	stdout := boundedBuffer{limit: maxCatalogBytes}
	stderr := boundedBuffer{limit: maxCatalogDiagnosticBytes}
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.WaitDelay = catalogCommandWaitDelay
	err := cmd.Run()
	if ctxErr := runCtx.Err(); ctxErr != nil {
		return catalogue.RemoteCatalog{}, ctxErr
	}
	if err != nil {
		if stdout.overflow || stderr.overflow {
			slog.Debug("remote catalog output too large", "target", target, "stdout_limit", maxCatalogBytes, "stderr_limit", maxCatalogDiagnosticBytes)
			return catalogue.RemoteCatalog{}, errCatalogTooLarge
		}
		stderrText := sanitizeCatalogDiagnostic(string(stderr.Bytes()))
		slog.Debug("remote catalog ssh failed", "target", target, "err", err, "stderr", stderrText)
		if stderrText != "" {
			return catalogue.RemoteCatalog{}, fmt.Errorf("%w: %w: %s", errCatalogSSH, err, stderrText)
		}
		return catalogue.RemoteCatalog{}, fmt.Errorf("%w: %w", errCatalogSSH, err)
	}
	if stdout.overflow || stderr.overflow {
		slog.Debug("remote catalog output too large", "target", target, "stdout_limit", maxCatalogBytes, "stderr_limit", maxCatalogDiagnosticBytes)
		return catalogue.RemoteCatalog{}, errCatalogTooLarge
	}

	catalog, err := decodeRemoteCatalog(stdout.Bytes())
	if err != nil {
		slog.Debug("remote catalog decode failed", "target", target, "err", err)
		return catalogue.RemoteCatalog{}, err
	}
	return catalog, nil
}

type catalogVersionEnvelope struct {
	ProtocolVersion *uint16         `json:"protocol_version"`
	SchemaVersion   *uint16         `json:"schema_version"`
	Sessions        json.RawMessage `json:"sessions"`
}

type catalogEnvelope struct {
	ProtocolVersion *uint16                   `json:"protocol_version"`
	SchemaVersion   *uint16                   `json:"schema_version"`
	Sessions        *[]catalogSessionEnvelope `json:"sessions"`
}

type catalogSessionEnvelope struct {
	LifecycleID *domain.SessionLifecycleID           `json:"lifecycle_id"`
	Name        *string                              `json:"name"`
	State       *catalogue.RemoteCatalogSessionState `json:"state"`
	Ephemeral   *bool                                `json:"ephemeral"`
	Tabs        *[]catalogTabEnvelope                `json:"tabs"`
	Attached    *bool                                `json:"attached"`
	LastUsedSeq uint64                               `json:"last_used_seq,omitempty"`
	ActiveTabID string                               `json:"active_tab_id,omitempty"`
	Reason      string                               `json:"reason,omitempty"`
}

type catalogTabEnvelope struct {
	ID        *string `json:"id"`
	Index     *uint16 `json:"index"`
	Name      *string `json:"name"`
	Detail    string  `json:"detail,omitempty"`
	Attention bool    `json:"attention,omitempty"`
}

func decodeRemoteCatalog(raw []byte) (catalogue.RemoteCatalog, error) {
	var version catalogVersionEnvelope
	if err := decodeCatalogEnvelope(raw, false, &version); err != nil {
		return catalogue.RemoteCatalog{}, err
	}
	if version.ProtocolVersion == nil {
		return catalogue.RemoteCatalog{}, errCatalogRequired
	}
	if version.SchemaVersion == nil {
		return catalogue.RemoteCatalog{}, &catalogue.RemoteCatalogVersionMismatchError{
			Got:  0,
			Want: catalogue.RemoteCatalogSchemaVersion,
			Kind: "catalog",
		}
	}
	if *version.ProtocolVersion != protocol.Version {
		return catalogue.RemoteCatalog{}, &catalogue.RemoteCatalogVersionMismatchError{
			Got:  *version.ProtocolVersion,
			Want: protocol.Version,
			Kind: "protocol",
		}
	}
	if *version.SchemaVersion != catalogue.RemoteCatalogSchemaVersion {
		return catalogue.RemoteCatalog{}, &catalogue.RemoteCatalogVersionMismatchError{
			Got:  *version.SchemaVersion,
			Want: catalogue.RemoteCatalogSchemaVersion,
			Kind: "catalog",
		}
	}

	var envelope catalogEnvelope
	if err := decodeCatalogEnvelope(raw, true, &envelope); err != nil {
		return catalogue.RemoteCatalog{}, err
	}
	if envelope.ProtocolVersion == nil || envelope.SchemaVersion == nil || envelope.Sessions == nil {
		return catalogue.RemoteCatalog{}, errCatalogRequired
	}
	sessions := make([]catalogue.RemoteCatalogSession, 0, len(*envelope.Sessions))
	for _, session := range *envelope.Sessions {
		if session.LifecycleID == nil || session.Name == nil || session.State == nil || session.Ephemeral == nil || session.Tabs == nil || session.Attached == nil {
			return catalogue.RemoteCatalog{}, errCatalogRequired
		}
		tabs := make([]catalogue.RemoteCatalogTab, 0, len(*session.Tabs))
		for _, tab := range *session.Tabs {
			if tab.ID == nil || tab.Index == nil || tab.Name == nil {
				return catalogue.RemoteCatalog{}, errCatalogRequired
			}
			tabs = append(tabs, catalogue.RemoteCatalogTab{
				ID: *tab.ID, Index: *tab.Index, Name: *tab.Name,
				Detail: tab.Detail, Attention: tab.Attention,
			})
		}
		sessions = append(sessions, catalogue.RemoteCatalogSession{
			LifecycleID: *session.LifecycleID, Name: *session.Name, State: *session.State,
			Ephemeral: *session.Ephemeral, Tabs: tabs, Attached: *session.Attached,
			LastUsedSeq: session.LastUsedSeq, ActiveTabID: session.ActiveTabID, Reason: session.Reason,
		})
	}
	catalog := catalogue.RemoteCatalog{
		ProtocolVersion: *envelope.ProtocolVersion,
		SchemaVersion:   *envelope.SchemaVersion,
		Sessions:        sessions,
	}
	if err := catalogue.ValidateRemoteCatalog(catalog); err != nil {
		return catalogue.RemoteCatalog{}, err
	}
	return catalog, nil
}

func decodeCatalogEnvelope(raw []byte, strict bool, dst any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	if strict {
		dec.DisallowUnknownFields()
	}
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("%w: %w", errCatalogDecode, err)
	}
	return rejectTrailingJSON(dec)
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
