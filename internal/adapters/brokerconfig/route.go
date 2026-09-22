package brokerconfig

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

// Bounded route variants (Plan 001 P3.4 slice E).
//
// A registration's route is the transport the offline broker dials for that
// endpoint. It is an explicit, bounded tagged union, never a caller-supplied
// address: the immutable configuration alone decides whether the broker reaches
// the daemon-mux carriage over a local Unix socket, an SSH stdio child, or an
// SSH-authenticated QUIC bootstrap. The route carries no identity, policy, or
// secret; identity and policy stay authoritative on the registration, and the
// one-time QUIC credentials are minted fresh per physical connection and never
// enter an address, a log, or the configuration.
//
// The route's opaque pool address is a digest of the route's own bounded
// contents, so two distinct routes can never collide and no route detail (an
// SSH target, a path, or an argv word) is exposed through the address the pool
// carries.
type RouteKind string

const (
	// RouteUnix dials one absolute private daemon-mux Unix socket directly.
	RouteUnix RouteKind = "unix"
	// RouteSSHStdio starts one explicit remote mux helper over a local ssh
	// child and carries the daemonmux conversation over its stdio.
	RouteSSHStdio RouteKind = "ssh-stdio"
	// RouteSSHQUIC runs one SSH-authenticated QUIC bootstrap on the remote
	// host and pins the freshly minted ephemeral certificate before dialing.
	RouteSSHQUIC RouteKind = "ssh-quic"
)

// Bounded route limits. They are explicit so a hostile or accidental
// configuration can never grow a route, an argv, or a trust input without
// limit, and so two distinct routes are always distinguishable inside the
// address digest.
const (
	// MaxRouteTargetBytes bounds one ssh target.
	MaxRouteTargetBytes = 256
	// MaxRouteArgvWords bounds the remote argv word count.
	MaxRouteArgvWords = 32
	// MaxRouteArgvBytes bounds the total serialized remote argv.
	MaxRouteArgvBytes = 4096
	// MaxRouteWordBytes bounds one remote argv word.
	MaxRouteWordBytes = 1024
	// MaxTrustPathBytes bounds one trust-input path.
	MaxTrustPathBytes = 1024
	// MaxConnectTimeout bounds a provisioned ssh connect timeout.
	MaxConnectTimeout = 5 * time.Minute

	// MaxRouteAddressBytes bounds the opaque pool address a route exposes.
	MaxRouteAddressBytes = 64
)

// TrustInputs carries the explicit OpenSSH inputs a provisioned ssh route may
// forward. They can only narrow or redirect verification; host-key checking is
// never disabled, authentication is never bypassed, and no PTY is ever
// requested. Every field is optional.
type TrustInputs struct {
	// knownHostsFile, when set, names the absolute known-hosts file ssh verifies
	// against. Verification still happens: an unknown or mismatched key fails.
	knownHostsFile string
	// connectTimeout, when positive, bounds ssh's own connection attempt.
	connectTimeout time.Duration
	// set records whether the trust object was present, so an empty object and
	// an absent object are distinguishable in the route digest.
	set bool
}

// Route is one immutable, validated daemonmux route.
type Route struct {
	kind   RouteKind
	path   string
	target string
	argv   []string
	trust  TrustInputs
	// address is the bounded opaque token the pool carries for this route. It
	// is a digest of the route contents, so it leaks no route detail and can
	// never collide with a distinct route.
	address string
	// local is true for a Unix route that this process can dial directly. Such a
	// route is the carriage a remote-side mux helper bridges to when a
	// registration provisions it; a local binding's own carriage is dialled by
	// the broker itself and is never a helper route.
	local bool
}

// Kind returns the route variant. The zero Route reports an empty kind.
func (r Route) Kind() RouteKind { return r.kind }

// Path returns the absolute private Unix mux socket for a Unix route.
func (r Route) Path() string { return r.path }

// Target returns the ssh target for an ssh route.
func (r Route) Target() string { return r.target }

// Argv returns a copy of the explicit remote argv for an ssh route.
func (r Route) Argv() []string { return append([]string(nil), r.argv...) }

// KnownHostsFile returns the provisioned known-hosts path, if any.
func (r Route) KnownHostsFile() string { return r.trust.knownHostsFile }

// ConnectTimeout returns the provisioned ssh connect timeout, if any.
func (r Route) ConnectTimeout() time.Duration { return r.trust.connectTimeout }

// Address returns the bounded opaque pool address for this route. It never
// contains a secret, an argv word, or a trust path.
func (r Route) Address() string { return r.address }

// IsLocal reports whether the route dials a Unix socket this process can reach
// directly. A local binding's carriage and a registration's Unix carriage are
// both local; only the latter is a route a remote-side mux helper bridges to.
func (r Route) IsLocal() bool { return r.local }

// Validate re-checks one already-parsed route. Parsed routes are validated
// once, so this is the seam a caller uses to fence a zero or hand-built value.
func (r Route) Validate() error {
	switch r.kind {
	case RouteUnix:
		if err := validateRoute(r.path); err != nil {
			return err
		}
		return nil
	case RouteSSHStdio, RouteSSHQUIC:
		return r.validateSSH()
	default:
		return fmt.Errorf("route kind %q is invalid", r.kind)
	}
}

func (r Route) validateSSH() error {
	if err := validateSSHTarget(r.target); err != nil {
		return err
	}
	if err := validateArgv(r.argv); err != nil {
		return err
	}
	if r.trust.knownHostsFile != "" {
		if err := validateTrustPath(r.trust.knownHostsFile); err != nil {
			return err
		}
	}
	if r.trust.connectTimeout < 0 || r.trust.connectTimeout > MaxConnectTimeout {
		return fmt.Errorf("trust connectTimeout must be between 0 and %s", MaxConnectTimeout)
	}
	return nil
}

// validateSSHTarget bounds and sanitizes one ssh target. It reuses the domain
// host-target rule (no whitespace or control characters) and additionally
// bounds the length and refuses a leading dash, so a target can never be
// mistaken for an ssh option even before the option terminator.
func validateSSHTarget(target string) error {
	if err := domain.ValidateRemoteHostTarget(target); err != nil {
		return err
	}
	if len(target) > MaxRouteTargetBytes {
		return fmt.Errorf("ssh target exceeds %d bytes", MaxRouteTargetBytes)
	}
	if strings.HasPrefix(target, "-") {
		return errors.New("ssh target must not start with a dash")
	}
	return nil
}

// validateArgv bounds the explicit remote argv. Every word must be non-empty,
// valid UTF-8, and free of NUL and control characters; the executable is the
// first word. The remote argv is passed to ssh as one POSIX-quoted string, so
// spaces inside a word are safe and are not treated as separators.
func validateArgv(argv []string) error {
	if len(argv) == 0 {
		return errors.New("ssh route requires a non-empty argv")
	}
	if len(argv) > MaxRouteArgvWords {
		return fmt.Errorf("ssh route argv exceeds %d words", MaxRouteArgvWords)
	}
	total := 0
	for i, word := range argv {
		if word == "" {
			return fmt.Errorf("ssh route argv[%d] is empty", i)
		}
		if len(word) > MaxRouteWordBytes {
			return fmt.Errorf("ssh route argv[%d] exceeds %d bytes", i, MaxRouteWordBytes)
		}
		if !utf8.ValidString(word) {
			return fmt.Errorf("ssh route argv[%d] is not valid UTF-8", i)
		}
		for _, r := range word {
			if r == 0 || unicode.IsControl(r) {
				return fmt.Errorf("ssh route argv[%d] contains a control character", i)
			}
		}
		total += len(word)
	}
	if total > MaxRouteArgvBytes {
		return fmt.Errorf("ssh route argv exceeds %d bytes", MaxRouteArgvBytes)
	}
	return nil
}

// validateTrustPath bounds one trust-input path. It must be absolute and clean
// and free of control characters, so it can never be reshaped by a later step
// or inject an ssh option.
func validateTrustPath(path string) error {
	if len(path) > MaxTrustPathBytes {
		return fmt.Errorf("trust path exceeds %d bytes", MaxTrustPathBytes)
	}
	if !utf8.ValidString(path) {
		return errors.New("trust path is not valid UTF-8")
	}
	if !filepath.IsAbs(path) {
		return fmt.Errorf("trust path %q is not absolute", path)
	}
	if filepath.Clean(path) != path {
		return fmt.Errorf("trust path %q is not clean", path)
	}
	for _, r := range path {
		if r == 0 || unicode.IsControl(r) || unicode.IsSpace(r) {
			return fmt.Errorf("trust path %q contains whitespace or control characters", path)
		}
	}
	return nil
}

// routeDocument is the strict on-disk shape of a route object. Unknown fields
// are refused by the decoder, so a route cannot smuggle additional behavior.
type routeDocument struct {
	Kind   string         `json:"kind"`
	Path   string         `json:"path,omitempty"`
	Target string         `json:"target,omitempty"`
	Argv   []string       `json:"argv,omitempty"`
	Trust  *trustDocument `json:"trust,omitempty"`
}

type trustDocument struct {
	KnownHostsFile string `json:"knownHostsFile,omitempty"`
	ConnectTimeout string `json:"connectTimeout,omitempty"`
}

// parseRoute strictly parses one route. A JSON string is the original Unix
// route form (an absolute private mux path); an object carries an explicit
// kind. Unknown kinds, unknown fields, trailing JSON, and a kind/field mismatch
// are refused.
func parseRoute(raw json.RawMessage) (Route, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return Route{}, errors.New("route is empty")
	}
	if trimmed[0] == '"' {
		var path string
		if err := json.Unmarshal(trimmed, &path); err != nil {
			return Route{}, fmt.Errorf("route: %w", err)
		}
		return newUnixRoute(path)
	}
	if trimmed[0] != '{' {
		return Route{}, errors.New("route must be a path string or an object")
	}
	var document routeDocument
	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&document); err != nil {
		return Route{}, fmt.Errorf("route: %w", err)
	}
	if _, err := decoder.Token(); err != io.EOF {
		return Route{}, errors.New("route: trailing JSON")
	}
	switch RouteKind(document.Kind) {
	case RouteUnix:
		if document.Target != "" || len(document.Argv) != 0 || document.Trust != nil {
			return Route{}, errors.New("unix route must not carry ssh fields")
		}
		return newUnixRoute(document.Path)
	case RouteSSHStdio, RouteSSHQUIC:
		trust, err := parseTrust(document.Trust)
		if err != nil {
			return Route{}, err
		}
		return newSSHRoute(RouteKind(document.Kind), document.Target, document.Argv, trust)
	case "":
		return Route{}, errors.New("route object requires a kind")
	default:
		return Route{}, fmt.Errorf("route kind %q is invalid", document.Kind)
	}
}

func parseTrust(document *trustDocument) (TrustInputs, error) {
	if document == nil {
		return TrustInputs{}, nil
	}
	trust := TrustInputs{knownHostsFile: document.KnownHostsFile, set: true}
	if document.ConnectTimeout != "" {
		timeout, err := time.ParseDuration(document.ConnectTimeout)
		if err != nil {
			return TrustInputs{}, fmt.Errorf("trust connectTimeout %q is not a duration", document.ConnectTimeout)
		}
		if timeout <= 0 {
			return TrustInputs{}, errors.New("trust connectTimeout must be positive")
		}
		trust.connectTimeout = timeout
	}
	return trust, nil
}

// RouteFromSpec materializes canonical durable routing authority. Runtime SSH
// trust is OpenSSH-owned; no startup configuration lookup participates.
func RouteFromSpec(spec ports.BrokerRouteSpec) (Route, error) {
	if err := spec.Validate(); err != nil {
		return Route{}, err
	}
	if spec.Kind == ports.BrokerRouteUnix {
		return newUnixRoute(spec.Path)
	}
	return newSSHRoute(RouteKind(spec.Kind), spec.Target, spec.Argv, TrustInputs{})
}

func newUnixRoute(path string) (Route, error) {
	route := Route{kind: RouteUnix, path: path, local: true}
	if err := route.Validate(); err != nil {
		return Route{}, err
	}
	route.address = route.digest()
	return route, nil
}

func newSSHRoute(kind RouteKind, target string, argv []string, trust TrustInputs) (Route, error) {
	route := Route{
		kind:   kind,
		target: target,
		argv:   append([]string(nil), argv...),
		trust:  trust,
	}
	if err := route.Validate(); err != nil {
		return Route{}, err
	}
	route.address = route.digest()
	return route, nil
}

// digest computes the bounded opaque pool address. The canonical encoding
// separates every field with a NUL and carries the kind first, so distinct
// routes always produce distinct digests.
func (r Route) digest() string {
	var builder strings.Builder
	builder.WriteString(string(r.kind))
	builder.WriteByte(0)
	builder.WriteString(r.path)
	builder.WriteByte(0)
	builder.WriteString(r.target)
	builder.WriteByte(0)
	for _, word := range r.argv {
		builder.WriteString(word)
		builder.WriteByte(0)
	}
	builder.WriteByte(0)
	builder.WriteString(r.trust.knownHostsFile)
	builder.WriteByte(0)
	fmt.Fprintf(&builder, "%d", r.trust.connectTimeout)
	if r.trust.set {
		builder.WriteByte(1)
	}
	sum := sha256.Sum256([]byte(builder.String()))
	return "offline-route-" + hex.EncodeToString(sum[:16])
}
