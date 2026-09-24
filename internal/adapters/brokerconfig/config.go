// Package brokerconfig owns the strict, isolated offline broker sandbox
// configuration.
//
// It is deliberately not production composition: nothing in internal/app or
// main consults it for ordinary daemon operation. It resolves one caller-
// supplied offline root into sandbox-owned runtime, state, and log paths,
// validates that the root is a private, absolute, cleaned, symlink-free
// directory that does not overlap production runtime or state, and strictly
// parses the bounded config.json marker that provisions immutable endpoint
// registrations, each carrying a stable daemon identity, an exact connection
// policy, and one bounded route (a private Unix mux socket, an SSH stdio mux
// helper, or an SSH-authenticated QUIC bootstrap).
//
// The parsed configuration is immutable and is the only source of endpoint
// addresses, identities, and policies. A client request can name a configured
// endpoint and must carry that endpoint's exact registration and compatible
// policy; it can never supply an address, identity, secret, command, or policy
// of its own, and an unknown or stale request is refused without dialing. A
// local request carries no endpoint or registration and is instead fenced
// against the optional broker-owned local binding (identity, policy, and Unix
// daemonmux route), so it too can never supply authority of its own. The
// route's pool address is an opaque digest, so no route detail is exposed to the
// pooling layer.
//
// Because the pool keys one physical transport by the exact authenticated
// identity plus policy pair and ignores that opaque address, two registrations
// that share the pair must agree on the one route the pool dials: distinct
// endpoints may alias an identical route, but a second route for the same pair
// is refused rather than silently stranded.
package brokerconfig

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// Marker is the exact config.json marker that identifies a broker offline
// sandbox configuration. A file without it is never treated as broker config,
// so an accidental --offline-root pointing at unrelated JSON fails closed.
const Marker = "vev.broker.offline/v1"

// ConfigFileName is the fixed config file name inside one offline root.
const ConfigFileName = "config.json"

// MaxConfigBytes bounds the config file before any decoding.
const MaxConfigBytes = 1 << 20

// MaxIdleGrace bounds a provisioned idle grace so an accidental enormous value
// cannot make an offline broker effectively immortal.
const MaxIdleGrace = 24 * time.Hour

// Warm transport retention (the broker-owned replacement for the removed
// client remote.attachment-cache keys). DefaultWarmTransports matches that
// cache's default capacity; MaxWarmTransports matches the pool's physical
// bound. The default idle timeout is off (no age bound), also matching it;
// a provisioned timeout is bounded by MaxIdleGrace.
const (
	DefaultWarmTransports = 8
	MaxWarmTransports     = 64
)

// Sandbox directory names derived from one offline root. SpawnDirName is the
// private directory under Runtime that holds the descriptor-backed
// spawn-election lock, deliberately separate from the broker lifetime lock in
// Runtime itself so electing a spawner can never collide with lifetime
// ownership.
const (
	RuntimeDirName = "runtime"
	StateDirName   = "state"
	LogDirName     = "log"
	SpawnDirName   = "spawn"
)

var (
	// errUnknownEndpoint reports a request for an endpoint the immutable
	// configuration never provisioned.
	errUnknownEndpoint = errors.New("brokerconfig: unknown broker endpoint")
	// errStaleRegistration reports a request whose registration no longer
	// matches the provisioned immutable registration.
	errStaleRegistration = errors.New("brokerconfig: stale broker registration")
	// errNoLocalRoute reports a local request when the configuration provisions
	// no broker-owned local binding, so the broker owns no local route.
	errNoLocalRoute = errors.New("brokerconfig: offline broker has no local route")
	// errLocalPolicyConflict reports a local request whose policy is not exactly
	// compatible with the provisioned local policy. The provisioned policy stays
	// authoritative: the request can never widen, narrow, or replace it.
	errLocalPolicyConflict = errors.New("brokerconfig: local request policy conflicts with the provisioned local policy")
)

// Layout names the sandbox-owned paths derived from one offline root. Every
// path is beneath Root; no production runtime or state path is ever derived. It
// also carries the reserved production directories it was validated against, so
// Load can fence every provisioned route against them too.
type Layout struct {
	Root    string
	Runtime string
	State   string
	Log     string
	// Spawn is the private spawn-election directory beneath Runtime.
	Spawn string

	// reserved holds the cleaned absolute production runtime and state paths
	// supplied to ResolveLayout. Load refuses a provisioned route that overlaps
	// them; it is unexported so only ResolveLayout can set it.
	reserved []string
}

// VerifyCreated re-walks every sandbox-owned path and refuses any existing
// component that is a symlink.
//
// ResolveLayout validates the root textually and stops at the first component
// that does not exist yet, so the composing caller calls this once
// pkg/safedir.EnsurePrivate has created the runtime, state, and log
// directories. It closes the gap between that validation and the first use of
// the created paths: a component swapped for a symlink in the window fails
// closed instead of being followed.
func (l Layout) VerifyCreated() error {
	for _, path := range []string{l.Root, l.Runtime, l.State, l.Log, l.Spawn} {
		if err := rejectSymlinkedComponents(path); err != nil {
			return err
		}
	}
	return nil
}

// rejectSymlinkedComponents refuses a path any of whose existing components is
// a symlink not owned by root. A component that does not exist yet ends the
// walk: the caller creates the remaining private directories itself.
//
// Root-owned symlinks are system layout, such as macOS /var -> private/var and
// /tmp -> private/tmp; no other user can plant or retarget them.
func rejectSymlinkedComponents(abs string) error {
	volume := filepath.VolumeName(abs)
	rest := strings.TrimPrefix(abs, volume)
	rest = strings.TrimPrefix(rest, string(filepath.Separator))
	current := volume + string(filepath.Separator)
	if rest == "" {
		return nil
	}
	for _, part := range strings.Split(rest, string(filepath.Separator)) {
		if part == "" {
			continue
		}
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return fmt.Errorf("brokerconfig: inspect %q: %w", current, err)
		}
		if info.Mode()&os.ModeSymlink != 0 && !ownedByRoot(info) {
			return fmt.Errorf("brokerconfig: %q is a symlink", current)
		}
	}
	return nil
}

func ownedByRoot(info fs.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == 0
}

// overlaps reports whether two absolute cleaned paths are equal or one contains
// the other.
func overlaps(a, b string) bool {
	if a == b {
		return true
	}
	sep := string(filepath.Separator)
	return strings.HasPrefix(a, b+sep) || strings.HasPrefix(b, a+sep)
}

// Registration is one immutable, provisioned endpoint: the exact daemon
// registration a request must carry, the stable authenticated daemon identity,
// the exact connection policy, and the bounded route the connector dials.
type Registration struct {
	Registration domain.RemoteRegistration
	Identity     ports.BrokerDaemonIdentity
	Policy       ports.BrokerPolicy
	Route        Route
}

// LocalBinding is the broker-owned binding for the broker's own machine
// daemon. Unlike a remote Registration it carries no
// registration: the local daemon is not a configured host, so a local stream
// request is fenced only against this binding's identity, policy, and route.
// The identity and policy are authoritative here and are the exact source of
// the resolved endpoint and of the published local observation's configured
// authority, so a local request can never select an identity or policy of its
// own.
type LocalBinding struct {
	// Identity is the stable authenticated daemon identity the local
	// daemonmux carriage must present during the physical preamble.
	Identity ports.BrokerDaemonIdentity
	// DisplayOrigin is the presentation hint published for the local
	// observation. It is sanitized at load time and is never routing authority.
	DisplayOrigin string
	// Policy is the exact connection policy the local carriage negotiates.
	Policy ports.BrokerPolicy
	// Route is the broker-owned local daemonmux carriage. It is always a Unix
	// route this process dials directly (Route.IsLocal() is true). The broker
	// dials it itself, so it is never a remote-side mux helper route
	// (Config.LocalMuxRoute).
	Route Route
}

// pooledIdentity is the exact (authenticated identity, policy) pair the broker
// pool keys one physical transport by the exact identity and policy pair. The pool deliberately ignores the opaque
// route address, so two registrations that share a pair share one pooled
// transport and must therefore agree on the single route that transport dials.
type pooledIdentity struct {
	identity ports.BrokerDaemonIdentity
	policy   ports.BrokerPolicy
}

// Config is the validated, immutable offline broker configuration.
type Config struct {
	byEndpoint map[string]Registration
	byAddress  map[string]Route
	unixRoutes []Route
	order      []string
	// local is the optional broker-owned local binding. It is nil when the
	// configuration provisions no local route, in which case a local request is
	// refused and no local observation is produced.
	local *LocalBinding
	// idleGrace is the provisioned effective idle grace; hasIdleGrace records
	// whether the file provisioned one, so a caller can distinguish "no
	// provisioning, use the built-in default" from "provisioned".
	idleGrace    time.Duration
	hasIdleGrace bool
	// warmTransports and warmIdleTimeout bound warm physical transport reuse;
	// see WarmTransports and WarmIdleTimeout.
	warmTransports  int
	warmIdleTimeout time.Duration
}

// LoadPath parses the broker configuration at an explicit application-owned
// path while retaining the route and reserved-path validation from layout.
func LoadPath(layout Layout, path string) (*Config, error) {
	return loadPath(layout, path, nil)
}

// LoadProduction reads broker.json while deriving local daemon authority from
// daemon-owned state. The production file must not carry a local identity.
func LoadProduction(layout Layout, path string, identity ports.BrokerDaemonIdentity, policy ports.BrokerPolicy, socketPath string) (*Config, error) {
	// An absent identity is valid before the local daemon's first start. A
	// present identity remains strict and becomes the expected handshake fence.
	if identity != "" {
		if err := identity.Validate(); err != nil {
			return nil, err
		}
	}
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	route, err := newUnixRoute(socketPath)
	if err != nil {
		return nil, err
	}
	local := &LocalBinding{Identity: identity, DisplayOrigin: LocalDisplayOrigin, Policy: policy, Route: route}
	return loadPath(layout, path, local)
}

func loadPath(layout Layout, path string, authoritativeLocal *LocalBinding) (*Config, error) {
	raw, err := readConfig(path)
	if err != nil {
		return nil, err
	}
	var document configDocument
	if err := decodeStrict(raw, &document); err != nil {
		return nil, fmt.Errorf("brokerconfig: %s: %w", ConfigFileName, err)
	}
	if document.Marker != Marker {
		return nil, fmt.Errorf("brokerconfig: %s: missing or invalid marker", ConfigFileName)
	}
	config := &Config{
		byEndpoint: make(map[string]Registration, len(document.Registrations)),
		byAddress:  make(map[string]Route, len(document.Registrations)),
		order:      make([]string, 0, len(document.Registrations)),
	}
	if document.IdleGrace != "" {
		grace, err := time.ParseDuration(document.IdleGrace)
		if err != nil {
			return nil, fmt.Errorf("brokerconfig: %s: idleGrace %q is not a duration", ConfigFileName, document.IdleGrace)
		}
		if grace <= 0 || grace > MaxIdleGrace {
			return nil, fmt.Errorf("brokerconfig: %s: idleGrace must be positive and at most %s", ConfigFileName, MaxIdleGrace)
		}
		config.idleGrace, config.hasIdleGrace = grace, true
	}
	if config.warmTransports, config.warmIdleTimeout, err = document.warm(); err != nil {
		return nil, err
	}
	pooledRoutes := make(map[pooledIdentity]string, len(document.Registrations)+1)
	for i, entry := range document.Registrations {
		registration, err := entry.validate()
		if err != nil {
			return nil, fmt.Errorf("brokerconfig: registration %d: %w", i, err)
		}
		if registration.Route.IsLocal() {
			if err := rejectRouteOverlap(registration.Route.Path(), layout.reserved); err != nil {
				return nil, fmt.Errorf("brokerconfig: registration %d: %w", i, err)
			}
		}
		if _, duplicate := config.byEndpoint[registration.Registration.Endpoint]; duplicate {
			return nil, fmt.Errorf("brokerconfig: registration %d: duplicate endpoint %q", i, registration.Registration.Endpoint)
		}
		// The pool keys one physical transport by (identity, policy) and ignores
		// the route address, so a second address for the same pair would be
		// dialed for neither or both depending on scheduling. Refuse it and allow
		// an alias only when the routes are identical.
		key := pooledIdentity{identity: registration.Identity, policy: registration.Policy}
		address := registration.Route.Address()
		if existing, present := pooledRoutes[key]; present && existing != address {
			return nil, fmt.Errorf("brokerconfig: registration %d: identity %q and policy already provisioned on a different route", i, registration.Identity)
		}
		pooledRoutes[key] = address
		config.byEndpoint[registration.Registration.Endpoint] = registration
		config.byAddress[address] = registration.Route
		if registration.Route.IsLocal() {
			config.unixRoutes = appendUnixRouteOnce(config.unixRoutes, registration.Route)
		}
		config.order = append(config.order, registration.Registration.Endpoint)
	}
	if authoritativeLocal != nil && document.Local != nil {
		return nil, fmt.Errorf("brokerconfig: %s: production local authority must not be configured", ConfigFileName)
	}
	local := authoritativeLocal
	if local == nil {
		local, err = parseLocal(document.Local)
		if err != nil {
			return nil, fmt.Errorf("brokerconfig: %s: local: %w", ConfigFileName, err)
		}
	}
	if local != nil {
		if err := rejectRouteOverlap(local.Route.Path(), layout.reserved); err != nil {
			return nil, fmt.Errorf("brokerconfig: %s: local: %w", ConfigFileName, err)
		}
		// The pool keys one physical transport by (identity, policy) and ignores
		// the route address, so a local binding that shares a remote (or another
		// local) pair must agree on the one route that transport dials. Refuse a
		// second route for the same pair, exactly as the registration loop does.
		key := pooledIdentity{identity: local.Identity, policy: local.Policy}
		address := local.Route.Address()
		if existing, present := pooledRoutes[key]; present && existing != address {
			return nil, fmt.Errorf("brokerconfig: %s: local identity %q and policy already provisioned on a different route", ConfigFileName, local.Identity)
		}
		pooledRoutes[key] = address
		config.byAddress[address] = local.Route
		config.local = local
	}
	return config, nil
}

// parseLocal strictly parses the optional local binding. A nil document
// provisions no local route. The identity and policy are required and
// validated; the route must be a local Unix daemonmux carriage, because the
// local daemon is reached in-process over a private Unix socket and never
// through an SSH or QUIC carriage. The display origin is sanitized and
// defaults to the broker-owned value "local", so an absent or wholly
// undisplayable origin never yields an empty publication.
func parseLocal(document *localDocument) (*LocalBinding, error) {
	if document == nil {
		return nil, nil
	}
	identity := ports.BrokerDaemonIdentity(document.Identity)
	if err := identity.Validate(); err != nil {
		return nil, err
	}
	policy, err := document.Policy.validate()
	if err != nil {
		return nil, err
	}
	route, err := parseRoute(document.Route)
	if err != nil {
		return nil, err
	}
	if !route.IsLocal() {
		return nil, errors.New("local route must be a Unix daemonmux carriage")
	}
	origin := ports.SanitizeBrokerDisplayText(document.DisplayOrigin)
	if origin == "" {
		origin = LocalDisplayOrigin
	}
	if len(origin) > ports.BrokerMaxDisplayOriginBytes {
		return nil, fmt.Errorf("local display origin exceeds %d bytes", ports.BrokerMaxDisplayOriginBytes)
	}
	return &LocalBinding{Identity: identity, DisplayOrigin: origin, Policy: policy, Route: route}, nil
}

// appendUnixRouteOnce adds a local Unix route unless an identical route (the
// same opaque address, hence the same bounded contents) is already present, so
// endpoint aliases of one route never make LocalMuxRoute report more than one
// provisioned route.
func appendUnixRouteOnce(routes []Route, route Route) []Route {
	for _, existing := range routes {
		if existing.Address() == route.Address() {
			return routes
		}
	}
	return append(routes, route)
}

// LocalDisplayOrigin is the broker-owned default presentation hint for the
// broker's own machine daemon when the configuration provisions none.
const LocalDisplayOrigin = "local"

// LocalBinding returns the provisioned broker-owned local binding and whether
// one exists. A nil configuration or one without a local object provisions
// nothing, and a local request is then refused.
func (c *Config) LocalBinding() (LocalBinding, bool) {
	if c == nil || c.local == nil {
		return LocalBinding{}, false
	}
	return *c.local, true
}

// Endpoints returns the configured endpoints in file order. The result is a
// copy; the configuration is immutable.
func (c *Config) Endpoints() []string {
	if c == nil {
		return nil
	}
	return append([]string(nil), c.order...)
}

// Registration returns the immutable configured authority for one endpoint.
func (c *Config) Registration(endpoint string) (Registration, bool) {
	if c == nil {
		return Registration{}, false
	}
	registration, ok := c.byEndpoint[endpoint]
	return registration, ok
}

// IdleGrace returns the provisioned effective idle grace and whether the
// configuration provisioned one. A nil configuration provisions nothing.
func (c *Config) IdleGrace() (time.Duration, bool) {
	if c == nil {
		return 0, false
	}
	return c.idleGrace, c.hasIdleGrace
}

// WarmTransports returns how many idle physical transports the broker keeps
// warm for reuse. Zero disables warm reuse. A nil configuration uses the
// default.
func (c *Config) WarmTransports() int {
	if c == nil {
		return DefaultWarmTransports
	}
	return c.warmTransports
}

// WarmIdleTimeout returns how long an idle physical transport stays warm.
// Zero means no age bound.
func (c *Config) WarmIdleTimeout() time.Duration {
	if c == nil {
		return 0
	}
	return c.warmIdleTimeout
}

// RouteByAddress resolves one opaque pool address back to the route it was
// derived from. The address is produced only by this configuration's own
// routes, so an unknown address is refused rather than dialed.
func (c *Config) RouteByAddress(address string) (Route, bool) {
	if c == nil {
		return Route{}, false
	}
	route, ok := c.byAddress[address]
	return route, ok
}

// Resolver returns the immutable endpoint resolver over this configuration.
func (c *Config) Resolver() *Resolver {
	if c == nil {
		return &Resolver{byEndpoint: map[string]Registration{}}
	}
	byEndpoint := make(map[string]Registration, len(c.byEndpoint))
	for endpoint, registration := range c.byEndpoint {
		registration.Route.argv = append([]string(nil), registration.Route.argv...)
		byEndpoint[endpoint] = registration
	}
	var local *LocalBinding
	if c.local != nil {
		copy := *c.local
		copy.Route.argv = append([]string(nil), copy.Route.argv...)
		local = &copy
	}
	return &Resolver{byEndpoint: byEndpoint, local: local}
}

// configDocument is the strict on-disk shape. Unknown fields are refused by the
// decoder, so the file cannot smuggle additional behavior.
type configDocument struct {
	Marker string `json:"marker"`
	// Local optionally provisions the broker-owned local binding. It is the
	// only source of the local identity, policy, and daemonmux route.
	Local *localDocument `json:"local,omitempty"`
	// IdleGrace optionally provisions the sandbox's effective idle grace as a Go
	// duration string (for example "90s"). It is the single deterministic source
	// for the idle grace, because a status probe cannot read a running broker's
	// effective grace through the broker IPC protocol.
	IdleGrace string `json:"idleGrace,omitempty"`
	// WarmTransports optionally bounds idle physical transports kept warm for
	// reuse (0 through MaxWarmTransports; 0 disables warm reuse).
	WarmTransports *int `json:"warmTransports,omitempty"`
	// WarmIdleTimeout optionally expires a warm transport: "off" (no age
	// bound, the default) or a positive Go duration up to MaxIdleGrace.
	WarmIdleTimeout string            `json:"warmIdleTimeout,omitempty"`
	Registrations   []registrationRaw `json:"registrations"`
}

func (d configDocument) warm() (int, time.Duration, error) {
	count := DefaultWarmTransports
	if d.WarmTransports != nil {
		count = *d.WarmTransports
		if count < 0 || count > MaxWarmTransports {
			return 0, 0, fmt.Errorf("brokerconfig: %s: warmTransports must be between 0 and %d", ConfigFileName, MaxWarmTransports)
		}
	}
	if d.WarmIdleTimeout == "" || d.WarmIdleTimeout == "off" {
		return count, 0, nil
	}
	timeout, err := time.ParseDuration(d.WarmIdleTimeout)
	if err != nil || timeout <= 0 || timeout > MaxIdleGrace {
		return 0, 0, fmt.Errorf("brokerconfig: %s: warmIdleTimeout %q must be off or a positive duration of at most %s", ConfigFileName, d.WarmIdleTimeout, MaxIdleGrace)
	}
	return count, timeout, nil
}

// localDocument is the strict on-disk shape of the broker-owned local binding.
// The route must be a local Unix daemonmux carriage, the identity and policy
// are required, and the display origin is optional (defaulting to "local").
type localDocument struct {
	Identity      string          `json:"identity"`
	DisplayOrigin string          `json:"displayOrigin,omitempty"`
	Route         json.RawMessage `json:"route"`
	Policy        policyRaw       `json:"policy"`
}

// registrationRaw is the raw JSON registration before semantic validation.
type registrationRaw struct {
	Endpoint    string          `json:"endpoint"`
	Incarnation string          `json:"incarnation"`
	Generation  uint64          `json:"generation"`
	Identity    string          `json:"identity"`
	Route       json.RawMessage `json:"route"`
	Policy      policyRaw       `json:"policy"`
}

// policyRaw is the raw JSON policy before semantic validation.
type policyRaw struct {
	ProtocolVersion      uint16 `json:"protocolVersion"`
	CatalogSchemaVersion uint16 `json:"catalogSchemaVersion"`
	EnvironmentPolicy    string `json:"environmentPolicy"`
	Transport            string `json:"transport"`
	Trust                string `json:"trust"`
	Launch               string `json:"launch"`
	Isolation            string `json:"isolation"`
}

func (r registrationRaw) validate() (Registration, error) {
	var registration domain.RemoteRegistration
	registration.Endpoint = r.Endpoint
	if err := domain.ValidateRemoteHostTarget(r.Endpoint); err != nil {
		return Registration{}, err
	}
	if len(r.Incarnation) != 32 {
		return Registration{}, errors.New("incarnation must be 32 hexadecimal characters")
	}
	decoded, err := hex.DecodeString(r.Incarnation)
	if err != nil {
		return Registration{}, fmt.Errorf("incarnation: %w", err)
	}
	copy(registration.Incarnation[:], decoded)
	registration.Generation = domain.RemoteGeneration(r.Generation)
	if err := registration.Validate(); err != nil {
		return Registration{}, err
	}
	identity := ports.BrokerDaemonIdentity(r.Identity)
	if err := identity.Validate(); err != nil {
		return Registration{}, err
	}
	policy, err := r.Policy.validate()
	if err != nil {
		return Registration{}, err
	}
	route, err := parseRoute(r.Route)
	if err != nil {
		return Registration{}, err
	}
	return Registration{Registration: registration, Identity: identity, Policy: policy, Route: route}, nil
}

func (p policyRaw) validate() (ports.BrokerPolicy, error) {
	var environment protocol.EnvironmentPolicy
	switch p.EnvironmentPolicy {
	case "client-owned":
		environment = protocol.EnvironmentPolicyClientOwned
	case "daemon-owned":
		environment = protocol.EnvironmentPolicyDaemonOwned
	default:
		return ports.BrokerPolicy{}, fmt.Errorf("policy environmentPolicy %q is invalid", p.EnvironmentPolicy)
	}
	policy := ports.BrokerPolicy{
		ProtocolVersion:      p.ProtocolVersion,
		CatalogSchemaVersion: p.CatalogSchemaVersion,
		EnvironmentPolicy:    environment,
		Transport:            p.Transport,
		Trust:                p.Trust,
		Launch:               p.Launch,
		Isolation:            p.Isolation,
	}
	if err := policy.Validate(); err != nil {
		return ports.BrokerPolicy{}, err
	}
	return policy, nil
}

// rejectRouteOverlap refuses a provisioned Unix mux route that equals, contains,
// or sits inside a reserved production runtime or state directory. A route is an
// address the broker connector dials, so a route overlapping production would
// let the isolated offline sandbox reach a production endpoint even though the
// sandbox root itself is outside production. Reserved entries are cleaned
// absolute paths; empty entries are ignored.
func rejectRouteOverlap(route string, reserved []string) error {
	for _, other := range reserved {
		if other == "" {
			continue
		}
		other = filepath.Clean(other)
		if overlaps(route, other) {
			return fmt.Errorf("route %q overlaps production path %q", route, other)
		}
	}
	return nil
}

// validateRoute refuses anything but an absolute, cleaned Unix path without
// symlinked components, so a provisioned route can never be reshaped by the
// request path or escape through a link.
func validateRoute(route string) error {
	if route == "" {
		return errors.New("route is empty")
	}
	if !filepath.IsAbs(route) {
		return fmt.Errorf("route %q is not absolute", route)
	}
	if filepath.Clean(route) != route {
		return fmt.Errorf("route %q is not clean", route)
	}
	return rejectSymlinkedComponents(route)
}

// readConfig reads one bounded regular owner-only config file without following
// a symlink.
func readConfig(path string) ([]byte, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		if errors.Is(err, syscall.ELOOP) {
			return nil, fmt.Errorf("brokerconfig: %s is a symlink", path)
		}
		return nil, fmt.Errorf("brokerconfig: open %s: %w", path, err)
	}
	file := os.NewFile(uintptr(fd), path)
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("brokerconfig: stat %s: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("brokerconfig: %s is not a regular file", path)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("brokerconfig: %s has permissions %04o, want owner-only", path, info.Mode().Perm())
	}
	if info.Size() > MaxConfigBytes {
		return nil, fmt.Errorf("brokerconfig: %s exceeds %d bytes", path, MaxConfigBytes)
	}
	raw, err := io.ReadAll(io.LimitReader(file, MaxConfigBytes+1))
	if err != nil {
		return nil, fmt.Errorf("brokerconfig: read %s: %w", path, err)
	}
	if len(raw) > MaxConfigBytes {
		return nil, fmt.Errorf("brokerconfig: %s exceeds %d bytes", path, MaxConfigBytes)
	}
	return raw, nil
}

// decodeStrict rejects duplicate keys, unknown fields, trailing JSON, invalid
// UTF-8, and excessive nesting, then decodes once. Byte bounds are applied
// before decoding.
func decodeStrict(raw []byte, dst any) error {
	if len(raw) > MaxConfigBytes || !utf8.Valid(raw) {
		return errors.New("invalid JSON size or encoding")
	}
	scan := json.NewDecoder(bytes.NewReader(raw))
	if err := scanJSON(scan, 0); err != nil {
		return err
	}
	if _, err := scan.Token(); err != io.EOF {
		return errors.New("trailing JSON")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	return decoder.Decode(dst)
}

// scanJSON walks one JSON value, rejecting duplicate keys and excessive depth.
func scanJSON(decoder *json.Decoder, depth int) error {
	if depth > 64 {
		return errors.New("JSON nesting limit")
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]bool)
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok || seen[name] {
				return errors.New("duplicate JSON key")
			}
			seen[name] = true
			if err := scanJSON(decoder, depth+1); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	case '[':
		for decoder.More() {
			if err := scanJSON(decoder, depth+1); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	default:
		return errors.New("unexpected JSON delimiter")
	}
}
