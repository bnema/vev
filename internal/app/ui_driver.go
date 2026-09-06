package app

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/bnema/vev/internal/adapters/clipboard"
	"github.com/bnema/vev/internal/adapters/clock"
	"github.com/bnema/vev/internal/adapters/ipc"
	remoteadapter "github.com/bnema/vev/internal/adapters/remote"
	"github.com/bnema/vev/internal/adapters/sshstdio"
	"github.com/bnema/vev/internal/adapters/term"
	"github.com/bnema/vev/internal/adapters/uidriver"
	"github.com/bnema/vev/internal/adapters/uiterm"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/logging"
	"github.com/bnema/vev/internal/platform"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/bnema/vev/internal/usecase/client"
	"github.com/bnema/vev/pkg/safedir"
)

const (
	uiDriverDefaultColumns          = 80
	uiDriverDefaultRows             = 24
	launchConfigVersion             = 1
	launchConfigMaxBytes            = 64 << 10
	launchConfigMaxDepth            = 8
	launchAllowedRemoteEndpointsEnv = "VEV_UI_DRIVER_ALLOWED_REMOTE_ENDPOINTS"
	uiDriverCleanupTimeout          = 10 * time.Second
)

type uiDriverOptions struct {
	socket       string
	session      string
	cols         int
	rows         int
	remote       string
	launchConfig string
}

type interactiveUIOptions struct {
	observe bool
	control bool
	socket  string
}

func parseInteractiveUIFlags(args []string, observe, control bool, socket string) (interactiveUIOptions, error) {
	options := interactiveUIOptions{observe: observe, control: control, socket: socket}
	for len(args) > 0 {
		switch args[0] {
		case "--ui-observe":
			options.observe = true
			args = args[1:]
		case "--ui-control":
			options.control = true
			args = args[1:]
		case "--ui-socket":
			if len(args) < 2 || args[1] == "" {
				return interactiveUIOptions{}, usagef("`--ui-socket` requires a path")
			}
			options.socket = args[1]
			args = args[2:]
		default:
			return interactiveUIOptions{}, usagef("unknown attach option %q", args[0])
		}
	}
	if options.socket != "" && !options.observe && !options.control {
		return interactiveUIOptions{}, usagef("`--ui-socket` requires `--ui-observe` or `--ui-control`")
	}
	if options.socket != "" && (!filepath.IsAbs(options.socket) || strings.IndexByte(options.socket, 0) >= 0) {
		return interactiveUIOptions{}, usagef("`--ui-socket` requires an absolute path")
	}
	return options, nil
}

func parseUIDriverArgs(args []string) (uiDriverOptions, error) {
	options := uiDriverOptions{cols: uiDriverDefaultColumns, rows: uiDriverDefaultRows}
	var socketSet, sessionSet, colsSet, rowsSet, remoteSet, launchSet bool
	for len(args) > 0 {
		name := args[0]
		if name == "--help" || name == "-h" {
			return uiDriverOptions{}, usagef("`ui-driver` options: --session NAME --cols N --rows N --remote ENDPOINT --launch-config PATH, or --socket PATH")
		}
		if !strings.HasPrefix(name, "--") {
			return uiDriverOptions{}, usagef("`ui-driver` does not accept positional arguments")
		}
		args = args[1:]
		value := func(flag string) (string, error) {
			if len(args) == 0 || args[0] == "" || strings.HasPrefix(args[0], "--") {
				return "", usagef("`ui-driver %s` requires a value", flag)
			}
			value := args[0]
			args = args[1:]
			return value, nil
		}
		switch name {
		case "--socket":
			if socketSet {
				return uiDriverOptions{}, usagef("duplicate `--socket`")
			}
			value, err := value("--socket")
			if err != nil {
				return uiDriverOptions{}, err
			}
			if !filepath.IsAbs(value) || strings.IndexByte(value, 0) >= 0 {
				return uiDriverOptions{}, usagef("`--socket` requires an absolute path")
			}
			options.socket, socketSet = value, true
		case "--session":
			if sessionSet {
				return uiDriverOptions{}, usagef("duplicate `--session`")
			}
			value, err := value("--session")
			if err != nil {
				return uiDriverOptions{}, err
			}
			if err := domain.ValidateSessionName(value); err != nil {
				return uiDriverOptions{}, err
			}
			options.session, sessionSet = value, true
		case "--cols", "--rows":
			value, err := value(name)
			if err != nil {
				return uiDriverOptions{}, err
			}
			number, err := strconv.Atoi(value)
			if err != nil || number <= 0 {
				return uiDriverOptions{}, usagef("`%s` requires a positive integer", name)
			}
			if name == "--cols" {
				if colsSet {
					return uiDriverOptions{}, usagef("duplicate `--cols`")
				}
				options.cols, colsSet = number, true
			} else {
				if rowsSet {
					return uiDriverOptions{}, usagef("duplicate `--rows`")
				}
				options.rows, rowsSet = number, true
			}
		case "--remote":
			if remoteSet {
				return uiDriverOptions{}, usagef("duplicate `--remote`")
			}
			value, err := value("--remote")
			if err != nil {
				return uiDriverOptions{}, err
			}
			if err := domain.ValidateRemoteHostTarget(value); err != nil {
				return uiDriverOptions{}, err
			}
			options.remote, remoteSet = value, true
		case "--launch-config":
			if launchSet {
				return uiDriverOptions{}, usagef("duplicate `--launch-config`")
			}
			value, err := value("--launch-config")
			if err != nil {
				return uiDriverOptions{}, err
			}
			if strings.IndexByte(value, 0) >= 0 {
				return uiDriverOptions{}, usagef("`--launch-config` contains a NUL")
			}
			options.launchConfig, launchSet = value, true
		default:
			return uiDriverOptions{}, usagef("unknown `ui-driver` option %q", name)
		}
	}
	if socketSet && (sessionSet || colsSet || rowsSet || remoteSet || launchSet) {
		return uiDriverOptions{}, usagef("`--socket` cannot be combined with headless options")
	}
	return options, nil
}

type launchEndpoint struct {
	binary string
	root   string
	env    map[string]string
}

type launchConfig struct {
	local   *launchEndpoint
	remotes map[string]launchEndpoint
}

func parseLaunchConfig(path string) (launchConfig, error) {
	var result launchConfig
	info, err := os.Lstat(path)
	if err != nil {
		return result, fmt.Errorf("vev: read launch config: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return result, errors.New("vev: launch config must be a regular file")
	}
	if info.Mode().Perm() != 0o600 {
		return result, errors.New("vev: launch config must have mode 0600")
	}
	if stat, ok := info.Sys().(*syscall.Stat_t); !ok || int(stat.Uid) != os.Getuid() {
		return result, errors.New("vev: launch config must be owned by the current user")
	}
	file, err := os.Open(path)
	if err != nil {
		return result, fmt.Errorf("vev: open launch config: %w", err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, launchConfigMaxBytes+1))
	if err != nil {
		return result, fmt.Errorf("vev: read launch config: %w", err)
	}
	if len(data) > launchConfigMaxBytes || !utf8.Valid(data) || !uniqueLaunchJSON(data) {
		return result, errors.New("vev: invalid launch config")
	}
	fields, ok := launchObject(data)
	if !ok {
		return result, errors.New("vev: launch config must be a JSON object")
	}
	version, ok := launchUint(fields["version"])
	if !ok || version != launchConfigVersion {
		return result, errors.New("vev: unsupported launch config version")
	}
	if !launchAllowedFields(fields, "version", "local", "remotes") {
		return result, errors.New("vev: launch config contains unknown fields")
	}
	if raw, exists := fields["local"]; exists {
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return result, errors.New("vev: launch config local endpoint cannot be null")
		}
		endpoint, err := parseLaunchEndpoint(raw, false)
		if err != nil {
			return result, err
		}
		result.local = &endpoint
	}
	result.remotes = make(map[string]launchEndpoint)
	if raw, exists := fields["remotes"]; exists {
		var entries []json.RawMessage
		if json.Unmarshal(raw, &entries) != nil || entries == nil {
			return result, errors.New("vev: launch config remotes must be an array")
		}
		for _, entry := range entries {
			object, ok := launchObject(entry)
			if !ok || !launchExactFields(object, "endpoint", "binary", "root", "env") {
				return result, errors.New("vev: invalid remote launch endpoint")
			}
			endpointName, ok := launchString(object["endpoint"])
			if !ok {
				return result, errors.New("vev: remote launch endpoint is required")
			}
			if err := domain.ValidateRemoteHostTarget(endpointName); err != nil {
				return result, fmt.Errorf("vev: invalid remote launch endpoint: %w", err)
			}
			endpoint, err := parseLaunchEndpoint(entry, true)
			if err != nil {
				return result, err
			}
			if _, duplicate := result.remotes[endpointName]; duplicate {
				return result, errors.New("vev: duplicate remote launch endpoint")
			}
			result.remotes[endpointName] = endpoint
		}
	}
	return result, nil
}

func parseLaunchEndpoint(data []byte, remote bool) (launchEndpoint, error) {
	fields, ok := launchObject(data)
	allowed := []string{"binary", "root", "env"}
	if remote {
		allowed = append(allowed, "endpoint")
	}
	if !ok || !launchExactFields(fields, allowed...) {
		return launchEndpoint{}, errors.New("vev: invalid launch endpoint")
	}
	binary, ok := launchString(fields["binary"])
	if !ok || !filepath.IsAbs(binary) || strings.IndexByte(binary, 0) >= 0 {
		return launchEndpoint{}, errors.New("vev: launch binary must be an absolute path")
	}
	root, ok := launchString(fields["root"])
	if !ok || !filepath.IsAbs(root) || strings.IndexByte(root, 0) >= 0 {
		return launchEndpoint{}, errors.New("vev: launch root must be an absolute path")
	}
	env, ok := launchEnvironment(fields["env"])
	if !ok {
		return launchEndpoint{}, errors.New("vev: invalid launch environment")
	}
	if !remote && env["VEV_ENV"] != "" {
		return launchEndpoint{}, errors.New("vev: launch environment cannot set reserved VEV_ENV")
	}
	return launchEndpoint{binary: binary, root: root, env: env}, nil
}

func launchEnvironment(data []byte) (map[string]string, bool) {
	object, ok := launchObject(data)
	if !ok {
		return nil, false
	}
	result := make(map[string]string, len(object))
	for name, raw := range object {
		if !validEnvironmentName(name) || isReservedLaunchEnvironment(name) {
			return nil, false
		}
		value, ok := launchString(raw)
		if !ok || strings.IndexByte(value, 0) >= 0 {
			return nil, false
		}
		result[name] = value
	}
	return result, true
}

func validEnvironmentName(name string) bool {
	if name == "" || !(name[0] == '_' || name[0] >= 'A' && name[0] <= 'Z' || name[0] >= 'a' && name[0] <= 'z') {
		return false
	}
	for i := 1; i < len(name); i++ {
		value := name[i]
		if !(value == '_' || value >= 'A' && value <= 'Z' || value >= 'a' && value <= 'z' || value >= '0' && value <= '9') {
			return false
		}
	}
	return true
}

func isReservedLaunchEnvironment(name string) bool {
	switch name {
	case "XDG_CONFIG_HOME", "XDG_STATE_HOME", "XDG_RUNTIME_DIR", "VEV_ENV", "VEV_ENV_ROOT", launchAllowedRemoteEndpointsEnv:
		return true
	default:
		return false
	}
}

func launchObject(data []byte) (map[string]json.RawMessage, bool) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(data, &fields) != nil || fields == nil {
		return nil, false
	}
	return fields, true
}

func launchString(data []byte) (string, bool) {
	var value string
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) || json.Unmarshal(data, &value) != nil || !utf8.ValidString(value) {
		return "", false
	}
	return value, true
}

func launchUint(data []byte) (uint64, bool) {
	var value uint64
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) || json.Unmarshal(data, &value) != nil {
		return 0, false
	}
	return value, true
}

func launchAllowedFields(fields map[string]json.RawMessage, names ...string) bool {
	allowed := make(map[string]bool, len(names))
	for _, name := range names {
		allowed[name] = true
	}
	for name := range fields {
		if !allowed[name] {
			return false
		}
	}
	return true
}

func launchExactFields(fields map[string]json.RawMessage, names ...string) bool {
	if !launchAllowedFields(fields, names...) {
		return false
	}
	for _, name := range names {
		if _, ok := fields[name]; !ok {
			return false
		}
	}
	return true
}

func uniqueLaunchJSON(data []byte) bool {
	decoder := json.NewDecoder(bytes.NewReader(data))
	var value func(int) bool
	value = func(depth int) bool {
		if depth > launchConfigMaxDepth {
			return false
		}
		token, err := decoder.Token()
		if err != nil {
			return false
		}
		delimiter, ok := token.(json.Delim)
		if !ok {
			return true
		}
		switch delimiter {
		case '{':
			seen := make(map[string]bool)
			for decoder.More() {
				token, err := decoder.Token()
				name, ok := token.(string)
				if err != nil || !ok || seen[name] {
					return false
				}
				seen[name] = true
				if !value(depth + 1) {
					return false
				}
			}
		case '[':
			for decoder.More() {
				if !value(depth + 1) {
					return false
				}
			}
		default:
			return false
		}
		_, err = decoder.Token()
		return err == nil
	}
	if !value(0) {
		return false
	}
	_, err := decoder.Token()
	return errors.Is(err, io.EOF)
}

func createLaunchRoot(root string) error {
	if !filepath.IsAbs(root) {
		return errors.New("vev: launch root must be absolute")
	}
	if _, err := os.Lstat(root); err == nil {
		return errors.New("vev: launch root already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("vev: inspect launch root: %w", err)
	}
	if err := os.Mkdir(root, 0o700); err != nil {
		return fmt.Errorf("vev: create launch root: %w", err)
	}
	if err := safedir.EnsurePrivate(root); err != nil {
		_ = os.Remove(root)
		return fmt.Errorf("vev: secure launch root: %w", err)
	}
	for _, child := range []string{"config", "state", "runtime", "tmp"} {
		if err := safedir.EnsurePrivate(filepath.Join(root, child)); err != nil {
			_ = os.RemoveAll(root)
			return fmt.Errorf("vev: create launch directory: %w", err)
		}
	}
	return nil
}

type environmentValue struct {
	value string
	set   bool
}

type environmentRestore struct {
	values map[string]environmentValue
}

func activateLaunchRoot(root string) (func(), error) {
	values := map[string]string{
		"XDG_CONFIG_HOME": filepath.Join(root, "config"),
		"XDG_STATE_HOME":  filepath.Join(root, "state"),
		"XDG_RUNTIME_DIR": filepath.Join(root, "runtime"),
		"VEV_ENV_ROOT":    root,
	}
	keys := []string{"XDG_CONFIG_HOME", "XDG_STATE_HOME", "XDG_RUNTIME_DIR", "VEV_ENV", "VEV_ENV_ROOT"}
	restore := environmentRestore{values: make(map[string]environmentValue, len(keys))}
	for _, key := range keys {
		value, set := os.LookupEnv(key)
		restore.values[key] = environmentValue{value: value, set: set}
	}
	rollback := func() {
		for key, previous := range restore.values {
			if previous.set {
				_ = os.Setenv(key, previous.value)
			} else {
				_ = os.Unsetenv(key)
			}
		}
	}
	for _, key := range keys {
		var err error
		if key == "VEV_ENV" {
			err = os.Unsetenv(key)
		} else {
			err = os.Setenv(key, values[key])
		}
		if err != nil {
			rollback()
			return nil, fmt.Errorf("vev: activate launch root: %w", err)
		}
	}
	return rollback, nil
}

func launchEnvironmentSlice(endpoint launchEndpoint) []string {
	return launchEnvironmentSliceForConfig(endpoint, nil)
}

func launchEnvironmentSliceForConfig(endpoint launchEndpoint, config *launchConfig) []string {
	values := make(map[string]string, len(endpoint.env)+6)
	for name, value := range endpoint.env {
		values[name] = value
	}
	values["XDG_CONFIG_HOME"] = filepath.Join(endpoint.root, "config")
	values["XDG_STATE_HOME"] = filepath.Join(endpoint.root, "state")
	values["XDG_RUNTIME_DIR"] = filepath.Join(endpoint.root, "runtime")
	values["VEV_ENV_ROOT"] = endpoint.root
	if config != nil {
		values[launchAllowedRemoteEndpointsEnv] = strings.Join(launchConfigRemoteTargets(config), "\n")
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]string, 0, len(keys))
	for _, key := range keys {
		result = append(result, key+"="+values[key])
	}
	return result
}

func launchConfigRemoteTargets(config *launchConfig) []string {
	if config == nil || len(config.remotes) == 0 {
		return nil
	}
	targets := make([]string, 0, len(config.remotes))
	for target := range config.remotes {
		targets = append(targets, target)
	}
	sort.Strings(targets)
	return targets
}

func clientTerminal(deps runAttachDeps) ports.Terminal {
	if deps.terminal != nil {
		if terminal := deps.terminal(); terminal != nil {
			return terminal
		}
	}
	return term.New()
}

func clientClock(deps runAttachDeps) ports.Clock {
	if deps.clock != nil {
		if value := deps.clock(); value != nil {
			return value
		}
	}
	return clock.New()
}

func clientEnvironment(factory func(string) []string, target string) []string {
	if factory == nil {
		return nil
	}
	return append([]string(nil), factory(target)...)
}

func runUIDriver(ctx context.Context, options uiDriverOptions) error {
	if options.socket != "" {
		return uidriver.Bridge(ctx, options.socket, os.Stdin, os.Stdout)
	}
	return runHeadlessUIDriver(ctx, options)
}

func runHeadlessUIDriver(ctx context.Context, options uiDriverOptions) (retErr error) {
	var config *launchConfig
	if options.launchConfig != "" {
		parsed, err := parseLaunchConfig(options.launchConfig)
		if err != nil {
			return err
		}
		config = &parsed
		if options.remote == "" && parsed.local == nil {
			return errors.New("vev: launch config requires a local endpoint for local headless attach")
		}
		if options.remote != "" {
			if _, ok := parsed.remotes[options.remote]; !ok {
				return &ports.UIError{Code: ports.UIErrEndpointNotConfigured}
			}
		}
	}
	remoteLaunches, err := newConfiguredRemoteLaunches(config)
	if err != nil {
		return err
	}
	var rootRestore func()
	var ownedRoot string
	removeRootOnReturn := true
	if config != nil && options.remote == "" {
		if err := createLaunchRoot(config.local.root); err != nil {
			return err
		}
		ownedRoot = config.local.root
		rootRestore, retErr = activateLaunchRoot(ownedRoot)
		if retErr != nil {
			_ = os.RemoveAll(ownedRoot)
			return retErr
		}
		defer func() {
			rootRestore()
			if !removeRootOnReturn {
				return
			}
			if err := removeOwnedLaunchRoot(ownedRoot); err != nil && retErr == nil {
				retErr = err
			}
		}()
	}

	log, logCloser, err := configureLogging(logging.Client, false)
	if err != nil {
		return err
	}
	defer func() { _ = logCloser.Close() }()
	clk := clock.New()
	terminal, err := uiterm.New(ctx, domain.Geometry{Size: domain.Size{Cols: options.cols, Rows: options.rows}}, "")
	if err != nil {
		return fmt.Errorf("vev: create headless terminal: %w", err)
	}
	defer terminal.Close()
	ui := client.NewUI(terminal, clk)

	localDir := ipc.SocketDir()
	deps := runAttachDeps{
		localDialer: func() wire.Dialer {
			if config != nil && config.local != nil {
				return localDaemonDialer{dir: localDir, executable: config.local.binary, environment: launchEnvironmentSliceForConfig(*config.local, config)}
			}
			return localDaemonDialer{dir: localDir}
		},
		remoteDialerFactory:     remoteLaunches.dialerFactory(),
		selectedRemoteTransport: os.Getenv(envRemoteTransport),
		runClient:               runClientWithDeps,
		createDetached:          createDetachedLocalSession,
		clipboard:               nil,
		ui:                      ui,
		terminal:                func() ports.Terminal { return terminal },
		clock:                   func() ports.Clock { return clk },
		disableCapabilityProbe:  true,
		localEnvironment:        launchEnvironmentForConfig(config),
		remoteEnvironment:       remoteEnvironmentForConfig(config),
		stateDir:                platform.StateDir,
	}
	if options.remote == "" {
		// A headless named session creates a new local session; an omitted name
		// retains the ordinary ephemeral attach intent.
		intent := uint8(protocol.IntentEphemeral)
		if options.session != "" {
			intent = protocol.IntentNew
		}
		runErr := runHeadlessClient(ctx, intent, options.session, "", log, deps, ui, terminal)
		cleanupErr := cleanupHeadlessEndpoint(ownedRoot, remoteLaunches)
		if cleanupErr != nil && ownedRoot != "" {
			removeRootOnReturn = false
		}
		return errors.Join(runErr, cleanupErr)
	}
	intent := uint8(protocol.IntentEphemeral)
	if options.session != "" {
		intent = protocol.IntentAttach
	}
	runErr := runHeadlessClient(ctx, intent, options.session, options.remote, log, deps, ui, terminal)
	cleanupErr := cleanupHeadlessEndpoint(ownedRoot, remoteLaunches)
	if cleanupErr != nil && ownedRoot != "" {
		removeRootOnReturn = false
	}
	return errors.Join(runErr, cleanupErr)
}

func launchEnvironmentForConfig(config *launchConfig) []string {
	if config == nil || config.local == nil {
		return nil
	}
	return launchEnvironmentSliceForConfig(*config.local, config)
}

func remoteEnvironmentForConfig(config *launchConfig) func(string) []string {
	if config == nil {
		return nil
	}
	return func(target string) []string {
		endpoint, ok := config.remotes[target]
		if !ok {
			return nil
		}
		return launchEnvironmentSliceForConfig(endpoint, config)
	}
}

type configuredRemoteLaunches struct {
	config    *launchConfig
	factory   remoteadapter.DialerFactory
	mu        sync.Mutex
	tokens    map[string]string
	attempted map[string]struct{}
}

func newConfiguredRemoteLaunches(config *launchConfig) (*configuredRemoteLaunches, error) {
	if config == nil {
		return nil, nil
	}
	launches := &configuredRemoteLaunches{config: config, factory: remoteadapter.NewDialerFactory(), tokens: make(map[string]string, len(config.remotes)), attempted: make(map[string]struct{})}
	for target := range config.remotes {
		token, err := newLaunchOwnerToken()
		if err != nil {
			return nil, fmt.Errorf("vev: create remote launch owner: %w", err)
		}
		launches.tokens[target] = token
	}
	return launches, nil
}

func newLaunchOwnerToken() (string, error) {
	value := make([]byte, 18)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}

func (l *configuredRemoteLaunches) dialerFactory() remoteDialerForTarget {
	if l == nil {
		return defaultRemoteDialerFactory()
	}
	return func(target, session string, mode remoteadapter.TransportMode, log *slog.Logger) (wire.Dialer, error) {
		endpoint, ok := l.config.remotes[target]
		if !ok {
			return nil, &ports.UIError{Code: ports.UIErrEndpointNotConfigured}
		}
		l.mu.Lock()
		l.attempted[target] = struct{}{}
		token := l.tokens[target]
		l.mu.Unlock()
		launch := &remoteadapter.EndpointLaunch{Binary: endpoint.binary, Root: endpoint.root, OwnerToken: token, Environment: launchEnvironmentSliceForConfig(endpoint, l.config)}
		return l.factory.DialerForRemoteWithLaunch(target, session, mode, log, launch)
	}
}

func (l *configuredRemoteLaunches) cleanup(ctx context.Context) error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	targets := make([]string, 0, len(l.attempted))
	for target := range l.attempted {
		targets = append(targets, target)
	}
	l.mu.Unlock()
	sort.Strings(targets)
	var cleanupErr error
	for _, target := range targets {
		endpoint := l.config.remotes[target]
		spec := sshstdio.BuildCommandForRemoteCleanup(target, endpoint.root, l.tokens[target], endpoint.binary, launchEnvironmentSliceForConfig(endpoint, l.config), uiRemoteCleanupCommand)
		if err := sshstdio.RunRemoteCommand(ctx, spec); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("vev: clean remote launch endpoint: %w", err))
		}
	}
	return cleanupErr
}

func configuredRemoteDialerFactory(config *launchConfig) remoteDialerForTarget {
	launches, err := newConfiguredRemoteLaunches(config)
	if err != nil {
		return func(string, string, remoteadapter.TransportMode, *slog.Logger) (wire.Dialer, error) { return nil, err }
	}
	return launches.dialerFactory()
}

// cleanupHeadlessEndpoint is the explicit disposable-endpoint teardown. It
// runs only after the driver stream has detached, never from the JSONL server
// or the shared client runner.
func cleanupHeadlessEndpoint(ownedRoot string, remote *configuredRemoteLaunches) error {
	if ownedRoot == "" && remote == nil {
		return nil
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), uiDriverCleanupTimeout)
	defer cancel()
	var cleanupErr error
	if ownedRoot != "" {
		stopErr := requestDaemonStop(cleanupCtx)
		if stopErr != nil && !errors.Is(stopErr, errDaemonNotRunning) {
			cleanupErr = errors.Join(cleanupErr, stopErr)
		}
	}
	if remote != nil {
		cleanupErr = errors.Join(cleanupErr, remote.cleanup(cleanupCtx))
	}
	return cleanupErr
}

func runHeadlessClient(ctx context.Context, intent uint8, name, remoteTarget string, log *slog.Logger, deps runAttachDeps, ui *client.UI, terminal *uiterm.Terminal) error {
	return runHeadlessClientWithStream(ctx, intent, name, remoteTarget, log, deps, ui, terminal, &stdioStream{reader: os.Stdin, writer: os.Stdout})
}

func runHeadlessClientWithStream(ctx context.Context, intent uint8, name, remoteTarget string, log *slog.Logger, deps runAttachDeps, ui *client.UI, terminal *uiterm.Terminal, stream io.ReadWriteCloser) (retErr error) {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	runnerDone := make(chan error, 1)
	go func() { runnerDone <- runAttachWithDeps(runCtx, intent, name, remoteTarget, "", log, deps) }()

	snapshot, err := waitForHeadlessPublication(runCtx, ui)
	if err != nil {
		cancel()
		runErr := <-runnerDone
		return errors.Join(err, ignoreContextCancellation(runErr))
	}
	ready := uidriver.Ready{Attachment: ui.Handle(), Generation: snapshot.Context.Generation, Control: true, Status: snapshot.Context.Status}
	if ready.Generation == 0 {
		ready.Generation = 1
	}
	if ready.Status == "" {
		ready.Status = ports.UIStatusAttached
	}
	server := uidriver.New(ui, deps.clock())
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(runCtx, stream, ready) }()

	select {
	case serveErr := <-serveDone:
		cancel()
		runErr := <-runnerDone
		if serveErr != nil {
			retErr = errors.Join(serveErr, ignoreContextCancellation(runErr))
		} else {
			retErr = ignoreContextCancellation(runErr)
		}
	case runErr := <-runnerDone:
		cancel()
		serveErr := <-serveDone
		retErr = errors.Join(runErr, ignoreContextCancellation(serveErr))
	}
	return retErr
}

func waitForHeadlessPublication(ctx context.Context, ui *client.UI) (ports.UISnapshot, error) {
	readyCtx, cancel := context.WithTimeout(ctx, protocol.HandshakeTimeout)
	defer cancel()
	snapshot, err := ui.WaitForSnapshot(readyCtx, func(snapshot ports.UISnapshot) bool {
		return snapshot.Context.Generation != 0 && snapshot.Context.Status == ports.UIStatusAttached && snapshot.Context.ViewPublication != 0
	})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			return ports.UISnapshot{}, errors.New("vev: headless attachment did not publish an initial view")
		}
		return ports.UISnapshot{}, err
	}
	return snapshot, nil
}

func removeOwnedLaunchRoot(root string) error {
	if root == "" {
		return nil
	}
	info, err := os.Lstat(root)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("vev: inspect launch root during cleanup: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return errors.New("vev: refusing to remove an unsafe launch root")
	}
	if stat, ok := info.Sys().(*syscall.Stat_t); !ok || int(stat.Uid) != os.Getuid() {
		return errors.New("vev: refusing to remove a launch root owned by another user")
	}
	if err := os.RemoveAll(root); err != nil {
		return fmt.Errorf("vev: remove launch root: %w", err)
	}
	return nil
}

func ignoreContextCancellation(err error) error {
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

type observedTerminal struct {
	ports.Terminal
	ports.UIOutputTransaction
}

type stdioStream struct {
	reader io.Reader
	writer io.Writer
	once   sync.Once
}

func (s *stdioStream) Read(data []byte) (int, error)  { return s.reader.Read(data) }
func (s *stdioStream) Write(data []byte) (int, error) { return s.writer.Write(data) }
func (s *stdioStream) Close() error {
	var closeErr error
	s.once.Do(func() {
		for _, value := range []io.Writer{s.writer} {
			if closer, ok := value.(io.Closer); ok {
				closeErr = closer.Close()
			}
		}
		if closer, ok := s.reader.(io.Closer); ok {
			if err := closer.Close(); closeErr == nil {
				closeErr = err
			}
		}
	})
	return closeErr
}

// runAttachWithOptions is the composition path for ordinary interactive
// clients. Observation is opt-in; the default path remains term.New().
func runAttachWithOptions(ctx context.Context, intent uint8, name, remoteTarget string, options interactiveUIOptions) error {
	if !options.observe && !options.control && options.socket == "" {
		return runAttach(ctx, intent, name, remoteTarget)
	}
	if options.control {
		options.observe = true
	}
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	log, logCloser, err := configureLogging(logging.Client, false)
	if err != nil {
		return err
	}
	defer func() { _ = logCloser.Close() }()
	clk := clock.New()
	physical := term.NewWithFilesAndObservation(os.Stdin, os.Stdout, nil)
	geometry, err := physical.Geometry()
	if err != nil {
		return fmt.Errorf("vev: reading terminal geometry: %w", err)
	}
	mirror, err := uiterm.NewMirror(ctx, geometry, "")
	if err != nil {
		return fmt.Errorf("vev: create UI mirror: %w", err)
	}
	defer mirror.Close()
	physical = term.NewWithFilesAndObservation(os.Stdin, os.Stdout, mirror)
	ui := client.NewUI(mirror, clk)
	var endpoint *uidriver.UnixEndpoint
	if options.observe {
		server := uidriver.New(ui, clk)
		socketPath := options.socket
		if socketPath == "" {
			socketPath = uidriver.DefaultSocketPath(ipc.SocketDir(), ui.Handle())
		}
		endpoint, err = uidriver.ListenUnix(socketPath, server, func() uidriver.Ready {
			ready := uidriver.Ready{Attachment: ui.Handle(), Generation: 1, Control: options.control, Status: ports.UIStatusReconnecting}
			if snapshot, snapshotErr := ui.Capture(ui.Handle()); snapshotErr == nil {
				ready.Generation = snapshot.Context.Generation
				ready.Status = snapshot.Context.Status
			}
			return ready
		})
		if err != nil {
			return err
		}
		fmt.Fprintln(os.Stderr, socketPath)
		defer endpoint.Close()
	}
	return runAttachWithDeps(ctx, intent, name, remoteTarget, "", log, runAttachDeps{
		localDialer:             func() wire.Dialer { return localDaemonDialer{dir: ipc.SocketDir()} },
		remoteDialerFactory:     newRemoteDialerFactoryWithRuntimeObserver(nil),
		selectedRemoteTransport: os.Getenv(envRemoteTransport),
		runClient:               runClientWithDeps,
		createDetached:          createDetachedLocalSession,
		clipboard:               clipboard.New(),
		runtimeObserver:         nil,
		ui:                      ui,
		terminal:                func() ports.Terminal { return observedTerminal{Terminal: physical, UIOutputTransaction: mirror} },
		clock:                   func() ports.Clock { return clk },
	})
}
