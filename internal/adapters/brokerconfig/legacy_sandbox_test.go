package brokerconfig

import (
	"errors"
	"fmt"
	"path/filepath"
)

// ResolveLayout validates one offline root and derives its sandbox paths.
//
// The root must be absolute and clean, must not be the filesystem root, must
// not contain a symlinked component, and must not equal, contain, or sit inside
// any reserved production directory. Reserved directories are compared cleaned
// and absolute; empty entries are ignored. The result is purely textual: no
// directory is created here, and the caller secures the paths with
// pkg/safedir.EnsurePrivate.
func ResolveLayout(rawRoot string, reserved []string) (Layout, error) {
	if rawRoot == "" {
		return Layout{}, errors.New("brokerconfig: offline root is empty")
	}
	if !filepath.IsAbs(rawRoot) {
		return Layout{}, fmt.Errorf("brokerconfig: offline root %q is not absolute", rawRoot)
	}
	if cleaned := filepath.Clean(rawRoot); cleaned != rawRoot {
		return Layout{}, fmt.Errorf("brokerconfig: offline root %q is not clean", rawRoot)
	}
	if rawRoot == string(filepath.Separator) {
		return Layout{}, errors.New("brokerconfig: offline root must not be the filesystem root")
	}
	if err := rejectSymlinkedComponents(rawRoot); err != nil {
		return Layout{}, err
	}
	for _, other := range reserved {
		if other == "" {
			continue
		}
		other = filepath.Clean(other)
		if !filepath.IsAbs(other) {
			return Layout{}, fmt.Errorf("brokerconfig: reserved directory %q is not absolute", other)
		}
		if overlaps(rawRoot, other) {
			return Layout{}, fmt.Errorf("brokerconfig: offline root %q overlaps production path %q", rawRoot, other)
		}
	}
	runtime := filepath.Join(rawRoot, RuntimeDirName)
	return Layout{
		Root:     rawRoot,
		Runtime:  runtime,
		State:    filepath.Join(rawRoot, StateDirName),
		Log:      filepath.Join(rawRoot, LogDirName),
		Spawn:    filepath.Join(runtime, SpawnDirName),
		reserved: append([]string(nil), reserved...),
	}, nil
}

// Load reads and strictly validates the config marker inside layout.Root.
//
// The file must be a regular owner-only file that is not a symlink and is
// bounded by MaxConfigBytes; the JSON must carry the exact marker, reject
// unknown fields, duplicate keys, trailing data, invalid UTF-8, and excessive
// nesting. Every registration is validated independently and endpoints must be
// unique. Two registrations may alias one identical route (the same opaque
// address) under different endpoints, but two that share the pool's exact
// (authenticated identity, policy) pair must agree on that one route: the pool
// keys a physical transport by the pair and ignores the address, so a second
// route for the same pair is refused rather than silently dialed or ignored. A
// missing or malformed config is an error, never an empty default.
func Load(layout Layout) (*Config, error) {
	return LoadPath(layout, filepath.Join(layout.Root, ConfigFileName))
}

// LocalMuxRoute returns the single provisioned Unix route a remote-side mux
// helper bridges to. A configuration that provisions no Unix route, or more
// than one, is refused: a helper must never guess which daemon-mux endpoint it
// serves. The local binding's own carriage is deliberately excluded: the broker
// dials it itself, and no remote-side helper ever bridges it.
func (c *Config) LocalMuxRoute() (Route, error) {
	if c == nil {
		return Route{}, errors.New("brokerconfig: no configuration")
	}
	switch len(c.unixRoutes) {
	case 0:
		return Route{}, errors.New("brokerconfig: no Unix daemon-mux route is provisioned")
	case 1:
		return c.unixRoutes[0], nil
	default:
		return Route{}, errors.New("brokerconfig: more than one Unix daemon-mux route is provisioned")
	}
}
