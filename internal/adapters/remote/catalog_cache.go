package remote

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
	"unicode/utf8"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

const (
	catalogCacheFileName    = "remote-catalog-cache.json"
	catalogCacheFileVersion = 1
)

// CatalogCachePath returns the canonical location of the remote catalog cache in stateDir.
func CatalogCachePath(stateDir string) string { return filepath.Join(stateDir, catalogCacheFileName) }

type catalogCacheFile struct {
	Version *int                `json:"version"`
	Hosts   *[]catalogCacheHost `json:"hosts"`
}

type catalogCacheHost struct {
	Target            *string                `json:"target"`
	FetchedAtUnixNano *int64                 `json:"fetched_at_unix_nano"`
	Sessions          *[]catalogCacheSession `json:"sessions"`
}

type catalogCacheSession struct {
	Name      *string `json:"name"`
	State     *string `json:"state"`
	Ephemeral *bool   `json:"ephemeral"`
	Tabs      *uint16 `json:"tabs"`
	Attached  *bool   `json:"attached"`
}

type fileCatalogCache struct {
	path string
}

// NewFileCatalogCache returns a durable remote catalog cache backed by path.
func NewFileCatalogCache(path string) ports.RemoteCatalogCache {
	return &fileCatalogCache{path: path}
}

var _ ports.RemoteCatalogCache = (*fileCatalogCache)(nil)

// Load reads and validates one complete cache snapshot. A missing cache is an
// empty snapshot; malformed cache data is never changed by Load.
func (c *fileCatalogCache) Load() ([]ports.RemoteCatalogCacheEntry, error) {
	raw, err := os.ReadFile(c.path)
	if errors.Is(err, os.ErrNotExist) {
		return []ports.RemoteCatalogCacheEntry{}, nil
	}
	if err != nil {
		return nil, err
	}
	if !utf8.Valid(raw) {
		return nil, fmt.Errorf("remote catalog cache: malformed cache file: invalid UTF-8")
	}

	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var file catalogCacheFile
	if err := dec.Decode(&file); err != nil {
		return nil, fmt.Errorf("remote catalog cache: malformed cache file: %w", err)
	}
	if err := rejectTrailingJSON(dec); err != nil {
		return nil, fmt.Errorf("remote catalog cache: malformed cache file: %w", err)
	}
	if file.Version == nil {
		return nil, fmt.Errorf("remote catalog cache: malformed cache file: missing version")
	}
	if *file.Version != catalogCacheFileVersion {
		return nil, fmt.Errorf("remote catalog cache: unsupported cache file version %d", *file.Version)
	}
	if file.Hosts == nil {
		return nil, fmt.Errorf("remote catalog cache: malformed cache file: missing hosts")
	}

	entries := make([]ports.RemoteCatalogCacheEntry, 0, len(*file.Hosts))
	for _, host := range *file.Hosts {
		if host.Target == nil || host.FetchedAtUnixNano == nil || host.Sessions == nil {
			return nil, fmt.Errorf("remote catalog cache: malformed cache file: missing host fields")
		}
		if !utf8.ValidString(*host.Target) {
			return nil, fmt.Errorf("remote catalog cache: malformed cache file: invalid UTF-8")
		}
		if *host.FetchedAtUnixNano <= 0 {
			return nil, fmt.Errorf("remote catalog cache: malformed cache file: non-positive fetched time")
		}
		entry := ports.RemoteCatalogCacheEntry{
			Host:      *host.Target,
			FetchedAt: time.Unix(0, *host.FetchedAtUnixNano),
			Sessions:  make([]ports.RemoteCatalogSession, 0, len(*host.Sessions)),
		}
		for _, session := range *host.Sessions {
			if session.Name == nil || session.State == nil || session.Ephemeral == nil || session.Tabs == nil || session.Attached == nil {
				return nil, fmt.Errorf("remote catalog cache: malformed cache file: missing session fields")
			}
			if !utf8.ValidString(*session.Name) || !utf8.ValidString(*session.State) {
				return nil, fmt.Errorf("remote catalog cache: malformed cache file: invalid UTF-8")
			}
			entry.Sessions = append(entry.Sessions, ports.RemoteCatalogSession{
				Name:      *session.Name,
				State:     *session.State,
				Ephemeral: *session.Ephemeral,
				Tabs:      *session.Tabs,
				Attached:  *session.Attached,
			})
		}
		entries = append(entries, entry)
	}
	normalized, err := normalizeCatalogCacheEntries(entries)
	if err != nil {
		return nil, err
	}
	return normalized, nil
}

// Store atomically replaces the cache with a complete normalized snapshot.
func (c *fileCatalogCache) Store(entries []ports.RemoteCatalogCacheEntry) error {
	normalized, err := normalizeCatalogCacheEntries(entries)
	if err != nil {
		return err
	}
	hosts := make([]catalogCacheHost, 0, len(normalized))
	for _, entry := range normalized {
		target := entry.Host
		fetchedAt := entry.FetchedAt.UnixNano()
		sessions := make([]catalogCacheSession, 0, len(entry.Sessions))
		for _, session := range entry.Sessions {
			name := session.Name
			state := session.State
			ephemeral := session.Ephemeral
			tabs := session.Tabs
			attached := session.Attached
			sessions = append(sessions, catalogCacheSession{
				Name:      &name,
				State:     &state,
				Ephemeral: &ephemeral,
				Tabs:      &tabs,
				Attached:  &attached,
			})
		}
		hosts = append(hosts, catalogCacheHost{
			Target:            &target,
			FetchedAtUnixNano: &fetchedAt,
			Sessions:          &sessions,
		})
	}
	version := catalogCacheFileVersion
	payload, err := json.Marshal(catalogCacheFile{Version: &version, Hosts: &hosts})
	if err != nil {
		return err
	}
	return atomicReplacePrivateFile(c.path, append(payload, '\n'))
}

func normalizeCatalogCacheEntries(entries []ports.RemoteCatalogCacheEntry) ([]ports.RemoteCatalogCacheEntry, error) {
	normalized := make([]ports.RemoteCatalogCacheEntry, 0, len(entries))
	hosts := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if err := validateHostTarget(entry.Host); err != nil {
			return nil, fmt.Errorf("remote catalog cache: invalid host %q: %w", entry.Host, err)
		}
		if entry.FetchedAt.UnixNano() <= 0 {
			return nil, fmt.Errorf("remote catalog cache: host %q has non-positive fetched time", entry.Host)
		}
		if _, duplicate := hosts[entry.Host]; duplicate {
			return nil, fmt.Errorf("remote catalog cache: duplicate host %q", entry.Host)
		}
		hosts[entry.Host] = struct{}{}

		copyEntry := ports.RemoteCatalogCacheEntry{
			Host:      entry.Host,
			FetchedAt: entry.FetchedAt,
			Sessions:  make([]ports.RemoteCatalogSession, 0, len(entry.Sessions)),
		}
		sessions := make(map[string]struct{}, len(entry.Sessions))
		for _, session := range entry.Sessions {
			if !utf8.ValidString(session.Name) || !utf8.ValidString(session.State) {
				return nil, fmt.Errorf("remote catalog cache: session is not valid UTF-8")
			}
			if err := domain.ValidateSessionName(session.Name); err != nil {
				return nil, fmt.Errorf("remote catalog cache: invalid session %q: %w", session.Name, err)
			}
			if !validCatalogState(session.State) {
				return nil, fmt.Errorf("remote catalog cache: invalid session state %q", session.State)
			}
			if _, duplicate := sessions[session.Name]; duplicate {
				return nil, fmt.Errorf("remote catalog cache: duplicate session %q for host %q", session.Name, entry.Host)
			}
			sessions[session.Name] = struct{}{}
			copyEntry.Sessions = append(copyEntry.Sessions, session)
		}
		sort.Slice(copyEntry.Sessions, func(i, j int) bool {
			return copyEntry.Sessions[i].Name < copyEntry.Sessions[j].Name
		})
		normalized = append(normalized, copyEntry)
	}
	sort.Slice(normalized, func(i, j int) bool {
		return normalized[i].Host < normalized[j].Host
	})
	return normalized, nil
}
