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
	appports "github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

const (
	catalogCacheFileName         = "remote-catalog-cache.json"
	catalogCacheExactFileVersion = 2
	catalogCacheFileVersion      = 3
)

// CatalogCachePath returns the canonical location of the remote catalog cache in stateDir.
func CatalogCachePath(stateDir string) string { return filepath.Join(stateDir, catalogCacheFileName) }

type catalogCacheFile struct {
	Version *int                `json:"version"`
	Hosts   *[]catalogCacheHost `json:"hosts"`
}

type catalogCacheVersion struct {
	Version *int `json:"version"`
}

type catalogCacheHost struct {
	Target            *string                `json:"target"`
	FetchedAtUnixNano *int64                 `json:"fetched_at_unix_nano"`
	Sessions          *[]catalogCacheSession `json:"sessions"`
}

type catalogCacheSession struct {
	LifecycleID *domain.SessionLifecycleID           `json:"lifecycle_id"`
	Name        *string                              `json:"name"`
	State       *catalogue.RemoteCatalogSessionState `json:"state"`
	Ephemeral   *bool                                `json:"ephemeral"`
	LastUsedSeq *uint64                              `json:"last_used_seq,omitempty"`
	Tabs        *[]catalogue.RemoteCatalogTab        `json:"tabs"`
	ActiveTabID *string                              `json:"active_tab_id,omitempty"`
	Attached    *bool                                `json:"attached"`
}

type catalogCacheFileV2 struct {
	Version *int                  `json:"version"`
	Hosts   *[]catalogCacheHostV2 `json:"hosts"`
}

type catalogCacheHostV2 struct {
	Target            *string                  `json:"target"`
	FetchedAtUnixNano *int64                   `json:"fetched_at_unix_nano"`
	Sessions          *[]catalogCacheSessionV2 `json:"sessions"`
}

type catalogCacheSessionV2 struct {
	LifecycleID *domain.SessionLifecycleID           `json:"lifecycle_id,omitempty"`
	Name        *string                              `json:"name"`
	State       *catalogue.RemoteCatalogSessionState `json:"state"`
	Ephemeral   *bool                                `json:"ephemeral"`
	LastUsedSeq *uint64                              `json:"last_used_seq,omitempty"`
	Tabs        *uint16                              `json:"tabs,omitempty"`
	TabList     *[]catalogue.RemoteCatalogTab        `json:"tab_list,omitempty"`
	ActiveTabID *string                              `json:"active_tab_id,omitempty"`
	Attached    *bool                                `json:"attached"`
}

type fileCatalogCache struct {
	path string
}

// NewFileCatalogCache returns a durable remote catalog cache backed by path.
func NewFileCatalogCache(path string) appports.RemoteCatalogCache {
	return &fileCatalogCache{path: path}
}

var _ appports.RemoteCatalogCache = (*fileCatalogCache)(nil)

// Load reads and validates one complete cache snapshot. Exact v2 tab-list
// snapshots are upgraded in memory; ambiguous count-only v2 data is rejected.
// A missing cache is an empty snapshot, and Load never changes cache data.
func (c *fileCatalogCache) Load() ([]catalogue.RemoteCatalogCacheEntry, error) {
	raw, err := os.ReadFile(c.path)
	if errors.Is(err, os.ErrNotExist) {
		return []catalogue.RemoteCatalogCacheEntry{}, nil
	}
	if err != nil {
		return nil, err
	}
	if !utf8.Valid(raw) {
		return nil, fmt.Errorf("remote catalog cache: malformed cache file: invalid UTF-8")
	}

	var version catalogCacheVersion
	if err := decodeCatalogCacheFile(raw, false, &version); err != nil {
		return nil, err
	}
	if version.Version == nil {
		return nil, fmt.Errorf("remote catalog cache: malformed cache file: missing version")
	}

	var file catalogCacheFile
	switch *version.Version {
	case catalogCacheFileVersion:
		if err := decodeCatalogCacheFile(raw, true, &file); err != nil {
			return nil, err
		}
	case catalogCacheExactFileVersion:
		var legacy catalogCacheFileV2
		if err := decodeCatalogCacheFile(raw, true, &legacy); err != nil {
			return nil, err
		}
		migrated, err := migrateExactCatalogCacheV2(legacy)
		if err != nil {
			return nil, err
		}
		file = migrated
	default:
		return nil, fmt.Errorf("remote catalog cache: unsupported cache file version %d", *version.Version)
	}
	if file.Hosts == nil {
		return nil, fmt.Errorf("remote catalog cache: malformed cache file: missing hosts")
	}

	entries := make([]catalogue.RemoteCatalogCacheEntry, 0, len(*file.Hosts))
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
		entry := catalogue.RemoteCatalogCacheEntry{
			Host:      *host.Target,
			FetchedAt: time.Unix(0, *host.FetchedAtUnixNano),
			Sessions:  make([]catalogue.RemoteCatalogSession, 0, len(*host.Sessions)),
		}
		for _, session := range *host.Sessions {
			if session.LifecycleID == nil || session.Name == nil || session.State == nil || session.Ephemeral == nil || session.Attached == nil || session.Tabs == nil {
				return nil, fmt.Errorf("remote catalog cache: malformed cache file: missing session fields")
			}
			if !utf8.ValidString(*session.Name) || !utf8.ValidString(string(*session.State)) {
				return nil, fmt.Errorf("remote catalog cache: malformed cache file: invalid UTF-8")
			}
			tabs := make([]catalogue.RemoteCatalogTab, len(*session.Tabs))
			copy(tabs, *session.Tabs)
			decoded := catalogue.RemoteCatalogSession{
				LifecycleID: *session.LifecycleID,
				Name:        *session.Name,
				State:       *session.State,
				Ephemeral:   *session.Ephemeral,
				Tabs:        tabs,
				Attached:    *session.Attached,
			}
			if session.LastUsedSeq != nil {
				decoded.LastUsedSeq = *session.LastUsedSeq
			}
			if session.ActiveTabID != nil {
				decoded.ActiveTabID = *session.ActiveTabID
			}
			entry.Sessions = append(entry.Sessions, decoded)
		}
		entries = append(entries, entry)
	}
	normalized, err := normalizeCatalogCacheEntries(entries)
	if err != nil {
		return nil, err
	}
	return normalized, nil
}

func decodeCatalogCacheFile(raw []byte, strict bool, dst any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	if strict {
		dec.DisallowUnknownFields()
	}
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("remote catalog cache: malformed cache file: %w", err)
	}
	if err := rejectTrailingJSON(dec); err != nil {
		return fmt.Errorf("remote catalog cache: malformed cache file: %w", err)
	}
	return nil
}

func migrateExactCatalogCacheV2(legacy catalogCacheFileV2) (catalogCacheFile, error) {
	if legacy.Version == nil || *legacy.Version != catalogCacheExactFileVersion || legacy.Hosts == nil {
		return catalogCacheFile{}, fmt.Errorf("remote catalog cache: malformed version 2 cache file")
	}
	hosts := make([]catalogCacheHost, 0, len(*legacy.Hosts))
	for _, host := range *legacy.Hosts {
		if host.Sessions == nil {
			return catalogCacheFile{}, fmt.Errorf("remote catalog cache: malformed cache file: missing host fields")
		}
		sessions := make([]catalogCacheSession, 0, len(*host.Sessions))
		for _, session := range *host.Sessions {
			if session.Tabs != nil || session.TabList == nil {
				return catalogCacheFile{}, fmt.Errorf("remote catalog cache: unsupported ambiguous version 2 tab count")
			}
			sessions = append(sessions, catalogCacheSession{
				LifecycleID: session.LifecycleID,
				Name:        session.Name,
				State:       session.State,
				Ephemeral:   session.Ephemeral,
				LastUsedSeq: session.LastUsedSeq,
				Tabs:        session.TabList,
				ActiveTabID: session.ActiveTabID,
				Attached:    session.Attached,
			})
		}
		hosts = append(hosts, catalogCacheHost{
			Target: host.Target, FetchedAtUnixNano: host.FetchedAtUnixNano, Sessions: &sessions,
		})
	}
	version := catalogCacheFileVersion
	return catalogCacheFile{Version: &version, Hosts: &hosts}, nil
}

// Store atomically replaces the cache with a complete normalized snapshot.
func (c *fileCatalogCache) Store(entries []catalogue.RemoteCatalogCacheEntry) error {
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
			attached := session.Attached
			id := session.LifecycleID
			tabs := make([]catalogue.RemoteCatalogTab, len(session.Tabs))
			copy(tabs, session.Tabs)
			row := catalogCacheSession{LifecycleID: &id, Name: &name, State: &state, Ephemeral: &ephemeral, Tabs: &tabs, Attached: &attached}
			if session.LastUsedSeq != 0 {
				lastUsed := session.LastUsedSeq
				row.LastUsedSeq = &lastUsed
			}
			if session.ActiveTabID != "" {
				active := session.ActiveTabID
				row.ActiveTabID = &active
			}
			sessions = append(sessions, row)
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

func normalizeCatalogCacheEntries(entries []catalogue.RemoteCatalogCacheEntry) ([]catalogue.RemoteCatalogCacheEntry, error) {
	if err := catalogue.ValidateRemoteCatalogCacheEntries(entries); err != nil {
		return nil, err
	}
	normalized := make([]catalogue.RemoteCatalogCacheEntry, 0, len(entries))
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

		copyEntry := catalogue.RemoteCatalogCacheEntry{
			Host:      entry.Host,
			FetchedAt: entry.FetchedAt,
			Sessions:  make([]catalogue.RemoteCatalogSession, 0, len(entry.Sessions)),
		}
		sessions := make(map[string]struct{}, len(entry.Sessions))
		for _, session := range entry.Sessions {
			if !utf8.ValidString(session.Name) || !utf8.ValidString(string(session.State)) {
				return nil, fmt.Errorf("remote catalog cache: session is not valid UTF-8")
			}
			if err := domain.ValidateSessionName(session.Name); err != nil {
				return nil, fmt.Errorf("remote catalog cache: invalid session %q: %w", session.Name, err)
			}
			if !session.State.Valid() {
				return nil, fmt.Errorf("remote catalog cache: invalid session state %q", session.State)
			}
			if _, duplicate := sessions[session.Name]; duplicate {
				return nil, fmt.Errorf("remote catalog cache: duplicate session %q for host %q", session.Name, entry.Host)
			}
			sessions[session.Name] = struct{}{}
			copySession := session
			copySession.Tabs = make([]catalogue.RemoteCatalogTab, len(session.Tabs))
			for i, tab := range session.Tabs {
				copySession.Tabs[i] = catalogue.RemoteCatalogTab{ID: tab.ID, Index: tab.Index, Name: tab.Name}
			}
			copyEntry.Sessions = append(copyEntry.Sessions, copySession)
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
