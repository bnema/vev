package brokerstore

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"
	"unicode/utf8"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

const (
	catalogCacheFileVersion       = 4
	catalogCacheLegacyFileVersion = 3
	catalogCacheExactFileVersion  = 2
)

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
	Incarnation       *string                `json:"incarnation,omitempty"`
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

func decodeCache(raw []byte) ([]catalogue.RemoteCatalogCacheEntry, error) {
	var version catalogCacheVersion
	if err := json.Unmarshal(raw, &version); err != nil {
		return nil, err
	}
	if version.Version == nil {
		return nil, fmt.Errorf("remote catalog cache: malformed cache file: missing version")
	}

	var file catalogCacheFile
	requireIncarnation := false
	switch *version.Version {
	case catalogCacheFileVersion:
		requireIncarnation = true
		if err := strict(raw, &file); err != nil {
			return nil, err
		}
	case catalogCacheLegacyFileVersion:
		if err := strict(raw, &file); err != nil {
			return nil, err
		}
	case catalogCacheExactFileVersion:
		var legacy catalogCacheFileV2
		if err := strict(raw, &legacy); err != nil {
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
		if host.Incarnation != nil {
			raw, err := hex.DecodeString(*host.Incarnation)
			if err != nil || len(raw) != 16 {
				return nil, fmt.Errorf("remote catalog cache: malformed cache file: invalid incarnation for %q", *host.Target)
			}
			copy(entry.Incarnation[:], raw)
		} else if requireIncarnation {
			return nil, fmt.Errorf("remote catalog cache: malformed cache file: missing incarnation for %q", *host.Target)
		}
		// A zero incarnation is unbound advisory data: it loads but must
		// never seed a registration.
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

func normalizeCatalogCacheEntries(entries []catalogue.RemoteCatalogCacheEntry) ([]catalogue.RemoteCatalogCacheEntry, error) {
	if err := catalogue.ValidateRemoteCatalogCacheEntries(entries); err != nil {
		return nil, err
	}
	normalized := make([]catalogue.RemoteCatalogCacheEntry, 0, len(entries))
	hosts := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if err := domain.ValidateRemoteHostTarget(entry.Host); err != nil {
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
			Host:        entry.Host,
			FetchedAt:   entry.FetchedAt,
			Incarnation: entry.Incarnation,
			Sessions:    make([]catalogue.RemoteCatalogSession, 0, len(entry.Sessions)),
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
