package remote

import (
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
)

func TestCatalogCachePath(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	want := filepath.Join(stateDir, "remote-catalog-cache.json")
	if got := CatalogCachePath(stateDir); got != want {
		t.Fatalf("CatalogCachePath(%q) = %q, want %q", stateDir, got, want)
	}
}

func TestCatalogCacheStoreLoad(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "vev", "remote-catalog-cache.json")
	cache := NewFileCatalogCache(path)
	fetchedAt := time.Unix(0, 1780000000000000000)
	entries := []ports.RemoteCatalogCacheEntry{
		{
			Host:      "zebra",
			FetchedAt: fetchedAt,
			Sessions: []ports.RemoteCatalogSession{
				{
					LifecycleID: [16]byte{1}, Name: "work", State: "up", LastUsedSeq: 42, ActiveTabID: "work-2",
					Tabs: []ports.RemoteCatalogTab{{ID: "work-1", Detail: "dynamic", Attention: true}, {ID: "work-2", Index: 1}},
				},
				{LifecycleID: [16]byte{2}, Name: "alpha", State: "down", Ephemeral: true, Tabs: []ports.RemoteCatalogTab{{ID: "alpha-1"}}, Attached: true},
			},
		},
		{
			Host:      "arch",
			FetchedAt: fetchedAt.Add(time.Second),
			Sessions:  []ports.RemoteCatalogSession{},
		},
	}

	if err := cache.Store(entries); err != nil {
		t.Fatalf("Store() error = %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read cache: %v", err)
	}
	arch := strings.Index(string(raw), `"target":"arch"`)
	zebra := strings.Index(string(raw), `"target":"zebra"`)
	if arch < 0 || zebra < 0 || arch >= zebra {
		t.Fatalf("stored hosts are not ordered: %s", raw)
	}
	alpha := strings.Index(string(raw), `"name":"alpha"`)
	work := strings.Index(string(raw), `"name":"work"`)
	if alpha < 0 || work < 0 || alpha >= work {
		t.Fatalf("stored sessions are not ordered: %s", raw)
	}
	if !strings.Contains(string(raw), `"version":3`) || !strings.Contains(string(raw), `"tabs":[{"id":"work-1"`) {
		t.Fatalf("stored cache does not pin the v3 typed-tab contract: %s", raw)
	}
	if strings.Contains(string(raw), `"detail"`) || strings.Contains(string(raw), `"attention"`) {
		t.Fatalf("stored cache contains dynamic tab fields: %s", raw)
	}
	got, err := cache.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	want := []ports.RemoteCatalogCacheEntry{
		{
			Host:      "arch",
			FetchedAt: fetchedAt.Add(time.Second),
			Sessions:  []ports.RemoteCatalogSession{},
		},
		{
			Host:      "zebra",
			FetchedAt: fetchedAt,
			Sessions: []ports.RemoteCatalogSession{
				{LifecycleID: [16]byte{2}, Name: "alpha", State: "down", Ephemeral: true, Tabs: []ports.RemoteCatalogTab{{ID: "alpha-1"}}, Attached: true},
				{LifecycleID: [16]byte{1}, Name: "work", State: "up", LastUsedSeq: 42, ActiveTabID: "work-2", Tabs: []ports.RemoteCatalogTab{{ID: "work-1"}, {ID: "work-2", Index: 1}}},
			},
		},
	}
	if !slices.EqualFunc(got, want, equalRemoteCatalogCacheEntry) {
		t.Fatalf("Load() = %#v, want %#v", got, want)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat cache: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("cache permissions = %04o, want 0600", perm)
	}
	dirInfo, err := os.Lstat(filepath.Dir(path))
	if err != nil {
		t.Fatalf("stat cache directory: %v", err)
	}
	if perm := dirInfo.Mode().Perm(); perm != 0o700 {
		t.Fatalf("cache directory permissions = %04o, want 0700", perm)
	}
	entriesOnDisk, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatalf("read cache directory: %v", err)
	}
	for _, entry := range entriesOnDisk {
		if strings.Contains(entry.Name(), ".tmp") {
			t.Fatalf("temporary file left behind: %q", entry.Name())
		}
	}
}

func TestCatalogCacheLoadMigratesExactV2TabList(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "vev", "remote-catalog-cache.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	raw := []byte(`{"version":2,"hosts":[{"target":"user@arch","fetched_at_unix_nano":1780000000000000000,"sessions":[{"lifecycle_id":"02000000000000000000000000000000","name":"dumber","state":"down","ephemeral":false,"tab_list":[],"attached":false},{"lifecycle_id":"01000000000000000000000000000000","name":"work","state":"up","ephemeral":false,"last_used_seq":42,"tab_list":[{"id":"t_work","index":0,"name":"shell"}],"active_tab_id":"t_work","attached":false}]}]}`)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write cache: %v", err)
	}

	got, err := NewFileCatalogCache(path).Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	want := []ports.RemoteCatalogCacheEntry{{
		Host:      "user@arch",
		FetchedAt: time.Unix(0, 1780000000000000000),
		Sessions: []ports.RemoteCatalogSession{
			{LifecycleID: [16]byte{2}, Name: "dumber", State: ports.RemoteCatalogSessionDown, Tabs: []ports.RemoteCatalogTab{}},
			{
				LifecycleID: [16]byte{1}, Name: "work", State: ports.RemoteCatalogSessionUp,
				LastUsedSeq: 42, Tabs: []ports.RemoteCatalogTab{{ID: "t_work", Name: "shell"}}, ActiveTabID: "t_work",
			},
		},
	}}
	if !slices.EqualFunc(got, want, equalRemoteCatalogCacheEntry) {
		t.Fatalf("Load() = %#v, want %#v", got, want)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read cache after load: %v", err)
	}
	if !slices.Equal(after, raw) {
		t.Fatalf("Load changed v2 cache: got %q, want %q", after, raw)
	}
}

func TestCatalogCacheLoadRejectsInvalidFilesWithoutReplacingThem(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		raw  []byte
	}{
		{name: "truncated", raw: []byte(`{"version":3,"hosts":[`)},
		{name: "trailing JSON", raw: []byte(`{"version":3,"hosts":[]} {}`)},
		{name: "obsolete count-only version", raw: []byte(`{"version":2,"hosts":[{"target":"arch","fetched_at_unix_nano":1,"sessions":[{"lifecycle_id":"01000000000000000000000000000000","name":"work","state":"up","ephemeral":false,"tabs":1,"active_tab_id":"t_work","attached":false}]}]}`)},
		{name: "unknown version", raw: []byte(`{"version":4,"hosts":[]}`)},
		{name: "missing hosts", raw: []byte(`{"version":3}`)},
		{name: "null hosts", raw: []byte(`{"version":3,"hosts":null}`)},
		{name: "null sessions", raw: []byte(`{"version":3,"hosts":[{"target":"arch","fetched_at_unix_nano":1,"sessions":null}]}`)},
		{name: "zero fetched at", raw: []byte(`{"version":3,"hosts":[{"target":"arch","fetched_at_unix_nano":0,"sessions":[]}]}`)},
		{name: "negative fetched at", raw: []byte(`{"version":3,"hosts":[{"target":"arch","fetched_at_unix_nano":-1,"sessions":[]}]}`)},
		{name: "duplicate hosts", raw: []byte(`{"version":3,"hosts":[{"target":"arch","fetched_at_unix_nano":1,"sessions":[]},{"target":"arch","fetched_at_unix_nano":2,"sessions":[]}]}`)},
		{name: "missing exact session fields", raw: []byte(`{"version":3,"hosts":[{"target":"arch","fetched_at_unix_nano":1,"sessions":[{"name":"work","state":"up","ephemeral":false,"tabs":[],"attached":false}]}]}`)},
		{name: "count-only tabs", raw: []byte(`{"version":3,"hosts":[{"target":"arch","fetched_at_unix_nano":1,"sessions":[{"lifecycle_id":"01000000000000000000000000000000","name":"work","state":"up","ephemeral":false,"tabs":1,"attached":false}]}]}`)},
		{name: "unknown field", raw: []byte(`{"version":3,"hosts":[],"future":true}`)},
		{name: "invalid utf8", raw: []byte("{\"version\":3,\"hosts\":[{\"target\":\"\xff\",\"fetched_at_unix_nano\":1,\"sessions\":[]}]}")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "vev", "remote-catalog-cache.json")
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				t.Fatalf("mkdir: %v", err)
			}
			if err := os.WriteFile(path, tc.raw, 0o600); err != nil {
				t.Fatalf("write cache: %v", err)
			}

			if _, err := NewFileCatalogCache(path).Load(); err == nil {
				t.Fatal("Load() error = nil, want malformed cache error")
			}
			after, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read cache: %v", err)
			}
			if !slices.Equal(after, tc.raw) {
				t.Fatalf("invalid cache changed: got %q, want %q", after, tc.raw)
			}
		})
	}
}

func TestCatalogCacheStoreFailurePreservesExistingFile(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "vev", "remote-catalog-cache.json")
	cache := NewFileCatalogCache(path)
	valid := []ports.RemoteCatalogCacheEntry{{
		Host:      "arch",
		FetchedAt: time.Unix(0, 1),
		Sessions:  []ports.RemoteCatalogSession{},
	}}
	if err := cache.Store(valid); err != nil {
		t.Fatalf("initial Store() error = %v", err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read initial cache: %v", err)
	}

	invalid := []ports.RemoteCatalogCacheEntry{{
		Host:      "arch",
		FetchedAt: time.Time{},
		Sessions:  []ports.RemoteCatalogSession{},
	}}
	if err := cache.Store(invalid); err == nil {
		t.Fatal("Store() error = nil, want validation error")
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read cache after failed Store: %v", err)
	}
	if !slices.Equal(after, before) {
		t.Fatalf("failed Store changed cache: got %q, want %q", after, before)
	}
}

func TestCatalogCacheStoreRejectsNonPositiveFetchedTimes(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		fetchedAt time.Time
	}{
		{name: "zero Unix nano", fetchedAt: time.Unix(0, 0)},
		{name: "negative Unix nano", fetchedAt: time.Unix(0, -1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "remote-catalog-cache.json")
			entries := []ports.RemoteCatalogCacheEntry{{
				Host:      "arch",
				FetchedAt: tc.fetchedAt,
				Sessions:  []ports.RemoteCatalogSession{},
			}}

			err := NewFileCatalogCache(path).Store(entries)

			if err == nil {
				t.Fatal("Store() error = nil, want non-positive fetched time error")
			}
			if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
				t.Fatalf("failed Store created cache: stat error = %v", statErr)
			}
		})
	}
}

func TestCatalogCacheStoreHardensExistingFilePermissions(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "vev", "remote-catalog-cache.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(`{"version":1,"hosts":[]}`), 0o644); err != nil {
		t.Fatalf("seed cache: %v", err)
	}

	cache := NewFileCatalogCache(path)
	if err := cache.Store([]ports.RemoteCatalogCacheEntry{}); err != nil {
		t.Fatalf("Store() error = %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat cache: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("cache permissions = %04o, want 0600", perm)
	}
}

func TestCatalogCacheStoreRenameFailureCleansTemporaryFile(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "vev")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir parent: %v", err)
	}
	path := filepath.Join(dir, "remote-catalog-cache.json")
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatalf("mkdir destination: %v", err)
	}

	cache := NewFileCatalogCache(path)
	if err := cache.Store([]ports.RemoteCatalogCacheEntry{}); err == nil {
		t.Fatal("Store() error = nil, want rename error")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read cache directory: %v", err)
	}
	for _, entry := range entries {
		if strings.Contains(entry.Name(), ".tmp") {
			t.Fatalf("temporary file left behind after failed rename: %q", entry.Name())
		}
	}
}

func equalRemoteCatalogCacheEntry(a, b ports.RemoteCatalogCacheEntry) bool {
	return a.Host == b.Host && a.FetchedAt.Equal(b.FetchedAt) && reflect.DeepEqual(a.Sessions, b.Sessions)
}
