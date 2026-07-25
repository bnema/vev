package snapshot

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	codec "github.com/bnema/vev/internal/usecase/snapshot"
)

func TestRepositoryHeadFailureKeepsOldCompleteGeneration(t *testing.T) {
	dir := privateDir(t)
	repo := NewRepository(dir)
	first := repositoryPublication(t, "named", 1, []byte("one"))
	if err := repo.Publish(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	second := repositoryPublicationAfter(t, repo, "named", 2, []byte("two"))
	repo.hooks.beforeHeadWrite = func(string) error { return errors.New("injected HEAD failure") }
	if err := repo.Publish(context.Background(), second); err == nil {
		t.Fatal("Publish succeeded with injected HEAD failure")
	}
	repo.hooks.beforeHeadWrite = nil
	got, err := repo.Load(context.Background(), "named")
	if err != nil {
		t.Fatal(err)
	}
	if got.Generation != 1 {
		t.Fatalf("generation after failed publish = %d, want 1", got.Generation)
	}
}

func TestRepositoryCleanupFaultsSurfaceForImmutableAndMutablePublication(t *testing.T) {
	primary := errors.New("primary write failure")
	cleanup := errors.New("cleanup failure")
	cases := []struct {
		name     string
		location string
		cleanup  string
	}{
		{name: "immutable/close temporary", location: "manifest", cleanup: "close"},
		{name: "immutable/remove temporary", location: "manifest", cleanup: "remove"},
		{name: "immutable/sync after temporary removal", location: "manifest", cleanup: "sync-after-remove"},
		{name: "mutable/close temporary", location: "HEAD", cleanup: "close"},
		{name: "mutable/remove temporary", location: "HEAD", cleanup: "remove"},
		{name: "mutable/sync after temporary removal", location: "HEAD", cleanup: "sync-after-remove"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			repo := NewRepository(privateDir(t))
			first := repositoryPublication(t, "named", 1, []byte("one"))
			if err := repo.Publish(context.Background(), first); err != nil {
				t.Fatal(err)
			}
			second := repositoryPublicationAfter(t, repo, "named", 2, []byte("two"))
			key := legacyIncarnationID(second.Name).String()
			if tc.location == "manifest" {
				prepareObjects(t, repo, second)
			} else {
				prepareHeadStage(t, repo, second)
			}
			dir := filepath.Dir(repo.legacyManifestPath(key, second.Generation))
			phase := "write manifest"
			if tc.location == "HEAD" {
				dir = filepath.Dir(repo.legacyHeadPath(key))
				phase = "write HEAD"
			}
			repo.hooks.writeTemp = func(path string) error {
				if filepath.Dir(path) == dir {
					return primary
				}
				return nil
			}
			repo.hooks.closeFile = func(path string) error {
				if tc.cleanup == "close" && filepath.Dir(path) == dir {
					return cleanup
				}
				return nil
			}
			repo.hooks.remove = func(string) error {
				if tc.cleanup == "remove" {
					return cleanup
				}
				return nil
			}
			repo.hooks.syncDirectory = func(path string) error {
				if tc.cleanup == "sync-after-remove" && path == dir {
					return cleanup
				}
				return nil
			}

			err := repo.Publish(context.Background(), second)
			if !errors.Is(err, primary) || !errors.Is(err, cleanup) || !strings.Contains(err.Error(), phase) {
				t.Fatalf("Publish error = %v, want %q and both primary and cleanup failures", err, phase)
			}
			if tc.cleanup != "remove" {
				matches, globErr := filepath.Glob(filepath.Join(dir, ".tmp-*"))
				if globErr != nil || len(matches) != 0 {
					t.Fatalf("temporary files after successful cleanup = %v, glob error = %v", matches, globErr)
				}
			}
			repo.hooks = repositoryHooks{}
			assertRepositoryCompleteGeneration(t, repo, "named", 1, 2)
		})
	}
}

func TestRepositoryNewDirectorySyncFaultsAreIndependent(t *testing.T) {
	cases := []struct {
		name       string
		phase      string
		occurrence int
		directory  func(*Repository, string) string
	}{
		{name: "repository", phase: "repository", occurrence: 1, directory: func(_ *Repository, root string) string { return filepath.Dir(root) }},
		{name: "sessions", phase: "sessions", occurrence: 1, directory: func(repo *Repository, _ string) string { return repo.dir }},
		{name: "session", phase: "session", occurrence: 1, directory: func(repo *Repository, _ string) string { return filepath.Join(repo.dir, repositorySessionsDir) }},
		{name: "objects", phase: "objects", occurrence: 1, directory: func(repo *Repository, _ string) string {
			return repo.legacySessionPath(legacyIncarnationID("named").String())
		}},
		{name: "generations", phase: "generations", occurrence: 2, directory: func(repo *Repository, _ string) string {
			return repo.legacySessionPath(legacyIncarnationID("named").String())
		}},
		{name: "object shard", phase: "object shard", occurrence: 1, directory: func(repo *Repository, _ string) string {
			return filepath.Join(repo.legacySessionPath(legacyIncarnationID("named").String()), repositoryObjectsDir)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parent := privateDir(t)
			root := filepath.Join(parent, "repository")
			repo := NewRepository(root)
			wantDirectory := tc.directory(repo, root)
			calls := 0
			injected := errors.New("new directory sync failure")
			repo.hooks.syncDirectory = func(directory string) error {
				if directory == wantDirectory {
					calls++
					if calls == tc.occurrence {
						return injected
					}
				}
				return nil
			}

			publication := repositoryPublication(t, "named", 1, []byte("one"))
			err := repo.Publish(context.Background(), publication)
			if !errors.Is(err, injected) || !strings.Contains(err.Error(), tc.phase) {
				t.Fatalf("Publish error = %v, want %q-labelled directory sync failure", err, tc.phase)
			}
			repo.hooks = repositoryHooks{}
			// A first-generation failure cannot have an older complete generation;
			// it must not expose an incomplete one as if it were loadable.
			if _, loadErr := repo.Load(context.Background(), "named"); loadErr == nil || !strings.Contains(loadErr.Error(), "no complete snapshot generation") {
				t.Fatalf("Load error after incomplete first generation = %v, want no complete generation", loadErr)
			}
			if err := repo.Publish(context.Background(), publication); err != nil {
				t.Fatal(err)
			}
			assertRepositoryCompleteGeneration(t, repo, "named", 1)
		})
	}
}

func assertRepositoryCompleteGeneration(t *testing.T, repo *Repository, name string, generations ...uint64) {
	t.Helper()
	got, err := repo.Load(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	for _, generation := range generations {
		if got.Generation == generation && len(got.Manifest) != 0 && len(got.Objects) != 0 {
			return
		}
	}
	t.Fatalf("Load = incomplete or mixed generation %+v, want one of %v", got, generations)
}

func TestRepositoryLoadFallsBackFromIncompleteNewestGeneration(t *testing.T) {
	dir := privateDir(t)
	repo := NewRepository(dir)
	first := repositoryPublication(t, "named", 1, []byte("one"))
	if err := repo.Publish(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	second := repositoryPublicationAfter(t, repo, "named", 2, []byte("two"))
	if err := repo.Publish(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	manifest, err := codec.UnmarshalManifest(second.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(repo.legacyObjectPath(legacyIncarnationID("named").String(), manifest.Tabs[0].Panes[0].Tail.Digest)); err != nil {
		t.Fatal(err)
	}
	got, err := repo.Load(context.Background(), "named")
	if err != nil {
		t.Fatal(err)
	}
	if got.Generation != 1 {
		t.Fatalf("Load generation = %d, want 1", got.Generation)
	}
}
