package snapshot

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestRepositoryRejectsSymlinkedRoot(t *testing.T) {
	configuredRoot := filepath.Join(t.TempDir(), "configured")
	externalRoot := privateDir(t)
	if err := os.Symlink(externalRoot, configuredRoot); err != nil {
		t.Fatal(err)
	}
	if _, err := NewRepository(configuredRoot).openRoot(); err == nil {
		t.Fatal("openRoot succeeded through configured-root symlink")
	}

	configuredRoot = privateDir(t)
	repo := NewRepository(configuredRoot)
	publication := repositoryPublication(t, "named", 1, []byte("configured"))
	if err := repo.Publish(context.Background(), publication); err != nil {
		t.Fatal(err)
	}
	external := NewRepository(externalRoot)
	externalPublication := repositoryPublication(t, publication.Name, 1, []byte("external"))
	if err := external.Publish(context.Background(), externalPublication); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(configuredRoot, configuredRoot+"-real"); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(externalRoot, configuredRoot); err != nil {
		t.Fatal(err)
	}

	if _, err := repo.Load(context.Background(), publication.Name); err == nil {
		t.Fatal("Load succeeded through replaced configured-root symlink")
	}
	if err := repo.Maintain(context.Background()); err == nil {
		t.Fatal("Maintain succeeded through replaced configured-root symlink")
	}
	loaded, err := external.Load(context.Background(), publication.Name)
	if err != nil {
		t.Fatal(err)
	}
	if string(loaded.Manifest) != string(externalPublication.Manifest) {
		t.Fatal("external repository changed")
	}
}

func TestOpenRootRejectsUnsafePinnedRoot(t *testing.T) {
	dir := privateDir(t)
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := NewRepository(dir).openRoot(); err == nil || !strings.Contains(err.Error(), "not private") {
		t.Fatalf("openRoot error = %v, want private-directory rejection", err)
	}
}

func TestOpenRootDetectsReplacementAfterOpen(t *testing.T) {
	dir := privateDir(t)
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	old := dir + "-old"
	replacementMarker := filepath.Join(dir, "replacement-marker")
	repo := NewRepository(dir)
	repo.hooks.afterOpenRoot = func() {
		repo.hooks.afterOpenRoot = nil
		if err := os.Rename(dir, old); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(replacementMarker, []byte("unchanged"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := repo.openRoot(); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("openRoot error = %v, want ESTALE", err)
	}
	if got, err := os.ReadFile(replacementMarker); err != nil || string(got) != "unchanged" {
		t.Fatalf("replacement marker = %q, %v", got, err)
	}
	if _, err := os.Stat(filepath.Join(old, "replacement-marker")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pinned tree was operated on: %v", err)
	}
}

func TestOpenDirectoryClosesChildWhenRootCloseFails(t *testing.T) {
	repo := NewRepository(privateDir(t))
	if err := os.Mkdir(repo.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	child := filepath.Join(repo.dir, "child")
	if err := os.Mkdir(child, 0o700); err != nil {
		t.Fatal(err)
	}
	closeErr := errors.New("injected root close")
	repo.hooks.closeRoot = func() error { return closeErr }
	before := openDescriptorCount(t)
	for range 20 {
		file, err := repo.openDirectory(child)
		if file != nil || !errors.Is(err, closeErr) {
			t.Fatalf("openDirectory = (%v, %v), want nil and close error", file, err)
		}
	}
	if got := openDescriptorCount(t); got > before+4 {
		t.Fatalf("open descriptors after failed openDirectory = %d, before = %d", got, before)
	}
}

func TestRootCloseErrorsJoinAndCloseRealRoot(t *testing.T) {
	repo := NewRepository(privateDir(t))
	if err := os.Mkdir(repo.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	closeErr := errors.New("injected root close")
	repo.hooks.closeRoot = func() error { return closeErr }
	root, err := repo.openRoot()
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.closeRoot(root); !errors.Is(err, closeErr) {
		t.Fatalf("closeRoot error = %v, want injected error", err)
	}
	if _, err := root.Stat("."); err == nil {
		t.Fatal("real root remained open after injected close error")
	}

	_, err = repo.stat(filepath.Join(repo.dir, "missing"))
	if !errors.Is(err, closeErr) || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stat error = %v, want primary and close errors", err)
	}
}

func TestRepositoryRejectsSymlinkedGenerationAndObjectShards(t *testing.T) {
	for _, target := range []string{repositoryGenerations, repositoryObjectsDir} {
		t.Run(target, func(t *testing.T) {
			repo := NewRepository(privateDir(t))
			publication := repositoryPublication(t, "named", 1, []byte("state"))
			if err := repo.Publish(context.Background(), publication); err != nil {
				t.Fatal(err)
			}
			key := legacyIncarnationID(publication.Name).String()
			inside := filepath.Join(repo.legacySessionPath(key), target)
			outside := t.TempDir()
			guard := filepath.Join(outside, "must-not-change")
			if err := os.WriteFile(guard, []byte("guard"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(inside, inside+"-real"); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, inside); err != nil {
				t.Fatal(err)
			}
			if _, err := repo.Load(context.Background(), publication.Name); err == nil {
				t.Fatal("Load succeeded through symlinked repository component")
			}
			if err := repo.Maintain(context.Background()); err == nil {
				t.Fatal("Maintain succeeded through symlinked repository component")
			}
			got, err := os.ReadFile(guard)
			if err != nil || string(got) != "guard" {
				t.Fatalf("outside guard = %q, %v", got, err)
			}
		})
	}
}

func TestFinalSymlinksRejectedByRootOperations(t *testing.T) {
	repo := NewRepository(privateDir(t))
	if err := repo.ensurePrivateDirectory(repo.dir); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(repo.dir, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(repo.dir, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.openDirectory(link); !errors.Is(err, syscall.ELOOP) || strings.Contains(err.Error(), repo.dir) {
		t.Fatalf("openDirectory error = %v, want sanitized ELOOP", err)
	}
	if _, err := repo.readBounded(link); !errors.Is(err, syscall.ELOOP) || strings.Contains(err.Error(), repo.dir) {
		t.Fatalf("readBounded error = %v, want sanitized ELOOP", err)
	}
}

func TestRepositoryLoadLegacyChargesUnrelatedRootEntries(t *testing.T) {
	repo := NewRepository(privateDir(t))
	if err := os.MkdirAll(repo.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	for i := 0; i <= maxDirectoryTraversalEntries; i++ {
		if err := os.WriteFile(filepath.Join(repo.dir, fmt.Sprintf("unrelated-%05d", i)), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := repo.LoadLegacy(context.Background()); !errors.Is(err, ErrDirectoryTraversalBudget) {
		t.Fatalf("LoadLegacy error = %v, want traversal budget error", err)
	}
}
