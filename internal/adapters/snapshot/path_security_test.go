package snapshot

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/bnema/vev/internal/domain"
	codec "github.com/bnema/vev/internal/usecase/snapshot"
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

	if _, err := loadPublication(context.Background(), repo, publication); err == nil {
		t.Fatal("LoadCheckpoint succeeded through replaced configured-root symlink")
	}
	keep := map[domain.IncarnationID]domain.CheckpointRef{publication.IncarnationID: {Generation: publication.Generation, ManifestDigest: codec.ManifestDigest(publication.Manifest)}}
	if err := repo.CollectGarbage(context.Background(), keep); err == nil {
		t.Fatal("CollectGarbage succeeded through replaced configured-root symlink")
	}
	loaded, err := loadPublication(context.Background(), external, externalPublication)
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

func TestRepositoryRejectsSymlinkedGenerationAndObjectShards(t *testing.T) {
	for _, target := range []string{repositoryGenerations, repositoryObjectsDir} {
		t.Run(target, func(t *testing.T) {
			repo := NewRepository(privateDir(t))
			publication := repositoryPublication(t, "named", 1, []byte("state"))
			if err := repo.Publish(context.Background(), publication); err != nil {
				t.Fatal(err)
			}
			inside := filepath.Join(repo.sessionPath(publication.IncarnationID), target)
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
			if _, err := loadPublication(context.Background(), repo, publication); err == nil {
				t.Fatal("LoadCheckpoint succeeded through symlinked repository component")
			}
			keep := map[domain.IncarnationID]domain.CheckpointRef{publication.IncarnationID: {Generation: publication.Generation, ManifestDigest: codec.ManifestDigest(publication.Manifest)}}
			if err := repo.CollectGarbage(context.Background(), keep); err == nil {
				t.Fatal("CollectGarbage succeeded through symlinked repository component")
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
