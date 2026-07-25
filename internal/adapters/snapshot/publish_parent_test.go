package snapshot

import (
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"

	"github.com/bnema/vev/internal/domain"
)

func TestRepositoryPublishAcceptsGenerationOneWithoutParent(t *testing.T) {
	repo := NewRepository(privateDir(t))
	publication := repositoryPublication(t, "named", 1, []byte("one"))

	if err := repo.Publish(context.Background(), publication); err != nil {
		t.Fatalf("Publish generation 1 without parent: %v", err)
	}
}

func TestRepositoryPublishRejectsInvalidLaterParentWithoutMutation(t *testing.T) {
	tests := []struct {
		name   string
		parent func(domain.CheckpointRef) *domain.CheckpointRef
	}{
		{name: "nil", parent: func(domain.CheckpointRef) *domain.CheckpointRef { return nil }},
		{name: "stale generation", parent: func(current domain.CheckpointRef) *domain.CheckpointRef {
			current.Generation--
			return &current
		}},
		{name: "mismatched digest", parent: func(current domain.CheckpointRef) *domain.CheckpointRef {
			current.ManifestDigest[0]++
			return &current
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repo := NewRepository(privateDir(t))
			first := repositoryPublication(t, "named", 1, []byte("one"))
			if err := repo.Publish(context.Background(), first); err != nil {
				t.Fatalf("Publish generation 1: %v", err)
			}
			firstRef := domain.CheckpointRef{Generation: 1, ManifestDigest: sha256.Sum256(first.Manifest)}
			second := incarnationPublication(t, first.IncarnationID, first.Name, 2, &firstRef)
			if err := repo.Publish(context.Background(), second); err != nil {
				t.Fatalf("Publish generation 2: %v", err)
			}
			current := domain.CheckpointRef{Generation: 2, ManifestDigest: sha256.Sum256(second.Manifest)}
			third := incarnationPublication(t, first.IncarnationID, first.Name, 3, test.parent(current))
			before := snapshotRepositoryFiles(t, repo.dir)

			if err := repo.Publish(context.Background(), third); err == nil {
				t.Fatal("Publish accepted invalid parent")
			}
			after := snapshotRepositoryFiles(t, repo.dir)
			if len(after) != len(before) {
				t.Fatalf("repository file count changed: before=%d after=%d", len(before), len(after))
			}
			for path, data := range before {
				if string(after[path]) != string(data) {
					t.Fatalf("repository file %q mutated", path)
				}
			}
		})
	}
}

func TestRepositoryPublishAcceptsExactCurrentParent(t *testing.T) {
	repo := NewRepository(privateDir(t))
	first := repositoryPublication(t, "named", 1, []byte("one"))
	if err := repo.Publish(context.Background(), first); err != nil {
		t.Fatalf("Publish generation 1: %v", err)
	}
	parent := &domain.CheckpointRef{Generation: 1, ManifestDigest: sha256.Sum256(first.Manifest)}
	second := incarnationPublication(t, first.IncarnationID, first.Name, 2, parent)

	if err := repo.Publish(context.Background(), second); err != nil {
		t.Fatalf("Publish generation 2 with exact parent: %v", err)
	}
}

func snapshotRepositoryFiles(t *testing.T, root string) map[string][]byte {
	t.Helper()
	files := make(map[string][]byte)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		files[relative] = data
		return nil
	})
	if err != nil {
		t.Fatalf("snapshot repository files: %v", err)
	}
	return files
}
