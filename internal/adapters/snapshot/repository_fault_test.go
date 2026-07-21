package snapshot

import (
	"context"
	"errors"
	"os"
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
	second := repositoryPublication(t, "named", 2, []byte("two"))
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

func TestRepositoryLoadFallsBackFromIncompleteNewestGeneration(t *testing.T) {
	dir := privateDir(t)
	repo := NewRepository(dir)
	first := repositoryPublication(t, "named", 1, []byte("one"))
	if err := repo.Publish(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	second := repositoryPublication(t, "named", 2, []byte("two"))
	if err := repo.Publish(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	manifest, err := codec.UnmarshalManifest(second.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(repo.objectPath(sessionKey("named"), manifest.Tabs[0].Panes[0].Tail.Digest)); err != nil {
		t.Fatal(err)
	}
	got, err := repo.Load(context.Background(), "named")
	if err != nil {
		t.Fatal(err)
	}
	if got.Generation != 1 || !got.Fallback {
		t.Fatalf("Load = generation %d fallback %v, want generation 1 fallback true", got.Generation, got.Fallback)
	}
}
