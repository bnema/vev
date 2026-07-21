package snapshot

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/bnema/vev/internal/ports"
	codec "github.com/bnema/vev/internal/usecase/snapshot"
)

func TestRepositoryImmutableInstallDoesNotOverwriteRacedTarget(t *testing.T) {
	repo := NewRepository(privateDir(t))
	pub := repositoryPublication(t, "named", 1, []byte("state"))
	object := pub.Objects[0]
	target := repo.objectPath(sessionKey(pub.Name), object.Digest)
	repo.hooks.installImmutable = func(path string) error {
		if path == target {
			return os.WriteFile(path, []byte("attacker bytes"), 0o600)
		}
		return nil
	}
	if err := repo.Publish(context.Background(), pub); err == nil {
		t.Fatal("Publish succeeded after raced immutable target")
	}
	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "attacker bytes" {
		t.Fatalf("raced target = %q, want attacker bytes", got)
	}
}

func TestRepositoryFaultsKeepAnAuthoritativeGeneration(t *testing.T) {
	stages := []string{"create", "write", "sync-file", "close", "install", "rename", "sync-directory"}
	for _, stage := range stages {
		t.Run(stage, func(t *testing.T) {
			repo := NewRepository(privateDir(t))
			first := repositoryPublication(t, "named", 1, []byte("one"))
			if err := repo.Publish(context.Background(), first); err != nil {
				t.Fatal(err)
			}
			second := repositoryPublication(t, "named", 2, []byte("two"))
			injectPublicationStage(t, repo, second, stage)
			if err := repo.Publish(context.Background(), second); err == nil {
				t.Fatal("Publish succeeded with injected failure")
			}
			repo.hooks = repositoryHooks{}
			got, err := repo.Load(context.Background(), "named")
			if err != nil {
				t.Fatal(err)
			}
			if got.Generation != 1 && got.Generation != 2 {
				t.Fatalf("authoritative generation = %d, want old or new", got.Generation)
			}
		})
	}
}

// injectPublicationStage directs failures at the blob, manifest, or HEAD
// occurrence of a primitive. This keeps tests deterministic while exercising
// every publication primitive rather than just the first blob write.
func injectPublicationStage(t *testing.T, repo *Repository, pub ports.SnapshotPublication, stage string) {
	t.Helper()
	key := sessionKey(pub.Name)
	blobDir := filepath.Dir(repo.objectPath(key, pub.Objects[0].Digest))
	manifestDir := filepath.Dir(repo.manifestPath(key, pub.Generation))
	headDir := filepath.Dir(repo.headPath(key))
	fail := errors.New("injected persistence failure")
	if stage == "create" {
		repo.hooks.createTemp = func(dir string) error {
			if dir == blobDir {
				return fail
			}
			return nil
		}
		return
	}
	if stage == "write" || stage == "sync-file" || stage == "close" || stage == "install" {
		// Preinstall blobs so the selected failure is at manifest publication.
		for _, object := range pub.Objects {
			ref := manifestReference(t, pub.Manifest, object.Digest)
			if err := repo.writeImmutable(repo.objectPath(key, object.Digest), object.Data, func(data []byte) error {
				if !equalBytes(data, object.Data) || !validObject(data, ref) {
					return errors.New("invalid preinstalled object")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		}
		repo.hooks.writeTemp = func(path string) error {
			if stage == "write" && filepath.Dir(path) == manifestDir {
				return fail
			}
			return nil
		}
		repo.hooks.syncFile = func(path string) error {
			if stage == "sync-file" && filepath.Dir(path) == manifestDir {
				return fail
			}
			return nil
		}
		repo.hooks.closeFile = func(path string) error {
			if stage == "close" && filepath.Dir(path) == manifestDir {
				return fail
			}
			return nil
		}
		repo.hooks.installImmutable = func(path string) error {
			if stage == "install" && filepath.Dir(path) == manifestDir {
				return fail
			}
			return nil
		}
		return
	}
	if stage == "rename" {
		// Preinstall immutable data and the manifest so Publish reaches HEAD.
		prepareHeadStage(t, repo, pub)
		repo.hooks.rename = func(path string) error {
			if path == repo.headPath(key) {
				return fail
			}
			return nil
		}
		return
	}
	if stage == "sync-directory" {
		prepareHeadStage(t, repo, pub)
		repo.hooks.syncDirectory = func(dir string) error {
			if dir == headDir {
				return fail
			}
			return nil
		}
	}
}

func prepareHeadStage(t *testing.T, repo *Repository, pub ports.SnapshotPublication) {
	t.Helper()
	key := sessionKey(pub.Name)
	for _, object := range pub.Objects {
		ref := manifestReference(t, pub.Manifest, object.Digest)
		if err := repo.writeImmutable(repo.objectPath(key, object.Digest), object.Data, func(data []byte) error {
			if !equalBytes(data, object.Data) || !validObject(data, ref) {
				return errors.New("invalid object")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := repo.writeImmutable(repo.manifestPath(key, pub.Generation), pub.Manifest, func(data []byte) error {
		if !equalBytes(data, pub.Manifest) {
			return errors.New("invalid manifest")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func manifestReference(t *testing.T, data []byte, digest ports.SnapshotDigest) codec.ObjectRef {
	t.Helper()
	manifest, err := codec.UnmarshalManifest(data)
	if err != nil {
		t.Fatal(err)
	}
	ref, ok := manifestRefs(manifest)[digest]
	if !ok {
		t.Fatal("missing object reference")
	}
	return ref
}

func TestRepositoryFaultsAtEveryPublicationBoundary(t *testing.T) {
	cases := []struct {
		location  string
		operation string
	}{
		{"blob", "create"}, {"blob", "write"}, {"blob", "sync-file"}, {"blob", "close"}, {"blob", "install"}, {"blob", "sync-directory"},
		{"manifest", "create"}, {"manifest", "write"}, {"manifest", "sync-file"}, {"manifest", "close"}, {"manifest", "install"}, {"manifest", "sync-directory"},
		{"HEAD", "create"}, {"HEAD", "write"}, {"HEAD", "sync-file"}, {"HEAD", "close"}, {"HEAD", "rename"}, {"HEAD", "sync-directory"},
	}
	for _, tc := range cases {
		t.Run(tc.location+"/"+tc.operation, func(t *testing.T) {
			repo := NewRepository(privateDir(t))
			first := repositoryPublication(t, "named", 1, []byte("one"))
			if err := repo.Publish(context.Background(), first); err != nil {
				t.Fatal(err)
			}
			second := repositoryPublication(t, "named", 2, []byte("two"))
			key := sessionKey(second.Name)
			if err := repo.ensureSession(key); err != nil {
				t.Fatal(err)
			}
			if tc.location == "manifest" {
				prepareObjects(t, repo, second)
			}
			if tc.location == "HEAD" {
				prepareHeadStage(t, repo, second)
			}
			injectBoundary(repo, tc.location, tc.operation, second)
			if err := repo.Publish(context.Background(), second); err == nil {
				t.Fatal("Publish succeeded with injected persistence failure")
			}
			repo.hooks = repositoryHooks{}
			got, err := repo.Load(context.Background(), "named")
			if err != nil {
				t.Fatal(err)
			}
			if got.Generation != 1 && got.Generation != 2 {
				t.Fatalf("authoritative generation = %d, want old or new", got.Generation)
			}
		})
	}
}

func injectBoundary(repo *Repository, location, operation string, pub ports.SnapshotPublication) {
	key := sessionKey(pub.Name)
	dir := filepath.Dir(repo.objectPath(key, pub.Objects[0].Digest))
	if location == "manifest" {
		dir = filepath.Dir(repo.manifestPath(key, pub.Generation))
	}
	if location == "HEAD" {
		dir = filepath.Dir(repo.headPath(key))
	}
	fail := errors.New("injected persistence failure")
	repo.hooks.createTemp = func(got string) error {
		if operation == "create" && got == dir {
			return fail
		}
		return nil
	}
	repo.hooks.writeTemp = func(path string) error {
		if operation == "write" && filepath.Dir(path) == dir {
			return fail
		}
		return nil
	}
	repo.hooks.syncFile = func(path string) error {
		if operation == "sync-file" && filepath.Dir(path) == dir {
			return fail
		}
		return nil
	}
	repo.hooks.closeFile = func(path string) error {
		if operation == "close" && filepath.Dir(path) == dir {
			return fail
		}
		return nil
	}
	repo.hooks.installImmutable = func(path string) error {
		if operation == "install" && filepath.Dir(path) == dir {
			return fail
		}
		return nil
	}
	repo.hooks.rename = func(path string) error {
		if operation == "rename" && location == "HEAD" && path == repo.headPath(key) {
			return fail
		}
		return nil
	}
	repo.hooks.syncDirectory = func(got string) error {
		if operation == "sync-directory" && got == dir {
			return fail
		}
		return nil
	}
}

func prepareObjects(t *testing.T, repo *Repository, pub ports.SnapshotPublication) {
	t.Helper()
	key := sessionKey(pub.Name)
	for _, object := range pub.Objects {
		ref := manifestReference(t, pub.Manifest, object.Digest)
		if err := repo.writeImmutable(repo.objectPath(key, object.Digest), object.Data, func(data []byte) error {
			if !equalBytes(data, object.Data) || !validObject(data, ref) {
				return errors.New("invalid object")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRepositoryCleanupAndDeletionSyncErrorsSurface(t *testing.T) {
	repo := NewRepository(privateDir(t))
	pub := repositoryPublication(t, "named", 1, []byte("state"))
	cleanupErr := errors.New("cleanup failed")
	repo.hooks.writeTemp = func(string) error { return errors.New("write failed") }
	repo.hooks.remove = func(string) error { return cleanupErr }
	if err := repo.Publish(context.Background(), pub); !errors.Is(err, cleanupErr) {
		t.Fatalf("Publish error = %v, want cleanup error", err)
	}
	repo.hooks = repositoryHooks{}
	if err := repo.Publish(context.Background(), pub); err != nil {
		t.Fatal(err)
	}
	repo.hooks.syncDirectory = func(dir string) error {
		if dir == filepath.Join(repo.dir, repositorySessionsDir) {
			return errors.New("delete sync failed")
		}
		return nil
	}
	if err := repo.Delete(context.Background(), "named"); err == nil {
		t.Fatal("Delete succeeded with directory sync failure")
	}
}
