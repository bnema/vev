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

func prepareHeadStage(t *testing.T, repo *Repository, pub ports.SnapshotPublication) {
	t.Helper()
	key := sessionKey(pub.Name)
	prepareObjects(t, repo, pub)
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
			preparePublicationLocation(t, repo, second, tc.location)
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

// preparePublicationLocation advances setup only far enough to target the
// requested persistence location in the publication fault matrix.
func preparePublicationLocation(t *testing.T, repo *Repository, pub ports.SnapshotPublication, location string) {
	t.Helper()
	switch location {
	case "manifest":
		prepareObjects(t, repo, pub)
	case "HEAD":
		prepareHeadStage(t, repo, pub)
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
