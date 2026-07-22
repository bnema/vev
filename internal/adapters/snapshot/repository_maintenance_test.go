package snapshot

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSafeFilesystemErrorDoesNotExposePath(t *testing.T) {
	cause := errors.New("permission denied")
	err := safeFilesystemError(&os.PathError{Op: "remove", Path: "\x1b[31munsafe", Err: cause})
	if !errors.Is(err, cause) || strings.Contains(err.Error(), "\x1b[31munsafe") {
		t.Fatalf("safe filesystem error = %q", err)
	}
}

func TestSafeFilesystemErrorDoesNotExposeLinkErrorPath(t *testing.T) {
	err := safeFilesystemError(&os.LinkError{Op: "rename", Old: "/unsafe/old", New: "/unsafe/new", Err: os.ErrExist})
	if !errors.Is(err, os.ErrExist) || strings.Contains(err.Error(), "/unsafe/") {
		t.Fatalf("safe link error = %q", err)
	}
}

func TestRepositoryMaintenanceFilesystemErrorsHaveSafeContext(t *testing.T) {
	injected := errors.New("injected filesystem failure")
	cases := []struct {
		name string
		run  func(*Repository) error
		want string
	}{
		{
			name: "delete sync",
			run: func(repo *Repository) error {
				publication := repositoryPublication(t, "named", 1, []byte("state"))
				if err := repo.Publish(context.Background(), publication); err != nil {
					t.Fatal(err)
				}
				repo.hooks.syncDirectory = func(string) error { return injected }
				return repo.Delete(context.Background(), "named")
			},
			want: `sync deleted snapshot session directory "named"`,
		},
		{
			name: "maintain remove",
			run: func(repo *Repository) error {
				if err := os.Mkdir(repo.dir, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(repo.dir, ".tmp-stale"), []byte("stale"), 0o600); err != nil {
					t.Fatal(err)
				}
				repo.hooks.remove = func(string) error { return injected }
				return repo.Maintain(context.Background())
			},
			want: `remove snapshot maintenance file ".tmp-stale"`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.run(NewRepository(privateDir(t)))
			if !errors.Is(err, injected) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want context %q and injected error", err, tc.want)
			}
		})
	}
}
