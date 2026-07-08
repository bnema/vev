package safedir

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEnsurePrivate(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(t *testing.T, path string)
		wantErr string
	}{
		{
			name: "missing dir is created private",
		},
		{
			name: "existing private dir is accepted",
			setup: func(t *testing.T, path string) {
				t.Helper()
				if err := os.MkdirAll(path, 0o700); err != nil {
					t.Fatalf("mkdir private dir: %v", err)
				}
			},
		},
		{
			name: "group writable dir is rejected",
			setup: func(t *testing.T, path string) {
				t.Helper()
				if err := os.MkdirAll(path, 0o700); err != nil {
					t.Fatalf("mkdir group writable dir: %v", err)
				}
				if err := os.Chmod(path, 0o770); err != nil {
					t.Fatalf("chmod group writable dir: %v", err)
				}
			},
			wantErr: "permissions 0770, want 0700",
		},
		{
			name: "world writable dir is rejected",
			setup: func(t *testing.T, path string) {
				t.Helper()
				if err := os.MkdirAll(path, 0o700); err != nil {
					t.Fatalf("mkdir world writable dir: %v", err)
				}
				if err := os.Chmod(path, 0o707); err != nil {
					t.Fatalf("chmod world writable dir: %v", err)
				}
			},
			wantErr: "permissions 0707, want 0700",
		},
		{
			name: "world readable dir is rejected",
			setup: func(t *testing.T, path string) {
				t.Helper()
				if err := os.MkdirAll(path, 0o700); err != nil {
					t.Fatalf("mkdir world readable dir: %v", err)
				}
				if err := os.Chmod(path, 0o755); err != nil {
					t.Fatalf("chmod world readable dir: %v", err)
				}
			},
			wantErr: "permissions 0755, want 0700",
		},
		{
			name: "execute-only others dir is rejected",
			setup: func(t *testing.T, path string) {
				t.Helper()
				if err := os.MkdirAll(path, 0o700); err != nil {
					t.Fatalf("mkdir execute-only others dir: %v", err)
				}
				if err := os.Chmod(path, 0o711); err != nil {
					t.Fatalf("chmod execute-only others dir: %v", err)
				}
			},
			wantErr: "permissions 0711, want 0700",
		},
		{
			name: "owner read execute dir is rejected",
			setup: func(t *testing.T, path string) {
				t.Helper()
				if err := os.MkdirAll(path, 0o700); err != nil {
					t.Fatalf("mkdir owner read execute dir: %v", err)
				}
				if err := os.Chmod(path, 0o500); err != nil {
					t.Fatalf("chmod owner read execute dir: %v", err)
				}
			},
			wantErr: "permissions 0500, want 0700",
		},
		{
			name: "symlink is rejected",
			setup: func(t *testing.T, path string) {
				t.Helper()
				if err := os.Symlink(t.TempDir(), path); err != nil {
					t.Fatalf("symlink dir: %v", err)
				}
			},
			wantErr: "symlink",
		},
		{
			name: "regular file is rejected",
			setup: func(t *testing.T, path string) {
				t.Helper()
				if err := os.WriteFile(path, []byte("not a dir"), 0o600); err != nil {
					t.Fatalf("write file: %v", err)
				}
			},
			wantErr: "not a directory",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "private")
			if tt.setup != nil {
				tt.setup(t, path)
			}

			err := EnsurePrivate(path)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("EnsurePrivate() error = nil, want containing %q", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("EnsurePrivate() error = %q, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("EnsurePrivate() error = %v", err)
			}
			fi, err := os.Stat(path)
			if err != nil {
				t.Fatalf("stat private dir: %v", err)
			}
			if !fi.IsDir() {
				t.Fatalf("%s is not a directory", path)
			}
			if perm := fi.Mode().Perm(); perm != 0o700 {
				t.Fatalf("dir perm = %o, want 0700", perm)
			}
		})
	}
}

// A wrong-uid directory is not covered here because creating one requires
// elevated privileges; EnsurePrivate checks the uid returned by Lstat.
