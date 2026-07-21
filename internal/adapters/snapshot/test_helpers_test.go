package snapshot

import (
	"path/filepath"
	"testing"
)

func privateDir(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "vev")
}
