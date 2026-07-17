package app

import (
	"strings"
	"testing"
)

// The release pipeline stamps these vars via -ldflags -X; the defaults must
// stay meaningful for plain `go build`.
func TestVersionDefaults(t *testing.T) {
	if version == "" || commit == "" || date == "" {
		t.Fatalf("version metadata must have non-empty defaults: version=%q commit=%q date=%q", version, commit, date)
	}
	if !strings.Contains(version, "dev") {
		t.Fatalf("unstamped version %q should carry a -dev marker", version)
	}
}

func TestVersionLine(t *testing.T) {
	want := "vev " + version + " (commit " + commit + ", built " + date + ")"
	if got := versionLine(); got != want {
		t.Fatalf("versionLine() = %q, want %q", got, want)
	}
}
