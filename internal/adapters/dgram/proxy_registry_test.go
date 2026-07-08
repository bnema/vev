package dgram

import (
	"os"
	"path/filepath"
	"testing"
)

func privateDir(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "vev")
}

func TestProxyRegistryReuseStaleCleanupAndSupersede(t *testing.T) {
	t.Run("reuses live matching record", func(t *testing.T) {
		r := NewProxyRegistry(privateDir(t))
		want := ProxyRecord{Session: "work", PID: os.Getpid(), Port: 61000, Key: "abc"}
		if err := r.Publish(want); err != nil {
			t.Fatal(err)
		}
		got, ok := r.Lookup("work")
		if !ok {
			t.Fatal("Lookup ok=false, want true")
		}
		if got.Port != want.Port || got.Key != want.Key || got.PID != want.PID {
			t.Fatalf("Lookup()=%+v, want %+v", got, want)
		}
	})
	t.Run("removes stale record", func(t *testing.T) {
		r := NewProxyRegistry(privateDir(t))
		if err := r.Publish(ProxyRecord{Session: "work", PID: os.Getpid(), Port: 61000, Key: "abc"}); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(r.path("work"), []byte(`{"session":"work","pid":-1,"port":61000,"key":"abc"}`), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, ok := r.Lookup("work"); ok {
			t.Fatal("Lookup ok=true for stale pid")
		}
		if _, err := os.Stat(r.path("work")); !os.IsNotExist(err) {
			t.Fatalf("stale record stat err=%v, want not exist", err)
		}
	})
	t.Run("supersedes without pid-based kill and cleanup is owned", func(t *testing.T) {
		r := NewProxyRegistry(privateDir(t))
		old := ProxyRecord{Session: "work", PID: os.Getpid(), Port: 61000, Key: "old"}
		if err := r.Publish(old); err != nil {
			t.Fatal(err)
		}
		newRec := ProxyRecord{Session: "work", PID: os.Getpid(), Port: 61001, Key: "new"}
		if err := r.Publish(newRec); err != nil {
			t.Fatal(err)
		}
		if err := r.RemoveOwned(old); err != nil {
			t.Fatal(err)
		}
		got, ok := r.Lookup("work")
		if !ok {
			t.Fatal("new live record removed by stale owner")
		}
		if got.Port != newRec.Port || got.Key != newRec.Key {
			t.Fatalf("Lookup()=%+v, want %+v", got, newRec)
		}
		if err := r.RemoveOwned(newRec); err != nil {
			t.Fatal(err)
		}
		if _, ok := r.Lookup("work"); ok {
			t.Fatal("owned remove left registry record")
		}
	})
}

func TestProxyRegistrySessionPathIsCollisionFree(t *testing.T) {
	r := NewProxyRegistry(privateDir(t))
	tests := []struct {
		a string
		b string
	}{
		{a: "", b: "default"},
		{a: "a/b", b: "a_b"},
		{a: `a\b`, b: "a_b"},
	}
	for _, tt := range tests {
		if r.path(tt.a) == r.path(tt.b) {
			t.Fatalf("path(%q) collides with path(%q): %s", tt.a, tt.b, r.path(tt.a))
		}
		first := ProxyRecord{Session: tt.a, PID: os.Getpid(), Port: 61000, Key: "first"}
		second := ProxyRecord{Session: tt.b, PID: os.Getpid(), Port: 61001, Key: "second"}
		if err := r.Publish(first); err != nil {
			t.Fatal(err)
		}
		if err := r.Publish(second); err != nil {
			t.Fatal(err)
		}
		gotFirst, ok := r.Lookup(tt.a)
		if !ok || gotFirst.Key != first.Key {
			t.Fatalf("Lookup(%q)=%+v ok=%v, want first record", tt.a, gotFirst, ok)
		}
		gotSecond, ok := r.Lookup(tt.b)
		if !ok || gotSecond.Key != second.Key {
			t.Fatalf("Lookup(%q)=%+v ok=%v, want second record", tt.b, gotSecond, ok)
		}
	}
}
