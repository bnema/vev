package dgram

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func privateDir(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "vev")
}

func TestKeyFingerprint(t *testing.T) {
	const want = "K7gNU3sdo+OL0wNhqoVWhr3g6s1xYv72ol/pe/Unols="
	if got := KeyFingerprint([]byte("secret")); got != want {
		t.Fatalf("KeyFingerprint()=%q, want %q", got, want)
	}
}

func TestProxyRegistryFileHoldsNoKeyMaterial(t *testing.T) {
	r := NewProxyRegistry(privateDir(t))
	key := []byte("raw-secret-key-material")
	fingerprint := KeyFingerprint(key)
	rec := ProxyRecord{Session: "work", PID: os.Getpid(), Port: 61000, KeyFingerprint: fingerprint}
	if err := r.Publish(rec); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(r.path("work"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, key) {
		t.Fatalf("registry file contains raw key material: %s", data)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	if _, ok := fields["key"]; ok {
		t.Fatalf("registry file contains raw key field: %s", data)
	}
	var got string
	if err := json.Unmarshal(fields["key_fingerprint"], &got); err != nil {
		t.Fatal(err)
	}
	if got != fingerprint {
		t.Fatalf("key_fingerprint=%q, want %q", got, fingerprint)
	}
}

func TestProxyRegistryReuseStaleCleanupAndSupersede(t *testing.T) {
	t.Run("reuses live matching record", func(t *testing.T) {
		r := NewProxyRegistry(privateDir(t))
		want := ProxyRecord{Session: "work", PID: os.Getpid(), Port: 61000, KeyFingerprint: "abc"}
		if err := r.Publish(want); err != nil {
			t.Fatal(err)
		}
		got, ok := r.Lookup("work")
		if !ok {
			t.Fatal("Lookup ok=false, want true")
		}
		if got.Port != want.Port || got.KeyFingerprint != want.KeyFingerprint || got.PID != want.PID {
			t.Fatalf("Lookup()=%+v, want %+v", got, want)
		}
	})
	t.Run("removes stale record", func(t *testing.T) {
		r := NewProxyRegistry(privateDir(t))
		if err := r.Publish(ProxyRecord{Session: "work", PID: os.Getpid(), Port: 61000, KeyFingerprint: "abc"}); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(r.path("work"), []byte(`{"session":"work","pid":-1,"port":61000,"key_fingerprint":"abc"}`), 0o600); err != nil {
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
		old := ProxyRecord{Session: "work", PID: os.Getpid(), Port: 61000, KeyFingerprint: "old"}
		if err := r.Publish(old); err != nil {
			t.Fatal(err)
		}
		newRec := ProxyRecord{Session: "work", PID: os.Getpid(), Port: 61001, KeyFingerprint: "new"}
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
		if got.Port != newRec.Port || got.KeyFingerprint != newRec.KeyFingerprint {
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
		first := ProxyRecord{Session: tt.a, PID: os.Getpid(), Port: 61000, KeyFingerprint: "first"}
		second := ProxyRecord{Session: tt.b, PID: os.Getpid(), Port: 61001, KeyFingerprint: "second"}
		if err := r.Publish(first); err != nil {
			t.Fatal(err)
		}
		if err := r.Publish(second); err != nil {
			t.Fatal(err)
		}
		gotFirst, ok := r.Lookup(tt.a)
		if !ok || gotFirst.KeyFingerprint != first.KeyFingerprint {
			t.Fatalf("Lookup(%q)=%+v ok=%v, want first record", tt.a, gotFirst, ok)
		}
		gotSecond, ok := r.Lookup(tt.b)
		if !ok || gotSecond.KeyFingerprint != second.KeyFingerprint {
			t.Fatalf("Lookup(%q)=%+v ok=%v, want second record", tt.b, gotSecond, ok)
		}
	}
}
