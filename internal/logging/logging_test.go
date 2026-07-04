package logging

import (
	"encoding/json"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestParseLevelAndEnvLevel(t *testing.T) {
	tests := []struct {
		in   string
		want slog.Level
	}{
		{"debug", slog.LevelDebug},
		{" DEBUG ", slog.LevelDebug},
		{"info", slog.LevelInfo},
		{"", slog.LevelInfo},
		{"warn", slog.LevelWarn},
		{"warning", slog.LevelWarn},
		{"error", slog.LevelError},
		{"bogus", slog.LevelInfo},
	}
	for _, tt := range tests {
		if got := ParseLevel(tt.in); got != tt.want {
			t.Fatalf("ParseLevel(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}

	t.Setenv("VEV_LOG", "debug")
	if got := EnvLevel(); got != slog.LevelDebug {
		t.Fatalf("EnvLevel() = %v, want %v", got, slog.LevelDebug)
	}
}

func TestSetupWritesJSONWithComponent(t *testing.T) {
	dir := t.TempDir()
	logger, closer, err := Setup(Config{Dir: dir, Component: Client, Level: slog.LevelDebug})
	if err != nil {
		t.Fatalf("Setup() error = %v", err)
	}
	defer func() {
		if err := closer.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	}()

	logger.Info("hello", "answer", 42)

	data, err := os.ReadFile(filepath.Join(dir, "vev-client.log"))
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	var entry map[string]any
	if err := json.Unmarshal(data, &entry); err != nil {
		t.Fatalf("log entry is not JSON: %v; data=%q", err, data)
	}
	if entry["component"] != "client" {
		t.Fatalf("component = %v, want client; entry=%v", entry["component"], entry)
	}
	if entry["msg"] != "hello" || entry["answer"] != float64(42) {
		t.Fatalf("unexpected entry fields: %v", entry)
	}
	if _, err := os.Stat(filepath.Join(dir, "vev-client-crash.log")); err != nil {
		t.Fatalf("crash log not created: %v", err)
	}
}

func TestRuntimeRotation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "vev-daemon.log")
	w, err := newRotatingWriter(path, 10, true)
	if err != nil {
		t.Fatalf("newRotatingWriter() error = %v", err)
	}
	defer func() {
		if err := w.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	}()

	if _, err := w.Write([]byte("0123456789")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if _, err := os.Stat(path + ".old"); err != nil {
		t.Fatalf("rotated file missing: %v", err)
	}
	live, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read live file: %v", err)
	}
	if len(live) != 0 {
		t.Fatalf("live file length = %d, want 0 after rotation", len(live))
	}
	old, err := os.ReadFile(path + ".old")
	if err != nil {
		t.Fatalf("read old file: %v", err)
	}
	if string(old) != "0123456789" {
		t.Fatalf("old file = %q, want original write", old)
	}
}

func TestStartupOnlyRotation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "vev-client.log")
	if err := os.WriteFile(path, []byte("already-too-large"), 0o600); err != nil {
		t.Fatalf("seed log: %v", err)
	}
	w, err := newRotatingWriter(path, 5, false)
	if err != nil {
		t.Fatalf("newRotatingWriter() error = %v", err)
	}
	defer func() {
		if err := w.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	}()

	if old, err := os.ReadFile(path + ".old"); err != nil || string(old) != "already-too-large" {
		t.Fatalf("startup rotation old = %q, err = %v", old, err)
	}
	if _, err := w.Write([]byte("first")); err != nil {
		t.Fatalf("Write(first) error = %v", err)
	}
	if _, err := w.Write([]byte("second")); err != nil {
		t.Fatalf("Write(second) error = %v", err)
	}
	if old, err := os.ReadFile(path + ".old"); err != nil || string(old) != "already-too-large" {
		t.Fatalf("startup-only mode rotated during writes; old = %q, err = %v", old, err)
	}
	live, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read live file: %v", err)
	}
	if string(live) != "firstsecond" {
		t.Fatalf("live file = %q, want appended writes", live)
	}
}

func TestCrashOutputCapturesPanicStack(t *testing.T) {
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=TestCrashOutputHelper")
	cmd.Env = append(os.Environ(), "VEV_CRASH_HELPER=1", "VEV_CRASH_DIR="+dir)
	if err := cmd.Run(); err == nil {
		t.Fatal("crash helper exited successfully, want panic failure")
	}

	data, err := os.ReadFile(filepath.Join(dir, "vev-daemon-crash.log"))
	if err != nil {
		t.Fatalf("read crash log: %v", err)
	}
	if !strings.Contains(string(data), "intentional crash for SetCrashOutput test") || !strings.Contains(string(data), "goroutine") {
		t.Fatalf("crash log did not contain panic stack; data=%q", data)
	}
}

func TestCrashOutputHelper(t *testing.T) {
	if os.Getenv("VEV_CRASH_HELPER") != "1" {
		return
	}
	if _, _, err := Setup(Config{Dir: os.Getenv("VEV_CRASH_DIR"), Component: Daemon, Level: slog.LevelInfo}); err != nil {
		panic(err)
	}
	panic("intentional crash for SetCrashOutput test")
}

func TestStartupOnlyConcurrentRotationRaceIsIgnored(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "vev-client.log")
	if err := os.WriteFile(path, []byte("already-too-large"), 0o600); err != nil {
		t.Fatalf("seed log: %v", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 8)
	for i := 0; i < cap(errCh); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w, err := newRotatingWriter(path, 5, false)
			if err != nil {
				errCh <- err
				return
			}
			if _, err := w.Write([]byte("x")); err != nil {
				errCh <- err
			}
			if err := w.Close(); err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent startup writer failed: %v", err)
		}
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("live log missing after concurrent startup rotation: %v", err)
	}
	if _, err := os.Stat(path + ".old"); err != nil {
		t.Fatalf("rotated log missing after concurrent startup rotation: %v", err)
	}
}

func TestDoubleClose(t *testing.T) {
	_, closer, err := Setup(Config{Dir: t.TempDir(), Component: Stdio, Level: slog.LevelInfo})
	if err != nil {
		t.Fatalf("Setup() error = %v", err)
	}
	if err := closer.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	if err := closer.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}

func TestRotationRenameFailureIsNonFatal(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "vev-daemon.log")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("seed log: %v", err)
	}
	if err := os.Mkdir(path+".old", 0o700); err != nil {
		t.Fatalf("make rename obstacle: %v", err)
	}
	if err := os.WriteFile(filepath.Join(path+".old", "child"), []byte("x"), 0o600); err != nil {
		t.Fatalf("make non-empty obstacle: %v", err)
	}

	w, err := newRotatingWriter(path, 4, true)
	if err != nil {
		t.Fatalf("newRotatingWriter() error = %v", err)
	}
	defer func() {
		if err := w.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	}()

	if _, err := w.Write([]byte("exceeds")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	live, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read live file: %v", err)
	}
	if !strings.Contains(string(live), "exceeds") {
		t.Fatalf("write was not preserved after failed rename: %q", live)
	}
}
