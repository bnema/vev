package remote

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/bnema/vev/internal/adapters/sshstdio"
	"github.com/bnema/vev/internal/ports"
)

func TestRemoteCatalogClientCommandConstruction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
	}{
		{name: "plain host", target: "arch"},
		{name: "metacharacters stay single local arg", target: "user@host;touch/tmp/pwn"},
		{name: "user at host", target: "build@mule"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var gotPath string
			var gotArgs []string
			client := &CatalogClient{
				command: func(ctx context.Context, name string, args ...string) *exec.Cmd {
					gotPath = name
					gotArgs = append([]string(nil), args...)
					return stdoutCmd(ctx, mustCatalogJSON(t, ports.RemoteCatalog{
						ProtocolVersion: ports.ProtocolVersion,
						SchemaVersion:   ports.RemoteCatalogSchemaVersion,
						Sessions:        []ports.RemoteCatalogSession{},
					}))
				},
			}

			if _, err := client.List(context.Background(), tt.target); err != nil {
				t.Fatalf("List() error = %v", err)
			}

			want := sshstdio.BuildCommandForRemoteCommand(tt.target, "vev", "cmd", "remote-catalog", "--json")
			if gotPath != want.Path {
				t.Fatalf("Path = %q, want %q", gotPath, want.Path)
			}
			if len(gotArgs) != len(want.Args) {
				t.Fatalf("Args len = %d, want %d (%q)", len(gotArgs), len(want.Args), gotArgs)
			}
			for i := range want.Args {
				if gotArgs[i] != want.Args[i] {
					t.Fatalf("Args[%d] = %q, want %q (all %q)", i, gotArgs[i], want.Args[i], gotArgs)
				}
			}
			if gotArgs[0] != "--" || gotArgs[1] != tt.target {
				t.Fatalf("target must remain one unquoted local argv word after --; got %q", gotArgs)
			}
			if !strings.Contains(gotArgs[2], "'vev'") || !strings.Contains(gotArgs[2], "'remote-catalog'") {
				t.Fatalf("remote command words must be shell-quoted; got %q", gotArgs[2])
			}
		})
	}
}

func TestRemoteCatalogClientRejectsUnsafeTarget(t *testing.T) {
	t.Parallel()
	client := NewCatalogClient()
	_, err := client.List(context.Background(), "bad host")
	if err == nil {
		t.Fatal("List() error = nil, want target validation error")
	}
}

func TestRemoteCatalogClientDecode(t *testing.T) {
	t.Parallel()

	valid := ports.RemoteCatalog{
		ProtocolVersion: ports.ProtocolVersion,
		SchemaVersion:   ports.RemoteCatalogSchemaVersion,
		Sessions: []ports.RemoteCatalogSession{
			{LifecycleID: [16]byte{1}, Name: "work", State: "up", Ephemeral: false, Tabs: []ports.RemoteCatalogTab{{ID: "tab-1"}, {ID: "tab-2", Index: 1}}, Attached: true},
		},
	}
	validJSON := mustCatalogJSON(t, valid)

	tests := []struct {
		name    string
		stdout  string
		stderr  string
		exit    int
		want    ports.RemoteCatalog
		wantErr error
		check   func(t *testing.T, err error)
	}{
		{
			name:   "matching protocol version",
			stdout: validJSON,
			want:   valid,
		},
		{
			name:   "trailing whitespace accepted",
			stdout: validJSON + " \n\t",
			want:   valid,
		},
		{
			name:   "mismatching protocol version",
			stdout: mustCatalogJSON(t, ports.RemoteCatalog{ProtocolVersion: ports.ProtocolVersion + 1, SchemaVersion: ports.RemoteCatalogSchemaVersion, Sessions: []ports.RemoteCatalogSession{}}),
			check: func(t *testing.T, err error) {
				t.Helper()
				var mismatch *ports.RemoteCatalogVersionMismatchError
				if !errors.As(err, &mismatch) {
					t.Fatalf("error = %v, want RemoteCatalogVersionMismatchError", err)
				}
				if mismatch.Got != ports.ProtocolVersion+1 || mismatch.Want != ports.ProtocolVersion {
					t.Fatalf("mismatch = %#v", mismatch)
				}
			},
		},
		{
			name:    "malformed json",
			stdout:  `{"protocol_version":`,
			wantErr: errCatalogDecode,
		},
		{
			name:    "truncated json",
			stdout:  `{"protocol_version":19,"sessions":[`,
			wantErr: errCatalogDecode,
		},
		{
			name: "count-only tabs rejected",
			stdout: fmt.Sprintf(`{"protocol_version":%d,"schema_version":%d,"sessions":[{"lifecycle_id":"01000000000000000000000000000000","name":"work","state":"up","ephemeral":false,"tabs":1,"attached":false}]}`,
				ports.ProtocolVersion, ports.RemoteCatalogSchemaVersion),
			wantErr: errCatalogDecode,
		},
		{
			name:    "trailing non-whitespace",
			stdout:  strings.TrimSpace(validJSON) + `{"extra":true}`,
			wantErr: errCatalogTrailing,
		},
		{
			name:    "missing protocol_version",
			stdout:  `{"sessions":[]}` + "\n",
			wantErr: errCatalogRequired,
		},
		{
			name:   "missing catalogue schema",
			stdout: fmt.Sprintf(`{"protocol_version":%d,"sessions":[]}`+"\n", ports.ProtocolVersion),
			check: func(t *testing.T, err error) {
				t.Helper()
				var mismatch *ports.RemoteCatalogVersionMismatchError
				if !errors.As(err, &mismatch) || mismatch.Kind != "catalog" || mismatch.Got != 0 || mismatch.Want != ports.RemoteCatalogSchemaVersion {
					t.Fatalf("error = %v, want missing catalogue version mismatch", err)
				}
			},
		},
		{
			name:    "missing sessions",
			stdout:  fmt.Sprintf(`{"protocol_version":%d,"schema_version":%d}`+"\n", ports.ProtocolVersion, ports.RemoteCatalogSchemaVersion),
			wantErr: errCatalogRequired,
		},
		{
			name: "invalid session name",
			stdout: mustCatalogJSON(t, ports.RemoteCatalog{ProtocolVersion: ports.ProtocolVersion, SchemaVersion: ports.RemoteCatalogSchemaVersion, Sessions: []ports.RemoteCatalogSession{{
				LifecycleID: [16]byte{1}, Name: "bad name", State: "up", Tabs: []ports.RemoteCatalogTab{},
			}}}),
			wantErr: ports.ErrInvalidRemoteCatalog,
		},
		{
			name: "invalid session state",
			stdout: mustCatalogJSON(t, ports.RemoteCatalog{ProtocolVersion: ports.ProtocolVersion, SchemaVersion: ports.RemoteCatalogSchemaVersion, Sessions: []ports.RemoteCatalogSession{{
				LifecycleID: [16]byte{1}, Name: "work", State: "weird", Tabs: []ports.RemoteCatalogTab{},
			}}}),
			wantErr: ports.ErrRemoteCatalogUnknownState,
		},
		{
			name:    "remote exit with stderr",
			stdout:  "",
			stderr:  "connection refused",
			exit:    255,
			wantErr: errCatalogSSH,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			client := &CatalogClient{
				command: func(ctx context.Context, name string, args ...string) *exec.Cmd {
					if tt.exit != 0 {
						cmd := exec.CommandContext(ctx, "sh", "-c", fmt.Sprintf("cat >&2; exit %d", tt.exit))
						cmd.Stdin = strings.NewReader(tt.stderr)
						return cmd
					}
					return stdoutCmd(ctx, tt.stdout)
				},
			}
			got, err := client.List(context.Background(), "arch")
			if tt.check != nil {
				if err == nil {
					t.Fatal("List() error = nil, want error")
				}
				tt.check(t, err)
				return
			}
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("List() error = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("List() error = %v", err)
			}
			if got.ProtocolVersion != tt.want.ProtocolVersion {
				t.Fatalf("ProtocolVersion = %d, want %d", got.ProtocolVersion, tt.want.ProtocolVersion)
			}
			if len(got.Sessions) != len(tt.want.Sessions) {
				t.Fatalf("Sessions len = %d, want %d", len(got.Sessions), len(tt.want.Sessions))
			}
			for i := range tt.want.Sessions {
				if !reflect.DeepEqual(got.Sessions[i], tt.want.Sessions[i]) {
					t.Fatalf("Sessions[%d] = %#v, want %#v", i, got.Sessions[i], tt.want.Sessions[i])
				}
			}
		})
	}
}

func TestRemoteCatalogClientContextCanceled(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client := NewCatalogClient()
	_, err := client.List(ctx, "arch")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("List() error = %v, want context.Canceled", err)
	}
}

func TestRemoteCatalogClientAppliesLifetimeAndWaitDelay(t *testing.T) {
	t.Parallel()

	var commandCtx context.Context
	var cmd *exec.Cmd
	client := &CatalogClient{
		command: func(ctx context.Context, name string, args ...string) *exec.Cmd {
			commandCtx = ctx
			cmd = stdoutCmd(ctx, mustCatalogJSON(t, ports.RemoteCatalog{
				ProtocolVersion: ports.ProtocolVersion,
				SchemaVersion:   ports.RemoteCatalogSchemaVersion,
				Sessions:        []ports.RemoteCatalogSession{},
			}))
			return cmd
		},
	}
	if _, err := client.List(context.Background(), "arch"); err != nil {
		t.Fatalf("List() error = %v", err)
	}
	deadline, ok := commandCtx.Deadline()
	if !ok {
		t.Fatal("command context missing default lifetime deadline")
	}
	if remaining := time.Until(deadline); remaining <= 0 || remaining > catalogCommandTimeout {
		t.Fatalf("deadline remaining = %v, want within (0, %v]", remaining, catalogCommandTimeout)
	}
	if cmd.WaitDelay != catalogCommandWaitDelay {
		t.Fatalf("WaitDelay = %v, want %v", cmd.WaitDelay, catalogCommandWaitDelay)
	}
}

func TestRemoteCatalogClientClassifiesRunContextErrors(t *testing.T) {
	tests := []struct {
		name       string
		newContext func() (context.Context, context.CancelFunc)
		timeout    time.Duration
		cancel     bool
		want       error
	}{
		{
			name: "own deadline",
			newContext: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			timeout: 20 * time.Millisecond,
			want:    context.DeadlineExceeded,
		},
		{
			name: "parent deadline",
			newContext: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 20*time.Millisecond)
			},
			timeout: time.Minute,
			want:    context.DeadlineExceeded,
		},
		{
			name: "parent cancellation",
			newContext: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			timeout: time.Minute,
			cancel:  true,
			want:    context.Canceled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := tt.newContext()
			defer cancel()
			started := make(chan struct{})
			client := &CatalogClient{
				timeout: tt.timeout,
				command: func(ctx context.Context, name string, args ...string) *exec.Cmd {
					close(started)
					return exec.CommandContext(ctx, "sleep", "10")
				},
			}

			errCh := make(chan error, 1)
			go func() {
				_, err := client.List(ctx, "arch")
				errCh <- err
			}()
			select {
			case <-started:
			case <-time.After(2 * time.Second):
				t.Fatal("command did not start")
			}
			if tt.cancel {
				cancel()
			}
			select {
			case err := <-errCh:
				if !errors.Is(err, tt.want) {
					t.Fatalf("List() error = %v, want %v", err, tt.want)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("List() did not return after context ended")
			}
		})
	}
}

func TestCatalogOutputSafety(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		command func(ctx context.Context) *exec.Cmd
		wantErr error
		check   func(t *testing.T, err error)
	}{
		{
			name: "oversized stdout",
			command: func(ctx context.Context) *exec.Cmd {
				return exec.CommandContext(ctx, "sh", "-c",
					fmt.Sprintf("head -c %d /dev/zero", maxCatalogBytes+1))
			},
			wantErr: errCatalogTooLarge,
			check: func(t *testing.T, err error) {
				t.Helper()
				if strings.Contains(err.Error(), strings.Repeat("\x00", 64)) {
					t.Fatalf("error embeds oversized stdout: %v", err)
				}
			},
		},
		{
			name: "oversized stderr",
			command: func(ctx context.Context) *exec.Cmd {
				return exec.CommandContext(ctx, "sh", "-c",
					fmt.Sprintf("head -c %d /dev/zero >&2; exit 1", maxCatalogDiagnosticBytes+1))
			},
			wantErr: errCatalogTooLarge,
			check: func(t *testing.T, err error) {
				t.Helper()
				if strings.Contains(err.Error(), strings.Repeat("\x00", 64)) {
					t.Fatalf("error embeds oversized stderr: %v", err)
				}
				if errors.Is(err, errCatalogSSH) {
					t.Fatalf("oversized stderr must not wrap SSH with captured output: %v", err)
				}
			},
		},
		{
			name: "ansi control stderr",
			command: func(ctx context.Context) *exec.Cmd {
				cmd := exec.CommandContext(ctx, "sh", "-c", "cat >&2; exit 255")
				cmd.Stdin = strings.NewReader("\x1b[31mconnection refused\x1b[0m\x07")
				return cmd
			},
			wantErr: errCatalogSSH,
			check: func(t *testing.T, err error) {
				t.Helper()
				msg := err.Error()
				if strings.ContainsRune(msg, '\x1b') || strings.ContainsRune(msg, '\x07') {
					t.Fatalf("error retains control/ANSI bytes: %q", msg)
				}
				if strings.Contains(msg, "[31m") || strings.Contains(msg, "[0m") {
					t.Fatalf("error retains terminal escape remnants: %q", msg)
				}
				if !strings.Contains(msg, "connection refused") {
					t.Fatalf("error = %q, want printable diagnostic text preserved", msg)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			client := &CatalogClient{
				command: func(ctx context.Context, name string, args ...string) *exec.Cmd {
					return tt.command(ctx)
				},
			}
			_, err := client.List(context.Background(), "arch")
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("List() error = %v, want %v", err, tt.wantErr)
			}
			if tt.check != nil {
				tt.check(t, err)
			}
		})
	}
}

func TestBoundedBufferLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		limit     int
		chunks    [][]byte
		wantLen   int
		wantOver  bool
		wantBytes string
	}{
		{
			name:      "under limit",
			limit:     8,
			chunks:    [][]byte{[]byte("abcd"), []byte("ef")},
			wantLen:   6,
			wantOver:  false,
			wantBytes: "abcdef",
		},
		{
			name:      "exact limit",
			limit:     4,
			chunks:    [][]byte{[]byte("abcd")},
			wantLen:   4,
			wantOver:  false,
			wantBytes: "abcd",
		},
		{
			name:      "overflow discards remainder",
			limit:     4,
			chunks:    [][]byte{[]byte("abc"), []byte("defgh")},
			wantLen:   4,
			wantOver:  true,
			wantBytes: "abcd",
		},
		{
			name:      "further writes stay discarded",
			limit:     2,
			chunks:    [][]byte{[]byte("abcdef"), []byte("xyz")},
			wantLen:   2,
			wantOver:  true,
			wantBytes: "ab",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var buf boundedBuffer
			buf.limit = tt.limit
			for _, chunk := range tt.chunks {
				n, err := buf.Write(chunk)
				if err != nil {
					t.Fatalf("Write(%q) error = %v", chunk, err)
				}
				if n != len(chunk) {
					t.Fatalf("Write(%q) n = %d, want %d", chunk, n, len(chunk))
				}
			}
			if buf.overflow != tt.wantOver {
				t.Fatalf("overflow = %v, want %v", buf.overflow, tt.wantOver)
			}
			if got := len(buf.Bytes()); got != tt.wantLen {
				t.Fatalf("len = %d, want %d", got, tt.wantLen)
			}
			if got := string(buf.Bytes()); got != tt.wantBytes {
				t.Fatalf("bytes = %q, want %q", got, tt.wantBytes)
			}
		})
	}
}

func stdoutCmd(ctx context.Context, stdout string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "cat")
	cmd.Stdin = strings.NewReader(stdout)
	return cmd
}

func mustCatalogJSON(t *testing.T, catalog ports.RemoteCatalog) string {
	t.Helper()
	raw, err := json.Marshal(catalog)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	return string(raw) + "\n"
}
