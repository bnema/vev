//go:build linux

package clipboard

import (
	"context"
	"errors"
	"os/exec"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
)

func TestPickImageMimePrefersPngOverOtherImageTypes(t *testing.T) {
	tests := []struct {
		name   string
		output string
		want   string
	}{
		{"no types", "", ""},
		{"text only", "text/plain\nUTF8_STRING\n", ""},
		{"png present", "text/plain\nimage/png\n", "image/png"},
		{"prefers png over jpeg", "image/jpeg\nimage/png\n", "image/png"},
		{"falls back to jpeg", "image/jpeg\nimage/webp\n", "image/jpeg"},
		{"falls back to webp", "image/webp\nimage/gif\n", "image/webp"},
		{"falls back to gif", "text/plain\nimage/gif\n", "image/gif"},
		{"unsupported image type only", "image/bmp\n", ""},
		{"ignores blank lines and whitespace", "\n  image/png  \n\n", "image/png"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := pickImageMime(tt.output); got != tt.want {
				t.Fatalf("pickImageMime(%q) = %q, want %q", tt.output, got, tt.want)
			}
		})
	}
}

// fakeExecCommand returns a substitute execCommand that never shells out to
// wl-paste itself: it inspects the requested command/args and swaps in a
// safe, universally available binary (printf) that produces the same
// stdout a real wl-paste invocation would, so command construction and the
// read pipeline can be verified without wl-paste installed.
func fakeExecCommand(t *testing.T, listTypesOutput, imageData string) (execCommand func(context.Context, string, ...string) *exec.Cmd, calls *[][]string) {
	t.Helper()
	var got [][]string
	fn := func(ctx context.Context, name string, args ...string) *exec.Cmd {
		call := append([]string{name}, args...)
		got = append(got, call)
		if name != "wl-paste" {
			return exec.CommandContext(ctx, "false")
		}
		if len(args) > 0 && args[0] == "--list-types" {
			return exec.CommandContext(ctx, "printf", "%s", listTypesOutput)
		}
		return exec.CommandContext(ctx, "printf", "%s", imageData)
	}
	return fn, &got
}

func TestReadImageConstructsExpectedCommandsAndReturnsData(t *testing.T) {
	execCommand, calls := fakeExecCommand(t, "text/plain\nimage/png\nimage/jpeg\n", "PNGBYTES")
	w := &WlPaste{execCommand: execCommand}

	mime, data, err := w.ReadImage(context.Background())
	if err != nil {
		t.Fatalf("ReadImage() error = %v", err)
	}
	if mime != "image/png" {
		t.Fatalf("mime = %q, want image/png", mime)
	}
	if string(data) != "PNGBYTES" {
		t.Fatalf("data = %q, want PNGBYTES", data)
	}

	want := [][]string{
		{"wl-paste", "--list-types"},
		{"wl-paste", "--type", "image/png", "--no-newline"},
	}
	got := *calls
	if len(got) != len(want) {
		t.Fatalf("calls = %#v, want %#v", got, want)
	}
	for i := range want {
		if len(got[i]) != len(want[i]) {
			t.Fatalf("call %d = %#v, want %#v", i, got[i], want[i])
		}
		for j := range want[i] {
			if got[i][j] != want[i][j] {
				t.Fatalf("call %d = %#v, want %#v", i, got[i], want[i])
			}
		}
	}
}

func TestReadImageNoImageOnClipboard(t *testing.T) {
	execCommand, _ := fakeExecCommand(t, "text/plain\nUTF8_STRING\n", "")
	w := &WlPaste{execCommand: execCommand}

	_, _, err := w.ReadImage(context.Background())
	if !errors.Is(err, ports.ErrNoClipboardImage) {
		t.Fatalf("err = %v, want ErrNoClipboardImage", err)
	}
}

func TestReadImageListTypesFailureReturnsNoClipboardImage(t *testing.T) {
	execCommand := func(ctx context.Context, name string, args ...string) *exec.Cmd {
		return exec.CommandContext(ctx, "false") // exits 1: simulates wl-paste missing/failing
	}
	w := &WlPaste{execCommand: execCommand}

	_, _, err := w.ReadImage(context.Background())
	if !errors.Is(err, ports.ErrNoClipboardImage) {
		t.Fatalf("err = %v, want ErrNoClipboardImage", err)
	}
}

// TestReadImageDataReadCommandHasDeadline proves the actual clipboard-data
// read (not just the --list-types probe) runs under a context carrying a
// deadline: called with context.Background() (as the client interceptor
// does), a wl-paste that hangs mid-read must not stall the caller forever.
func TestReadImageDataReadCommandHasDeadline(t *testing.T) {
	var readCtx context.Context
	execCommand := func(ctx context.Context, name string, args ...string) *exec.Cmd {
		if name == "wl-paste" && len(args) > 0 && args[0] == "--list-types" {
			return exec.CommandContext(ctx, "printf", "%s", "image/png\n")
		}
		readCtx = ctx
		return exec.CommandContext(ctx, "printf", "%s", "PNGBYTES")
	}
	w := &WlPaste{execCommand: execCommand}

	_, _, err := w.ReadImage(context.Background())
	if err != nil {
		t.Fatalf("ReadImage() error = %v", err)
	}
	if readCtx == nil {
		t.Fatal("data read command was never constructed")
	}
	if _, ok := readCtx.Deadline(); !ok {
		t.Fatal("data read command's context must carry a deadline, even when ReadImage is called with context.Background()")
	}
}

// TestReadImageHungDataReadTimesOut proves a wl-paste that hangs while
// streaming the actual clipboard bytes (as opposed to during --list-types,
// covered by listTypesTimeout) is bounded by readTimeout instead of stalling
// the caller indefinitely.
func TestReadImageHungDataReadTimesOut(t *testing.T) {
	execCommand := func(ctx context.Context, name string, args ...string) *exec.Cmd {
		if name == "wl-paste" && len(args) > 0 && args[0] == "--list-types" {
			return exec.CommandContext(ctx, "printf", "%s", "image/png\n")
		}
		return exec.CommandContext(ctx, "sleep", "5") // simulates a hung read
	}
	w := &WlPaste{execCommand: execCommand, readTimeout: 50 * time.Millisecond}

	start := time.Now()
	_, _, err := w.ReadImage(context.Background())
	elapsed := time.Since(start)

	if !errors.Is(err, ports.ErrNoClipboardImage) {
		t.Fatalf("err = %v, want ErrNoClipboardImage", err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("ReadImage() took %v; a hung read must be bounded by readTimeout, not context.Background()", elapsed)
	}
}

// TestReadImageAgainstRealWlPaste is a best-effort integration check against
// the real binary; it skips itself when wl-paste (or a Wayland clipboard) is
// not available, which is the common case in CI/sandbox environments.
func TestReadImageAgainstRealWlPaste(t *testing.T) {
	if _, err := exec.LookPath("wl-paste"); err != nil {
		t.Skip("wl-paste not installed")
	}
	w := New()
	_, _, err := w.ReadImage(context.Background())
	// No clipboard image is a perfectly valid outcome in CI; just confirm
	// the call doesn't panic and errors are of the expected shape.
	if err != nil && !errors.Is(err, ports.ErrNoClipboardImage) {
		t.Fatalf("ReadImage() unexpected error = %v", err)
	}
}
