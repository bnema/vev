package wire

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"testing"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

// mustNotPanic runs f and fails the test (instead of crashing the test
// binary) if f panics.
func mustNotPanic(t *testing.T, f func()) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panicked: %v", r)
		}
	}()
	f()
}

// assertAllPrefixesFail decodes every strict prefix of full and requires an
// error (never a panic, never a successful decode) for each one. This
// exercises truncation at every possible boundary — mid-length,
// mid-string-len, mid-string — in one sweep.
func assertAllPrefixesFail[T any](t *testing.T, full []byte, unmarshal func([]byte) (T, error)) {
	t.Helper()
	for n := range len(full) {
		t.Run(fmt.Sprintf("truncated_at_%d", n), func(t *testing.T) {
			mustNotPanic(t, func() {
				if _, err := unmarshal(full[:n]); err == nil {
					t.Fatalf("expected error decoding %d/%d truncated bytes, got nil", n, len(full))
				}
			})
		})
	}
}

// assertTrailingGarbageFails decodes full with extra bytes appended and
// requires an error for a fixed-shape message.
func assertTrailingGarbageFails[T any](t *testing.T, full []byte, unmarshal func([]byte) (T, error)) {
	t.Helper()
	garbage := append(append([]byte(nil), full...), 0xff)
	mustNotPanic(t, func() {
		if _, err := unmarshal(garbage); err == nil {
			t.Fatalf("expected error decoding payload with trailing garbage, got nil")
		}
	})
}

func appendNoNavigationTail(payload []byte) []byte {
	// exact-target absent, preferred-tab absent, navigation capabilities none,
	// startup overlay none, local carriage.
	return append(payload, 0, 0, 0, 0, 0, 0, 0)
}

func TestHelloGoldenAndRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		msg  protocol.Hello
		want []byte
	}{
		{
			name: "typical",
			msg: protocol.Hello{
				Version:           1,
				Intent:            protocol.IntentNew,
				ClientID:          [16]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10},
				ResumeToken:       0x0102030405060708,
				Name:              "w0",
				Size:              domain.Size{Cols: 80, Rows: 24},
				TermEnv:           "xterm-256color",
				Cwd:               "/tmp/project",
				TrueColor:         true,
				MaxOutputInFlight: 8,
			},
			want: []byte{0x00, 0x01, 0x01, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x02, 0x77, 0x30, 0x00, 0x50, 0x00, 0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0e, 0x78, 0x74, 0x65, 0x72, 0x6d, 0x2d, 0x32, 0x35, 0x36, 0x63, 0x6f, 0x6c, 0x6f, 0x72, 0x00, 0x0c, 0x2f, 0x74, 0x6d, 0x70, 0x2f, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x01, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		},
		{
			name: "empty strings",
			msg: protocol.Hello{
				Version:   1,
				Intent:    protocol.IntentEphemeral,
				Name:      "",
				Size:      domain.Size{Cols: 1, Rows: 1},
				TermEnv:   "",
				Cwd:       "",
				TrueColor: false,
			},
			want: func() []byte {
				out := append([]byte{0x00, 0x01, 0x00}, make([]byte, 26)...)
				out = append(out, 0x00, 0x01, 0x00, 0x01) // cell size
				return append(out, make([]byte, 16)...)   // pixels and remaining fields
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := appendNoNavigationTail(append([]byte(nil), tt.want...))
			got := MarshalHello(tt.msg)
			if !bytes.Equal(got, want) {
				t.Fatalf("MarshalHello() = %#v, want %#v", got, want)
			}
			back, err := UnmarshalHello(got)
			if err != nil {
				t.Fatalf("UnmarshalHello() error = %v", err)
			}
			if !reflect.DeepEqual(back, tt.msg) {
				t.Fatalf("round trip = %#v, want %#v", back, tt.msg)
			}
			peek, ok := PeekHelloVersion(got)
			if !ok || peek != tt.msg.Version {
				t.Fatalf("PeekHelloVersion() = %d, %v; want %d, true", peek, ok, tt.msg.Version)
			}
		})
	}

	full := MarshalHello(tests[0].msg)
	assertAllPrefixesFail(t, full, UnmarshalHello)
	assertTrailingGarbageFails(t, full, UnmarshalHello)
}

func TestHelloEnvironmentCodec(t *testing.T) {
	t.Run("literal wire layout", func(t *testing.T) {
		msg := protocol.Hello{
			Version: protocol.Version,
			Size:    domain.Size{Cols: 1, Rows: 1},
			Env:     []string{"A=B", "XY=123"},
		}
		got := MarshalHello(msg)
		want := []byte{
			0x00, 0x28, 0x00, // version, intent, first client ID byte
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // client ID
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // resume token
			0x00, 0x00, // name
			0x00, 0x01, 0x00, 0x01, // size
			0x00, 0x00, 0x00, 0x00, // pixel width and height
			0x00, 0x00, // TERM
			0x00, 0x00, // cwd
			0x00, 0x00, // true color, max output in flight
			0x00, 0x00, 0x00, 0x02, // uint32 environment entry count
			0x00, 0x00, 0x00, 0x03, 'A', '=', 'B',
			0x00, 0x00, 0x00, 0x06, 'X', 'Y', '=', '1', '2', '3',
			0x00, 0x00, // no remote target, client-owned environment
		}
		want = appendNoNavigationTail(want)
		require.Equal(t, want, got)
		decoded, err := UnmarshalHello(want)
		require.NoError(t, err)
		require.Equal(t, msg, decoded)
		assertAllPrefixesFail(t, want, UnmarshalHello)
		assertTrailingGarbageFails(t, want, UnmarshalHello)
	})

	t.Run("lossless order and values", func(t *testing.T) {
		entryOverUint16 := "LARGE=" + string(bytes.Repeat([]byte("x"), 65536))
		want := []string{"A=first", "TOKEN=a=b=c", "EMPTY=", entryOverUint16, "A=second"}
		payload := MarshalHello(protocol.Hello{Version: protocol.Version, Size: domain.Size{Cols: 1, Rows: 1}, Env: want})
		got, err := UnmarshalHello(payload)
		require.NoError(t, err)
		require.Equal(t, want, got.Env)
	})

	t.Run("empty", func(t *testing.T) {
		payload := MarshalHello(protocol.Hello{Version: protocol.Version, Size: domain.Size{Cols: 1, Rows: 1}, Env: []string{}})
		got, err := UnmarshalHello(payload)
		require.NoError(t, err)
		require.Empty(t, got.Env)
	})

	base := MarshalHello(protocol.Hello{Version: protocol.Version, Size: domain.Size{Cols: 1, Rows: 1}})
	withCount := func(count byte) []byte {
		payload := append([]byte(nil), base...)
		// The uint32 environment count occupies bytes len-13 through len-10.
		payload[len(payload)-10] = count
		return payload
	}
	withEntries := func(count byte, entries ...byte) []byte {
		payload := withCount(count)
		// The nine-byte tail follows the environment-count field.
		tail := append([]byte(nil), payload[len(payload)-9:]...)
		payload = append(payload[:len(payload)-9], entries...)
		return append(payload, tail...)
	}
	tests := []struct {
		name    string
		payload []byte
	}{
		{name: "truncated count", payload: base[:len(base)-1]},
		{name: "truncated entry length", payload: withEntries(1, 0, 0)},
		{name: "truncated entry body", payload: withEntries(1, 0, 0, 0, 3, 'x')},
		{name: "impossible count", payload: withCount(2)},
		{name: "trailing garbage", payload: append(append([]byte(nil), base...), 0xff)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := UnmarshalHello(tt.payload)
			require.Error(t, err)
			if tt.name != "trailing garbage" {
				require.ErrorIs(t, err, errShortPayload)
			}
		})
	}
}

func TestInputGoldenAndRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		msg  protocol.Input
		want []byte
	}{
		{name: "data", msg: protocol.Input{InputSeq: 0x0102030405060708, ActionID: 9, Data: []byte("hi")}, want: []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0, 0, 0, 0, 0, 0, 0, 9, 0x68, 0x69}},
		{name: "empty", msg: protocol.Input{InputSeq: 0, Data: nil}, want: make([]byte, 16)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MarshalInput(tt.msg)
			if !bytes.Equal(got, tt.want) {
				t.Fatalf("MarshalInput() = %#v, want %#v", got, tt.want)
			}
			back, err := UnmarshalInput(got)
			if err != nil {
				t.Fatalf("UnmarshalInput() error = %v", err)
			}
			if !reflect.DeepEqual(back, tt.msg) {
				t.Fatalf("round trip = %#v, want %#v", back, tt.msg)
			}
		})
	}

	full := MarshalInput(tests[0].msg)
	assertAllPrefixesFail(t, full[:16], UnmarshalInput)
}

func TestImagePushGoldenAndRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		msg  protocol.ImagePush
		want []byte
	}{
		{
			name: "png data",
			msg:  protocol.ImagePush{InputSeq: 7, Mime: "image/png", Data: []byte{0x01, 0x02, 0x03}},
			want: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x09, 0x69, 0x6d, 0x61, 0x67, 0x65, 0x2f, 0x70, 0x6e, 0x67, 0x01, 0x02, 0x03},
		},
		{name: "empty", msg: protocol.ImagePush{}, want: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MarshalImagePush(tt.msg)
			if !bytes.Equal(got, tt.want) {
				t.Fatalf("MarshalImagePush() = %#v, want %#v", got, tt.want)
			}
			back, err := UnmarshalImagePush(got)
			if err != nil {
				t.Fatalf("UnmarshalImagePush() error = %v", err)
			}
			if !reflect.DeepEqual(back, tt.msg) {
				t.Fatalf("round trip = %#v, want %#v", back, tt.msg)
			}
		})
	}

	full := MarshalImagePush(tests[0].msg)
	assertAllPrefixesFail(t, full[:19], UnmarshalImagePush)
}

func TestThemeGoldenAndRoundTrip(t *testing.T) {
	palette := [16]renderer.RGB{
		{R: 0x10, G: 0x11, B: 0x12}, {R: 0x13, G: 0x14, B: 0x15},
		{R: 0x17, G: 0x17, B: 0x18}, {R: 0x19, G: 0x1a, B: 0x1b},
		{R: 0x1c, G: 0x1d, B: 0x1e}, {R: 0x1f, G: 0x20, B: 0x21},
		{R: 0x22, G: 0x23, B: 0x24}, {R: 0x25, G: 0x26, B: 0x27},
		{R: 0x28, G: 0x29, B: 0x2a}, {R: 0x2b, G: 0x2c, B: 0x2d},
		{R: 0x2e, G: 0x2f, B: 0x30}, {R: 0x31, G: 0x32, B: 0x33},
		{R: 0x34, G: 0x35, B: 0x36}, {R: 0x37, G: 0x38, B: 0x39},
		{R: 0x3a, G: 0x3b, B: 0x3c}, {R: 0x3d, G: 0x3e, B: 0x3f},
	}
	zeroes := make([]byte, 57)
	tests := []struct {
		name string
		msg  protocol.Theme
		want []byte
	}{
		{
			name: "empty",
			msg:  protocol.Theme{},
			want: zeroes,
		},
		{
			name: "foreground only",
			msg: protocol.Theme{
				HasForeground: true,
				Foreground:    renderer.RGB{R: 10, G: 20, B: 30},
				Background:    renderer.RGB{R: 40, G: 50, B: 60},
			},
			want: append([]byte{0x01, 0x0a, 0x14, 0x1e, 0x28, 0x32, 0x3c}, zeroes[7:]...),
		},
		{
			name: "light flag without known scheme",
			msg: protocol.Theme{
				Light: true,
			},
			want: append([]byte{0x10}, zeroes[1:]...),
		},
		{
			name: "full palette",
			msg: protocol.Theme{
				HasForeground: true,
				Foreground:    renderer.RGB{R: 1, G: 2, B: 3},
				HasBackground: true,
				Background:    renderer.RGB{R: 4, G: 5, B: 6},
				TrueColor:     true,
				SchemeKnown:   true,
				PaletteKnown:  0x8001,
				Palette:       palette,
			},
			want: []byte{
				0x0f, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x80, 0x01,
				0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x17, 0x17, 0x18,
				0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20, 0x21,
				0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a,
				0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x30, 0x31, 0x32, 0x33,
				0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3a, 0x3b, 0x3c,
				0x3d, 0x3e, 0x3f,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MarshalTheme(tt.msg)
			require.Len(t, got, 57)
			require.Equal(t, tt.want, got)
			back, err := UnmarshalTheme(got)
			require.NoError(t, err)
			require.Equal(t, tt.msg, back)
		})
	}

	full := MarshalTheme(tests[len(tests)-1].msg)
	assertAllPrefixesFail(t, full, UnmarshalTheme)
	assertTrailingGarbageFails(t, full, UnmarshalTheme)
}

func TestThemeGenerationClearedWireGoldenPreservesProtocolVersion(t *testing.T) {
	// A generation clear retains defaults/capabilities but has no palette bits.
	// This literal payload locks the existing 57-byte Theme layout while the
	// full definitive-palette golden above locks the final publication.
	cleared := protocol.Theme{
		HasForeground: true,
		Foreground:    renderer.RGB{R: 1, G: 2, B: 3},
		HasBackground: true,
		Background:    renderer.RGB{R: 4, G: 5, B: 6},
		TrueColor:     true,
		SchemeKnown:   true,
	}
	want := append([]byte{0x0f, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x00, 0x00}, make([]byte, 48)...)
	require.Equal(t, want, MarshalTheme(cleared))
	require.Equal(t, uint16(40), protocol.Version)
}

func TestResizeGoldenAndRoundTrip(t *testing.T) {
	msg := protocol.Resize{Size: domain.Size{Cols: 100, Rows: 40}}
	want := []byte{0x00, 0x64, 0x00, 0x28, 0x00, 0x00, 0x00, 0x00}

	got, err := MarshalResize(msg)
	if err != nil {
		t.Fatalf("MarshalResize() error = %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("MarshalResize() = %#v, want %#v", got, want)
	}
	back, err := UnmarshalResize(got)
	if err != nil {
		t.Fatalf("UnmarshalResize() error = %v", err)
	}
	if !reflect.DeepEqual(back, msg) {
		t.Fatalf("round trip = %#v, want %#v", back, msg)
	}

	assertAllPrefixesFail(t, got, UnmarshalResize)
	assertTrailingGarbageFails(t, got, UnmarshalResize)
}

func TestDetachGoldenAndRoundTrip(t *testing.T) {
	got := MarshalDetach(protocol.Detach{})
	if len(got) != 0 {
		t.Fatalf("MarshalDetach() = %#v, want empty", got)
	}
	back, err := UnmarshalDetach(got)
	if err != nil {
		t.Fatalf("UnmarshalDetach() error = %v", err)
	}
	if back != (protocol.Detach{}) {
		t.Fatalf("round trip = %#v, want %#v", back, protocol.Detach{})
	}
	assertTrailingGarbageFails(t, got, UnmarshalDetach)
}

func TestPingGoldenAndRoundTrip(t *testing.T) {
	got := MarshalPing(protocol.Ping{})
	if len(got) != 0 {
		t.Fatalf("MarshalPing() = %#v, want empty", got)
	}
	back, err := UnmarshalPing(got)
	if err != nil {
		t.Fatalf("UnmarshalPing() error = %v", err)
	}
	if back != (protocol.Ping{}) {
		t.Fatalf("round trip = %#v, want %#v", back, protocol.Ping{})
	}
	assertTrailingGarbageFails(t, got, UnmarshalPing)
}

func TestPongGoldenAndRoundTrip(t *testing.T) {
	got := MarshalPong(protocol.Pong{})
	if len(got) != 0 {
		t.Fatalf("MarshalPong() = %#v, want empty", got)
	}
	back, err := UnmarshalPong(got)
	if err != nil {
		t.Fatalf("UnmarshalPong() error = %v", err)
	}
	if back != (protocol.Pong{}) {
		t.Fatalf("round trip = %#v, want %#v", back, protocol.Pong{})
	}
	assertTrailingGarbageFails(t, got, UnmarshalPong)
}

func TestMarshalWelcomeRejectsInvalidCommittedIdentity(t *testing.T) {
	got := MarshalWelcome(protocol.Welcome{
		SessionName: "work",
		CommittedIdentity: &protocol.CommittedRouteIdentity{
			Target: protocol.ExactSessionTarget{SessionName: "different"},
		},
	})
	require.Nil(t, got)
}

func TestMarshalWelcomeRejectsCommittedIdentityMetadataMismatch(t *testing.T) {
	base := protocol.Welcome{
		SessionID:   "session",
		SessionName: "work",
		Ephemeral:   true,
		CommittedIdentity: &protocol.CommittedRouteIdentity{
			Target:    protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "work"},
			Ephemeral: true,
		},
	}
	for name, mutate := range map[string]func(*protocol.Welcome){
		"session name": func(w *protocol.Welcome) { w.SessionName = "other" },
		"ephemeral":    func(w *protocol.Welcome) { w.Ephemeral = false },
	} {
		t.Run(name, func(t *testing.T) {
			got := base
			mutate(&got)
			require.Nil(t, MarshalWelcome(got))
		})
	}
	require.NotNil(t, MarshalWelcome(base))
}

func TestWelcomeGoldenAndRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		msg  protocol.Welcome
		want []byte
	}{
		{
			name: "ephemeral",
			msg:  protocol.Welcome{SessionID: "sess-1", SessionName: "main", Ephemeral: true, ResumeToken: 0x0102030405060708, Capabilities: protocol.CapabilityResume | protocol.CapabilityPredict},
			want: []byte{0x00, 0x06, 0x73, 0x65, 0x73, 0x73, 0x2d, 0x31, 0x00, 0x04, 0x6d, 0x61, 0x69, 0x6e, 0x01, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x05, 0x00},
		},
		{
			name: "non-ephemeral empty name",
			msg:  protocol.Welcome{SessionID: "abc", SessionName: "", Ephemeral: false},
			want: []byte{0x00, 0x03, 0x61, 0x62, 0x63, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MarshalWelcome(tt.msg)
			if !bytes.Equal(got, tt.want) {
				t.Fatalf("MarshalWelcome() = %#v, want %#v", got, tt.want)
			}
			back, err := UnmarshalWelcome(got)
			if err != nil {
				t.Fatalf("UnmarshalWelcome() error = %v", err)
			}
			if !reflect.DeepEqual(back, tt.msg) {
				t.Fatalf("round trip = %#v, want %#v", back, tt.msg)
			}
		})
	}

	full := MarshalWelcome(tests[0].msg)
	assertAllPrefixesFail(t, full, UnmarshalWelcome)
	assertTrailingGarbageFails(t, full, UnmarshalWelcome)
}

func TestCommandErrorCodes(t *testing.T) {
	require.Equal(t, uint16(6), protocol.ErrUnknownCommand)
	require.Equal(t, uint16(7), protocol.ErrNotScriptable)
	require.Equal(t, uint16(8), protocol.ErrInvalidCommandArgs)
	require.Equal(t, uint16(9), protocol.ErrNoSuchTarget)
	require.Equal(t, uint16(10), protocol.ErrAmbiguousTarget)
}

func TestCommandRequestGoldenAndRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		msg  protocol.CommandRequest
		want []byte
	}{
		{
			name: "minimal",
			msg:  protocol.CommandRequest{Version: protocol.Version, Slug: "split-right"},
			want: []byte{
				0x00, 0x28,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // request ID
				0x00, // attached
				0x00, // self
				0x00, 0x0b, 's', 'p', 'l', 'i', 't', '-', 'r', 'i', 'g', 'h', 't',
				0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00,
			},
		},
		{
			name: "full",
			msg: protocol.CommandRequest{
				Version:       protocol.Version,
				Self:          true,
				Slug:          "toast",
				Args:          []string{"-l", "warn", "tests KO"},
				TargetSession: "dev",
				TargetTab:     "t_abc",
				TargetPane:    "p_def",
				JSON:          true,
			},
			want: []byte{
				0x00, 0x28,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // request ID
				0x00, // attached
				0x01, // self
				0x00, 0x05, 't', 'o', 'a', 's', 't',
				0x00, 0x03,
				0x00, 0x00, 0x00, 0x02, '-', 'l',
				0x00, 0x00, 0x00, 0x04, 'w', 'a', 'r', 'n',
				0x00, 0x00, 0x00, 0x08, 't', 'e', 's', 't', 's', ' ', 'K', 'O',
				0x00, 0x03, 'd', 'e', 'v',
				0x00, 0x05, 't', '_', 'a', 'b', 'c',
				0x00, 0x05, 'p', '_', 'd', 'e', 'f',
				0x01,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := MarshalCommandRequest(tt.msg)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
			back, err := UnmarshalCommandRequest(got)
			require.NoError(t, err)
			require.Equal(t, tt.msg, back)
			version, ok := PeekCommandVersion(got)
			require.True(t, ok)
			require.Equal(t, tt.msg.Version, version)
			requestID, ok := PeekCommandRequestID(got)
			require.True(t, ok)
			require.Equal(t, tt.msg.RequestID, requestID)
			assertAllPrefixesFail(t, got, UnmarshalCommandRequest)
			assertTrailingGarbageFails(t, got, UnmarshalCommandRequest)
		})
	}

	if _, ok := PeekCommandVersion([]byte{0x00}); ok {
		t.Fatal("PeekCommandVersion accepted a one-byte payload")
	}
	if _, ok := PeekCommandRequestID(make([]byte, 9)); ok {
		t.Fatal("PeekCommandRequestID accepted a nine-byte payload")
	}
}

func TestMarshalCommandRequestRejectsTooManyArguments(t *testing.T) {
	payload, err := MarshalCommandRequest(protocol.CommandRequest{
		Version: protocol.Version,
		Slug:    "toast",
		Args:    make([]string, math.MaxUint16+1),
	})

	require.Nil(t, payload)
	require.ErrorIs(t, err, ErrTooManyCommandArgs)
}

func TestCommandRequestRejectsImpossibleArgumentCount(t *testing.T) {
	payload, err := MarshalCommandRequest(protocol.CommandRequest{Version: protocol.Version, Slug: "toast"})
	require.NoError(t, err)
	argCountOffset := 2 + 8 + 1 + 1 + 2 + len("toast")
	payload[argCountOffset] = 0xff
	payload[argCountOffset+1] = 0xff

	if _, err := UnmarshalCommandRequest(payload); err == nil {
		t.Fatal("UnmarshalCommandRequest accepted an impossible argument count")
	}
}

func TestCommandResultGoldenAndRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		msg  protocol.CommandResult
		want []byte
	}{
		{
			name: "error",
			msg:  protocol.CommandResult{Code: protocol.ErrNoSuchTarget, Text: "no such session"},
			want: []byte{
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // request ID
				0x00,
				0x00, 0x09,
				0x00, 0x0f, 'n', 'o', ' ', 's', 'u', 'c', 'h', ' ', 's', 'e', 's', 's', 'i', 'o', 'n',
				0x00, 0x00, 0x00, 0x00,
			},
		},
		{
			name: "success with output",
			msg:  protocol.CommandResult{OK: true, Text: "listed", Output: "ID\tFOCUSED\n"},
			want: []byte{
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // request ID
				0x01,
				0x00, 0x00,
				0x00, 0x06, 'l', 'i', 's', 't', 'e', 'd',
				0x00, 0x00, 0x00, 0x0b, 'I', 'D', '\t', 'F', 'O', 'C', 'U', 'S', 'E', 'D', '\n',
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MarshalCommandResult(tt.msg)
			require.Equal(t, tt.want, got)
			back, err := UnmarshalCommandResult(got)
			require.NoError(t, err)
			require.Equal(t, tt.msg, back)
			assertAllPrefixesFail(t, got, UnmarshalCommandResult)
			assertTrailingGarbageFails(t, got, UnmarshalCommandResult)
		})
	}
}

func TestErrorMsgGoldenAndRoundTrip(t *testing.T) {
	msg := protocol.ErrorMsg{Code: protocol.ErrNameTaken, Text: "name taken"}
	want := []byte{0x00, 0x03, 0x00, 0x0a, 0x6e, 0x61, 0x6d, 0x65, 0x20, 0x74, 0x61, 0x6b, 0x65, 0x6e}

	got := MarshalErrorMsg(msg)
	if !bytes.Equal(got, want) {
		t.Fatalf("MarshalErrorMsg() = %#v, want %#v", got, want)
	}
	back, err := UnmarshalErrorMsg(got)
	if err != nil {
		t.Fatalf("UnmarshalErrorMsg() error = %v", err)
	}
	if !reflect.DeepEqual(back, msg) {
		t.Fatalf("round trip = %#v, want %#v", back, msg)
	}

	assertAllPrefixesFail(t, got, UnmarshalErrorMsg)
	assertTrailingGarbageFails(t, got, UnmarshalErrorMsg)
}

func TestOutputGoldenAndRoundTrip(t *testing.T) {
	msg := protocol.Output{Epoch: 1, Base: 0, New: 2, Echo: 3, Size: domain.Size{Cols: 1, Rows: 1}, Full: true, Data: []byte("hello\n")}
	want := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00, 0x06, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x0a}

	got, err := MarshalOutput(msg)
	if err != nil {
		t.Fatalf("MarshalOutput() error = %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("MarshalOutput() = %#v, want %#v", got, want)
	}
	back, err := UnmarshalOutput(got)
	if err != nil {
		t.Fatalf("UnmarshalOutput() error = %v", err)
	}
	if !reflect.DeepEqual(back, msg) {
		t.Fatalf("round trip = %#v, want %#v", back, msg)
	}

	assertAllPrefixesFail(t, got, UnmarshalOutput)
	assertTrailingGarbageFails(t, got, UnmarshalOutput)
}

func TestAckGoldenAndRoundTrip(t *testing.T) {
	msg := protocol.Ack{Epoch: 1, State: 0x0102030405060708}
	want := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}

	got, err := MarshalAck(msg)
	if err != nil {
		t.Fatalf("MarshalAck() error = %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("MarshalAck() = %#v, want %#v", got, want)
	}
	back, err := UnmarshalAck(got)
	if err != nil {
		t.Fatalf("UnmarshalAck() error = %v", err)
	}
	if back != msg {
		t.Fatalf("round trip = %#v, want %#v", back, msg)
	}

	assertAllPrefixesFail(t, got, UnmarshalAck)
	assertTrailingGarbageFails(t, got, UnmarshalAck)
}

func TestDetachedGoldenAndRoundTrip(t *testing.T) {
	msg := protocol.Detached{Reason: protocol.ReasonReplaced}
	want := []byte{0x03}

	got := MarshalDetached(msg)
	if !bytes.Equal(got, want) {
		t.Fatalf("MarshalDetached() = %#v, want %#v", got, want)
	}
	back, err := UnmarshalDetached(got)
	if err != nil {
		t.Fatalf("UnmarshalDetached() error = %v", err)
	}
	if !reflect.DeepEqual(back, msg) {
		t.Fatalf("round trip = %#v, want %#v", back, msg)
	}

	assertAllPrefixesFail(t, got, UnmarshalDetached)
	assertTrailingGarbageFails(t, got, UnmarshalDetached)
}

func TestClientNoticeGoldenAndStrict(t *testing.T) {
	tests := []struct {
		name string
		msg  protocol.ClientNotice
		want []byte
	}{
		{name: "clipboard fallback", msg: protocol.ClientNotice{Action: protocol.ClientNoticeClipboardFallback}, want: []byte{0x01}},
		{name: "clipboard too large", msg: protocol.ClientNotice{Action: protocol.ClientNoticeClipboardTooLarge}, want: []byte{0x02}},
		{name: "link degraded", msg: protocol.ClientNotice{Action: protocol.ClientNoticeLinkDegraded}, want: []byte{0x03}},
		{name: "link connected", msg: protocol.ClientNotice{Action: protocol.ClientNoticeLinkConnected}, want: []byte{0x04}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MarshalClientNotice(tt.msg)
			require.Equal(t, tt.want, got)
			back, err := UnmarshalClientNotice(got)
			require.NoError(t, err)
			require.Equal(t, tt.msg, back)
			assertAllPrefixesFail(t, got, UnmarshalClientNotice)
			assertTrailingGarbageFails(t, got, UnmarshalClientNotice)
		})
	}
	_, err := UnmarshalClientNotice([]byte{0xff})
	require.Error(t, err, "unknown actions must not become display text")
}

func TestListGoldenAndRoundTrip(t *testing.T) {
	got := MarshalList(protocol.List{})
	if len(got) != 0 {
		t.Fatalf("MarshalList() = %#v, want empty", got)
	}
	back, err := UnmarshalList(got)
	if err != nil {
		t.Fatalf("UnmarshalList() error = %v", err)
	}
	if back != (protocol.List{}) {
		t.Fatalf("round trip = %#v, want %#v", back, protocol.List{})
	}
	assertTrailingGarbageFails(t, got, UnmarshalList)
}

func TestKillGoldenAndRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		msg  protocol.Kill
		want []byte
	}{
		{name: "named", msg: protocol.Kill{Name: "main"}, want: []byte{0x00, 0x04, 0x6d, 0x61, 0x69, 0x6e}},
		{name: "empty", msg: protocol.Kill{Name: ""}, want: []byte{0x00, 0x00}},
		{name: "daemon", msg: protocol.Kill{Scope: protocol.KillDaemon}, want: []byte{0x00, 0x00, 0x01}},
		{name: "all", msg: protocol.Kill{Scope: protocol.KillAll}, want: []byte{0x00, 0x00, 0x02}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MarshalKill(tt.msg)
			if !bytes.Equal(got, tt.want) {
				t.Fatalf("MarshalKill() = %#v, want %#v", got, tt.want)
			}
			back, err := UnmarshalKill(got)
			if err != nil {
				t.Fatalf("UnmarshalKill() error = %v", err)
			}
			if !reflect.DeepEqual(back, tt.msg) {
				t.Fatalf("round trip = %#v, want %#v", back, tt.msg)
			}
		})
	}

	legacy := MarshalKill(tests[0].msg)
	back, err := UnmarshalKill(legacy)
	if err != nil {
		t.Fatalf("UnmarshalKill(legacy) error = %v", err)
	}
	if !reflect.DeepEqual(back, protocol.Kill{Name: "main"}) {
		t.Fatalf("legacy round trip = %#v, want named kill", back)
	}
	assertAllPrefixesFail(t, legacy, UnmarshalKill)
	assertTrailingGarbageFails(t, append(append([]byte(nil), legacy...), 0x00), UnmarshalKill)
}

func TestSessionInfoRecoveryState(t *testing.T) {
	tests := []struct {
		name  string
		state protocol.SessionState
		want  []byte
	}{
		{name: "up", state: protocol.SessionUp, want: []byte{0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
		{name: "down", state: protocol.SessionDown, want: []byte{0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1}},
		{name: "broken", state: protocol.SessionBroken, want: []byte{0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := MarshalSessions(protocol.Sessions{Sessions: []protocol.SessionInfo{{State: tt.state}}})
			require.Equal(t, tt.want, payload)
			got, err := UnmarshalSessions(payload)
			require.NoError(t, err)
			require.Equal(t, tt.state, got.Sessions[0].State)
			assertAllPrefixesFail(t, payload, UnmarshalSessions)
			assertTrailingGarbageFails(t, payload, UnmarshalSessions)
		})
	}
	for _, state := range []byte{3, 255} {
		payload := append([]byte(nil), tests[0].want...)
		payload[len(payload)-1] = state
		_, err := UnmarshalSessions(payload)
		require.Error(t, err, "state %d", state)
	}
}

func TestSessionsGoldenAndRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		msg  protocol.Sessions
		want []byte
	}{
		{
			name: "empty",
			msg:  protocol.Sessions{},
			want: []byte{0x00, 0x00},
		},
		{
			name: "two",
			msg: protocol.Sessions{Sessions: []protocol.SessionInfo{
				{SessionID: "0", Name: "0", State: protocol.SessionUp, Ephemeral: true, Tabs: 1, Attached: false},
				{SessionID: "work", Name: "proj", State: protocol.SessionDown, Ephemeral: false, Tabs: 5, Attached: true},
			}},
			want: []byte{
				0x00, 0x02,
				0x00, 0x01, 0x30, 0x00, 0x01, 0x30, 0x01, 0x00, 0x01, 0x00, 0x00,
				0x00, 0x04, 0x77, 0x6f, 0x72, 0x6b, 0x00, 0x04, 0x70, 0x72, 0x6f, 0x6a, 0x00, 0x00, 0x05, 0x01, 0x01,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MarshalSessions(tt.msg)
			if !bytes.Equal(got, tt.want) {
				t.Fatalf("MarshalSessions() = %#v, want %#v", got, tt.want)
			}
			back, err := UnmarshalSessions(got)
			if err != nil {
				t.Fatalf("UnmarshalSessions() error = %v", err)
			}
			// Normalize nil vs empty slice for comparison.
			if len(back.Sessions) == 0 && len(tt.msg.Sessions) == 0 {
				return
			}
			if !reflect.DeepEqual(back, tt.msg) {
				t.Fatalf("round trip = %#v, want %#v", back, tt.msg)
			}
		})
	}

	full := MarshalSessions(tests[1].msg)
	assertAllPrefixesFail(t, full, UnmarshalSessions)
	_, err := UnmarshalSessions([]byte{0xff, 0xff})
	require.ErrorIs(t, err, errShortPayload)
	assertTrailingGarbageFails(t, full, UnmarshalSessions)
}

// TestMsgTypeConstantsDistinct guards the enumerations defined in this
// package against accidental collisions (Intent, ErrorMsg code, Detached
// reason are each independent small spaces, but duplicates within one
// space would silently corrupt the protocol).
func TestMsgTypeConstantsDistinct(t *testing.T) {
	t.Run("intents", func(t *testing.T) {
		vals := map[uint8]string{
			protocol.IntentEphemeral: "IntentEphemeral",
			protocol.IntentNew:       "IntentNew",
			protocol.IntentAttach:    "IntentAttach",
			protocol.IntentResume:    "IntentResume",
		}
		if len(vals) != 4 {
			t.Fatalf("expected 4 distinct intent values, got %d", len(vals))
		}
	})

	t.Run("error codes", func(t *testing.T) {
		vals := map[uint16]string{
			protocol.ErrVersionMismatch:    "ErrVersionMismatch",
			protocol.ErrNoSuchSession:      "ErrNoSuchSession",
			protocol.ErrNameTaken:          "ErrNameTaken",
			protocol.ErrServerShutdown:     "ErrServerShutdown",
			protocol.ErrInvalidSessionName: "ErrInvalidSessionName",
			protocol.ErrInternal:           "ErrInternal",
		}
		if len(vals) != 6 {
			t.Fatalf("expected 6 distinct error codes, got %d", len(vals))
		}
	})

	t.Run("detached reasons", func(t *testing.T) {
		vals := map[uint8]string{
			protocol.ReasonDetach:         "ReasonDetach",
			protocol.ReasonSessionKilled:  "ReasonSessionKilled",
			protocol.ReasonServerShutdown: "ReasonServerShutdown",
			protocol.ReasonReplaced:       "ReasonReplaced",
		}
		if len(vals) != 4 {
			t.Fatalf("expected 4 distinct detached reasons, got %d", len(vals))
		}
	})
}

func TestHelloOutputWindowByteExactValues(t *testing.T) {
	for _, window := range []uint8{0, 1, 8} {
		t.Run(fmt.Sprintf("window_%d", window), func(t *testing.T) {
			hello := protocol.Hello{Version: 14, Size: domain.Size{Cols: 1, Rows: 1}, MaxOutputInFlight: window}
			// The empty Hello has a fixed 1x1 size and zero pixel geometry before the negotiated output window.
			want := append(make([]byte, 42), window, 0, 0, 0, 0, 0, 0)
			want = appendNoNavigationTail(want)
			want[1] = 14
			want[30], want[32] = 1, 1
			got := MarshalHello(hello)
			requireBytesEqual(t, want, got)
			back, err := UnmarshalHello(got)
			require.NoError(t, err)
			require.Equal(t, window, back.MaxOutputInFlight)
			assertAllPrefixesFail(t, got, UnmarshalHello)
			assertTrailingGarbageFails(t, got, UnmarshalHello)
		})
	}
}

func requireBytesEqual(t *testing.T, want, got []byte) {
	t.Helper()
	if !bytes.Equal(want, got) {
		t.Fatalf("bytes = %#v, want %#v", got, want)
	}
}
func TestCommandCorrelationGoldenAndStrict(t *testing.T) {
	tests := []struct {
		name string
		msg  protocol.CommandRequest
		want []byte
	}{
		{
			name: "attached",
			msg:  protocol.CommandRequest{Version: protocol.Version, RequestID: 0x0102030405060708, Attached: true, Self: true, Slug: "split-right", Args: []string{"--vertical"}},
			want: []byte{
				0x00, 0x28, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x01, 0x01,
				0x00, 0x0b, 's', 'p', 'l', 'i', 't', '-', 'r', 'i', 'g', 'h', 't',
				0x00, 0x01, 0x00, 0x00, 0x00, 0x0a, '-', '-', 'v', 'e', 'r', 't', 'i', 'c', 'a', 'l',
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			},
		},
		{
			name: "control",
			msg:  protocol.CommandRequest{Version: protocol.Version, Slug: "ls"},
			want: []byte{
				0x00, 0x28, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x02, 'l', 's', 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := MarshalCommandRequest(tt.msg)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
			back, err := UnmarshalCommandRequest(got)
			require.NoError(t, err)
			require.Equal(t, tt.msg, back)
			assertAllPrefixesFail(t, got, UnmarshalCommandRequest)
			assertTrailingGarbageFails(t, got, UnmarshalCommandRequest)
		})
	}

	payload, err := MarshalCommandRequest(tests[0].msg)
	require.NoError(t, err)
	for _, offset := range []int{10, 11, len(payload) - 1} {
		malformed := append([]byte(nil), payload...)
		malformed[offset] = 2
		_, err := UnmarshalCommandRequest(malformed)
		require.Error(t, err)
	}
}

func TestCommandResultCorrelationGoldenAndStrict(t *testing.T) {
	msg := protocol.CommandResult{RequestID: 0x0102030405060708, OK: true, Code: 7, Text: "ok", Output: "result"}
	want := []byte{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x01, 0x00, 0x07, 0x00, 0x02, 'o', 'k', 0x00, 0x00, 0x00, 0x06, 'r', 'e', 's', 'u', 'l', 't',
	}
	got := MarshalCommandResult(msg)
	require.Equal(t, want, got)
	back, err := UnmarshalCommandResult(got)
	require.NoError(t, err)
	require.Equal(t, msg, back)
	assertAllPrefixesFail(t, got, UnmarshalCommandResult)
	assertTrailingGarbageFails(t, got, UnmarshalCommandResult)

	malformed := append([]byte(nil), got...)
	malformed[8] = 2
	_, err = UnmarshalCommandResult(malformed)
	require.Error(t, err)
}

func TestOutputResetRequestStrict(t *testing.T) {
	got := MarshalOutputResetRequest(protocol.OutputResetRequest{})
	require.Empty(t, got)
	back, err := UnmarshalOutputResetRequest(got)
	require.NoError(t, err)
	require.Equal(t, protocol.OutputResetRequest{}, back)
	assertTrailingGarbageFails(t, got, UnmarshalOutputResetRequest)
}
