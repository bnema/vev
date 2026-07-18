package ports

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/pkg/renderer"
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

func TestHelloGoldenAndRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		msg  Hello
		want []byte
	}{
		{
			name: "typical",
			msg: Hello{
				Version:           1,
				Intent:            IntentNew,
				ClientID:          [16]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10},
				ResumeToken:       0x0102030405060708,
				Name:              "w0",
				Size:              domain.Size{Cols: 80, Rows: 24},
				TermEnv:           "xterm-256color",
				Cwd:               "/tmp/project",
				TrueColor:         true,
				MaxOutputInFlight: 8,
			},
			want: []byte{0x00, 0x01, 0x01, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x02, 0x77, 0x30, 0x00, 0x50, 0x00, 0x18, 0x00, 0x0e, 0x78, 0x74, 0x65, 0x72, 0x6d, 0x2d, 0x32, 0x35, 0x36, 0x63, 0x6f, 0x6c, 0x6f, 0x72, 0x00, 0x0c, 0x2f, 0x74, 0x6d, 0x70, 0x2f, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x01, 0x08, 0x00, 0x00, 0x00, 0x00},
		},
		{
			name: "empty strings",
			msg: Hello{
				Version:   1,
				Intent:    IntentEphemeral,
				Name:      "",
				Size:      domain.Size{Cols: 0, Rows: 0},
				TermEnv:   "",
				Cwd:       "",
				TrueColor: false,
			},
			want: []byte{0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MarshalHello(tt.msg)
			if !bytes.Equal(got, tt.want) {
				t.Fatalf("MarshalHello() = %#v, want %#v", got, tt.want)
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
		got := MarshalHello(Hello{
			Version: ProtocolVersion,
			Env:     []string{"A=B", "XY=123"},
		})
		want := []byte{
			0x00, 0x10, 0x00, // version, intent
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // client ID
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // resume token
			0x00, 0x00, // name
			0x00, 0x00, 0x00, 0x00, // size
			0x00, 0x00, // TERM
			0x00, 0x00, // cwd
			0x00, 0x00, // true color, max output in flight
			0x00, 0x00, 0x00, 0x02, // uint32 environment entry count
			0x00, 0x00, 0x00, 0x03, 'A', '=', 'B',
			0x00, 0x00, 0x00, 0x06, 'X', 'Y', '=', '1', '2', '3',
		}
		require.Equal(t, want, got)
		assertAllPrefixesFail(t, got, UnmarshalHello)
		assertTrailingGarbageFails(t, got, UnmarshalHello)
	})

	t.Run("lossless order and values", func(t *testing.T) {
		entryOverUint16 := "LARGE=" + string(bytes.Repeat([]byte("x"), 65536))
		want := []string{"A=first", "TOKEN=a=b=c", "EMPTY=", entryOverUint16, "A=second"}
		payload := MarshalHello(Hello{Version: ProtocolVersion, Env: want})
		got, err := UnmarshalHello(payload)
		require.NoError(t, err)
		require.Equal(t, want, got.Env)
	})

	t.Run("empty", func(t *testing.T) {
		payload := MarshalHello(Hello{Version: ProtocolVersion, Env: []string{}})
		got, err := UnmarshalHello(payload)
		require.NoError(t, err)
		require.Empty(t, got.Env)
	})

	base := MarshalHello(Hello{Version: ProtocolVersion})
	withCount := func(count byte) []byte {
		payload := append([]byte(nil), base...)
		payload[len(payload)-1] = count
		return payload
	}
	tests := []struct {
		name    string
		payload []byte
	}{
		{name: "truncated count", payload: base[:len(base)-1]},
		{name: "truncated entry length", payload: append(withCount(1), 0, 0)},
		{name: "truncated entry body", payload: append(withCount(1), 0, 0, 0, 3, 'x')},
		{name: "impossible count", payload: withCount(2)},
		{name: "trailing garbage", payload: append(append([]byte(nil), base...), 0xff)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := UnmarshalHello(tt.payload)
			require.Error(t, err)
		})
	}
}

func TestInputGoldenAndRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		msg  Input
		want []byte
	}{
		{name: "data", msg: Input{InputSeq: 0x0102030405060708, Data: []byte("hi")}, want: []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x68, 0x69}},
		{name: "empty", msg: Input{InputSeq: 0, Data: nil}, want: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
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
	assertAllPrefixesFail(t, full[:8], UnmarshalInput)
}

func TestImagePushGoldenAndRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		msg  ImagePush
		want []byte
	}{
		{
			name: "png data",
			msg:  ImagePush{InputSeq: 7, Mime: "image/png", Data: []byte{0x01, 0x02, 0x03}},
			want: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x09, 0x69, 0x6d, 0x61, 0x67, 0x65, 0x2f, 0x70, 0x6e, 0x67, 0x01, 0x02, 0x03},
		},
		{name: "empty", msg: ImagePush{}, want: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
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
		{R: 0x16, G: 0x17, B: 0x18}, {R: 0x19, G: 0x1a, B: 0x1b},
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
		msg  Theme
		want []byte
	}{
		{
			name: "empty",
			msg:  Theme{},
			want: zeroes,
		},
		{
			name: "foreground only",
			msg: Theme{
				HasForeground: true,
				Foreground:    renderer.RGB{R: 10, G: 20, B: 30},
				Background:    renderer.RGB{R: 40, G: 50, B: 60},
			},
			want: append([]byte{0x01, 0x0a, 0x14, 0x1e, 0x28, 0x32, 0x3c}, zeroes[7:]...),
		},
		{
			name: "light flag without known scheme",
			msg: Theme{
				Light: true,
			},
			want: append([]byte{0x10}, zeroes[1:]...),
		},
		{
			name: "full palette",
			msg: Theme{
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
				0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
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
	cleared := Theme{
		HasForeground: true,
		Foreground:    renderer.RGB{R: 1, G: 2, B: 3},
		HasBackground: true,
		Background:    renderer.RGB{R: 4, G: 5, B: 6},
		TrueColor:     true,
		SchemeKnown:   true,
	}
	want := append([]byte{0x0f, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x00, 0x00}, make([]byte, 48)...)
	require.Equal(t, want, MarshalTheme(cleared))
	require.Equal(t, uint16(16), ProtocolVersion)
}

func TestResizeGoldenAndRoundTrip(t *testing.T) {
	msg := Resize{Size: domain.Size{Cols: 100, Rows: 40}}
	want := []byte{0x00, 0x64, 0x00, 0x28}

	got := MarshalResize(msg)
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
	got := MarshalDetach(Detach{})
	if len(got) != 0 {
		t.Fatalf("MarshalDetach() = %#v, want empty", got)
	}
	back, err := UnmarshalDetach(got)
	if err != nil {
		t.Fatalf("UnmarshalDetach() error = %v", err)
	}
	if back != (Detach{}) {
		t.Fatalf("round trip = %#v, want %#v", back, Detach{})
	}
	assertTrailingGarbageFails(t, got, UnmarshalDetach)
}

func TestPingGoldenAndRoundTrip(t *testing.T) {
	got := MarshalPing(Ping{})
	if len(got) != 0 {
		t.Fatalf("MarshalPing() = %#v, want empty", got)
	}
	back, err := UnmarshalPing(got)
	if err != nil {
		t.Fatalf("UnmarshalPing() error = %v", err)
	}
	if back != (Ping{}) {
		t.Fatalf("round trip = %#v, want %#v", back, Ping{})
	}
	assertTrailingGarbageFails(t, got, UnmarshalPing)
}

func TestPongGoldenAndRoundTrip(t *testing.T) {
	got := MarshalPong(Pong{})
	if len(got) != 0 {
		t.Fatalf("MarshalPong() = %#v, want empty", got)
	}
	back, err := UnmarshalPong(got)
	if err != nil {
		t.Fatalf("UnmarshalPong() error = %v", err)
	}
	if back != (Pong{}) {
		t.Fatalf("round trip = %#v, want %#v", back, Pong{})
	}
	assertTrailingGarbageFails(t, got, UnmarshalPong)
}

func TestWelcomeGoldenAndRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		msg  Welcome
		want []byte
	}{
		{
			name: "ephemeral",
			msg:  Welcome{SessionID: "sess-1", SessionName: "main", Ephemeral: true, ResumeToken: 0x0102030405060708, Capabilities: CapabilityResume | CapabilityPredict},
			want: []byte{0x00, 0x06, 0x73, 0x65, 0x73, 0x73, 0x2d, 0x31, 0x00, 0x04, 0x6d, 0x61, 0x69, 0x6e, 0x01, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x05},
		},
		{
			name: "non-ephemeral empty name",
			msg:  Welcome{SessionID: "abc", SessionName: "", Ephemeral: false},
			want: []byte{0x00, 0x03, 0x61, 0x62, 0x63, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
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

func TestErrorMsgGoldenAndRoundTrip(t *testing.T) {
	msg := ErrorMsg{Code: ErrNameTaken, Text: "name taken"}
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
	msg := Output{BaseStateNum: 1, NewStateNum: 2, EchoAck: 3, Data: []byte("hello\n")}
	want := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x0a}

	got := MarshalOutput(msg)
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

	assertAllPrefixesFail(t, got[:24], UnmarshalOutput)
}

func TestAckGoldenAndRoundTrip(t *testing.T) {
	msg := Ack{AckedStateNum: 0x0102030405060708}
	want := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}

	got := MarshalAck(msg)
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
	msg := Detached{Reason: ReasonSessionKilled}
	want := []byte{0x01}

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

func TestListGoldenAndRoundTrip(t *testing.T) {
	got := MarshalList(List{})
	if len(got) != 0 {
		t.Fatalf("MarshalList() = %#v, want empty", got)
	}
	back, err := UnmarshalList(got)
	if err != nil {
		t.Fatalf("UnmarshalList() error = %v", err)
	}
	if back != (List{}) {
		t.Fatalf("round trip = %#v, want %#v", back, List{})
	}
	assertTrailingGarbageFails(t, got, UnmarshalList)
}

func TestKillGoldenAndRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		msg  Kill
		want []byte
	}{
		{name: "named", msg: Kill{Name: "main"}, want: []byte{0x00, 0x04, 0x6d, 0x61, 0x69, 0x6e}},
		{name: "empty", msg: Kill{Name: ""}, want: []byte{0x00, 0x00}},
		{name: "all", msg: Kill{All: true}, want: []byte{0x00, 0x00, 0x01}},
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
	if !reflect.DeepEqual(back, Kill{Name: "main"}) {
		t.Fatalf("legacy round trip = %#v, want named kill", back)
	}
	assertAllPrefixesFail(t, legacy, UnmarshalKill)
	assertTrailingGarbageFails(t, append(append([]byte(nil), legacy...), 0x00), UnmarshalKill)
}

func TestSessionsGoldenAndRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		msg  Sessions
		want []byte
	}{
		{
			name: "empty",
			msg:  Sessions{},
			want: []byte{0x00, 0x00},
		},
		{
			name: "two",
			msg: Sessions{Sessions: []SessionInfo{
				{SessionID: "0", Name: "0", Ephemeral: true, Tabs: 1, Attached: false},
				{SessionID: "work", Name: "proj", Ephemeral: false, Tabs: 5, Attached: true, Stopped: true},
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
	assertTrailingGarbageFails(t, full, UnmarshalSessions)
}

// TestMsgTypeConstantsDistinct guards the enumerations defined in this
// package against accidental collisions (Intent, ErrorMsg code, Detached
// reason are each independent small spaces, but duplicates within one
// space would silently corrupt the protocol).
func TestMsgTypeConstantsDistinct(t *testing.T) {
	t.Run("intents", func(t *testing.T) {
		vals := map[uint8]string{
			IntentEphemeral: "IntentEphemeral",
			IntentNew:       "IntentNew",
			IntentAttach:    "IntentAttach",
			IntentResume:    "IntentResume",
		}
		if len(vals) != 4 {
			t.Fatalf("expected 4 distinct intent values, got %d", len(vals))
		}
	})

	t.Run("error codes", func(t *testing.T) {
		vals := map[uint16]string{
			ErrVersionMismatch:    "ErrVersionMismatch",
			ErrNoSuchSession:      "ErrNoSuchSession",
			ErrNameTaken:          "ErrNameTaken",
			ErrServerShutdown:     "ErrServerShutdown",
			ErrInvalidSessionName: "ErrInvalidSessionName",
			ErrInternal:           "ErrInternal",
		}
		if len(vals) != 6 {
			t.Fatalf("expected 6 distinct error codes, got %d", len(vals))
		}
	})

	t.Run("detached reasons", func(t *testing.T) {
		vals := map[uint8]string{
			ReasonDetach:         "ReasonDetach",
			ReasonSessionKilled:  "ReasonSessionKilled",
			ReasonServerShutdown: "ReasonServerShutdown",
		}
		if len(vals) != 3 {
			t.Fatalf("expected 3 distinct detached reasons, got %d", len(vals))
		}
	})
}

func TestHelloOutputWindowByteExactValues(t *testing.T) {
	for _, window := range []uint8{0, 1, 8} {
		t.Run(fmt.Sprintf("window_%d", window), func(t *testing.T) {
			hello := Hello{Version: 14, MaxOutputInFlight: window}
			// The empty Hello has 38 zero bytes before the negotiated output window,
			// followed by a uint32 zero environment-entry count.
			want := append(make([]byte, 38), window, 0, 0, 0, 0)
			want[1] = 14
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
