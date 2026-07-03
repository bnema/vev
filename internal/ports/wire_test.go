package ports

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/bnema/vev/internal/domain"
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
				Version: 1,
				Intent:  IntentNew,
				Name:    "w0",
				Size:    domain.Size{Cols: 80, Rows: 24},
				TermEnv: "xterm-256color",
				Cwd:     "/tmp/project",
			},
			want: []byte{0x00, 0x01, 0x01, 0x00, 0x02, 0x77, 0x30, 0x00, 0x50, 0x00, 0x18, 0x00, 0x0e, 0x78, 0x74, 0x65, 0x72, 0x6d, 0x2d, 0x32, 0x35, 0x36, 0x63, 0x6f, 0x6c, 0x6f, 0x72, 0x00, 0x0c, 0x2f, 0x74, 0x6d, 0x70, 0x2f, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74},
		},
		{
			name: "empty strings",
			msg: Hello{
				Version: 1,
				Intent:  IntentEphemeral,
				Name:    "",
				Size:    domain.Size{Cols: 0, Rows: 0},
				TermEnv: "",
				Cwd:     "",
			},
			want: []byte{0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
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
		})
	}

	full := MarshalHello(tests[0].msg)
	assertAllPrefixesFail(t, full, UnmarshalHello)
	assertTrailingGarbageFails(t, full, UnmarshalHello)
}

func TestInputGoldenAndRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		msg  Input
		want []byte
	}{
		{name: "data", msg: Input{Data: []byte("hi")}, want: []byte{0x68, 0x69}},
		{name: "empty", msg: Input{Data: nil}, want: []byte{}},
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

	// Input is rest-of-payload: any byte sequence, including a truncated
	// one, is a valid (if empty or partial) Data value. No error path
	// exists to exercise beyond "never panics".
	mustNotPanic(t, func() {
		if _, err := UnmarshalInput([]byte{0x01}); err != nil {
			t.Fatalf("UnmarshalInput() unexpected error = %v", err)
		}
	})
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
			msg:  Welcome{SessionID: "sess-1", SessionName: "main", Ephemeral: true},
			want: []byte{0x00, 0x06, 0x73, 0x65, 0x73, 0x73, 0x2d, 0x31, 0x00, 0x04, 0x6d, 0x61, 0x69, 0x6e, 0x01},
		},
		{
			name: "non-ephemeral empty name",
			msg:  Welcome{SessionID: "abc", SessionName: "", Ephemeral: false},
			want: []byte{0x00, 0x03, 0x61, 0x62, 0x63, 0x00, 0x00, 0x00},
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
	msg := Output{Data: []byte("hello\n")}
	want := []byte{0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x0a}

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
				{SessionID: "work", Name: "proj", Ephemeral: false, Tabs: 5, Attached: true},
			}},
			want: []byte{
				0x00, 0x02,
				0x00, 0x01, 0x30, 0x00, 0x01, 0x30, 0x01, 0x00, 0x01, 0x00,
				0x00, 0x04, 0x77, 0x6f, 0x72, 0x6b, 0x00, 0x04, 0x70, 0x72, 0x6f, 0x6a, 0x00, 0x00, 0x05, 0x01,
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
		}
		if len(vals) != 3 {
			t.Fatalf("expected 3 distinct intent values, got %d", len(vals))
		}
	})

	t.Run("error codes", func(t *testing.T) {
		vals := map[uint16]string{
			ErrVersionMismatch: "ErrVersionMismatch",
			ErrNoSuchSession:   "ErrNoSuchSession",
			ErrNameTaken:       "ErrNameTaken",
			ErrInternal:        "ErrInternal",
		}
		if len(vals) != 4 {
			t.Fatalf("expected 4 distinct error codes, got %d", len(vals))
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
