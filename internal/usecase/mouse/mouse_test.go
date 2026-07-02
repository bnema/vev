package mouse

import (
	"bytes"
	"reflect"
	"testing"
)

type scanToken struct {
	kind  string
	data  []byte
	event Event
}

func collectScan(s *Scanner, chunks ...[]byte) []scanToken {
	var tokens []scanToken
	for _, chunk := range chunks {
		s.Scan(chunk, func(ev Event) {
			tokens = append(tokens, scanToken{kind: "mouse", event: ev})
		}, func(b []byte) {
			tokens = append(tokens, scanToken{kind: "bytes", data: append([]byte(nil), b...)})
		})
	}
	return tokens
}

func TestScanDecodesWheelClickAndDrag(t *testing.T) {
	tests := []struct {
		name string
		in   []byte
		want Event
	}{
		{
			name: "wheel up",
			in:   []byte("\x1b[<64;10;20M"),
			want: Event{Type: Press, Button: WheelUp, Col: 9, Row: 19, Raw: []byte("\x1b[<64;10;20M")},
		},
		{
			name: "wheel down",
			in:   []byte("\x1b[<65;3;4M"),
			want: Event{Type: Press, Button: WheelDown, Col: 2, Row: 3, Raw: []byte("\x1b[<65;3;4M")},
		},
		{
			name: "left press",
			in:   []byte("\x1b[<0;1;2M"),
			want: Event{Type: Press, Button: Left, Col: 0, Row: 1, Raw: []byte("\x1b[<0;1;2M")},
		},
		{
			name: "left release",
			in:   []byte("\x1b[<0;1;2m"),
			want: Event{Type: Release, Button: Left, Col: 0, Row: 1, Raw: []byte("\x1b[<0;1;2m")},
		},
		{
			name: "right motion",
			in:   []byte("\x1b[<34;7;8M"),
			want: Event{Type: Motion, Button: Right, Col: 6, Row: 7, Raw: []byte("\x1b[<34;7;8M")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tokens := collectScan(&Scanner{}, tt.in)
			if len(tokens) != 1 || tokens[0].kind != "mouse" {
				t.Fatalf("tokens = %#v, want one mouse token", tokens)
			}
			if !reflect.DeepEqual(tokens[0].event, tt.want) {
				t.Fatalf("event = %#v, want %#v", tokens[0].event, tt.want)
			}
		})
	}
}

func TestScanCoalescedEvents(t *testing.T) {
	tokens := collectScan(&Scanner{}, []byte("\x1b[<0;1;1M\x1b[<65;2;3M"))
	if len(tokens) != 2 || tokens[0].kind != "mouse" || tokens[1].kind != "mouse" {
		t.Fatalf("tokens = %#v, want two mouse tokens", tokens)
	}
	if tokens[0].event.Button != Left || tokens[1].event.Button != WheelDown {
		t.Fatalf("buttons = %v, %v; want Left, WheelDown", tokens[0].event.Button, tokens[1].event.Button)
	}
}

func TestScanBuffersSplitReportAfterIntroducer(t *testing.T) {
	var s Scanner
	first := collectScan(&s, []byte("\x1b[<64;"))
	if len(first) != 0 {
		t.Fatalf("first scan tokens = %#v, want none while pending", first)
	}
	second := collectScan(&s, []byte("5;6M"))
	if len(second) != 1 || second[0].kind != "mouse" {
		t.Fatalf("second scan tokens = %#v, want one mouse token", second)
	}
	want := Event{Type: Press, Button: WheelUp, Col: 4, Row: 5, Raw: []byte("\x1b[<64;5;6M")}
	if !reflect.DeepEqual(second[0].event, want) {
		t.Fatalf("event = %#v, want %#v", second[0].event, want)
	}
}

func TestScanPreservesInterleavedOrdering(t *testing.T) {
	tokens := collectScan(&Scanner{}, []byte("a\x1b[<0;1;1Mb\x1b[<65;2;2Mc"))
	want := []scanToken{
		{kind: "bytes", data: []byte("a")},
		{kind: "mouse", event: Event{Type: Press, Button: Left, Col: 0, Row: 0, Raw: []byte("\x1b[<0;1;1M")}},
		{kind: "bytes", data: []byte("b")},
		{kind: "mouse", event: Event{Type: Press, Button: WheelDown, Col: 1, Row: 1, Raw: []byte("\x1b[<65;2;2M")}},
		{kind: "bytes", data: []byte("c")},
	}
	if !reflect.DeepEqual(tokens, want) {
		t.Fatalf("tokens = %#v, want %#v", tokens, want)
	}
}

func TestScanMalformedBailsToBytes(t *testing.T) {
	tests := [][]byte{
		[]byte("\x1b[<x"),
		[]byte("\x1b[<0;;1M"),
		[]byte("\x1b[<0;0;1M"),
		[]byte("\x1b[<0;1M"),
	}
	for _, in := range tests {
		t.Run(string(bytes.ReplaceAll(in, []byte("\x1b"), []byte("ESC"))), func(t *testing.T) {
			tokens := collectScan(&Scanner{}, in)
			if len(tokens) != 1 || tokens[0].kind != "bytes" || !bytes.Equal(tokens[0].data, in) {
				t.Fatalf("tokens = %#v, want one byte token %q", tokens, in)
			}
		})
	}
}

func TestScanPassesThroughBareESCAndESCBracket(t *testing.T) {
	tests := [][]byte{
		[]byte("\x1b"),
		[]byte("\x1b["),
		[]byte("x\x1b"),
		[]byte("x\x1b["),
	}
	for _, in := range tests {
		t.Run(string(bytes.ReplaceAll(in, []byte("\x1b"), []byte("ESC"))), func(t *testing.T) {
			tokens := collectScan(&Scanner{}, in)
			if len(tokens) != 1 || tokens[0].kind != "bytes" || !bytes.Equal(tokens[0].data, in) {
				t.Fatalf("tokens = %#v, want passthrough %q", tokens, in)
			}
		})
	}
}
