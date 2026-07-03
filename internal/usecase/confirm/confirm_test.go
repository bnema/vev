package confirm

import (
	"bytes"
	"strings"
	"testing"
)

func TestConfirmerAcceptsYes(t *testing.T) {
	var out bytes.Buffer
	ok, err := NewConfirmer(strings.NewReader("y\n"), &out).Confirm("Kill daemon?")
	if err != nil {
		t.Fatalf("Confirm returned error: %v", err)
	}
	if !ok {
		t.Fatal("Confirm returned false, want true")
	}
	if got := out.String(); !strings.Contains(got, "Kill daemon?") || !strings.Contains(got, "[y/N]") {
		t.Fatalf("prompt output = %q, want question and default", got)
	}
}

func TestConfirmerDefaultsNo(t *testing.T) {
	ok, err := NewConfirmer(strings.NewReader("\n"), &bytes.Buffer{}).Confirm("Kill daemon?")
	if err != nil {
		t.Fatalf("Confirm returned error: %v", err)
	}
	if ok {
		t.Fatal("Confirm returned true for empty answer, want false")
	}
}

func TestConfirmerRejectsUnknownAnswer(t *testing.T) {
	ok, err := NewConfirmer(strings.NewReader("later\n"), &bytes.Buffer{}).Confirm("Kill daemon?")
	if err != nil {
		t.Fatalf("Confirm returned error: %v", err)
	}
	if ok {
		t.Fatal("Confirm returned true for unknown answer, want false")
	}
}
