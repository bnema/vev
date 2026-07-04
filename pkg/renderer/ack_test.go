package renderer

import (
	"strings"
	"testing"
)

func TestAdvanceOnAckRediffsScrollDamageUntilAcked(t *testing.T) {
	r := New(Capabilities{AdvancePolicy: AdvanceOnAck})
	frame := NewFrame(4, 3)
	fillFrame(frame, []string{"AAAA", "BBBB", "CCCC"})
	if out, err := r.Draw(frame, nil); err != nil {
		t.Fatal(err)
	} else if len(out) == 0 {
		t.Fatal("initial draw empty")
	}
	if err := r.Ack(frame); err != nil {
		t.Fatal(err)
	}

	scrolled := NewFrame(4, 3)
	copy(scrolled.Cells[0:8], frame.Cells[4:12])
	for i := 8; i < 12; i++ {
		scrolled.Cells[i] = BlankCell()
	}
	scrolled.Set(0, 2, Cell{Rune: 'N', Style: DefaultStyle()})
	damage := []Damage{
		{Kind: DamageScrollUp, X: 0, Y: 0, Width: 4, Height: 3, Count: 1},
		{Kind: DamageText, X: 0, Y: 2, Width: 4, Height: 1},
	}

	first, err := r.Draw(scrolled, damage)
	if err != nil {
		t.Fatal(err)
	}
	again, err := r.Draw(scrolled, damage)
	if err != nil {
		t.Fatal(err)
	}
	for name, out := range map[string][]byte{"first": first, "again": again} {
		if !strings.Contains(string(out), "\x1b[1;3r") || !strings.Contains(string(out), "N") {
			t.Fatalf("%s scroll draw did not include scroll region and exposed row: %q", name, out)
		}
	}
	if err := r.Ack(scrolled); err != nil {
		t.Fatal(err)
	}
	afterAck, err := r.Draw(scrolled, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(afterAck) != 0 {
		t.Fatalf("after ack got %q", afterAck)
	}
}

func TestAdvanceOnAckRediffsLostOutput(t *testing.T) {
	r := New(Capabilities{AdvancePolicy: AdvanceOnAck})
	f := NewFrame(3, 1)
	fillFrame(f, []string{"abc"})
	first, err := r.Draw(f, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) == 0 {
		t.Fatal("first draw empty")
	}
	again, err := r.Draw(f, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) == 0 {
		t.Fatal("lost unacked output was not re-diffed")
	}
	if err := r.Ack(f); err != nil {
		t.Fatal(err)
	}
	afterAck, err := r.Draw(f, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(afterAck) != 0 {
		t.Fatalf("after ack got %q", afterAck)
	}
}
