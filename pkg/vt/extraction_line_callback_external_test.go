package vt_test

import (
	"testing"

	"github.com/bnema/vev/pkg/vt"
	"github.com/bnema/vev/pkg/vtcore"
)

func TestExternalLineEvictionCallbackReceivesStableCopy(t *testing.T) {
	screen := vt.NewScreen(4, 3)
	inWrite := false
	var evicted [][]vtcore.Cell
	screen.OnLineEvicted = func(row []vtcore.Cell) {
		if !inWrite {
			t.Error("line eviction callback ran after Write returned")
		}
		evicted = append(evicted, row)
	}

	inWrite = true
	screen.Write([]byte("AAAA\r\nBBBB\r\nCCCC\r\n"))
	inWrite = false

	if len(evicted) != 1 || len(evicted[0]) != 4 || evicted[0][0].Rune != 'A' {
		t.Fatalf("evicted rows = %#v, want one stable AAAA row", evicted)
	}
	evicted[0][0] = vtcore.Cell{Rune: 'X'}
	if got := screen.Frame.At(0, 0).Rune; got != 'B' {
		t.Fatalf("mutating callback row changed live frame to %q", got)
	}
	if got := evicted[0][0].Rune; got != 'X' {
		t.Fatalf("callback row was not retained as a stable copy: %q", got)
	}
}
