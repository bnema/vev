package snapshot

import (
	"errors"
	"testing"

	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/pkg/renderer"
)

func TestPreflightRejectsDanglingTreeAndFocusReferencesBeforeDecode(t *testing.T) {
	sealed, tail := historyBlobs(t, nil)
	visible := visibleBlob(t, [][]renderer.Cell{{{Rune: 'v'}}})
	for _, tt := range []struct {
		name string
		tab  Tab
	}{
		{
			name: "tree leaf",
			tab: Tab{
				Focus: "pane",
				Tree:  layout.NewTree("missing"),
				Panes: []Pane{{ID: "pane", SealedChunks: sealed, Tail: tail, Visible: visible}},
			},
		},
		{
			name: "tab focus",
			tab: Tab{
				Focus: "missing",
				Tree:  layout.NewTree("pane"),
				Panes: []Pane{{ID: "pane", SealedChunks: sealed, Tail: tail, Visible: visible}},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := Marshal(Session{Name: "hostile", Tabs: []Tab{tt.tab}})
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			if err := preflightSession(encoded[16:]); !errors.Is(err, ErrUnknownPane) {
				t.Fatalf("preflightSession() error = %v, want %v", err, ErrUnknownPane)
			}
		})
	}
}
