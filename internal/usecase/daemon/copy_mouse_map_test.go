package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	scopy "github.com/bnema/vev/internal/usecase/copy"
	"github.com/bnema/vev/internal/usecase/mouse"
	"github.com/bnema/vev/pkg/renderer"
)

func TestCopyMouseMapTopBarAndContent(t *testing.T) {
	p := newPane("pane", nil, domain.Size{Cols: 5, Rows: 2})
	doc := scopy.NewDocument(scopy.NewSnapshotFromRows([][]renderer.Cell{testRow("alpha"), testRow("bravo")}, 5, 2), domain.DefaultWordSeparators)
	geometry := copyMouseGeometry{pane: p, content: domain.Rect{X: 0, Y: 1, Width: 5, Height: 2}}

	_, ok := mapCopyMouse(mouse.Event{Col: 0, Row: 0}, geometry, 0, doc, false)
	require.False(t, ok)
	mapped, ok := mapCopyMouse(mouse.Event{Col: 0, Row: 1}, geometry, 0, doc, false)
	require.True(t, ok)
	require.Same(t, p, mapped.pane)
	require.Equal(t, scopy.Pos{Row: 0, Col: 0}, mapped.pos)
	mapped, ok = mapCopyMouse(mouse.Event{Col: 4, Row: 2}, geometry, 0, doc, false)
	require.True(t, ok)
	require.Equal(t, scopy.Pos{Row: 1, Col: 4}, mapped.pos)
	_, ok = mapCopyMouse(mouse.Event{Col: 0, Row: 3}, geometry, 0, doc, false)
	require.False(t, ok, "bottom status is not content")
}

func TestCopyMouseMapClampsAndNormalizesWideCells(t *testing.T) {
	p := newPane("pane", nil, domain.Size{Cols: 4, Rows: 1})
	doc := scopy.NewDocument(scopy.NewSnapshotFromRows([][]renderer.Cell{{{Rune: 'a'}, {Rune: '界'}, {Continuation: true}, {Rune: 'b'}}}, 4, 1), domain.DefaultWordSeparators)
	geometry := copyMouseGeometry{pane: p, content: domain.Rect{X: 3, Y: 1, Width: 4, Height: 1}}

	mapped, ok := mapCopyMouse(mouse.Event{Col: 5, Row: 1}, geometry, 0, doc, false)
	require.True(t, ok)
	require.Equal(t, scopy.Pos{Row: 0, Col: 1}, mapped.pos)
	mapped, ok = mapCopyMouse(mouse.Event{Col: 99, Row: 99}, geometry, 0, doc, true)
	require.True(t, ok)
	require.Equal(t, scopy.Pos{Row: 0, Col: 3}, mapped.pos)
}

func TestCopyMouseMapSplitAndFloatingRects(t *testing.T) {
	p := newPane("pane", nil, domain.Size{Cols: 4, Rows: 2})
	doc := scopy.NewDocument(scopy.NewSnapshotFromRows([][]renderer.Cell{testRow("abcd"), testRow("efgh")}, 4, 2), domain.DefaultWordSeparators)
	for _, tc := range []struct {
		name     string
		geometry domain.Rect
		ev       mouse.Event
		want     scopy.Pos
		ok       bool
	}{
		{"split content", domain.Rect{X: 21, Y: 2, Width: 4, Height: 2}, mouse.Event{Col: 23, Row: 3}, scopy.Pos{Row: 1, Col: 2}, true},
		{"split title", domain.Rect{X: 21, Y: 2, Width: 4, Height: 2}, mouse.Event{Col: 21, Row: 1}, scopy.Pos{}, false},
		{"floating inner", domain.Rect{X: 10, Y: 5, Width: 4, Height: 2}, mouse.Event{Col: 11, Row: 5}, scopy.Pos{Row: 0, Col: 1}, true},
		{"floating border", domain.Rect{X: 10, Y: 5, Width: 4, Height: 2}, mouse.Event{Col: 9, Row: 5}, scopy.Pos{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mapped, ok := mapCopyMouse(tc.ev, copyMouseGeometry{pane: p, content: tc.geometry}, 0, doc, false)
			require.Equal(t, tc.ok, ok)
			if ok {
				require.Equal(t, tc.want, mapped.pos)
			}
		})
	}
}

func TestCopyMouseGeometryUsesClientFrameExactlyOnce(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, sess, _, _ := newManualSessionWithPTYs(t, p)
	tb := testAttachmentTab(sess)

	tb.mu.Lock()
	geometry, ok := hitTestCopyMouseGeometryLocked(tb, d.currentFloatingConfig(), 0, 1)
	tb.mu.Unlock()
	require.True(t, ok)
	require.Equal(t, 1, geometry.content.Y)
	_, ok = mapCopyMouse(mouse.Event{Col: 0, Row: 1}, geometry, 0,
		scopy.NewDocument(scopy.NewSnapshotFromRows([][]renderer.Cell{testRow("a")}, 80, 1), domain.DefaultWordSeparators), false)
	require.True(t, ok)
}
