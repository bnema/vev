package client

import (
	ansirenderer "github.com/bnema/vev-vt/ansi"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/picker"
)

// This file owns the client side of the client-picker interaction:
// snapshot admission, local model ownership, and typed commit/cancel.
// It mirrors the inventoryRelay admission discipline
// (navigation_inventory.go) but never canonicalizes row order: picker
// order is semantically significant. The loop consumes terminal-pump
// records, never raw reads; commit sends a typed PickerSelection on the
// ordered control channel, never picker bytes toward the PTY.

// pickerInteraction tracks one client-picker namespace: the admitted
// interaction ID, the latest displayed revision, and the key set that
// revision published. Late snapshots for closed interactions drop;
// stale revisions never step the display backward.
type pickerInteraction struct {
	open        bool
	interaction uint64
	revision    uint64
	keys        map[string]struct{}
	displayed   uint64
}

// setOpen starts or stops the interaction namespace. Opening keeps a fresh
// namespace: admitted keys never cross interactions.
func (p *pickerInteraction) setOpen(open bool, interaction uint64) {
	if p == nil {
		return
	}
	p.open = open
	if open && interaction != p.interaction {
		p.interaction = interaction
		p.revision = 0
		p.displayed = 0
		p.keys = make(map[string]struct{})
	}
	if !open {
		p.keys = nil
	}
}

// admitSnapshot validates one snapshot against the open interaction and
// reports whether it carries a newer revision worth displaying. Older or
// duplicate revisions discard; foreign interactions drop silently.
func (p *pickerInteraction) admitSnapshot(snapshot protocol.PickerSnapshot) bool {
	if p == nil {
		return false
	}
	if protocol.ValidatePickerSnapshot(snapshot) != nil {
		return false
	}
	if !p.open || snapshot.InteractionID != p.interaction {
		return false
	}
	if snapshot.Revision <= p.displayed {
		return false
	}
	if snapshot.Revision < p.revision {
		return false
	}
	p.revision = snapshot.Revision
	p.displayed = snapshot.Revision
	keys := make(map[string]struct{}, len(snapshot.Rows))
	for _, row := range snapshot.Rows {
		keys[row.Key] = struct{}{}
	}
	p.keys = keys
	return true
}

// commitKey checks one commit target against the displayed revision: the
// key must be published at exactly the revision the user acted on.
func (p *pickerInteraction) commitKey(revision uint64, key string) bool {
	if p == nil || !p.open {
		return false
	}
	if revision == 0 || revision != p.displayed {
		return false
	}
	_, ok := p.keys[key]
	return ok
}

// pickerLoop owns one admitted *picker.Model plus the snapshot revision it
// was built from. Cursor, search, and sort are local-only presentation;
// commit and cancel cross the wire as typed messages.
type pickerLoop struct {
	model       *picker.Model
	interaction uint64
	revision    uint64
	sorted      bool
}

// openPickerLoop builds the local model from one admitted snapshot. Views
// are reconstructed display-first: the daemon sent display-ready rows and
// the client rebuilds equivalent SessionViews so picker.New applies the
// same selection geometry as the daemon model.
func openPickerLoop(snapshot protocol.PickerSnapshot) *pickerLoop {
	views := pickerViewsFromSnapshot(snapshot)
	model := picker.New(views, picker.SelectionConfig{Mode: picker.SelectNavigationTab})
	loop := &pickerLoop{model: model, interaction: snapshot.InteractionID, revision: snapshot.Revision}
	loop.restoreCursor(snapshot.Cursor)
	return loop
}

// pickerViewsFromSnapshot rebuilds session views from opaque rows. Each
// row becomes one single-tab session view: the row key round-trips in the
// tab ID namespace reserved for client-picker rows, so commitKey recovers
// the exact opaque key without parsing display text. TargetName stays
// empty — the client never invents navigation authority; commit sends the
// opaque key back and the daemon resolves it. Stopped rows keep a header
// fallback because rowsForSession makes stopped session headers selectable
// for navigate intent; live rows commit through their tab entry.
func pickerViewsFromSnapshot(snapshot protocol.PickerSnapshot) []picker.SessionView {
	views := make([]picker.SessionView, 0, len(snapshot.Rows))
	for _, row := range snapshot.Rows {
		view := picker.SessionView{
			ID:      domain.SessionID("picker-client:" + row.Key),
			Name:    row.Display,
			Stopped: row.Stopped,
			Tabs: []picker.TabEntry{
				{TabID: domain.TabStableID("picker-client:" + row.Key), Name: row.Display, Detail: row.Detail},
			},
		}
		if row.Stopped {
			view.Tabs = nil
		}
		views = append(views, view)
	}
	return views
}

// restoreCursor moves the model cursor to the snapshot cursor by index,
// clamped to the model bounds. The daemon's cursor key identifies the row;
// the index positions it without parsing display text.
func (l *pickerLoop) restoreCursor(cursor protocol.PickerCursor) {
	if l == nil || l.model == nil {
		return
	}
	if cursor.Index < 0 {
		return
	}
	l.model.SelectNearestRow(cursor.Index)
}

// up moves the cursor one focusable row up.
func (l *pickerLoop) up() {
	if l == nil || l.model == nil {
		return
	}
	l.model.Up()
}

// down moves the cursor one focusable row down.
func (l *pickerLoop) down() {
	if l == nil || l.model == nil {
		return
	}
	l.model.Down()
}

// enterSearch begins search mode; text inserts through insert().
func (l *pickerLoop) enterSearch() {
	if l == nil || l.model == nil {
		return
	}
	if !l.model.SearchActive() {
		l.model.EnterSearch()
	}
}

// insert types one rune into the active search, entering search first.
func (l *pickerLoop) insert(r rune) {
	if l == nil || l.model == nil {
		return
	}
	l.enterSearch()
	l.model.InsertSearch(r)
}

// backspace deletes one search rune; Escape handling distinguishes
// search-exit from picker-close at the call site.
func (l *pickerLoop) backspace() {
	if l == nil || l.model == nil {
		return
	}
	l.model.BackspaceSearch()
}

// escape handles one Escape press: exit search when searching, else report
// picker-close. The boolean is true when the caller must send PickerClose.
func (l *pickerLoop) escape() bool {
	if l == nil || l.model == nil {
		return true
	}
	if l.model.SearchActive() {
		l.model.ExitSearch()
		return false
	}
	return true
}

// commitKey returns the opaque key under the cursor for a typed commit.
// The second result is false when no row is selectable there. The key
// round-trips through the reserved tab-ID namespace from
// pickerViewsFromSnapshot, so no display text is ever parsed. Stopped
// rows commit through their selectable session header (session ID
// namespace); live rows commit through their tab entry.
func (l *pickerLoop) commitKey() (string, bool) {
	if l == nil || l.model == nil {
		return "", false
	}
	target, ok := l.model.Selected()
	if !ok {
		return "", false
	}
	const prefix = "picker-client:"
	for _, id := range []string{string(target.TabID), string(target.Session)} {
		if len(id) > len(prefix) && id[:len(prefix)] == prefix {
			return id[len(prefix):], true
		}
	}
	return "", false
}

// pickerRenderer encodes one picker frame to terminal bytes. It owns a
// dedicated ANSI renderer shadow (never the daemon output shadow): the
// client-picker frame is a local presentation overlay, not daemon output,
// so its bytes never enter the output ACK window.
type pickerRenderer struct {
	renderer *ansirenderer.Renderer
	size     domain.Size
}

func newPickerRenderer() *pickerRenderer {
	return &pickerRenderer{}
}

// render composes the loop model into terminal bytes for one display
// refresh. Preview is empty in the pilot: local preview stays
// daemon-composed inside ordinary paints and remote preview stays on the
// RemotePreview path; cursor movement is local-only and notifies nothing.
// A full redraw every refresh keeps the shadow trivially consistent: the
// frame is small and modal, never a PTY stream.
func (r *pickerRenderer) render(loop *pickerLoop, size domain.Size) []byte {
	if r == nil || loop == nil || loop.model == nil {
		return nil
	}
	if size.Cols <= 0 || size.Rows <= 0 {
		return nil
	}
	if r.renderer == nil || r.size != size {
		r.renderer = ansirenderer.New(ansirenderer.Capabilities{})
		r.size = size
	}
	frame := loop.model.Render(size, picker.Preview{})
	data, err := r.renderer.Draw(frame, []ansirenderer.Damage{ansirenderer.FullRedraw()})
	if err != nil {
		return nil
	}
	return data
}

// commitSelection builds the typed commit for the row under the cursor at
// the displayed revision. Zero picker bytes travel toward the PTY: the
// opaque key plus revision ride a PickerSelection on the ordered control
// channel and the daemon resolves them.
func commitSelection(loop *pickerLoop, interaction uint64, causeActionID uint64) (protocol.PickerSelection, bool) {
	if loop == nil {
		return protocol.PickerSelection{}, false
	}
	key, ok := loop.commitKey()
	if !ok {
		return protocol.PickerSelection{}, false
	}
	selection := protocol.PickerSelection{
		CauseActionID: causeActionID, InteractionID: interaction,
		Revision: loop.revision, Key: key,
	}
	if protocol.ValidatePickerSelection(selection) != nil {
		return protocol.PickerSelection{}, false
	}
	return selection, true
}

// pickerDriverOp is one ui-driver operation applied to an open loop. Keys
// drive cursor/search/exit; text types into search. The boolean reports
// whether the caller must send PickerClose (idle Escape); commit and
// failure handling stay at the call site so CauseActionID attribution is
// exact.
func pickerDriverOp(loop *pickerLoop, keys []string, text string) (close bool) {
	if loop == nil || loop.model == nil {
		return false
	}
	for _, key := range keys {
		switch key {
		case "Up":
			loop.up()
		case "Down":
			loop.down()
		case "Enter":
			// Commit is typed at the call site via commitSelection; the
			// key itself sends zero bytes toward the PTY.
		case "Escape":
			if loop.escape() {
				return true
			}
		case "Backspace":
			loop.backspace()
		case "s":
			if loop.model.SearchActive() {
				loop.insert('s')
			} else {
				loop.sorted = !loop.sorted
			}
		case "x":
			if loop.model.SearchActive() {
				loop.insert('x')
			}
			// Normal-mode x is a documented no-op: no key path to
			// killPickerTarget exists in client-picker mode.
		default:
			if len(key) == 1 {
				loop.insert(rune(key[0]))
			}
		}
	}
	for _, r := range text {
		loop.insert(r)
	}
	return false
}
