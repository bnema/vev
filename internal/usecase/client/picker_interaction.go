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
// interaction ID, its latest admitted revision, and the newest interaction
// this attachment retired. A retired interaction is never resurrected by a
// late snapshot, and stale revisions never step the display backward.
type pickerInteraction struct {
	open        bool
	interaction uint64
	revision    uint64
	retired     uint64
}

// setOpen starts or stops the interaction namespace. Superseding an open
// interaction retires it too: a late snapshot for the replaced ID must not
// take presentation back. Closing retires the namespace for good.
func (p *pickerInteraction) setOpen(open bool, interaction uint64) {
	if p == nil {
		return
	}
	if open && interaction != p.interaction && p.open && p.interaction > p.retired {
		p.retired = p.interaction
	}
	p.open = open
	if open && interaction != p.interaction {
		p.interaction = interaction
		p.revision = 0
	}
	if !open && interaction > p.retired {
		p.retired = interaction
	}
}

// admitSnapshot validates one snapshot against the open interaction and
// reports whether it carries a newer revision worth displaying. Retired
// interactions, foreign interactions, and older or duplicate revisions
// discard.
func (p *pickerInteraction) admitSnapshot(snapshot protocol.PickerSnapshot) bool {
	if p == nil {
		return false
	}
	if protocol.ValidatePickerSnapshot(snapshot) != nil {
		return false
	}
	if snapshot.InteractionID <= p.retired {
		return false
	}
	if !p.open || snapshot.InteractionID != p.interaction {
		return false
	}
	if snapshot.Revision <= p.revision {
		return false
	}
	p.revision = snapshot.Revision
	return true
}

// pickerLoop owns one admitted *picker.Model plus the snapshot revision it
// was built from. Cursor and search are local-only presentation; commit
// and cancel cross the wire as typed messages. The daemon revalidates the
// committed revision and key, so the client never keeps a second copy of
// the row set: it commits from the model it is displaying. Sort stays
// daemon-owned: the client never sees recency metadata, so `s` is a
// documented no-op here and a refresh arrives as a new snapshot revision.
type pickerLoop struct {
	model       *picker.Model
	interaction uint64
	revision    uint64
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
	// pasteMode records whether bracketed paste was enabled on the terminal
	// for this interaction, so it is enabled once and disabled exactly once.
	pasteMode bool
}

func newPickerRenderer() *pickerRenderer {
	return &pickerRenderer{}
}

// Bracketed-paste mode is enabled while the picker owns the terminal: the
// terminal then wraps pasted text in markers, which the input decoder drops
// as a unit. Without it, a paste arrives as ordinary bytes and only the
// event ordering distinguishes it from fast typing.
const (
	bracketedPasteEnable  = "\x1b[?2004h"
	bracketedPasteDisable = "\x1b[?2004l"
)

// disableBracketedPaste returns the mode reset once, or nil when nothing was
// enabled. The caller writes it through the terminal's sole writer.
func (r *pickerRenderer) disableBracketedPaste() []byte {
	if r == nil || !r.pasteMode {
		return nil
	}
	r.pasteMode = false
	return []byte(bracketedPasteDisable)
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
	if !r.pasteMode {
		// Enable bracketed paste with the first frame so the terminal marks
		// every paste from the moment the picker owns the screen.
		r.pasteMode = true
		return append([]byte(bracketedPasteEnable), data...)
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
// drive cursor/search/exit with daemon-overlay parity: arrows and j/k move
// in normal mode (j/k are literal in search), `/` enters search, `s`/`x`
// are commands in normal mode and literal in search, `q`/Ctrl-C/Escape
// close in normal mode (Escape exits search first), Backspace edits the
// query. Text runes insert only while search is active; normal-mode typing
// is ignored exactly like the overlay. The commit result reports whether
// the caller must build a typed commit for the cursor row; the close
// result reports whether the caller must send PickerClose. Close wins over
// commit when both land in one op, mirroring the overlay exit
// short-circuit. Commit and failure handling stay at the call site so
// CauseActionID attribution is exact.
func pickerDriverOp(loop *pickerLoop, keys []string, text string) (commit, close bool) {
	if loop == nil || loop.model == nil {
		return false, false
	}
	insertLit := func(r rune) {
		if loop.model.SearchActive() {
			loop.insert(r)
		}
	}
	for _, key := range keys {
		active := loop.model.SearchActive()
		switch key {
		case "Up":
			loop.up()
		case "Down":
			loop.down()
		case "j":
			if active {
				loop.insert('j')
			} else {
				loop.down()
			}
		case "k":
			if active {
				loop.insert('k')
			} else {
				loop.up()
			}
		case "Enter":
			// Commit is typed at the call site via commitSelection; the
			// key itself sends zero bytes toward the PTY.
			commit = true
		case "Escape":
			if loop.escape() {
				return commit, true
			}
		case "Backspace":
			if active {
				loop.backspace()
			}
		case "/":
			if active {
				loop.insert('/')
			} else {
				loop.enterSearch()
			}
		// Normal-mode "s" (sort) and "x" (kill) keep no client branch:
		// they fall through to the default case, which acts only while
		// search is active. Sort stays daemon-owned and no client key
		// path kills a target.
		case "q":
			if active {
				loop.insert('q')
			} else {
				return commit, true
			}
		case "Ctrl+C":
			// The overlay exits on Ctrl-C in both modes; the byte
			// itself never reaches the query.
			return commit, true
		default:
			if len(key) == 1 {
				insertLit(rune(key[0]))
			}
		}
	}
	for _, r := range text {
		insertLit(r)
	}
	return commit, false
}

// pickerInputBatch is one decoded batch: ordered input events.
type pickerInputBatch struct {
	events []pickerEvent
}

// pickerConsumeOutcome is one decoded operation reported by the stdin
// pump to the attach loop. The pump decodes and identifies; the attach
// loop is the only writer of the picker model, so the outcome carries the
// ordered events and their interaction identity, never a mutation or a
// message. actionID echoes the admitted record so the loop completes the
// right ui-driver action.
type pickerConsumeOutcome struct {
	consumed    bool
	actionID    uint64
	generation  uint64
	interaction uint64
	events      []pickerEvent
}

// acceptOutcome reports whether this operation still belongs to the
// interaction the attachment currently presents. Retired generations and
// interactions drop.
func (o pickerConsumeOutcome) acceptOutcome(interaction, generation uint64) bool {
	return o.consumed && o.interaction != 0 && o.interaction == interaction && o.generation == generation
}

// applyPickerBatch applies one decoded batch in arrival order. Every event
// is applied: a terminal read can legitimately carry several keystrokes, so
// batch size never decides whether input is a command. Paste protection is
// the decoder's paste state, not a heuristic here: a bracketed paste's
// content never reaches this function. changed reports whether the display
// needs a repaint; a close ends the batch.
func applyPickerBatch(loop *pickerLoop, events []pickerEvent) (commit, close, changed bool) {
	if loop == nil || loop.model == nil || len(events) == 0 {
		return false, false, false
	}
	for _, event := range events {
		if event.kind == pickerEventRune {
			if loop.model.SearchActive() {
				loop.insert(event.r)
				changed = true
			}
			continue
		}
		before, searchBefore := loop.model.SelectedIndex(), loop.model.SearchActive()
		opCommit, opClose := pickerDriverOp(loop, []string{event.key}, "")
		if loop.model.SelectedIndex() != before || loop.model.SearchActive() != searchBefore {
			changed = true
		}
		if opClose {
			return commit, true, changed
		}
		commit = commit || opCommit
	}
	return commit, close, changed
}
