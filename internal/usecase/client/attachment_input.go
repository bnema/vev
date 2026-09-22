package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/theme"
)

// attachmentInput is the attached loop's terminal input path, ported from the
// old client's stdin pump. Each physical read is demultiplexed before anything
// reaches the session:
//
//   - theme.Scanner strips OSC 10/11/4 color replies and CSI ?997 scheme
//     notifications;
//   - paletteMarkerScanner strips DECRQM 2031 boundary replies;
//   - the remaining ordinary bytes go to an overlay when one owns input, or
//     through the paste coalescer, which keeps a bracketed paste in one Input.
//
// Color replies and markers drive the palette generation, whose queries are
// written through the foreground and whose results are sent as
// protocol.Theme. Only the attached loop calls these methods. The coalescer's
// timers run on their own goroutines, but they only queue bytes and wake the
// loop, so the loop stays the single sender.
type attachmentInput struct {
	worker *sessionAttachmentWorker
	fg     AttachmentForeground
	stream ports.BrokerLogicalConnection
	picker *attachmentMovePicker
	clock  ports.Clock
	seq    uint64

	scanner theme.Scanner
	markers paletteMarkerScanner
	// markerTimer bounds how long an undecided DECRQM prefix (a lone Escape
	// included) is withheld before it is forwarded as ordinary input.
	markerTimer ports.Timer

	paste  *pasteCoalescer
	mu     sync.Mutex
	queued [][]byte
	wake   chan struct{}

	// palette is nil when detection is disabled: no retained theme or no
	// query seam on the foreground. Replies are stripped either way.
	palette    *paletteGenerationCoordinator
	themes     *terminalThemeState
	query      attachmentQueryForeground
	drain      paletteDeadline
	completion paletteDeadline
}

// paletteDeadline is the one armed timer of a deadline kind. The coordinator
// cancels a replaced generation's deadlines before arming the next, so one
// slot per kind is enough.
type paletteDeadline struct {
	id    paletteGenerationID
	timer ports.Timer
}

func (d *paletteDeadline) arm(clock ports.Clock, id paletteGenerationID, delay time.Duration) {
	d.stop()
	d.id = id
	d.timer = clock.NewTimer(delay)
}

func (d *paletteDeadline) cancel(id paletteGenerationID) {
	if d.timer != nil && d.id == id {
		d.stop()
	}
}

func (d *paletteDeadline) stop() {
	if d.timer != nil {
		d.timer.Stop()
	}
	*d = paletteDeadline{}
}

func (d *paletteDeadline) c() <-chan time.Time {
	if d.timer == nil {
		return nil
	}
	return d.timer.C()
}

// fired consumes the slot after its timer delivered, returning the event the
// coordinator expects.
func (d *paletteDeadline) fired(kind paletteGenerationEventKind) paletteGenerationEvent {
	event := paletteGenerationEvent{id: d.id, kind: kind}
	*d = paletteDeadline{}
	return event
}

func newAttachmentInput(w *sessionAttachmentWorker, fg AttachmentForeground, stream ports.BrokerLogicalConnection, picker *attachmentMovePicker) *attachmentInput {
	in := &attachmentInput{
		worker: w,
		fg:     fg,
		stream: stream,
		picker: picker,
		clock:  w.cfg.Clock,
		wake:   make(chan struct{}, 1),
		themes: w.cfg.Theme,
	}
	in.paste = newPasteCoalescer(in.clock, in.enqueue)
	if query, ok := fg.(attachmentQueryForeground); ok && in.themes != nil {
		in.query = query
		in.palette = newPaletteGenerationCoordinator()
	}
	return in
}

// enqueue is the coalescer's emit callback. It may run on a timer goroutine,
// so it only queues and wakes the attached loop.
func (in *attachmentInput) enqueue(data []byte) {
	if len(data) == 0 {
		return
	}
	in.mu.Lock()
	in.queued = append(in.queued, append([]byte(nil), data...))
	in.mu.Unlock()
	select {
	case in.wake <- struct{}{}:
	default:
	}
}

// start publishes the cleared theme and writes the first palette query. The
// retained definitive colors avoid a neutral flash on a replacement
// attachment.
func (in *attachmentInput) start(ctx context.Context) error {
	if in.palette == nil {
		return nil
	}
	retained := in.themes.update(func(current *protocol.Theme) { current.TrueColor = in.worker.cfg.TrueColor })
	return in.apply(ctx, in.palette.start(retained, false))
}

// close stops every timer. Undecided bytes are dropped with the attachment,
// as the owner boundary drops every other attachment-held byte.
func (in *attachmentInput) close() {
	in.paste.Close()
	if in.markerTimer != nil {
		in.markerTimer.Stop()
		in.markerTimer = nil
	}
	in.drain.stop()
	in.completion.stop()
}

// read demultiplexes one physical terminal read.
func (in *attachmentInput) read(ctx context.Context, state outputApplyState, data []byte) error {
	var ordinary []byte
	if err := in.disarmMarker(ctx, state); err != nil {
		return err
	}
	// Every callback of one read keeps the generation it started with: a
	// scheme notification early in the read starts a replacement, and an old
	// marker later in the same read must not advance it.
	var id paletteGenerationID
	if in.palette != nil {
		id = in.palette.current.id
	}
	var events []paletteGenerationEvent
	in.scanner.Scan(data, func(kind int, rgb renderer.RGB) {
		event := paletteEventForeground
		if kind == 11 {
			event = paletteEventBackground
		}
		events = append(events, paletteGenerationEvent{id: id, kind: event, rgb: rgb})
	}, func(slot int, rgb renderer.RGB) {
		if slot8, ok := paletteSlot(slot); ok {
			events = append(events, paletteGenerationEvent{id: id, kind: paletteEventPalette, slot: slot8, rgb: rgb})
		}
	}, func(light bool) {
		events = append(events, paletteGenerationEvent{id: id, kind: paletteEventScheme, light: light})
	}, func(bytes []byte) {
		in.markers.scan(bytes, func(b []byte) { ordinary = append(ordinary, b...) }, func() {
			events = append(events, paletteGenerationEvent{id: id, kind: paletteEventMarker})
		})
	})
	for _, event := range events {
		if err := in.handle(ctx, event); err != nil {
			return err
		}
	}
	if in.markers.hasPendingPrefix() {
		in.markerTimer = in.clock.NewTimer(paletteMarkerAmbiguityDeadline)
	}
	return in.route(ctx, state, ordinary)
}

// flushHeld forwards every undecided ordinary byte before an automation batch,
// so driver input never overtakes a keypress still being disambiguated. An
// open bracketed paste is left alone.
func (in *attachmentInput) flushHeld(ctx context.Context, state outputApplyState) error {
	if in.markerTimer != nil {
		in.markerTimer.Stop()
		in.markerTimer = nil
	}
	var ordinary []byte
	collect := func(b []byte) { ordinary = append(ordinary, b...) }
	in.scanner.EndBatch(func(b []byte) {
		in.markers.scan(b, collect, func() {})
	})
	in.markers.flush(collect)
	if err := in.route(ctx, state, ordinary); err != nil {
		return err
	}
	in.paste.EndBatch()
	return in.flush(ctx)
}

// disarmMarker stops the ambiguity deadline before a later read. A deadline
// that already fired wins, so bytes due as ordinary input are never
// retroactively consumed as a marker.
func (in *attachmentInput) disarmMarker(ctx context.Context, state outputApplyState) error {
	if in.markerTimer == nil {
		return nil
	}
	timer := in.markerTimer
	select {
	case <-timer.C():
		return in.markerExpired(ctx, state)
	default:
	}
	if !timer.Stop() {
		return in.markerExpired(ctx, state)
	}
	in.markerTimer = nil
	return nil
}

func (in *attachmentInput) markerC() <-chan time.Time {
	if in.markerTimer == nil {
		return nil
	}
	return in.markerTimer.C()
}

// markerExpired forwards a withheld DECRQM prefix as ordinary input.
func (in *attachmentInput) markerExpired(ctx context.Context, state outputApplyState) error {
	in.markerTimer = nil
	var ordinary []byte
	in.markers.flush(func(b []byte) { ordinary = append(ordinary, b...) })
	return in.route(ctx, state, ordinary)
}

// route hands ordinary bytes to the overlay that owns input, or to the paste
// coalescer on their way to the session.
func (in *attachmentInput) route(ctx context.Context, state outputApplyState, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	consumed, err := in.picker.consumeInput(ctx, state, AttachmentInputEvent{Data: data})
	if err != nil || consumed {
		return err
	}
	in.paste.Scan(data)
	return in.flush(ctx)
}

// flush sends every queued coalescer emission as one session Input each.
func (in *attachmentInput) flush(ctx context.Context) error {
	in.mu.Lock()
	queued := in.queued
	in.queued = nil
	in.mu.Unlock()
	for _, data := range queued {
		if err := in.send(ctx, protocol.Input{InputSeq: in.nextSeq(), Data: data}); err != nil {
			return err
		}
	}
	return nil
}

func (in *attachmentInput) nextSeq() uint64 {
	in.seq++
	return in.seq
}

func (in *attachmentInput) send(ctx context.Context, message protocol.ClientMessage) error {
	return in.worker.send(ctx, in.fg, in.stream, message)
}

// handle applies one palette event. A scheme notification starts a
// replacement generation from the retained colors.
func (in *attachmentInput) handle(ctx context.Context, event paletteGenerationEvent) error {
	if in.palette == nil {
		return nil
	}
	if event.kind == paletteEventScheme {
		retained := in.themes.update(func(current *protocol.Theme) {
			current.SchemeKnown = true
			current.Light = event.light
		})
		return in.apply(ctx, in.palette.start(retained, true))
	}
	return in.apply(ctx, in.palette.handle(event))
}

// drainFired and completionFired feed an expired palette deadline back to the
// coordinator.
func (in *attachmentInput) drainFired(ctx context.Context) error {
	return in.handle(ctx, in.drain.fired(paletteEventDrainDeadline))
}

func (in *attachmentInput) completionFired(ctx context.Context) error {
	return in.handle(ctx, in.completion.fired(paletteEventCompletionDeadline))
}

func (in *attachmentInput) apply(ctx context.Context, actions []paletteGenerationAction) error {
	for _, action := range actions {
		switch action.kind {
		case paletteActionPublishCleared:
			if err := in.send(ctx, action.theme); err != nil {
				return fmt.Errorf("vev: publishing theme: %w", err)
			}
		case paletteActionPublishFinal:
			in.themes.update(func(current *protocol.Theme) { *current = action.theme })
			if err := in.send(ctx, action.theme); err != nil {
				return fmt.Errorf("vev: publishing theme: %w", err)
			}
		case paletteActionWriteDrain, paletteActionWriteBatch:
			if err := in.query.writeTerminalQuery([]byte(action.bytes)); err != nil {
				return fmt.Errorf("vev: writing palette query: %w", err)
			}
		case paletteActionArmDrainDeadline:
			in.drain.arm(in.clock, action.id, action.deadline)
		case paletteActionArmCompletionDeadline:
			in.completion.arm(in.clock, action.id, action.deadline)
		case paletteActionCancelDrainDeadline:
			in.drain.cancel(action.id)
		case paletteActionCancelCompletionDeadline:
			in.completion.cancel(action.id)
		}
	}
	return nil
}
