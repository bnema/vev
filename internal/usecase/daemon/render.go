// Package daemon holds vev's server-side session multiplexer use case: the
// accept loop, the ephemeral/named session registry, the per-tab PTY reader
// and VT screen, and the per-client debounced render scheduler.
//
// Concurrency model (sessions own one or more PTY-backed tabs):
//
//   - Serve runs the accept loop. Each accepted connection is handled by its
//     own goroutine (handleConn): it reads the first frame and routes it to a
//     session create/attach, a list, or a kill.
//   - Per session there are exactly two long-lived goroutines: the PTY reader
//     (drains child output into the VT screen and pokes a cap-1 dirty channel)
//     and the render scheduler (debounces dirties and paints the attached
//     client). Both are tied to the session context and unwind when the
//     session is killed (pty.Close unblocks the reader; ctx cancel stops the
//     scheduler).
//   - The daemon exits (Serve returns) when the last session is removed, or
//     when the parent context is cancelled (graceful shutdown notifies any
//     attached clients with ReasonServerShutdown).
//
// Locking: a session's screen and per-client renderer shadow are both guarded
// by tab.mu; the attached-client pointer by session.mu; the registry by
// Daemon.mu. When more than one is held the order is always
// Daemon.mu > session.mu, and (for the transport) attachedClient.sendMu >
// tab.mu — the PTY reader only ever takes tab.mu, so it never blocks on
// a slow client.
package daemon

import (
	"bytes"
	"strconv"
	"time"

	scopy "github.com/bnema/vev/internal/usecase/copy"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/bnema/vev/pkg/vt"
)

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (d *Daemon) ptyReader(sess *session, tb *tab) {
	defer d.sessWg.Done()
	buf := make([]byte, ptyReadBufSize)
	var resp []byte
	tb.mu.Lock()
	tb.screen.OnResponse = func(b []byte) { resp = append(resp, b...) }
	tb.mu.Unlock()
	for {
		n, err := tb.pty.Read(buf)
		if n > 0 {
			data := buf[:n]
			tb.mu.Lock()
			wasSyncing := tb.screen.SyncUpdateActive()
			tb.screen.Write(data)
			isSyncing := tb.screen.SyncUpdateActive()
			tb.mu.Unlock()
			if len(resp) > 0 {
				if _, writeErr := tb.pty.Write(resp); writeErr != nil {
					d.log.Warn("pty response write failed", "err", writeErr, "session", sess.name)
				}
				resp = resp[:0]
			}
			if wasSyncing != isSyncing {
				tb.mu.Lock()
				tb.syncGen++
				gen := tb.syncGen
				tb.mu.Unlock()
				if isSyncing {
					go d.syncWatchdog(tb, gen)
				}
			}
			if (wasSyncing && !isSyncing) || (!isSyncing && syncUpdateEndIn(data)) {
				signal(tb.flush)
				continue
			}
			if isSyncing {
				continue
			}
			signal(tb.dirty)
		}
		if err != nil {
			d.closeTab(sess, tb, true)
			return
		}
	}
}

// scheduler debounces dirty signals. The first dirty opens a short tab;
// sustained floods progressively widen that tab, while isolated updates
// return to the minimum delay for interactive latency.
func (d *Daemon) scheduler(sess *session, tb *tab) {
	defer d.sessWg.Done()
	delay := minDebounceInterval
	lastRender := d.clock.Now()
outer:
	for {
		select {
		case <-sess.ctx.Done():
			return
		case <-tabDone(tb):
			return
		case <-tb.flush:
			d.render(sess, tb)
			lastRender = d.clock.Now()
			continue
		case <-tb.dirty:
			if d.clock.Now().Sub(lastRender) >= maxDebounceInterval {
				delay = minDebounceInterval
			}
		}

		coalesced := 0
		timer := d.clock.NewTimer(delay)
	absorb:
		for {
			select {
			case <-sess.ctx.Done():
				timer.Stop()
				return
			case <-tabDone(tb):
				timer.Stop()
				return
			case <-tb.flush:
				if !timer.Stop() {
					select {
					case <-timer.C():
					default:
					}
				}
				d.render(sess, tb)
				lastRender = d.clock.Now()
				continue outer
			case <-tb.dirty:
				coalesced++
			case <-timer.C():
				break absorb
			}
		}
		delay = nextDebounceDelay(delay, coalesced)
		d.render(sess, tb)
		lastRender = d.clock.Now()
	}
}

func nextDebounceDelay(delay time.Duration, coalesced int) time.Duration {
	if coalesced == 0 {
		return minDebounceInterval
	}
	if delay >= maxDebounceInterval {
		return maxDebounceInterval
	}
	delay += debounceStep
	if delay > maxDebounceInterval {
		return maxDebounceInterval
	}
	return delay
}

func (d *Daemon) syncWatchdog(tb *tab, gen uint64) {
	timer := d.clock.NewTimer(maxSyncUpdateDuration)
	select {
	case <-tabDone(tb):
		timer.Stop()
		return
	case <-timer.C():
	}

	tb.mu.Lock()
	if tb.syncGen != gen || !tb.screen.SyncUpdateActive() {
		tb.mu.Unlock()
		return
	}
	tb.screen.ForceSyncEnd()
	tb.mu.Unlock()
	signal(tb.flush)
}

func tabDone(tb *tab) <-chan struct{} {
	if tb.ctx == nil {
		return nil
	}
	return tb.ctx.Done()
}

func syncUpdateEndIn(data []byte) bool {
	return bytes.Contains(data, []byte("\x1b[?2026l"))
}

// render paints the current client, or (when detached) just clears accumulated
// damage so it never grows unbounded while headless.
func (d *Daemon) render(sess *session, tb *tab) {
	tb.mu.Lock()
	if tb.screen.SyncUpdateActive() {
		tb.mu.Unlock()
		return
	}
	tb.mu.Unlock()

	tb.mu.Lock()
	previewer := tb.previewClient
	tb.mu.Unlock()

	sess.mu.Lock()
	ac := sess.client
	active := sess.active >= 0 && sess.active < len(sess.tabs) && sess.tabs[sess.active] == tb
	sess.mu.Unlock()

	if previewer != nil {
		if previewSess := previewer.currentSession(); previewSess != nil {
			d.paint(previewSess, previewer, false)
		}
		if ac != nil && active && ac != previewer {
			d.paint(sess, ac, false)
			return
		}
		tb.mu.Lock()
		tb.screen.ClearDamage()
		tb.mu.Unlock()
		return
	}

	if ac == nil || !active {
		tb.mu.Lock()
		tb.screen.ClearDamage()
		tb.mu.Unlock()
		return
	}
	d.paint(sess, ac, false)
}

// paint draws the composed client frame (active tab plus status bar) and
// sends the resulting bytes. The renderer shadow is reset on explicit invalidations
// such as switch/create/close/rename/resize so the repaint is complete.
func (d *Daemon) paint(sess *session, ac *attachedClient, reset bool) {
	tb := sess.activeTab()
	if tb == nil {
		return
	}

	ac.sendMu.Lock()
	ac.copyMu.Lock()
	copyActive := ac.copyMode != nil
	var copyMode *scopy.Mode
	if ac.copyMode != nil {
		copyModeValue := *ac.copyMode
		copyMode = &copyModeValue
	}
	copyFeedback := ac.copyFeedback
	if copyFeedback != "" && !copyActive {
		ac.copyFeedback = ""
	}
	ac.copyMu.Unlock()
	ac.pickerMu.Lock()
	pickerActive := ac.picker != nil
	pickerModel := ac.picker.Clone()
	previewTab := ac.pickerPreview
	ac.pickerMu.Unlock()
	preview := snapshotPickerPreview(previewTab)
	ac.paletteMu.Lock()
	paletteModel := ac.palette
	paletteActive := paletteModel != nil
	if !paletteActive {
		ac.paletteMu.Unlock()
	}
	ac.promptMu.Lock()
	promptModel := ac.prompt
	promptActive := promptModel != nil
	if !promptActive {
		ac.promptMu.Unlock()
	}

	tb.mu.Lock()
	if reset || copyActive || pickerActive || paletteActive || promptActive {
		ac.rend.Reset()
	}
	if reset || pickerActive || paletteActive || promptActive {
		ac.lastCursor.valid = false
	}
	frame, damage := composeClientFrame(sess, tb, reset, copyFeedback)
	if copyActive {
		frame, damage = composeCopyClientFrame(copyMode, tb)
	}
	if pickerActive {
		if previewTab == tb {
			preview = pickerPreviewFromLockedTab(tb)
		}
		frame, damage = composePickerClientFrame(pickerModel, preview, frame)
	}
	if paletteActive {
		frame, damage = composePaletteClientFrame(paletteModel, frame)
		ac.paletteMu.Unlock()
	}
	if promptActive {
		frame, damage = composePromptClientFrame(promptModel, frame)
		ac.promptMu.Unlock()
	}
	desiredCursor := desiredCursorOut(tb.screen, copyActive || pickerActive || paletteActive || promptActive)
	data, err := ac.rend.Draw(frame, damage)
	var cursorTail []byte
	if err == nil {
		cursorTail = ac.encodeCursorTail(desiredCursor, len(data) > 0)
	}
	tb.screen.ClearDamage()
	tb.mu.Unlock()

	var serr error
	if err == nil {
		data = append(data, cursorTail...)
		if len(data) > 0 {
			serr = ac.tr.Send(frameOutput(data))
		}
	}
	ac.sendMu.Unlock()

	if err != nil {
		d.log.Error("render draw failed", "err", err, "session", sess.name)
		return
	}
	if serr != nil {
		d.detachOnSendError(sess, ac)
	}
}

// desiredCursorOut computes the terminal cursor state that should be shown to
// the client for the current tab and overlay mode.
func desiredCursorOut(s *vt.Screen, copyActive bool) cursorOut {
	if copyActive || !s.CursorVisible() {
		return cursorOut{hidden: true}
	}
	style, ok := s.CursorStyle()
	if !ok {
		style = 5
	}
	return cursorOut{row: s.CursorRow(), col: s.CursorCol(), style: style, hasStyle: true}
}

func (ac *attachedClient) encodeCursorTail(desired cursorOut, force bool) []byte {
	changed := force || !ac.lastCursor.valid || ac.lastCursor.hidden != desired.hidden || ac.lastCursor.row != desired.row || ac.lastCursor.col != desired.col || ac.lastCursor.style != desired.style || ac.lastCursor.hasStyle != desired.hasStyle
	if !changed {
		return nil
	}
	prev := ac.lastCursor
	ac.lastCursor = desired
	ac.lastCursor.valid = true
	if desired.hidden {
		return []byte("\x1b[?25l")
	}
	var b []byte
	b = append(b, "\x1b["...)
	b = strconv.AppendInt(b, int64(desired.row+1), 10)
	b = append(b, ';')
	b = strconv.AppendInt(b, int64(desired.col+1), 10)
	b = append(b, 'H')
	if !prev.valid || prev.hidden || prev.style != desired.style || prev.hasStyle != desired.hasStyle {
		b = append(b, "\x1b["...)
		b = strconv.AppendInt(b, int64(desired.style), 10)
		b = append(b, " q"...)
	}
	b = append(b, "\x1b[?25h"...)
	return b
}

func composeClientFrame(sess *session, tb *tab, full bool, rightStatus string) (renderer.Frame, []renderer.Damage) {
	width, screenRows := tb.screen.Frame.Width, tb.screen.Frame.Height
	frame := renderer.NewFrame(width, screenRows+1)
	for y := range screenRows {
		copy(frame.Row(y), tb.screen.Frame.Row(y))
	}
	drawStatus(frame.Row(screenRows), sess, rightStatus)
	if full {
		return frame, []renderer.Damage{renderer.FullRedraw()}
	}
	damage := append([]renderer.Damage(nil), tb.screen.Damage()...)
	damage = append(damage, renderer.Damage{Kind: renderer.DamageText, X: 0, Y: screenRows, Width: width, Height: 1})
	return frame, damage
}
