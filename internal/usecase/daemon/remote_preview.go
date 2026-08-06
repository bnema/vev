package daemon

import (
	"errors"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/renderer"
)

func (d *Daemon) handleRemotePreview(tr ports.Transport, frame ports.Frame) error {
	defer func() { _ = tr.Close() }()
	request, err := ports.UnmarshalRemotePreviewRequest(frame.Payload)
	if err != nil {
		return d.sendRemotePreview(tr, ports.RemotePreview{Version: ports.RemotePreviewSchemaVersion, Status: ports.RemotePreviewMalformed})
	}
	preview, err := d.captureRemotePreview(request)
	if err != nil {
		status := ports.RemotePreviewUnavailable
		if errors.Is(err, errRemotePreviewNoSuchTarget) {
			status = ports.RemotePreviewNoSuchTarget
		}
		preview = ports.RemotePreview{Version: ports.RemotePreviewSchemaVersion, Status: status}
	}
	return d.sendRemotePreview(tr, preview)
}

func (d *Daemon) sendRemotePreview(tr ports.Transport, preview ports.RemotePreview) error {
	payload := ports.MarshalRemotePreview(preview)
	if payload == nil {
		payload = ports.MarshalRemotePreview(ports.RemotePreview{Version: ports.RemotePreviewSchemaVersion, Status: ports.RemotePreviewMalformed})
	}
	return d.boundedControlSend(tr, ports.Frame{Type: ports.MsgRemotePreviewResponse, Payload: payload})
}

var errRemotePreviewNoSuchTarget = errors.New("daemon: remote preview target does not exist")

func (d *Daemon) captureRemotePreview(request ports.RemotePreviewRequest) (ports.RemotePreview, error) {
	target := request.Target
	d.mu.Lock()
	sess := d.findByNameLocked(target.SessionName)
	if sess == nil || sess.incarnation != target.LifecycleID {
		d.mu.Unlock()
		return ports.RemotePreview{}, errRemotePreviewNoSuchTarget
	}
	index, ok := remoteTargetTabIndexLocked(sess, target)
	d.mu.Unlock()
	if !ok {
		return ports.RemotePreview{}, errRemotePreviewNoSuchTarget
	}

	sess.mu.Lock()
	if index < 0 || index >= len(sess.tabs) {
		sess.mu.Unlock()
		return ports.RemotePreview{}, errRemotePreviewNoSuchTarget
	}
	tab := sess.tabs[index]
	if tab == nil || domain.TabStableID(tab.stableID) != target.LiveTabID {
		sess.mu.Unlock()
		return ports.RemotePreview{}, errRemotePreviewNoSuchTarget
	}
	sess.mu.Unlock()
	tab.mu.Lock()
	pane := tab.focusedPane()
	if pane == nil {
		tab.mu.Unlock()
		return ports.RemotePreview{}, errRemotePreviewNoSuchTarget
	}
	pane.mu.Lock()
	if pane.screen == nil {
		pane.mu.Unlock()
		tab.mu.Unlock()
		return ports.RemotePreview{}, errRemotePreviewNoSuchTarget
	}
	source := pane.screen.Frame.Clone()
	revision := pane.syncGen
	pane.mu.Unlock()
	tab.mu.Unlock()
	if revision == 0 {
		revision = 1
	}

	width := min(int(request.Width), source.Width)
	height := min(int(request.Height), source.Height)
	if width <= 0 || height <= 0 {
		return ports.RemotePreview{}, errRemotePreviewNoSuchTarget
	}
	startY, endY := source.Height-height, source.Height
	if last := remotePreviewLastContentRow(source, width, startY, endY); last >= startY {
		endY = last + 1
	} else if last := remotePreviewLastContentRow(source, width, 0, startY); last >= 0 {
		startY = max(0, last-height+1)
		endY = last + 1
	}
	height = endY - startY
	cells := make([]renderer.Cell, 0, width*height)
	for y := 0; y < height; y++ {
		row := source.Row(startY + y)
		for x := 0; x < width; x++ {
			cell, _ := cropRemotePreviewCell(row[x], x, width)
			cells = append(cells, cell)
		}
	}
	preview := ports.RemotePreview{
		Version:     ports.RemotePreviewSchemaVersion,
		Status:      ports.RemotePreviewOK,
		LifecycleID: target.LifecycleID,
		TabID:       target.LiveTabID,
		Revision:    revision,
		Width:       uint16(width),
		Height:      uint16(height),
		Cells:       cells,
	}
	if err := ports.ValidateRemotePreview(preview); err != nil {
		// A crop must not split a wide cell. Return a safe unavailable response
		// instead of exposing a malformed cell stream.
		return ports.RemotePreview{}, err
	}
	return preview, nil
}

// remotePreviewLastContentRow finds the final row with a visible glyph inside
// the bounded viewport crop. Remote terminal frames retain their full PTY
// height even when the only output is near the top; compacting the blank tail lets the
// picker bottom-anchor short output instead of clipping it away.
func remotePreviewLastContentRow(frame renderer.Frame, width, start, end int) int {
	for y := end - 1; y >= start; y-- {
		for x, cell := range frame.Row(y)[:width] {
			cell, splitWideRune := cropRemotePreviewCell(cell, x, width)
			if !splitWideRune && remotePreviewCellVisible(cell) {
				return y
			}
		}
	}
	return -1
}

// cropRemotePreviewCell converts a left wide-rune half at the right crop edge
// to the styled blank that the picker can safely render. The flag distinguishes
// that replacement from an actual styled blank in the source frame.
func cropRemotePreviewCell(cell renderer.Cell, x, width int) (renderer.Cell, bool) {
	if x == width-1 && !cell.Continuation && renderer.RuneWidth(cell.Rune) == 2 {
		return renderer.Cell{Rune: ' ', Style: cell.Style}, true
	}
	return cell, false
}

func remotePreviewCellVisible(cell renderer.Cell) bool {
	return !cell.Continuation &&
		(cell.Rune != 0 && cell.Rune != ' ' || !cell.Style.Equal(renderer.DefaultStyle()))
}
