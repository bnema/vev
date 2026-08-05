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
	startY := source.Height - height
	cells := make([]renderer.Cell, 0, width*height)
	for y := 0; y < height; y++ {
		row := source.Row(startY + y)
		for x := 0; x < width; x++ {
			cell := row[x]
			// A crop may end immediately after the left half of a wide
			// rune. Replace that boundary cell with a styled blank rather
			// than sending an invalid continuation sequence to the picker.
			if x == width-1 && !cell.Continuation && renderer.RuneWidth(cell.Rune) == 2 {
				cell = renderer.Cell{Rune: ' ', Style: cell.Style}
			}
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
