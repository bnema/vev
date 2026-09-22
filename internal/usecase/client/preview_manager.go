package client

import (
	"time"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	pickerusecase "github.com/bnema/vev/internal/usecase/picker"
)

// pickerPreviewDebounce bounds how long the cursor may rest before the client
// subscribes to that row's preview. The daemon pushes frames afterwards, so
// this only throttles cursor movement.
const pickerPreviewDebounce = 80 * time.Millisecond

// previewCacheSize bounds the last-good frames kept for stale-while-refresh.
const previewCacheSize = 8

// previewState is how the displayed preview relates to its target.
type previewState uint8

const (
	previewFresh previewState = iota
	previewStale
	previewLoading
	previewUnavailable
	previewMissing
)

// pickerPreviewHost is the picker half of the preview: it names the selected
// row's route and viewport and displays whatever the manager decides.
type pickerPreviewHost interface {
	PreviewRequest(domain.Size) (ports.BrokerPreviewRoute, protocol.RemotePreviewRequest, bool)
	SetPreview(protocol.RemotePreview, previewState)
	ClearPreview()
}

type previewCacheKey struct {
	endpoint  string
	lifecycle domain.SessionLifecycleID
	tab       domain.TabStableID
	width     uint16
	height    uint16
}

func previewKeyFor(request protocol.RemotePreviewRequest) previewCacheKey {
	return previewCacheKey{endpoint: request.Target.Endpoint, lifecycle: request.Target.LifecycleID, tab: request.Target.LiveTabID, width: request.Width, height: request.Height}
}

type previewCacheEntry struct {
	key   previewCacheKey
	frame protocol.RemotePreview
}

// previewCache is a tiny LRU of the last OK frame per target; the most
// recently used entry is last.
type previewCache struct {
	entries []previewCacheEntry
}

func (c *previewCache) get(key previewCacheKey) (protocol.RemotePreview, bool) {
	for i, entry := range c.entries {
		if entry.key == key {
			c.entries = append(append(c.entries[:i:i], c.entries[i+1:]...), entry)
			return entry.frame, true
		}
	}
	return protocol.RemotePreview{}, false
}

func (c *previewCache) put(key previewCacheKey, frame protocol.RemotePreview) {
	for i, entry := range c.entries {
		if entry.key == key {
			c.entries = append(c.entries[:i:i], c.entries[i+1:]...)
			break
		}
	}
	if len(c.entries) >= previewCacheSize {
		c.entries = append(c.entries[:0:0], c.entries[len(c.entries)-previewCacheSize+1:]...)
	}
	c.entries = append(c.entries, previewCacheEntry{key: key, frame: frame})
}

// previewManager owns the selected row's broker preview subscription. It is
// driven only by the supervisor's serialized loops; generation never
// decreases, including across broker reconnections.
type previewManager struct {
	clock      ports.Clock
	generation ports.BrokerPreviewGeneration
	sub        ports.BrokerPreviewSubscription
	request    ports.BrokerPreviewRequest
	// pending is the debounced selection not yet subscribed. Its wake is
	// the manager's changed channel until it fires or is replaced.
	pending *previewDebounce
	cache   previewCache
	// frame is the last OK frame shown for the current selection.
	frame    protocol.RemotePreview
	hasFrame bool
}

// previewDebounce is one armed selection. Its goroutine only forwards the
// clock timer to wake and exits on fire or cancellation.
type previewDebounce struct {
	service ports.BrokerService
	route   ports.BrokerPreviewRoute
	preview protocol.RemotePreviewRequest
	wake    chan struct{}
	stop    chan struct{}
}

func (d *previewDebounce) cancel() { close(d.stop) }

func (m *previewManager) armDebounce(service ports.BrokerService, route ports.BrokerPreviewRoute, preview protocol.RemotePreviewRequest) {
	pending := &previewDebounce{service: service, route: route, preview: preview, wake: make(chan struct{}, 1), stop: make(chan struct{})}
	m.pending = pending
	if supervisorNil(m.clock) {
		pending.wake <- struct{}{}
		return
	}
	timer := m.clock.NewTimer(pickerPreviewDebounce)
	go func() {
		select {
		case <-timer.C():
			pending.wake <- struct{}{}
		case <-pending.stop:
			timer.Stop()
		}
	}()
}

// stop ends the current subscription and debounce without touching the
// display or the cache.
func (m *previewManager) stop() {
	if m.pending != nil {
		m.pending.cancel()
		m.pending = nil
	}
	if m.sub != nil {
		m.sub.Close()
		m.sub = nil
	}
	m.request = ports.BrokerPreviewRequest{}
}

func (m *previewManager) close(host pickerHost) {
	m.stop()
	m.frame, m.hasFrame = protocol.RemotePreview{}, false
	if picker, ok := host.(pickerPreviewHost); ok {
		picker.ClearPreview()
	}
}

// selected reports whether route/preview is already subscribed or debounced.
func (m *previewManager) selected(route ports.BrokerPreviewRoute, preview protocol.RemotePreviewRequest) bool {
	if m.pending != nil {
		return m.pending.route == route && m.pending.preview == preview
	}
	return m.sub != nil && m.request.Route == route && m.request.Preview == preview
}

// refresh follows the picker's selected row. A new selection shows its cached
// frame (stale) or a loading placeholder at once, then subscribes after the
// cursor rests for the debounce.
func (m *previewManager) refresh(service ports.BrokerService, host pickerHost, size domain.Size) {
	picker, ok := host.(pickerPreviewHost)
	if supervisorNil(service) || !ok {
		m.close(host)
		return
	}
	route, preview, ok := picker.PreviewRequest(size)
	if !ok {
		m.close(host)
		return
	}
	if m.selected(route, preview) {
		return
	}
	m.stop()
	m.frame, m.hasFrame = m.cache.get(previewKeyFor(preview))
	if m.hasFrame {
		picker.SetPreview(m.frame, previewStale)
	} else {
		picker.SetPreview(protocol.RemotePreview{}, previewLoading)
	}
	m.armDebounce(service, route, preview)
}

func (m *previewManager) changed() <-chan struct{} {
	if m.pending != nil {
		return m.pending.wake
	}
	if m.sub == nil {
		return nil
	}
	return m.sub.Changed()
}

// publish handles one wake of changed and reports whether the display
// changed.
func (m *previewManager) publish(host pickerHost) bool {
	picker, ok := host.(pickerPreviewHost)
	if !ok {
		return false
	}
	if m.pending != nil {
		return m.subscribe(picker)
	}
	if m.sub == nil {
		return false
	}
	publication := m.sub.Latest()
	if publication.Validate() != nil ||
		publication.Epoch != m.request.Epoch || publication.Connection != m.request.Connection ||
		publication.Generation != m.request.Generation || publication.Target != m.request.Preview.Target ||
		publication.Width != m.request.Preview.Width || publication.Height != m.request.Preview.Height {
		return false
	}
	if publication.Err == nil && publication.Preview.Status == protocol.RemotePreviewOK {
		m.frame, m.hasFrame = publication.Preview, true
		m.cache.put(previewKeyFor(m.request.Preview), publication.Preview)
		picker.SetPreview(publication.Preview, previewFresh)
		return true
	}
	missing := publication.Err == nil && publication.Preview.Status == protocol.RemotePreviewNoSuchTarget
	m.showFailure(picker, missing)
	return true
}

// subscribe opens the debounced selection's broker subscription.
func (m *previewManager) subscribe(picker pickerPreviewHost) bool {
	pending := m.pending
	m.pending = nil
	m.generation++
	request := ports.BrokerPreviewRequest{Epoch: pending.service.Snapshot().Epoch, Connection: pending.service.ConnectionID(), Generation: m.generation, Route: pending.route, Preview: pending.preview}
	sub, err := pending.service.SubscribePreview(request)
	if err != nil || supervisorNil(sub) {
		m.showFailure(picker, false)
		return true
	}
	m.request, m.sub = request, sub
	return false
}

// showFailure keeps the last good frame, marked stale, or names why there is
// none.
func (m *previewManager) showFailure(picker pickerPreviewHost, missing bool) {
	switch {
	case m.hasFrame:
		picker.SetPreview(m.frame, previewStale)
	case missing:
		picker.SetPreview(protocol.RemotePreview{}, previewMissing)
	default:
		picker.SetPreview(protocol.RemotePreview{}, previewUnavailable)
	}
}

// previewView turns one frame and state into the picker's preview pane. Stale
// frames are dimmed; frameless states show one short dim label.
func previewView(preview protocol.RemotePreview, state previewState) pickerusecase.Preview {
	if state == previewFresh || state == previewStale {
		rows := preview.FrameRows()
		if preview.Status != protocol.RemotePreviewOK || rows == nil {
			return pickerusecase.Preview{}
		}
		if state == previewStale {
			for _, row := range rows {
				for x := range row {
					row[x].Style.Attrs |= renderer.AttrDim
				}
			}
		}
		return pickerusecase.Preview{Rows: rows, Width: int(preview.Width), Height: int(preview.Height)}
	}
	label := "loading preview…"
	switch state {
	case previewUnavailable:
		label = "preview unavailable"
	case previewMissing:
		label = "session gone"
	}
	style := renderer.DefaultStyle()
	style.Attrs |= renderer.AttrDim
	row := make([]renderer.Cell, 0, len(label))
	for _, r := range label {
		row = append(row, renderer.Cell{Rune: r, Style: style})
	}
	return pickerusecase.Preview{Rows: [][]renderer.Cell{row}, Width: len(row), Height: 1}
}
