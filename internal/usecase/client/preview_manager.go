package client

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// previewManager is driven only by the supervisor's serialized ready loop.
// generation never decreases, including across broker reconnections.
type pickerPreviewHost interface {
	PreviewRequest(ports.BrokerConnectionID, ports.BrokerStreamID, domain.Size) (ports.BrokerOpenStreamRequest, protocol.RemotePreviewRequest, bool)
	SetPreview(protocol.RemotePreview)
	ClearPreview()
}

type previewManager struct {
	generation ports.BrokerPreviewGeneration
	sub        ports.BrokerPreviewSubscription
	request    ports.BrokerPreviewRequest
}

func (m *previewManager) close(host pickerHost) {
	picker, _ := host.(pickerPreviewHost)
	m.generation++ // invalidate a publication already selected by the runtime
	if m.sub != nil {
		m.sub.Close()
		m.sub = nil
	}
	m.request = ports.BrokerPreviewRequest{}
	if picker != nil {
		picker.ClearPreview()
	}
}

func (m *previewManager) refresh(service ports.BrokerService, host pickerHost, size domain.Size) {
	picker, ok := host.(pickerPreviewHost)
	if service == nil || !ok {
		m.close(host)
		return
	}
	// Probe selection eligibility before consuming a logical stream ID. The
	// controller only uses the provisional non-zero ID for validation.
	_, preview, ok := picker.PreviewRequest(service.ConnectionID(), 1, size)
	if !ok {
		m.close(host)
		return
	}
	if m.sub != nil && m.request.Preview.Target == preview.Target && m.request.Preview.Width == preview.Width && m.request.Preview.Height == preview.Height {
		return
	}
	stream, err := service.NextStreamID()
	if err != nil {
		m.close(host)
		return
	}
	route, preview, ok := picker.PreviewRequest(service.ConnectionID(), stream, size)
	if !ok {
		m.close(host)
		return
	}
	m.close(host) // contract requires old observation closed before replacement
	m.generation++
	req := ports.BrokerPreviewRequest{Epoch: route.Epoch, Connection: service.ConnectionID(), Generation: m.generation, Route: route, Preview: preview}
	sub, err := service.SubscribePreview(req)
	if err != nil || supervisorNil(sub) {
		return
	}
	m.request, m.sub = req, sub
}

func (m *previewManager) changed() <-chan struct{} {
	if m.sub == nil {
		return nil
	}
	return m.sub.Changed()
}

func (m *previewManager) publish(host pickerHost) bool {
	picker, ok := host.(pickerPreviewHost)
	if m.sub == nil || !ok {
		return false
	}
	publication := m.sub.Latest()
	if publication.Validate() != nil || publication.Err != nil ||
		publication.Epoch != m.request.Epoch || publication.Connection != m.request.Connection ||
		publication.Generation != m.request.Generation || publication.Target != m.request.Preview.Target ||
		publication.Width != m.request.Preview.Width || publication.Height != m.request.Preview.Height {
		return false
	}
	picker.SetPreview(publication.Preview)
	return true
}
