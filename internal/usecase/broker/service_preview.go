package broker

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

const previewPollCadence = time.Second

// SubscribePreview replaces this connection's selected-row observation after
// validating the complete authority and its strictly increasing generation.
func (s *Service) SubscribePreview(request ports.BrokerPreviewRequest) (ports.BrokerPreviewSubscription, error) {
	if err := request.Validate(); err != nil {
		return nil, errors.Join(ports.BrokerAdmissionInvalid, err)
	}
	if request.Epoch != s.epoch || request.Connection != s.id {
		return nil, ports.BrokerAdmissionStale
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ports.BrokerAdmissionClosed
	}
	if request.Generation <= s.previewGeneration {
		s.mu.Unlock()
		return nil, ports.BrokerAdmissionStale
	}
	previous := s.preview
	s.previewGeneration = request.Generation
	s.preview = nil
	s.mu.Unlock()
	if previous != nil {
		previous.Close()
	}

	ctx, cancel := context.WithCancel(s.ctx)
	sub := &servicePreviewSubscription{
		service: s, request: request, ctx: ctx, cancel: cancel,
		changed: make(chan struct{}, 1),
		latest:  previewPublication(request, protocol.RemotePreview{}, ports.BrokerError{Code: ports.BrokerErrorUnavailable, Text: "preview pending"}),
		done:    make(chan struct{}),
	}
	s.mu.Lock()
	if s.closed || s.previewGeneration != request.Generation {
		s.mu.Unlock()
		sub.Close()
		return nil, ports.BrokerAdmissionStale
	}
	s.preview = sub
	s.wg.Add(1)
	s.mu.Unlock()
	go func() { defer s.wg.Done(); sub.run() }()
	return sub, nil
}

// previewIdentifiesTarget rejects a daemon preview whose inner identity does
// not name the requested session lifecycle and live tab. A non-OK status
// carries no inner identity and is always accepted: it is already typed as
// unavailable rather than as another tab's content. Successful previews
// report viewport-maximum dimensions from the daemon crop, so inner
// dimensions must fit inside the request instead of matching it exactly.
func previewIdentifiesTarget(preview protocol.RemotePreview, request protocol.RemotePreviewRequest) bool {
	if preview.Status != protocol.RemotePreviewOK {
		return true
	}
	return preview.LifecycleID == request.Target.LifecycleID &&
		preview.TabID == request.Target.LiveTabID &&
		preview.Width != 0 && preview.Height != 0 &&
		preview.Width <= request.Width && preview.Height <= request.Height
}

func previewPublication(request ports.BrokerPreviewRequest, preview protocol.RemotePreview, err error) ports.BrokerPreviewPublication {
	return ports.BrokerPreviewPublication{Epoch: request.Epoch, Connection: request.Connection, Generation: request.Generation,
		Target: request.Preview.Target, Width: request.Preview.Width, Height: request.Preview.Height, Preview: preview, Err: err}
}

type servicePreviewSubscription struct {
	service *Service
	request ports.BrokerPreviewRequest
	ctx     context.Context
	cancel  context.CancelFunc
	mu      sync.Mutex
	latest  ports.BrokerPreviewPublication
	changed chan struct{}
	done    chan struct{}
	once    sync.Once
}

func (p *servicePreviewSubscription) Changed() <-chan struct{} { return p.changed }
func (p *servicePreviewSubscription) Latest() ports.BrokerPreviewPublication {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.latest
}
func (p *servicePreviewSubscription) Close() {
	p.once.Do(func() {
		p.cancel()
		p.service.mu.Lock()
		if p.service.preview == p {
			p.service.preview = nil
		}
		p.service.mu.Unlock()
	})
}

func (p *servicePreviewSubscription) current() bool {
	if p.ctx.Err() != nil || p.request.Validate() != nil {
		return false
	}
	p.service.mu.Lock()
	defer p.service.mu.Unlock()
	return !p.service.closed && p.service.epoch == p.request.Epoch && p.service.id == p.request.Connection && p.service.preview == p && p.service.previewGeneration == p.request.Generation
}

func (p *servicePreviewSubscription) publish(preview protocol.RemotePreview, err error) bool {
	if !p.current() {
		return false
	}
	publication := previewPublication(p.request, preview, err)
	if publication.Validate() != nil {
		return false
	}
	p.mu.Lock()
	p.latest = publication
	p.mu.Unlock()
	select {
	case p.changed <- struct{}{}:
	default:
	}
	return true
}

func (p *servicePreviewSubscription) run() {
	defer close(p.done)
	for p.current() {
		// Every poll opens one fresh logical stream. Stream identities are
		// consumed exactly once by the pool admission window, so reusing the
		// original route's stream would be refused as stale after the first
		// poll and degrade the preview to perpetual unavailability.
		route := p.request.Route
		streamID, err := p.service.NextStreamID()
		if err == nil {
			route.Stream = streamID
		}
		var stream ports.BrokerLogicalConnection
		if err == nil {
			stream, err = p.service.OpenStream(p.ctx, route)
		}
		if err == nil {
			err = stream.SendClient(p.request.Preview)
		}
		var preview protocol.RemotePreview
		if err == nil {
			var message protocol.ServerMessage
			message, err = stream.ReceiveServer()
			if err == nil {
				var ok bool
				preview, ok = message.(protocol.RemotePreview)
				if !ok {
					err = errors.New("broker: invalid preview response")
				}
			}
		}
		if stream != nil {
			_ = stream.Close()
		}
		if err == nil && !previewIdentifiesTarget(preview, p.request.Preview) {
			err = errors.New("broker: preview response is for another target")
		}

		if !p.current() || errors.Is(err, context.Canceled) {
			return
		}
		if err == nil {
			if !p.publish(preview, nil) {
				return
			}
		} else if !p.publish(protocol.RemotePreview{}, ports.BrokerError{Code: ports.BrokerErrorUnavailable, Text: "preview unavailable", Cause: err}) {
			return
		}
		timer := p.service.clock.NewTimer(previewPollCadence)
		select {
		case <-p.ctx.Done():
			timer.Stop()
			return
		case <-timer.C():
		}
	}
}
