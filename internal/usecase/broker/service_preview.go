package broker

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// Preview push cadence and recovery. The broker owns the frame rate: local
// previews coalesce at display speed, remote ones at a bandwidth-friendly rate.
const (
	previewLocalInterval   = 33 * time.Millisecond
	previewRemoteInterval  = 125 * time.Millisecond
	previewFirstFrameLimit = 3 * time.Second
	previewReopenMin       = 250 * time.Millisecond
	previewReopenMax       = 5 * time.Second
)

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
		latest:  previewPublication(request, protocol.RemotePreview{}, previewUnavailable("preview pending", nil)),
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

func previewUnavailable(text string, cause error) error {
	return ports.BrokerError{Code: ports.BrokerErrorUnavailable, Text: text, Cause: cause}
}

func previewPublication(request ports.BrokerPreviewRequest, preview protocol.RemotePreview, err error) ports.BrokerPreviewPublication {
	return ports.BrokerPreviewPublication{Epoch: request.Epoch, Connection: request.Connection, Generation: request.Generation,
		Target: request.Preview.Target, Width: request.Preview.Width, Height: request.Preview.Height, Preview: preview, Err: err}
}

// previewStreamRequest builds the observation stream for one watch. The broker
// allocates the stream identity, so a client never names one.
func (s *Service) previewStreamRequest(route ports.BrokerPreviewRoute) (ports.BrokerOpenStreamRequest, error) {
	stream, err := s.NextStreamID()
	if err != nil {
		return ports.BrokerOpenStreamRequest{}, err
	}
	return ports.BrokerOpenStreamRequest{
		Epoch: s.epoch, Connection: s.id, Stream: stream,
		Purpose: ports.BrokerStreamObservation, StartMode: ports.BrokerDaemonExistingOnly,
		Local: route.Local, Endpoint: route.Endpoint, Registration: route.Registration, Policy: route.Policy,
	}, nil
}

type servicePreviewSubscription struct {
	service *Service
	request ports.BrokerPreviewRequest
	ctx     context.Context
	cancel  context.CancelFunc
	mu      sync.Mutex
	latest  ports.BrokerPreviewPublication
	changed chan struct{}
	once    sync.Once
}

func (p *servicePreviewSubscription) Changed() <-chan struct{} { return p.changed }
func (p *servicePreviewSubscription) Latest() ports.BrokerPreviewPublication {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.latest
}

// Close never waits: cancellation closes the live stream, which unblocks the
// watch goroutine. The service wait group joins it.
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
	if p.ctx.Err() != nil {
		return false
	}
	p.service.mu.Lock()
	defer p.service.mu.Unlock()
	return !p.service.closed && p.service.preview == p && p.service.previewGeneration == p.request.Generation
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

// run keeps one daemon watch stream open for the subscription's lifetime and
// reopens it with a bounded backoff after a failure. A dead target ends the
// subscription's recovery: there is nothing left to watch.
func (p *servicePreviewSubscription) run() {
	backoff := previewReopenMin
	for p.current() {
		reopen, delivered := p.watch()
		if !reopen || !p.current() {
			return
		}
		if delivered {
			backoff = previewReopenMin
		}
		timer := p.service.clock.NewTimer(backoff)
		select {
		case <-p.ctx.Done():
			timer.Stop()
			return
		case <-timer.C():
		}
		backoff = min(2*backoff, previewReopenMax)
	}
}

// watch runs one stream until it fails. It reports whether the stream should
// be reopened and whether it delivered at least one frame.
func (p *servicePreviewSubscription) watch() (reopen, delivered bool) {
	interval := previewRemoteInterval
	if p.request.Route.Local {
		interval = previewLocalInterval
	}
	route, err := p.service.previewStreamRequest(p.request.Route)
	var stream ports.BrokerLogicalConnection
	if err == nil {
		stream, err = p.service.OpenStream(p.ctx, route)
	}
	if err != nil {
		return p.fail(err), false
	}
	defer func() { _ = stream.Close() }()
	stop := context.AfterFunc(p.ctx, func() { _ = stream.Close() })
	defer stop()
	if err := stream.SendClient(protocol.RemotePreviewWatch{Request: p.request.Preview, MinInterval: interval}); err != nil {
		return p.fail(err), false
	}

	// The first-frame deadline only reports the stall; the stream keeps
	// waiting, so a slow host still delivers once it answers.
	firstFrame := make(chan struct{})
	deadlineDone := make(chan struct{})
	timer := p.service.clock.NewTimer(previewFirstFrameLimit)
	go func() {
		defer close(deadlineDone)
		select {
		case <-timer.C():
			p.publish(protocol.RemotePreview{}, previewUnavailable("preview timed out", ports.BrokerError{Code: ports.BrokerErrorTimeout}))
		case <-firstFrame:
			timer.Stop()
		}
	}()
	var firstOnce sync.Once
	markFirst := func() { firstOnce.Do(func() { close(firstFrame) }) }
	defer func() { markFirst(); <-deadlineDone }()

	for {
		message, err := stream.ReceiveServer()
		if err != nil {
			return p.fail(err), delivered
		}
		preview, ok := message.(protocol.RemotePreview)
		if !ok || protocol.ValidateRemotePreview(preview) != nil {
			return p.fail(errors.New("broker: invalid preview response")), delivered
		}
		if !previewIdentifiesTarget(preview, p.request.Preview) {
			return p.fail(errors.New("broker: preview response is for another target")), delivered
		}
		markFirst()
		delivered = true
		if !p.publish(preview, nil) {
			return false, delivered
		}
		if preview.Status == protocol.RemotePreviewNoSuchTarget {
			return false, delivered
		}
	}
}

// fail publishes one typed unavailable failure and reports whether recovery
// should continue. Cancellation is the owner leaving, not a failure.
func (p *servicePreviewSubscription) fail(err error) bool {
	if !p.current() || errors.Is(err, context.Canceled) {
		return false
	}
	return p.publish(protocol.RemotePreview{}, previewUnavailable("preview unavailable", err))
}
