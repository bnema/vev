package brokeripc

import (
	"errors"
	"sync"

	"github.com/bnema/vev/internal/adapters/brokerwire"
	"github.com/bnema/vev/internal/ports"
)

func (s *serverSession) startPreview(m brokerwire.StartPreview) error {
	request := ports.BrokerPreviewRequest{Epoch: m.Epoch, Connection: m.Connection, Generation: m.Generation,
		Route: m.Route, Preview: m.Preview}
	s.subMu.Lock()
	if m.Generation <= s.previewGeneration {
		s.subMu.Unlock()
		return s.sendError(ports.BrokerAdmissionStale)
	}
	s.subMu.Unlock()
	sub, err := s.core.SubscribePreview(request)
	if err != nil {
		return s.sendError(err)
	}
	forwarder := newPreviewForwarder(sub, request.Generation)
	s.subMu.Lock()
	previous := s.preview
	previousForward := s.previewForward
	s.preview, s.previewGeneration, s.previewForward = sub, m.Generation, forwarder
	s.subMu.Unlock()
	if previousForward != nil {
		previousForward.stop()
	}
	if previous != nil {
		previous.Close()
	}
	go s.forwardPreview(sub, forwarder, request)
	return nil
}

// previewForwarder is the replacement signal for one server-side preview
// forward loop. A replaced subscription's Changed channel never signals
// again, so the loop cannot notice its replacement by waiting on it: closing
// stopped wakes the loop so it exits instead of parking until session
// shutdown. It is safe for concurrent use and idempotent.
type previewForwarder struct {
	stopped chan struct{}
	once    sync.Once
}

func newPreviewForwarder(sub ports.BrokerPreviewSubscription, generation ports.BrokerPreviewGeneration) *previewForwarder {
	_ = sub
	_ = generation
	return &previewForwarder{stopped: make(chan struct{})}
}

func (f *previewForwarder) stop() {
	if f == nil {
		return
	}
	f.once.Do(func() { close(f.stopped) })
}

func (s *serverSession) forwardPreview(sub ports.BrokerPreviewSubscription, forwarder *previewForwarder, request ports.BrokerPreviewRequest) {
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-forwarder.stopped:
			return
		case <-sub.Changed():
			s.subMu.Lock()
			active := s.preview == sub && s.previewGeneration == request.Generation
			s.subMu.Unlock()
			if !active {
				return
			}
			publication := sub.Latest()
			message := brokerwire.PreviewPublication{Epoch: publication.Epoch, Connection: publication.Connection, Generation: publication.Generation, Preview: publication.Preview}
			if publication.Err != nil {
				message.HasError = true
				message.Error = errorDetail(publication.Err)
			}
			if err := s.send(message); err != nil {
				s.cancel()
				return
			}
		}
	}
}

func (s *serverSession) sendError(err error) error {
	if errors.Is(err, ports.BrokerAdmissionStale) {
		return s.send(brokerwire.BrokerErrorMessage{Epoch: s.scope.Epoch, Connection: s.scope.Connection, Error: errorDetail(err)})
	}
	return err
}
