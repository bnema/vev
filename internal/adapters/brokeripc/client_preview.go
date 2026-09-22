package brokeripc

import (
	"sync"

	"github.com/bnema/vev/internal/adapters/brokerwire"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

type clientPreviewSubscription struct {
	client  *client
	request ports.BrokerPreviewRequest
	mu      sync.Mutex
	latest  ports.BrokerPreviewPublication
	changed chan struct{}
	once    sync.Once
}

func newClientPreviewSubscription(c *client, request ports.BrokerPreviewRequest) *clientPreviewSubscription {
	return &clientPreviewSubscription{client: c, request: request, changed: make(chan struct{}, 1), latest: ports.BrokerPreviewPublication{
		Epoch: request.Epoch, Connection: request.Connection, Generation: request.Generation, Target: request.Preview.Target,
		Width: request.Preview.Width, Height: request.Preview.Height, Err: ports.BrokerError{Code: ports.BrokerErrorUnavailable, Text: "preview pending"},
	}}
}
func (s *clientPreviewSubscription) Changed() <-chan struct{} { return s.changed }
func (s *clientPreviewSubscription) Latest() ports.BrokerPreviewPublication {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.latest
}
func (s *clientPreviewSubscription) publish(p ports.BrokerPreviewPublication) {
	s.mu.Lock()
	s.latest = p
	s.mu.Unlock()
	select {
	case s.changed <- struct{}{}:
	default:
	}
}
func (s *clientPreviewSubscription) Close() {
	s.once.Do(func() {
		s.client.mu.Lock()
		active := s.client.preview == s
		if active {
			s.client.preview = nil
		}
		s.client.mu.Unlock()
		if active {
			_ = s.client.sendAsync(brokerwire.CancelPreview{Epoch: s.request.Epoch, Connection: s.request.Connection, Generation: s.request.Generation})
		}
	})
}

var _ ports.BrokerPreviewSubscription = (*clientPreviewSubscription)(nil)
var _ = protocol.RemotePreview{}
