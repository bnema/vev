package brokeripc

import (
	"context"
	"errors"

	"github.com/bnema/vev/internal/adapters/brokerwire"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

// Snapshot publication and mutating operations (P3.3, server side).
//
// A Subscribe frame starts one publisher goroutine for exactly that connection
// and generation. The publisher waits on the admitted core service's coalescing
// subscription, reads the newest publication, and writes it as one multipart
// transfer. A slow client therefore blocks only its own publisher: the core
// subscription coalesces, so the next wake reads the newest snapshot rather than
// replaying a backlog, and no other connection is affected. Each generation owns
// its wake channel, so a superseded publisher can never consume the wake that
// was meant for its successor, and a resubscribe or resync always publishes the
// current snapshot for the generation that asked.
//
// Mutating operations run one goroutine each, bounded by the brokerwire
// per-connection pending-operation tracker, and complete exactly once with a
// typed result. The request context is the connection's, so shutdown cancels an
// operation that is still in flight.

// publisher is one subscription generation's publication series: the core
// subscription it drains, the context that ends it, and the capacity-one wake
// channel that coalesces demands for it. The wake is owned by exactly one
// generation, so a superseded publisher can never drain a later generation's
// demand.
type publisher struct {
	generation brokerwire.SubscriptionGeneration
	sub        ports.BrokerSubscription
	cancel     context.CancelFunc
	wake       chan struct{}
}

// passiveSubscriber is the optional core capability behind a passive
// Subscribe: a read-only snapshot subscription that never counts as
// observation demand. A core without it serves every subscription as demand.
type passiveSubscriber interface {
	SubscribePassive() (ports.BrokerSubscription, error)
}

// retargetPublisher replaces any active publisher with one for the new
// generation and publishes the current snapshot immediately. A failed
// subscription ends the superseded series first, so no publisher survives to
// publish under a generation the connection tracker already retired.
func (s *serverSession) retargetPublisher(generation brokerwire.SubscriptionGeneration, passive bool) error {
	subscribe := s.core.Subscribe
	if reader, ok := s.core.(passiveSubscriber); passive && ok {
		subscribe = reader.SubscribePassive
	}
	sub, err := subscribe()
	if err != nil {
		s.stopPublisher()
		detail := errorDetail(err)
		return s.send(brokerwire.BrokerErrorMessage{Epoch: s.epoch, Connection: s.scope.Connection, Error: detail})
	}
	s.stopPublisher()
	ctx, cancel := context.WithCancel(s.ctx)
	pub := &publisher{generation: generation, sub: sub, cancel: cancel, wake: make(chan struct{}, 1)}
	s.subMu.Lock()
	s.pub = pub
	s.subMu.Unlock()
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer sub.Close()
		s.publishLoop(ctx, pub)
	}()
	s.wakePublisher()
	return nil
}

// publishLoop publishes the newest committed snapshot on every coalesced wake
// for one generation until it is superseded or the session ends.
func (s *serverSession) publishLoop(ctx context.Context, pub *publisher) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-pub.wake:
		case <-pub.sub.Changed():
		}
		if err := s.publishSnapshot(pub.generation); err != nil {
			// A publication failure is a carriage failure: settle the
			// connection so its reader observes the terminal cause.
			s.abort(err)
			return
		}
	}
}

// publishSnapshot writes one multipart transfer for the active generation.
func (s *serverSession) publishSnapshot(generation brokerwire.SubscriptionGeneration) error {
	snapshot := s.core.Snapshot()
	if snapshot.Epoch == 0 {
		// The core has published nothing yet; there is no transfer to send.
		return nil
	}
	if snapshot.Epoch != s.epoch {
		// A publication from another broker incarnation can never be applied
		// to this connection's scope.
		return nil
	}
	parts, err := snapshotParts(snapshot, s.epoch, s.scope.Connection, generation)
	if err != nil {
		return err
	}
	total, err := snapshotTransferBytes(parts, s.ceilings.MaxReceiveEnvelopeBytes, s.ceilings.StreamChunkLimit)
	if err != nil {
		return err
	}
	if total > uint64(brokerwire.MaxSnapshotStagedBytes) {
		return errors.New("brokeripc: snapshot publication exceeds the staged ceiling")
	}
	for _, part := range parts {
		if err := s.send(part); err != nil {
			return err
		}
	}
	return nil
}

// stopPublisher ends the active publication series, if any. The superseded
// publisher observes the canceled context and its closed subscription, and only
// its own wake channel could ever wake it, so it can never drain a demand meant
// for a later generation.
func (s *serverSession) stopPublisher() {
	s.subMu.Lock()
	pub := s.pub
	s.pub = nil
	s.subMu.Unlock()
	if pub == nil {
		return
	}
	pub.cancel()
	pub.sub.Close()
}

// wakePublisher coalesces one publication demand on the active generation's own
// wake channel. A demand raised while no publisher is active is dropped: the
// next subscription publishes the current snapshot as its first action.
func (s *serverSession) wakePublisher() {
	s.subMu.Lock()
	pub := s.pub
	s.subMu.Unlock()
	if pub == nil {
		return
	}
	select {
	case pub.wake <- struct{}{}:
	default:
	}
}

// startMutation admits one mutating operation under the brokerwire pending
// bound and runs it in its own goroutine. A duplicate admission is dropped:
// exactly one completion ever travels for one operation identity, and the peer's
// own tracker already fences replays.
func (s *serverSession) startMutation(operation ports.BrokerOperationID, kind brokerwire.RegisterMutationKind, run func(context.Context) (domain.RemoteRegistration, bool, error)) error {
	if err := s.conn.AdmitOperation(operation); err != nil {
		switch {
		case errors.Is(err, brokerwire.ErrOperationCompleted), errors.Is(err, brokerwire.ErrOperationPending):
			return nil
		case errors.Is(err, brokerwire.ErrTooManyPendingOperations):
			detail := errorDetail(ports.BrokerAdmissionLimit)
			return s.send(brokerwire.BrokerErrorMessage{Epoch: s.epoch, Connection: s.scope.Connection, Error: detail})
		default:
			return errors.Join(ErrProtocol, err)
		}
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		registration, removed, err := run(s.ctx)
		outcome := ports.BrokerOutcomeOK
		if err != nil {
			outcome = ports.BrokerOutcomeFailed
			var unknown ports.BrokerStoreOutcomeUnknownError
			var unknownPointer *ports.BrokerStoreOutcomeUnknownError
			if errors.As(err, &unknown) || errors.As(err, &unknownPointer) {
				outcome = ports.BrokerOutcomeUnknown
			}
			registration = domain.RemoteRegistration{}
			removed = false
		}
		_ = s.conn.CompleteOperation(operation, outcome)
		if s.ctx.Err() != nil {
			return
		}
		detail := errorDetail(err)
		result := brokerwire.OperationResult{
			Epoch: s.epoch, Connection: s.scope.Connection, Operation: operation,
			Outcome: outcome, Removed: removed, Registration: registration,
			Error: detail, HasError: detail != (brokerwire.ErrorDetail{}),
		}
		if err := result.ValidateForMutation(kind); err != nil {
			s.abort(errors.Join(ErrProtocol, err))
			return
		}
		if err := s.send(result); err != nil {
			s.abort(err)
		}
	}()
	return nil
}
