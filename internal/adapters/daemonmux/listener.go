// Typed server-side listener over one physical Pump (P3.2).
//
// Listener is the daemon half of one physical daemonmux connection. The pump's
// reader admits each incoming MuxOpen; the listener turns every admitted stream
// into one independent typed ports.ServerConnection through sessionwire over
// the mux stream, exactly as LogicalConnector does on the broker side. Like the
// connector it owns neither framing nor composition: the carriage is the pump's
// private byte stream and the typed protocol is sessionwire's.
//
// Admission and acceptance are decoupled by a bounded FIFO accept queue: an
// admitted stream is queued, Accept hands back the oldest still-live one, and a
// slow or stalled sibling never blocks the daemon's accept loop. One physical
// connection admits at most its negotiated stream ceiling, so the queue is
// bounded by MaxAcceptQueue (<=128) by construction; a stream admitted while
// the queue is already full is refused on its own stream - exactly one mux
// Reset, with no effect on a sibling or on the physical connection - instead of
// growing the queue.
//
// Every stream carries one absolute local handshake deadline fixed at Open
// admission (admission time + protocol.HandshakeTimeout). Queue delay and the
// session handshake spend that one budget: the listener never restarts it, the
// session connection built for the stream adopts the same absolute deadline
// instead of starting a second one, and the deadline and the completion signal
// are exposed to the daemon through the narrow optional HandshakePlumbing
// interface. When the deadline elapses before the session handshake completed,
// the deadline watchdog resets exactly that stream (one mux Reset, its
// admission slot released) and leaves every sibling and the physical connection
// untouched. A completed handshake retires the watchdog, so a stream that
// finished its handshake is never expired afterwards.
//
// Failures stay stream-local and are never fatal to the shared accept loop: a
// stream the peer resets or overflows, and a malformed inner session frame, all
// settle that stream alone, and Accept skips settled streams instead of
// returning an error for them. Accept fails only when it can never again
// produce a stream: the listener was closed (ErrListenerClosed) or the one
// physical connection reached its terminal outcome (ErrListenerLost, wrapping
// the pump's cause), which is exactly when the daemon's accept loop must stop.
//
// Close unblocks every blocked Accept, drains every admitted-but-unaccepted
// stream through its one stream-local Reset (releasing each admission slot and
// unblocking the broker's pending Open), ends the deadline watchdog, and joins
// it. It closes neither the physical pump - the pooled physical connection
// outlives the listener - nor a connection already handed to the daemon.
//
// The listener must be constructed before the pump starts: it registers the
// pump's single admission observer, which the pump refuses after Start with
// ErrAdmissionObserverLate. That ordering is what guarantees the listener sees
// every admitted Open. A stream whose inner session carriage closes on its own
// during the handshake (a sessionwire preamble failure) is settled by the
// connection's terminal watcher with exactly one stream-local Reset, releasing
// its admission slot and watchdog without any consumer Close.
package daemonmux

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/bnema/vev/internal/adapters/clock"
	"github.com/bnema/vev/internal/adapters/sessionwire"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// Listener sentinels. They are typed so a caller can classify the two fatal
// Accept outcomes without matching on message text.
var (
	// ErrListenerConfig reports a listener constructed without a pump or over a
	// pump that does not read broker-to-daemon frames.
	ErrListenerConfig = errors.New("daemonmux: invalid listener configuration")
	// ErrListenerClosed reports Accept on a listener that was closed.
	ErrListenerClosed = errors.New("daemonmux: listener is closed")
	// ErrListenerLost reports Accept once the one physical connection reached
	// its terminal outcome. The pump's cause is wrapped when it has one; the
	// outcome is stable for every later Accept.
	ErrListenerLost = errors.New("daemonmux: listener physical connection lost")
)

// errSessionCarriageClosed is the stream-local terminal cause published when a
// listener connection's inner session carriage closed on its own during the
// handshake - a sessionwire preamble failure closes the raw transport - without
// a stream cause already recorded and without any consumer Close. It never
// escapes as a physical failure: the offending stream is reset on its own.
var errSessionCarriageClosed = errors.New("daemonmux: session carriage closed during handshake")

const (
	// MaxAcceptQueue bounds the admitted-but-unaccepted streams one listener
	// holds: the negotiated per-connection stream ceiling (1..128). The engine
	// admits at most that many opening or open streams, so the listener's FIFO
	// can never exceed it; a stream admitted while the queue is full is refused
	// on its own stream rather than queued.
	MaxAcceptQueue = int(MaxMuxStreams)

	// acceptQueueFullText and listenerClosedText are the bounded,
	// presentation-safe texts carried by the stream-local Reset a listener
	// sends when it refuses a stream it cannot queue, and
	// handshakeDeadlineText is the text of the stream-local Reset it sends when
	// a stream's absolute handshake deadline elapses.
	acceptQueueFullText   = "accept queue full"
	listenerClosedText    = "listener closed"
	handshakeDeadlineText = "handshake deadline exceeded"

	// invalidAdmissionText is the bounded, presentation-safe text of the
	// stream-local Reset a listener sends when an admitted Open contradicts the
	// accepted carriage origin (a liar Local) or fails the closed admission
	// contract. The stream is refused rather than stamped with a provenance the
	// accepting side did not provision.
	invalidAdmissionText = "invalid session admission"

	// sessionCarriageClosedText is the bounded, presentation-safe text carried
	// by the stream-local Reset a listener connection sends when its inner
	// session carriage closed on its own during the handshake (a sessionwire
	// preamble failure) without any consumer Close.
	sessionCarriageClosedText = "session handshake failed"
)

// HandshakePlumbing is the narrow, optional seam a typed daemonmux connection
// exposes so its consumer can adopt the exact absolute handshake deadline the
// stream was admitted with and observe the session handshake's completion
// without restarting the deadline. Both the broker-side LogicalConnection and
// the daemon-side ListenerConnection implement it. It deliberately lives here,
// not in ports, so the session contract is unchanged by this plumbing; a
// consumer asserts it structurally.
type HandshakePlumbing interface {
	// HandshakeDeadline is the absolute local deadline of the handshake,
	// fixed at Open admission and stable for the connection's lifetime.
	HandshakeDeadline() time.Time
	// OnHandshakeComplete registers fn to run exactly once when the session
	// handshake has run, whether it succeeded or failed. Registering after
	// completion runs fn immediately.
	OnHandshakeComplete(func())
}

// Listener accepts the logical streams the daemon side of one physical
// daemonmux connection admitted. It implements ports.ServerListener over the
// pump's carriage: Accept returns independent typed ports.ServerConnection
// values, Close unblocks Accept and joins the listener's goroutine, and Addr
// names the multiplexed carriage. It is safe for concurrent use.
type Listener struct {
	pump       *Pump
	clock      ports.Clock
	budget     time.Duration
	queueLimit int
	// accepted is the provisioned carriage shape the physical handshake
	// admitted. Every admitted session connection is stamped with it, so the
	// daemon consumes the accepting side's authority rather than the peer's
	// Open.
	accepted ServerPolicyAdmission

	// mu guards the accept queue, the deadline registry, and the closed flag.
	// change is the closed-and-replaced broadcast channel every blocked Accept
	// and the watchdog select on, so a state change never loses a wakeup.
	mu     sync.Mutex
	queue  []*admittedStream
	active map[PhysicalStreamID]*admittedStream
	closed bool
	change chan struct{}

	done      chan struct{}
	watchDone chan struct{}

	closeOnce sync.Once
}

var (
	_ ports.ServerListener           = (*Listener)(nil)
	_ ports.ServerConnection         = (*ListenerConnection)(nil)
	_ HandshakePlumbing              = (*ListenerConnection)(nil)
	_ ports.SessionAdmissionProvider = (*ListenerConnection)(nil)
)

// admittedStream is one inbound logical stream the engine admitted and the
// listener tracks until it is settled: delivered with a completed handshake, or
// terminal (expired, peer-settled, refused, or released). admission is the
// cloned provisioned admission stamped when the stream was admitted.
type admittedStream struct {
	ref       StreamRef
	deadline  time.Time
	done      <-chan struct{}
	admission ports.SessionAdmission
	queued    bool
	settled   bool
}

// NewListener returns a listener over one daemon-side pump under the package
// policy: the accepted absolute handshake deadline budget is
// protocol.HandshakeTimeout, and the accept queue is bounded by
// MaxAcceptQueue. accepted is the provisioned carriage shape the physical
// handshake admitted: its exact policy and its locality origin. Every admitted
// session connection is stamped with it. The pump must decode broker-to-daemon
// frames (Inbound == DirectionClient) and emit daemon-to-broker frames, as the
// daemon side of a pooled physical connection does; a nil, engine-less, or
// broker-side pump, or an invalid accepted entry, is refused. The pump must not
// have started: the listener registers the pump's admission observer, and a
// pump that already started refuses the registration with
// ErrAdmissionObserverLate (wrapped in ErrListenerConfig), because an observer
// registered after Start could miss an already-admitted Open.
func NewListener(pump *Pump, accepted ServerPolicyAdmission) (*Listener, error) {
	return newListener(pump, protocol.HandshakeTimeout, MaxAcceptQueue, clock.New(), accepted)
}

// newListener builds a listener from explicit policy. budget is the handshake
// deadline budget each stream receives at admission, limit is the accept-queue
// bound, clock supplies the admission time and the deadline timer, and accepted
// is the provisioned carriage shape stamped on every admitted session
// connection. The exported constructor delegates with the package policy; a
// test tightens the budget, the queue bound, or the clock without changing live
// behavior.
func newListener(pump *Pump, budget time.Duration, limit int, timeSource ports.Clock, accepted ServerPolicyAdmission) (*Listener, error) {
	if pump == nil || pump.Engine() == nil || pump.Inbound() != DirectionClient {
		return nil, ErrListenerConfig
	}
	if err := accepted.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrListenerConfig, err)
	}
	if budget <= 0 {
		budget = protocol.HandshakeTimeout
	}
	if limit <= 0 {
		limit = MaxAcceptQueue
	}
	if timeSource == nil {
		timeSource = clock.New()
	}
	listener := &Listener{
		pump:       pump,
		clock:      timeSource,
		budget:     budget,
		queueLimit: limit,
		accepted:   accepted,
		active:     make(map[PhysicalStreamID]*admittedStream),
		change:     make(chan struct{}),
		done:       make(chan struct{}),
		watchDone:  make(chan struct{}),
	}
	if err := pump.OnAdmitted(listener.onAdmitted); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrListenerConfig, err)
	}
	go listener.watchLoop()
	return listener, nil
}

// Addr names the listener's carriage for ports.ServerListener. One physical
// daemonmux connection multiplexes every logical stream, so the listener has no
// per-stream address.
func (l *Listener) Addr() string { return "daemonmux" }

// Accept returns the oldest admitted stream as one independent typed session
// connection, blocking until one is available. It hands a stream back at most
// once and skips streams that already reached a terminal outcome - expired,
// peer-reset, overflowed, or refused - instead of returning them. It fails with
// ErrListenerClosed after Close and with ErrListenerLost once the physical
// connection reached its terminal outcome; both are stable, so the shared
// daemon accept loop stops exactly when no stream can ever arrive again. A
// malformed or failed individual stream never becomes an Accept error.
func (l *Listener) Accept() (ports.ServerConnection, error) {
	if l == nil {
		return nil, ErrListenerConfig
	}
	for {
		entry, wait, err := l.nextAdmitted()
		if err != nil {
			return nil, err
		}
		if entry != nil {
			if connection := l.deliver(entry); connection != nil {
				return connection, nil
			}
			continue
		}
		select {
		case <-wait:
		case <-l.done:
		case <-l.pump.Done():
		}
	}
}

// Close stops accepting: it unblocks every blocked Accept with
// ErrListenerClosed, drains every admitted-but-unaccepted stream through its
// one stream-local Reset (releasing its admission slot and unblocking the
// broker's pending Open), ends the deadline watchdog, and joins it. Close is
// idempotent and concurrent-safe; closeOnce guarantees a single drain across
// concurrent callers. It does not close the physical pump, which the pooled
// connection owns, and it does not close or abort a connection already handed
// to the daemon, which owns that stream from then on.
func (l *Listener) Close() error {
	if l == nil {
		return nil
	}
	l.closeOnce.Do(func() {
		l.mu.Lock()
		l.closed = true
		queued := l.queue
		l.queue = nil
		for _, entry := range queued {
			if entry.settled {
				continue
			}
			entry.settled = true
			delete(l.active, entry.ref.Physical)
		}
		l.signalLocked()
		l.mu.Unlock()
		// Settle every drained admission on its own stream: one Reset releases
		// the admission slot and unblocks the broker's pending Open. Every abort
		// runs outside the listener lock, and closeOnce makes it exactly once.
		for _, entry := range queued {
			l.abort(entry.ref.Physical, listenerClosedText)
		}
		close(l.done)
		<-l.watchDone
	})
	return nil
}

// sessionAdmissionFor stamps the provisioned admission metadata onto one
// admitted Open: the accepted entry's exact policy and origin, plus the peer's
// closed purpose, attachment variant, name, target, and bounded environment. It
// refuses an Open whose declared locality contradicts the accepted origin (a
// liar Local must never be stamped as the provisioned locality), whose offered
// policy is not the accepted member, or whose shape the closed admission
// contract rejects, so a consumer only ever receives a validated admission. The
// policy and locality checks are repeated here rather than relying on the
// engine's prior restriction, so a listener built without one still enforces
// the accepted member.
func sessionAdmissionFor(accepted ServerPolicyAdmission, open Open) (ports.SessionAdmission, error) {
	if !accepted.Policy.Compatible(open.Policy) {
		return ports.SessionAdmission{}, ErrInvalidMessage
	}
	origin := ports.SessionOriginRemote
	if open.Local {
		origin = ports.SessionOriginLocal
	}
	if accepted.Origin != origin {
		return ports.SessionAdmission{}, ErrInvalidMessage
	}
	admission := ports.SessionAdmission{
		Origin:    accepted.Origin,
		Policy:    accepted.Policy,
		Purpose:   open.Purpose,
		Admission: open.Admission,
		Name:      open.Name,
		Target:    open.Target,
		Env:       append([]string(nil), open.Env...),
	}
	if err := admission.Validate(); err != nil {
		return ports.SessionAdmission{}, ErrInvalidMessage
	}
	return admission, nil
}

// onAdmitted is the pump's admission observer. It runs on the pump's reader
// goroutine immediately after one inbound Open was admitted: it stamps the
// provisioned admission (refusing exactly that stream when the Open contradicts
// the accepted origin or fails the closed admission contract) and queues the
// stream under its admission deadline, or refuses exactly that stream when the
// accept queue is full or the listener is closed. It never blocks - the reader
// is the one goroutine that applies every inbound frame of the connection.
func (l *Listener) onAdmitted(open Open) {
	admitted := l.clock.Now()
	admission, err := sessionAdmissionFor(l.accepted, open)
	if err != nil {
		l.abort(open.Ref.Physical, invalidAdmissionText)
		return
	}
	entry := &admittedStream{
		ref:       open.Ref,
		deadline:  admitted.Add(l.budget),
		admission: admission,
	}
	if status, ok := l.pump.Engine().Status(open.Ref.Physical); ok {
		entry.done = status.Done
	}

	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		l.abort(open.Ref.Physical, listenerClosedText)
		return
	}
	if len(l.queue) >= l.queueLimit {
		l.mu.Unlock()
		l.abort(open.Ref.Physical, acceptQueueFullText)
		return
	}
	entry.queued = true
	l.queue = append(l.queue, entry)
	l.active[open.Ref.Physical] = entry
	l.signalLocked()
	l.mu.Unlock()
}

// nextAdmitted pops the oldest still-live admitted stream, or reports the
// channel to wait on and any stable fatal outcome. A stream that already
// reached a terminal outcome is dropped instead of handed to the daemon.
func (l *Listener) nextAdmitted() (*admittedStream, <-chan struct{}, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil, nil, ErrListenerClosed
	}
	for len(l.queue) > 0 {
		entry := l.queue[0]
		l.queue = l.queue[1:]
		entry.queued = false
		if entry.settled {
			continue
		}
		if watchClosed(entry.done) {
			entry.settled = true
			delete(l.active, entry.ref.Physical)
			continue
		}
		return entry, nil, nil
	}
	if err := l.lostLocked(); err != nil {
		return nil, nil, err
	}
	return nil, l.change, nil
}

// deliver confirms one admitted stream towards the peer and builds its typed
// session connection. It reports nil when the stream can no longer be
// delivered - its absolute admission deadline elapsed before the watchdog ran,
// it was reset by the peer, the listener was closed, or the physical connection
// reached its terminal outcome between admission and acceptance - so the caller
// moves on to the next stream instead of surfacing a fatal Accept error. An
// entry it finds expired is settled on its own here, without waiting for the
// watchdog to observe the same deadline.
func (l *Listener) deliver(entry *admittedStream) ports.ServerConnection {
	if ready, abortText := l.claimForDelivery(entry); !ready {
		if abortText != "" {
			l.abort(entry.ref.Physical, abortText)
		}
		return nil
	}
	if err := l.pump.Engine().Opened(Opened{Ref: entry.ref}); err != nil {
		l.finishStream(entry.ref.Physical)
		return nil
	}
	connection := newListenerConnection(l, entry)
	if err := l.pump.Send(Opened{Ref: entry.ref}); err != nil {
		// The peer can never be confirmed: settle this stream alone so its
		// carriage is released instead of leaking a half-delivered stream.
		_ = connection.Close()
		return nil
	}
	return connection
}

// claimForDelivery classifies one popped admission under the listener lock. It
// returns true when the stream may still be delivered. A stream the watchdog
// already settled is simply reported as not ready; a stream whose absolute
// admission deadline elapsed before the watchdog observed it, or whose listener
// was closed after the stream left the queue, is retired from the watchdog here
// and reported with the bounded Reset text its caller must signal the peer
// with, so the admission slot is released even when the watchdog never ran.
func (l *Listener) claimForDelivery(entry *admittedStream) (ready bool, abortText string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if entry.settled {
		return false, ""
	}
	switch {
	case l.closed:
		l.retireLocked(entry)
		return false, listenerClosedText
	case !entry.deadline.After(l.clock.Now()):
		l.retireLocked(entry)
		return false, handshakeDeadlineText
	default:
		return true, ""
	}
}

// retireLocked marks one popped admission settled and removes it from the
// deadline watchdog. The caller holds the listener lock.
func (l *Listener) retireLocked(entry *admittedStream) {
	entry.settled = true
	delete(l.active, entry.ref.Physical)
	l.signalLocked()
}

// finishStream retires one stream from the deadline watchdog: its session
// handshake completed, its connection was closed, or it reached a terminal
// outcome. A retired stream is never expired afterwards, so a completed
// handshake never triggers a late stream-local reset.
func (l *Listener) finishStream(physical PhysicalStreamID) {
	if l == nil || physical.Validate() != nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	entry, ok := l.active[physical]
	if !ok {
		return
	}
	entry.settled = true
	delete(l.active, physical)
	l.signalLocked()
}

// abort settles one locally refused, expired, or malformed stream and
// best-effort signals the peer with exactly one mux Reset. Only the offending
// stream is touched: siblings and the physical connection keep flowing. An
// identity the engine never admitted, and an identity that already reached its
// terminal outcome, are left alone by the engine itself.
func (l *Listener) abort(physical PhysicalStreamID, text string) {
	if physical.Validate() != nil {
		return
	}
	reset := Reset{
		Physical: physical,
		Error: ErrorDetail{
			Code:        ports.BrokerErrorUnavailable,
			Text:        text,
			FailureKind: domain.RemoteFailureTransport,
		},
		HasError: true,
	}
	_, _ = l.pump.Engine().Reset(reset)
	_ = l.pump.Send(reset)
}

// lostLocked returns the stable fatal Accept outcome once the physical
// connection reached its terminal outcome, or nil while it is still live. The
// caller holds the listener lock.
func (l *Listener) lostLocked() error {
	select {
	case <-l.pump.Done():
		if err := l.pump.Err(); err != nil {
			return fmt.Errorf("%w: %w", ErrListenerLost, err)
		}
		return ErrListenerLost
	default:
		return nil
	}
}

// signalLocked wakes every blocked Accept and the deadline watchdog. The
// broadcast channel is closed and replaced, so no waiter can miss the change.
// The caller holds the listener lock.
func (l *Listener) signalLocked() {
	close(l.change)
	l.change = make(chan struct{})
}

// watchLoop is the deadline watchdog. It never polls: it waits for the earliest
// active stream deadline, expires every stream that reached it, and re-scans
// whenever the registry changed. It ends when the listener is closed or the
// physical connection reached its terminal outcome.
func (l *Listener) watchLoop() {
	defer close(l.watchDone)
	for {
		deadline, change, ok := l.watchSnapshot()
		var timer ports.Timer
		var fire <-chan time.Time
		if ok {
			wait := deadline.Sub(l.clock.Now())
			if wait < 0 {
				wait = 0
			}
			timer = l.clock.NewTimer(wait)
			fire = timer.C()
		}
		select {
		case <-fire:
			l.expireDue()
		case <-change:
		case <-l.done:
			stopTimer(timer)
			return
		case <-l.pump.Done():
			stopTimer(timer)
			return
		}
		stopTimer(timer)
	}
}

// watchSnapshot returns the earliest tracked deadline and the broadcast channel
// to wait on atomically, so a state change published between the scan and the
// wait is never missed: the captured channel is already closed when it does.
func (l *Listener) watchSnapshot() (time.Time, <-chan struct{}, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	earliest, found := l.earliestDeadlineLocked()
	return earliest, l.change, found
}

// earliestDeadlineLocked reports the earliest tracked deadline. The caller
// holds the listener lock.
func (l *Listener) earliestDeadlineLocked() (time.Time, bool) {
	var earliest time.Time
	found := false
	for _, entry := range l.active {
		if entry.settled {
			continue
		}
		if !found || entry.deadline.Before(earliest) {
			earliest = entry.deadline
			found = true
		}
	}
	return earliest, found
}

// expireDue resets every tracked stream whose absolute admission deadline
// elapsed. Each reset is stream-local: the stream's admission slot is released
// and its delivered connection observes its own terminal outcome, while every
// sibling and the physical connection stay untouched. A stream that already
// reached a terminal outcome is retired without a second reset.
func (l *Listener) expireDue() {
	now := l.clock.Now()
	l.mu.Lock()
	due := make([]*admittedStream, 0, len(l.active))
	for id, entry := range l.active {
		if entry.settled || entry.deadline.After(now) {
			continue
		}
		entry.settled = true
		delete(l.active, id)
		if entry.queued {
			l.dropLocked(entry)
		}
		due = append(due, entry)
	}
	if len(due) > 0 {
		l.signalLocked()
	}
	l.mu.Unlock()
	for _, entry := range due {
		if watchClosed(entry.done) {
			continue
		}
		l.abort(entry.ref.Physical, handshakeDeadlineText)
	}
}

// dropLocked removes one queued stream from the FIFO, so Accept never hands out
// a stream the watchdog already expired. The caller holds the listener lock.
func (l *Listener) dropLocked(target *admittedStream) {
	for i, entry := range l.queue {
		if entry == target {
			l.queue = append(l.queue[:i], l.queue[i+1:]...)
			return
		}
	}
}

// stopTimer releases a deadline timer, tolerating a loop that never armed one.
func stopTimer(timer ports.Timer) {
	if timer != nil {
		timer.Stop()
	}
}

// ListenerConnection is one typed daemon-side logical stream accepted from a
// Listener: the sessionwire server connection over the mux byte stream, plus
// the stream lifecycle the daemon owns. It implements ports.ServerConnection
// and exposes the narrow optional HandshakePlumbing seam - the absolute
// handshake deadline fixed at admission and the completion signal - without
// restarting the deadline. Done, Err, and FailureKind publish the stream's
// terminal outcome independently of reads. It is safe for concurrent use.
type ListenerConnection struct {
	ports.ServerConnection

	listener *Listener
	pump     *Pump
	ref      StreamRef
	pipe     *muxStreamPipe
	raw      *muxTransport
	stream   <-chan struct{}
	// cause is the stream's retained publish-once terminal authority, captured
	// at acceptance. It outlives the engine's bounded record, so a settled
	// stream keeps its exact cause even after the record is evicted.
	cause *terminalState

	// plumbing is the wrapped session connection's handshake seam, resolved
	// once at construction; nil when the wrapped connection lacks it.
	plumbing HandshakePlumbing
	// deadline is the exact absolute handshake deadline fixed at Open
	// admission, resolved once at construction.
	deadline time.Time

	// admission is the cloned provisioned admission stamped at admission. It is
	// resolved once at construction and exposed only through a defensive copy.
	admission ports.SessionAdmission

	terminal  *terminalState
	watchDone chan struct{}
	closeOnce sync.Once
	closeErr  error
}

// newListenerConnection builds the typed session channel over one confirmed
// open stream and starts the terminal watcher. It never blocks. The session
// connection adopts the stream's admission deadline, so accepting a stream late
// consumes that budget instead of starting a second one, and the listener
// retires its watchdog exactly when the session handshake completes.
func newListenerConnection(l *Listener, entry *admittedStream) *ListenerConnection {
	limit := l.pump.Ceilings().StreamChunkLimit
	pipe := newMuxStreamPipe(l.pump, entry.ref.Physical, limit, entry.done)
	raw := newMuxTransport(pipe)
	connection := &ListenerConnection{
		ServerConnection: sessionwire.NewServerConnectionWithAdmission(raw, entry.deadline, entry.admission),
		listener:         l,
		pump:             l.pump,
		ref:              entry.ref,
		pipe:             pipe,
		raw:              raw,
		stream:           entry.done,
		cause:            l.pump.cause(entry.ref.Physical),
		deadline:         entry.deadline,
		admission:        entry.admission.Clone(),
		terminal:         newTerminalState(),
		watchDone:        make(chan struct{}),
	}
	if plumbing, ok := connection.ServerConnection.(HandshakePlumbing); ok {
		connection.plumbing = plumbing
		connection.deadline = plumbing.HandshakeDeadline()
	}
	connection.OnHandshakeComplete(func() { l.finishStream(entry.ref.Physical) })
	go connection.watch()
	return connection
}

// Ref returns the complete stream reference recorded at admission.
func (c *ListenerConnection) Ref() StreamRef { return c.ref }

// SessionAdmission returns a defensive copy of the provisioned admission
// stamped at acceptance, implementing ports.SessionAdmissionProvider. It always
// reports ok=true: a ListenerConnection is built only from an already-admitted
// stream whose provenance a listener stamped and validated.
func (c *ListenerConnection) SessionAdmission() (ports.SessionAdmission, bool) {
	return c.admission.Clone(), true
}

// HandshakeDeadline returns the accepted absolute local deadline of the
// session handshake, fixed at Open admission and plumbing the exact deadline
// the wrapped session connection started with. Queue delay and the handshake
// spend this one budget; the value is stable for the connection's lifetime.
func (c *ListenerConnection) HandshakeDeadline() time.Time { return c.deadline }

// OnHandshakeComplete registers fn to run exactly once when the session
// handshake completes, plumbing the wrapped session connection's completion
// hook. Registering after completion runs fn immediately.
func (c *ListenerConnection) OnHandshakeComplete(fn func()) {
	if c.plumbing != nil {
		c.plumbing.OnHandshakeComplete(fn)
		return
	}
	if fn != nil {
		fn()
	}
}

// Done returns the logical terminal channel independent of reads. It closes
// exactly once, before Err becomes stable.
func (c *ListenerConnection) Done() <-chan struct{} { return c.terminal.Done() }

// Err returns the stable terminal cause: nil for an orderly local or peer
// Close, the retained stream cause for a peer Reset, a queue overflow, or a
// watchdog expiry, and the pump's cause for a physical loss.
func (c *ListenerConnection) Err() error { return c.terminal.Err() }

// FailureKind classifies Err. It is None while open and for an orderly Close.
func (c *ListenerConnection) FailureKind() domain.RemoteFailureKind { return c.terminal.FailureKind() }

// ReceiveClient receives one typed client message. An orderly end of the inner
// stream returns io.EOF; any other inner-stream error is treated as a malformed
// or failed session frame: exactly this stream is reset towards the peer, its
// terminal outcome is published, and the error is returned. Neither outcome
// touches a sibling stream or the physical connection.
func (c *ListenerConnection) ReceiveClient() (protocol.ClientMessage, error) {
	if c.settled() {
		return nil, c.terminalError()
	}
	message, err := c.ServerConnection.ReceiveClient()
	if err == nil {
		return message, nil
	}
	if errors.Is(err, io.EOF) {
		c.syncFromStream()
		return nil, c.terminalError()
	}
	return nil, c.abortMalformed(err)
}

// Close independently closes the logical stream: it discards the inbound
// chunks the engine already accepted, settles this stream locally, sends
// exactly one mux Close, publishes the orderly nil outcome, and unblocks every
// read and write. It never touches the physical connection or a sibling
// stream. Close is idempotent and concurrent-safe; every caller observes the
// same stored transport Close error.
func (c *ListenerConnection) Close() error {
	c.closeOnce.Do(func() {
		// Discard what the engine already accepted before closing locally: this
		// side stops reading as soon as it closes, so draining first is what lets
		// the engine's orderly Close reach its terminal state and release the
		// stream's admission slot and aggregate bytes instead of parking the
		// stream in closing with a queue nothing will ever drain.
		discardQueuedInbound(c.pump, c.ref.Physical)
		_, _ = c.pump.Engine().Close(Close{Physical: c.ref.Physical})
		// A chunk the reader accepted between the drain and the Close parked the
		// stream in closing instead of terminal; no new chunk is accepted after
		// the Close, so this drain terminalizes the stream and releases its slot
		// and bytes.
		discardQueuedInbound(c.pump, c.ref.Physical)
		_ = c.pump.Send(Close{Physical: c.ref.Physical})
		c.terminal.Close()
		c.closeErr = c.raw.Close()
		// The watcher retires the stream from the deadline watchdog and the
		// pump's watch table before it closes watchDone.
		<-c.watchDone
	})
	return c.closeErr
}

// watch publishes the logical terminal outcome when the stream settles on its
// own (peer Close, peer Reset, queue overflow, watchdog expiry, or physical
// loss) independently of reads, and unblocks the carriage. A local Close
// settles first and makes this a no-op.
func (c *ListenerConnection) watch() {
	defer c.pump.ReleaseWatch(c.ref.Physical)
	defer c.listener.finishStream(c.ref.Physical)
	defer close(c.watchDone)
	select {
	case <-c.stream:
		// The stream reached its terminal state. Publish the retained cause,
		// not a fresh engine lookup: the engine record may already be evicted.
		c.syncFromStream()
	case <-c.pump.Done():
		// A physical terminal outcome wakes every stream. A stream-local cause
		// recorded before the physical failure stays authoritative.
		if !c.settleFromCause() {
			if err := c.pump.Err(); err != nil {
				c.settleLoss(c.pump.FailureKind(), err)
			} else {
				c.terminal.Close()
			}
		}
	case <-c.pipe.Done():
		// The inner session carriage closed on its own. A sessionwire preamble
		// failure closes the raw transport, which closes the pipe, before any
		// consumer Close: settle the stream from the retained or physical cause
		// when one already exists, and otherwise issue exactly one stream-local
		// Reset so the admission slot and watchdog are released without a
		// consumer Close.
		if !c.settleFromCarriage() {
			_ = c.abortClosedCarriage()
		}
	}
	// Closing the framed transport also closes the byte stream and joins
	// streamframe's writer, so a self-terminating stream leaves no goroutine
	// behind.
	_ = c.raw.Close()
}

// syncFromStream settles this connection from the stream's retained terminal
// authority when it published, and otherwise from the pump's own terminal
// outcome. It closes the race between a blocked read observing the inner-stream
// end and the watcher publishing the same outcome, and it never lets a physical
// failure surface as a clean io.EOF: the retained authority outlives the
// bounded engine record, so an evicted stream still reports its exact cause.
func (c *ListenerConnection) syncFromStream() {
	if c.settled() {
		return
	}
	if c.settleFromCause() {
		return
	}
	if err := c.pump.Err(); err != nil {
		c.settleLoss(c.pump.FailureKind(), err)
		return
	}
	if status, ok := c.pump.Engine().Status(c.ref.Physical); ok && status.State == StreamTerminal {
		c.settleFromStatus(status)
		return
	}
	c.terminal.Close()
}

// settleFromCause publishes the terminal outcome recorded by the stream's
// retained publish-once authority. The authority is captured when the stream is
// accepted and stays valid for the connection's lifetime even after the engine
// evicts the bounded record, so a settled stream keeps its exact cause
// independent of retention: a Reset or an expiry never degrades into an orderly
// close. It reports whether the authority had published.
func (c *ListenerConnection) settleFromCause() bool {
	cause := c.cause
	if cause == nil {
		return false
	}
	select {
	case <-cause.Done():
	default:
		return false
	}
	if err := cause.Err(); err != nil {
		c.settleLoss(cause.FailureKind(), err)
	} else {
		c.terminal.Close()
	}
	return true
}

// settleFromStatus publishes the terminal outcome recorded by the engine.
func (c *ListenerConnection) settleFromStatus(status StreamStatus) {
	if status.Err == nil {
		c.terminal.Close()
		return
	}
	c.settleLoss(status.FailureKind, status.Err)
}

// settleFromCarriage publishes this stream's terminal outcome when its inner
// session carriage closed on its own while the stream was still live. It syncs
// the retained stream authority or the pump's physical cause first, and reports
// whether one of them published. It deliberately does not fall back to an
// orderly close, because a carriage that closed without a recorded cause is a
// failed handshake, not an orderly end; the caller resets the stream instead.
func (c *ListenerConnection) settleFromCarriage() bool {
	if c.settled() {
		return true
	}
	if c.settleFromCause() {
		return true
	}
	if err := c.pump.Err(); err != nil {
		c.settleLoss(c.pump.FailureKind(), err)
		return true
	}
	if status, ok := c.pump.Engine().Status(c.ref.Physical); ok && status.State == StreamTerminal {
		c.settleFromStatus(status)
		return true
	}
	return false
}

// abortMalformed resets exactly this stream after a malformed or failed inner
// session frame: it settles the local engine record, best-effort signals the
// peer with one Reset, publishes the stream-local failure, unblocks the byte
// stream, and returns the stable terminal error.
func (c *ListenerConnection) abortMalformed(cause error) error {
	return c.resetStream(malformedResetText, domain.RemoteFailureInvalidResponse, cause)
}

// abortClosedCarriage resets exactly this stream when its inner session
// carriage closed on its own during the handshake with no stream cause recorded
// and no consumer Close. It issues one stream-local Reset so the peer learns the
// stream is gone, publishes the stream-local failure, and returns the stable
// terminal error, so the watchdog and the admission slot are released without
// waiting for a read or a Close.
func (c *ListenerConnection) abortClosedCarriage() error {
	return c.resetStream(sessionCarriageClosedText, domain.RemoteFailureInvalidResponse, errSessionCarriageClosed)
}

// resetStream settles this stream with exactly one stream-local Reset. It claims
// the publish-once terminal outcome before signalling the peer, so a consumer
// read and the terminal watcher that observe the same failure never send a
// second Reset for one stream. The engine record is settled and the carriage is
// unblocked. It returns the caller's own cause - the specific read or decode
// error - so a consumer that lost the publish race to the terminal watcher still
// reports its precise diagnostic; the stable terminal error is its fallback.
func (c *ListenerConnection) resetStream(text string, kind domain.RemoteFailureKind, cause error) error {
	if c.settled() || !c.settleLoss(kind, cause) {
		// Another publisher already claimed this stream's outcome and issued its
		// one Reset; never signal the peer twice.
		if cause != nil {
			return cause
		}
		return c.terminalError()
	}
	reset := Reset{
		Physical: c.ref.Physical,
		Error: ErrorDetail{
			Code:        ports.BrokerErrorUnavailable,
			Text:        text,
			FailureKind: kind,
		},
		HasError: true,
	}
	_, _ = c.pump.Engine().Reset(reset)
	_ = c.pump.Send(reset)
	_ = c.raw.Close()
	if cause != nil {
		return cause
	}
	return c.terminalError()
}

// settleLoss publishes one stream-local loss carrying the given diagnostic
// cause, normalized to the transport-failure taxonomy. It reports whether this
// call won the publish-once outcome, so a caller that must signal the peer
// exactly once knows it is the one publisher.
func (c *ListenerConnection) settleLoss(kind domain.RemoteFailureKind, cause error) bool {
	if kind < domain.RemoteFailureTransport || kind > domain.RemoteFailureInvalidResponse {
		kind = domain.RemoteFailureTransport
	}
	return c.terminal.publishFail(kind, cause)
}

// settled reports whether the logical connection already reached its terminal
// outcome.
func (c *ListenerConnection) settled() bool {
	select {
	case <-c.terminal.Done():
		return true
	default:
		return false
	}
}

// terminalError is the stable error of a settled connection: the published
// cause, or io.EOF for an orderly close.
func (c *ListenerConnection) terminalError() error {
	if err := c.terminal.Err(); err != nil {
		return err
	}
	return io.EOF
}
