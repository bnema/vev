// Per-connection broker state.
//
// Connection walks one exact lifecycle: New is entered when the listener
// assigns the connection scope, one locally validated preamble moves it to
// AwaitRegister, the first Register moves it to Ready, and shutdown runs
// Closing then Closed. Every later frame must carry the scope assigned at
// accept: a mismatched epoch or connection is rejected before it can touch
// connection state.
//
// The connection owns one subscription generation series, one snapshot
// assembler per active generation, one bounded operation tracker, and one
// bounded stream tracker. Subscription generations advance strictly: a
// resubscribe must carry a newer generation, while a resync or unsubscribe
// must carry the active one. A generation advance discards in-flight
// snapshot staging; the committed snapshot survives until a newer transfer
// commits. Operation and stream state is scoped to exactly this connection
// and epoch.
//
// There is no I/O, socket, pump, or transport here.
package brokerwire

import (
	"errors"
	"fmt"
	"sync"

	"github.com/bnema/vev/internal/ports"
)

// Per-connection bounds.
const (
	// MaxPendingOperations bounds admitted, unfinished mutating operations
	// per connection.
	MaxPendingOperations = 64
	// MaxTrackedOperations bounds the completed-operation dedup set.
	MaxTrackedOperations = 4096
	// operationIDBytes is the encoded size of one operation identity.
	operationIDBytes = 16
	// MaxOperationDedupBytes bounds the completed-operation dedup memory:
	// MaxTrackedOperations identities of 16 bytes each (64 KiB).
	MaxOperationDedupBytes = MaxTrackedOperations * operationIDBytes
	// MaxStreams bounds concurrently opening or open logical streams per
	// connection.
	MaxStreams = 128
	// MaxRetiredStreams bounds retired stream records retained per
	// connection so a late frame can be classified and discarded instead of
	// mistaken for a future stream.
	MaxRetiredStreams = 128
)

var (
	// ErrInvalidScope reports an incomplete broker scope: a zero epoch or a
	// zero connection identity.
	ErrInvalidScope = errors.New("brokerwire: invalid broker scope")
	// ErrConnectionState reports an operation that is not legal in the
	// connection's current lifecycle state.
	ErrConnectionState = errors.New("brokerwire: invalid connection state")
	// ErrConnectionClosed reports work presented to a closing or closed
	// connection.
	ErrConnectionClosed = errors.New("brokerwire: connection is closed")
	// ErrDuplicateRegister reports a second Register on one connection.
	ErrDuplicateRegister = errors.New("brokerwire: duplicate register")
	// ErrNotSubscribed reports a snapshot part or resync without an active
	// subscription.
	ErrNotSubscribed = errors.New("brokerwire: connection has no active subscription")
	// ErrInvalidOperation reports a zero operation identity.
	ErrInvalidOperation = errors.New("brokerwire: invalid broker operation")
	// ErrInvalidOutcome reports an outcome outside the closed mutation
	// taxonomy.
	ErrInvalidOutcome = errors.New("brokerwire: invalid broker mutation outcome")
	// ErrOperationPending reports a duplicate admission of an in-flight
	// operation.
	ErrOperationPending = errors.New("brokerwire: operation is already pending")
	// ErrOperationCompleted reports an operation that already has a recorded
	// outcome. It is deduped and never replayed.
	ErrOperationCompleted = errors.New("brokerwire: operation already has an outcome")
	// ErrTooManyPendingOperations reports a full pending set.
	ErrTooManyPendingOperations = errors.New("brokerwire: too many pending operations")
	// ErrUnknownOperation reports a completion for an operation that was
	// never admitted, including one whose dedup record was evicted.
	ErrUnknownOperation = errors.New("brokerwire: unknown operation")
	// ErrInvalidStream reports a zero stream identity.
	ErrInvalidStream = errors.New("brokerwire: invalid broker stream")
	// ErrStreamIDReused reports a stream identity that is already consumed or
	// evicted outside the bounded anti-replay window. Stream identities are
	// allocated by the calling service and are never reused.
	ErrStreamIDReused = errors.New("brokerwire: stream ID is not fresh")
	// ErrFutureStream reports a frame for a stream identity that was never
	// allocated.
	ErrFutureStream = errors.New("brokerwire: stream was never opened")
	// ErrTooManyStreams reports the per-connection concurrent stream
	// ceiling.
	ErrTooManyStreams = errors.New("brokerwire: too many open streams")
	// ErrStreamState reports an event that is illegal in the stream's
	// current state.
	ErrStreamState = errors.New("brokerwire: invalid stream state")
)

// Scope is one exact broker connection scope: the broker epoch plus the
// connection identity assigned at accept. Every frame carries it, and a
// mismatched scope is rejected rather than applied.
type Scope struct {
	Epoch      ports.BrokerEpoch
	Connection ports.BrokerConnectionID
}

// Validate rejects an incomplete scope.
func (s Scope) Validate() error {
	if s.Epoch == 0 {
		return fmt.Errorf("%w: missing epoch", ErrInvalidScope)
	}
	if err := s.Connection.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidScope, err)
	}
	return nil
}

// ConnState is the closed broker connection lifecycle.
type ConnState uint8

const (
	// ConnNew is the accepted connection state: only a preamble is legal.
	ConnNew ConnState = iota
	// ConnAwaitRegister waits for exactly one Register after the preamble.
	ConnAwaitRegister
	// ConnReady accepts subscription, snapshot, operation, and stream work.
	ConnReady
	// ConnClosing has stopped accepting new work and discarded in-flight
	// snapshot staging.
	ConnClosing
	// ConnClosed has released all connection state.
	ConnClosed
)

func (s ConnState) String() string {
	switch s {
	case ConnNew:
		return "new"
	case ConnAwaitRegister:
		return "await_register"
	case ConnReady:
		return "ready"
	case ConnClosing:
		return "closing"
	case ConnClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// Connection is the per-connection broker state machine: handshake
// progression, one subscription generation series, one snapshot assembler
// per active generation, and the bounded operation and stream trackers for
// this exact scope. It is safe for concurrent use.
type Connection struct {
	mu         sync.Mutex
	scope      Scope
	state      ConnState
	operations *OperationTracker
	streams    *StreamTracker

	subscribed bool
	generation SubscriptionGeneration
	highest    SubscriptionGeneration
	assembler  *SnapshotAssembler
}

// NewConnection binds one connection to the exact scope assigned at accept.
func NewConnection(scope Scope) (*Connection, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	operations, err := newOperationTracker(scope)
	if err != nil {
		return nil, err
	}
	streams, err := newStreamTracker(scope)
	if err != nil {
		return nil, err
	}
	return &Connection{scope: scope, state: ConnNew, operations: operations, streams: streams}, nil
}

// State returns the current lifecycle state.
func (c *Connection) State() ConnState {
	if c == nil {
		return ConnClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

// Scope returns the exact scope assigned at accept.
func (c *Connection) Scope() Scope {
	if c == nil {
		return Scope{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.scope
}

// Ready reports whether the handshake completed.
func (c *Connection) Ready() bool { return c.State() == ConnReady }

// SignalPreamble records one locally validated preamble. It is legal exactly
// once, in New, and moves the connection to AwaitRegister.
func (c *Connection) SignalPreamble() error {
	if c == nil {
		return ErrConnectionClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state != ConnNew {
		return c.stateErrorLocked()
	}
	c.state = ConnAwaitRegister
	return nil
}

// Register accepts the first Register and moves to Ready. A second Register,
// or one before the preamble, is refused.
func (c *Connection) Register() error {
	if c == nil {
		return ErrConnectionClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	switch c.state {
	case ConnAwaitRegister:
		c.state = ConnReady
		return nil
	case ConnReady:
		return ErrDuplicateRegister
	default:
		return c.stateErrorLocked()
	}
}

// BeginClose stops accepting new work and discards in-flight snapshot
// staging. The committed snapshot survives until the connection is finally
// closed. It is idempotent.
func (c *Connection) BeginClose() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	switch c.state {
	case ConnClosing, ConnClosed:
		return nil
	}
	c.state = ConnClosing
	c.subscribed = false
	c.generation = 0
	if c.assembler != nil {
		c.assembler.DiscardStaging()
	}
	return nil
}

// FinishClose completes shutdown and releases operation and stream state.
// Only a closing connection may close; a second call is a no-op.
func (c *Connection) FinishClose() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	switch c.state {
	case ConnClosed:
		return nil
	case ConnClosing:
		c.state = ConnClosed
		c.assembler = nil
		c.operations.reset()
		c.streams.reset()
		return nil
	default:
		return ErrConnectionState
	}
}

// Subscribe starts one subscription generation series. The generation must be
// nonzero and strictly above every generation this connection has used, so a
// superseded series can never be revived. A new generation discards any
// in-flight staging and keeps publishing the committed snapshot until a newer
// transfer commits.
func (c *Connection) Subscribe(generation uint64) error {
	if c == nil {
		return ErrConnectionClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.requireReadyLocked(); err != nil {
		return err
	}
	if generation == 0 {
		return ErrInvalidGeneration
	}
	if generation <= c.highest {
		return ErrStaleGeneration
	}
	var committed ports.BrokerSnapshot
	var hasCommitted bool
	if c.assembler != nil {
		committed, hasCommitted = c.assembler.Snapshot()
	}
	next := NewSnapshotAssembler(c.scope.Epoch, c.scope.Connection, WithGeneration(generation))
	if hasCommitted {
		next.adoptCommitted(committed)
	}
	c.subscribed = true
	c.generation = generation
	c.highest = generation
	c.assembler = next
	return nil
}

// Resync restarts the active generation's publication: it must carry the
// active generation exactly, and it discards any in-flight staging so a full
// transfer can restart at index 0.
func (c *Connection) Resync(generation uint64) error {
	if c == nil {
		return ErrConnectionClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.requireReadyLocked(); err != nil {
		return err
	}
	if !c.subscribed {
		return ErrNotSubscribed
	}
	if generation != c.generation {
		return generationFence(generation, c.generation)
	}
	c.assembler.DiscardStaging()
	return nil
}

// Unsubscribe ends the active generation. It must carry the active generation
// exactly; the generation remains consumed and is never reused. The committed
// snapshot survives until the connection is finally closed.
func (c *Connection) Unsubscribe(generation uint64) error {
	if c == nil {
		return ErrConnectionClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.requireReadyLocked(); err != nil {
		return err
	}
	if !c.subscribed {
		return ErrNotSubscribed
	}
	if generation != c.generation {
		return generationFence(generation, c.generation)
	}
	c.subscribed = false
	c.generation = 0
	if c.assembler != nil {
		c.assembler.DiscardStaging()
	}
	return nil
}

// AddSnapshotPart stages one part of the active generation's publication. The
// committed snapshot changes only when End validates. The ready check, the
// subscription check, and the assembler mutation share one connection lock, so
// a concurrent close can never commit a part after it discarded staging.
func (c *Connection) AddSnapshotPart(part SnapshotPart) (bool, ports.BrokerSnapshot, error) {
	if c == nil {
		return false, ports.BrokerSnapshot{}, ErrConnectionClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.requireReadyLocked(); err != nil {
		return false, ports.BrokerSnapshot{}, err
	}
	if !c.subscribed || c.assembler == nil {
		return false, ports.BrokerSnapshot{}, ErrNotSubscribed
	}
	return c.assembler.Add(part)
}

// Snapshot returns the last committed snapshot and whether one exists.
func (c *Connection) Snapshot() (ports.BrokerSnapshot, bool) {
	if c == nil {
		return ports.BrokerSnapshot{}, false
	}
	c.mu.Lock()
	assembler := c.assembler
	c.mu.Unlock()
	if assembler == nil {
		return ports.BrokerSnapshot{}, false
	}
	return assembler.Snapshot()
}

// StagingActive reports whether a snapshot transfer is in flight for the
// active subscription generation.
func (c *Connection) StagingActive() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	assembler := c.assembler
	c.mu.Unlock()
	if assembler == nil {
		return false
	}
	return assembler.StagingActive()
}

// AdmitOperation records one in-flight mutating operation for this scope.
// The ready check and the tracker mutation share one connection lock, so a
// concurrent close can never admit work after shutdown.
func (c *Connection) AdmitOperation(operation ports.BrokerOperationID) error {
	if c == nil {
		return ErrConnectionClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.requireReadyLocked(); err != nil {
		return err
	}
	return c.operations.Admit(c.scope, operation)
}

// CompleteOperation records exactly one outcome for an in-flight operation.
// The ready check and the tracker mutation share one connection lock.
func (c *Connection) CompleteOperation(operation ports.BrokerOperationID, outcome ports.BrokerMutationOutcome) error {
	if c == nil {
		return ErrConnectionClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.requireReadyLocked(); err != nil {
		return err
	}
	return c.operations.Complete(c.scope, operation, outcome)
}

// OpenStream allocates one fresh logical stream in this scope. The ready
// check and the tracker mutation share one connection lock.
func (c *Connection) OpenStream(stream ports.BrokerStreamID) error {
	if c == nil {
		return ErrConnectionClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.requireReadyLocked(); err != nil {
		return err
	}
	return c.streams.Open(c.scope, stream)
}

// StreamOpened confirms one logical stream the broker established. The
// ready check and the tracker mutation share one connection lock.
func (c *Connection) StreamOpened(stream ports.BrokerStreamID) error {
	if c == nil {
		return ErrConnectionClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.requireReadyLocked(); err != nil {
		return err
	}
	return c.streams.Opened(c.scope, stream)
}

// StreamData delivers one opaque daemon frame to an open stream. The ready
// check and the tracker mutation share one connection lock.
func (c *Connection) StreamData(stream ports.BrokerStreamID) (StreamDisposition, error) {
	if c == nil {
		return 0, ErrConnectionClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.requireReadyLocked(); err != nil {
		return 0, err
	}
	return c.streams.Data(c.scope, stream)
}

// StreamProgress reports one phase of an in-flight stream operation. The
// ready check and the tracker mutation share one connection lock.
func (c *Connection) StreamProgress(stream ports.BrokerStreamID) (StreamDisposition, error) {
	if c == nil {
		return 0, ErrConnectionClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.requireReadyLocked(); err != nil {
		return 0, err
	}
	return c.streams.Progress(c.scope, stream)
}

// CloseStream ends one logical stream. The ready check and the tracker
// mutation share one connection lock.
func (c *Connection) CloseStream(stream ports.BrokerStreamID) (StreamDisposition, error) {
	if c == nil {
		return 0, ErrConnectionClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.requireReadyLocked(); err != nil {
		return 0, err
	}
	return c.streams.Close(c.scope, stream)
}

func (c *Connection) requireReadyLocked() error {
	switch c.state {
	case ConnReady:
		return nil
	case ConnClosing, ConnClosed:
		return ErrConnectionClosed
	default:
		return ErrConnectionState
	}
}

func (c *Connection) stateErrorLocked() error {
	if c.state == ConnClosing || c.state == ConnClosed {
		return ErrConnectionClosed
	}
	return ErrConnectionState
}

// generationFence classifies a generation that is not the active one.
func generationFence(got, active uint64) error {
	if got < active {
		return ErrStaleGeneration
	}
	return ErrFutureGeneration
}

// OperationTracker bounds the mutating operations admitted on one
// connection scope. At most MaxPendingOperations operations are in flight at
// once. Completed operations keep only their identity and outcome, bounded
// to MaxTrackedOperations identities (MaxOperationDedupBytes), so a repeated
// operation is deduped rather than re-executed inside that window.
// Outcome-unknown is recorded like any other outcome: the client refreshes
// authoritative state and never blindly replays a non-idempotent operation.
// It is safe for concurrent use.
type OperationTracker struct {
	mu      sync.Mutex
	scope   Scope
	pending map[ports.BrokerOperationID]struct{}
	results map[ports.BrokerOperationID]ports.BrokerMutationOutcome
	order   []ports.BrokerOperationID
}

// newOperationTracker binds a tracker to one exact connection scope. It is
// unexported on purpose: the trackers are connection-owned state, and every
// production mutation goes through the Connection methods that hold the
// connection lock across the ready check and the tracker mutation.
func newOperationTracker(scope Scope) (*OperationTracker, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	return &OperationTracker{
		scope:   scope,
		pending: make(map[ports.BrokerOperationID]struct{}, MaxPendingOperations),
		results: make(map[ports.BrokerOperationID]ports.BrokerMutationOutcome, MaxTrackedOperations),
	}, nil
}

// Admit registers one mutating operation as in flight. A mismatched scope, a
// zero identity, a duplicate in-flight operation, an operation that already
// has a recorded outcome, and a full pending set are all refused.
func (t *OperationTracker) Admit(scope Scope, operation ports.BrokerOperationID) error {
	if t == nil {
		return ErrConnectionClosed
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if scope != t.scope {
		return ErrScopeMismatch
	}
	if operation.IsZero() {
		return ErrInvalidOperation
	}
	if _, done := t.results[operation]; done {
		return ErrOperationCompleted
	}
	if _, pending := t.pending[operation]; pending {
		return ErrOperationPending
	}
	if len(t.pending) >= MaxPendingOperations {
		return ErrTooManyPendingOperations
	}
	t.pending[operation] = struct{}{}
	return nil
}

// Complete records exactly one outcome for an in-flight operation. The
// identity moves into the bounded dedup set, evicting the oldest record when
// full, so a repeat of a recently completed operation is deduped instead of
// replayed.
func (t *OperationTracker) Complete(scope Scope, operation ports.BrokerOperationID, outcome ports.BrokerMutationOutcome) error {
	if t == nil {
		return ErrConnectionClosed
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if scope != t.scope {
		return ErrScopeMismatch
	}
	if err := outcome.Validate(); err != nil {
		return ErrInvalidOutcome
	}
	if _, pending := t.pending[operation]; !pending {
		if _, done := t.results[operation]; done {
			return ErrOperationCompleted
		}
		return ErrUnknownOperation
	}
	delete(t.pending, operation)
	t.results[operation] = outcome
	t.order = append(t.order, operation)
	for len(t.order) > MaxTrackedOperations {
		evicted := t.order[0]
		t.order = t.order[1:]
		delete(t.results, evicted)
	}
	return nil
}

// Pending returns the number of in-flight operations.
func (t *OperationTracker) Pending() int {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.pending)
}

// Tracked returns the number of dedup records retained.
func (t *OperationTracker) Tracked() int {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.results)
}

// DedupBytes returns the dedup memory in bytes: one fixed-size identity per
// retained record. It never exceeds MaxOperationDedupBytes.
func (t *OperationTracker) DedupBytes() int {
	return t.Tracked() * operationIDBytes
}

// Outcome returns the recorded outcome for one operation.
func (t *OperationTracker) Outcome(operation ports.BrokerOperationID) (ports.BrokerMutationOutcome, bool) {
	if t == nil {
		return 0, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	outcome, ok := t.results[operation]
	return outcome, ok
}

func (t *OperationTracker) reset() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pending = make(map[ports.BrokerOperationID]struct{}, MaxPendingOperations)
	t.results = make(map[ports.BrokerOperationID]ports.BrokerMutationOutcome, MaxTrackedOperations)
	t.order = nil
}

// StreamState is the closed lifecycle of one logical stream.
type StreamState uint8

const (
	// StreamOpening is an allocated stream the broker has not established.
	StreamOpening StreamState = iota + 1
	// StreamOpen carries data and progress.
	StreamOpen
	// StreamRetired is terminal: no further frame is applied.
	StreamRetired
)

func (s StreamState) String() string {
	switch s {
	case StreamOpening:
		return "opening"
	case StreamOpen:
		return "open"
	case StreamRetired:
		return "retired"
	default:
		return "unknown"
	}
}

// StreamDisposition classifies one stream frame: applied, or a late frame for
// a retired stream that is discarded without effect.
type StreamDisposition uint8

const (
	// StreamAccepted reports the frame was applied.
	StreamAccepted StreamDisposition = iota + 1
	// StreamDiscarded reports a late frame for a retired stream. It is
	// ignored and never revives, reopens, or closes anything.
	StreamDiscarded
)

// StreamTracker bounds the logical streams of one connection scope. Stream
// identities are allocated by the calling service and may arrive slightly out
// of order when two opens race, so admission uses one bounded anti-replay
// window instead of a bare high-water mark: every unconsumed identity inside
// the window is admitted exactly once, a duplicate is refused, and an identity
// that trails the newest admitted identity by the window size is stale. At most
// MaxStreams streams are opening or open at once; at most MaxRetiredStreams
// retired records are retained, so an older late frame is still classified as
// retired rather than mistaken for a future stream. It is safe for concurrent
// use.
type StreamTracker struct {
	mu      sync.Mutex
	scope   Scope
	states  map[ports.BrokerStreamID]StreamState
	retired []ports.BrokerStreamID
	highest ports.BrokerStreamID
	window  ports.BrokerStreamWindow
	active  int
}

// newStreamTracker binds a tracker to one exact connection scope. It is
// unexported on purpose: the trackers are connection-owned state, and every
// production mutation goes through the Connection methods that hold the
// connection lock across the ready check and the tracker mutation.
func newStreamTracker(scope Scope) (*StreamTracker, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	return &StreamTracker{scope: scope, states: make(map[ports.BrokerStreamID]StreamState, MaxStreams)}, nil
}

// Open allocates one fresh logical stream in Opening state. The identity is
// admitted on this connection's bounded anti-replay window: it must be
// unconsumed and inside the window, and it must not already be in use. An
// identity allocated by a concurrent open is therefore admitted even when it
// arrives just after a higher one, while a replay and an identity evicted past
// the window are both refused.
func (t *StreamTracker) Open(scope Scope, stream ports.BrokerStreamID) error {
	if t == nil {
		return ErrConnectionClosed
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if scope != t.scope {
		return ErrScopeMismatch
	}
	if err := stream.Validate(); err != nil {
		return ErrInvalidStream
	}
	if err := t.window.Admit(stream); err != nil {
		// A duplicate or an identity evicted from the window is stale rather
		// than malformed, so the caller can classify it as a never-reused
		// identity instead of a protocol violation.
		return ErrStreamIDReused
	}
	if t.active >= MaxStreams {
		return ErrTooManyStreams
	}
	t.states[stream] = StreamOpening
	if stream > t.highest {
		t.highest = stream
	}
	t.active++
	return nil
}

// Opened moves one opening stream to Open. Every other state is illegal:
// opening is only ever confirmed once.
func (t *StreamTracker) Opened(scope Scope, stream ports.BrokerStreamID) error {
	if t == nil {
		return ErrConnectionClosed
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	state, err := t.lookupLocked(scope, stream)
	if err != nil {
		return err
	}
	if state != StreamOpening {
		return ErrStreamState
	}
	t.states[stream] = StreamOpen
	return nil
}

// Data delivers one opaque daemon frame. Only an open stream accepts data; a
// retired stream discards it, and a stream that was never allocated is
// refused.
func (t *StreamTracker) Data(scope Scope, stream ports.BrokerStreamID) (StreamDisposition, error) {
	if t == nil {
		return 0, ErrConnectionClosed
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	state, err := t.lookupLocked(scope, stream)
	if err != nil {
		return 0, err
	}
	switch state {
	case StreamOpen:
		return StreamAccepted, nil
	case StreamRetired:
		return StreamDiscarded, nil
	default:
		return 0, ErrStreamState
	}
}

// Progress reports one phase of an in-flight stream operation. It is legal
// while opening and while open; a retired stream discards it.
func (t *StreamTracker) Progress(scope Scope, stream ports.BrokerStreamID) (StreamDisposition, error) {
	if t == nil {
		return 0, ErrConnectionClosed
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	state, err := t.lookupLocked(scope, stream)
	if err != nil {
		return 0, err
	}
	if state == StreamRetired {
		return StreamDiscarded, nil
	}
	return StreamAccepted, nil
}

// Close moves one opening or open stream to its terminal retired state. A
// late close for a retired stream is discarded.
func (t *StreamTracker) Close(scope Scope, stream ports.BrokerStreamID) (StreamDisposition, error) {
	if t == nil {
		return 0, ErrConnectionClosed
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	state, err := t.lookupLocked(scope, stream)
	if err != nil {
		return 0, err
	}
	if state == StreamRetired {
		return StreamDiscarded, nil
	}
	t.retireLocked(stream)
	return StreamAccepted, nil
}

// Active returns the number of opening or open streams.
func (t *StreamTracker) Active() int {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.active
}

// State returns the tracked state of one stream.
func (t *StreamTracker) State(stream ports.BrokerStreamID) (StreamState, bool) {
	if t == nil {
		return 0, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	state, ok := t.states[stream]
	return state, ok
}

// lookupLocked fences the frame scope and classifies the stream: a tracked
// opening or open state, a retired stream that was allocated earlier (a
// tracked retired record or one evicted past the retention bound), or a
// future identity that was never allocated.
func (t *StreamTracker) lookupLocked(scope Scope, stream ports.BrokerStreamID) (StreamState, error) {
	if scope != t.scope {
		return 0, ErrScopeMismatch
	}
	if err := stream.Validate(); err != nil {
		return 0, ErrInvalidStream
	}
	if state, ok := t.states[stream]; ok {
		return state, nil
	}
	if stream > t.highest {
		return 0, ErrFutureStream
	}
	return StreamRetired, nil
}

func (t *StreamTracker) retireLocked(stream ports.BrokerStreamID) {
	t.states[stream] = StreamRetired
	t.retired = append(t.retired, stream)
	t.active--
	for len(t.retired) > MaxRetiredStreams {
		evicted := t.retired[0]
		t.retired = t.retired[1:]
		delete(t.states, evicted)
	}
}

func (t *StreamTracker) reset() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.states = make(map[ports.BrokerStreamID]StreamState, MaxStreams)
	t.retired = nil
	t.highest = 0
	t.window = ports.BrokerStreamWindow{}
	t.active = 0
}
