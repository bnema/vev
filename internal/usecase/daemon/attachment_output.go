package daemon

import (
	"errors"
	"strconv"
	"sync"
	"sync/atomic"

	renderer "github.com/bnema/vev-vt/ansi"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

// attachmentOutput owns the terminal-output dependency chain and emitted
// render state for one attached client. Each incremental output is rendered
// from the preceding emitted frame and remains pipelineable without waiting for an
// ACK. Cumulative ACKs advance the bounded paint window.
// A reset frame starts a new epoch, clears the dependency chain, and advertises
// state 0 as its base.
//
// Callers serialize access with attachedClient.sendMu. A prepared transaction
// must be sent or committed as a no-op before the next transaction is prepared.
type attachmentOutput struct {
	renderer                  *renderer.Renderer
	epoch                     uint64
	next                      uint64
	acked                     uint64
	generation                uint64
	maxOutstanding            uint64
	outstandingAtomic         atomic.Uint64
	maxOutstandingAtomic      atomic.Uint64
	forceSnapshot             bool
	initialized               bool
	lastCursor                cursorOut
	lastRoutePosition         ports.RoutePosition
	graphicsOutput            *graphicsOutputState
	graphicsUnsupportedWarned atomic.Bool
	attachment                *attachedClient
	stateMu                   sync.Mutex
}

// reconfigureAttachmentOutput applies a replacement terminal's declared
// capabilities to attachment-owned output state. Caller holds d.mu.
func (d *Daemon) reconfigureAttachmentOutput(sess *session, ac *attachedClient, h ports.Hello) {
	if d == nil || ac == nil || ac.output == nil {
		return
	}
	ac.terminalCapabilities.KittyGraphics = h.KittyDirectGraphics
	ac.output.graphicsUnsupportedWarned.Store(false)
	// A replacement connection is a different outer terminal. Retire the old
	// attachment-local state without writing cleanup to the new terminal, then
	// allocate a fresh namespace for any replay it accepts.
	d.discardAttachmentOutputLocked(ac)
	if !h.KittyDirectGraphics {
		// The parked transport has already been retired. Do not emit cleanup into
		// a replacement connection that did not declare graphics support.
		return
	}
	if namespace, fence := d.reserveGraphicsNamespaceLeaseLocked(graphicsNamespaceKey(sess, ac.clientID)); namespace != 0 {
		ac.output.graphicsOutput = newGraphicsOutputStateWithLease(namespace, fence)
		return
	}
	// Namespace exhaustion fails closed; ANSI output remains available.
	ac.terminalCapabilities.KittyGraphics = false
}

// lockView serializes an attachment's view publication with every output
// state transition. It deliberately does not take sendMu: callers that hold
// architecture locks can publish a view without reversing the paint path's
// sendMu -> session lock order.
func (s *attachmentOutput) lockView() {
	if s == nil {
		return
	}
	if s.attachment == nil {
		s.stateMu.Lock()
		return
	}
	s.attachment.viewMu.Lock()
}

func (s *attachmentOutput) unlockView() {
	if s == nil {
		return
	}
	if s.attachment == nil {
		s.stateMu.Unlock()
		return
	}
	s.attachment.viewMu.Unlock()
}

func newOutputStateStream(windowSize ...uint8) *attachmentOutput {
	return newOutputStateStreamForProfile(renderer.ColorProfileTrueColor, windowSize...)
}

func newOutputStateStreamForCapabilities(capabilities ports.TerminalCapabilities, windowSize ...uint8) *attachmentOutput {
	profile := renderer.ColorProfileANSI256
	if capabilities.TrueColor() {
		profile = renderer.ColorProfileTrueColor
	}
	return newOutputStateStreamForProfile(profile, windowSize...)
}

func newOutputStateStreamForProfile(profile renderer.ColorProfile, windowSize ...uint8) *attachmentOutput {
	window := uint8(maxUnackedOutputStates)
	if len(windowSize) > 0 {
		window = normalizeOutputWindow(windowSize[0])
	}
	stream := &attachmentOutput{
		renderer:       renderer.NewWithColorProfile(renderer.Capabilities{}, profile),
		epoch:          1,
		maxOutstanding: uint64(window),
	}
	stream.maxOutstandingAtomic.Store(uint64(window))
	return stream
}

type cursorCandidate struct {
	data []byte
	next cursorOut
}

// preparedAttachmentOutput is one speculative terminal-output transaction.
// ANSI renderer state and cursor state advance together only after the output
// frame is accepted by the attachment's state stream.
type preparedAttachmentOutput struct {
	output   *attachmentOutput
	ansi     *preparedOutput
	graphics *preparedGraphicsOutput
	cursor   cursorCandidate
	data     []byte
}

func (o *attachmentOutput) prepareFrame(d *Daemon, state *capturedRenderState, frame renderer.Frame, damage []renderer.Damage, reset bool, desired cursorOut) (*preparedAttachmentOutput, error) {
	ansi, err := o.prepare(frame, damage, reset)
	if err != nil {
		return nil, err
	}
	cursor := o.prepareCursorTail(desired, len(ansi.data) > 0)
	graphicsReset := reset || o.forceSnapshot || !o.initialized
	graphicsBudget := max(ports.MaxOutputDataLen-len(ansi.data)-len(cursor.data), 0)
	preparedGraphics, err := graphicsOutputDataWithDaemonLimit(d, state, o.attachment, graphicsReset, graphicsBudget)
	if err != nil {
		return nil, err
	}
	data := append([]byte(nil), ansi.data...)
	if preparedGraphics != nil {
		data = append(data, preparedGraphics.data...)
	}
	data = append(data, cursor.data...)
	return &preparedAttachmentOutput{output: o, ansi: ansi, graphics: preparedGraphics, cursor: cursor, data: data}, nil
}

func (p *preparedAttachmentOutput) send(echoAck uint64, send func(ports.Frame) error) error {
	if p == nil || p.ansi == nil {
		return nil
	}
	sendFrame := send
	if p.graphics != nil {
		sendFrame = func(frame ports.Frame) error {
			p.graphics.markSendAttempted()
			return send(frame)
		}
	}
	if err := p.ansi.send(p.data, echoAck, sendFrame); err != nil {
		p.abortGraphics()
		return err
	}
	p.commitIfSent()
	return nil
}

func (p *preparedAttachmentOutput) commitNoSend() {
	if p == nil || p.ansi == nil {
		return
	}
	p.ansi.commitNoSend()
	p.commitIfSent()
}

func (p *preparedAttachmentOutput) commitIfSent() {
	if p == nil || p.ansi == nil || !p.ansi.sent {
		p.abortGraphics()
		return
	}
	if p.graphics != nil {
		p.graphics.commit()
	}
	if p.output != nil {
		p.output.lastCursor = p.cursor.next
	}
}

func (p *preparedAttachmentOutput) abort() {
	if p != nil {
		p.abortGraphics()
	}
}

func (p *preparedAttachmentOutput) abortGraphics() {
	if p != nil && p.graphics != nil {
		p.graphics.abort()
	}
}

func (p *preparedAttachmentOutput) sent() bool {
	return p != nil && p.ansi != nil && p.ansi.sent
}

func (o *attachmentOutput) prepareCursorTail(desired cursorOut, force bool) cursorCandidate {
	candidate := cursorCandidate{next: desired}
	candidate.next.valid = true
	prev := o.lastCursor
	changed := force || !prev.valid || prev.hidden != desired.hidden || prev.row != desired.row || prev.col != desired.col || prev.style != desired.style || prev.hasStyle != desired.hasStyle
	if !changed {
		return candidate
	}
	if desired.hidden {
		candidate.data = []byte("\x1b[?25l")
		return candidate
	}
	candidate.data = append(candidate.data, "\x1b["...)
	candidate.data = strconv.AppendInt(candidate.data, int64(desired.row+1), 10)
	candidate.data = append(candidate.data, ';')
	candidate.data = strconv.AppendInt(candidate.data, int64(desired.col+1), 10)
	candidate.data = append(candidate.data, 'H')
	if !prev.valid || prev.hidden || prev.style != desired.style || prev.hasStyle != desired.hasStyle {
		candidate.data = append(candidate.data, "\x1b["...)
		candidate.data = strconv.AppendInt(candidate.data, int64(desired.style), 10)
		candidate.data = append(candidate.data, " q"...)
	}
	candidate.data = append(candidate.data, "\x1b[?25h"...)
	return candidate
}

type preparedOutput struct {
	stream               *attachmentOutput
	draw                 renderer.PreparedDraw
	base                 uint64
	next                 uint64
	epoch                uint64
	generation           uint64
	connectionGeneration uint64
	viewRevision         uint64
	size                 domain.Size
	data                 []byte
	reset                bool
	attempted            bool
	sent                 bool
}

func (s *attachmentOutput) prepare(frame renderer.Frame, damage []renderer.Damage, reset bool) (*preparedOutput, error) {
	s.lockView()
	defer s.unlockView()
	if s.epoch == 0 {
		s.epoch = 1
	}
	if reset && s.initialized {
		s.rebaseLocked()
	}
	reset = reset || s.forceSnapshot || !s.initialized
	if s.next == ^uint64(0) {
		return nil, errors.New("output state number exhausted")
	}
	draw, err := s.renderer.Prepare(frame, damage, reset)
	if err != nil {
		return nil, err
	}
	s.initialized = true
	generation, epoch, viewRevision := s.fenceLocked()
	return &preparedOutput{
		stream:               s,
		draw:                 draw,
		base:                 s.next,
		next:                 s.next + 1,
		epoch:                epoch,
		generation:           s.generation,
		connectionGeneration: generation,
		viewRevision:         viewRevision,
		size:                 domain.Size{Cols: frame.Width, Rows: frame.Height},
		data:                 draw.Bytes(),
		reset:                reset,
	}, nil
}

func (p *preparedOutput) send(data []byte, echoAck uint64, send func(ports.Frame) error) error {
	if p == nil || p.stream == nil || p.attempted {
		return nil
	}
	p.stream.lockView()
	defer p.stream.unlockView()
	p.attempted = true
	if !p.currentLocked() {
		return nil
	}
	if len(data) == 0 {
		p.commit()
		p.sent = true
		return nil
	}
	base := p.base
	if p.reset {
		base = 0
	}
	out, err := marshalOutputState(data, p.epoch, base, p.next, echoAck, p.viewRevision, p.size, p.reset)
	if err != nil {
		p.stream.forceSnapshot = true
		return err
	}
	if err := send(out); err != nil {
		p.stream.forceSnapshot = true
		return err
	}
	p.commit()
	p.stream.next = p.next
	p.stream.publishOutstanding()
	p.sent = true
	return nil
}

func (p *preparedOutput) commitNoSend() {
	if p == nil || p.stream == nil || p.attempted {
		return
	}
	p.stream.lockView()
	defer p.stream.unlockView()
	p.attempted = true
	if !p.currentLocked() || len(p.data) != 0 {
		return
	}
	p.commit()
	p.sent = true
}

func (p *preparedOutput) commit() {
	p.draw.Commit()
	p.stream.forceSnapshot = false
}

func (s *attachmentOutput) currentEpoch() uint64 {
	s.lockView()
	defer s.unlockView()
	return s.currentEpochLocked()
}

func (s *attachmentOutput) currentEpochLocked() uint64 {
	if s.epoch == 0 {
		s.epoch = 1
	}
	return s.epoch
}

func (s *attachmentOutput) fenceLocked() (uint64, uint64, uint64) {
	if s == nil {
		return 0, 1, 0
	}
	generation, revision := uint64(0), uint64(0)
	if s.attachment != nil {
		generation = s.attachment.lifecycle.generationValue()
		revision = s.attachment.view.revision
	}
	return generation, s.currentEpochLocked(), revision
}

func (s *attachmentOutput) preparedCurrentLocked(generation, epoch, viewRevision, base uint64) bool {
	if s == nil || s.currentEpochLocked() != epoch || s.generation != generation || s.next != base {
		return false
	}
	if s.attachment == nil {
		return true
	}
	return s.attachment.view.revision == viewRevision
}

func (p *preparedOutput) currentLocked() bool {
	return p != nil && p.stream != nil && p.stream.preparedCurrentLocked(p.generation, p.epoch, p.viewRevision, p.base) &&
		(p.stream.attachment == nil || p.stream.attachment.lifecycle.generationValue() == p.connectionGeneration)
}

func marshalOutputState(data []byte, epoch, base, next, echoAck, viewRevision uint64, size domain.Size, full bool) (ports.Frame, error) {
	payload, err := ports.MarshalOutput(ports.Output{
		Epoch: epoch, Base: base, New: next, Echo: echoAck, ViewRevision: viewRevision,
		Size: size, Full: full, Data: data,
	})
	if err != nil {
		return ports.Frame{}, err
	}
	return ports.Frame{Type: ports.MsgOutput, Payload: payload}, nil
}

// sideEffect builds an output frame without advancing the state stream. When
// an attachment is present, callers must hold its sendMu; attachment-bound
// sends use sideEffectLocked after validating their transport incarnation.
func (s *attachmentOutput) sideEffect(data []byte, echoAck uint64) (ports.Frame, error) {
	s.lockView()
	defer s.unlockView()
	return s.sideEffectLocked(data, echoAck)
}

// sideEffectLocked requires the attachment view lock for both frame creation
// and transport emission. Keeping that lock across the send prevents a view
// publication from overtaking a frame that was fenced just before it.
func (s *attachmentOutput) sideEffectLocked(data []byte, echoAck uint64) (ports.Frame, error) {
	_, epoch, viewRevision := s.fenceLocked()
	size := domain.Size{Cols: 1, Rows: 1}
	if s != nil && s.attachment != nil {
		size = s.attachment.sizeSnapshot()
	}
	return marshalOutputState(data, epoch, 0, 0, echoAck, viewRevision, size, false)
}

func (s *attachmentOutput) ack(epoch, state uint64) bool {
	if s == nil {
		return false
	}
	s.lockView()
	defer s.unlockView()
	if epoch != s.currentEpochLocked() || state > s.next || state <= s.acked {
		return false
	}
	s.acked = state
	s.publishOutstanding()
	return true
}

func (s *attachmentOutput) rebase() {
	if s == nil {
		return
	}
	s.lockView()
	defer s.unlockView()
	s.rebaseLocked()
}

// rebaseAttachment retires all output state coupled to the attachment's
// current session or transport while preserving independent client state.
func (s *attachmentOutput) rebaseAttachment() {
	if s == nil {
		return
	}
	s.rebase()
	s.lastRoutePosition = ports.RoutePosition{}
}

// rebaseLocked is called while the attachment view lock is held. Publishing
// the next view revision takes this path before making that revision visible.
func (s *attachmentOutput) rebaseLocked() {
	if s == nil {
		return
	}
	epoch := s.currentEpochLocked()
	if epoch == ^uint64(0) {
		return
	}
	s.epoch = epoch + 1
	s.next = 0
	s.acked = 0
	s.generation++
	s.publishOutstanding()
	s.maxOutstandingAtomic.Store(s.maxOutstanding)
	s.renderer.Reset()
	s.forceSnapshot = true
	s.initialized = false
}

func (s *attachmentOutput) publishOutstanding() {
	if s == nil {
		return
	}
	s.outstandingAtomic.Store(s.next - s.acked)
}

// syncCapacityLocked refreshes the lock-free readiness snapshot after a
// caller mutates the legacy fields under attachedClient.sendMu.
func (s *attachmentOutput) syncCapacityLocked() {
	if s == nil {
		return
	}
	s.lockView()
	defer s.unlockView()
	s.maxOutstandingAtomic.Store(s.maxOutstanding)
	s.publishOutstanding()
}

func (s *attachmentOutput) setWindow(window uint8) {
	if s == nil {
		return
	}
	s.maxOutstanding = uint64(normalizeOutputWindow(window))
	s.maxOutstandingAtomic.Store(s.maxOutstanding)
}

func (s *attachmentOutput) atCapacity() bool {
	if s == nil {
		return false
	}
	maxOutstanding := s.maxOutstandingAtomic.Load()
	if maxOutstanding == 0 {
		maxOutstanding = s.maxOutstanding
	}
	return s.outstandingAtomic.Load() >= maxOutstanding
}

func (s *attachmentOutput) outstanding() uint64 {
	if s == nil {
		return 0
	}
	s.lockView()
	defer s.unlockView()
	return s.next - s.acked
}
