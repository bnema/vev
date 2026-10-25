package app

import (
	"bytes"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
)

// terminalState is the recorded and shared state behind one newTerminalMock:
// a mutable geometry, an output buffer, and the flush and raw-mode restore
// counts. It implements no interface; the mock's closures capture it.
type terminalState struct {
	mu       sync.Mutex
	geometry domain.Geometry
	out      bytes.Buffer
	flushes  int
	restores int
}

// terminalOutput is the io.Writer a mock terminal's Out returns. It serializes
// writes through the state's own lock so a test reads a consistent buffer,
// exactly as the composition's writer mutex does in production.
type terminalOutput struct{ state *terminalState }

func (w terminalOutput) Write(data []byte) (int, error) {
	w.state.mu.Lock()
	defer w.state.mu.Unlock()
	return w.state.out.Write(data)
}

func (s *terminalState) resize(geometry domain.Geometry) {
	s.mu.Lock()
	s.geometry = geometry
	s.mu.Unlock()
}

// seed writes bytes exactly as an admitted attachment foreground would, so a
// test can prove the composition never paints over session output.
func (s *terminalState) seed(data string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.out.WriteString(data)
}

// written returns the bytes the composition actually wrote and the flush count.
func (s *terminalState) written() (string, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.out.String(), s.flushes
}

// counts returns the written byte count, the flush count and the raw-mode
// restore count.
func (s *terminalState) counts() (int, int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.out.Len(), s.flushes, s.restores
}

// newTerminalMock is a ports.Terminal over a mutable geometry and a recording
// output buffer. By default its resize channel is already closed and its input
// is empty. Options register overrides first, then the defaults are registered
// with Maybe, and testify matches the first registered expectation.
func newTerminalMock(t *testing.T, geometry domain.Geometry, opts ...func(*portsmocks.MockTerminal, *terminalState)) (*portsmocks.MockTerminal, *terminalState) {
	t.Helper()
	terminal := portsmocks.NewMockTerminal(t)
	state := &terminalState{geometry: geometry}
	for _, opt := range opts {
		opt(terminal, state)
	}
	terminal.EXPECT().EnableKittyKeyboard(mock.Anything).Return(nil).Maybe()
	terminal.EXPECT().EnterRaw().RunAndReturn(func() (func() error, error) {
		return func() error {
			state.mu.Lock()
			state.restores++
			state.mu.Unlock()
			return nil
		}, nil
	}).Maybe()
	terminal.EXPECT().Geometry().RunAndReturn(func() (domain.Geometry, error) {
		state.mu.Lock()
		defer state.mu.Unlock()
		return state.geometry, nil
	}).Maybe()
	terminal.EXPECT().ResizeEvents().RunAndReturn(func() <-chan domain.Geometry {
		ch := make(chan domain.Geometry)
		close(ch)
		return ch
	}).Maybe()
	terminal.EXPECT().In().RunAndReturn(func() io.Reader { return strings.NewReader("") }).Maybe()
	terminal.EXPECT().Out().RunAndReturn(func() io.Writer { return terminalOutput{state: state} }).Maybe()
	terminal.EXPECT().Flush().RunAndReturn(func() error {
		state.mu.Lock()
		defer state.mu.Unlock()
		state.flushes++
		return nil
	}).Maybe()
	return terminal, state
}

// transactionState is the recorded state behind one newUITransactionMock.
type transactionState struct {
	mu           sync.Mutex
	contexts     []ports.UIContext
	statuses     []ports.UIPresentationStatus
	beginDepth   int
	endSuccesses []bool
}

// published returns every context a Begin or Publish committed.
func (s *transactionState) published() []ports.UIContext {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]ports.UIContext(nil), s.contexts...)
}

// publishes returns the status of every PublishContext call.
func (s *transactionState) publishes() []ports.UIPresentationStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]ports.UIPresentationStatus(nil), s.statuses...)
}

// balance returns the open transaction depth and the End success flags.
func (s *transactionState) balance() (int, []bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.beginDepth, append([]bool(nil), s.endSuccesses...)
}

// newUITransactionMock is a ports.UIOutputTransaction recording the output
// transactions a paint commits, so a test can prove a frame and its published
// context are one coherent state.
func newUITransactionMock(t *testing.T, opts ...func(*portsmocks.MockUIOutputTransaction, *transactionState)) (*portsmocks.MockUIOutputTransaction, *transactionState) {
	t.Helper()
	transaction := portsmocks.NewMockUIOutputTransaction(t)
	state := &transactionState{}
	for _, opt := range opts {
		opt(transaction, state)
	}
	transaction.EXPECT().BeginOutput(mock.Anything).Run(func(context ports.UIContext) {
		state.mu.Lock()
		defer state.mu.Unlock()
		state.contexts = append(state.contexts, context)
		state.beginDepth++
	}).Maybe()
	transaction.EXPECT().EndOutput(mock.Anything).Run(func(success bool) {
		state.mu.Lock()
		defer state.mu.Unlock()
		state.beginDepth--
		state.endSuccesses = append(state.endSuccesses, success)
	}).Maybe()
	transaction.EXPECT().PublishContext(mock.Anything).RunAndReturn(func(context ports.UIContext) error {
		state.mu.Lock()
		defer state.mu.Unlock()
		state.contexts = append(state.contexts, context)
		state.statuses = append(state.statuses, context.Status)
		return nil
	}).Maybe()
	return transaction, state
}
