// Package client implements vev's thin attach client: it speaks the framed
// IPC protocol to a daemon, pumping terminal input, resize events, and PTY
// output between the controlling terminal and the session.
//
// The package depends only on ports (transport, terminal, wire codecs) and
// domain, never on a concrete transport or terminal implementation; the app
// layer wires those in.
package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

// ProtocolError is a session- or protocol-level failure reported by the
// daemon before attach could proceed (a Hello rejected with an ErrorMsg).
// The app layer prints it to the user.
type ProtocolError struct {
	Code uint16
	Text string
}

func (e *ProtocolError) Error() string {
	if e.Text == "" {
		return fmt.Sprintf("vev: daemon rejected attach (code %d)", e.Code)
	}
	return fmt.Sprintf("vev: %s", e.Text)
}

// DetachedError reports that the daemon detached this client for a reason
// other than an ordinary user-initiated detach.
type DetachedError struct {
	Reason uint8
	Text   string
}

func (e *DetachedError) Error() string { return "vev: " + e.Text }

// milestones tracks lifecycle progress so a failure can report which stages
// were never reached — the key diagnostic when an attach dies early.
type milestones struct {
	dialed      bool
	helloSent   bool
	welcomed    bool
	rawEntered  bool
	firstOutput bool
	detached    bool
}

func (m milestones) missing() []string {
	var out []string
	if !m.dialed {
		out = append(out, "dialed")
	}
	if !m.helloSent {
		out = append(out, "hello_sent")
	}
	if !m.welcomed {
		out = append(out, "welcomed")
	}
	if !m.rawEntered {
		out = append(out, "raw_entered")
	}
	if !m.firstOutput {
		out = append(out, "first_output")
	}
	if !m.detached {
		out = append(out, "detached")
	}
	return out
}

// stdinBufSize is the read buffer for the terminal input pump.
const stdinBufSize = 4096

// sendQueueDepth bounds the buffered frames awaiting the single sender
// goroutine, decoupling the input/resize pumps from transport back-pressure.
const sendQueueDepth = 64

// Attach connects an already-dialed transport to the controlling terminal
// and runs the attach loop until the session detaches, the daemon
// disappears, or the context is cancelled.
//
// It owns transport for its lifetime and closes it before returning. The
// terminal is put into raw mode after a successful Welcome and restored on
// every exit path — normal return, error, or panic.
func Attach(ctx context.Context, transport ports.Transport, term ports.Terminal, intent uint8, name string) (retErr error) {
	log := slog.Default().With("component", "client")
	ms := milestones{dialed: true} // transport arrives already connected

	// Runs last: a single structured diagnostic on failure, to the log file
	// (never the terminal). Console output while raw is forbidden.
	defer func() {
		if retErr != nil {
			log.Error("attach ended with error", "err", retErr, "missing_milestones", ms.missing())
		}
	}()

	// Attach owns the transport: close it on every exit path.
	defer func() { _ = transport.Close() }()

	// 1. Handshake: send Hello with our size and TERM.
	size, err := term.Size()
	if err != nil {
		return fmt.Errorf("vev: reading terminal size: %w", err)
	}
	hello := ports.Hello{
		Version: ports.ProtocolVersion,
		Intent:  intent,
		Name:    name,
		Size:    size,
		TermEnv: os.Getenv("TERM"),
	}
	if err := transport.Send(ports.Frame{Type: ports.MsgHello, Payload: ports.MarshalHello(hello)}); err != nil {
		return fmt.Errorf("vev: sending hello: %w", err)
	}
	ms.helloSent = true

	// 2. Await Welcome or a typed rejection.
	reply, err := transport.Recv()
	if err != nil {
		return fmt.Errorf("vev: awaiting welcome: %w", err)
	}
	switch reply.Type {
	case ports.MsgWelcome:
		ms.welcomed = true
	case ports.MsgError:
		em, derr := ports.UnmarshalErrorMsg(reply.Payload)
		if derr != nil {
			return fmt.Errorf("vev: decoding error reply: %w", derr)
		}
		return &ProtocolError{Code: em.Code, Text: em.Text}
	default:
		return fmt.Errorf("vev: unexpected reply type %d before welcome", reply.Type)
	}

	// 3. Enter raw mode; restore on every subsequent exit path.
	restore, err := term.EnterRaw()
	if err != nil {
		return fmt.Errorf("vev: entering raw mode: %w", err)
	}
	ms.rawEntered = true
	defer func() {
		if rerr := restore(); rerr != nil && retErr == nil {
			retErr = fmt.Errorf("vev: restoring terminal: %w", rerr)
		}
	}()

	// 4. Derive a cancellable context so the pumps always unwind when the
	// loop returns. cancel runs first (LIFO): it signals the pumps; the
	// deferred Close above then unblocks any pump parked in a syscall.
	loopCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	sendCh := make(chan ports.Frame, sendQueueDepth)
	sendErrCh := make(chan error, 1)

	go runSender(loopCtx, cancel, transport, sendCh, sendErrCh)
	go runStdin(loopCtx, cancel, term.In(), sendCh)
	go runResize(loopCtx, term.ResizeEvents(), sendCh)

	// 5. Output/main loop: the only goroutine that touches the terminal.
	recvCh := make(chan recvResult, 1)
	go runRecv(loopCtx, transport, recvCh)

	for {
		select {
		case <-loopCtx.Done():
			// A pump initiated shutdown (stdin EOF/detach) or the parent
			// context was cancelled. A queued sender error takes priority.
			select {
			case serr := <-sendErrCh:
				return fmt.Errorf("vev: sending to daemon: %w", serr)
			default:
				return nil
			}
		case r := <-recvCh:
			if r.err != nil {
				if errors.Is(r.err, io.EOF) {
					return fmt.Errorf("vev: daemon vanished (missing: %v): %w", ms.missing(), r.err)
				}
				return fmt.Errorf("vev: receiving from daemon: %w", r.err)
			}
			switch r.frame.Type {
			case ports.MsgOutput:
				o, derr := ports.UnmarshalOutput(r.frame.Payload)
				if derr != nil {
					return fmt.Errorf("vev: decoding output: %w", derr)
				}
				if _, werr := term.Out().Write(o.Data); werr != nil {
					return fmt.Errorf("vev: writing terminal output: %w", werr)
				}
				if ferr := term.Flush(); ferr != nil {
					return fmt.Errorf("vev: flushing terminal: %w", ferr)
				}
				ms.firstOutput = true
			case ports.MsgDetached:
				d, derr := ports.UnmarshalDetached(r.frame.Payload)
				if derr != nil {
					return fmt.Errorf("vev: decoding detached: %w", derr)
				}
				ms.detached = true
				return detachedResult(d.Reason)
			case ports.MsgPong:
				// Liveness reply; nothing to do.
			default:
				// Unknown/out-of-band message types are ignored so a newer
				// daemon can add server->client messages without breaking us.
			}
		}
	}
}

// recvResult carries one framed message (or a read error) from the receive
// pump to the main loop.
type recvResult struct {
	frame ports.Frame
	err   error
}

// runRecv reads frames until an error, forwarding each to the main loop.
// It exits on the first error or when the loop context is cancelled.
func runRecv(ctx context.Context, transport ports.Transport, out chan<- recvResult) {
	for {
		f, err := transport.Recv()
		select {
		case out <- recvResult{frame: f, err: err}:
		case <-ctx.Done():
			return
		}
		if err != nil {
			return
		}
	}
}

// runSender is the sole caller of transport.Send: serialising the input and
// resize pumps through one goroutine keeps the transport's single-writer
// contract intact. A send failure cancels the loop and is surfaced once.
func runSender(ctx context.Context, cancel context.CancelFunc, transport ports.Transport, in <-chan ports.Frame, errCh chan<- error) {
	for {
		select {
		case f := <-in:
			select {
			case <-ctx.Done():
				return
			default:
			}
			if err := transport.Send(f); err != nil {
				select {
				case errCh <- err:
				default:
				}
				cancel()
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

// runStdin pumps terminal input to the daemon as Input frames. A read error
// or EOF is treated as a detach: it best-effort sends a Detach frame and
// cancels the loop.
//
// Known MVP limitation: a bare io.Reader cannot be unblocked from outside,
// so on shutdown initiated elsewhere (daemon detach, transport loss) this
// goroutine stays parked in Read until the next byte arrives or the process
// exits. That is harmless here — Attach has already returned and restored
// the terminal — and matches the standard pattern for stdin pumps; a
// closable stdin duplicate could lift it later if ever needed.
func runStdin(ctx context.Context, cancel context.CancelFunc, in io.Reader, out chan<- ports.Frame) {
	buf := make([]byte, stdinBufSize)
	for {
		n, rerr := in.Read(buf)
		if n > 0 {
			data := make([]byte, n)
			copy(data, buf[:n])
			frame := ports.Frame{Type: ports.MsgInput, Payload: ports.MarshalInput(ports.Input{Data: data})}
			select {
			case out <- frame:
			case <-ctx.Done():
				return
			}
		}
		if rerr != nil {
			// Best-effort detach notification, then unwind.
			select {
			case out <- ports.Frame{Type: ports.MsgDetach, Payload: ports.MarshalDetach(ports.Detach{})}:
			case <-ctx.Done():
			}
			cancel()
			return
		}
	}
}

// runResize forwards coalesced terminal resize events to the daemon. It
// tolerates an already-closed resize channel (which the terminal adapter
// hands back when restore ran before ResizeEvents was first called).
func runResize(ctx context.Context, events <-chan domain.Size, out chan<- ports.Frame) {
	for {
		select {
		case sz, ok := <-events:
			if !ok {
				return
			}
			frame := ports.Frame{Type: ports.MsgResize, Payload: ports.MarshalResize(ports.Resize{Size: sz})}
			select {
			case out <- frame:
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

// detachedResult maps a Detached reason to a return value: an ordinary
// user-initiated detach is a clean (nil) exit; anything else is surfaced so
// the user learns why the session went away.
func detachedResult(reason uint8) error {
	switch reason {
	case ports.ReasonDetach:
		return nil
	case ports.ReasonSessionKilled:
		return &DetachedError{Reason: reason, Text: "session was killed"}
	case ports.ReasonServerShutdown:
		return &DetachedError{Reason: reason, Text: "daemon shut down"}
	default:
		return &DetachedError{Reason: reason, Text: "detached by daemon"}
	}
}
