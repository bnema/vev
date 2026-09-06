package uidriver

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"

	clockadapter "github.com/bnema/vev/internal/adapters/clock"
	"github.com/bnema/vev/internal/ports"
)

const (
	responseSlots        = 2
	errorResponseReserve = 1024
)

// Server shares attachment-wide connection, action, and serialized-response
// limits. Closing a stream cancels its request waiters, never the attachment's
// Runner.
type Server struct {
	service ports.UIService
	clock   ports.Clock

	mu          sync.Mutex
	connections int
	serialized  int
	actionBusy  bool
}

func New(service ports.UIService, clock ports.Clock) *Server {
	if clock == nil {
		clock = clockadapter.New()
	}
	return &Server{service: service, clock: clock}
}

func (s *Server) reserve(bytes int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Keep room for one small error in each connection's response slots.
	limit := maxSerializedBytes
	if bytes > errorResponseReserve {
		limit -= maxConnections * responseSlots * errorResponseReserve
	}
	if s.serialized+bytes > limit {
		return false
	}
	s.serialized += bytes
	return true
}

func (s *Server) release(bytes int) {
	s.mu.Lock()
	s.serialized -= bytes
	s.mu.Unlock()
}

func (s *Server) admitAction() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.actionBusy {
		return false
	}
	s.actionBusy = true
	return true
}

func (s *Server) finishAction() {
	s.mu.Lock()
	s.actionBusy = false
	s.mu.Unlock()
}

type queuedResponse struct {
	data     []byte
	reserved int
	id       uint64
	ownsID   bool
}

// Serve owns stream until return. Close must interrupt its blocked reads and
// writes (Unix sockets and the app's owned stdio pipes satisfy this contract).
func (s *Server) Serve(ctx context.Context, stream io.ReadWriteCloser, ready Ready) error {
	s.mu.Lock()
	if s.connections >= maxConnections {
		s.mu.Unlock()
		_ = stream.Close()
		return &ports.UIError{Code: ports.UIErrBusy}
	}
	s.connections++
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.connections--
		s.mu.Unlock()
	}()
	defer stream.Close()

	serving, cancelServing := context.WithCancel(ctx)
	defer cancelServing()
	requests, cancelRequests := context.WithCancel(serving)
	defer cancelRequests()
	var closeOnce sync.Once
	closeStream := func() { closeOnce.Do(func() { _ = stream.Close() }) }
	watcherDone := make(chan struct{})
	stopWatcher := make(chan struct{})
	go func() {
		defer close(watcherDone)
		select {
		case <-serving.Done():
			closeStream()
		case <-stopWatcher:
		}
	}()
	defer func() {
		close(stopWatcher)
		<-watcherDone
	}()

	queue := make(chan queuedResponse, responseSlots)
	slots := make(chan struct{}, responseSlots)
	var stateMu sync.Mutex
	ids := make(map[uint64]bool)
	var waitActive bool

	writerDone := make(chan error, 1)
	go func() {
		var writeErr error
		for response := range queue {
			if writeErr == nil {
				writeErr = s.writeResponse(serving, closeStream, stream, response.data)
			}
			s.release(response.reserved)
			<-slots
			if response.ownsID {
				stateMu.Lock()
				delete(ids, response.id)
				stateMu.Unlock()
			}
		}
		writerDone <- writeErr
	}()

	respond := func(id uint64, ownsID bool, build func() envelope) bool {
		select {
		case slots <- struct{}{}:
		case <-serving.Done():
			return false
		default:
			cancelServing()
			closeStream()
			return false
		}

		reserved := maxResponseBytes
		var response envelope
		if !s.reserve(reserved) {
			reserved = errorResponseReserve
			if !s.reserve(reserved) {
				<-slots
				cancelServing()
				closeStream()
				return false
			}
			response = errorEnvelope(id, &ports.UIError{Code: ports.UIErrBusy})
		} else {
			// Build outside all adapter and terminal locks. Capture owns its own
			// immutable snapshot and may be evaluated while a wait is blocked.
			response = build()
		}
		data, err := marshalResponse(response)
		if err != nil {
			response = errorEnvelope(id, &ports.UIError{Code: ports.UIErrCaptureTooLarge})
			data, err = marshalResponse(response)
			if err != nil {
				// The fixed error envelope is deliberately tiny; reaching this
				// branch means the process cannot produce a valid response.
				data = []byte(`{"version":1,"id":0,"error":{"code":"unavailable","message":"unavailable","accepted":false}}\n`)
			}
		}
		queued := queuedResponse{data: data, reserved: reserved, id: id, ownsID: ownsID}
		select {
		case queue <- queued:
			return true
		case <-serving.Done():
			s.release(reserved)
			<-slots
			return false
		}
	}

	if !respond(0, false, func() envelope { return envelope{Version: apiVersion, Result: ready} }) {
		cancelServing()
		closeStream()
	}

	var workers sync.WaitGroup
	scanner := bufio.NewScanner(stream)
	scanner.Buffer(make([]byte, 4096), maxRequestBytes+2)
	for scanner.Scan() {
		parsed, err := decodeRequest(scanner.Bytes())
		if err != nil {
			if !respond(parsed.ID, false, func() envelope { return errorEnvelope(parsed.ID, err) }) {
				break
			}
			continue
		}

		stateMu.Lock()
		duplicate := ids[parsed.ID]
		if !duplicate {
			ids[parsed.ID] = true
		}
		isAction := parsed.Op == opKeys || parsed.Op == opText
		busy := false
		if !duplicate && isAction && ready.Control {
			busy = !s.admitAction()
		}
		if !duplicate && parsed.Op == opWait {
			busy = waitActive
			if !busy {
				waitActive = true
			}
		}
		stateMu.Unlock()

		if duplicate || busy {
			code := ports.UIErrBusy
			if duplicate {
				code = ports.UIErrInvalidRequest
			}
			if !respond(parsed.ID, !duplicate, func() envelope {
				return errorEnvelope(parsed.ID, &ports.UIError{Code: code})
			}) {
				break
			}
			continue
		}

		execute := func() {
			defer func() {
				stateMu.Lock()
				if parsed.Op == opWait {
					waitActive = false
				}
				stateMu.Unlock()
				if isAction && ready.Control {
					s.finishAction()
				}
			}()
			if isAction && !ready.Control {
				respond(parsed.ID, true, func() envelope {
					return errorEnvelope(parsed.ID, &ports.UIError{Code: ports.UIErrPermissionDenied})
				})
				return
			}
			switch parsed.Op {
			case opCapture:
				respond(parsed.ID, true, func() envelope {
					snapshot, err := s.service.Capture(parsed.Attachment)
					if err != nil {
						return errorEnvelope(parsed.ID, err)
					}
					capture, err := publicCapture(snapshot, parsed.Format)
					if err != nil {
						return errorEnvelope(parsed.ID, err)
					}
					return envelope{Version: apiVersion, ID: parsed.ID, Result: capture}
				})
			case opKeys, opText:
				result, err := s.service.Action(serving, parsed.Action)
				respond(parsed.ID, true, func() envelope {
					if err != nil {
						return errorEnvelope(parsed.ID, err)
					}
					return envelope{Version: apiVersion, ID: parsed.ID, Result: actionResponse{ActionID: result.ActionID, Accepted: result.Accepted, Status: result.Status, Revision: result.Revision, Context: publicContext(result.Context)}}
				})
			case opWait:
				result, err := s.service.Wait(requests, parsed.Wait)
				respond(parsed.ID, true, func() envelope {
					if err != nil {
						return errorEnvelope(parsed.ID, err)
					}
					return envelope{Version: apiVersion, ID: parsed.ID, Result: waitResponse{ActionID: result.ActionID, ActionStatus: result.ActionStatus, Revision: result.Revision, Context: publicContext(result.Context)}}
				})
			}
		}

		// Capture is kept on the read loop so a fast query is never starved by
		// an unbounded operation worker. Wait and action must be independent of
		// subsequent reads.
		if parsed.Op == opCapture {
			execute()
		} else {
			workers.Add(1)
			go func() {
				defer workers.Done()
				execute()
			}()
		}
		if serving.Err() != nil {
			break
		}
	}

	readErr := scanner.Err()
	if readErr != nil && serving.Err() == nil {
		respond(0, false, func() envelope { return errorEnvelope(0, invalidRequest()) })
	}
	cancelRequests()
	workers.Wait()
	close(queue)
	writeErr := <-writerDone
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if writeErr != nil {
		return &ports.UIError{Code: ports.UIErrUnavailable}
	}
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return invalidRequest()
	}
	return nil
}

func (s *Server) writeResponse(ctx context.Context, closeStream func(), stream io.Writer, data []byte) error {
	timer := s.clock.NewTimer(writeTimeout)
	writeDone := make(chan struct{})
	deadlineDone := make(chan struct{})
	go func() {
		defer close(deadlineDone)
		select {
		case <-timer.C():
			closeStream()
		case <-writeDone:
		case <-ctx.Done():
			closeStream()
		}
	}()
	n, err := stream.Write(data)
	close(writeDone)
	timer.Stop()
	<-deadlineDone
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	return nil
}

func marshalResponse(response envelope) ([]byte, error) {
	data, err := json.Marshal(response)
	if err != nil {
		return nil, err
	}
	if len(data)+1 > maxResponseBytes {
		return nil, errors.New("response exceeds configured size")
	}
	return append(data, '\n'), nil
}
