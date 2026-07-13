// Package observability contains concrete process-local trace sinks.
package observability

import (
	"encoding/json"
	"io"
	"os"
	"sync"

	"github.com/bnema/vev/internal/ports"
)

// NewJSONL opens path as the sole timestamp owner for one OS process. Marks
// carrying a caller timestamp are rejected rather than rewritten, preventing
// accidental arithmetic across clock domains.
func NewJSONL(path string, clock ports.Clock, processID string) (ports.RuntimeObserver, io.Closer, error) {
	if path == "" || clock == nil || processID == "" {
		return nil, nil, os.ErrInvalid
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return nil, nil, err
	}
	j := &jsonl{file: f, clock: clock, processID: processID}
	return j, j, nil
}

type jsonl struct {
	mu        sync.Mutex
	file      *os.File
	clock     ports.Clock
	processID string
	closed    bool
	err       error
}

type record struct {
	Schema      uint16                `json:"schema"`
	ProcessID   string                `json:"process_id"`
	Component   string                `json:"component"`
	Scenario    string                `json:"scenario"`
	Run         uint64                `json:"run"`
	Sequence    uint64                `json:"sequence"`
	RequestID   uint64                `json:"request_id"`
	Epoch       uint64                `json:"epoch"`
	Kind        ports.RuntimeMarkKind `json:"kind"`
	Tick        int64                 `json:"tick"`
	Bytes       uint64                `json:"bytes"`
	Fragments   uint64                `json:"fragments"`
	Retransmits uint64                `json:"retransmits"`
	Pending     uint64                `json:"pending"`
	AckRTTNanos int64                 `json:"ack_rtt_nanos"`
	Valid       bool                  `json:"valid"`
}

func (j *jsonl) ObserveRuntime(mark ports.RuntimeMark) {
	// Tick is a write-once value owned by this adapter. Reject it before any
	// mutation so callers cannot smuggle a timestamp from their own clock.
	if j == nil || mark.Tick != 0 {
		return
	}
	mark.ProcessID = j.processID
	if !ports.RuntimeMarkValid(mark) {
		return
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return
	}
	r := record{Schema: mark.Schema, ProcessID: mark.ProcessID, Component: mark.Component, Scenario: mark.Scenario, Run: mark.Run, Sequence: mark.Sequence, RequestID: mark.RequestID, Epoch: mark.Epoch, Kind: mark.Kind, Tick: j.clock.Now().UnixNano(), Bytes: mark.Bytes, Fragments: mark.Fragments, Retransmits: mark.Retransmits, Pending: mark.Pending, AckRTTNanos: mark.AckRTTNanos, Valid: mark.Valid}
	encoded, err := json.Marshal(r)
	if err != nil {
		j.recordError(err)
		return
	}
	if _, err := j.file.Write(append(encoded, '\n')); err != nil {
		j.recordError(err)
	}
}

func (j *jsonl) recordError(err error) {
	if err != nil && j.err == nil {
		j.err = err
	}
}

func (j *jsonl) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return nil
	}
	j.closed = true
	closeErr := j.file.Close()
	if j.err != nil {
		return j.err
	}
	return closeErr
}
