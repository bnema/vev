package sshstdio

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/adapters/observability"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/wire"
)

func TestTransportObservabilitySSHStdioEOFEndsReceive(t *testing.T) {
	tracePath := t.TempDir() + "/ssh-eof.jsonl"
	observer, closer, err := observability.NewJSONL(tracePath, sshRuntimeClock{now: time.Unix(0, 309)}, "ssh-process")
	if err != nil {
		t.Fatalf("NewJSONL() error = %v", err)
	}
	reporter := observability.NewSerialized(observer, 64)
	defer reporter.Close()
	if _, err := NewTransport(bytes.NewReader(nil), &bytes.Buffer{}, nil, WithRuntimeObserver(reporter)).Recv(); err == nil {
		t.Fatal("Recv() error = nil at EOF")
	}
	reporter.Close()
	if err := closer.Close(); err != nil {
		t.Fatalf("trace Close() error = %v", err)
	}
	assertSSHReceiveFailurePair(t, tracePath)
}

func TestTransportObservabilitySSHStdioCloseEndsBlockedReceive(t *testing.T) {
	tracePath := t.TempDir() + "/ssh-close.jsonl"
	observer, closer, err := observability.NewJSONL(tracePath, sshRuntimeClock{now: time.Unix(0, 311)}, "ssh-process")
	if err != nil {
		t.Fatalf("NewJSONL() error = %v", err)
	}
	reporter := observability.NewSerialized(observer, 64)
	defer reporter.Close()
	reader := &sshShutdownReader{entered: make(chan struct{}), done: make(chan struct{})}
	shutdownErr := errors.New("ssh shutdown failed")
	transport := NewTransport(reader, &bytes.Buffer{}, func() error { return shutdownErr }, WithRuntimeObserver(reporter))
	recvDone := make(chan error, 1)
	go func() { _, err := transport.Recv(); recvDone <- err }()
	<-reader.entered
	if err := transport.Close(); !errors.Is(err, shutdownErr) {
		t.Fatalf("Close() error = %v, want %v", err, shutdownErr)
	}
	if err := <-recvDone; err == nil {
		t.Fatal("Recv() error = nil after Close()")
	}
	reporter.Close()
	if err := closer.Close(); err != nil {
		t.Fatalf("trace Close() error = %v", err)
	}
	assertSSHReceiveFailurePair(t, tracePath)
}

type sshShutdownReader struct {
	entered chan struct{}
	done    chan struct{}
	once    sync.Once
}

func (r *sshShutdownReader) Read([]byte) (int, error) {
	r.once.Do(func() { close(r.entered) })
	<-r.done
	return 0, errors.New("reader closed")
}

func (r *sshShutdownReader) Close() error {
	r.once.Do(func() { close(r.entered) })
	close(r.done)
	return nil
}

func assertSSHReceiveFailurePair(t *testing.T, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(trace): %v", err)
	}
	var starts, ends []struct {
		ProcessID string `json:"process_id"`
		Scenario  string `json:"scenario"`
		Run       uint64 `json:"run"`
		Sequence  uint64 `json:"sequence"`
		RequestID uint64 `json:"request_id"`
		Epoch     uint64 `json:"epoch"`
		Kind      string `json:"kind"`
		Valid     bool   `json:"valid"`
	}
	for line := range strings.SplitSeq(strings.TrimSpace(string(data)), "\n") {
		var mark struct {
			ProcessID string `json:"process_id"`
			Scenario  string `json:"scenario"`
			Run       uint64 `json:"run"`
			Sequence  uint64 `json:"sequence"`
			RequestID uint64 `json:"request_id"`
			Epoch     uint64 `json:"epoch"`
			Kind      string `json:"kind"`
			Valid     bool   `json:"valid"`
		}
		if err := json.Unmarshal([]byte(line), &mark); err != nil {
			t.Fatalf("Unmarshal(trace): %v", err)
		}
		switch mark.Kind {
		case "adapter_receive_start":
			starts = append(starts, mark)
		case "adapter_receive_end":
			ends = append(ends, mark)
		}
	}
	if len(starts) != 1 || len(ends) != 1 {
		t.Fatalf("receive spans start=%d end=%d, want exactly one pair", len(starts), len(ends))
	}
	start, end := starts[0], ends[0]
	if start.ProcessID != end.ProcessID || start.Scenario != end.Scenario || start.Run != end.Run || start.Sequence != end.Sequence || start.RequestID != end.RequestID || start.Epoch != end.Epoch {
		t.Fatalf("mismatched receive correlation: start=%+v end=%+v", start, end)
	}
	if !start.Valid || end.Valid {
		t.Fatalf("receive validity start=%t end=%t, want true/false", start.Valid, end.Valid)
	}
}

func TestProcessStdinPumpIsSingleton(t *testing.T) {
	first := processStdinPumpFor(os.Stdin)
	second := processStdinPumpFor(os.Stdin)
	if first != second {
		t.Fatal("process stdin created more than one lifetime pump")
	}
}

func TestTransportCloseLeavesProcessStdinOpen(t *testing.T) {
	transport := NewTransport(os.Stdin, io.Discard, nil)
	if err := transport.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := os.Stdin.Stat(); err != nil {
		t.Fatalf("process stdin was closed: %v", err)
	}
}

func TestTransportObservabilitySSHStdioCloseDoesNotWaitForUnownedReceive(t *testing.T) {
	tracePath := t.TempDir() + "/ssh-unowned-close.jsonl"
	observer, closer, err := observability.NewJSONL(tracePath, sshRuntimeClock{now: time.Unix(0, 313)}, "ssh-process")
	if err != nil {
		t.Fatalf("NewJSONL() error = %v", err)
	}
	reporter := observability.NewSerialized(observer, 64)
	defer reporter.Close()
	reader := &sshUnownedBlockedReader{entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(reader.unblock)
	transport := NewTransport(reader, &bytes.Buffer{}, nil, WithRuntimeObserver(reporter))
	recvDone := make(chan error, 1)
	go func() { _, err := transport.Recv(); recvDone <- err }()
	<-reader.entered

	closeDone := make(chan error, 1)
	go func() { closeDone <- transport.Close() }()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close() waited for an unowned blocked reader")
	}

	select {
	case err := <-recvDone:
		if err == nil {
			t.Fatal("Recv() error = nil after Close()")
		}
	case <-time.After(time.Second):
		t.Fatal("Recv() did not finish after Close()")
	}
	reporter.Close()
	if err := closer.Close(); err != nil {
		t.Fatalf("trace Close() error = %v", err)
	}
	assertSSHReceiveFailurePair(t, tracePath)
}

type sshUnownedBlockedReader struct {
	entered     chan struct{}
	release     chan struct{}
	enteredOnce sync.Once
	releaseOnce sync.Once
}

func (r *sshUnownedBlockedReader) Read([]byte) (int, error) {
	r.enteredOnce.Do(func() { close(r.entered) })
	<-r.release
	return 0, errors.New("reader released")
}

func (r *sshUnownedBlockedReader) unblock() {
	r.releaseOnce.Do(func() { close(r.release) })
}

func TestTransportObservabilitySSHStdioPreservesCarriage(t *testing.T) {
	tracePath := t.TempDir() + "/ssh.jsonl"
	observer, closer, err := observability.NewJSONL(tracePath, sshRuntimeClock{now: time.Unix(0, 307)}, "ssh-process")
	if err != nil {
		t.Fatalf("NewJSONL() error = %v", err)
	}
	reporter := observability.NewSerialized(observer, 64)
	defer reporter.Close()

	frame := wire.Frame{Type: wire.MsgOutput, Payload: []byte("stdio bytes stay exact")}
	var observed, baseline bytes.Buffer
	if err := NewTransport(bytes.NewReader(nil), &observed, nil, WithRuntimeObserver(reporter)).Send(frame); err != nil {
		t.Fatalf("observed Send() error = %v", err)
	}
	if err := NewTransport(bytes.NewReader(nil), &baseline, nil).Send(frame); err != nil {
		t.Fatalf("baseline Send() error = %v", err)
	}
	if !bytes.Equal(observed.Bytes(), baseline.Bytes()) {
		t.Fatalf("observer changed SSH stdio wire bytes: got %x, want %x", observed.Bytes(), baseline.Bytes())
	}
	received, err := NewTransport(bytes.NewReader(baseline.Bytes()), &bytes.Buffer{}, nil, WithRuntimeObserver(reporter)).Recv()
	if err != nil {
		t.Fatalf("observed Recv() error = %v", err)
	}
	if received.Type != frame.Type || !bytes.Equal(received.Payload, frame.Payload) {
		t.Fatalf("observed Recv() = %#v, want %#v", received, frame)
	}
	reporter.Close()
	if err := closer.Close(); err != nil {
		t.Fatalf("trace Close() error = %v", err)
	}
	trace, err := os.ReadFile(tracePath)
	if err != nil {
		t.Fatalf("ReadFile(trace): %v", err)
	}
	for _, kind := range []string{"adapter_send_start", "adapter_send_end", "adapter_receive_start", "adapter_receive_end"} {
		if !strings.Contains(string(trace), `"kind":"`+kind+`"`) {
			t.Errorf("trace missing %s", kind)
		}
	}
}

type sshRuntimeClock struct{ now time.Time }

func (c sshRuntimeClock) Now() time.Time { return c.now }
func (c sshRuntimeClock) NewTimer(d time.Duration) ports.Timer {
	return sshRuntimeTimer{timer: time.NewTimer(d)}
}

type sshRuntimeTimer struct{ timer *time.Timer }

func (t sshRuntimeTimer) C() <-chan time.Time        { return t.timer.C }
func (t sshRuntimeTimer) Reset(d time.Duration) bool { return t.timer.Reset(d) }
func (t sshRuntimeTimer) Stop() bool                 { return t.timer.Stop() }
