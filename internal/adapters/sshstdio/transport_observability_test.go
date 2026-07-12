package sshstdio

import (
	"bytes"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/bnema/vev/internal/adapters/observability"
	"github.com/bnema/vev/internal/ports"
)

func TestTransportObservabilitySSHStdioPreservesCarriage(t *testing.T) {
	tracePath := t.TempDir() + "/ssh.jsonl"
	observer, closer, err := observability.NewJSONL(tracePath, sshRuntimeClock{now: time.Unix(0, 307)}, "ssh-process")
	if err != nil {
		t.Fatalf("NewJSONL() error = %v", err)
	}

	frame := ports.Frame{Type: ports.MsgOutput, Payload: []byte("stdio bytes stay exact")}
	var observed, baseline bytes.Buffer
	if err := NewTransport(bytes.NewReader(nil), &observed, nil, WithRuntimeObserver(observer)).Send(frame); err != nil {
		t.Fatalf("observed Send() error = %v", err)
	}
	if err := NewTransport(bytes.NewReader(nil), &baseline, nil).Send(frame); err != nil {
		t.Fatalf("baseline Send() error = %v", err)
	}
	if !bytes.Equal(observed.Bytes(), baseline.Bytes()) {
		t.Fatalf("observer changed SSH stdio wire bytes: got %x, want %x", observed.Bytes(), baseline.Bytes())
	}
	received, err := NewTransport(bytes.NewReader(baseline.Bytes()), &bytes.Buffer{}, nil, WithRuntimeObserver(observer)).Recv()
	if err != nil {
		t.Fatalf("observed Recv() error = %v", err)
	}
	if received.Type != frame.Type || !bytes.Equal(received.Payload, frame.Payload) {
		t.Fatalf("observed Recv() = %#v, want %#v", received, frame)
	}
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
