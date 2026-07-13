package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type fakeClock struct{ tick int64 }

func (c *fakeClock) Now() int64 { c.tick += 10; return c.tick }

func (c *fakeClock) Sleep(d time.Duration) {
	c.tick += d.Nanoseconds()
}

type fakeUDPNetem struct {
	port   int
	closed bool
}

func (n *fakeUDPNetem) Port() int { return n.port }

func (n *fakeUDPNetem) Close() error { n.closed = true; return nil }

type fakeProcess struct {
	fail       bool
	measureErr error
	closeErr   error
	warmups    [][]byte
	measures   [][]byte
	closed     bool
}

func (p *fakeProcess) Warmup(input []byte) error {
	p.warmups = append(p.warmups, append([]byte(nil), input...))
	return nil
}

func (p *fakeProcess) Measure(input []byte, injected, flush func() error) error {
	p.measures = append(p.measures, append([]byte(nil), input...))
	if p.measureErr != nil {
		return p.measureErr
	}
	if p.fail {
		return errors.New("measure failure")
	}
	if err := injected(); err != nil {
		return err
	}
	return flush()
}

func (p *fakeProcess) Close() error { p.closed = true; return p.closeErr }

type fakeLauncher struct {
	mappings        []processMapping
	args            [][]string
	commands        []roleCommand
	process         []*fakeProcess
	measureErr      map[string]error
	closeErr        map[string]error
	manifestPresent bool
}

func (l *fakeLauncher) Launch(m processMapping, command roleCommand) (launchedProcess, error) {
	l.mappings = append(l.mappings, m)
	l.args = append(l.args, append([]string(nil), command.Args...))
	l.commands = append(l.commands, command)
	_, e := os.Stat(filepath.Join(filepath.Dir(m.TracePath), "manifest.json"))
	l.manifestPresent = e == nil
	p := &fakeProcess{measureErr: l.measureErr[m.Role], closeErr: l.closeErr[m.Role]}
	l.process = append(l.process, p)
	return p, nil
}

type lateFlushProcess struct{ clock *fakeClock }

func (*lateFlushProcess) Warmup([]byte) error { return nil }

func (p *lateFlushProcess) Measure(_ []byte, injected, flushed func() error) error {
	if err := injected(); err != nil {
		return err
	}
	p.clock.tick += minimumDuration.Nanoseconds()
	return flushed()
}

func (*lateFlushProcess) Close() error { return nil }

type insufficientEventLauncher struct{ clock *fakeClock }

func (l insufficientEventLauncher) Launch(m processMapping, _ roleCommand) (launchedProcess, error) {
	if m.Role == "client" {
		return &lateFlushProcess{clock: l.clock}, nil
	}
	return &fakeProcess{}, nil
}

type failedFlushProcess struct{ err error }

func (*failedFlushProcess) Warmup([]byte) error { return nil }

func (p *failedFlushProcess) Measure(_ []byte, injected, _ func() error) error {
	if err := injected(); err != nil {
		return err
	}
	return p.err
}

func (*failedFlushProcess) Close() error { return nil }

type failedFlushLauncher struct{ err error }

func (l failedFlushLauncher) Launch(m processMapping, _ roleCommand) (launchedProcess, error) {
	if m.Role == "client" {
		return &failedFlushProcess{err: l.err}, nil
	}
	return &fakeProcess{}, nil
}

func closeTestFile(t *testing.T, f *os.File) {
	t.Helper()
	t.Cleanup(func() {
		if err := f.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			t.Error(err)
		}
	})
}

func removeTestTree(t *testing.T, path string) {
	t.Helper()
	t.Cleanup(func() {
		if err := os.RemoveAll(path); err != nil {
			t.Error(err)
		}
	})
}

func readHarnessMarks(t *testing.T, path string) []harnessMark {
	t.Helper()
	records, err := readJSONL(path)
	if err != nil {
		t.Fatal(err)
	}
	marks := make([]harnessMark, len(records))
	for i, record := range records {
		encoded, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(encoded, &marks[i]); err != nil {
			t.Fatal(err)
		}
	}
	return marks
}

type fakePTY struct{ writes [][]byte }

func (p *fakePTY) Read([]byte) (int, error) { return 0, os.ErrClosed }

func (p *fakePTY) Write(b []byte) (int, error) {
	p.writes = append(p.writes, append([]byte(nil), b...))
	return len(b), nil
}

func (*fakePTY) Close() error { return nil }

type stagedPTY struct {
	allow  <-chan struct{}
	writes chan []byte
}

func (*stagedPTY) Read([]byte) (int, error) { return 0, os.ErrClosed }

func (p *stagedPTY) Write(b []byte) (int, error) {
	if p.allow != nil {
		<-p.allow
	}
	p.writes <- append([]byte(nil), b...)
	return len(b), nil
}

func (*stagedPTY) Close() error { return nil }

type fakeOutput struct {
	bytes.Buffer
	syncs int
}

func (o *fakeOutput) Sync() error { o.syncs++; return nil }

func (*fakeOutput) Close() error { return nil }

type orderedPTY struct {
	write func([]byte)
	close func()
}

func (*orderedPTY) Read([]byte) (int, error) { return 0, os.ErrClosed }

func (p *orderedPTY) Write(b []byte) (int, error) {
	if p.write != nil {
		p.write(b)
	}
	return len(b), nil
}

func (p *orderedPTY) Close() error {
	p.close()
	return nil
}

type orderedOutput struct{ close func() }

func (*orderedOutput) Write(b []byte) (int, error) { return len(b), nil }

func (*orderedOutput) Sync() error { return nil }

func (o *orderedOutput) Close() error {
	o.close()
	return nil
}

type failingLauncher struct{ *fakeLauncher }

func (l *failingLauncher) Launch(m processMapping, command roleCommand) (launchedProcess, error) {
	p, err := l.fakeLauncher.Launch(m, command)
	if err != nil {
		return nil, err
	}
	if m.Role == "client" {
		fake, ok := p.(*fakeProcess)
		if !ok {
			return nil, fmt.Errorf("fake launcher returned %T, want *fakeProcess", p)
		}
		fake.fail = true
	}
	return p, nil
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type traceLauncher struct{ fakeLauncher }

func (l *traceLauncher) Launch(m processMapping, command roleCommand) (launchedProcess, error) {
	p, err := l.fakeLauncher.Launch(m, command)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(m.TracePath, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	for i, pair := range spanPairs {
		for n, kind := range []string{pair.start, pair.end} {
			b, err := json.Marshal(traceRecord{ProcessID: m.ProcessID, Component: m.Role, Scenario: m.Scenario, Run: uint64(m.Run), Sequence: uint64(i + 1), RequestID: 1, Epoch: 1, Kind: kind, Tick: int64(i*20 + n*5), Valid: true})
			if err != nil {
				return nil, errors.Join(err, f.Close())
			}
			if _, err := f.Write(append(b, '\n')); err != nil {
				return nil, errors.Join(err, f.Close())
			}
		}
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	return p, nil
}

type closeTraceProcess struct {
	*fakeProcess
	traceEnd func() error
}

func (p *closeTraceProcess) Close() error {
	p.closed = true
	return errors.Join(p.closeErr, p.traceEnd())
}

type closeTraceLauncher struct{ fakeLauncher }

func (l *closeTraceLauncher) Launch(m processMapping, command roleCommand) (launchedProcess, error) {
	p, err := l.fakeLauncher.Launch(m, command)
	if err != nil {
		return nil, err
	}
	start := traceRecord{ProcessID: m.ProcessID, Component: m.Role, Scenario: m.Scenario, Run: uint64(m.Run), Sequence: 1, RequestID: 1, Epoch: 1, Kind: "adapter_receive_start", Tick: 10, Valid: true}
	if err := appendTraceRecord(m.TracePath, start); err != nil {
		return nil, err
	}
	fake, ok := p.(*fakeProcess)
	if !ok {
		return nil, fmt.Errorf("fake launcher returned %T, want *fakeProcess", p)
	}
	return &closeTraceProcess{fakeProcess: fake, traceEnd: func() error {
		return appendTraceRecord(m.TracePath, traceRecord{ProcessID: m.ProcessID, Component: m.Role, Scenario: m.Scenario, Run: uint64(m.Run), Sequence: 1, RequestID: 1, Epoch: 1, Kind: "adapter_receive_end", Tick: 20, Valid: true})
	}}, nil
}

func appendTraceRecord(path string, record traceRecord) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	b, err := json.Marshal(record)
	if err != nil {
		return errors.Join(err, f.Close())
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		return errors.Join(err, f.Close())
	}
	return f.Close()
}

// boundedLocalConcurrentTrace is an excerpt from the second run of the
// bounded local public-CLI smoke. Receive sequence 3 is in flight while the
// same process serializes sequence 4; shared-process records therefore cannot
// have a global sequence ordering requirement.
const boundedLocalConcurrentTrace = `{"schema":1,"process_id":"1x4-idle-local-r002-client-02","component":"ipc","scenario":"1x4-idle-local","run":2,"sequence":3,"request_id":3,"epoch":3,"kind":"adapter_receive_start","tick":1783902621495284782,"bytes":0,"fragments":0,"retransmits":0,"pending":0,"ack_rtt_nanos":0,"valid":true}
{"schema":1,"process_id":"1x4-idle-local-r002-client-02","component":"ipc","scenario":"1x4-idle-local","run":2,"sequence":4,"request_id":4,"epoch":4,"kind":"adapter_send_start","tick":1783902621495364552,"bytes":96,"fragments":0,"retransmits":0,"pending":0,"ack_rtt_nanos":0,"valid":true}
{"schema":1,"process_id":"1x4-idle-local-r002-client-02","component":"ipc","scenario":"1x4-idle-local","run":2,"sequence":4,"request_id":4,"epoch":4,"kind":"adapter_send_end","tick":1783902621495386452,"bytes":96,"fragments":0,"retransmits":0,"pending":0,"ack_rtt_nanos":0,"valid":true}
{"schema":1,"process_id":"1x4-idle-local-r002-client-02","component":"ipc","scenario":"1x4-idle-local","run":2,"sequence":3,"request_id":3,"epoch":3,"kind":"adapter_receive_end","tick":1783902621495463332,"bytes":0,"fragments":0,"retransmits":0,"pending":0,"ack_rtt_nanos":0,"valid":true}
`
