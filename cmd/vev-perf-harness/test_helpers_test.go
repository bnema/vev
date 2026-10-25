//go:build linux

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
)

// newClockMock preserves the harness's deterministic nanosecond accounting.
func newClockMock(t *testing.T) *mockclock {
	t.Helper()
	c := newMockclock(t)
	var tick int64
	c.EXPECT().Now().RunAndReturn(func() int64 { tick += 10; return tick }).Maybe()
	c.EXPECT().Sleep(mock.Anything).Run(func(d time.Duration) { tick += d.Nanoseconds() }).Maybe()
	return c
}

// processState records observations without implementing launchedProcess.
type processState struct {
	fail                 bool
	measureErr, closeErr error
	warmups, measures    [][]byte
	closed               bool
}

type processOption func(*mocklaunchedProcess)

func newProcessMock(t *testing.T, p *processState, opts ...processOption) *mocklaunchedProcess {
	t.Helper()
	m := newMocklaunchedProcess(t)
	for _, opt := range opts {
		opt(m)
	}
	m.EXPECT().Warmup(mock.Anything).RunAndReturn(func(input []byte) error {
		p.warmups = append(p.warmups, append([]byte(nil), input...))
		return nil
	}).Maybe()
	m.EXPECT().Measure(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(input []byte, injected, flush func() error) error {
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
	}).Maybe()
	m.EXPECT().Close().RunAndReturn(func() error { p.closed = true; return p.closeErr }).Maybe()
	return m
}

type launcherState struct {
	mappings             []processMapping
	args                 [][]string
	commands             []roleCommand
	process              []*processState
	measureErr, closeErr map[string]error
	manifestPresent      bool
}

type launcherOption func(*mocklauncher)

func newLauncherMock(t *testing.T, l *launcherState, opts ...launcherOption) *mocklauncher {
	t.Helper()
	m := newMocklauncher(t)
	for _, opt := range opts {
		opt(m)
	}
	m.EXPECT().Launch(mock.Anything, mock.Anything).RunAndReturn(func(mapping processMapping, command roleCommand) (launchedProcess, error) {
		return launchRecordedProcess(t, l, mapping, command)
	}).Maybe()
	return m
}

func launchRecordedProcess(t *testing.T, l *launcherState, m processMapping, command roleCommand, opts ...processOption) (launchedProcess, error) {
	l.mappings = append(l.mappings, m)
	l.args = append(l.args, append([]string(nil), command.Args...))
	l.commands = append(l.commands, command)
	_, e := os.Stat(filepath.Join(filepath.Dir(m.TracePath), "manifest.json"))
	l.manifestPresent = e == nil
	p := &processState{measureErr: l.measureErr[m.Role], closeErr: l.closeErr[m.Role]}
	l.process = append(l.process, p)
	return newProcessMock(t, p, opts...), nil
}

func insufficientEventLauncher(t *testing.T, clock *mockclock) *mocklauncher {
	return newLauncherMock(t, &launcherState{}, func(l *mocklauncher) {
		l.EXPECT().Launch(mock.Anything, mock.Anything).RunAndReturn(func(m processMapping, _ roleCommand) (launchedProcess, error) {
			if m.Role != "client" {
				return newProcessMock(t, &processState{}), nil
			}
			return newProcessMock(t, &processState{}, func(p *mocklaunchedProcess) {
				p.EXPECT().Measure(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(_ []byte, injected, flushed func() error) error {
					if err := injected(); err != nil {
						return err
					}
					clock.Sleep(minimumDuration)
					return flushed()
				})
			}), nil
		})
	})
}

func failedFlushLauncher(t *testing.T, failure error) *mocklauncher {
	return newLauncherMock(t, &launcherState{}, func(l *mocklauncher) {
		l.EXPECT().Launch(mock.Anything, mock.Anything).RunAndReturn(func(m processMapping, _ roleCommand) (launchedProcess, error) {
			if m.Role != "client" {
				return newProcessMock(t, &processState{}), nil
			}
			return newProcessMock(t, &processState{}, func(p *mocklaunchedProcess) {
				p.EXPECT().Measure(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(_ []byte, injected, _ func() error) error {
					if err := injected(); err != nil {
						return err
					}
					return failure
				})
			}), nil
		})
	})
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

func failingLauncher(t *testing.T, state *launcherState) *mocklauncher {
	return newLauncherMock(t, state, func(l *mocklauncher) {
		l.EXPECT().Launch(mock.Anything, mock.Anything).RunAndReturn(func(m processMapping, command roleCommand) (launchedProcess, error) {
			p, err := launchRecordedProcess(t, state, m, command)
			if m.Role == "client" {
				state.process[len(state.process)-1].fail = true
			}
			return p, err
		})
	})
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

func traceLauncher(t *testing.T, state *launcherState) *mocklauncher {
	return newLauncherMock(t, state, func(l *mocklauncher) {
		l.EXPECT().Launch(mock.Anything, mock.Anything).RunAndReturn(func(m processMapping, command roleCommand) (launchedProcess, error) {
			p, err := launchRecordedProcess(t, state, m, command)
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
		})
	})
}

func closeTraceLauncher(t *testing.T, state *launcherState) *mocklauncher {
	return newLauncherMock(t, state, func(l *mocklauncher) {
		l.EXPECT().Launch(mock.Anything, mock.Anything).RunAndReturn(func(m processMapping, command roleCommand) (launchedProcess, error) {
			start := traceRecord{ProcessID: m.ProcessID, Component: m.Role, Scenario: m.Scenario, Run: uint64(m.Run), Sequence: 1, RequestID: 1, Epoch: 1, Kind: "adapter_receive_start", Tick: 10, Valid: true}
			if err := appendTraceRecord(m.TracePath, start); err != nil {
				return nil, err
			}
			return launchRecordedProcess(t, state, m, command, func(p *mocklaunchedProcess) {
				recorded := state.process[len(state.process)-1]
				p.EXPECT().Close().RunAndReturn(func() error {
					recorded.closed = true
					return errors.Join(recorded.closeErr, appendTraceRecord(m.TracePath, traceRecord{ProcessID: m.ProcessID, Component: m.Role, Scenario: m.Scenario, Run: uint64(m.Run), Sequence: 1, RequestID: 1, Epoch: 1, Kind: "adapter_receive_end", Tick: 20, Valid: true}))
				})
			})
		})
	})
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
