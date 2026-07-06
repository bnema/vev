package platform

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const processRecordCacheTTL = 250 * time.Millisecond

// ProcessInspector implements ports.ProcessInspector using Linux /proc.
type ProcessInspector struct {
	root string

	mu                 sync.Mutex
	cachedRecords      []processRecord
	cachedRecordsUntil time.Time
}

// NewProcessInspector returns a Linux /proc-backed process inspector.
func NewProcessInspector() *ProcessInspector { return newProcessInspector("/proc") }

func newProcessInspector(root string) *ProcessInspector { return &ProcessInspector{root: root} }

func (p *ProcessInspector) Cwd(pid int) (string, error) {
	return processCwd(p.root, pid)
}
func (p *ProcessInspector) Comm(pid int) (string, error) {
	return processComm(p.root, pid)
}
func (p *ProcessInspector) Argv(pid int) ([]string, error) {
	return processArgv(filepath.Join(p.root, strconv.Itoa(pid), "cmdline"), pid)
}
func (p *ProcessInspector) GroupArgv(pgid int, shellPid int) ([]string, error) {
	recs, err := p.processRecords()
	if err != nil {
		return nil, err
	}
	pid, ok := selectProcessGroupPID(recs, pgid, shellPid)
	if !ok {
		return nil, nil
	}
	return p.Argv(pid)
}

func (p *ProcessInspector) processRecords() ([]processRecord, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	if now.Before(p.cachedRecordsUntil) {
		return p.cachedRecords, nil
	}
	recs, err := readProcRecords(p.root)
	if err != nil {
		return nil, err
	}
	p.cachedRecords = recs
	p.cachedRecordsUntil = now.Add(processRecordCacheTTL)
	return recs, nil
}

// ProcessArgv returns argv from /proc/<pid>/cmdline.
func ProcessArgv(pid int) ([]string, error) {
	return processArgv(filepath.Join("/proc", strconv.Itoa(pid), "cmdline"), pid)
}

func processArgv(path string, pid int) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	data = bytes.TrimRight(data, "\x00")
	if len(data) == 0 {
		return nil, fmt.Errorf("process argv: empty argv for pid %d", pid)
	}
	parts := bytes.Split(data, []byte{0})
	argv := make([]string, len(parts))
	for i, part := range parts {
		argv[i] = string(part)
	}
	return argv, nil
}

type processRecord struct{ pid, pgrp int }

// ProcessGroupArgv returns argv for a restorable foreground process in pgid.
// A bare pane shell (the only process in its foreground group) returns nil argv.
func ProcessGroupArgv(pgid int, shellPid int) ([]string, error) {
	recs, err := readProcRecords("/proc")
	if err != nil {
		return nil, err
	}
	pid, ok := selectProcessGroupPID(recs, pgid, shellPid)
	if !ok {
		return nil, nil
	}
	return ProcessArgv(pid)
}

func selectProcessGroupPID(recs []processRecord, pgid int, shellPid int) (int, bool) {
	if pgid <= 0 {
		return 0, false
	}
	matches := make([]int, 0)
	for _, r := range recs {
		if r.pgrp == pgid && r.pid != shellPid {
			matches = append(matches, r.pid)
		}
	}
	if len(matches) == 0 {
		return 0, false
	}
	sort.Ints(matches)
	return matches[0], true
}

func readProcRecords(root string) ([]processRecord, error) {
	ents, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	var recs []processRecord
	for _, ent := range ents {
		pid, err := strconv.Atoi(ent.Name())
		if err != nil {
			continue
		}
		data, err := os.ReadFile(filepath.Join(root, ent.Name(), "stat"))
		if err != nil {
			continue
		}
		pgrp, err := parseStatPgrp(string(data))
		if err != nil {
			continue
		}
		recs = append(recs, processRecord{pid: pid, pgrp: pgrp})
	}
	return recs, nil
}

func parseStatPgrp(stat string) (int, error) {
	end := strings.LastIndex(stat, ")")
	if end < 0 || end+2 >= len(stat) {
		return 0, fmt.Errorf("invalid stat")
	}
	fields := strings.Fields(stat[end+2:])
	if len(fields) < 3 {
		return 0, fmt.Errorf("invalid stat")
	}
	return strconv.Atoi(fields[2])
}
