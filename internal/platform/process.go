package platform

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"time"
)

const processRecordCacheTTL = 250 * time.Millisecond

type processRecord struct{ pid, pgrp int }

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

// parseKernProcArgs2 parses the KERN_PROCARGS2 buffer returned by Darwin.
// The layout is int argc, executable path, zero padding, argv, then envp.
func parseKernProcArgs2(data []byte) ([]string, error) {
	const argcSize = 4
	if len(data) < argcSize {
		return nil, fmt.Errorf("KERN_PROCARGS2: truncated argc")
	}
	argc := int(binary.LittleEndian.Uint32(data[:argcSize]))
	if argc <= 0 {
		return nil, fmt.Errorf("KERN_PROCARGS2: invalid argc %d", argc)
	}

	rest := data[argcSize:]
	execEnd := bytes.IndexByte(rest, 0)
	if execEnd < 0 {
		return nil, fmt.Errorf("KERN_PROCARGS2: unterminated executable path")
	}
	rest = rest[execEnd+1:]
	for len(rest) > 0 && rest[0] == 0 {
		rest = rest[1:]
	}

	argv := make([]string, 0, argc)
	for len(argv) < argc {
		end := bytes.IndexByte(rest, 0)
		if end < 0 {
			return nil, fmt.Errorf("KERN_PROCARGS2: truncated argv")
		}
		argv = append(argv, string(rest[:end]))
		rest = rest[end+1:]
	}
	return argv, nil
}
