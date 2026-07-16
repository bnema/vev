//go:build darwin && arm64

package platform

import (
	"bytes"
	"fmt"
	"runtime"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

const (
	ctlKern                = 1
	kernProc               = 14
	kernProcAll            = 0
	kernProcPID            = 1
	kernProcArgs2          = 49
	procInfoCallPIDInfo    = 2
	procPIDVnodePathInfo   = 9
	darwinKinfoProcSize    = 0x288
	darwinVnodePathInfoLen = 2352
)

// ProcessInspector implements ports.ProcessInspector using Darwin sysctl and
// proc_pidinfo interfaces.
type ProcessInspector struct {
	mu                 sync.Mutex
	cachedRecords      []processRecord
	cachedRecordsUntil time.Time
}

// NewProcessInspector returns a Darwin process inspector.
func NewProcessInspector() *ProcessInspector { return &ProcessInspector{} }

func (p *ProcessInspector) Cwd(pid int) (string, error)    { return ProcessCwd(pid) }
func (p *ProcessInspector) Comm(pid int) (string, error)   { return ProcessComm(pid) }
func (p *ProcessInspector) Argv(pid int) ([]string, error) { return ProcessArgv(pid) }
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
	recs, err := readDarwinProcessRecords()
	if err != nil {
		return nil, err
	}
	p.cachedRecords = recs
	p.cachedRecordsUntil = now.Add(processRecordCacheTTL)
	return recs, nil
}

// ProcessCwd returns the current working directory from proc_pidinfo.
func ProcessCwd(pid int) (string, error) {
	if pid <= 0 {
		return "", fmt.Errorf("process cwd: invalid pid %d", pid)
	}
	var info procVnodePathInfo
	n, _, errno := syscall.Syscall6(
		syscall.SYS_PROC_INFO,
		procInfoCallPIDInfo,
		uintptr(pid),
		procPIDVnodePathInfo,
		0,
		uintptr(unsafe.Pointer(&info)),
		unsafe.Sizeof(info),
	)
	runtime.KeepAlive(&info)
	if errno != 0 {
		return "", fmt.Errorf("process cwd: proc_pidinfo pid %d: %w", pid, errno)
	}
	if n != unsafe.Sizeof(info) {
		return "", fmt.Errorf("process cwd: proc_pidinfo pid %d returned %d bytes; want %d", pid, n, unsafe.Sizeof(info))
	}
	path := info.Cdir.Path[:]
	if end := bytes.IndexByte(path, 0); end >= 0 {
		path = path[:end]
	}
	if len(path) == 0 {
		return "", fmt.Errorf("process cwd: empty cwd for pid %d", pid)
	}
	return string(path), nil
}

// ProcessComm returns the p_comm field from KERN_PROC_PID data.
func ProcessComm(pid int) (string, error) {
	if pid <= 0 {
		return "", fmt.Errorf("process comm: invalid pid %d", pid)
	}
	data, err := rawSysctl([]int32{ctlKern, kernProc, kernProcPID, int32(pid)})
	if err != nil {
		return "", fmt.Errorf("process comm: sysctl pid %d: %w", pid, err)
	}
	if len(data) != darwinKinfoProcSize {
		return "", fmt.Errorf("process comm: sysctl pid %d returned %d bytes; want %d", pid, len(data), darwinKinfoProcSize)
	}
	proc := *(*kinfoProc)(unsafe.Pointer(&data[0]))
	runtime.KeepAlive(data)
	comm := proc.Proc.Comm[:]
	if end := bytes.IndexByte(comm, 0); end >= 0 {
		comm = comm[:end]
	}
	if len(comm) == 0 {
		return "", fmt.Errorf("process comm: empty comm for pid %d", pid)
	}
	return string(comm), nil
}

// ProcessArgv returns argv from KERN_PROCARGS2.
func ProcessArgv(pid int) ([]string, error) {
	if pid <= 0 {
		return nil, fmt.Errorf("process argv: invalid pid %d", pid)
	}
	data, err := rawSysctl([]int32{ctlKern, kernProcArgs2, int32(pid)})
	if err != nil {
		return nil, fmt.Errorf("process argv: sysctl pid %d: %w", pid, err)
	}
	argv, err := parseKernProcArgs2(data)
	if err != nil {
		return nil, fmt.Errorf("process argv: pid %d: %w", pid, err)
	}
	return argv, nil
}

// ProcessGroupArgv returns argv for a restorable foreground process in pgid.
// A bare pane shell (the only process in its foreground group) returns nil argv.
func ProcessGroupArgv(pgid int, shellPid int) ([]string, error) {
	return NewProcessInspector().GroupArgv(pgid, shellPid)
}

func readDarwinProcessRecords() ([]processRecord, error) {
	data, err := rawSysctl([]int32{ctlKern, kernProc, kernProcAll})
	if err != nil {
		return nil, err
	}
	if len(data)%darwinKinfoProcSize != 0 {
		return nil, fmt.Errorf("KERN_PROC_ALL returned %d bytes, not a multiple of %d", len(data), darwinKinfoProcSize)
	}
	recs := make([]processRecord, 0, len(data)/darwinKinfoProcSize)
	for len(data) > 0 {
		proc := (*kinfoProc)(unsafe.Pointer(&data[0]))
		recs = append(recs, processRecord{pid: int(proc.Proc.Pid), pgrp: int(proc.Eproc.Pgid)})
		data = data[darwinKinfoProcSize:]
	}
	runtime.KeepAlive(data)
	return recs, nil
}

// rawSysctl reads a binary sysctl MIB, retrying if a growing result races the
// size query. It is the stdlib syscall equivalent of sysctl(3).
func rawSysctl(mib []int32) ([]byte, error) {
	for {
		var size uintptr
		if err := sysctl(mib, nil, &size); err != nil {
			return nil, err
		}
		if size == 0 {
			return nil, nil
		}
		buf := make([]byte, size)
		n := size
		if err := sysctl(mib, &buf[0], &n); err != nil {
			if err == syscall.ENOMEM {
				continue
			}
			return nil, err
		}
		return buf[:n], nil
	}
}

func sysctl(mib []int32, old *byte, oldlen *uintptr) error {
	_, _, errno := syscall.Syscall6(
		syscall.SYS___SYSCTL,
		uintptr(unsafe.Pointer(&mib[0])),
		uintptr(len(mib)),
		uintptr(unsafe.Pointer(old)),
		uintptr(unsafe.Pointer(oldlen)),
		0,
		0,
	)
	runtime.KeepAlive(mib)
	if errno != 0 {
		return errno
	}
	return nil
}

// The following Darwin/arm64 ABI layouts are derived from Apple XNU
// bsd/sys/proc.h and bsd/sys/proc_info.h, via golang.org/x/sys/unix
// ztypes_darwin_arm64.go (BSD-3-Clause). Pointer fields are uintptr so this
// package has no dependency on x/sys and never lets kernel pointers escape.
type kinfoProc struct {
	Proc  externProc
	Eproc eproc
}

type eproc struct {
	Paddr   uintptr
	Sess    uintptr
	Pcred   pcred
	Ucred   ucred
	Vm      vmspace
	Ppid    int32
	Pgid    int32
	Jobc    int16
	Tdev    int32
	Tpgid   int32
	Tsess   uintptr
	Wmesg   [8]byte
	Xsize   int32
	Xrssize int16
	Xccount int16
	Xswrss  int16
	Flag    int32
	Login   [12]byte
	Spare   [4]int32
	_       [4]byte
}

type externProc struct {
	Un         [16]byte
	Vmspace    uintptr
	Sigacts    uintptr
	Flag       int32
	Stat       int8
	Pid        int32
	Oppid      int32
	Dupfd      int32
	_          [4]byte
	UserStack  uintptr
	ExitThread uintptr
	Debugger   int32
	Sigwait    int32
	Estcpu     uint32
	Cpticks    int32
	Pctcpu     uint32
	_          [4]byte
	Wchan      uintptr
	Wmesg      uintptr
	Swtime     uint32
	Slptime    uint32
	Realtimer  itimerval
	Rtime      timeval
	Uticks     uint64
	Sticks     uint64
	Iticks     uint64
	Traceflag  int32
	_          [4]byte
	Tracep     uintptr
	Siglist    int32
	_          [4]byte
	Textvp     uintptr
	Holdcnt    int32
	Sigmask    uint32
	Sigignore  uint32
	Sigcatch   uint32
	Priority   uint8
	Usrpri     uint8
	Nice       int8
	Comm       [17]byte
	_          [4]byte
	Pgrp       uintptr
	Addr       uintptr
	Xstat      uint16
	Acflag     uint16
	_          [4]byte
	Ru         uintptr
}

type itimerval struct {
	Interval timeval
	Value    timeval
}

type timeval struct {
	Sec  int64
	Usec int32
	_    [4]byte
}

type pcred struct {
	Lock   [72]int8
	Ucred  uintptr
	Ruid   uint32
	Svuid  uint32
	Rgid   uint32
	Svgid  uint32
	Refcnt int32
	_      [4]byte
}

type ucred struct {
	Ref     int32
	UID     uint32
	Ngroups int16
	Groups  [16]uint32
}

type vmspace struct {
	Dummy  int32
	Dummy2 uintptr
	Dummy3 [5]int32
	Dummy4 [3]uintptr
}

type vinfoStat struct {
	Dev           uint32
	Mode          uint16
	Nlink         uint16
	Ino           uint64
	UID           uint32
	GID           uint32
	Atime         int64
	Atimensec     int64
	Mtime         int64
	Mtimensec     int64
	Ctime         int64
	Ctimensec     int64
	Birthtime     int64
	Birthtimensec int64
	Size          int64
	Blocks        int64
	Blksize       int32
	Flags         uint32
	Gen           uint32
	Rdev          uint32
	Qspare        [2]int64
}

type fsid struct{ Val [2]int32 }

type vnodeInfo struct {
	Stat vinfoStat
	Type int32
	Pad  int32
	Fsid fsid
}

type vnodeInfoPath struct {
	Vi   vnodeInfo
	Path [1024]byte
}

type procVnodePathInfo struct {
	Cdir vnodeInfoPath
	Rdir vnodeInfoPath
}

func init() {
	if unsafe.Sizeof(kinfoProc{}) != darwinKinfoProcSize {
		panic(fmt.Sprintf("darwin kinfo_proc ABI size = %d; want %d", unsafe.Sizeof(kinfoProc{}), darwinKinfoProcSize))
	}
	if unsafe.Sizeof(procVnodePathInfo{}) != darwinVnodePathInfoLen {
		panic(fmt.Sprintf("darwin proc_vnodepathinfo ABI size = %d; want %d", unsafe.Sizeof(procVnodePathInfo{}), darwinVnodePathInfoLen))
	}
}
