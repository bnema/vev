package daemon

import (
	"errors"

	"github.com/bnema/vev/internal/ports"
)

var errProcessInspectionUnavailable = errors.New("process inspection unavailable")

// cwdOverrideInspector replaces only Cwd on top of an optional base inspector.
// With a nil base, Comm, Argv, and GroupArgv report the inspection as
// unavailable, matching a daemon that was given only a cwd reader.
type cwdOverrideInspector struct {
	base ports.ProcessInspector
	cwd  func(int) (string, error)
}

func (i cwdOverrideInspector) Cwd(pid int) (string, error) { return i.cwd(pid) }

func (i cwdOverrideInspector) Comm(pid int) (string, error) {
	if i.base == nil {
		return "", errProcessInspectionUnavailable
	}
	return i.base.Comm(pid)
}

func (i cwdOverrideInspector) Argv(pid int) ([]string, error) {
	if i.base == nil {
		return nil, errProcessInspectionUnavailable
	}
	return i.base.Argv(pid)
}

func (i cwdOverrideInspector) GroupArgv(pgid, shellPid int) ([]string, error) {
	if i.base == nil {
		return nil, errProcessInspectionUnavailable
	}
	return i.base.GroupArgv(pgid, shellPid)
}

// unwrapCwdOverride returns the inspector beneath a previous cwd override so
// repeated overrides do not stack.
func unwrapCwdOverride(ins ports.ProcessInspector) ports.ProcessInspector {
	if o, ok := ins.(cwdOverrideInspector); ok {
		return o.base
	}
	return ins
}

// hasProcessDetails reports whether Comm, Argv, and GroupArgv are backed by a
// real inspector rather than a cwd-only override.
func (d *Daemon) hasProcessDetails() bool {
	if o, ok := d.proc.(cwdOverrideInspector); ok {
		return o.base != nil
	}
	return d.proc != nil
}
