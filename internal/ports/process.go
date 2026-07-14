package ports

// ProcessInspector reads process metadata for daemon snapshot/persistence use.
// Implementations may live in platform packages; usecases depend on this port.
type ProcessInspector interface {
	Cwd(pid int) (string, error)
	Comm(pid int) (string, error)
	Argv(pid int) ([]string, error)
	GroupArgv(pgid int, shellPid int) ([]string, error)
}
