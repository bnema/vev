//go:build linux

package platform

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// real I/O: exercises OS process inspection, subprocesses or file-backed terminal I/O.
func TestProcessComm_CurrentProcess(t *testing.T) {
	comm, err := ProcessComm(os.Getpid())
	require.NoError(t, err)
	require.NotEmpty(t, comm)
	require.NotContains(t, comm, "\n")
}

// real I/O: exercises OS process inspection, subprocesses or file-backed terminal I/O.
func TestProcessComm_InvalidPID(t *testing.T) {
	comm, err := ProcessComm(0)
	require.Error(t, err)
	require.Empty(t, comm)
}
