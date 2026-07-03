package platform

import (
	"fmt"
	"os"
)

// ProcessCwd returns the current working directory for a Linux process.
func ProcessCwd(pid int) (string, error) {
	return os.Readlink(fmt.Sprintf("/proc/%d/cwd", pid))
}
