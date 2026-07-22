//go:build darwin

package snapshot

import "syscall"

func directoryCookie(dirent *syscall.Dirent) int64 { return int64(dirent.Seekoff) }
