//go:build linux

package snapshot

import "syscall"

func directoryCookie(dirent *syscall.Dirent) int64 { return dirent.Off }
