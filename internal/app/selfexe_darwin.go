//go:build darwin

package app

import "os"

func selfExePath() (string, error) {
	return os.Executable()
}
