//go:build linux

package app

func selfExePath() (string, error) {
	return "/proc/self/exe", nil
}
