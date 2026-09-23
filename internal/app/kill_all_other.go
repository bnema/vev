//go:build !linux

package app

import (
	"context"
	"errors"
	"io"
)

func runKillAll(context.Context, io.Writer) error {
	return errors.New("vev: `kill --all` requires Linux /proc")
}
