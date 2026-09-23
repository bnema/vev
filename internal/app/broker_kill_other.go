//go:build !linux

package app

import (
	"context"
	"errors"
)

func runKillBroker(context.Context) error {
	return errors.New("vev: stopping the broker requires Linux peer credentials")
}
