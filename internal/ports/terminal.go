package ports

import (
	"io"

	"github.com/bnema/vev/internal/domain"
)

// Terminal is the client-side controlling terminal.
type Terminal interface {
	EnterRaw() (restore func() error, err error)
	Geometry() (domain.Geometry, error)
	ResizeEvents() <-chan domain.Geometry
	In() io.Reader
	Out() io.Writer
	Flush() error
	// EnableKittyKeyboard pushes the given kitty keyboard protocol flags on
	// the outer terminal's alternate screen. The restore returned by EnterRaw
	// pops them before leaving the alternate screen. Terminals without an
	// outer keyboard return nil and do nothing.
	EnableKittyKeyboard(flags int) error
}
