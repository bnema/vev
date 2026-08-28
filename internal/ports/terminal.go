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
}
