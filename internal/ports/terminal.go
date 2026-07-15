package ports

import (
	"io"

	"github.com/bnema/vev/internal/domain"
)

// Terminal is the CLIENT-side controlling terminal.
type Terminal interface {
	EnterRaw() (restore func() error, err error)
	Size() (domain.Size, error)
	ResizeEvents() <-chan domain.Size
	QueryColors() error
	In() io.Reader
	Out() io.Writer
	Flush() error
}
