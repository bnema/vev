package ports

import (
	"io"
	"strings"

	"github.com/bnema/vev/internal/domain"
)

// Terminal is the CLIENT-side controlling terminal.
type Terminal interface {
	EnterRaw() (restore func() error, err error)
	Size() (domain.Size, error)
	ResizeEvents() <-chan domain.Size
	In() io.Reader
	Out() io.Writer
	Flush() error
}

// DetectTrueColor reports whether TERM/COLORTERM advertise direct color support.
func DetectTrueColor(termEnv, colorTerm string) bool {
	switch strings.ToLower(strings.TrimSpace(colorTerm)) {
	case "truecolor", "24bit":
		return true
	}

	termEnv = strings.ToLower(strings.TrimSpace(termEnv))
	return termEnv == "xterm-direct" || strings.HasSuffix(termEnv, "-direct")
}
