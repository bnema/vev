// Package protocol defines vev's typed, transport-neutral client/daemon
// contract. Binary framing and codecs live outside this package.
package protocol

import (
	"errors"
	"fmt"
	"math"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
)

// MaxOutputDataLen is the largest decoded terminal byte stream accepted by one
// output publication in protocol version 37.
const MaxOutputDataLen = (16 << 20) - 55

var (
	ErrInvalidSize       = errors.New("ports: invalid terminal size")
	ErrInvalidOutput     = errors.New("ports: invalid output")
	ErrInvalidAck        = errors.New("ports: invalid ack")
	ErrInvalidNavigation = errors.New("ports: invalid navigation")
)

type Resize struct {
	Size        domain.Size
	PixelWidth  int
	PixelHeight int
}

func (m Resize) Geometry() domain.Geometry {
	return domain.Geometry{Size: m.Size, PixelWidth: m.PixelWidth, PixelHeight: m.PixelHeight}.NormalizePixels()
}

type Theme struct {
	HasForeground bool
	Foreground    renderer.RGB
	HasBackground bool
	Background    renderer.RGB
	TrueColor     bool
	SchemeKnown   bool
	Light         bool
	PaletteKnown  uint16
	Palette       [16]renderer.RGB
}

type ImagePush struct {
	InputSeq uint64
	Mime     string
	Data     []byte
}

type Ack struct {
	Epoch uint64
	State uint64
}

type Output struct {
	Epoch        uint64
	Base         uint64
	New          uint64
	Echo         uint64
	ViewRevision uint64
	Size         domain.Size
	Full         bool
	Data         []byte
}

func ValidateOutput(m Output) error {
	if m.Epoch == 0 {
		return ErrInvalidOutput
	}
	if m.New == 0 {
		if m.Base != 0 || m.Full {
			return ErrInvalidOutput
		}
	} else if (m.Base == 0 && !m.Full) || (m.Base != 0 && (m.Full || m.New != m.Base+1)) {
		return ErrInvalidOutput
	}
	if err := ValidateSize(m.Size); err != nil {
		return fmt.Errorf("%w: size", ErrInvalidOutput)
	}
	if uint64(len(m.Data)) > math.MaxUint32 || len(m.Data) > MaxOutputDataLen {
		return ErrInvalidOutput
	}
	return nil
}

func ValidateAck(m Ack) error {
	if m.Epoch == 0 {
		return ErrInvalidAck
	}
	return nil
}

func validateRemoteTarget(target domain.RemoteSessionTarget) error {
	if len(target.Endpoint) > math.MaxUint16 || len(target.DisplayOrigin) > math.MaxUint16 || len(target.SessionName) > math.MaxUint16 || len(target.LiveTabID) > math.MaxUint16 || len(target.StoppedTab.RawName) > math.MaxUint16 {
		return errors.New("remote target string too long")
	}
	return target.Validate()
}
