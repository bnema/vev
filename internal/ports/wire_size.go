package ports

import (
	"math"

	"github.com/bnema/vev/internal/domain"
)

// MaxWireScreenCells bounds every terminal size carried by the final wire
// protocol. The uint16 dimension limit is checked separately.
const MaxWireScreenCells = 1 << 18

// ValidateSize validates a terminal size before a codec allocates or mutates
// state.
func ValidateSize(size domain.Size) error {
	if size.Cols <= 0 || size.Rows <= 0 || size.Cols > math.MaxUint16 || size.Rows > math.MaxUint16 {
		return ErrInvalidSize
	}
	if size.Rows > MaxWireScreenCells/size.Cols {
		return ErrInvalidSize
	}
	return nil
}

// ValidateGeometry validates cell geometry and its optional uint16 pixel pair.
func ValidateGeometry(geometry domain.Geometry) error {
	if err := ValidateSize(geometry.Size); err != nil {
		return err
	}
	if geometry.PixelWidth == 0 && geometry.PixelHeight == 0 {
		return nil
	}
	if geometry.PixelWidth <= 0 || geometry.PixelHeight <= 0 || geometry.PixelWidth > math.MaxUint16 || geometry.PixelHeight > math.MaxUint16 {
		return ErrInvalidSize
	}
	return nil
}
