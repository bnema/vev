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

func validWireSize(size domain.Size) bool {
	return ValidateSize(size) == nil
}
