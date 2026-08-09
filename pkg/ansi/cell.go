package ansi

import "github.com/bnema/vev/pkg/vtcore"

// Cell is retained as an alias while vev consumers move to vtcore. New
// frontend-neutral code should import github.com/bnema/vev/pkg/vtcore.
type Cell = vtcore.Cell

var BlankCell = vtcore.BlankCell
