package daemon

import (
	"fmt"
	"math"
	"unicode/utf8"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/renderer"
)

const (
	maxProxyScreenCells  = 1 << 18
	maxProxyScreenDamage = 1024
	maxProxyScreenSpans  = 4096
)

type proxyScreenDamageCapture struct {
	Damage     []renderer.Damage
	Generation uint64
}

// proxyScreenState is the replayable semantic screen owned by a proxy link.
// frame and scratch are independent frames and equal whenever Apply returns.
// A delta is applied to scratch, the frames are swapped, and the same delta is
// applied to the old frame. This avoids copying a complete frame on deltas.
type proxyScreenState struct {
	frame     renderer.Frame
	scratch   renderer.Frame
	cursorOut ports.ScreenCursor

	damage                 []renderer.Damage
	generation             uint64
	damageFullRedrawSticky bool
	stateNum               uint64
}

func newProxyScreenState(size domain.Size) *proxyScreenState {
	if !validProxyScreenSize(size) {
		return &proxyScreenState{}
	}
	return &proxyScreenState{
		frame:      renderer.NewFrame(size.Cols, size.Rows),
		scratch:    renderer.NewFrame(size.Cols, size.Rows),
		cursorOut:  ports.ScreenCursor{Visible: true},
		damage:     []renderer.Damage{renderer.FullRedraw()},
		generation: 1,
	}
}

// Apply validates the complete semantic update before mutating either frame.
func (s *proxyScreenState) Apply(update ports.ScreenUpdate) error {
	if s == nil {
		return fmt.Errorf("proxy screen state is nil")
	}
	if err := s.validateUpdate(update); err != nil {
		return err
	}

	if update.Kind == ports.ScreenUpdateSnapshot {
		next := renderer.NewFrame(update.Size.Cols, update.Size.Rows)
		nextScratch := renderer.NewFrame(update.Size.Cols, update.Size.Rows)
		applyScreenUpdate(&next, update)
		applyScreenUpdate(&nextScratch, update)
		s.frame, s.scratch = next, nextScratch
	} else {
		applyScreenUpdate(&s.scratch, update)
		s.frame, s.scratch = s.scratch, s.frame
		applyScreenUpdate(&s.scratch, update)
	}
	s.cursorOut = update.Cursor
	if update.NewStateNum != 0 {
		s.stateNum = update.NewStateNum
	}
	s.recordUpdate(update)
	return nil
}

func (s *proxyScreenState) validateUpdate(update ports.ScreenUpdate) error {
	if update.Kind != ports.ScreenUpdateSnapshot && update.Kind != ports.ScreenUpdateDelta {
		return fmt.Errorf("proxy screen: unknown update kind %d", update.Kind)
	}
	if !validProxyScreenSize(update.Size) {
		return fmt.Errorf("proxy screen: invalid size %dx%d", update.Size.Cols, update.Size.Rows)
	}
	if update.Kind == ports.ScreenUpdateDelta {
		if s.frame.Width != update.Size.Cols || s.frame.Height != update.Size.Rows {
			return fmt.Errorf("proxy screen: delta size %dx%d does not match %dx%d", update.Size.Cols, update.Size.Rows, s.frame.Width, s.frame.Height)
		}
	} else if update.Scroll != nil {
		return fmt.Errorf("proxy screen: snapshot cannot contain scroll")
	}
	if update.Cursor.Row >= uint16(update.Size.Rows) || update.Cursor.Col >= uint16(update.Size.Cols) {
		return fmt.Errorf("proxy screen: cursor %d,%d outside %dx%d", update.Cursor.Col, update.Cursor.Row, update.Size.Cols, update.Size.Rows)
	}
	if !update.Cursor.StyleSet && update.Cursor.Style != 0 {
		return fmt.Errorf("proxy screen: cursor style is not set")
	}
	if update.Cursor.StyleSet && update.Cursor.Style > 6 {
		return fmt.Errorf("proxy screen: cursor style is out of range")
	}
	if len(update.Spans) > maxProxyScreenSpans {
		return fmt.Errorf("proxy screen: too many spans")
	}
	if update.Kind == ports.ScreenUpdateSnapshot {
		if len(update.Spans) != update.Size.Rows {
			return fmt.Errorf("proxy screen: snapshot must cover every row")
		}
		for y, span := range update.Spans {
			if int(span.Y) != y || span.X != 0 || len(span.Cells) != update.Size.Cols {
				return fmt.Errorf("proxy screen: snapshot span %d is not a complete row", y)
			}
		}
	}
	if err := s.validateStateNumbers(update); err != nil {
		return err
	}
	if scroll := update.Scroll; scroll != nil {
		if scroll.Height == 0 || scroll.Count == 0 || scroll.Count >= scroll.Height {
			return fmt.Errorf("proxy screen: invalid scroll %d,%d,%d", scroll.Top, scroll.Height, scroll.Count)
		}
		if uint64(scroll.Top)+uint64(scroll.Height) > uint64(update.Size.Rows) {
			return fmt.Errorf("proxy screen: scroll outside screen")
		}
	}
	var previousY uint16
	var previousEnd uint32
	for i, span := range update.Spans {
		if len(span.Cells) == 0 {
			return fmt.Errorf("proxy screen: span %d is empty", i)
		}
		if uint64(span.Y) >= uint64(update.Size.Rows) || uint64(span.X) >= uint64(update.Size.Cols) ||
			uint64(span.X)+uint64(len(span.Cells)) > uint64(update.Size.Cols) {
			return fmt.Errorf("proxy screen: span %d outside screen", i)
		}
		if i > 0 && (span.Y < previousY || (span.Y == previousY && uint32(span.X) < previousEnd)) {
			return fmt.Errorf("proxy screen: spans overlap or are out of order")
		}
		for _, cell := range span.Cells {
			if cell.Continuation && cell.Rune != 0 || !cell.Continuation && !utf8.ValidRune(cell.Rune) {
				return fmt.Errorf("proxy screen: invalid cell rune")
			}
			if !validProxyScreenStyle(cell.Style) {
				return fmt.Errorf("proxy screen: invalid cell style")
			}
		}
		previousY = span.Y
		previousEnd = uint32(span.X) + uint32(len(span.Cells))
	}
	return nil
}

func (s *proxyScreenState) validateStateNumbers(update ports.ScreenUpdate) error {
	if update.NewStateNum == 0 || update.NewStateNum <= update.BaseStateNum {
		return fmt.Errorf("proxy screen: invalid state transition %d to %d", update.BaseStateNum, update.NewStateNum)
	}
	if update.Kind == ports.ScreenUpdateSnapshot {
		if update.BaseStateNum != 0 {
			return fmt.Errorf("proxy screen: snapshot base state must be zero")
		}
		return nil
	}
	if update.BaseStateNum == 0 || update.NewStateNum != update.BaseStateNum+1 {
		return fmt.Errorf("proxy screen: delta state transition %d to %d is not consecutive", update.BaseStateNum, update.NewStateNum)
	}
	if s.stateNum == 0 || update.BaseStateNum != s.stateNum {
		return fmt.Errorf("proxy screen: delta base state %d does not match %d", update.BaseStateNum, s.stateNum)
	}
	return nil
}

func applyScreenUpdate(frame *renderer.Frame, update ports.ScreenUpdate) {
	if update.Scroll != nil {
		scroll := update.Scroll
		frame.ScrollUp(int(scroll.Top), int(scroll.Top+scroll.Height-1), int(scroll.Count))
	}
	for _, span := range update.Spans {
		copy(frame.Row(int(span.Y))[int(span.X):], span.Cells)
	}
}

func (s *proxyScreenState) recordUpdate(update ports.ScreenUpdate) {
	s.generation++
	if update.Kind == ports.ScreenUpdateSnapshot {
		s.setFullRedraw()
		return
	}
	if s.damageFullRedrawSticky {
		return
	}
	if update.Scroll != nil && len(s.damage) > 0 && !(len(s.damage) == 1 && s.damage[0].Kind == renderer.DamageFullRedraw) {
		s.setFullRedraw()
		return
	}
	needed := len(update.Spans)
	if update.Scroll != nil {
		needed++
	}
	if needed == 0 {
		return
	}
	if needed > maxProxyScreenDamage-len(s.damage) {
		s.setFullRedraw()
		return
	}
	if len(s.damage) == 1 && s.damage[0].Kind == renderer.DamageFullRedraw {
		s.damage = s.damage[:0]
	}
	if update.Scroll != nil {
		s.damage = append(s.damage, renderer.Damage{
			Kind:   renderer.DamageScrollUp,
			X:      0,
			Y:      int(update.Scroll.Top),
			Width:  s.frame.Width,
			Height: int(update.Scroll.Height),
			Count:  int(update.Scroll.Count),
		})
	}
	for _, span := range update.Spans {
		s.damage = append(s.damage, renderer.Damage{
			Kind:   renderer.DamageText,
			X:      int(span.X),
			Y:      int(span.Y),
			Width:  len(span.Cells),
			Height: 1,
			Count:  1,
		})
	}
}

func (s *proxyScreenState) setFullRedraw() {
	s.damage = []renderer.Damage{renderer.FullRedraw()}
	s.damageFullRedrawSticky = true
}

// CaptureDamage returns an owned view of pending damage at one generation.
func (s *proxyScreenState) CaptureDamage() proxyScreenDamageCapture {
	if s == nil {
		return proxyScreenDamageCapture{}
	}
	return proxyScreenDamageCapture{
		Damage:     append([]renderer.Damage(nil), s.damage...),
		Generation: s.generation,
	}
}

// AcknowledgeDamage consumes damage only for the exact generation captured.
// A stale acknowledgement forces a sticky full redraw so intervening updates
// cannot be lost.
func (s *proxyScreenState) AcknowledgeDamage(generation uint64) bool {
	if s == nil {
		return false
	}
	if generation != s.generation {
		s.generation++
		s.setFullRedraw()
		return false
	}
	s.damage = s.damage[:0]
	s.damageFullRedrawSticky = false
	return true
}

// CaptureInto updates dst from the live frame. Safe scroll/span damage is
// replayed incrementally; snapshots, uncertain damage, and dimensions use a
// complete clone.
func (s *proxyScreenState) CaptureInto(dst *renderer.Frame) {
	if s == nil || dst == nil || !validProxyFrame(*s) {
		return
	}
	if dst.Width != s.frame.Width || dst.Height != s.frame.Height || dst.Validate() != nil {
		*dst = s.frame.Clone()
		return
	}
	if len(s.damage) == 0 {
		return
	}
	for _, damage := range s.damage {
		switch damage.Kind {
		case renderer.DamageScrollUp:
			if damage.X != 0 || damage.Width != s.frame.Width || damage.Y < 0 || damage.Height <= 0 || damage.Count <= 0 || damage.Count > damage.Height || damage.Y+damage.Height > s.frame.Height {
				*dst = s.frame.Clone()
				return
			}
			dst.ScrollUp(damage.Y, damage.Y+damage.Height-1, damage.Count)
		case renderer.DamageText, renderer.DamageClear:
			if damage.X < 0 || damage.Y < 0 || damage.Width <= 0 || damage.Height <= 0 || damage.X+damage.Width > s.frame.Width || damage.Y+damage.Height > s.frame.Height {
				*dst = s.frame.Clone()
				return
			}
			for y := damage.Y; y < damage.Y+damage.Height; y++ {
				copy(dst.Row(y)[damage.X:damage.X+damage.Width], s.frame.Row(y)[damage.X:damage.X+damage.Width])
			}
		default:
			*dst = s.frame.Clone()
			return
		}
	}
}

// ResizePlaceholder changes geometry while preserving the overlapping logical
// cells. It deliberately requests a full redraw and resets the state chain.
func (s *proxyScreenState) ResizePlaceholder(size domain.Size) bool {
	if s == nil || !validProxyScreenSize(size) {
		return false
	}
	if s.frame.Width == size.Cols && s.frame.Height == size.Rows {
		return false
	}
	next := renderer.NewFrame(size.Cols, size.Rows)
	nextScratch := renderer.NewFrame(size.Cols, size.Rows)
	copyFrameOverlap(&next, s.frame)
	copyFrameOverlap(&nextScratch, s.frame)
	s.frame, s.scratch = next, nextScratch
	if int(s.cursorOut.Row) >= size.Rows {
		s.cursorOut.Row = uint16(size.Rows - 1)
	}
	if int(s.cursorOut.Col) >= size.Cols {
		s.cursorOut.Col = uint16(size.Cols - 1)
	}
	s.stateNum = 0
	s.generation++
	s.setFullRedraw()
	return true
}

func copyFrameOverlap(dst *renderer.Frame, src renderer.Frame) {
	width := src.Width
	if dst.Width < width {
		width = dst.Width
	}
	height := src.Height
	if dst.Height < height {
		height = dst.Height
	}
	for y := 0; y < height; y++ {
		copy(dst.Row(y)[:width], src.Row(y)[:width])
	}
}

func validProxyScreenSize(size domain.Size) bool {
	if size.Cols <= 0 || size.Rows <= 0 || size.Cols > math.MaxUint16 || size.Rows > math.MaxUint16 {
		return false
	}
	return size.Rows <= maxProxyScreenCells/size.Cols
}

func validProxyFrame(s proxyScreenState) bool {
	return s.frame.Validate() == nil && s.scratch.Validate() == nil
}

func validProxyScreenStyle(style renderer.Style) bool {
	if style.Attrs&^(renderer.AttrDim|renderer.AttrUnderline|renderer.AttrBlink|renderer.AttrStrikethrough) != 0 || style.UnderlineStyle > renderer.UnderlineDashed {
		return false
	}
	if style.HasForegroundRGB {
		if style.Foreground < -1 || style.Foreground > math.MaxInt16 {
			return false
		}
	} else if style.Foreground < -1 || style.Foreground > math.MaxUint8 || style.ForegroundRGB != (renderer.RGB{}) {
		return false
	}
	if style.HasBackgroundRGB {
		if style.Background < -1 || style.Background > math.MaxInt16 {
			return false
		}
	} else if style.Background < -1 || style.Background > math.MaxUint8 || style.BackgroundRGB != (renderer.RGB{}) {
		return false
	}
	if style.HasUnderlineColorRGB {
		return !style.HasUnderlineColor && style.UnderlineColor >= -1 && style.UnderlineColor <= math.MaxInt16
	}
	if style.HasUnderlineColor {
		return style.UnderlineColor >= 0 && style.UnderlineColor <= math.MaxUint8 && style.UnderlineColorRGB == (renderer.RGB{})
	}
	return (style.UnderlineColor == 0 || style.UnderlineColor == -1) && style.UnderlineColorRGB == (renderer.RGB{})
}
