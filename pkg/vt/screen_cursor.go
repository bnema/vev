package vt

import (
	"strconv"
	"strings"

	renderer "github.com/bnema/vev/pkg/vtcore"
)

type cursorState struct {
	row        int
	col        int
	style      renderer.Style
	originMode bool
	insertMode bool
	saved      bool
}

func (s *Screen) cursorReportRow() int {
	if s.originMode {
		return clamp(s.Row, s.cursorMinRow(), s.cursorMaxRow()) - s.cursorMinRow() + 1
	}
	return s.Row + 1
}

func (s *Screen) cursorMinRow() int {
	if s.originMode {
		return clamp(s.scrollTop, 0, s.Frame.Height-1)
	}
	return 0
}

func (s *Screen) cursorMaxRow() int {
	if s.originMode {
		return clamp(s.scrollBottom, 0, s.Frame.Height-1)
	}
	return max(s.Frame.Height-1, 0)
}

func (s *Screen) addressedRow(row int) int {
	if s.originMode {
		return clamp(s.scrollTop+row-1, s.cursorMinRow(), s.cursorMaxRow())
	}
	return clamp(row-1, 0, s.Frame.Height-1)
}

func (s *Screen) addressCursor(row, col int) {
	s.Row = s.addressedRow(row)
	s.Col = clamp(col-1, 0, s.Frame.Width-1)
}

func (s *Screen) homeCursor() {
	s.addressCursor(1, 1)
}

func (s *Screen) applyCursorStyle(params string) {
	if strings.HasPrefix(params, ">") || !strings.HasSuffix(params, " ") {
		return
	}
	styleParam := strings.TrimSuffix(params, " ")
	style := 0
	if styleParam != "" {
		v, err := strconv.Atoi(styleParam)
		if err != nil {
			return
		}
		style = v
	}
	if style < 0 || style > 6 {
		return
	}
	s.cursorStyle = style
	s.cursorStyleSet = true
}

func (s *Screen) saveCursor() {
	s.savedCursor = cursorState{row: s.Row, col: s.Col, style: s.Style, originMode: s.originMode, insertMode: s.insertMode, saved: true}
}

func (s *Screen) restoreCursor() {
	if !s.savedCursor.saved {
		return
	}
	s.Row = clamp(s.savedCursor.row, 0, s.Frame.Height-1)
	s.Col = clamp(s.savedCursor.col, 0, s.Frame.Width-1)
	s.Style = s.savedCursor.style
	s.originMode = s.savedCursor.originMode
	s.insertMode = s.savedCursor.insertMode
}
