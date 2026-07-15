package vt

import (
	"strconv"
	"strings"

	"github.com/bnema/vev/pkg/renderer"
)

func (s *Screen) consumeCSI(data []byte) (consumed int, partial bool) {
	end := -1
	for i := 2; i < len(data); i++ {
		if data[i] >= '@' && data[i] <= '~' {
			end = i
			break
		}
	}
	if end == -1 {
		return 0, true
	}
	params := string(data[2:end])
	cmd := data[end]
	s.applyCSI(params, cmd)
	return end + 1, false
}

func (s *Screen) applyCSI(params string, cmd byte) {
	private := strings.HasPrefix(params, "?")
	parts := s.parseCSIInts(params)
	switch cmd {
	case 'c':
		switch params {
		case "", "0":
			s.respond([]byte("\x1b[?6c"))
		case ">", ">0":
			s.respond([]byte("\x1b[>0;0;0c"))
		}
	case 'n':
		switch firstPositive(parts, 0) {
		case 5:
			if !private {
				s.respond([]byte("\x1b[0n"))
			}
		case 996:
			if private {
				if report := s.colorSchemeReport(); report != nil {
					s.respond(report)
				}
			}
		case 6:
			resp := make([]byte, 0, 16)
			resp = append(resp, "\x1b["...)
			if private {
				resp = append(resp, '?')
			}
			resp = strconv.AppendInt(resp, int64(s.cursorReportRow()), 10)
			resp = append(resp, ';')
			resp = strconv.AppendInt(resp, int64(s.Col+1), 10)
			resp = append(resp, 'R')
			s.respond(resp)
		}
	case 'p':
		if modeText, ok := strings.CutSuffix(params, "$"); ok {
			if private {
				modeText = strings.TrimPrefix(modeText, "?")
			}
			mode, err := strconv.Atoi(modeText)
			if err != nil {
				return
			}
			state := s.modeReportState(private, mode)
			resp := make([]byte, 0, 16)
			resp = append(resp, "\x1b["...)
			if private {
				resp = append(resp, '?')
			}
			resp = strconv.AppendInt(resp, int64(mode), 10)
			resp = append(resp, ';')
			resp = strconv.AppendInt(resp, int64(state), 10)
			resp = append(resp, "$y"...)
			s.respond(resp)
		}
	case 'm':
		if strings.HasPrefix(params, ">") {
			return
		}
		s.applySGR(params)
	case 'q':
		s.applyCursorStyle(params)
	case 'J':
		mode := 0
		if len(parts) > 0 {
			mode = parts[0]
		}
		s.clearScreenMode(mode)
	case 'K':
		mode := 0
		if len(parts) > 0 {
			mode = parts[0]
		}
		s.clearLineMode(mode)
	case 'S':
		s.scrollUpBy(firstPositive(parts, 1))
	case 'T':
		s.scrollDownBy(firstPositive(parts, 1))
	case 'H', 'f':
		row, col := 1, 1
		if len(parts) > 0 && parts[0] > 0 {
			row = parts[0]
		}
		if len(parts) > 1 && parts[1] > 0 {
			col = parts[1]
		}
		s.addressCursor(row, col)
	case 'A':
		s.Row = clamp(s.Row-firstPositive(parts, 1), s.cursorMinRow(), s.cursorMaxRow())
	case 'B':
		s.Row = clamp(s.Row+firstPositive(parts, 1), s.cursorMinRow(), s.cursorMaxRow())
	case 'C':
		s.Col = clamp(s.Col+firstPositive(parts, 1), 0, s.Frame.Width-1)
	case 'D':
		s.Col = clamp(s.Col-firstPositive(parts, 1), 0, s.Frame.Width-1)
	case 'E':
		s.Row = clamp(s.Row+firstPositive(parts, 1), s.cursorMinRow(), s.cursorMaxRow())
		s.Col = 0
	case 'F':
		s.Row = clamp(s.Row-firstPositive(parts, 1), s.cursorMinRow(), s.cursorMaxRow())
		s.Col = 0
	case 'G':
		col := firstPositive(parts, 1)
		s.Col = clamp(col-1, 0, s.Frame.Width-1)
	case 'd':
		row := firstPositive(parts, 1)
		s.Row = s.addressedRow(row)
	case '@':
		s.insertChars(firstPositive(parts, 1))
	case 'P':
		s.deleteChars(firstPositive(parts, 1))
	case 'L':
		s.insertLines(firstPositive(parts, 1))
	case 'M':
		s.deleteLines(firstPositive(parts, 1))
	case 'r':
		s.setScrollRegion(parts)
	case 's':
		s.saveCursor()
	case 'u':
		if params == "" {
			s.restoreCursor()
		}
	case 'h':
		s.setMode(private, parts, true)
	case 'l':
		s.setMode(private, parts, false)
	}
}

func firstPositive(parts []int, fallback int) int {
	if len(parts) == 0 || parts[0] <= 0 {
		return fallback
	}
	return parts[0]
}

func (s *Screen) modeReportState(private bool, mode int) int {
	if private {
		switch mode {
		case 6:
			return boolModeReportState(s.originMode)
		case 2026:
			return boolModeReportState(s.syncUpdateActive)
		case 2031:
			return boolModeReportState(s.colorSchemeMode)
		}
		return 0
	}
	if mode == 4 {
		return boolModeReportState(s.insertMode)
	}
	return 0
}

func boolModeReportState(enabled bool) int {
	if enabled {
		return 1
	}
	return 2
}

const sgrUnderlineStyleMarker = -1000

func (s *Screen) applySGR(params string) {
	if params == "" {
		s.Style = renderer.DefaultStyle()
		return
	}
	parts := s.parseSGRInts(params)
	for i := 0; i < len(parts); i++ {
		switch parts[i] {
		case 0:
			s.Style = renderer.DefaultStyle()
		case 1:
			s.Style.Bold = true
		case 2:
			s.Style.Attrs |= renderer.AttrDim
		case 3:
			s.Style.Italic = true
		case 4:
			s.Style.Attrs |= renderer.AttrUnderline
			s.Style.UnderlineStyle = renderer.UnderlineSingle
			if i+2 < len(parts) && parts[i+1] == sgrUnderlineStyleMarker {
				underlineStyle := sgrUnderlineStyle(parts[i+2])
				if underlineStyle == renderer.UnderlineNone {
					s.Style.Attrs &^= renderer.AttrUnderline
				} else {
					s.Style.Attrs |= renderer.AttrUnderline
				}
				s.Style.UnderlineStyle = underlineStyle
				i += 2
			}
		case 5, 6:
			s.Style.Attrs |= renderer.AttrBlink
		case 7:
			s.Style.Inverse = true
		case 9:
			s.Style.Attrs |= renderer.AttrStrikethrough
		case 21:
			s.Style.Attrs |= renderer.AttrUnderline
			s.Style.UnderlineStyle = renderer.UnderlineDouble
		case 22:
			s.Style.Bold = false
			s.Style.Attrs &^= renderer.AttrDim
		case 23:
			s.Style.Italic = false
		case 24:
			s.Style.Attrs &^= renderer.AttrUnderline
			s.Style.UnderlineStyle = renderer.UnderlineNone
		case 25:
			s.Style.Attrs &^= renderer.AttrBlink
		case 27:
			s.Style.Inverse = false
		case 29:
			s.Style.Attrs &^= renderer.AttrStrikethrough
		case 30, 31, 32, 33, 34, 35, 36, 37:
			s.Style.Foreground = parts[i] - 30
			s.Style.HasForegroundRGB = false
		case 39:
			s.Style.Foreground = -1
			s.Style.HasForegroundRGB = false
		case 40, 41, 42, 43, 44, 45, 46, 47:
			s.Style.Background = parts[i] - 40
			s.Style.HasBackgroundRGB = false
		case 49:
			s.Style.Background = -1
			s.Style.HasBackgroundRGB = false
		case 90, 91, 92, 93, 94, 95, 96, 97:
			s.Style.Foreground = parts[i] - 90 + 8
			s.Style.HasForegroundRGB = false
		case 100, 101, 102, 103, 104, 105, 106, 107:
			s.Style.Background = parts[i] - 100 + 8
			s.Style.HasBackgroundRGB = false
		case 38:
			if i+2 < len(parts) && parts[i+1] == 5 {
				s.Style.Foreground = parts[i+2]
				s.Style.HasForegroundRGB = false
				i += 2
			} else if i+4 < len(parts) && parts[i+1] == 2 {
				s.Style.Foreground = -1
				s.Style.HasForegroundRGB = true
				s.Style.ForegroundRGB = sgrRGB(parts[i+2], parts[i+3], parts[i+4])
				i += 4
			}
		case 48:
			if i+2 < len(parts) && parts[i+1] == 5 {
				s.Style.Background = parts[i+2]
				s.Style.HasBackgroundRGB = false
				i += 2
			} else if i+4 < len(parts) && parts[i+1] == 2 {
				s.Style.Background = -1
				s.Style.HasBackgroundRGB = true
				s.Style.BackgroundRGB = sgrRGB(parts[i+2], parts[i+3], parts[i+4])
				i += 4
			}
		case 58:
			if i+2 < len(parts) && parts[i+1] == 5 {
				s.Style.UnderlineColor = parts[i+2]
				s.Style.HasUnderlineColor = true
				s.Style.HasUnderlineColorRGB = false
				i += 2
			} else if i+4 < len(parts) && parts[i+1] == 2 {
				s.Style.HasUnderlineColor = false
				s.Style.HasUnderlineColorRGB = true
				s.Style.UnderlineColorRGB = sgrRGB(parts[i+2], parts[i+3], parts[i+4])
				i += 4
			}
		case 59:
			s.Style.HasUnderlineColor = false
			s.Style.HasUnderlineColorRGB = false
		}
	}
}

func sgrRGB(r, g, b int) renderer.RGB {
	return renderer.RGB{
		R: uint8(clamp(r, 0, 255)),
		G: uint8(clamp(g, 0, 255)),
		B: uint8(clamp(b, 0, 255)),
	}
}

func sgrUnderlineStyle(param int) renderer.UnderlineStyle {
	switch param {
	case 0:
		return renderer.UnderlineNone
	case 2:
		return renderer.UnderlineDouble
	case 3:
		return renderer.UnderlineCurly
	case 4:
		return renderer.UnderlineDotted
	case 5:
		return renderer.UnderlineDashed
	default:
		return renderer.UnderlineSingle
	}
}

func (s *Screen) insertChars(n int) {
	if s.Row < 0 || s.Row >= s.Frame.Height || s.Col >= s.Frame.Width || n <= 0 {
		return
	}
	w := s.Frame.Width
	if n > w-s.Col {
		n = w - s.Col
	}
	row := s.Frame.Row(s.Row)
	// A wide left half at Col-1 whose continuation sits at Col will be orphaned
	// by the shift; its repair falls outside the default damage rect.
	leftSplit := s.Col > 0 && row[s.Col].Continuation
	for x := w - 1; x >= s.Col+n; x-- {
		row[x] = row[x-n]
	}
	for x := s.Col; x < s.Col+n; x++ {
		row[x] = renderer.BlankCell()
	}
	s.repairRow(s.Row)
	dmgX := s.Col
	if leftSplit {
		dmgX = s.Col - 1
	}
	s.record(renderer.Damage{Kind: renderer.DamageText, X: dmgX, Y: s.Row, Width: w - dmgX, Height: 1, Count: 1})
}

func (s *Screen) deleteChars(n int) {
	if s.Row < 0 || s.Row >= s.Frame.Height || s.Col >= s.Frame.Width || n <= 0 {
		return
	}
	w := s.Frame.Width
	if n > w-s.Col {
		n = w - s.Col
	}
	row := s.Frame.Row(s.Row)
	// A wide left half at Col-1 whose continuation sits at Col will be orphaned
	// by the shift; its repair falls outside the default damage rect.
	leftSplit := s.Col > 0 && row[s.Col].Continuation
	for x := s.Col; x < w-n; x++ {
		row[x] = row[x+n]
	}
	for x := w - n; x < w; x++ {
		row[x] = renderer.BlankCell()
	}
	s.repairRow(s.Row)
	dmgX := s.Col
	if leftSplit {
		dmgX = s.Col - 1
	}
	s.record(renderer.Damage{Kind: renderer.DamageText, X: dmgX, Y: s.Row, Width: w - dmgX, Height: 1, Count: 1})
}

func (s *Screen) insertLines(n int) {
	if s.Row < s.scrollTop || s.Row > s.scrollBottom || n <= 0 {
		return
	}
	s.scrollDownRegion(s.Row, s.scrollBottom, n)
}

func (s *Screen) deleteLines(n int) {
	if s.Row < s.scrollTop || s.Row > s.scrollBottom || n <= 0 {
		return
	}
	s.scrollUpRegion(s.Row, s.scrollBottom, n)
}

func (s *Screen) clearScreenMode(mode int) {
	s.clampCursor()
	switch mode {
	case 1:
		for y := range min(s.Row+1, s.Frame.Height) {
			end := s.Frame.Width
			if y == s.Row {
				end = min(s.Col+1, s.Frame.Width)
			}
			s.clearRow(y, 0, end)
		}
		s.record(renderer.Damage{Kind: renderer.DamageClear, X: 0, Y: 0, Width: s.Frame.Width, Height: min(s.Row+1, s.Frame.Height), Count: 1})
	case 2, 3:
		blank := s.eraseCell()
		for i := range s.Frame.Cells {
			s.Frame.Cells[i] = blank
		}
		for y := range s.buffer.boundaries {
			s.buffer.boundaries[y] = lineBoundary{soft: s.buffer.boundaries[y].soft}
		}
		s.record(renderer.Damage{Kind: renderer.DamageClear, X: 0, Y: 0, Width: s.Frame.Width, Height: s.Frame.Height, Count: 1})
	default:
		for y := s.Row; y < s.Frame.Height; y++ {
			start := 0
			if y == s.Row {
				start = s.Col
			}
			s.clearRow(y, start, s.Frame.Width)
		}
		s.record(renderer.Damage{Kind: renderer.DamageClear, X: 0, Y: s.Row, Width: s.Frame.Width, Height: s.Frame.Height - s.Row, Count: 1})
	}
}

func (s *Screen) clearLineMode(mode int) {
	s.clampCursor()
	switch mode {
	case 1:
		start, width := s.clearRow(s.Row, 0, min(s.Col+1, s.Frame.Width))
		s.record(renderer.Damage{Kind: renderer.DamageClear, X: start, Y: s.Row, Width: width, Height: 1, Count: 1})
	case 2:
		start, width := s.clearRow(s.Row, 0, s.Frame.Width)
		s.record(renderer.Damage{Kind: renderer.DamageClear, X: start, Y: s.Row, Width: width, Height: 1, Count: 1})
	default:
		start, width := s.clearRow(s.Row, s.Col, s.Frame.Width)
		s.record(renderer.Damage{Kind: renderer.DamageClear, X: start, Y: s.Row, Width: width, Height: 1, Count: 1})
	}
}

func (s *Screen) clampCursor() {
	s.Row = clamp(s.Row, 0, s.Frame.Height-1)
	s.Col = clamp(s.Col, 0, s.Frame.Width-1)
}

func (s *Screen) parseCSIInts(params string) []int {
	s.csiScratch = parseCSIIntsInto(s.csiScratch[:0], params)
	return s.csiScratch
}

func parseCSIInts(params string) []int {
	return parseCSIIntsInto(nil, params)
}

func parseCSIIntsInto(out []int, params string) []int {
	if params == "" {
		return out
	}
	params = strings.TrimPrefix(params, "?")
	params = strings.TrimPrefix(params, ">")
	start := 0
	for i := 0; i <= len(params); i++ {
		if i == len(params) || params[i] == ';' {
			out = append(out, parseCSIInt(params[start:i]))
			start = i + 1
		}
	}
	return out
}

func (s *Screen) parseSGRInts(params string) []int {
	s.sgrScratch = parseSGRIntsInto(s.sgrScratch[:0], params)
	return s.sgrScratch
}

func parseSGRInts(params string) []int {
	return parseSGRIntsInto(nil, params)
}

func parseSGRIntsInto(out []int, params string) []int {
	if params == "" {
		return out
	}
	start := 0
	for i := 0; i <= len(params); i++ {
		if i == len(params) || params[i] == ';' {
			out = appendSGRGroup(out, params[start:i])
			start = i + 1
		}
	}
	return out
}

func appendSGRGroup(out []int, group string) []int {
	colon := false
	for i := 0; i < len(group); i++ {
		if group[i] == ':' {
			colon = true
			break
		}
	}
	if !colon {
		return append(out, parseCSIInt(group))
	}

	parts := countSGRColonFields(group)
	code := parseCSIInt(sgrColonField(group, 0))
	if code == 4 {
		if parts < 2 {
			return append(out, code)
		}
		return append(out, code, sgrUnderlineStyleMarker, parseCSIInt(sgrColonField(group, 1)))
	}
	if code != 38 && code != 48 && code != 58 {
		return append(out, code)
	}
	mode := 0
	if parts > 1 {
		mode = parseCSIInt(sgrColonField(group, 1))
	}
	switch mode {
	case 5:
		if parts < 3 {
			return out
		}
		out = append(out, code, mode, parseCSIInt(sgrColonField(group, 2)))
	case 2:
		// code:mode::R:G:B and code:mode:cs:R:G:B both put RGB after
		// the colorspace slot; code:mode:R:G:B omits that slot.
		start := 2
		if parts > 2 && (sgrColonField(group, 2) == "" || parts >= 6) {
			start = 3
		}
		if parts < start+3 {
			return out
		}
		out = append(out, code, mode)
		for i := range 3 {
			out = append(out, parseCSIInt(sgrColonField(group, start+i)))
		}
	}
	return out
}

func countSGRColonFields(group string) int {
	count := 1
	for i := 0; i < len(group); i++ {
		if group[i] == ':' {
			count++
		}
	}
	return count
}

func sgrColonField(group string, index int) string {
	start := 0
	field := 0
	for i := 0; i <= len(group); i++ {
		if i == len(group) || group[i] == ':' {
			if field == index {
				return group[start:i]
			}
			field++
			start = i + 1
		}
	}
	return ""
}

func parseCSIInt(param string) int {
	if param == "" {
		return 0
	}
	v, err := strconv.Atoi(param)
	if err != nil {
		return 0
	}
	return v
}
