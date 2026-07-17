package theme

import (
	"bytes"
	"strconv"
	"strings"

	"github.com/bnema/vev/pkg/renderer"
	"github.com/bnema/vev/pkg/vt"
)

const maxPending = 64

// Theme describes terminal default colors and ANSI palette entries.
type Theme struct {
	Foreground   renderer.RGB
	Background   renderer.RGB
	Palette      [16]renderer.RGB
	PaletteKnown uint16
	HasFG        bool
	HasBG        bool
	TrueColor    bool
	Known        bool
	SchemeKnown  bool
	Light        bool
	UsePalette   bool
}

// PaletteColor returns a palette color only when palette inheritance is
// enabled and the terminal reported the requested ANSI slot.
func (t Theme) PaletteColor(slot int) (renderer.RGB, bool) {
	if !t.UsePalette || slot < 0 || slot >= len(t.Palette) || t.PaletteKnown&(uint16(1)<<slot) == 0 {
		return renderer.RGB{}, false
	}
	return t.Palette[slot], true
}

var (
	BuiltinDark = Theme{
		Foreground:  renderer.RGB{R: 0xd8, G: 0xd8, B: 0xd8},
		Background:  renderer.RGB{R: 0x18, G: 0x18, B: 0x18},
		HasFG:       true,
		HasBG:       true,
		TrueColor:   true,
		Known:       true,
		SchemeKnown: true,
		Light:       false,
	}
	BuiltinLight = Theme{
		Foreground:  renderer.RGB{R: 0x20, G: 0x20, B: 0x20},
		Background:  renderer.RGB{R: 0xf8, G: 0xf8, B: 0xf8},
		HasFG:       true,
		HasBG:       true,
		TrueColor:   true,
		Known:       true,
		SchemeKnown: true,
		Light:       true,
	}
)

// ParseXColor parses the XParseColor formats commonly returned by OSC 10/11.
func ParseXColor(s string) (renderer.RGB, bool) {
	if strings.HasPrefix(s, "#") {
		if len(s) != 7 {
			return renderer.RGB{}, false
		}
		r, ok := parseHexByte(s[1:3])
		if !ok {
			return renderer.RGB{}, false
		}
		g, ok := parseHexByte(s[3:5])
		if !ok {
			return renderer.RGB{}, false
		}
		b, ok := parseHexByte(s[5:7])
		if !ok {
			return renderer.RGB{}, false
		}
		return renderer.RGB{R: r, G: g, B: b}, true
	}

	if !strings.HasPrefix(s, "rgb:") {
		return renderer.RGB{}, false
	}
	parts := strings.Split(s[len("rgb:"):], "/")
	if len(parts) != 3 {
		return renderer.RGB{}, false
	}
	var out [3]uint8
	for i, part := range parts {
		component, ok := parseXColorComponent(part)
		if !ok {
			return renderer.RGB{}, false
		}
		out[i] = component
	}
	return renderer.RGB{R: out[0], G: out[1], B: out[2]}, true
}

// parseXColorComponent scales an XParseColor component of one to four hex
// digits to an 8-bit channel.
func parseXColorComponent(s string) (uint8, bool) {
	if len(s) < 1 || len(s) > 4 {
		return 0, false
	}
	value, err := strconv.ParseUint(s, 16, 16)
	if err != nil {
		return 0, false
	}
	maxValue := uint64((1 << (4 * len(s))) - 1)
	return uint8(value * 255 / maxValue), true
}

func parseHexByte(s string) (uint8, bool) {
	v, err := strconv.ParseUint(s, 16, 8)
	if err != nil {
		return 0, false
	}
	return uint8(v), true
}

// Scanner decodes terminal color responses while preserving ordinary bytes.
type Scanner struct {
	pending []byte
}

// Scan extracts ESC ] 10;<color>, ESC ] 11;<color>, and ESC ] 4;<slot>;<color>
// responses terminated by BEL or ST.
// All non-matching bytes are emitted through onBytes in original order, and a
// contiguous run of ordinary bytes (including any ESC that does not start a
// color-OSC sequence, e.g. keyboard/mouse escape sequences like SGR mouse
// reports or arrow keys) is always delivered as a single onBytes call so
// callers never see it split. Partial OSC color responses are buffered across
// calls, but the buffer is bounded so an unterminated OSC cannot block
// unrelated input forever.
func (s *Scanner) Scan(data []byte, onColor func(kind int, rgb renderer.RGB), onPalette func(slot int, rgb renderer.RGB), onScheme func(light bool), onBytes func([]byte)) {
	if len(s.pending) > 0 {
		combined := make([]byte, 0, len(s.pending)+len(data))
		combined = append(combined, s.pending...)
		combined = append(combined, data...)
		data = combined
		s.pending = nil
	}

	byteStart := 0
	for i := 0; i < len(data); i++ {
		if data[i] != '\x1b' {
			continue
		}

		if complete, possible, light := schemeCSINotification(data[i:]); complete {
			if byteStart < i {
				onBytes(data[byteStart:i])
			}
			onScheme(light)
			i += len(vt.ColorSchemeReportDark) - 1
			byteStart = i + 1
			continue
		} else if possible {
			if byteStart < i {
				onBytes(data[byteStart:i])
			}
			s.bufferOrFlush(data[i:], onBytes)
			return
		}

		kind, prefixLen, completePrefix, possiblePrefix := colorOSCPrefix(data[i:])
		if !completePrefix {
			if possiblePrefix {
				if byteStart < i {
					onBytes(data[byteStart:i])
				}
				s.bufferOrFlush(data[i:], onBytes)
				return
			}
			// Not the start of a color-OSC sequence: leave this ESC inside
			// the current passthrough run instead of splitting it out. It
			// may be the introducer of an unrelated escape sequence (SGR
			// mouse report, arrow key, etc.) that must reach the reader
			// intact.
			continue
		}

		if byteStart < i {
			onBytes(data[byteStart:i])
		}

		termStart, termEnd := findTerminator(data[i+prefixLen:])
		if termStart < 0 {
			s.bufferOrFlush(data[i:], onBytes)
			return
		}
		termStart += i + prefixLen
		termEnd += i + prefixLen
		raw := data[i:termEnd]
		body := string(data[i+prefixLen : termStart])
		switch kind {
		case 4:
			slot, rgb, ok := parsePaletteResponse(body)
			if ok {
				onPalette(slot, rgb)
			} else {
				onBytes(raw)
			}
		default:
			rgb, ok := ParseXColor(body)
			if ok {
				onColor(kind, rgb)
			} else {
				onBytes(raw)
			}
		}
		i = termEnd - 1
		byteStart = termEnd
	}

	if byteStart < len(data) {
		onBytes(data[byteStart:])
	}
}

func (s *Scanner) bufferOrFlush(partial []byte, onBytes func([]byte)) {
	if len(partial) > maxPending {
		onBytes(partial)
		s.pending = nil
		return
	}
	s.pending = append(s.pending, partial...)
}

func colorOSCPrefix(data []byte) (kind, prefixLen int, complete bool, possible bool) {
	if len(data) < 2 {
		return 0, 0, false, false
	}
	prefixes := []struct {
		kind   int
		prefix []byte
	}{
		{kind: 10, prefix: []byte("\x1b]10;")},
		{kind: 11, prefix: []byte("\x1b]11;")},
		{kind: 4, prefix: []byte("\x1b]4;")},
	}
	for _, candidate := range prefixes {
		if len(data) >= len(candidate.prefix) {
			if bytes.Equal(data[:len(candidate.prefix)], candidate.prefix) {
				return candidate.kind, len(candidate.prefix), true, true
			}
			continue
		}
		if bytes.Equal(data, candidate.prefix[:len(data)]) {
			return candidate.kind, len(candidate.prefix), false, true
		}
	}
	return 0, 0, false, false
}

func parsePaletteResponse(body string) (int, renderer.RGB, bool) {
	indexEnd := strings.IndexByte(body, ';')
	if indexEnd < 1 {
		return 0, renderer.RGB{}, false
	}
	slot := 0
	for _, b := range []byte(body[:indexEnd]) {
		if b < '0' || b > '9' {
			return 0, renderer.RGB{}, false
		}
		slot = slot*10 + int(b-'0')
		if slot > 15 {
			return 0, renderer.RGB{}, false
		}
	}
	rgb, ok := ParseXColor(body[indexEnd+1:])
	if !ok {
		return 0, renderer.RGB{}, false
	}
	return slot, rgb, true
}

// schemeCSIPrefixes lists the CSI ?997 scheme-notification sequences vev
// recognizes. Package-level so schemeCSINotification (called per ESC byte in
// the stdin hot path) doesn't allocate on every call.
var schemeCSIPrefixes = []struct {
	seq   []byte
	light bool
}{
	{seq: []byte(vt.ColorSchemeReportDark), light: false},
	{seq: []byte(vt.ColorSchemeReportLight), light: true},
}

func schemeCSINotification(data []byte) (complete bool, possible bool, light bool) {
	// A bare trailing ESC (handled elsewhere) or "ESC [" (2 bytes) is
	// something a user can type; unlike OSC's "ESC ]" it must never be
	// withheld as a partial match with nothing following, or those keystrokes
	// would be buffered and never forwarded. Partial buffering only starts
	// from "ESC [ ?" onward; a complete match is always >= 9 bytes, so this
	// guard cannot reject one.
	if len(data) < 3 {
		return false, false, false
	}
	for _, prefix := range schemeCSIPrefixes {
		if len(data) >= len(prefix.seq) {
			if bytes.Equal(data[:len(prefix.seq)], prefix.seq) {
				return true, true, prefix.light
			}
			continue
		}
		if bytes.Equal(data, prefix.seq[:len(data)]) {
			return false, true, false
		}
	}
	return false, false, false
}

func findTerminator(data []byte) (start int, end int) {
	for i := range data {
		switch data[i] {
		case '\x07':
			return i, i + 1
		case '\x1b':
			if i+1 < len(data) && data[i+1] == '\\' {
				return i, i + 2
			}
		}
	}
	return -1, -1
}

// Blend linearly interpolates between a and b.
func Blend(a, b renderer.RGB, t float64) renderer.RGB {
	if t < 0 {
		t = 0
	}
	if t > 1 {
		t = 1
	}
	return renderer.RGB{
		R: blendByte(a.R, b.R, t),
		G: blendByte(a.G, b.G, t),
		B: blendByte(a.B, b.B, t),
	}
}

func blendByte(a, b uint8, t float64) uint8 {
	return uint8(float64(a) + (float64(b)-float64(a))*t + 0.5)
}

func StatusBarStyle(t Theme) renderer.Style {
	if !usable(t) {
		return renderer.DefaultStyle()
	}
	style := renderer.DefaultStyle()
	style.HasForegroundRGB = true
	style.ForegroundRGB = t.Foreground
	style.HasBackgroundRGB = true
	style.BackgroundRGB = Blend(t.Background, t.Foreground, 0.12)
	return style
}

const (
	ansiBlue       = 4
	ansiBrightBlue = 12
	accentBlend    = 0.25
	titleHueBlend  = 0.2
)

func AccentStyle(t Theme) renderer.Style {
	if !usable(t) {
		return inverseStyle()
	}
	style := renderer.DefaultStyle()
	style.HasForegroundRGB = true
	style.ForegroundRGB = t.Foreground
	style.HasBackgroundRGB = true
	style.BackgroundRGB = accentBackground(t)
	return style
}

// accentBackground uses a terminal's blue palette only when the resulting
// foreground/background pair remains legible. A neutral blend is always the
// fallback, including when palette inheritance is disabled.
func accentBackground(t Theme) renderer.RGB {
	neutral := Blend(t.Background, t.Foreground, accentBlend)
	if !t.UsePalette {
		return neutral
	}
	for _, slot := range []int{ansiBlue, ansiBrightBlue} {
		blue, ok := t.PaletteColor(slot)
		if !ok {
			continue
		}
		candidate := Blend(t.Background, blue, accentBlend)
		if ContrastRatio(t.Foreground, candidate) >= accentContrastMin {
			return candidate
		}
	}
	return neutral
}

func BorderStyle(t Theme) renderer.Style {
	neutral := renderer.DefaultStyle()
	if usable(t) {
		neutral.HasForegroundRGB = true
		neutral.ForegroundRGB = Blend(t.Foreground, t.Background, 0.40)
	}
	if !t.UsePalette {
		return neutral
	}

	base := renderer.DefaultStyle()
	if usable(t) {
		base.HasForegroundRGB = true
		base.ForegroundRGB = t.Foreground
		base.HasBackgroundRGB = true
		base.BackgroundRGB = t.Background
	}
	return paletteForegroundStyle(base, neutral, t)
}

func MutedTextStyle(t Theme) renderer.Style {
	neutral := renderer.DefaultStyle()
	if usable(t) {
		neutral.HasForegroundRGB = true
		neutral.ForegroundRGB = Blend(t.Foreground, t.Background, 0.45)
	}
	if !t.UsePalette {
		return neutral
	}

	base := renderer.DefaultStyle()
	if usable(t) {
		base.HasForegroundRGB = true
		base.ForegroundRGB = t.Foreground
		base.HasBackgroundRGB = true
		base.BackgroundRGB = t.Background
	}
	return paletteForegroundStyle(base, neutral, t)
}

// mutedVariantBlend is the fraction MutedVariantStyle blends a style's own
// foreground toward its own background, subtler than MutedTextStyle's 0.45
// (which blends the theme's global fg/bg instead of a style's own colors).
const mutedVariantBlend = 0.4

// EmphasisStyle returns base with bold applied when the client theme supports
// styled output; otherwise base is returned unchanged.
func EmphasisStyle(base renderer.Style, t Theme) renderer.Style {
	if !usable(t) {
		return base
	}
	out := base
	out.Bold = true
	return out
}

// MutedVariantStyle fades base's foreground toward its background when both
// are RGB and the client theme supports styled output; otherwise base is
// returned unchanged. When base has an RGB foreground but no RGB background,
// it fades toward t.Background instead.
func MutedVariantStyle(base renderer.Style, t Theme) renderer.Style {
	if !usable(t) || !base.HasForegroundRGB {
		return base
	}
	bg := base.BackgroundRGB
	if !base.HasBackgroundRGB {
		if !t.HasBG {
			return base
		}
		bg = t.Background
	}
	out := base
	out.ForegroundRGB = Blend(base.ForegroundRGB, bg, mutedVariantBlend)
	return out
}

// PaletteMutedVariantStyle derives a title foreground from a terminal blue
// palette entry. Its contrast is evaluated against the title's actual base
// background, rather than the terminal background, because status and active
// title backgrounds differ.
func PaletteMutedVariantStyle(base renderer.Style, t Theme) renderer.Style {
	return paletteForegroundStyle(base, MutedVariantStyle(base, t), t)
}

// paletteForegroundStyle uses palette blues for a foreground only. It never
// changes a background, so palette inheritance cannot introduce indexed
// backgrounds. If terminal RGB data is unavailable, a known color scheme can
// still select a best-effort ANSI foreground. A reported blue that fails
// contrast deliberately suppresses that indexed fallback.
func paletteForegroundStyle(base, neutral renderer.Style, t Theme) renderer.Style {
	if !t.UsePalette {
		return neutral
	}

	knownBlue := paletteBlueKnown(t)
	if usable(t) && base.HasForegroundRGB && base.HasBackgroundRGB {
		for _, slot := range []int{ansiBlue, ansiBrightBlue} {
			blue, ok := t.PaletteColor(slot)
			if !ok {
				continue
			}
			candidate := Blend(base.ForegroundRGB, blue, titleHueBlend)
			if ContrastRatio(candidate, base.BackgroundRGB) >= accentContrastMin {
				out := neutral
				out.HasForegroundRGB = true
				out.ForegroundRGB = candidate
				return out
			}
		}
	}

	if !knownBlue && t.SchemeKnown {
		out := neutral
		out.HasForegroundRGB = false
		if t.Light {
			out.Foreground = ansiBlue
		} else {
			out.Foreground = ansiBrightBlue
		}
		return out
	}
	return neutral
}

func paletteBlueKnown(t Theme) bool {
	if !t.UsePalette {
		return false
	}
	return t.PaletteKnown&(uint16(1)<<ansiBlue|uint16(1)<<ansiBrightBlue) != 0
}

const defaultDimmingPercent = 35

// Dimmer transforms resolved terminal colors for subdued UI states. Construct
// it once and reuse it while rendering cells with the same theme and policy.
type Dimmer struct {
	theme             Theme
	backgroundPercent int
	foregroundPercent int
}

type dimmingTarget uint8

const (
	dimBackground dimmingTarget = iota
	dimForeground
)

// DimmerOption customizes a Dimmer without allocating a closure.
type DimmerOption struct {
	target  dimmingTarget
	percent int
}

// WithBackgroundDimming overrides the default 35 percent background fade.
func WithBackgroundDimming(percent int) DimmerOption {
	return DimmerOption{target: dimBackground, percent: percent}
}

// WithForegroundDimming overrides the default 35 percent foreground fade.
func WithForegroundDimming(percent int) DimmerOption {
	return DimmerOption{target: dimForeground, percent: percent}
}

// NewDimmer returns a reusable dimmer with optional per-channel overrides.
func NewDimmer(t Theme, opts ...DimmerOption) Dimmer {
	d := Dimmer{
		theme:             t,
		backgroundPercent: defaultDimmingPercent,
		foregroundPercent: defaultDimmingPercent,
	}
	for _, opt := range opts {
		switch opt.target {
		case dimBackground:
			d.backgroundPercent = opt.percent
		case dimForeground:
			d.foregroundPercent = opt.percent
		}
	}
	d.backgroundPercent = clampPercent(d.backgroundPercent)
	d.foregroundPercent = clampPercent(d.foregroundPercent)
	return d
}

// Dim fades a style's resolved background toward the terminal background,
// then fades its foreground and custom underline toward that dimmed background.
func (d Dimmer) Dim(style renderer.Style) renderer.Style {
	if !usable(d.theme) {
		return style
	}

	foreground := resolveForeground(style, d.theme)
	background := resolveBackground(style, d.theme)
	if style.Inverse {
		foreground, background = background, foreground
	}
	background = blendPercent(background, d.theme.Background, d.backgroundPercent)

	out := style
	out.Inverse = false
	out.HasForegroundRGB = true
	out.ForegroundRGB = blendPercent(foreground, background, d.foregroundPercent)
	out.HasBackgroundRGB = true
	out.BackgroundRGB = background
	if style.HasUnderlineColorRGB || style.HasUnderlineColor {
		underline := style.UnderlineColorRGB
		if !style.HasUnderlineColorRGB {
			underline = xterm256Color(style.UnderlineColor)
		}
		out.HasUnderlineColor = false
		out.HasUnderlineColorRGB = true
		out.UnderlineColorRGB = blendPercent(underline, background, d.foregroundPercent)
	}
	return out
}

func resolveForeground(style renderer.Style, t Theme) renderer.RGB {
	if style.HasForegroundRGB {
		return style.ForegroundRGB
	}
	if style.Foreground >= 0 {
		return xterm256Color(style.Foreground)
	}
	return t.Foreground
}

func resolveBackground(style renderer.Style, t Theme) renderer.RGB {
	if style.HasBackgroundRGB {
		return style.BackgroundRGB
	}
	if style.Background >= 0 {
		return xterm256Color(style.Background)
	}
	return t.Background
}

func blendPercent(a, b renderer.RGB, percent int) renderer.RGB {
	return renderer.RGB{
		R: blendBytePercent(a.R, b.R, percent),
		G: blendBytePercent(a.G, b.G, percent),
		B: blendBytePercent(a.B, b.B, percent),
	}
}

func blendBytePercent(a, b uint8, percent int) uint8 {
	return uint8((int(a)*(100-percent) + int(b)*percent + 50) / 100)
}

func clampPercent(percent int) int {
	if percent < 0 {
		return 0
	}
	if percent > 100 {
		return 100
	}
	return percent
}

var (
	xterm256Base = [...]renderer.RGB{
		{R: 0x00, G: 0x00, B: 0x00}, {R: 0x80, G: 0x00, B: 0x00}, {R: 0x00, G: 0x80, B: 0x00}, {R: 0x80, G: 0x80, B: 0x00},
		{R: 0x00, G: 0x00, B: 0x80}, {R: 0x80, G: 0x00, B: 0x80}, {R: 0x00, G: 0x80, B: 0x80}, {R: 0xc0, G: 0xc0, B: 0xc0},
		{R: 0x80, G: 0x80, B: 0x80}, {R: 0xff, G: 0x00, B: 0x00}, {R: 0x00, G: 0xff, B: 0x00}, {R: 0xff, G: 0xff, B: 0x00},
		{R: 0x00, G: 0x00, B: 0xff}, {R: 0xff, G: 0x00, B: 0xff}, {R: 0x00, G: 0xff, B: 0xff}, {R: 0xff, G: 0xff, B: 0xff},
	}
	xterm256Levels = [...]uint8{0, 95, 135, 175, 215, 255}
)

func xterm256Color(index int) renderer.RGB {
	if index < 0 {
		return renderer.RGB{}
	}
	if index < len(xterm256Base) {
		return xterm256Base[index]
	}
	if index < 232 {
		index -= 16
		return renderer.RGB{R: xterm256Levels[index/36], G: xterm256Levels[(index/6)%6], B: xterm256Levels[index%6]}
	}
	if index < 256 {
		level := uint8(8 + (index-232)*10)
		return renderer.RGB{R: level, G: level, B: level}
	}
	return xterm256Base[index%len(xterm256Base)]
}

func SelectionStyle(t Theme) renderer.Style {
	if !usable(t) {
		return inverseStyle()
	}
	return AccentStyle(t)
}

func usable(t Theme) bool {
	return t.Known && t.TrueColor && t.HasFG && t.HasBG
}

func inverseStyle() renderer.Style {
	style := renderer.DefaultStyle()
	style.Inverse = true
	return style
}
