package daemon

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain/terminalcap"
	"github.com/bnema/vev/internal/protocol"
)

var (
	colorTrue = terminalcap.ColorCapabilities{Mode: terminalcap.TrueColor, Source: terminalcap.SourceDeclared}
	color16   = terminalcap.ColorCapabilities{Mode: terminalcap.ANSI16, Source: terminalcap.SourceDeclared}
	colorMono = terminalcap.ColorCapabilities{Mode: terminalcap.Monochrome, Source: terminalcap.SourceDeclared}

	// paneColorWrites exercise every color channel a pane can carry: RGB
	// foreground/background, indexed 16-255 foreground/background, and RGB and
	// indexed underline color.
	paneColorWrites = []string{
		"\x1b[38;2;10;200;30mR\x1b[0m",
		"\x1b[48;2;200;10;30mB\x1b[0m",
		"\x1b[38;5;196mF\x1b[0m",
		"\x1b[48;5;21mG\x1b[0m",
		"\x1b[4;58;2;0;255;0mU\x1b[0m",
		"\x1b[4;58;5;200mV\x1b[0m",
	}
	extendedColor = regexp.MustCompile(`^(38|48|58)`)
)

// colorClient is one attachment plus the output frames its transport received.
type colorClient struct {
	ac   *attachedClient
	tr   *closeTrackingTransport
	seen int
}

// newOutput returns the Output messages sent since the previous call and their
// concatenated terminal bytes.
func (c *colorClient) newOutput(t *testing.T) ([]protocol.Output, []byte) {
	t.Helper()
	frames := testFramesOfType(c.tr.Sends(), "Output")
	var (
		outs []protocol.Output
		data []byte
	)
	for _, frame := range frames[c.seen:] {
		out := unmarshalTestOutput(t, frame.Payload)
		outs = append(outs, out)
		data = append(data, out.Data...)
	}
	c.seen = len(frames)
	return outs, data
}

func colorHello(intent uint8, name string, client byte, token uint64, color terminalcap.ColorCapabilities) protocol.Hello {
	h := helloResumeCapable(intent, name, token)
	h.ClientID = [16]byte{client}
	h.Color = color
	return h
}

// requireProfile asserts data is encoded for color: pane RGB/indexed channels
// survive only where the mode can express them.
func requireProfile(t *testing.T, mode terminalcap.ColorMode, data []byte) {
	t.Helper()
	params := colorParams(data)
	switch mode {
	case terminalcap.TrueColor:
		require.Contains(t, params, "38;2;10;200;30", "%q", data)
		require.Contains(t, params, "48;2;200;10;30", "%q", data)
		require.Contains(t, params, "58;2;0;255;0", "%q", data)
		require.Contains(t, params, "38;5;196", "%q", data)
		require.Contains(t, params, "48;5;21", "%q", data)
		require.Contains(t, params, "58;5;200", "%q", data)
	case terminalcap.ANSI16:
		require.NotEmpty(t, params, "ansi16 keeps basic colors: %q", data)
		for _, p := range params {
			require.NotRegexp(t, extendedColor, p, "ansi16 output carries extended color: %q", data)
		}
	case terminalcap.Monochrome:
		require.Empty(t, params, "monochrome output carries color: %q", data)
		require.NotRegexp(t, `\x1b\[[0-9;:]*(38|48|58)[;:]`, string(data))
	default:
		t.Fatalf("unhandled mode %v", mode)
	}
}

func attachColorClients(t *testing.T, modes []terminalcap.ColorCapabilities) (*Daemon, *session, []*colorClient) {
	t.Helper()
	pty, release := newBlockingPTY(t)
	t.Cleanup(release)
	d := newTestDaemon(t, newFactorySeq(t, pty), stubClock{})
	var (
		sess    *session
		clients []*colorClient
	)
	for i, color := range modes {
		tr := &closeTrackingTransport{}
		intent := protocol.IntentAttach
		if i == 0 {
			intent = protocol.IntentNew
		}
		s, ac, err := d.route(colorHello(intent, "work", byte(i+1), 0, color), tr)
		require.NoError(t, err)
		if sess == nil {
			sess = s
		}
		require.Same(t, sess, s)
		require.Equal(t, color.Mode, ac.terminalCapabilities.Color.Mode)
		clients = append(clients, &colorClient{ac: ac, tr: tr})
	}
	return d, sess, clients
}

// TestAttachmentsEncodePaneOutputWithOwnColorProfile covers pane output for
// attachments of one session that negotiated different color modes: full
// first frame, incremental update, and explicit renderer reset.
func TestAttachmentsEncodePaneOutputWithOwnColorProfile(t *testing.T) {
	modes := []terminalcap.ColorCapabilities{colorTrue, color16, colorMono}
	d, sess, clients := attachColorClients(t, modes)
	pane := sess.tabs[0].focusedPane()
	for _, w := range paneColorWrites {
		pane.screen.Write([]byte(w))
	}

	paintAll := func(reset bool) {
		for _, c := range clients {
			d.paint(sess, c.ac, reset, nil)
		}
	}
	// Each step repaints every attachment and then checks that attachment's
	// bytes against its own mode.
	steps := []struct {
		name  string
		setup func()
		reset bool
		full  bool
	}{
		{name: "initial full frame", reset: true, full: true},
		{name: "incremental draw", setup: func() {
			for _, w := range paneColorWrites {
				pane.screen.Write([]byte("\r\n" + w))
			}
		}},
		{name: "renderer reset", reset: true, full: true},
		{name: "incremental after reset", setup: func() {
			for _, w := range paneColorWrites {
				pane.screen.Write([]byte("\r\n" + w))
			}
		}},
	}
	var prevNew [3]uint64
	for _, step := range steps {
		if step.setup != nil {
			step.setup()
		}
		paintAll(step.reset)
		for i, c := range clients {
			t.Run(step.name+"/"+modeName(modes[i].Mode), func(t *testing.T) {
				outs, data := c.newOutput(t)
				require.NotEmpty(t, outs)
				first := outs[0]
				if step.full {
					require.Zero(t, first.Base, "full frames start a new chain")
				} else {
					require.Equal(t, prevNew[i], first.Base, "incremental frames extend the chain")
				}
				prevNew[i] = outs[len(outs)-1].New
				c.ac.ackOutputState(outs[len(outs)-1].Epoch, outs[len(outs)-1].New)
				requireProfile(t, modes[i].Mode, data)
			})
		}
	}
}

func modeName(m terminalcap.ColorMode) string {
	switch m {
	case terminalcap.TrueColor:
		return "truecolor"
	case terminalcap.ANSI16:
		return "ansi16"
	case terminalcap.Monochrome:
		return "monochrome"
	default:
		return "other"
	}
}

// TestResumeAdoptsColorOfLatestHello proves a resumed attachment encodes for
// the replacement terminal's Hello.Color, not the one it parked with.
func TestResumeAdoptsColorOfLatestHello(t *testing.T) {
	tests := []struct {
		name       string
		from, to   terminalcap.ColorCapabilities
		liveResume bool // resume while the old link is still registered
		// wantToasts is checked only when checkToasts is set; it lists the
		// visible toast messages after resume.
		checkToasts bool
		wantToasts  []string
	}{
		{name: "truecolor to monochrome", from: colorTrue, to: colorMono},
		{name: "truecolor to ansi16", from: colorTrue, to: color16},
		{
			name: "declared ansi16 resume shows downgrade notice", from: colorTrue, to: color16,
			checkToasts: true, wantToasts: []string{"Terminal supports 16 colors; vev UI colors are reduced."},
		},
		{
			name: "forced monochrome resume shows no notice", from: colorTrue,
			to:          terminalcap.ColorCapabilities{Mode: terminalcap.Monochrome, Source: terminalcap.SourceForced},
			checkToasts: true,
		},
		{name: "monochrome to truecolor", from: colorMono, to: colorTrue},
		{name: "ansi16 to truecolor", from: color16, to: colorTrue},
		{name: "ansi16 to monochrome", from: color16, to: colorMono},
		{name: "same mode", from: color16, to: color16},
		{name: "live resume truecolor to monochrome", from: colorTrue, to: colorMono, liveResume: true},
		{name: "live resume monochrome to ansi16", from: colorMono, to: color16, liveResume: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pty, release := newBlockingPTY(t)
			defer release()
			d := newTestDaemon(t, newFactorySeq(t, pty), stubClock{})
			oldTr := &closeTrackingTransport{}
			sess, ac, err := d.route(colorHello(protocol.IntentNew, "work", 1, 0, tt.from), oldTr)
			require.NoError(t, err)
			for _, w := range paneColorWrites {
				sess.tabs[0].focusedPane().screen.Write([]byte(w))
			}
			before := &colorClient{ac: ac, tr: oldTr}
			d.paint(sess, ac, true, nil)
			_, data := before.newOutput(t)
			requireProfile(t, tt.from.Mode, data)

			token := ac.resumeToken
			if !tt.liveResume {
				d.clientGone(sess, ac, oldTr, false)
			}
			newTr := &closeTrackingTransport{}
			resumedSess, resumed, err := d.route(colorHello(protocol.IntentResume, "work", 1, token, tt.to), newTr)
			require.NoError(t, err)
			require.Same(t, sess, resumedSess)
			require.Same(t, ac, resumed)
			require.Equal(t, tt.to, resumed.terminalCapabilities.Color)

			after := &colorClient{ac: resumed, tr: newTr}
			d.paint(resumedSess, resumed, true, nil)
			outs, data := after.newOutput(t)
			require.NotEmpty(t, outs)
			require.Zero(t, outs[0].Base, "resume starts with a full frame")
			requireProfile(t, tt.to.Mode, data)
			resumed.ackOutputState(outs[len(outs)-1].Epoch, outs[len(outs)-1].New)

			// The profile also holds for incremental updates after resume.
			for _, w := range paneColorWrites {
				sess.tabs[0].focusedPane().screen.Write([]byte("\r\n" + w))
			}
			d.paint(resumedSess, resumed, false, nil)
			outs, data = after.newOutput(t)
			require.NotEmpty(t, outs)
			require.NotZero(t, outs[0].Base)
			requireProfile(t, tt.to.Mode, data)

			if tt.checkToasts {
				toasts, _ := visibleToasts(resumed)
				var got []string
				for _, toast := range toasts {
					got = append(got, toast.Message)
				}
				require.Equal(t, tt.wantToasts, got)
			}

			// Chrome styling follows the new mode as well.
			reduced := tt.to.Mode != terminalcap.TrueColor
			require.Equal(t, reduced, resumed.getAppliedTheme().Raw.DimByAttribute)
		})
	}
}
