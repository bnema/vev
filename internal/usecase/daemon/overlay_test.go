package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRouteOverlayBytesHandlesTextAndSplitUTF8(t *testing.T) {
	var pending []byte
	var got []rune
	ev := overlayEvents{rune: func(r rune) { got = append(got, r) }}

	routeOverlayBytes([]byte{'w', 0xc3}, &pending, ev)
	require.Equal(t, []rune{'w'}, got)
	require.Equal(t, []byte{0xc3}, pending)

	routeOverlayBytes([]byte{0xb8, 'k'}, &pending, ev)
	require.Equal(t, []rune{'w', 'ø', 'k'}, got)
	require.Empty(t, pending)
}

func TestRouteOverlayBytesHandlesSplitArrowsAndCtrlNavigation(t *testing.T) {
	var pending []byte
	var up, down int
	ev := overlayEvents{
		up:   func() { up++ },
		down: func() { down++ },
	}

	routeOverlayBytes([]byte("\x1b["), &pending, ev)
	require.Equal(t, []byte("\x1b["), pending)
	routeOverlayBytes([]byte("B"), &pending, ev)
	require.Equal(t, 1, down)
	require.Empty(t, pending)

	routeOverlayBytes([]byte("\x1b[A\x0e\x10"), &pending, ev)
	require.Equal(t, 2, up)
	require.Equal(t, 2, down)
}

func TestRouteOverlayBytesHandlesSS3Arrows(t *testing.T) {
	var pending []byte
	var up, down int
	ev := overlayEvents{
		up:   func() { up++ },
		down: func() { down++ },
	}

	routeOverlayBytes([]byte("\x1bO"), &pending, ev)
	require.Equal(t, []byte("\x1bO"), pending)
	routeOverlayBytes([]byte("A\x1bOB"), &pending, ev)

	require.Equal(t, 1, up)
	require.Equal(t, 1, down)
	require.Empty(t, pending)
}

func TestRouteOverlayBytesHandlesEditingSubmitAndCancel(t *testing.T) {
	var pending []byte
	var backspace, enter, cancel int
	ev := overlayEvents{
		backspace: func() { backspace++ },
		enter:     func() { enter++ },
		cancel:    func() { cancel++ },
	}

	routeOverlayBytes([]byte{0x7f, 0x08, '\r', '\n', 0x1b, 0x03}, &pending, ev)

	require.Equal(t, 2, backspace)
	require.Equal(t, 2, enter)
	require.Equal(t, 2, cancel)
	require.Empty(t, pending)
}

func TestRouteOverlayBytesIgnoresControlsAndInvalidUTF8(t *testing.T) {
	var pending []byte
	var got []rune
	ev := overlayEvents{rune: func(r rune) { got = append(got, r) }}

	routeOverlayBytes([]byte{'a', 0x01, 0xff, 'b'}, &pending, ev)

	require.Equal(t, []rune{'a', 'b'}, got)
	require.Empty(t, pending)
}
