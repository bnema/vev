package client

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/renderer"
)

func TestTerminalThemeStateClearPalettePreservesReportedColors(t *testing.T) {
	state := &terminalThemeState{}
	state.update(func(theme *ports.Theme) {
		theme.HasForeground = true
		theme.Foreground = renderer.RGB{R: 1, G: 2, B: 3}
		theme.HasBackground = true
		theme.Background = renderer.RGB{R: 4, G: 5, B: 6}
		theme.SchemeKnown = true
		theme.Light = true
		theme.PaletteKnown = 1<<1 | 1<<14
		theme.Palette[1] = renderer.RGB{R: 7, G: 8, B: 9}
		theme.Palette[14] = renderer.RGB{R: 10, G: 11, B: 12}
	})

	got := state.clearPalette()
	require.True(t, got.HasForeground)
	require.Equal(t, renderer.RGB{R: 1, G: 2, B: 3}, got.Foreground)
	require.True(t, got.HasBackground)
	require.Equal(t, renderer.RGB{R: 4, G: 5, B: 6}, got.Background)
	require.True(t, got.SchemeKnown)
	require.True(t, got.Light)
	require.Zero(t, got.PaletteKnown)
	require.Equal(t, [16]renderer.RGB{}, got.Palette)

	_, reported := state.reportedTheme()
	require.True(t, reported, "foreground/background remain usable after palette invalidation")
}

type queryBoundaryReader struct {
	first, second      []byte
	readSecond         chan struct{}
	allowSecond, close <-chan struct{}
	reads              int
}

func (r *queryBoundaryReader) Read(p []byte) (int, error) {
	switch r.reads {
	case 0:
		r.reads++
		return copy(p, r.first), nil
	case 1:
		select {
		case r.readSecond <- struct{}{}:
		case <-r.close:
			return 0, io.EOF
		}
		select {
		case <-r.allowSecond:
			r.reads++
			return copy(p, r.second), nil
		case <-r.close:
			return 0, io.EOF
		}
	default:
		<-r.close
		return 0, io.EOF
	}
}

func TestStdinPumpCoalescesPaletteAndBackgroundThemePerRead(t *testing.T) {
	state := &terminalThemeState{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan ports.Frame, 3)
	pump := stdinPump{
		ctx:        ctx,
		cancel:     cancel,
		in:         bytes.NewReader([]byte("\x1b]4;1;#112233\a\x1b]11;rgb:0404/0505/0606\a\x1b]4;14;#778899\a")),
		out:        out,
		themeState: state,
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	pump.run()

	first := <-out
	require.Equal(t, ports.MsgTheme, first.Type)
	theme, err := ports.UnmarshalTheme(first.Payload)
	require.NoError(t, err)
	require.True(t, theme.HasBackground)
	require.Equal(t, renderer.RGB{R: 4, G: 5, B: 6}, theme.Background)
	require.Equal(t, uint16(1<<1|1<<14), theme.PaletteKnown)
	require.Equal(t, renderer.RGB{R: 0x11, G: 0x22, B: 0x33}, theme.Palette[1])
	require.Equal(t, renderer.RGB{R: 0x77, G: 0x88, B: 0x99}, theme.Palette[14])

	second := <-out
	require.Equal(t, ports.MsgDetach, second.Type)
	select {
	case extra := <-out:
		t.Fatalf("got extra frame after one input chunk: %v", extra.Type)
	default:
	}
}

func TestStdinPumpWaitsForQueryAcknowledgementBeforeNextRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	allowSecond := make(chan struct{})
	closeReader := make(chan struct{})
	reader := &queryBoundaryReader{
		first:       []byte("\x1b]4;1;#112233\a\x1b[?997;2n"),
		second:      []byte("\x1b]4;2;#445566\a"),
		readSecond:  make(chan struct{}, 1),
		allowSecond: allowSecond,
		close:       closeReader,
	}
	queryStarted := make(chan struct{})
	allowQuery := make(chan struct{})
	out := make(chan ports.Frame, 3)
	pump := stdinPump{
		ctx:        ctx,
		cancel:     cancel,
		in:         reader,
		out:        out,
		themeState: &terminalThemeState{},
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		requestColors: func() (colorQueryRequest, bool) {
			close(queryStarted)
			select {
			case <-allowQuery:
				return colorQueryRequest{}, true
			case <-ctx.Done():
				return colorQueryRequest{}, false
			}
		},
	}
	pumpDone := make(chan struct{})
	go func() {
		pump.run()
		close(pumpDone)
	}()

	select {
	case <-queryStarted:
	case <-time.After(time.Second):
		t.Fatal("palette re-query was not requested")
	}
	select {
	case <-reader.readSecond:
		t.Fatal("stdin pump started the next read before the query acknowledgement")
	default:
	}

	close(allowQuery)
	select {
	case <-reader.readSecond:
	case <-time.After(time.Second):
		t.Fatal("stdin pump did not resume after the query acknowledgement")
	}

	cancel()
	close(closeReader)
	select {
	case <-pumpDone:
	case <-time.After(time.Second):
		t.Fatal("stdin pump did not unwind after cancellation")
	}
}

type chunksReader struct {
	chunks [][]byte
	next   int
}

func (r *chunksReader) Read(p []byte) (int, error) {
	if r.next == len(r.chunks) {
		return 0, io.EOF
	}
	chunk := r.chunks[r.next]
	r.next++
	return copy(p, chunk), nil
}

func TestStdinPumpDefersRepeatedSchemeChunksUntilBoundary(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	requests := 0
	pump := stdinPump{
		ctx:    ctx,
		cancel: cancel,
		in: &chunksReader{chunks: [][]byte{
			[]byte("\x1b[?997;2n"),
			[]byte("\x1b[?997;1n"),
		}},
		out:        make(chan ports.Frame, 3),
		themeState: &terminalThemeState{},
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		requestColors: func() (colorQueryRequest, bool) {
			requests++
			return colorQueryRequest{}, true
		},
	}

	pump.run()
	require.Equal(t, 1, requests, "a later scheme response is drained until the boundary response arrives")
}

func TestPaletteDrainConsumesOnlyPrivateModeBoundaryResponses(t *testing.T) {
	for _, status := range []byte{'0', '1', '2', '3', '4'} {
		t.Run(string(status), func(t *testing.T) {
			response := []byte("\x1b[?2031;" + string(status) + "$y")
			var got []byte
			boundaries := 0
			drain := paletteDrain{}
			drain.scan(append([]byte("x"), response[:len(response)-2]...), func(data []byte) {
				got = append(got, data...)
			}, func() { boundaries++ })
			drain.scan(append(response[len(response)-2:], 'y'), func(data []byte) {
				got = append(got, data...)
			}, func() { boundaries++ })

			require.Equal(t, []byte("xy"), got)
			require.Equal(t, 1, boundaries)
		})
	}
}

func TestPaletteDrainForwardsNonmatchingAndPartialBytes(t *testing.T) {
	var got []byte
	boundaries := 0
	drain := paletteDrain{}
	emit := func(data []byte) { got = append(got, data...) }
	drain.scan([]byte("\x1b[0n\x1b[?2031;5$y"), emit, func() { boundaries++ })
	drain.scan([]byte("\x1b[?2031;"), emit, func() { boundaries++ })
	drain.flush(emit)

	require.Equal(t, []byte("\x1b[0n\x1b[?2031;5$y\x1b[?2031;"), got)
	require.Zero(t, boundaries)
}

func TestStdinPumpQueryAcknowledgementCancellationDoesNotDeadlock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	closeReader := make(chan struct{})
	reader := &queryBoundaryReader{
		first: []byte("\x1b[?997;2n"),
		close: closeReader,
	}
	requestStarted := make(chan struct{})
	pump := stdinPump{
		ctx:        ctx,
		cancel:     cancel,
		in:         reader,
		out:        make(chan ports.Frame, 2),
		themeState: &terminalThemeState{},
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		requestColors: func() (colorQueryRequest, bool) {
			close(requestStarted)
			<-ctx.Done()
			return colorQueryRequest{}, false
		},
	}
	pumpDone := make(chan struct{})
	go func() {
		pump.run()
		close(pumpDone)
	}()

	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("palette re-query was not requested")
	}
	cancel()
	select {
	case <-pumpDone:
	case <-time.After(time.Second):
		t.Fatal("stdin pump deadlocked waiting for a cancelled query acknowledgement")
	}
	close(closeReader)
}
