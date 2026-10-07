package client

import (
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
)

func newTestAttachmentEcho(t *testing.T, mode domain.EchoPredictMode) (*attachmentEcho, *mockattachmentPredictionForeground, *portsmocks.MockClock) {
	t.Helper()
	out := newMockattachmentPredictionForeground(t)
	clock := portsmocks.NewMockClock(t)
	clock.EXPECT().Now().Return(echoTestStart).Maybe()
	echo := &attachmentEcho{predictor: newEchoPredictor(mode), out: out, clock: clock}
	echo.seed(protocol.Output{Epoch: 1, New: 1, Full: true, Size: domain.Size{Cols: 20, Rows: 3}, Data: []byte("\x1b[H\x1b[2J$ ")})
	// One acknowledged input gives the engine its first round trip.
	echo.predictor.input(1, nil, echoTestStart)
	echo.predictor.applyOutput(protocol.Output{Epoch: 1, Echo: 1}, echoTestStart.Add(echoServerDelay))
	echo.predictor.confEpoch = echo.predictor.predEpoch
	return echo, out, clock
}

func TestNewAttachmentEchoSkipsNonPredictingAttachments(t *testing.T) {
	t.Parallel()
	fg := newMockattachmentPredictionForeground(t)
	tests := []struct {
		name  string
		local bool
		mode  domain.EchoPredictMode
		fg    AttachmentForeground
	}{
		{name: "local attachment", local: true, mode: domain.EchoPredictAdaptive},
		{name: "prediction off", mode: domain.EchoPredictNever},
		{name: "foreground without prediction seam", mode: domain.EchoPredictAdaptive},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			w := &sessionAttachmentWorker{cfg: sessionAttachmentConfig{EchoPredict: tt.mode}}
			w.cfg.Request.Local = tt.local
			require.Nil(t, newAttachmentEcho(w, tt.fg))
		})
	}
	require.NotNil(t, newAttachmentEcho(&sessionAttachmentWorker{cfg: sessionAttachmentConfig{EchoPredict: domain.EchoPredictAdaptive, Clock: systemClock{}}}, struct {
		AttachmentForeground
		attachmentPredictionForeground
	}{attachmentPredictionForeground: fg}), "a remote attachment with the seam predicts")
}

func TestAttachmentEchoDrawsGuessAndArmsTick(t *testing.T) {
	t.Parallel()
	echo, out, clock := newTestAttachmentEcho(t, domain.EchoPredictAdaptive)
	// A slow link: guesses show, and the tick waits on the glitch trigger.
	echo.predictor.srtt = 100 * time.Millisecond
	tick := portsmocks.NewMockTimer(t)
	clock.EXPECT().NewTimer(echoTickInterval).Return(tick).Once()
	var drawn []byte
	out.EXPECT().writePrediction(mock.Anything).RunAndReturn(func(data []byte) (bool, error) {
		drawn = append([]byte(nil), data...)
		return true, nil
	}).Once()

	require.NoError(t, echo.input(2, []byte("b")))
	require.Equal(t, "\x1b[1;3H\x1b[0mb\x1b[0m\x1b[1;4H", string(drawn))

	fired := make(chan time.Time)
	tick.EXPECT().C().Return(fired).Once()
	require.Equal(t, (<-chan time.Time)(fired), echo.tickC())
	tick.EXPECT().Stop().Return(true).Once()
	echo.close()
	require.Nil(t, echo.tickC())
}

func TestAttachmentEchoForgetsGuessesWhileOverlayOwnsTerminal(t *testing.T) {
	t.Parallel()
	echo, out, _ := newTestAttachmentEcho(t, domain.EchoPredictAlways)
	out.EXPECT().writePrediction(mock.Anything).Return(false, nil).Once()

	require.NoError(t, echo.input(2, []byte("b")))
	require.False(t, echo.predictor.active(), "guesses survived an overlay")
	require.Empty(t, echo.predictor.drawn, "an overlay-covered guess must not be repainted later")
}

func TestAttachmentEchoSameSizeResizeRepaintsDrawnGuesses(t *testing.T) {
	t.Parallel()
	echo, out, _ := newTestAttachmentEcho(t, domain.EchoPredictAlways)
	out.EXPECT().writePrediction(mock.Anything).Return(true, nil).Once()
	require.NoError(t, echo.input(2, []byte("b")))

	var repaint []byte
	out.EXPECT().writePrediction(mock.Anything).RunAndReturn(func(data []byte) (bool, error) {
		repaint = append([]byte(nil), data...)
		return true, nil
	}).Once()
	require.NoError(t, echo.resized(domain.Size{Cols: 20, Rows: 3}))
	require.Equal(t, "\x1b[1;3H\x1b[0m \x1b[0m\x1b[1;3H", string(repaint))

	// A new size brings a full repaint from the daemon: nothing is written.
	require.NoError(t, echo.resized(domain.Size{Cols: 30, Rows: 3}))
}
