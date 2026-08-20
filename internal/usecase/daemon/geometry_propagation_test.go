package daemon

import (
	"io"
	"log/slog"
	"testing"

	vt "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestSessionPaneGeometryUsesLatestAttachmentClaimAndTruncates(t *testing.T) {
	sess := &session{}
	first := &attachedClient{}
	first.setGeometry(domain.Geometry{
		Size: domain.Size{Cols: 80, Rows: 24}, PixelWidth: 800, PixelHeight: 480,
	})
	second := &attachedClient{}
	second.setGeometry(domain.Geometry{
		Size: domain.Size{Cols: 80, Rows: 24}, PixelWidth: 803, PixelHeight: 487,
	})

	require.True(t, sess.registerAttachment(first))
	require.True(t, sess.registerAttachment(second))
	require.Equal(t, domain.Geometry{
		Size: domain.Size{Cols: 39, Rows: 23}, PixelWidth: 391, PixelHeight: 466,
	}, sess.paneGeometry(domain.Size{Cols: 39, Rows: 23}))

	require.True(t, sess.unregisterAttachment(second))
	require.Equal(t, domain.Geometry{
		Size: domain.Size{Cols: 39, Rows: 23}, PixelWidth: 390, PixelHeight: 460,
	}, sess.paneGeometry(domain.Size{Cols: 39, Rows: 23}))
}

func TestPreparedLayoutPropagatesGeometryToPTYAndScreenQueries(t *testing.T) {
	geometry := domain.Geometry{
		Size: domain.Size{Cols: 39, Rows: 23}, PixelWidth: 391, PixelHeight: 466,
	}
	pty := &recordingGeometryPTY{}
	pane := &pane{pty: pty, screen: vt.NewScreen(1, 1), rect: domain.Rect{Width: 1, Height: 1}}
	plan := preparedTabLayout{members: []resizeMember{{pane: pane, rect: domain.Rect{Width: 39, Height: 23}, geometry: geometry}}}
	daemon := &Daemon{log: slog.New(slog.NewTextHandler(io.Discard, nil))}

	daemon.applyPreparedTabMembers(&plan)
	require.True(t, commitPreparedTabLayoutLocked(&plan))
	require.Equal(t, geometry, pty.geometry)
	require.Equal(t, vt.Geometry{
		Cols: 39, Rows: 23, PixelWidth: 391, PixelHeight: 466,
	}, pane.screen.Geometry())

	var responses string
	pane.screen.OnResponse = func(response []byte) { responses += string(response) }
	pane.screen.Write([]byte("\x1b[14t\x1b[16t"))
	require.Equal(t, "\x1b[4;466;391t\x1b[6;20;10t", responses)
}

type recordingGeometryPTY struct {
	geometry domain.Geometry
}

func (p *recordingGeometryPTY) Read([]byte) (int, error)    { return 0, io.EOF }
func (p *recordingGeometryPTY) Write(b []byte) (int, error) { return len(b), nil }
func (p *recordingGeometryPTY) Close() error                { return nil }
func (p *recordingGeometryPTY) Resize(size domain.Size) error {
	p.geometry = domain.Geometry{Size: size}
	return nil
}
func (p *recordingGeometryPTY) ResizeGeometry(geometry domain.Geometry) error {
	p.geometry = geometry
	return nil
}
func (p *recordingGeometryPTY) Pid() int                     { return 1 }
func (p *recordingGeometryPTY) ForegroundPgid() (int, error) { return 1, nil }
