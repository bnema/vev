package daemon

import (
	"fmt"
	"testing"

	vt "github.com/bnema/vev-vt"
	"github.com/bnema/vev-vt/ansi"
	"github.com/stretchr/testify/require"
)

func newTestHistory(rows int) *vt.History { return vt.NewHistory(vt.HistoryConfig{MaxRows: rows}) }

func captureTestFrame(source vt.CellSource) vt.Frame {
	frame := vt.NewFrame(source.Columns(), source.Rows())
	for y := range source.Rows() {
		for x := range source.Columns() {
			frame.Set(x, y, source.Cell(x, y))
		}
	}
	return frame
}

func writeTestRow(screen *vt.Screen, row int, text string) {
	screen.Write([]byte(fmt.Sprintf("\x1b7\x1b[%d;1H%s\x1b8", row+1, text)))
}

func writeTestFrame(t testing.TB, screen *vt.Screen, frame vt.Frame) {
	t.Helper()
	data, err := ansi.New(ansi.Capabilities{}).Draw(frame, []ansi.Damage{ansi.FullRedraw()})
	require.NoError(t, err)
	screen.Write(data)
}

func installTestHistory(p *pane, config vt.HistoryConfig) {
	p.screen = vt.NewScreenWithHistory(p.screen.Columns(), p.screen.Rows(), config)
	p.history = p.screen.History()
}
