package protocol

import (
	"testing"
	"time"

	renderer "github.com/bnema/vev-vt"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
)

func previewTestRequest() RemotePreviewRequest {
	return RemotePreviewRequest{
		Version: RemotePreviewSchemaVersion,
		Target: domain.RemoteSessionTarget{
			Endpoint: "local", DisplayOrigin: "local", LifecycleID: domain.SessionLifecycleID{1}, SessionName: "work", LiveTabID: "tab-1",
		},
		Width: 80, Height: 24,
	}
}

func TestValidateRemotePreviewWatch(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*RemotePreviewWatch)
		wantErr bool
	}{
		{name: "valid"},
		{name: "minimum interval", mutate: func(w *RemotePreviewWatch) { w.MinInterval = RemotePreviewWatchMinInterval }},
		{name: "maximum interval", mutate: func(w *RemotePreviewWatch) { w.MinInterval = RemotePreviewWatchMaxInterval }},
		{name: "zero interval", mutate: func(w *RemotePreviewWatch) { w.MinInterval = 0 }, wantErr: true},
		{name: "interval below bound", mutate: func(w *RemotePreviewWatch) { w.MinInterval = 15 * time.Millisecond }, wantErr: true},
		{name: "interval above bound", mutate: func(w *RemotePreviewWatch) { w.MinInterval = time.Second + time.Millisecond }, wantErr: true},
		{name: "fractional millisecond", mutate: func(w *RemotePreviewWatch) { w.MinInterval = 33*time.Millisecond + 1 }, wantErr: true},
		{name: "invalid request", mutate: func(w *RemotePreviewWatch) { w.Request.Width = 0 }, wantErr: true},
		{name: "stopped target", mutate: func(w *RemotePreviewWatch) { w.Request.Target.Stopped = true }, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			watch := RemotePreviewWatch{Request: previewTestRequest(), MinInterval: 33 * time.Millisecond}
			if tt.mutate != nil {
				tt.mutate(&watch)
			}
			err := ValidateRemotePreviewWatch(watch)
			if tt.wantErr {
				require.ErrorIs(t, err, ErrInvalidRemotePreviewWatch)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestValidateRemotePreviewCellRun(t *testing.T) {
	valid := RemotePreview{
		Version: RemotePreviewSchemaVersion, Status: RemotePreviewOK,
		LifecycleID: domain.SessionLifecycleID{1}, TabID: "tab-1", Revision: 1,
		Width: 2, Height: 1, Cells: []renderer.Cell{{Rune: 'x'}, {Rune: 'y'}},
	}
	tests := []struct {
		name    string
		mutate  func(*RemotePreview)
		wantErr bool
	}{
		{name: "valid"},
		{name: "double width continuation", mutate: func(p *RemotePreview) { p.Cells = []renderer.Cell{{Rune: '界'}, {Continuation: true}} }},
		{name: "unknown status", mutate: func(p *RemotePreview) { p.Status = RemotePreviewTooLarge + 1 }, wantErr: true},
		{name: "non-ok carries a viewport", mutate: func(p *RemotePreview) { p.Status = RemotePreviewNoSuchTarget }, wantErr: true},
		{name: "cell count mismatch", mutate: func(p *RemotePreview) { p.Cells = p.Cells[:1] }, wantErr: true},
		{name: "control rune", mutate: func(p *RemotePreview) { p.Cells = []renderer.Cell{{Rune: '\x1b'}, {Rune: 'x'}} }, wantErr: true},
		{name: "orphan continuation", mutate: func(p *RemotePreview) { p.Cells = []renderer.Cell{{Continuation: true}, {Rune: 'x'}} }, wantErr: true},
		{name: "unpaired double width", mutate: func(p *RemotePreview) { p.Cells = []renderer.Cell{{Rune: '界'}, {Rune: 'x'}} }, wantErr: true},
		{name: "unsupported style", mutate: func(p *RemotePreview) {
			p.Cells = []renderer.Cell{{Rune: 'x', Style: renderer.Style{Attrs: 1 << 7}}, {Rune: 'x'}}
		}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			preview := valid
			preview.Cells = append([]renderer.Cell(nil), valid.Cells...)
			if tt.mutate != nil {
				tt.mutate(&preview)
			}
			require.Equal(t, tt.wantErr, ValidateRemotePreview(preview) != nil)
		})
	}
}
