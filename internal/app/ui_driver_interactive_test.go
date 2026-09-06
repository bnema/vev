//go:build linux

package app

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/bnema/vev/internal/adapters/term"
	"github.com/bnema/vev/internal/adapters/uiterm"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

func TestInteractiveObservationPublishesPhysicalWriterTransaction(t *testing.T) {
	inputReader, inputWriter, err := os.Pipe()
	require.NoError(t, err)
	defer inputReader.Close()
	defer inputWriter.Close()
	outputReader, outputWriter, err := os.Pipe()
	require.NoError(t, err)
	defer outputReader.Close()
	defer outputWriter.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mirror, err := uiterm.NewMirror(ctx, domain.Geometry{Size: domain.Size{Cols: 8, Rows: 2}}, "")
	require.NoError(t, err)
	defer mirror.Close()
	physical := term.NewWithFilesAndObservation(inputReader, outputWriter, mirror)
	observed := observedTerminal{Terminal: physical, UIOutputTransaction: mirror}
	contextValue := ports.UIContext{AttachmentHandle: "attachment", Generation: 1, Status: ports.UIStatusAttached}

	observed.BeginOutput(contextValue)
	_, err = observed.Out().Write([]byte("hello"))
	require.NoError(t, err)
	require.NoError(t, observed.Flush())
	observed.EndOutput(true)
	_, err = io.ReadAll(io.LimitReader(outputReader, int64(len("hello"))))
	require.NoError(t, err)

	snapshot, err := mirror.Snapshot()
	require.NoError(t, err)
	require.Equal(t, contextValue.AttachmentHandle, snapshot.Context.AttachmentHandle)
	require.Equal(t, contextValue.Generation, snapshot.Context.Generation)
	require.True(t, strings.HasPrefix(snapshotText(snapshot), "hello"))
}

func snapshotText(snapshot ports.UISnapshot) string {
	var text []byte
	for _, cell := range snapshot.Cells {
		if cell.Continuation {
			continue
		}
		if cell.Text == "" {
			text = append(text, ' ')
			continue
		}
		text = append(text, cell.Text...)
	}
	return string(text)
}
