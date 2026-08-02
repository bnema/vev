package vt

import (
	"encoding/binary"
	"testing"

	"github.com/bnema/vev/pkg/renderer"
	"github.com/stretchr/testify/require"
)

func TestHistoryCodecRejectsMalformedRowIDsAndCounters(t *testing.T) {
	history := NewHistory(HistoryConfig{MaxRows: 4, ChunkRows: 4})
	require.NoError(t, history.Append(historyRow("a"), LineBound{End: 1}))
	require.NoError(t, history.Append(historyRow("b"), LineBound{End: 1}))
	encoded, err := MarshalHistory(history.SealAndView())
	require.NoError(t, err)

	firstIDOffset := 17 + 4
	secondIDOffset := firstIDOffset + 8 + 4 + historyCellBytes + historyBoundBytes
	for _, test := range []struct {
		name   string
		mutate func([]byte)
	}{
		{name: "zero row ID", mutate: func(data []byte) { binary.BigEndian.PutUint64(data[firstIDOffset:], 0) }},
		{name: "duplicate row ID", mutate: func(data []byte) { copy(data[secondIDOffset:secondIDOffset+8], data[firstIDOffset:firstIDOffset+8]) }},
		{name: "counter does not exceed IDs", mutate: func(data []byte) { binary.BigEndian.PutUint64(data[9:17], 2) }},
		{name: "max counter", mutate: func(data []byte) { binary.BigEndian.PutUint64(data[9:17], ^uint64(0)) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			data := append([]byte(nil), encoded...)
			test.mutate(data)
			_, unmarshalErr := UnmarshalHistory(data)
			require.Error(t, unmarshalErr)
			_, preflightErr := PreflightHistoryBlob(data)
			require.Error(t, preflightErr)
		})
	}
}

func TestHistoryAppendRejectsExhaustedRowIDCounter(t *testing.T) {
	history := NewHistory(HistoryConfig{MaxRows: 1, ChunkRows: 1})
	history.nextRowID = ^RowID(0)
	require.Error(t, history.Append(historyRow("row"), LineBound{End: 3}))
}

func TestScreenRowIDBoundaryMatchesHistoryPersistence(t *testing.T) {
	const maxPersistedRowID = ^RowID(0) - 2
	for _, test := range []struct {
		name    string
		current RowID
		want    RowID
		persist bool
	}{
		{name: "last persistable ID", current: maxPersistedRowID - 1, want: maxPersistedRowID, persist: true},
		{name: "first non-persistable ID", current: maxPersistedRowID, want: maxPersistedRowID + 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			screen := NewScreen(1, 1)
			screen.nextRowID = test.current
			allocate := func() {
				require.Equal(t, test.want, screen.nextRowIDValue())
			}
			if !test.persist {
				require.Panics(t, allocate)
				return
			}

			allocate()
			row := historyRow("x")
			screen.buffer.rowIDs[0] = test.want
			screen.buffer.frame.Row(0)[0] = row[0]
			screen.buffer.boundaries[0] = LineBound{End: 1}
			transcript, err := screen.RecoveryTranscriptSnapshot().Marshal()
			require.NoError(t, err)
			view, err := UnmarshalHistory(transcript)
			require.NoError(t, err)
			require.Equal(t, test.want, view.RowID(0))
			require.Equal(t, test.want+1, view.NextRowID())

			history := NewHistory(HistoryConfig{MaxRows: 1, ChunkRows: 1})
			require.NoError(t, history.Append(row, LineBound{End: 1}, screen.RowID(0)))
			_, err = MarshalHistory(history.SealAndView())
			require.NoError(t, err)
		})
	}
}

func TestHistoryCodecRejectsMissingRowIDsOnMarshal(t *testing.T) {
	_, err := MarshalHistory(HistoryView{
		chunks: []*HistoryChunk{{rows: [][]renderer.Cell{historyRow("row")}, bounds: []LineBound{{End: 3}}}},
		rows:   1,
	})
	require.Error(t, err)
}

func TestRestoredScreenAllocatesAbovePersistedRowIDs(t *testing.T) {
	history := NewHistory(HistoryConfig{MaxRows: 4, ChunkRows: 1})
	require.NoError(t, history.Append(historyRow("old"), LineBound{End: 3}, RowID(40)))
	sealed, tail, err := MarshalSealedHistory(history.SealAndView())
	require.NoError(t, err)
	transcript, err := MarshalHistory(HistoryView{
		chunks:    []*HistoryChunk{{rows: [][]renderer.Cell{historyRow("live")}, bounds: []LineBound{{End: 4}}, rowIDs: []RowID{50}}},
		rows:      1,
		nextRowID: 51,
	})
	require.NoError(t, err)

	screen, err := NewScreenWithRecoveryTranscript(3, 1, HistoryConfig{MaxRows: 8, ChunkRows: 2}, sealed, tail, transcript)
	require.NoError(t, err)
	require.Equal(t, RowID(40), screen.History().View().RowID(0))
	require.Equal(t, RowID(50), screen.History().View().RowID(1))
	require.Equal(t, RowID(51), screen.History().NextRowID())
	require.Greater(t, screen.RowID(0), RowID(50))

	before := screen.RowID(0)
	screen.Write([]byte("\x1b[2J"))
	require.Greater(t, screen.RowID(0), before)
}

func TestHistoryRestoreRejectsDuplicateIDsAcrossEvictedHistoryAndTranscript(t *testing.T) {
	sealedView := HistoryView{
		chunks:    []*HistoryChunk{{rows: [][]renderer.Cell{historyRow("old")}, bounds: []LineBound{{End: 3}}, rowIDs: []RowID{7}}},
		rows:      1,
		nextRowID: 8,
	}
	sealed, err := MarshalHistoryChunk(sealedView.Chunk(0))
	require.NoError(t, err)
	tail, err := MarshalEmptyHistoryTail()
	require.NoError(t, err)
	transcript, err := MarshalHistory(HistoryView{
		chunks:    []*HistoryChunk{{rows: [][]renderer.Cell{historyRow("live")}, bounds: []LineBound{{End: 4}}, rowIDs: []RowID{7}}},
		rows:      1,
		nextRowID: 8,
	})
	require.NoError(t, err)

	_, err = NewScreenWithRecoveryTranscript(4, 1, HistoryConfig{MaxRows: 1, ChunkRows: 1}, [][]byte{sealed}, tail, transcript)
	require.Error(t, err)
}

func TestHistoryRestoreRejectsDuplicateIDsAcrossBlobs(t *testing.T) {
	first := NewHistory(HistoryConfig{MaxRows: 2, ChunkRows: 1})
	second := NewHistory(HistoryConfig{MaxRows: 2, ChunkRows: 1})
	require.NoError(t, first.Append(historyRow("one"), LineBound{End: 3}, RowID(7)))
	require.NoError(t, second.Append(historyRow("two"), LineBound{End: 3}, RowID(7)))
	firstBlob, err := MarshalHistoryChunk(first.SealAndView().Chunk(0))
	require.NoError(t, err)
	secondBlob, err := MarshalHistoryChunk(second.SealAndView().Chunk(0))
	require.NoError(t, err)
	tail, err := MarshalEmptyHistoryTail()
	require.NoError(t, err)

	_, err = HistoryFromBlobs(HistoryConfig{MaxRows: 4, ChunkRows: 1}, [][]byte{firstBlob, secondBlob}, tail)
	require.Error(t, err)
}
