package daemon

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestStartupRecoveryCounts(t *testing.T) {
	var buffer bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buffer, nil))
	ref := &domain.CheckpointRef{Generation: 1, ManifestDigest: [32]byte{1}}
	records := []domain.CatalogueRecord{
		{Name: "one", IncarnationID: domain.IncarnationID{1}, Committed: ref},
		{Name: "two", IncarnationID: domain.IncarnationID{2}, Committed: ref},
		{Name: "new", IncarnationID: domain.IncarnationID{3}},
		{Name: "broken", IncarnationID: domain.IncarnationID{4}, Committed: ref, DegradedReason: "terminal contents must not appear"},
	}
	d := New(nil, stubClock{}, log, WithCatalogue(newDurableRecoveryCatalogue(records), records))
	d.logSessionRestoreComplete(records[0], 7, false)
	d.logSessionDegraded(records[3], "checkpoint-invalid")
	d.logStartupRecoveryCounts(0)

	entries := daemonJSONLogs(t, buffer.Bytes())
	restore := daemonRequireEvent(t, entries, "session_restore_complete")
	require.Equal(t, "one", restore["session"])
	require.Equal(t, records[0].IncarnationID.String(), restore["incarnation"])
	require.EqualValues(t, 7, restore["generation"])
	degraded := daemonRequireEvent(t, entries, "session_degraded")
	require.Equal(t, "checkpoint-invalid", degraded["reason_code"])
	require.NotContains(t, degraded, "reason")
	startup := daemonRequireEvent(t, entries, "daemon_startup_complete")
	require.EqualValues(t, 2, startup["healthy"])
	require.EqualValues(t, 1, startup["fresh"])
	require.EqualValues(t, 0, startup["restoring"])
	require.EqualValues(t, 1, startup["broken"])
}

type failingRecordsCatalogue struct {
	*durableRecoveryCatalogue
	err error
}

func (c failingRecordsCatalogue) Records() ([]domain.CatalogueRecord, error) {
	return nil, c.err
}

func TestStartupRecoveryCountsFallsBackWhenCatalogueReadFails(t *testing.T) {
	var buffer bytes.Buffer
	record := domain.CatalogueRecord{Name: "broken", IncarnationID: domain.IncarnationID{4}, Committed: &domain.CheckpointRef{Generation: 1, ManifestDigest: [32]byte{1}}, DegradedReason: "invalid"}
	d := New(nil, stubClock{}, slog.New(slog.NewJSONHandler(&buffer, nil)))
	d.persistEnabled = true
	d.catalogue = failingRecordsCatalogue{durableRecoveryCatalogue: newDurableRecoveryCatalogue(nil), err: errors.New("read failed")}
	d.stopped[record.Name] = stoppedSession{record: record}

	d.logStartupRecoveryCounts(0)

	entries := daemonJSONLogs(t, buffer.Bytes())
	daemonRequireEvent(t, entries, "daemon_startup_catalogue_read_failed")
	startup := daemonRequireEvent(t, entries, "daemon_startup_complete")
	require.EqualValues(t, 1, startup["broken"])
}

func daemonJSONLogs(t *testing.T, data []byte) []map[string]any {
	t.Helper()
	var entries []map[string]any
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		var entry map[string]any
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &entry))
		entries = append(entries, entry)
	}
	require.NoError(t, scanner.Err())
	return entries
}

func daemonRequireEvent(t *testing.T, entries []map[string]any, name string) map[string]any {
	t.Helper()
	for _, entry := range entries {
		if entry["msg"] == name {
			return entry
		}
	}
	t.Fatalf("event %q not found", name)
	return nil
}
