package noticefile

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
)

func TestStoreClaimAckRoundTrip(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "vev")
	store := New(dir)

	first := domain.Notification{
		Code:      domain.NoticeSnapshotWrite,
		Severity:  domain.NoticeError,
		Message:   "session foo shut down without saving terminal state",
		Details:   "boom: disk full",
		Time:      time.Date(2026, 7, 20, 10, 0, 0, 0, time.UTC),
		Count:     1,
		SessionID: "sess-1",
	}
	second := domain.Notification{
		Code:     domain.NoticeSnapshotRestore,
		Severity: domain.NoticeWarn,
		Message:  "session bar could not be restored",
		Time:     time.Date(2026, 7, 20, 10, 5, 0, 0, time.UTC),
		Count:    2,
	}
	require.NoError(t, store.Append(first))
	require.NoError(t, store.Append(second))

	claimed, err := store.Claim()
	require.NoError(t, err)
	require.Equal(t, []domain.Notification{first, second}, claimed)

	// Releasing the claim lock simulates the kernel closing it when the daemon
	// crashes. The in-flight file remains and a fresh process/store replays it.
	store.releaseClaimLock()
	recovered := New(dir)
	replayed, err := recovered.Claim()
	require.NoError(t, err)
	require.Equal(t, claimed, replayed)

	require.NoError(t, recovered.Ack())
	again, err := New(dir).Claim()
	require.NoError(t, err)
	require.Empty(t, again)
}

func TestStoreClaimWaitsForAppendInCriticalSection(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "vev")
	store := New(dir)
	first := domain.Notification{Message: "before claim"}
	second := domain.Notification{Message: "append holds pending file open"}
	require.NoError(t, store.Append(first))

	appendEntered := make(chan struct{})
	allowAppend := make(chan struct{})
	store.afterAppendOpen = func() {
		close(appendEntered)
		<-allowAppend
	}
	appendDone := make(chan error, 1)
	go func() { appendDone <- store.Append(second) }()
	<-appendEntered // Append owns the operation lock and an open pending file.

	claimant := New(dir)
	claimDone := make(chan []domain.Notification, 1)
	errDone := make(chan error, 1)
	go func() {
		claimed, err := claimant.Claim()
		claimDone <- claimed
		errDone <- err
	}()
	close(allowAppend)
	require.NoError(t, <-appendDone)
	require.NoError(t, <-errDone)
	require.Equal(t, []domain.Notification{first, second}, <-claimDone)
	require.NoError(t, claimant.Ack())
}

func TestStoreRepeatedClaimReplaysOwnerAndBlocksCompetitor(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "vev")
	owner := New(dir)
	want := domain.Notification{Message: "owned"}
	require.NoError(t, owner.Append(want))

	first, err := owner.Claim()
	require.NoError(t, err)
	require.Equal(t, []domain.Notification{want}, first)

	replayed, err := owner.Claim()
	require.NoError(t, err)
	require.Equal(t, first, replayed)

	other := New(dir)
	blocked, err := other.Claim()
	require.ErrorIs(t, err, ErrClaimInProgress)
	require.Nil(t, blocked)

	require.NoError(t, owner.Ack())
	empty, err := other.Claim()
	require.NoError(t, err)
	require.Empty(t, empty)
}

func TestStoreAckReleasesClaimAfterCleanupFailure(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name   string
		inject func(*Store, error)
		want   []domain.Notification
	}{
		{
			name: "remove",
			inject: func(store *Store, cause error) {
				store.removeFile = func(string) error { return cause }
			},
			want: []domain.Notification{{Message: "owned"}},
		},
		{
			name: "sync directory",
			inject: func(store *Store, cause error) {
				store.syncDirectory = func(string) error { return cause }
			},
			want: nil,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "vev")
			owner := New(dir)
			require.NoError(t, owner.Append(domain.Notification{Message: "owned"}))
			_, err := owner.Claim()
			require.NoError(t, err)

			cause := errors.New("cleanup failed")
			tt.inject(owner, cause)
			require.ErrorIs(t, owner.Ack(), cause)

			// A new Store proves the failed Ack released the ownership flock.
			recovered, err := New(dir).Claim()
			require.NoError(t, err)
			require.Equal(t, tt.want, recovered)
		})
	}
}

func TestStoreClaimOwnershipPreventsLiveReplayOrAck(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "vev")
	owner := New(dir)
	require.NoError(t, owner.Append(domain.Notification{Message: "owned"}))
	claimed, err := owner.Claim()
	require.NoError(t, err)
	require.Len(t, claimed, 1)

	other := New(dir)
	replayed, err := other.Claim()
	require.ErrorIs(t, err, ErrClaimInProgress)
	require.Nil(t, replayed)
	require.ErrorIs(t, other.Ack(), ErrNoClaimOwner)

	// The rejected Ack must leave the owner's in-flight record intact.
	require.NoError(t, owner.Ack())
	empty, err := New(dir).Claim()
	require.NoError(t, err)
	require.Empty(t, empty)
}

func TestStoreClaimMissingFileReturnsNil(t *testing.T) {
	t.Parallel()

	got, err := New(filepath.Join(t.TempDir(), "vev")).Claim()
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestStoreClaimSkipsGarbageLinesInOrder(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "vev")
	store := New(dir)
	path := filepath.Join(dir, fileName)
	require.NoError(t, os.MkdirAll(dir, 0o700))

	good := `{"Code":9,"Severity":2,"Message":"first"}` + "\n"
	garbage := "not json at all\n"
	good2 := `{"Code":10,"Severity":1,"Message":"second"}` + "\n"
	require.NoError(t, os.WriteFile(path, []byte(good+garbage+good2), 0o600))

	got, err := store.Claim()
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, "first", got[0].Message)
	require.Equal(t, "second", got[1].Message)
	require.NoError(t, store.Ack())
}

func TestStoreClaimSkipsOversizedLines(t *testing.T) {
	t.Parallel()

	validLine := func(message string) string {
		t.Helper()
		data, err := json.Marshal(domain.Notification{Message: message})
		require.NoError(t, err)
		return string(data)
	}
	// Derive JSON overhead from the encoder rather than duplicating its current
	// field layout here. The boundary payload uses ASCII so each added message
	// byte remains one marshaled byte.
	boundaryMessage := strings.Repeat("x", maxNoticeRecordSize-len(validLine("")))

	for _, tt := range []struct {
		name  string
		lines []string
		want  []string
	}{
		{
			name:  "recovers after oversized line",
			lines: []string{validLine("first"), validLine(strings.Repeat("x", len(boundaryMessage)+1)), validLine("second")},
			want:  []string{"first", "second"},
		},
		{
			name:  "skips oversized final unterminated line",
			lines: []string{validLine("first"), validLine(strings.Repeat("x", len(boundaryMessage)+1))},
			want:  []string{"first"},
		},
		{
			name:  "accepts record at size limit",
			lines: []string{validLine(boundaryMessage)},
			want:  []string{boundaryMessage},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "vev")
			records := strings.Join(tt.lines, "\n")
			if tt.name != "skips oversized final unterminated line" {
				records += "\n"
			}
			require.NoError(t, os.MkdirAll(dir, 0o700))
			require.NoError(t, os.WriteFile(filepath.Join(dir, fileName), []byte(records), 0o600))

			got, err := New(dir).Claim()
			require.NoError(t, err)
			require.Len(t, got, len(tt.want))
			for i, want := range tt.want {
				require.Equal(t, want, got[i].Message)
			}
		})
	}
}

func TestStoreAppendUsesPrivatePermissions(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "vev")
	store := New(dir)

	require.NoError(t, store.Append(domain.Notification{Message: "x"}))
	info, err := os.Stat(dir)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o700), info.Mode().Perm())
	fi, err := os.Stat(filepath.Join(dir, fileName))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), fi.Mode().Perm())

	_, err = store.Claim()
	require.NoError(t, err)
	fi, err = os.Stat(filepath.Join(dir, inFlightFileName))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), fi.Mode().Perm())
}
