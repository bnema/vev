package noticefile

import (
	"os"
	"path/filepath"
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

	// A fresh store simulates a daemon crash after Claim but before Ack.
	recovered := New(dir)
	replayed, err := recovered.Claim()
	require.NoError(t, err)
	require.Equal(t, claimed, replayed)

	require.NoError(t, recovered.Ack())
	again, err := New(dir).Claim()
	require.NoError(t, err)
	require.Empty(t, again)
}

func TestStoreClaimDoesNotLoseAppendAfterRotation(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "vev")
	store := New(dir)
	first := domain.Notification{Message: "before claim"}
	second := domain.Notification{Message: "after claim rotation"}
	require.NoError(t, store.Append(first))

	rotated := make(chan struct{})
	allowRead := make(chan struct{})
	store.afterClaimRead = func() {
		close(rotated)
		<-allowRead
	}
	claimDone := make(chan []domain.Notification, 1)
	errDone := make(chan error, 1)
	go func() {
		claimed, err := store.Claim()
		claimDone <- claimed
		errDone <- err
	}()
	<-rotated

	appendDone := make(chan error, 1)
	go func() { appendDone <- New(dir).Append(second) }()
	close(allowRead)
	require.NoError(t, <-errDone)
	require.Equal(t, []domain.Notification{first}, <-claimDone)
	require.NoError(t, <-appendDone)

	require.NoError(t, store.Ack())
	claimed, err := New(dir).Claim()
	require.NoError(t, err)
	require.Equal(t, []domain.Notification{second}, claimed)
	require.NoError(t, New(dir).Ack())
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
