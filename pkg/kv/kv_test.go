package kv

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSyncCrashBoundaries(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		leaveTmp  bool
		corrupt   bool
		wantOpen  bool
		wantValue string
	}{
		{name: "clean file opens", wantOpen: true, wantValue: "v1"},
		{name: "stray tmp is ignored", leaveTmp: true, wantOpen: true, wantValue: "v1"},
		{name: "corrupt file fails closed", corrupt: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			path := privateStorePath(t)
			store, err := Open(path)
			require.NoError(t, err)
			require.NoError(t, store.Set([]byte("k"), []byte("v1")))
			require.NoError(t, store.Sync())
			require.NoError(t, store.Close())

			if tt.leaveTmp {
				require.NoError(t, os.WriteFile(path+".tmp", []byte("half written"), 0o600))
			}
			if tt.corrupt {
				data, err := os.ReadFile(path)
				require.NoError(t, err)
				data[len(data)-1] ^= 0xff
				require.NoError(t, os.WriteFile(path, data, 0o600))
			}

			reopened, err := Open(path)
			if !tt.wantOpen {
				require.ErrorIs(t, err, ErrCorrupt)
				return
			}
			require.NoError(t, err)
			t.Cleanup(func() { _ = reopened.Close() })
			got, ok := reopened.Get([]byte("k"))
			require.True(t, ok)
			require.Equal(t, tt.wantValue, string(got))
		})
	}
}

func TestSyncIsAtomicAcrossKeys(t *testing.T) {
	t.Parallel()
	path := privateStorePath(t)
	store, err := Open(path)
	require.NoError(t, err)
	require.NoError(t, store.Set([]byte("old"), []byte("x")))
	require.NoError(t, store.Sync())
	require.NoError(t, store.Delete([]byte("old")))
	require.NoError(t, store.Set([]byte("new"), []byte("x")))
	require.NoError(t, store.Sync())
	require.NoError(t, store.Close())

	reopened, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	_, hadOld := reopened.Get([]byte("old"))
	_, hasNew := reopened.Get([]byte("new"))
	require.False(t, hadOld)
	require.True(t, hasNew)
}

func TestOpenFailsClosedWithoutMutation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		raw  []byte
	}{
		{name: "empty", raw: []byte{}},
		{name: "short magic", raw: []byte("VE")},
		{name: "unknown magic", raw: []byte("NOPE\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00")},
		{name: "unknown version", raw: func() []byte { b := validEmptyFile(); binary.BigEndian.PutUint16(b[4:6], 99); return b }()},
		{name: "truncated entry", raw: append(validEmptyFile()[:10], 0, 0, 0, 1)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			path := privateStorePath(t)
			require.NoError(t, os.WriteFile(path, tt.raw, 0o600))
			_, err := Open(path)
			require.ErrorIs(t, err, ErrCorrupt)
			after, readErr := os.ReadFile(path)
			require.NoError(t, readErr)
			require.Equal(t, tt.raw, after)
		})
	}
}

func TestCloseSyncsDirtyStore(t *testing.T) {
	t.Parallel()
	path := privateStorePath(t)
	store, err := Open(path)
	require.NoError(t, err)
	require.NoError(t, store.Set([]byte("k"), []byte("v")))
	require.NoError(t, store.Close())

	reopened, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	got, ok := reopened.Get([]byte("k"))
	require.True(t, ok)
	require.Equal(t, []byte("v"), got)
}

func TestStoreCopiesValuesAndRangesInKeyOrder(t *testing.T) {
	t.Parallel()
	store, err := Open(privateStorePath(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	value := []byte("two")
	require.NoError(t, store.Set([]byte("b"), value))
	value[0] = 'x'
	require.NoError(t, store.Set([]byte("a"), []byte("one")))
	got, ok := store.Get([]byte("b"))
	require.True(t, ok)
	require.Equal(t, []byte("two"), got)
	got[0] = 'x'
	gotAgain, _ := store.Get([]byte("b"))
	require.Equal(t, []byte("two"), gotAgain)

	var keys []string
	store.Range(func(k, v []byte) bool {
		keys = append(keys, string(k))
		return true
	})
	require.Equal(t, []string{"a", "b"}, keys)
}

func TestStoreLockAndClosedErrors(t *testing.T) {
	t.Parallel()
	path := privateStorePath(t)
	store, err := Open(path)
	require.NoError(t, err)
	_, err = Open(path)
	require.Error(t, err)
	require.NoError(t, store.Close())
	require.NoError(t, store.Close())
	require.ErrorIs(t, store.Set([]byte("k"), []byte("v")), os.ErrClosed)
	require.ErrorIs(t, store.Delete([]byte("k")), os.ErrClosed)
	require.ErrorIs(t, store.Sync(), os.ErrClosed)
}

func privateStorePath(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "vev")
	require.NoError(t, os.Mkdir(dir, 0o700))
	return filepath.Join(dir, "sessions.kv")
}

func validEmptyFile() []byte {
	return encodeFile(map[string][]byte{})
}

func TestDecodeRejectsDuplicateKeysAndTrailingData(t *testing.T) {
	t.Parallel()
	payload := make([]byte, 0)
	payload = append(payload, fileMagic[:]...)
	payload = binary.BigEndian.AppendUint16(payload, fileVersion)
	payload = binary.BigEndian.AppendUint32(payload, 2)
	for range 2 {
		payload = binary.BigEndian.AppendUint32(payload, 1)
		payload = append(payload, 'k')
		payload = binary.BigEndian.AppendUint32(payload, 1)
		payload = append(payload, 'v')
	}
	raw := appendCRC(payload)
	_, err := decodeFile(raw)
	require.ErrorIs(t, err, ErrCorrupt)

	valid := validEmptyFile()
	valid = append(valid, 0)
	_, err = decodeFile(valid)
	require.True(t, errors.Is(err, ErrCorrupt))
}

func TestCloseWithoutSyncPreventsLaterCloseFromSyncing(t *testing.T) {
	t.Parallel()
	path := privateStorePath(t)
	store, err := Open(path)
	require.NoError(t, err)
	require.NoError(t, store.Set([]byte("rejected"), []byte("value")))
	require.NoError(t, store.CloseWithoutSync())
	require.NoError(t, store.Close(), "a later normal Close must remain a no-op")

	reopened, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	_, ok := reopened.Get([]byte("rejected"))
	require.False(t, ok)
}
