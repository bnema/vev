package palette

import (
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestSessionResultsAlwaysQualifyClientOrigin(t *testing.T) {
	for _, tc := range []struct {
		name   string
		result Result
		want   string
	}{
		{"local", NewActiveSessionResult(testExactTarget("sample", 1), time.Time{}), "Switch to session sample.local"},
		{"serving remote", NewActiveSessionResultWithDisplayOrigin(testExactTarget("sample", 1), time.Time{}, "host-a"), "Switch to session sample.host-a"},
		{"imported local", NewImportedSessionResult("local", "key", "sample", "local", "up", ""), "Switch to session sample.local"},
		{"discovered remote", NewRemoteSessionResult(domain.RemoteSessionKey{Name: "sample", Host: "user@host-a", DisplayOrigin: "host-a"}, domain.RemoteSessionTarget{}, ""), "Switch to session sample.host-a"},
	} {
		t.Run(tc.name, func(t *testing.T) { require.Equal(t, tc.want, tc.result.DisplayText()) })
	}
}
