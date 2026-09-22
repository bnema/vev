package client

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
)

func TestSessionEnvironmentValidate(t *testing.T) {
	oversizeEntry := "K=" + strings.Repeat("x", ports.BrokerMaxEnvEntryBytes-2+1)
	maxEntry := "K=" + strings.Repeat("x", ports.BrokerMaxEnvEntryBytes-2)
	require.Len(t, maxEntry, ports.BrokerMaxEnvEntryBytes)
	longCwd := "/" + strings.Repeat("a", 65535)
	require.Len(t, longCwd, 65536)

	validLocal := SessionEnvironment{
		Provenance: SessionEnvironmentLocalCLI,
		Env:        []string{"FOO=bar", "EMPTY="},
		Cwd:        "/tmp",
	}

	tests := []struct {
		name    string
		env     SessionEnvironment
		wantErr bool
	}{
		{name: "unspecified empty", env: SessionEnvironment{}, wantErr: true},
		{name: "unspecified with env", env: SessionEnvironment{Env: []string{"A=b"}}, wantErr: true},
		{name: "unknown provenance", env: SessionEnvironment{Provenance: SessionEnvironmentProvenance(99)}, wantErr: true},
		{name: "local cli valid", env: validLocal, wantErr: false},
		{name: "local picker valid empty", env: SessionEnvironment{Provenance: SessionEnvironmentLocalPicker}, wantErr: false},
		{name: "local picker valid cwd", env: SessionEnvironment{Provenance: SessionEnvironmentLocalPicker, Env: []string{"A=b"}, Cwd: "/x"}, wantErr: false},
		{name: "local max entry valid", env: SessionEnvironment{Provenance: SessionEnvironmentLocalCLI, Env: []string{maxEntry}}, wantErr: false},
		{name: "too many entries", env: SessionEnvironment{Provenance: SessionEnvironmentLocalCLI, Env: make([]string, ports.BrokerMaxEnvEntries+1)}, wantErr: true},
		{name: "entry invalid utf8", env: SessionEnvironment{Provenance: SessionEnvironmentLocalCLI, Env: []string{"A=" + string([]byte{0xff})}}, wantErr: true},
		{name: "entry NUL", env: SessionEnvironment{Provenance: SessionEnvironmentLocalCLI, Env: []string{"A=b\x00c"}}, wantErr: true},
		{name: "entry too long", env: SessionEnvironment{Provenance: SessionEnvironmentLocalCLI, Env: []string{oversizeEntry}}, wantErr: true},
		{name: "entry missing equals", env: SessionEnvironment{Provenance: SessionEnvironmentLocalCLI, Env: []string{"NOEQUALS"}}, wantErr: true},
		{name: "entry empty name", env: SessionEnvironment{Provenance: SessionEnvironmentLocalCLI, Env: []string{"=value"}}, wantErr: true},
		{name: "cwd relative", env: SessionEnvironment{Provenance: SessionEnvironmentLocalCLI, Cwd: "relative/path"}, wantErr: true},
		{name: "cwd invalid utf8", env: SessionEnvironment{Provenance: SessionEnvironmentLocalCLI, Cwd: "/tmp/" + string([]byte{0xff})}, wantErr: true},
		{name: "cwd NUL", env: SessionEnvironment{Provenance: SessionEnvironmentLocalCLI, Cwd: "/tmp/\x00x"}, wantErr: true},
		{name: "cwd too long", env: SessionEnvironment{Provenance: SessionEnvironmentLocalCLI, Cwd: longCwd}, wantErr: true},
		{name: "remote empty valid", env: SessionEnvironment{Provenance: SessionEnvironmentRemote}, wantErr: false},
		{name: "remote with env", env: SessionEnvironment{Provenance: SessionEnvironmentRemote, Env: []string{"A=b"}}, wantErr: true},
		{name: "remote with cwd", env: SessionEnvironment{Provenance: SessionEnvironmentRemote, Cwd: "/tmp"}, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.env.Validate()
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSessionEnvironmentClone(t *testing.T) {
	t.Run("nil stays nil", func(t *testing.T) {
		orig := SessionEnvironment{Provenance: SessionEnvironmentLocalCLI}
		require.Nil(t, orig.Env)
		cloned := orig.Clone()
		require.Nil(t, cloned.Env)
		require.Equal(t, orig, cloned)
	})

	t.Run("empty stays non-nil empty", func(t *testing.T) {
		orig := SessionEnvironment{Provenance: SessionEnvironmentLocalCLI, Env: []string{}}
		require.NotNil(t, orig.Env)
		cloned := orig.Clone()
		require.NotNil(t, cloned.Env)
		require.Empty(t, cloned.Env)
	})

	t.Run("deep copy isolates", func(t *testing.T) {
		orig := SessionEnvironment{
			Provenance: SessionEnvironmentLocalPicker,
			Env:        []string{"A=1", "B=2"},
			Cwd:        "/tmp",
		}
		cloned := orig.Clone()
		require.Equal(t, orig, cloned)
		cloned.Env[0] = "MUTATED=x"
		require.Equal(t, "A=1", orig.Env[0])
		orig.Env[1] = "MUTATED=y"
		require.Equal(t, "B=2", cloned.Env[1])
	})
}
