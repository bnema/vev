package keys

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/synctest"

	configadapter "bnema.dev/vev/internal/adapters/config"
	"github.com/stretchr/testify/require"
)

func TestLoadedMarkBindingsRemainCaseSensitive(t *testing.T) {
	for _, tt := range []struct{ name, content string }{
		{"defaults", ""},
		{"jump first", "jump-mark = alt+M\ntoggle-mark = alt+m\n"},
		{"toggle first", "toggle-mark = alt+m\njump-mark = alt+M\n"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config")
			require.NoError(t, os.WriteFile(path, []byte(tt.content), 0600))
			cfg, warnings, err := configadapter.Load(path)
			require.NoError(t, err)
			require.Empty(t, warnings)
			bindings, warnings := BuildBindingEntries(cfg.BindingEntries)
			require.Empty(t, warnings)
			assertAltRuneAction(t, bindings, 'm', ActionToggleMark)
			assertAltRuneAction(t, bindings, 'M', ActionJumpMark)
			synctest.Test(t, func(t *testing.T) {
				handler := NewMockHandler(t)
				handler.EXPECT().Action(ActionToggleMark, []byte{ESC, 'm'}).Once()
				handler.EXPECT().Action(ActionJumpMark, []byte{ESC, 'M'}).Once()
				var published atomic.Pointer[Bindings]
				published.Store(bindings)
				router := NewRouter(nil, handler, &published)
				router.Route([]byte{ESC, 'm'})
				router.Route([]byte{ESC, 'M'})
			})
		})
	}
}
